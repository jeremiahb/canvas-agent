"""
Canvas Crawler
Navigates Canvas like a student: reads courses, assignments, modules,
announcements, files, grades, and discussion boards.
Stores everything in ChromaDB for the agent brain to use.

Review fixes applied:
  - RF-4  Rate limiting: random human-paced delays on every navigation
  - RF-5  Async context manager guarantees browser cleanup on exceptions
  - RF-17 Canvas URL driven by CANVAS_URL env var
  - RF-24 Emoji removed from log strings (ASCII labels only)
"""

import asyncio
import json
import logging
import os
import random
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from playwright.async_api import async_playwright, BrowserContext, Page

from agent.document_ingester import DocumentIngester

logger = logging.getLogger(__name__)

CANVAS_URL = os.environ.get("CANVAS_URL", "https://wilmu.instructure.com").rstrip("/")

_MIN_DELAY = 1.5
_MAX_DELAY = 3.5


async def _polite_goto(page: Page, url: str) -> None:
    """
    Navigate to url after a randomised human-paced delay.
    Extracted as a standalone coroutine so DocumentIngester can reuse
    the same rate-limiting behaviour without coupling to CanvasCrawler.
    RF-4, RF-Ingester-bypass.
    """
    delay = random.uniform(_MIN_DELAY, _MAX_DELAY)
    logger.debug(f"Sleeping {delay:.1f}s before: {url}")
    await asyncio.sleep(delay)
    try:
        await page.goto(url, wait_until="networkidle", timeout=45000)
    except Exception:
        # networkidle can time out on dynamic Canvas pages — fall back to
        # domcontentloaded which resolves as soon as the HTML is parsed
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(2.0)  # brief settle time for JS rendering


class CanvasCrawler:
    def __init__(self, cookie_path: str = ""):
        """
        cookie_path: path to canvas_cookies.json.
        Defaults to DATA_DIR/cookies/canvas_cookies.json.
        """
        if not cookie_path:
            data_dir = os.environ.get("DATA_DIR", "data")
            cookie_path = str(Path(data_dir) / "cookies" / "canvas_cookies.json")
        self.cookie_path = cookie_path
        self.base_url = CANVAS_URL
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self._pw = None
        self.knowledge: dict = {
            "crawled_at": None,
            "courses": [],
        }

    # ------------------------------------------------------------------ #
    #  RF-5: context manager guarantees stop() is always called           #
    # ------------------------------------------------------------------ #

    async def __aenter__(self) -> "CanvasCrawler":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self.stop()
        return False

    # ------------------------------------------------------------------ #
    #  Session                                                             #
    # ------------------------------------------------------------------ #

    async def load_cookies(self) -> list:
        """Load, validate, and clean cookies from disk."""
        path = Path(self.cookie_path)
        if not path.exists():
            raise FileNotFoundError(f"Cookie file not found: {self.cookie_path}")

        with open(path) as f:
            data = json.load(f)

        if not isinstance(data.get("cookies"), list):
            raise ValueError("Cookie file is missing a valid 'cookies' list")

        # Map Chrome extension sameSite values to Playwright-accepted values.
        # Chrome uses "no_restriction" / "lax" / "strict" / "unspecified".
        # Playwright requires exactly "None" / "Lax" / "Strict".
        SAME_SITE_MAP = {
            "no_restriction": "None",
            "lax":            "Lax",
            "strict":         "Strict",
            "unspecified":    "Lax",
        }
        cleaned = []
        for c in data["cookies"]:
            raw_ss = (c.get("sameSite") or "").lower()
            c["sameSite"] = SAME_SITE_MAP.get(raw_ss, "Lax")
            if c.get("expires") is not None and c["expires"] < 0:
                c.pop("expires")
            cleaned.append(c)

        return cleaned

    async def start(self, headless: bool = True) -> None:
        """Launch headless Chromium and inject Canvas session cookies."""
        self._pw = await async_playwright().start()

        # If PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH is set (e.g. pointing to the
        # Nix-installed chromium on Railway), use it directly so we don't depend
        # on Playwright's own downloaded browser binary which may be missing libs.
        executable_path = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH") or None

        browser = await self._pw.chromium.launch(
            headless=headless,
            executable_path=executable_path,
        )
        self.context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        cookies = await self.load_cookies()
        await self.context.add_cookies(cookies)
        self.page = await self.context.new_page()
        logger.info("Browser started with Canvas session cookies")

    async def stop(self) -> None:
        """Release all browser resources. Safe to call multiple times."""
        try:
            if self.context:
                await self.context.close()
        except Exception as e:
            logger.warning(f"Error closing browser context: {e}")
        try:
            if self._pw:
                await self._pw.stop()
        except Exception as e:
            logger.warning(f"Error stopping Playwright: {e}")
        finally:
            self.context = None
            self.page = None
            self._pw = None

    async def _goto(self, url: str) -> None:
        """Delegate to module-level helper so DocumentIngester can share it."""
        await _polite_goto(self.page, url)

    async def verify_session(self) -> bool:
        """Return True if session cookies grant an authenticated Canvas session."""
        await self._goto(f"{self.base_url}/profile")
        if "login" in self.page.url.lower():
            logger.error("Session invalid — cookies may have expired")
            return False
        logger.info("Session valid")
        return True

    # ------------------------------------------------------------------ #
    #  Courses                                                             #
    # ------------------------------------------------------------------ #

    async def get_courses(self) -> list:
        """Return all active enrolled courses from the Canvas dashboard."""
        # Canvas dashboard cards are at the root URL, not /courses.
        # /courses shows an enrollment list without the ic-DashboardCard elements.
        await self._goto(f"{self.base_url}/")
        await self.page.wait_for_timeout(2000)  # cards load via JS after networkidle

        courses = []
        for card in await self.page.query_selector_all(".ic-DashboardCard"):
            try:
                link = await card.query_selector("a.ic-DashboardCard__link")
                if not link:
                    continue

                href = await link.get_attribute("href") or ""
                if "/courses/" not in href:
                    logger.warning(f"Skipping card with unexpected href: {href!r}")
                    continue

                name_el = await card.query_selector(".ic-DashboardCard__header-title")
                code_el = await card.query_selector(".ic-DashboardCard__header-subtitle")

                name = await name_el.inner_text() if name_el else "Unknown"
                code = await code_el.inner_text() if code_el else ""
                course_id = href.split("/courses/")[1].split("/")[0]

                courses.append({
                    "id": course_id,
                    "name": name.strip(),
                    "code": code.strip(),
                    "url": f"{self.base_url}{href}",
                })
                logger.info(f"[COURSE] {name.strip()} (id={course_id})")  # RF-24

            except Exception as e:
                logger.warning(f"Error parsing course card: {e}", exc_info=True)

        return courses

    # ------------------------------------------------------------------ #
    #  Assignments                                                         #
    # ------------------------------------------------------------------ #

    async def get_assignments(self, course_id: str) -> list:
        """Return all assignments for a course."""
        await self._goto(f"{self.base_url}/courses/{course_id}/assignments")

        # Canvas assignments page is React-rendered — wait for the assignment
        # groups to actually appear in the DOM, up to 10 seconds.
        try:
            await self.page.wait_for_selector(
                ".assignment-group, .ig-row, [data-view='assignment']",
                timeout=10000
            )
        except Exception:
            logger.warning(f"Assignments selector not found for course {course_id} — page may be empty or still loading")
            await self.page.wait_for_timeout(3000)

        assignments = []

        # Try primary Canvas selectors (.assignment-group > .assignment)
        groups = await self.page.query_selector_all(".assignment-group")
        if groups:
            for group in groups:
                for item in await group.query_selector_all(".assignment"):
                    try:
                        link = await item.query_selector("a.ig-title")
                        if not link:
                            continue
                        href = await link.get_attribute("href") or ""
                        if "/assignments/" not in href:
                            continue
                        due_el = await item.query_selector(".assignment-date-due")
                        points_el = await item.query_selector(".non-screenreader")
                        assignments.append({
                            "id": href.split("/assignments/")[1].split("/")[0],
                            "title": (await link.inner_text()).strip(),
                            "url": f"{self.base_url}{href}",
                            "due": (await due_el.inner_text()).strip() if due_el else "No due date",
                            "points": (await points_el.inner_text()).strip() if points_el else "",
                            "details": None,
                        })
                    except Exception as e:
                        logger.warning(f"Error parsing assignment item: {e}")

        # Fallback: scan all assignment links directly from the page
        if not assignments:
            logger.info(f"Primary selector found nothing for {course_id}, trying link fallback")
            links = await self.page.query_selector_all("a[href*='/assignments/']")
            seen = set()
            for link in links:
                try:
                    href = await link.get_attribute("href") or ""
                    if "/assignments/" not in href or href in seen:
                        continue
                    # Skip syllabus and submission links
                    if "syllabus" in href or "submissions" in href:
                        continue
                    seen.add(href)
                    title = (await link.inner_text()).strip()
                    if not title:
                        continue
                    aid = href.split("/assignments/")[1].split("/")[0]
                    if not aid.isdigit():
                        continue
                    assignments.append({
                        "id": aid,
                        "title": title,
                        "url": f"{self.base_url}{href}" if href.startswith("/") else href,
                        "due": "No due date",
                        "points": "",
                        "details": None,
                    })
                except Exception as e:
                    logger.warning(f"Error in link fallback: {e}")

        return assignments

    async def get_assignment_details(self, assignment_url: str) -> dict:
        """Return description, submission types, points, and rubric for one assignment."""
        await self._goto(assignment_url)
        try:
            await self.page.wait_for_selector("#assignment_description, .assignment-description", timeout=8000)
        except Exception:
            await self.page.wait_for_timeout(2000)

        details: dict = {
            "description": "",
            "submission_types": [],
            "points_possible": "",
            "rubric": [],
        }

        try:
            desc_el = await self.page.query_selector("#assignment_description")
            if desc_el:
                details["description"] = await desc_el.inner_text()

            details["submission_types"] = [
                await el.inner_text()
                for el in await self.page.query_selector_all(".submission_type")
            ]

            pts_el = await self.page.query_selector(".points_possible")
            if pts_el:
                details["points_possible"] = await pts_el.inner_text()

            for row in await self.page.query_selector_all(".rubric_criterion"):
                desc = await row.query_selector(".description")
                long_desc = await row.query_selector(".long_description")
                pts = await row.query_selector(".criterion_points")
                details["rubric"].append({
                    "criterion": await desc.inner_text() if desc else "",
                    "description": await long_desc.inner_text() if long_desc else "",
                    "points": await pts.inner_text() if pts else "",
                })

        except Exception as e:
            logger.warning(f"Error parsing details at {assignment_url}: {e}", exc_info=True)

        return details

    # ------------------------------------------------------------------ #
    #  Announcements                                                       #
    # ------------------------------------------------------------------ #

    async def get_announcements(self, course_id: str) -> list:
        """Return all announcements for a course."""
        await self._goto(f"{self.base_url}/courses/{course_id}/announcements")
        try:
            await self.page.wait_for_selector(".ic-announcement-row, .discussion-topic", timeout=8000)
        except Exception:
            await self.page.wait_for_timeout(2000)

        announcements = []
        for item in await self.page.query_selector_all(".ic-announcement-row"):
            try:
                title_el = await item.query_selector(".ic-announcement-row__content-title")
                date_el = await item.query_selector("time")
                title = (await title_el.inner_text()).strip() if title_el else ""
                if title:
                    announcements.append({
                        "title": title,
                        "date": await date_el.get_attribute("datetime") if date_el else "",
                    })
            except Exception as e:
                logger.warning(f"Error parsing announcement: {e}", exc_info=True)

        return announcements

    # ------------------------------------------------------------------ #
    #  Modules                                                             #
    # ------------------------------------------------------------------ #

    async def get_modules(self, course_id: str) -> list:
        """Return all modules and their items for a course."""
        await self._goto(f"{self.base_url}/courses/{course_id}/modules")
        try:
            await self.page.wait_for_selector(".context_module", timeout=8000)
        except Exception:
            await self.page.wait_for_timeout(2000)

        modules = []
        for mod in await self.page.query_selector_all(".context_module"):
            try:
                name_el = await mod.query_selector(".ig-header-title")
                name = (await name_el.inner_text()).strip() if name_el else "Unnamed Module"

                items = []
                for item in await mod.query_selector_all(".context_module_item"):
                    link = await item.query_selector("a.title")
                    if link:
                        item_href = await link.get_attribute("href") or ""
                        items.append({
                            "title": (await link.inner_text()).strip(),
                            "url": f"{self.base_url}{item_href}" if item_href else "",
                        })

                modules.append({"name": name, "items": items})

            except Exception as e:
                logger.warning(f"Error parsing module: {e}", exc_info=True)

        return modules

    # ------------------------------------------------------------------ #
    #  Grades                                                              #
    # ------------------------------------------------------------------ #

    async def get_grades(self, course_id: str) -> list:
        """Return all graded assignments for a course."""
        await self._goto(f"{self.base_url}/courses/{course_id}/grades")
        try:
            await self.page.wait_for_selector("tr.student_assignment, .gradebook-cell", timeout=8000)
        except Exception:
            await self.page.wait_for_timeout(2000)

        grades = []
        for row in await self.page.query_selector_all("tr.student_assignment"):
            try:
                title_el = await row.query_selector(".title a")
                score_el = await row.query_selector(".score")
                possible_el = await row.query_selector(".possible")

                title = (await title_el.inner_text()).strip() if title_el else ""
                if title:
                    grades.append({
                        "assignment": title,
                        "score": (await score_el.inner_text()).strip() if score_el else "-",
                        "possible": (await possible_el.inner_text()).strip() if possible_el else "-",
                    })
            except Exception as e:
                logger.warning(f"Error parsing grade row: {e}", exc_info=True)

        return grades

    # ------------------------------------------------------------------ #
    #  Syllabus                                                            #
    # ------------------------------------------------------------------ #

    async def get_syllabus(self, course_id: str) -> str:
        """Return the plain-text syllabus for a course."""
        await self._goto(f"{self.base_url}/courses/{course_id}/assignments/syllabus")
        await self.page.wait_for_timeout(500)
        el = await self.page.query_selector("#course_syllabus")
        return (await el.inner_text()) if el else ""

    # ------------------------------------------------------------------ #
    #  Full crawl                                                          #
    # ------------------------------------------------------------------ #

    async def crawl_all(self) -> dict:
        """Crawl every course and all its content, returning the full knowledge dict."""
        logger.info("[START] Full Canvas crawl")
        self.knowledge["crawled_at"] = datetime.now().isoformat()
        self.knowledge["courses"] = []

        courses = await self.get_courses()

        for course in courses:
            cid = course["id"]
            logger.info(f"[CRAWL] {course['name']}")  # RF-24: no emoji

            course["syllabus"] = await self.get_syllabus(cid)
            course["announcements"] = await self.get_announcements(cid)
            course["modules"] = await self.get_modules(cid)
            course["grades"] = await self.get_grades(cid)

            assignments = await self.get_assignments(cid)
            logger.info(f"  Found {len(assignments)} assignments")

            for a in assignments:
                logger.info(f"  -> {a['title']} (due: {a['due']})")
                a["details"] = await self.get_assignment_details(a["url"])

            course["assignments"] = assignments

            # RF-Ingester-bypass: pass _polite_goto so the ingester uses
            # the same rate-limiting behaviour as the crawler.
            logger.info(f"  Ingesting documents for: {course['name']}")
            ingester = DocumentIngester(
                page=self.page,
                base_url=self.base_url,
                goto_fn=_polite_goto,
            )
            doc_results = await ingester.ingest_course_documents(cid, course["name"])
            course["documents"] = doc_results["ingested"]
            course["flagged_external"] = doc_results["flagged"]
            logger.info(
                f"  Documents: {len(doc_results['ingested'])} ingested, "
                f"{len(doc_results['flagged'])} flagged"
            )

            self.knowledge["courses"].append(course)

        logger.info(f"[DONE] Crawl complete -- {len(courses)} courses indexed")  # RF-24
        return self.knowledge

    async def save_knowledge(self, path: str = "") -> None:
        """Persist the crawl snapshot to disk."""
        if not path:
            data_dir = os.environ.get("DATA_DIR", "data")
            path = str(Path(data_dir) / "knowledge" / "canvas_knowledge.json")
        dest = Path(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(self.knowledge, indent=2))
        logger.info(f"Knowledge saved to {path}")


# ------------------------------------------------------------------ #
#  Standalone entry point                                              #
# ------------------------------------------------------------------ #

async def run_crawl() -> None:
    """Run a full crawl from the command line."""
    logging.basicConfig(level=logging.INFO)

    async with CanvasCrawler() as crawler:
        if not await crawler.verify_session():
            print("\nSession invalid. Re-export your cookies and try again.")
            return

        knowledge = await crawler.crawl_all()
        await crawler.save_knowledge()

    print("\nSummary:")
    for course in knowledge["courses"]:
        print(
            f"  {course['name']}: "
            f"{len(course.get('assignments', []))} assignments, "
            f"{len(course.get('modules', []))} modules"
        )


if __name__ == "__main__":
    asyncio.run(run_crawl())
