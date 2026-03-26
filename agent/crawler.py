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
import re
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from playwright.async_api import async_playwright, BrowserContext, Page

from agent.document_ingester import DocumentIngester, extract_html_text

logger = logging.getLogger(__name__)

CANVAS_URL = os.environ.get("CANVAS_URL", "https://wilmu.instructure.com").rstrip("/")

_MIN_DELAY = 0.8
_MAX_DELAY = 1.8

# Course name fragments to skip — admin/orientation courses with no real assignments
_SKIP_COURSE_FRAGMENTS = ["BrushUp", "Student Assistance", "Orientation", "Tutorial"]


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
    #  REST API helper                                                     #
    # ------------------------------------------------------------------ #

    async def _api_get(self, path: str) -> list | dict:
        """
        Fetch a Canvas REST API endpoint using the current session cookies.
        Uses Playwright's API client so it shares the authenticated browser
        session without needing a separate token.
        Returns parsed JSON (list or dict) or empty list on failure.
        """
        url = f"{self.base_url}{path}"
        try:
            response = await self.page.context.request.get(url)
            if not response.ok:
                logger.warning(f"API {path} returned {response.status}")
                return []
            return await response.json()
        except Exception as e:
            logger.warning(f"API request failed for {path}: {e}")
            return []

    # ------------------------------------------------------------------ #
    #  Courses                                                             #
    # ------------------------------------------------------------------ #

    async def get_courses(self) -> list:
        """Return all active enrolled courses from the Canvas dashboard."""
        # Canvas dashboard cards are at the root URL, not /courses.
        # /courses shows an enrollment list without the ic-DashboardCard elements.
        await self._goto(f"{self.base_url}/")
        await self.page.wait_for_timeout(2000)  # cards load via JS after networkidle
        await self._save_page_snapshot("00_dashboard")

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

        if not courses:
            logger.warning("No dashboard cards found — falling back to Canvas REST API")
            courses = await self._get_courses_via_api()

        return courses

    async def _get_courses_via_api(self) -> list:
        """Fallback: fetch enrolled courses via Canvas REST API using session cookies."""
        url = (
            f"{self.base_url}/api/v1/courses"
            "?enrollment_type=student&enrollment_state=active&per_page=100"
        )
        try:
            response = await self.page.context.request.get(url)
            if not response.ok:
                logger.error(f"Canvas API returned {response.status} for courses")
                return []
            data = await response.json()
            courses = []
            for c in data:
                if not isinstance(c, dict) or "id" not in c:
                    continue
                courses.append({
                    "id": str(c["id"]),
                    "name": c.get("name", "Unknown").strip(),
                    "code": c.get("course_code", "").strip(),
                    "url": c.get("html_url", f"{self.base_url}/courses/{c['id']}"),
                })
                logger.info(f"[COURSE-API] {c.get('name')} (id={c['id']})")
            return courses
        except Exception as e:
            logger.error(f"Canvas REST API course fetch failed: {e}", exc_info=True)
            return []

    # ------------------------------------------------------------------ #
    #  Assignments                                                         #
    # ------------------------------------------------------------------ #

    async def get_assignments(self, course_id: str) -> list:
        """Return all assignments for a course via REST API (includes description inline)."""
        data = await self._api_get(
            f"/api/v1/courses/{course_id}/assignments"
            "?per_page=100&order_by=due_at&include[]=submission&include[]=rubric_assessment"
        )
        assignments = []
        for a in (data if isinstance(data, list) else []):
            sub = a.get("submission") or {}
            rubric = []
            for crit in (a.get("rubric") or []):
                rubric.append({
                    "criterion": crit.get("description", ""),
                    "description": crit.get("long_description", ""),
                    "points": str(crit.get("points", "")),
                })
            assignments.append({
                "id": str(a["id"]),
                "title": a.get("name", "").strip(),
                "url": a.get("html_url", f"{self.base_url}/courses/{course_id}/assignments/{a['id']}"),
                "due": a.get("due_at") or "No due date",
                "points": str(a.get("points_possible") or ""),
                "submission_types": a.get("submission_types", []),
                "description": extract_html_text(a.get("description") or ""),
                "submitted_at": sub.get("submitted_at") or "",
                "workflow_state": sub.get("workflow_state") or "",
                "rubric": rubric,
                "details": {},
            })
            logger.debug(f"  [ASSIGN] {a.get('name')} due={a.get('due_at')}")
        return assignments

    async def get_assignment_details(self, assignment_url: str) -> dict:
        """
        Return description, submission types, points, and rubric for one assignment
        by fetching the assignment via REST API.  Falls back to browser scraping
        if the assignment ID cannot be parsed from the URL.
        """
        # Extract course_id and assignment_id from URL like /courses/123/assignments/456
        m = re.search(r"/courses/(\d+)/assignments/(\d+)", assignment_url)
        if m:
            course_id, assignment_id = m.group(1), m.group(2)
            data = await self._api_get(
                f"/api/v1/courses/{course_id}/assignments/{assignment_id}"
            )
            if isinstance(data, dict) and "id" in data:
                rubric = [
                    {
                        "criterion": c.get("description", ""),
                        "description": c.get("long_description", ""),
                        "points": str(c.get("points", "")),
                    }
                    for c in (data.get("rubric") or [])
                ]
                return {
                    "description": extract_html_text(data.get("description") or ""),
                    "submission_types": data.get("submission_types", []),
                    "points_possible": str(data.get("points_possible") or ""),
                    "rubric": rubric,
                }

        # Browser fallback for unusual URL shapes
        await self._goto(assignment_url)
        try:
            await self.page.wait_for_selector("#assignment_description, .assignment-description", timeout=8000)
        except Exception:
            await self.page.wait_for_timeout(2000)

        details: dict = {"description": "", "submission_types": [], "points_possible": "", "rubric": []}
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
        """Return all announcements for a course via REST API."""
        data = await self._api_get(
            f"/api/v1/courses/{course_id}/discussion_topics"
            "?only_announcements=true&per_page=50&order_by=posted_at&scope=unlocked"
        )
        announcements = []
        for a in (data if isinstance(data, list) else []):
            announcements.append({
                "title": a.get("title", ""),
                "date": a.get("posted_at", ""),
                "message": extract_html_text(a.get("message") or ""),
            })
        return announcements

    # ------------------------------------------------------------------ #
    #  Modules                                                             #
    # ------------------------------------------------------------------ #

    async def get_modules(self, course_id: str) -> list:
        """
        Return all modules and their items via REST API.
        The API returns real item URLs — not the {{ id }} Handlebars
        placeholders that the browser DOM exposes before React renders.
        """
        data = await self._api_get(
            f"/api/v1/courses/{course_id}/modules?include[]=items&per_page=100"
        )
        modules = []
        for mod in (data if isinstance(data, list) else []):
            items = []
            for item in mod.get("items", []):
                url = item.get("html_url") or item.get("url") or ""
                items.append({
                    "title": item.get("title", ""),
                    "url": url,
                    "type": item.get("type", ""),
                    "content_id": str(item.get("content_id") or ""),
                })
            modules.append({
                "name": mod.get("name", "Unnamed Module"),
                "items": items,
            })
        return modules

    # ------------------------------------------------------------------ #
    #  Grades                                                              #
    # ------------------------------------------------------------------ #

    async def get_grades(self, course_id: str) -> list:
        """Return grades for all assignments via REST API (submission data included)."""
        data = await self._api_get(
            f"/api/v1/courses/{course_id}/assignments"
            "?per_page=100&include[]=submission"
        )
        grades = []
        for a in (data if isinstance(data, list) else []):
            sub = a.get("submission") or {}
            grades.append({
                "assignment": a.get("name", ""),
                "score": str(sub.get("score") if sub.get("score") is not None else "-"),
                "possible": str(a.get("points_possible") or "-"),
                "submitted_at": sub.get("submitted_at") or "",
                "workflow_state": sub.get("workflow_state") or "unsubmitted",
            })
        return grades

    # ------------------------------------------------------------------ #
    #  Syllabus                                                            #
    # ------------------------------------------------------------------ #

    async def get_syllabus(self, course_id: str) -> str:
        """Return the plain-text syllabus for a course via REST API."""
        data = await self._api_get(
            f"/api/v1/courses/{course_id}?include[]=syllabus_body"
        )
        if isinstance(data, dict):
            return extract_html_text(data.get("syllabus_body") or "")
        return ""

    # ------------------------------------------------------------------ #
    #  Discussions                                                         #
    # ------------------------------------------------------------------ #

    async def get_discussions(self, course_id: str) -> list:
        """Return all discussion topics for a course via REST API."""
        data = await self._api_get(
            f"/api/v1/courses/{course_id}/discussion_topics"
            "?per_page=50&order_by=recent_activity&scope=unlocked"
        )
        discussions = []
        for d in (data if isinstance(data, list) else []):
            discussions.append({
                "title": d.get("title", ""),
                "posted_at": d.get("posted_at", ""),
                "message": extract_html_text(d.get("message") or ""),
                "url": d.get("html_url", ""),
            })
        return discussions

    # ------------------------------------------------------------------ #
    #  Page snapshot saving                                               #
    # ------------------------------------------------------------------ #

    async def _save_page_snapshot(self, name: str) -> None:
        """
        Save the current page's full HTML to the debug snapshots directory.
        These snapshots let us inspect exactly what Canvas is rendering so
        we can verify and fix CSS selectors without guessing.

        Files are saved to DATA_DIR/debug_snapshots/<name>.html
        and are overwritten on each crawl so they always reflect the latest.
        """
        try:
            data_dir = os.environ.get("DATA_DIR", "data")
            snap_dir = Path(data_dir) / "debug_snapshots"
            snap_dir.mkdir(parents=True, exist_ok=True)

            # Sanitize name for use as a filename
            safe_name = re.sub(r"[^\w\-]", "_", name)[:80]
            dest = snap_dir / f"{safe_name}.html"

            html = await self.page.content()
            dest.write_text(html, encoding="utf-8", errors="replace")
            logger.debug(f"Snapshot saved: {dest}")
        except Exception as e:
            logger.warning(f"Could not save snapshot for {name}: {e}")

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
            slug = re.sub(r"[^\w]", "_", course["name"])[:30]

            # Skip admin/orientation courses — no real assignments
            if any(f.lower() in course["name"].lower() for f in _SKIP_COURSE_FRAGMENTS):
                logger.info(f"[SKIP] {course['name']} — non-academic course")
                continue

            logger.info(f"[CRAWL] {course['name']}")

            # All structured data fetched via REST API — no browser navigation needed
            course["syllabus"]      = await self.get_syllabus(cid)
            course["announcements"] = await self.get_announcements(cid)
            course["modules"]       = await self.get_modules(cid)
            course["grades"]        = await self.get_grades(cid)
            course["discussions"]   = await self.get_discussions(cid)

            assignments = await self.get_assignments(cid)
            logger.info(f"  Found {len(assignments)} assignments")
            for a in assignments:
                logger.info(f"  -> {a['title']} (due={a['due']})")
            course["assignments"] = assignments

            # Document ingester: pass pre-fetched modules so it can process
            # module item URLs without re-navigating to the modules page.
            logger.info(f"  Ingesting documents for: {course['name']}")
            ingester = DocumentIngester(
                page=self.page,
                base_url=self.base_url,
                goto_fn=_polite_goto,
            )
            doc_results = await ingester.ingest_course_documents(
                cid, course["name"], modules=course["modules"]
            )
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
