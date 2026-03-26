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

from agent.document_ingester import DocumentIngester

logger = logging.getLogger(__name__)

CANVAS_URL = os.environ.get("CANVAS_URL", "https://wilmu.instructure.com").rstrip("/")

_MIN_DELAY = 0.8
_MAX_DELAY = 1.8

# Course name fragments to skip — admin/orientation courses with no real assignments
_SKIP_COURSE_FRAGMENTS = ["BrushUp", "Student Assistance", "Orientation", "Tutorial"]

# A real enrolled course always has a 5-digit CRN in its name or code,
# e.g. "Appl Concepts in Acct and Fin (20345.B2)" or "FIN300.20345.B2.Online"
_CRN_RE = re.compile(r"\b\d{5}\b")


def _is_real_course(course: dict) -> bool:
    """Return True only for genuine enrolled courses that carry a CRN."""
    name = course.get("name", "")
    code = course.get("code", "")
    if any(f.lower() in name.lower() for f in _SKIP_COURSE_FRAGMENTS):
        return False
    return bool(_CRN_RE.search(name) or _CRN_RE.search(code))


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
        logger.debug("[start] Launching Playwright")
        self._pw = await async_playwright().start()

        # If PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH is set AND the path actually exists,
        # use it directly (e.g. a system Chromium). If the path doesn't exist — which
        # happens when migrating from Nixpacks to Docker — fall back to Playwright's
        # own baked-in Chromium so the stale env var doesn't crash the crawler.
        _raw_exec = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", "").strip()
        if _raw_exec and not Path(_raw_exec).exists():
            logger.warning(
                f"PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH={_raw_exec!r} does not exist — "
                "falling back to Playwright's built-in Chromium. "
                "Remove this env var from Railway Variables to silence this warning."
            )
            _raw_exec = ""
        executable_path = _raw_exec or None
        logger.debug(f"[start] headless={headless} executable_path={executable_path!r}")

        browser = await self._pw.chromium.launch(
            headless=headless,
            executable_path=executable_path,
        )
        logger.debug("[start] Chromium launched — creating browser context")
        self.context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        cookies = await self.load_cookies()
        logger.debug(f"[start] Injecting {len(cookies)} cookies into browser context")
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
        logger.debug(f"[verify_session] Navigating to {self.base_url}/profile")
        await self._goto(f"{self.base_url}/profile")
        logger.debug(f"[verify_session] Landed on: {self.page.url}")
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
        logger.debug(f"[get_courses] Navigating to dashboard: {self.base_url}/")
        await self._goto(f"{self.base_url}/")
        await self.page.wait_for_timeout(2000)  # cards load via JS after networkidle
        await self._save_page_snapshot("00_dashboard")

        cards = await self.page.query_selector_all(".ic-DashboardCard")
        logger.debug(f"[get_courses] Found {len(cards)} dashboard cards — parsing each")

        courses = []
        for card in cards:
            try:
                link = await card.query_selector("a.ic-DashboardCard__link")
                if not link:
                    logger.debug("[get_courses] Card has no .ic-DashboardCard__link — skipping")
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

        logger.debug(f"[get_courses] Returning {len(courses)} courses")
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
        """Return all assignments for a course."""
        logger.debug(f"[get_assignments] course_id={course_id} — navigating to assignments page")
        await self._goto(f"{self.base_url}/courses/{course_id}/assignments")
        # Wait briefly for React to render — primary selector often doesn't appear
        # so we proceed quickly to the link fallback which always works
        await self.page.wait_for_timeout(1500)

        assignments = []

        # Try primary Canvas selectors (.assignment-group > .assignment)
        groups = await self.page.query_selector_all(".assignment-group")
        logger.debug(f"[get_assignments] course_id={course_id} — found {len(groups)} .assignment-group elements")
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

        # Fallback: scan all assignment links directly — always works regardless of CSS class
        if not assignments:
            logger.info(f"Using link fallback for course {course_id}")
            links = await self.page.query_selector_all("a[href*='/assignments/']")
            logger.debug(f"[get_assignments] Link fallback found {len(links)} assignment links")
            seen = set()
            for link in links:
                try:
                    href = await link.get_attribute("href") or ""
                    if "/assignments/" not in href or href in seen:
                        continue
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

        logger.debug(f"[get_assignments] course_id={course_id} — returning {len(assignments)} assignments")
        return assignments

    async def get_assignment_details(self, assignment_url: str) -> dict:
        """Return description, submission types, points, and rubric for one assignment."""
        logger.debug(f"[get_assignment_details] Navigating to {assignment_url}")
        await self._goto(assignment_url)
        try:
            await self.page.wait_for_selector("#assignment_description, .assignment-description", timeout=8000)
        except Exception:
            logger.debug(f"[get_assignment_details] Selector timeout — falling back to 2s wait for {assignment_url}")
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
                logger.debug(f"[get_assignment_details] Description: {len(details['description'])} chars")
            else:
                logger.debug(f"[get_assignment_details] No #assignment_description found at {assignment_url}")

            sub_els = await self.page.query_selector_all(".submission_type")
            details["submission_types"] = [await el.inner_text() for el in sub_els]
            logger.debug(f"[get_assignment_details] submission_types={details['submission_types']}")

            pts_el = await self.page.query_selector(".points_possible")
            if pts_el:
                details["points_possible"] = await pts_el.inner_text()
                logger.debug(f"[get_assignment_details] points_possible={details['points_possible']!r}")

            rubric_rows = await self.page.query_selector_all(".rubric_criterion")
            logger.debug(f"[get_assignment_details] Found {len(rubric_rows)} rubric criteria rows")
            for row in rubric_rows:
                desc = await row.query_selector(".description")
                long_desc = await row.query_selector(".long_description")
                pts = await row.query_selector(".criterion_points")
                criterion = await desc.inner_text() if desc else ""
                details["rubric"].append({
                    "criterion": criterion,
                    "description": await long_desc.inner_text() if long_desc else "",
                    "points": await pts.inner_text() if pts else "",
                })
                logger.debug(f"[get_assignment_details] Rubric criterion: {criterion!r}")

        except Exception as e:
            logger.warning(f"Error parsing details at {assignment_url}: {e}", exc_info=True)

        # Vision: capture rubric tables, diagrams, and other visual content
        from agent.brain import describe_page_visuals
        try:
            screenshot = await self.page.screenshot(full_page=True)
            vision_text = await describe_page_visuals(screenshot, assignment_url)
            if vision_text:
                details["description"] = (
                    (details["description"] + "\n\n" if details["description"] else "")
                    + f"[VISUAL CONTENT]\n{vision_text}"
                )
        except Exception as e:
            logger.warning(f"Vision failed for assignment {assignment_url}: {e}")

        return details

    # ------------------------------------------------------------------ #
    #  Announcements                                                       #
    # ------------------------------------------------------------------ #

    async def get_announcements(self, course_id: str) -> list:
        """Return all announcements for a course."""
        logger.debug(f"[get_announcements] course_id={course_id}")
        await self._goto(f"{self.base_url}/courses/{course_id}/announcements")
        try:
            await self.page.wait_for_selector(".ic-announcement-row, .discussion-topic", timeout=8000)
        except Exception:
            logger.debug(f"[get_announcements] Selector timeout — falling back to 2s wait")
            await self.page.wait_for_timeout(2000)

        rows = await self.page.query_selector_all(".ic-announcement-row")
        logger.debug(f"[get_announcements] Found {len(rows)} .ic-announcement-row elements")
        announcements = []
        for item in rows:
            try:
                # Actual Canvas HTML uses a.ic-item-row__content-link for the title —
                # .ic-announcement-row__content-title does not exist in the rendered DOM.
                title_el = await item.query_selector("a.ic-item-row__content-link")
                date_el = await item.query_selector("time")
                title = (await title_el.inner_text()).strip() if title_el else ""
                if title:
                    date_val = await date_el.get_attribute("datetime") if date_el else ""
                    logger.debug(f"[get_announcements] Announcement: {title!r} date={date_val!r}")
                    announcements.append({"title": title, "date": date_val})
            except Exception as e:
                logger.warning(f"Error parsing announcement: {e}", exc_info=True)

        logger.debug(f"[get_announcements] Returning {len(announcements)} announcements for course {course_id}")
        return announcements

    # ------------------------------------------------------------------ #
    #  Modules                                                             #
    # ------------------------------------------------------------------ #

    async def get_modules(self, course_id: str) -> list:
        """Return all modules and their items for a course."""
        logger.debug(f"[get_modules] course_id={course_id}")
        await self._goto(f"{self.base_url}/courses/{course_id}/modules")
        try:
            await self.page.wait_for_selector(".context_module", timeout=8000)
        except Exception:
            logger.debug(f"[get_modules] Selector timeout — falling back to 2s wait")
            await self.page.wait_for_timeout(2000)
        # Extra settle time for Canvas's React to finish populating item hrefs.
        # Without this, ExternalTool items have {{ id }} placeholder hrefs.
        await self.page.wait_for_timeout(3000)

        mod_els = await self.page.query_selector_all(".context_module")
        logger.debug(f"[get_modules] Found {len(mod_els)} .context_module elements")
        modules = []
        for mod in mod_els:
            try:
                name_el = await mod.query_selector(".ig-header-title")
                name = (await name_el.inner_text()).strip() if name_el else "Unnamed Module"

                items = []
                for item in await mod.query_selector_all(".context_module_item"):
                    link = await item.query_selector("a.title")
                    if link:
                        item_href = await link.get_attribute("href") or ""
                        # Skip Handlebars template placeholders that React hasn't
                        # rendered yet — these look like /external_tools/1?...={{ id }}
                        if "{{" in item_href or "}}" in item_href:
                            logger.debug(f"Skipping unrendered module item href: {item_href!r}")
                            continue
                        items.append({
                            "title": (await link.inner_text()).strip(),
                            "url": f"{self.base_url}{item_href}" if item_href.startswith("/") else item_href,
                        })

                logger.debug(f"[get_modules] Module: {name!r} — {len(items)} items")
                modules.append({"name": name, "items": items})

            except Exception as e:
                logger.warning(f"Error parsing module: {e}", exc_info=True)

        logger.debug(f"[get_modules] Returning {len(modules)} modules for course {course_id}")
        return modules

    # ------------------------------------------------------------------ #
    #  Grades                                                              #
    # ------------------------------------------------------------------ #

    async def get_grades(self, course_id: str) -> list:
        """Return all graded assignments for a course."""
        logger.debug(f"[get_grades] course_id={course_id}")
        await self._goto(f"{self.base_url}/courses/{course_id}/grades")
        try:
            await self.page.wait_for_selector("tr.student_assignment, .gradebook-cell", timeout=8000)
        except Exception:
            logger.debug(f"[get_grades] Selector timeout — falling back to 2s wait")
            await self.page.wait_for_timeout(2000)

        grade_rows = await self.page.query_selector_all("tr.student_assignment")
        logger.debug(f"[get_grades] Found {len(grade_rows)} tr.student_assignment rows")
        grades = []
        for row in grade_rows:
            try:
                title_el = await row.query_selector(".title a")
                # Actual Canvas HTML: score is inside td.assignment_score > span.grade;
                # points possible is the following span (text like "/ 100");
                # due date is td.due.  (.score and .possible do not exist.)
                score_el = await row.query_selector(".assignment_score .grade")
                possible_el = await row.query_selector(".assignment_score span:nth-child(2)")
                due_el = await row.query_selector("td.due")

                title = (await title_el.inner_text()).strip() if title_el else ""
                if title:
                    score_val = (await score_el.inner_text()).strip() if score_el else "-"
                    possible_val = (await possible_el.inner_text()).strip().lstrip("/ ") if possible_el else "-"
                    due_val = (await due_el.inner_text()).strip() if due_el else ""
                    logger.debug(f"[get_grades] Grade: {title!r} score={score_val!r}/{possible_val!r} due={due_val!r}")
                    grades.append({
                        "assignment": title,
                        "score": score_val,
                        "possible": possible_val,
                        "due": due_val,
                    })
            except Exception as e:
                logger.warning(f"Error parsing grade row: {e}", exc_info=True)

        logger.debug(f"[get_grades] Returning {len(grades)} grade records for course {course_id}")
        return grades

    # ------------------------------------------------------------------ #
    #  Syllabus                                                            #
    # ------------------------------------------------------------------ #

    async def get_syllabus(self, course_id: str) -> str:
        """Return the plain-text syllabus for a course."""
        logger.debug(f"[get_syllabus] course_id={course_id}")
        await self._goto(f"{self.base_url}/courses/{course_id}/assignments/syllabus")
        await self.page.wait_for_timeout(500)
        el = await self.page.query_selector("#course_syllabus")
        if el:
            text = await el.inner_text()
            logger.debug(f"[get_syllabus] Found syllabus: {len(text)} chars for course {course_id}")
            return text
        logger.debug(f"[get_syllabus] No #course_syllabus element found for course {course_id}")
        return ""

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

        logger.debug("[crawl_all] Fetching course list from dashboard")
        courses = await self.get_courses()
        logger.debug(f"[crawl_all] {len(courses)} total courses found on dashboard")

        enrolled = [c for c in courses if _is_real_course(c)]
        skipped = [c for c in courses if not _is_real_course(c)]
        logger.debug(f"[crawl_all] {len(enrolled)} enrolled courses (have CRN), {len(skipped)} will be skipped")

        for course in courses:
            cid = course["id"]
            slug = re.sub(r"[^\w]", "_", course["name"])[:30]

            # Only crawl real enrolled courses — must contain a 5-digit CRN
            if not _is_real_course(course):
                logger.info(f"[SKIP] {course['name']} — no CRN, not an enrolled course")
                continue

            logger.info(f"[CRAWL] {course['name']}")
            logger.debug(f"[crawl_all] --- Starting full crawl of course id={cid} ---")

            logger.debug(f"[crawl_all] Fetching syllabus for {course['name']}")
            raw_syllabus = await self.get_syllabus(cid)
            await self._save_page_snapshot(f"{slug}_{cid}_syllabus")
            if raw_syllabus:
                logger.debug(f"[crawl_all] Syllabus found ({len(raw_syllabus)} chars) — enriching with AI")
                from agent.brain import enrich_for_knowledge_base
                course["syllabus"] = await enrich_for_knowledge_base(
                    raw_syllabus, "Syllabus", course["name"], "syllabus"
                )
                logger.debug(f"[crawl_all] Syllabus enriched: {len(course['syllabus'])} chars")
            else:
                logger.debug(f"[crawl_all] No syllabus found for {course['name']}")
                course["syllabus"] = raw_syllabus

            logger.debug(f"[crawl_all] Fetching announcements for {course['name']}")
            course["announcements"] = await self.get_announcements(cid)
            await self._save_page_snapshot(f"{slug}_{cid}_announcements")
            logger.debug(f"[crawl_all] Got {len(course['announcements'])} announcements")

            logger.debug(f"[crawl_all] Fetching modules for {course['name']}")
            course["modules"] = await self.get_modules(cid)
            await self._save_page_snapshot(f"{slug}_{cid}_modules")
            logger.debug(f"[crawl_all] Got {len(course['modules'])} modules")

            logger.debug(f"[crawl_all] Fetching grades for {course['name']}")
            course["grades"] = await self.get_grades(cid)
            await self._save_page_snapshot(f"{slug}_{cid}_grades")
            logger.debug(f"[crawl_all] Got {len(course['grades'])} grade records")

            logger.debug(f"[crawl_all] Fetching assignments for {course['name']}")
            assignments = await self.get_assignments(cid)
            await self._save_page_snapshot(f"{slug}_{cid}_assignments")
            logger.info(f"  Found {len(assignments)} assignments")

            # Skip per-assignment detail pages during bulk crawl — they each cost
            # a full page navigation + delay. Details are fetched on-demand when
            # the user generates a specific assignment.
            for a in assignments:
                logger.info(f"  -> {a['title']}")
                logger.debug(f"[crawl_all]    id={a['id']} due={a.get('due')!r} points={a.get('points')!r}")
                a["details"] = {}  # populated on-demand via get_assignment_details()

            course["assignments"] = assignments

            # RF-Ingester-bypass: pass _polite_goto so the ingester uses
            # the same rate-limiting behaviour as the crawler.
            logger.info(f"  Ingesting documents for: {course['name']}")
            logger.debug(f"[crawl_all] Creating DocumentIngester for {course['name']}")
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
            logger.debug(f"[crawl_all] --- Finished course: {course['name']} ---")

            self.knowledge["courses"].append(course)

        logger.info(f"[DONE] Crawl complete -- {len(courses)} courses indexed")  # RF-24
        logger.debug(f"[crawl_all] Knowledge snapshot: {len(self.knowledge['courses'])} courses stored")
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
