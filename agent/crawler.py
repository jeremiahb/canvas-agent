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

_MIN_DELAY = 0.4
_MAX_DELAY = 0.9

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
        self.kb = None  # Injected by api/main.py before crawl_all() for normalization pass
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

        # If PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH is set (e.g. pointing to the
        # Nix-installed chromium on Railway), use it directly so we don't depend
        # on Playwright's own downloaded browser binary which may be missing libs.
        executable_path = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH") or None
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

    async def get_dashboard_signals(self) -> dict:
        """
        Capture urgency signals from the Canvas dashboard: To-Do items,
        Recent Activity feed, and instructor feedback notices.

        Called once before the per-course crawl loop so that cross-course
        urgency context is available during normalization.
        """
        logger.debug("[get_dashboard_signals] Navigating to dashboard")
        await self._goto(f"{self.base_url}/")
        await self.page.wait_for_timeout(2500)  # JS-heavy dashboard needs extra time

        signals: dict = {"todo_items": [], "recent_activity": [], "feedback_items": []}

        # ------------------------------------------------------------------
        # To-Do items (right sidebar)
        # ------------------------------------------------------------------
        try:
            todo_els = await self.page.query_selector_all(
                "#right-side .to-do-list .to-do-item, "
                ".todo-list-item, "
                "[data-testid='todo-item']"
            )
            for el in todo_els:
                try:
                    title_el = await el.query_selector("a, .item-details-header")
                    title = (await title_el.inner_text()).strip() if title_el else ""
                    if not title:
                        continue
                    href = await title_el.get_attribute("href") if title_el else ""
                    url = f"{self.base_url}{href}" if href and href.startswith("/") else href or ""
                    date_el = await el.query_selector("time, .date-available, .todo-date")
                    due_text = (await date_el.inner_text()).strip() if date_el else ""
                    course_el = await el.query_selector(".context-name, .todo-course")
                    course_name = (await course_el.inner_text()).strip() if course_el else ""
                    signals["todo_items"].append({
                        "title": title,
                        "url": url,
                        "due_date": due_text,
                        "course_name": course_name,
                    })
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"[get_dashboard_signals] To-do parse failed: {e}")

        # ------------------------------------------------------------------
        # Recent Activity stream
        # ------------------------------------------------------------------
        try:
            activity_els = await self.page.query_selector_all(
                "#dashboard_activity_stream .stream-item, "
                ".activity-stream .stream-item, "
                "[data-testid='activity-item']"
            )
            for el in activity_els:
                try:
                    title_el = await el.query_selector("a, h3, .title")
                    title = (await title_el.inner_text()).strip() if title_el else ""
                    if not title:
                        continue
                    href = await title_el.get_attribute("href") if title_el else ""
                    url = f"{self.base_url}{href}" if href and href.startswith("/") else href or ""
                    time_el = await el.query_selector("time")
                    posted = (await time_el.get_attribute("datetime") or "").strip() if time_el else ""
                    signals["recent_activity"].append({
                        "title": title,
                        "url": url,
                        "posted_at": posted,
                    })
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"[get_dashboard_signals] Recent activity parse failed: {e}")

        # ------------------------------------------------------------------
        # Instructor feedback notices (sidebar / cards)
        # ------------------------------------------------------------------
        try:
            feedback_els = await self.page.query_selector_all(
                ".recent-feedback, .grade-summary, [data-testid='recent-feedback']"
            )
            for el in feedback_els:
                try:
                    text = (await el.inner_text()).strip()
                    if text:
                        signals["feedback_items"].append({"text": text[:500]})
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"[get_dashboard_signals] Feedback parse failed: {e}")

        logger.info(
            f"[get_dashboard_signals] todo={len(signals['todo_items'])} "
            f"activity={len(signals['recent_activity'])} "
            f"feedback={len(signals['feedback_items'])}"
        )
        return signals

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

    async def get_assignment_details(self, assignment_url: str, page=None) -> dict:
        """Return description, submission types, points, and rubric for one assignment.

        Pass an explicit ``page`` to run concurrently on a separate Playwright page;
        omit to use ``self.page`` (the default, single-page behaviour).
        """
        _page = page or self.page
        logger.debug(f"[get_assignment_details] Navigating to {assignment_url}")
        await _polite_goto(_page, assignment_url)
        try:
            await _page.wait_for_selector("#assignment_description, .assignment-description", timeout=8000)
        except Exception:
            logger.debug(f"[get_assignment_details] Selector timeout — falling back to 2s wait for {assignment_url}")
            await _page.wait_for_timeout(2000)

        details: dict = {
            "description": "",
            "submission_types": [],
            "points_possible": "",
            "rubric": [],
        }

        try:
            desc_el = await _page.query_selector("#assignment_description")
            if desc_el:
                details["description"] = await desc_el.inner_text()
                logger.debug(f"[get_assignment_details] Description: {len(details['description'])} chars")
            else:
                logger.debug(f"[get_assignment_details] No #assignment_description found at {assignment_url}")

            sub_els = await _page.query_selector_all(".submission_type")
            details["submission_types"] = [await el.inner_text() for el in sub_els]
            logger.debug(f"[get_assignment_details] submission_types={details['submission_types']}")

            pts_el = await _page.query_selector(".points_possible")
            if pts_el:
                details["points_possible"] = await pts_el.inner_text()
                logger.debug(f"[get_assignment_details] points_possible={details['points_possible']!r}")

            rubric_rows = await _page.query_selector_all(".rubric_criterion")
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
            screenshot = await _page.screenshot(full_page=True)
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
        for mod_idx, mod in enumerate(mod_els):
            try:
                name_el = await mod.query_selector(".ig-header-title")
                name = (await name_el.inner_text()).strip() if name_el else "Unnamed Module"

                # Week label: header subtitle or date range visible next to module title
                week_label_el = await mod.query_selector(".ig-header-subtitle, .module-unlock-at")
                week_label = (await week_label_el.inner_text()).strip() if week_label_el else ""

                # Lock state: locked modules show a lock icon or locked CSS class
                lock_el = await mod.query_selector(".locked_icon, .icon-lock, [data-locked='true']")
                is_locked = lock_el is not None

                items = []
                for item_idx, item in enumerate(await mod.query_selector_all(".context_module_item")):
                    link = await item.query_selector("a.title")
                    if link:
                        item_href = await link.get_attribute("href") or ""
                        # Skip Handlebars template placeholders that React hasn't
                        # rendered yet — these look like /external_tools/1?...={{ id }}
                        if "{{" in item_href or "}}" in item_href:
                            logger.debug(f"Skipping unrendered module item href: {item_href!r}")
                            continue

                        # Completion state from row CSS classes
                        item_classes = await item.get_attribute("class") or ""
                        if "completed" in item_classes:
                            completion_state = "completed"
                        elif "overdue" in item_classes:
                            completion_state = "overdue"
                        else:
                            completion_state = "not_started"

                        # Item type from icon class
                        icon_el = await item.query_selector("i[class*='icon-'], .item-icon i")
                        item_type = "unknown"
                        if icon_el:
                            icon_class = await icon_el.get_attribute("class") or ""
                            if "assignment" in icon_class:
                                item_type = "assignment"
                            elif "quiz" in icon_class:
                                item_type = "quiz"
                            elif "discussion" in icon_class:
                                item_type = "discussion"
                            elif "page" in icon_class or "document" in icon_class:
                                item_type = "page"
                            elif "file" in icon_class:
                                item_type = "file"
                            elif "external" in icon_class or "link" in icon_class:
                                item_type = "external_url"

                        items.append({
                            "title": (await link.inner_text()).strip(),
                            "url": f"{self.base_url}{item_href}" if item_href.startswith("/") else item_href,
                            "item_type": item_type,
                            "completion_state": completion_state,
                            "item_order": item_idx,
                        })

                logger.debug(f"[get_modules] Module: {name!r} — {len(items)} items locked={is_locked}")
                modules.append({
                    "name": name,
                    "title": name,
                    "items": items,
                    "position": mod_idx,
                    "week_label": week_label,
                    "is_locked": is_locked,
                })

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
    #  Dashboard discovery                                                #
    # ------------------------------------------------------------------ #

    async def crawl_dashboard(self) -> dict:
        """
        Crawl the Canvas dashboard for urgency signals:
        - To Do items (due soon)
        - Recent activity
        - Course cards and visible tabs
        - Recent feedback
        Returns dict with keys: todo_items, recent_activity, course_cards, recent_feedback
        """
        logger.info("[crawl_dashboard] Navigating to Canvas dashboard")
        await self._goto(f"{self.base_url}")
        await self.page.wait_for_timeout(2000)

        result: dict = {
            "todo_items": [],
            "recent_activity": [],
            "course_cards": [],
            "recent_feedback": [],
        }

        # --- To Do items ---
        try:
            todo_els = await self.page.query_selector_all(
                ".todo-list-header + ul li, .todo-item, .ic-Dashboard__activity"
            )
            logger.debug(f"[crawl_dashboard] Found {len(todo_els)} todo elements")
            for el in todo_els:
                try:
                    title_el = await el.query_selector("a, .title, .todo-title")
                    due_el = await el.query_selector(".date-available, .due-date, time")
                    course_el = await el.query_selector(".context-name, .course-title, .todo-course")
                    type_el = await el.query_selector(".type, .todo-type, .badge")

                    title = (await title_el.inner_text()).strip() if title_el else ""
                    if not title:
                        continue
                    due = (await due_el.inner_text()).strip() if due_el else ""
                    course_name = (await course_el.inner_text()).strip() if course_el else ""
                    item_type = (await type_el.inner_text()).strip() if type_el else ""
                    result["todo_items"].append({
                        "title": title,
                        "due_date": due,
                        "course": course_name,
                        "type": item_type,
                    })
                    logger.debug(f"[crawl_dashboard] Todo: {title!r} due={due!r}")
                except Exception as e:
                    logger.warning(f"Error parsing todo item: {e}")
        except Exception as e:
            logger.warning(f"[crawl_dashboard] Todo extraction failed: {e}")

        # --- Recent Activity ---
        try:
            activity_els = await self.page.query_selector_all(
                ".stream-activity .stream-item, .activity-feed .stream-item"
            )
            logger.debug(f"[crawl_dashboard] Found {len(activity_els)} activity items")
            for el in activity_els:
                try:
                    title_el = await el.query_selector("a, .title, h3, h4")
                    date_el = await el.query_selector("time, .date, .updated-at")
                    course_el = await el.query_selector(".context-name, .course-title")
                    summary_el = await el.query_selector(".summary, .preview, p")

                    title = (await title_el.inner_text()).strip() if title_el else ""
                    if not title:
                        continue
                    date = (await date_el.get_attribute("datetime") or await date_el.inner_text()).strip() if date_el else ""
                    course_name = (await course_el.inner_text()).strip() if course_el else ""
                    summary = (await summary_el.inner_text()).strip() if summary_el else ""
                    result["recent_activity"].append({
                        "title": title,
                        "date": date,
                        "course": course_name,
                        "summary": summary,
                    })
                    logger.debug(f"[crawl_dashboard] Activity: {title!r}")
                except Exception as e:
                    logger.warning(f"Error parsing activity item: {e}")
        except Exception as e:
            logger.warning(f"[crawl_dashboard] Activity extraction failed: {e}")

        # --- Course cards ---
        try:
            card_els = await self.page.query_selector_all(".ic-DashboardCard")
            logger.debug(f"[crawl_dashboard] Found {len(card_els)} dashboard cards")
            for card in card_els:
                try:
                    name_el = await card.query_selector(".ic-DashboardCard__header-title")
                    link_el = await card.query_selector("a.ic-DashboardCard__link")
                    course_name = (await name_el.inner_text()).strip() if name_el else ""
                    course_link = await link_el.get_attribute("href") if link_el else ""
                    if course_link and course_link.startswith("/"):
                        course_link = f"{self.base_url}{course_link}"

                    tab_els = await card.query_selector_all(".ic-DashboardCard__action-container a")
                    tabs = []
                    for tab in tab_els:
                        tab_href = await tab.get_attribute("href") or ""
                        tab_text = (await tab.inner_text()).strip()
                        if tab_href and tab_text:
                            tabs.append({"label": tab_text, "url": f"{self.base_url}{tab_href}" if tab_href.startswith("/") else tab_href})

                    result["course_cards"].append({
                        "name": course_name,
                        "url": course_link,
                        "tabs": tabs,
                    })
                    logger.debug(f"[crawl_dashboard] Card: {course_name!r} tabs={len(tabs)}")
                except Exception as e:
                    logger.warning(f"Error parsing course card: {e}")
        except Exception as e:
            logger.warning(f"[crawl_dashboard] Course card extraction failed: {e}")

        # --- Recent Feedback ---
        try:
            feedback_els = await self.page.query_selector_all(".recent-feedback, .submission-feedback")
            logger.debug(f"[crawl_dashboard] Found {len(feedback_els)} feedback items")
            for el in feedback_els:
                try:
                    title_el = await el.query_selector("a, .title, .assignment-title")
                    grade_el = await el.query_selector(".grade, .score")
                    comment_el = await el.query_selector(".comment, .feedback-comment, p")

                    title = (await title_el.inner_text()).strip() if title_el else ""
                    if not title:
                        continue
                    grade = (await grade_el.inner_text()).strip() if grade_el else ""
                    comment = (await comment_el.inner_text()).strip() if comment_el else ""
                    result["recent_feedback"].append({
                        "assignment": title,
                        "grade": grade,
                        "comment": comment,
                    })
                    logger.debug(f"[crawl_dashboard] Feedback: {title!r} grade={grade!r}")
                except Exception as e:
                    logger.warning(f"Error parsing feedback item: {e}")
        except Exception as e:
            logger.warning(f"[crawl_dashboard] Feedback extraction failed: {e}")

        await self._save_page_snapshot("dashboard_signals")
        logger.info(
            f"[crawl_dashboard] Done: {len(result['todo_items'])} todos, "
            f"{len(result['recent_activity'])} activity items, "
            f"{len(result['course_cards'])} cards, "
            f"{len(result['recent_feedback'])} feedback items"
        )
        return result

    # ------------------------------------------------------------------ #
    #  Course navigation discovery                                        #
    # ------------------------------------------------------------------ #

    async def discover_course_nav(self, course_id: str) -> list[dict]:
        """
        Dynamically discover all visible navigation entries for a course.
        Returns list of {label, url, nav_id} dicts.
        Canvas navigation varies per course — don't assume fixed items.
        """
        logger.debug(f"[discover_course_nav] course_id={course_id}")
        await self._goto(f"{self.base_url}/courses/{course_id}")
        await self.page.wait_for_timeout(1500)

        nav_items = []
        try:
            nav_els = await self.page.query_selector_all(
                "#section-tabs a, .nav-item a, .menu-item a"
            )
            logger.debug(f"[discover_course_nav] Found {len(nav_els)} nav elements for course {course_id}")
            seen_hrefs = set()
            for el in nav_els:
                try:
                    href = await el.get_attribute("href") or ""
                    label = (await el.inner_text()).strip()
                    nav_id = await el.get_attribute("data-id") or ""

                    if not href or not label:
                        continue
                    if href in seen_hrefs:
                        continue
                    seen_hrefs.add(href)

                    full_url = f"{self.base_url}{href}" if href.startswith("/") else href
                    nav_items.append({"label": label, "url": full_url, "nav_id": nav_id})
                    logger.debug(f"[discover_course_nav] Nav: {label!r} -> {full_url}")
                except Exception as e:
                    logger.warning(f"Error parsing nav item: {e}")
        except Exception as e:
            logger.warning(f"[discover_course_nav] Nav extraction failed for course {course_id}: {e}")

        logger.info(f"[discover_course_nav] Found {len(nav_items)} nav entries for course {course_id}")
        return nav_items

    # ------------------------------------------------------------------ #
    #  Quiz discovery                                                      #
    # ------------------------------------------------------------------ #

    async def get_quizzes(self, course_id: str) -> list[dict]:
        """
        Discover quizzes from the quizzes index page.
        SAFE MODE: Only captures summary/index data. Never clicks Take Quiz.
        Returns list of quiz summary dicts.
        """
        logger.debug(f"[get_quizzes] course_id={course_id}")
        await self._goto(f"{self.base_url}/courses/{course_id}/quizzes")
        await self.page.wait_for_timeout(1500)
        await self._save_page_snapshot(f"{course_id}_quizzes")

        # SAFETY CHECK: abort if an active quiz attempt is in progress
        active_attempt = await self.page.query_selector(".quiz-submit, #submit_quiz_form")
        if active_attempt:
            logger.warning(f"[SAFETY] Active quiz attempt detected on course {course_id} quizzes page — aborting extraction")
            return []

        quizzes = []
        try:
            quiz_els = await self.page.query_selector_all(
                ".quiz, .quiz-list .quiz, #assignment-quizzes .quiz"
            )
            logger.debug(f"[get_quizzes] Found {len(quiz_els)} quiz elements for course {course_id}")

            for el in quiz_els:
                try:
                    # title
                    title_el = (
                        await el.query_selector(".quiz-title")
                        or await el.query_selector("h3")
                        or await el.query_selector("h4")
                    )
                    title = (await title_el.inner_text()).strip() if title_el else ""
                    if not title:
                        continue

                    # quiz URL
                    link_el = await el.query_selector("a")
                    quiz_url = ""
                    if link_el:
                        href = await link_el.get_attribute("href") or ""
                        quiz_url = f"{self.base_url}{href}" if href.startswith("/") else href

                    # due date
                    due_el = (
                        await el.query_selector(".due-date")
                        or await el.query_selector(".date-available")
                        or await el.query_selector("[data-due-date]")
                    )
                    due_date = ""
                    if due_el:
                        due_date = (await due_el.inner_text()).strip()

                    # points
                    pts_el = (
                        await el.query_selector(".point-count")
                        or await el.query_selector(".display_points_possible")
                    )
                    point_value = (await pts_el.inner_text()).strip() if pts_el else ""

                    # question count — parse "N Questions" style text
                    qcount_el = await el.query_selector(".question-count")
                    question_count = None
                    if qcount_el:
                        qcount_text = (await qcount_el.inner_text()).strip()
                        try:
                            question_count = int(qcount_text.strip().split()[0])
                        except Exception:
                            question_count = None

                    # time limit — parse "N Minutes" style text
                    time_el = await el.query_selector(".time-limit")
                    time_limit = ""
                    if time_el:
                        time_limit = (await time_el.inner_text()).strip()
                    is_timed = bool(time_limit)

                    # allowed attempts
                    attempts_el = await el.query_selector(".allowed-attempts")
                    allowed_attempts = (await attempts_el.inner_text()).strip() if attempts_el else ""

                    # status
                    status_el = (
                        await el.query_selector(".submitted")
                        or await el.query_selector(".complete")
                        or await el.query_selector(".not-submitted")
                    )
                    status = (await status_el.inner_text()).strip() if status_el else ""

                    quiz = {
                        "title": title,
                        "quiz_url": quiz_url,
                        "due_date": due_date,
                        "point_value": point_value,
                        "question_count": question_count,
                        "time_limit": time_limit,
                        "is_timed": is_timed,
                        "allowed_attempts": allowed_attempts,
                        "status": status,
                    }
                    quizzes.append(quiz)
                    logger.debug(f"[get_quizzes] Quiz: {title!r} timed={is_timed} status={status!r}")
                except Exception as e:
                    logger.warning(f"Error parsing quiz element: {e}")
        except Exception as e:
            logger.warning(f"[get_quizzes] Quiz extraction failed for course {course_id}: {e}")

        logger.info(f"[get_quizzes] Found {len(quizzes)} quizzes for course {course_id}")
        return quizzes

    # ------------------------------------------------------------------ #
    #  Discussion discovery                                               #
    # ------------------------------------------------------------------ #

    async def get_discussions(self, course_id: str) -> list[dict]:
        """
        Discover discussions with state signals: pinned, closed, reply counts, graded status.
        """
        logger.debug(f"[get_discussions] course_id={course_id}")
        await self._goto(f"{self.base_url}/courses/{course_id}/discussion_topics")
        await self.page.wait_for_timeout(1500)
        await self._save_page_snapshot(f"{course_id}_discussions")

        discussions = []
        try:
            disc_els = await self.page.query_selector_all(
                ".discussion-list .discussion, .discussion-topic"
            )
            logger.debug(f"[get_discussions] Found {len(disc_els)} discussion elements for course {course_id}")

            for el in disc_els:
                try:
                    # title and url
                    title_el = (
                        await el.query_selector(".discussion-title a")
                        or await el.query_selector("h3 a")
                    )
                    title = (await title_el.inner_text()).strip() if title_el else ""
                    if not title:
                        continue
                    href = await title_el.get_attribute("href") if title_el else ""
                    url = f"{self.base_url}{href}" if href and href.startswith("/") else href or ""

                    # pinned
                    pinned_el = (
                        await el.query_selector(".pinned")
                        or await el.query_selector("[data-pinned='true']")
                    )
                    is_pinned = pinned_el is not None

                    # closed / locked
                    closed_el = (
                        await el.query_selector(".locked")
                        or await el.query_selector(".closed")
                        or await el.query_selector("[data-closed='true']")
                    )
                    is_closed = closed_el is not None

                    # reply count
                    reply_el = (
                        await el.query_selector(".total-items")
                        or await el.query_selector(".reply-count")
                    )
                    reply_count = 0
                    if reply_el:
                        try:
                            reply_count = int((await reply_el.inner_text()).strip().split()[0])
                        except Exception:
                            reply_count = 0

                    # unread count
                    unread_el = await el.query_selector(".unread-items")
                    unread_count = 0
                    if unread_el:
                        try:
                            unread_count = int((await unread_el.inner_text()).strip().split()[0])
                        except Exception:
                            unread_count = 0

                    # graded
                    graded_el = (
                        await el.query_selector(".discussion-points-possible")
                        or await el.query_selector("[data-assignment-id]")
                    )
                    is_graded = graded_el is not None

                    # due date (only meaningful if graded)
                    due_el = await el.query_selector(".due-date")
                    due_date = (await due_el.inner_text()).strip() if due_el else ""

                    # points possible
                    pts_el = await el.query_selector(".points_possible")
                    point_value = (await pts_el.inner_text()).strip() if pts_el else ""

                    # last reply date
                    last_reply_el = await el.query_selector(".last-reply-at")
                    last_reply_date = (await last_reply_el.inner_text()).strip() if last_reply_el else ""

                    discussion = {
                        "title": title,
                        "url": url,
                        "is_pinned": is_pinned,
                        "is_closed": is_closed,
                        "reply_count": reply_count,
                        "unread_count": unread_count,
                        "is_graded": is_graded,
                        "due_date": due_date,
                        "point_value": point_value,
                        "last_reply_date": last_reply_date,
                    }
                    discussions.append(discussion)
                    logger.debug(f"[get_discussions] Discussion: {title!r} graded={is_graded} replies={reply_count}")
                except Exception as e:
                    logger.warning(f"Error parsing discussion element: {e}")
        except Exception as e:
            logger.warning(f"[get_discussions] Discussion extraction failed for course {course_id}: {e}")

        logger.info(f"[get_discussions] Found {len(discussions)} discussions for course {course_id}")
        return discussions

    # ------------------------------------------------------------------ #
    #  Calendar discovery                                                 #
    # ------------------------------------------------------------------ #

    async def get_calendar_events(self, course_ids: list[str]) -> list[dict]:
        """
        Capture calendar events spanning all enrolled courses.
        Captures dated and undated items.
        Uses the Canvas calendar page.
        """
        logger.info(f"[get_calendar_events] Navigating to calendar (course_ids={course_ids})")
        await self._goto(f"{self.base_url}/calendar")
        await self.page.wait_for_timeout(2000)
        await self._save_page_snapshot("calendar")

        all_events: list[dict] = []

        # --- Dated events ---
        try:
            event_els = await self.page.query_selector_all(
                ".fc-event, .calendar-event, .event-item"
            )
            logger.debug(f"[get_calendar_events] Found {len(event_els)} dated event elements")
            for el in event_els:
                try:
                    title_el = (
                        await el.query_selector(".fc-title")
                        or await el.query_selector(".event-title")
                    )
                    title = ""
                    if title_el:
                        title = (await title_el.inner_text()).strip()
                    if not title:
                        title = (await el.inner_text()).strip()
                    if not title:
                        continue

                    # date — try data attribute, then title attribute, then text
                    date = await el.get_attribute("data-date") or ""
                    if not date:
                        date = await el.get_attribute("title") or ""

                    # event type from icon/class
                    class_attr = await el.get_attribute("class") or ""
                    event_type = "event"
                    if "assignment" in class_attr:
                        event_type = "assignment"
                    elif "quiz" in class_attr:
                        event_type = "quiz"
                    elif "discussion" in class_attr:
                        event_type = "discussion"

                    # event url
                    link_el = await el.query_selector("a")
                    event_url = ""
                    if link_el:
                        href = await link_el.get_attribute("href") or ""
                        event_url = f"{self.base_url}{href}" if href.startswith("/") else href

                    # course id — try data attribute
                    course_id = await el.get_attribute("data-course-id") or ""

                    all_events.append({
                        "title": title,
                        "date": date,
                        "course_id": course_id,
                        "event_url": event_url,
                        "event_type": event_type,
                        "is_undated": False,
                    })
                    logger.debug(f"[get_calendar_events] Event: {title!r} date={date!r} type={event_type}")
                except Exception as e:
                    logger.warning(f"Error parsing calendar event: {e}")
        except Exception as e:
            logger.warning(f"[get_calendar_events] Dated event extraction failed: {e}")

        # --- Undated events ---
        try:
            await self._goto(f"{self.base_url}/calendar#view_name=undated")
            await self.page.wait_for_timeout(2000)

            undated_els = await self.page.query_selector_all(
                ".fc-event, .calendar-event, .event-item"
            )
            logger.debug(f"[get_calendar_events] Found {len(undated_els)} undated event elements")
            for el in undated_els:
                try:
                    title_el = (
                        await el.query_selector(".fc-title")
                        or await el.query_selector(".event-title")
                    )
                    title = ""
                    if title_el:
                        title = (await title_el.inner_text()).strip()
                    if not title:
                        title = (await el.inner_text()).strip()
                    if not title:
                        continue

                    class_attr = await el.get_attribute("class") or ""
                    event_type = "event"
                    if "assignment" in class_attr:
                        event_type = "assignment"
                    elif "quiz" in class_attr:
                        event_type = "quiz"
                    elif "discussion" in class_attr:
                        event_type = "discussion"

                    link_el = await el.query_selector("a")
                    event_url = ""
                    if link_el:
                        href = await link_el.get_attribute("href") or ""
                        event_url = f"{self.base_url}{href}" if href.startswith("/") else href

                    course_id = await el.get_attribute("data-course-id") or ""

                    all_events.append({
                        "title": title,
                        "date": "",
                        "course_id": course_id,
                        "event_url": event_url,
                        "event_type": event_type,
                        "is_undated": True,
                    })
                    logger.debug(f"[get_calendar_events] Undated event: {title!r} type={event_type}")
                except Exception as e:
                    logger.warning(f"Error parsing undated calendar event: {e}")
        except Exception as e:
            logger.warning(f"[get_calendar_events] Undated event extraction failed: {e}")

        logger.info(f"[get_calendar_events] Total events captured: {len(all_events)}")
        return all_events

    # ------------------------------------------------------------------ #
    #  Safe quiz detail                                                   #
    # ------------------------------------------------------------------ #

    async def get_quiz_details(self, quiz_url: str, course_id: str) -> dict:
        """
        Navigate to a quiz summary/detail page ONLY.
        SAFETY: If we detect an active attempt page, abort immediately.
        Returns quiz detail dict or empty dict if unsafe.
        """
        logger.debug(f"[get_quiz_details] Navigating to {quiz_url} (course_id={course_id})")
        await self._goto(quiz_url)
        await self.page.wait_for_timeout(1500)

        # SAFETY CHECK — abort immediately if an active quiz attempt is detected
        active_form = await self.page.query_selector(
            "#submit_quiz_form, .quiz-submit-button, .question-body form[action*='submission']"
        )
        if active_form:
            logger.warning(f"[SAFETY] Active quiz attempt detected at {quiz_url} — aborting")
            return {"is_restricted": True, "url": quiz_url}

        details: dict = {
            "url": quiz_url,
            "course_id": course_id,
            "is_restricted": False,
            "instructions": "",
            "time_limit": "",
            "allowed_attempts": "",
            "due_date": "",
            "available_from": "",
            "available_until": "",
            "take_quiz_link": "",
            "proctoring_notice": False,
        }

        try:
            # instructions
            instr_el = (
                await self.page.query_selector(".description.user_content")
                or await self.page.query_selector(".quiz-instructions")
            )
            if instr_el:
                details["instructions"] = (await instr_el.inner_text()).strip()
                logger.debug(f"[get_quiz_details] Instructions: {len(details['instructions'])} chars")

            # time limit
            tl_el = await self.page.query_selector(".time-limit-minutes")
            if tl_el:
                details["time_limit"] = (await tl_el.inner_text()).strip()

            # allowed attempts
            att_el = await self.page.query_selector(".allowed-attempts")
            if att_el:
                details["allowed_attempts"] = (await att_el.inner_text()).strip()

            # due date
            due_el = await self.page.query_selector(".due-date")
            if due_el:
                details["due_date"] = (await due_el.inner_text()).strip()

            # available from / until
            avail_els = await self.page.query_selector_all(".available-date")
            if len(avail_els) >= 1:
                details["available_from"] = (await avail_els[0].inner_text()).strip()
            if len(avail_els) >= 2:
                details["available_until"] = (await avail_els[1].inner_text()).strip()

            # take quiz link — capture href but do NOT click
            take_el = await self.page.query_selector("a.btn-primary[href*='quiz'], a[href*='take_quiz']")
            if not take_el:
                # secondary: look for any link with "Take" in text
                all_links = await self.page.query_selector_all("a")
                for lnk in all_links:
                    txt = (await lnk.inner_text()).strip().lower()
                    if "take" in txt and "quiz" in txt:
                        take_el = lnk
                        break
            if take_el:
                href = await take_el.get_attribute("href") or ""
                details["take_quiz_link"] = f"{self.base_url}{href}" if href.startswith("/") else href
                logger.debug(f"[get_quiz_details] Take quiz link found: {details['take_quiz_link']!r} (NOT clicked)")

            # proctoring notice
            instr_text = details["instructions"].lower()
            proctoring_keywords = ["respondus", "lockdown browser", "proctored", "proctoring"]
            details["proctoring_notice"] = any(kw in instr_text for kw in proctoring_keywords)

            logger.info(f"[get_quiz_details] Details captured for {quiz_url}: proctored={details['proctoring_notice']}")
        except Exception as e:
            logger.warning(f"[get_quiz_details] Error extracting quiz details from {quiz_url}: {e}", exc_info=True)

        return details

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

        # Capture dashboard urgency signals first (To-Do, Recent Activity, feedback)
        logger.debug("[crawl_all] Capturing dashboard signals")
        try:
            self.knowledge["dashboard"] = await self.get_dashboard_signals()
        except Exception as e:
            logger.warning(f"[crawl_all] Dashboard signals failed: {e}")
            self.knowledge["dashboard"] = {}

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

            for a in assignments:
                logger.info(f"  -> {a['title']}")

            # Fetch assignment details concurrently — up to 5 Playwright pages in parallel
            _DETAIL_CONCURRENCY = 5
            detail_pages = []
            try:
                detail_pages = [await self.context.new_page() for _ in range(min(_DETAIL_CONCURRENCY, len(assignments)))]
            except Exception as _dp_err:
                logger.warning(f"[crawl_all] Could not create detail pages: {_dp_err}")

            detail_sem = asyncio.Semaphore(_DETAIL_CONCURRENCY)
            page_pool = list(detail_pages)
            page_idx = 0
            page_pool_lock = asyncio.Lock()

            async def _fetch_details(a: dict) -> None:
                nonlocal page_idx
                async with detail_sem:
                    async with page_pool_lock:
                        _pg = page_pool[page_idx % len(page_pool)] if page_pool else None
                        page_idx += 1
                    try:
                        a["details"] = await self.get_assignment_details(a["url"], page=_pg)
                        desc_len = len(a["details"].get("description", "") or "")
                        logger.debug(f"[crawl_all] details for {a['title']!r}: {desc_len} chars")
                    except Exception as _det_err:
                        logger.warning(f"[crawl_all] Failed to get details for {a['title']!r}: {_det_err}")
                        a["details"] = {}

            try:
                await asyncio.gather(*[_fetch_details(a) for a in assignments])
            finally:
                for _dp in detail_pages:
                    try:
                        await _dp.close()
                    except Exception:
                        pass

            course["assignments"] = assignments

            logger.debug(f"[crawl_all] Fetching discussions for {course['name']}")
            try:
                course["discussions"] = await self.get_discussions(cid)
                logger.info(f"  Found {len(course['discussions'])} discussions")
            except Exception as e:
                logger.warning(f"[crawl_all] Discussions failed for {course['name']}: {e}")
                course["discussions"] = []

            logger.debug(f"[crawl_all] Fetching quizzes for {course['name']}")
            try:
                course["quizzes"] = await self.get_quizzes(cid)
                logger.info(f"  Found {len(course['quizzes'])} quizzes")
            except Exception as e:
                logger.warning(f"[crawl_all] Quizzes failed for {course['name']}: {e}")
                course["quizzes"] = []

            # Fetch quiz details concurrently — up to 3 pages in parallel
            if course["quizzes"]:
                _quiz_sem = asyncio.Semaphore(3)

                async def _fetch_quiz_detail(quiz: dict) -> None:
                    async with _quiz_sem:
                        try:
                            quiz["details"] = await self.get_quiz_details(quiz["quiz_url"], cid)
                            logger.debug(f"[crawl_all] Quiz details for {quiz.get('title','')!r} fetched")
                        except Exception as _qerr:
                            logger.warning(f"[crawl_all] Quiz details failed for {quiz.get('title','')!r}: {_qerr}")
                            quiz["details"] = {}

                try:
                    await asyncio.gather(*[_fetch_quiz_detail(q) for q in course["quizzes"]])
                except Exception as _qall_err:
                    logger.warning(f"[crawl_all] Quiz detail gather failed: {_qall_err}")

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

        # ------------------------------------------------------------------
        # Post-crawl: normalize → detect changes → store CanvasObjects → build graph
        # This runs in-process after all courses are scraped so it doesn't
        # interrupt the browser session.
        # ------------------------------------------------------------------
        if self.kb:
            try:
                await self._run_normalization_pass()
            except Exception as e:
                logger.error(f"[crawl_all] Normalization pass failed: {e}", exc_info=True)

        return self.knowledge

    async def _run_normalization_pass(self) -> None:
        """
        Convert all crawled dicts to CanvasObjects, detect changes, store in
        ChromaDB, and build the knowledge graph.

        Requires self.kb to be set (injected by api/main.py before crawl_all()).

        The synchronous work (embedding generation, ChromaDB I/O) is offloaded
        to a thread-pool executor so it does not block the async event loop.
        """
        loop = asyncio.get_event_loop()
        logger.info("[normalization] Starting normalization pass in executor")
        await loop.run_in_executor(None, self._run_normalization_pass_sync)

    def _run_normalization_pass_sync(self) -> None:
        """Synchronous body of the normalization pass — runs in a thread executor."""
        from agent.canvas_normalizer import CanvasNormalizer
        from agent.graph_builder import GraphBuilder
        from agent.change_detector import ChangeDetector

        normalizer = CanvasNormalizer()
        graph_builder = GraphBuilder()
        detector = ChangeDetector()

        all_objects = []

        # Normalize dashboard signals
        dashboard = self.knowledge.get("dashboard", {})
        for todo_raw in dashboard.get("todo_items", []):
            try:
                all_objects.append(normalizer.normalize_dashboard_todo(todo_raw))
            except Exception:
                pass

        # Normalize per-course objects
        for course in self.knowledge.get("courses", []):
            try:
                course_objects = normalizer.normalize_course(course)
                all_objects.extend(course_objects)
            except Exception as e:
                logger.warning(f"[normalization] Failed for {course.get('name','?')}: {e}")

        logger.info(f"[normalization] {len(all_objects)} objects normalized — running change detection")

        # Change detection (reads existing objects from ChromaDB)
        changes = detector.detect_batch(all_objects, self.kb)

        # Batch upsert — single embedding call for all objects
        try:
            self.kb.upsert_canvas_objects_batch(all_objects)
        except Exception as e:
            logger.error(f"[normalization] batch upsert failed: {e}", exc_info=True)

        # Store change records (as JSON in a simple collection)
        for chg in changes:
            try:
                import dataclasses
                import hashlib as _hs
                chg_dict = dataclasses.asdict(chg)
                chg_id = chg.change_id
                # Store as plain JSON in course_content collection
                self.kb.course_content.upsert(
                    ids=[chg_id],
                    documents=[f"Change: {chg.change_type} on {chg.object_id}"],
                    metadatas=[{
                        "type": "change_record",
                        "course_id": chg.course_id,
                        "object_id": chg.object_id,
                        "change_type": chg.change_type,
                        "change_severity": chg.change_severity,
                        "restudy_flag": chg.restudy_flag,
                        "replan_flag": chg.replan_flag,
                        "detected_at": chg.detected_at,
                    }],
                )
            except Exception as e:
                logger.warning(f"[normalization] change record store failed: {e}")

        # Build and store graph edges
        try:
            edges = graph_builder.build_from_objects(all_objects)
            for edge in edges:
                try:
                    self.kb.upsert_graph_edge(edge)
                except Exception as e:
                    logger.warning(f"[graph] Edge upsert failed {edge.edge_id}: {e}")
            logger.info(
                f"[normalization] {len(all_objects)} objects, "
                f"{len(changes)} change(s), {len(edges)} edges stored"
            )
        except Exception as e:
            logger.error(f"[graph] build_from_objects failed: {e}", exc_info=True)

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
