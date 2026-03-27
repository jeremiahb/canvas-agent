"""
canvas_normalizer.py — Converts raw crawler output dicts to CanvasObject instances.

The crawler (crawler.py) produces course data as nested Python dicts stored in
canvas_knowledge.json.  This module reads those dicts and returns fully-typed
CanvasObject instances that can be stored in the knowledge graph.

Each normalize_* method:
  - Sets the canonical object_type, educational_role, and classification flags
  - Applies assignment subtype, discussion tag, quiz safe-mode, and stub detection
  - Computes an initial change_hash for change-detection on future crawls
  - Sets created_at / updated_at timestamps

Priority scores (importance, urgency, etc.) are also computed here based on
due-date proximity and object type.  The knowledge organizer may later refine
these scores after generating AI study notes.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from agent.canvas_schema import (
    AssignmentSubtype,
    CanvasObject,
    CaptureCompleteness,
    DiscussionTag,
    EducationalRole,
    ObjectType,
    QuizTag,
    RelationType,
    RubricCriterion,
    SourceMode,
    StatusSignal,
    StructuredSection,
    infer_educational_role,
    make_change_hash,
    make_object_id,
    now_iso,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_date(s: str) -> datetime | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt[:len(s[:19])])
        except ValueError:
            continue
    return None


def _days_until(date_str: str) -> float | None:
    d = _parse_date(date_str)
    if d is None:
        return None
    now = _now()
    # Make both naive for comparison
    if d.tzinfo:
        d = d.replace(tzinfo=None)
    return (d - now.replace(tzinfo=None)).total_seconds() / 86400


def _urgency_from_due(due_date: str) -> float:
    days = _days_until(due_date)
    if days is None:
        return 0.4
    if days <= 1:
        return 1.0
    if days <= 3:
        return 0.9
    if days <= 7:
        return 0.7
    return max(0.3, 0.7 - (days - 7) * 0.02)


def _recency_score(posted_date: str) -> float:
    days = _days_until(posted_date)
    if days is None:
        return 0.4
    # days_until for past dates is negative
    days_ago = -days
    if days_ago <= 2:
        return 0.9
    if days_ago <= 7:
        return 0.7
    if days_ago <= 30:
        return 0.5
    return 0.2


def _announcement_mentions_change(body: str) -> list[str]:
    """Check if the announcement body mentions changed deadlines or instructions."""
    action_items = []
    body_l = body.lower()
    if any(kw in body_l for kw in ("due date", "deadline", "extended", "postponed", "moved")):
        action_items.append("Review for updated due dates or deadlines")
    if any(kw in body_l for kw in ("changed", "updated", "revised", "correction", "erratum")):
        action_items.append("Instructions or requirements may have changed — re-read carefully")
    if any(kw in body_l for kw in ("cancelled", "canceled", "no longer")):
        action_items.append("Something may have been cancelled — verify current requirements")
    return action_items


def _detect_assignment_subtype(raw: dict) -> str:
    """Infer AssignmentSubtype from the submission_types list or text."""
    # Crawler may store as list or comma-joined string
    stypes = raw.get("submission_types") or raw.get("submission_type") or ""
    if isinstance(stypes, list):
        stypes_str = " ".join(stypes).lower()
    else:
        stypes_str = str(stypes).lower()

    if raw.get("group_category_id") or "group" in stypes_str:
        return AssignmentSubtype.GROUP.value
    if raw.get("peer_reviews") or "peer" in stypes_str:
        return AssignmentSubtype.PEER_REVIEW.value
    if "online_upload" in stypes_str or "file_upload" in stypes_str or "upload" in stypes_str:
        return AssignmentSubtype.FILE_UPLOAD.value
    if "online_text_entry" in stypes_str or "text_entry" in stypes_str:
        return AssignmentSubtype.TEXT_ENTRY.value
    if "online_url" in stypes_str or "url" in stypes_str:
        return AssignmentSubtype.WEBSITE_URL.value
    if "none" in stypes_str or not stypes_str:
        return AssignmentSubtype.NO_SUBMISSION.value
    return AssignmentSubtype.UNKNOWN.value


def _parse_rubric(raw_rubric) -> list[RubricCriterion]:
    """Convert raw rubric list (dicts or strings) to RubricCriterion instances."""
    if not raw_rubric or not isinstance(raw_rubric, list):
        return []
    result = []
    for item in raw_rubric:
        if isinstance(item, dict):
            result.append(RubricCriterion(
                criterion_id=str(item.get("id", "")),
                description=item.get("description", item.get("long_description", "")),
                points_max=float(item.get("points", 0) or 0),
                ratings=[
                    {"label": r.get("description", ""), "points": r.get("points", 0)}
                    for r in (item.get("ratings") or [])
                    if isinstance(r, dict)
                ],
            ))
        elif isinstance(item, str):
            result.append(RubricCriterion(description=item))
    return result


def _strip_html(html: str) -> str:
    """Very fast HTML-to-text strip for content fields."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# Main normalizer class
# ---------------------------------------------------------------------------

class CanvasNormalizer:
    """
    Converts raw crawler dicts to fully-typed CanvasObject instances.

    Usage:
        normalizer = CanvasNormalizer()
        obj = normalizer.normalize_assignment(raw_assignment_dict, course_dict)
    """

    # ------------------------------------------------------------------
    # Modules
    # ------------------------------------------------------------------

    def normalize_module(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        title = raw.get("title", raw.get("name", ""))
        url = raw.get("url") or f"{course.get('url','')}/modules#{title}"

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.MODULE.value, url or title),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.MODULE.value,
            educational_role=EducationalRole.LOW_VALUE_NAVIGATION.value,
            title=title,
            source_url=url,
            canonical_url=url,
            module_name=title,
            module_order=int(raw.get("position", raw.get("order", -1))),
            week_label=raw.get("week_label", ""),
            capture_completeness=CaptureCompleteness.FULL.value,
            importance_score=0.5,
            urgency_score=0.3,
            study_value_score=0.4,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Module items
    # ------------------------------------------------------------------

    def normalize_module_item(
        self, raw: dict, course: dict, module: dict, item_order: int
    ) -> CanvasObject:
        course_id = str(course.get("id", ""))
        title = raw.get("title", "")
        url = raw.get("url", raw.get("html_url", ""))
        item_type_raw = raw.get("type", raw.get("item_type", "")).lower()
        lock_state = raw.get("lock_state", raw.get("completion_state", "not_started"))

        # Map item_type to ObjectType
        type_map = {
            "assignment": ObjectType.ASSIGNMENT.value,
            "quiz": ObjectType.QUIZ_SUMMARY.value,
            "discussion": ObjectType.DISCUSSION.value,
            "page": ObjectType.PAGE.value,
            "file": ObjectType.FILE.value,
            "external_url": ObjectType.EXTERNAL_URL.value,
            "external_tool": ObjectType.EXTERNAL_TOOL.value,
            "sub_header": ObjectType.MODULE_ITEM.value,
        }
        obj_type = type_map.get(item_type_raw, ObjectType.MODULE_ITEM.value)

        status_signals = []
        if lock_state == "completed":
            status_signals.append(StatusSignal(signal_type="completion", value="completed"))
        elif lock_state == "overdue":
            status_signals.append(StatusSignal(signal_type="urgency", value="overdue"))
        elif raw.get("lock_state") == "locked":
            status_signals.append(StatusSignal(signal_type="lock", value="locked"))

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.MODULE_ITEM.value, url or title),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.MODULE_ITEM.value,
            secondary_tags=[obj_type],
            educational_role=infer_educational_role(obj_type, [], title),
            title=title,
            source_url=url,
            canonical_url=url,
            module_name=module.get("title", module.get("name", "")),
            module_order=int(module.get("position", module.get("order", -1))),
            item_order=item_order,
            week_label=module.get("week_label", ""),
            status_signals=status_signals,
            capture_completeness=CaptureCompleteness.MINIMAL.value,
            importance_score=0.6,
            urgency_score=0.4,
            study_value_score=0.5,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Assignments
    # ------------------------------------------------------------------

    def normalize_assignment(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        title = raw.get("title", raw.get("name", ""))
        url = raw.get("url", raw.get("html_url", ""))
        due = raw.get("due_at", raw.get("due_date", ""))
        avail_from = raw.get("unlock_at", raw.get("available_from", ""))
        avail_until = raw.get("lock_at", raw.get("available_until", ""))
        points = float(raw.get("points_possible", raw.get("points", 0)) or 0)

        body_html = raw.get("description", raw.get("body", ""))
        body_text = _strip_html(body_html) if body_html else ""

        rubric = _parse_rubric(raw.get("rubric"))
        subtype = _detect_assignment_subtype(raw)

        secondary_tags: list[str] = []
        if raw.get("has_rubric") or rubric:
            secondary_tags.append("has_rubric")
        if raw.get("peer_reviews"):
            secondary_tags.append(AssignmentSubtype.PEER_REVIEW.value)
        if raw.get("group_category_id"):
            secondary_tags.append(AssignmentSubtype.GROUP.value)

        urgency = _urgency_from_due(due)
        importance = 0.85 if due else 0.6
        if points >= 100:
            importance = min(1.0, importance + 0.1)

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.ASSIGNMENT.value, url or title),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.ASSIGNMENT.value,
            secondary_tags=secondary_tags,
            educational_role=EducationalRole.ASSIGNMENT_DIRECTIONS.value,
            assignment_subtype=subtype,
            title=title,
            source_url=url,
            canonical_url=url,
            module_name=raw.get("module_name", ""),
            due_date=due,
            available_from=avail_from,
            available_until=avail_until,
            main_content=body_text[:8000],
            rubric=rubric,
            point_value=points,
            submission_state=raw.get("submission_state", ""),
            capture_completeness=CaptureCompleteness.FULL.value,
            importance_score=importance,
            urgency_score=urgency,
            study_value_score=0.8,
            risk_score=urgency if not raw.get("submission_state") else 0.1,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Discussions
    # ------------------------------------------------------------------

    def normalize_discussion(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        title = raw.get("title", "")
        url = raw.get("url", raw.get("html_url", ""))
        due = raw.get("due_at", raw.get("due_date", ""))
        body_html = raw.get("message", raw.get("body", ""))
        body_text = _strip_html(body_html) if body_html else ""

        discussion_tags: list[str] = []
        if raw.get("pinned"):
            discussion_tags.append(DiscussionTag.PINNED.value)
        if raw.get("locked") or raw.get("closed_for_comments"):
            discussion_tags.append(DiscussionTag.CLOSED_FOR_COMMENTS.value)
        if raw.get("allow_rating") or raw.get("subscribed_capable"):
            discussion_tags.append(DiscussionTag.SUBSCRIBED_CAPABLE.value)
        if raw.get("discussion_type") == "threaded":
            discussion_tags.append(DiscussionTag.THREADED_REPLIES_CAPABLE.value)
        if raw.get("assignment") or raw.get("assignment_id") or raw.get("points_possible"):
            discussion_tags.append(DiscussionTag.GRADED_DISCUSSION.value)
        else:
            discussion_tags.append(DiscussionTag.UNGRADED_DISCUSSION.value)

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.DISCUSSION.value, url or title),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.DISCUSSION.value,
            discussion_tags=discussion_tags,
            educational_role=EducationalRole.DISCUSSION_PROMPT.value,
            title=title,
            source_url=url,
            canonical_url=url,
            module_name=raw.get("module_name", ""),
            due_date=due,
            posted_date=raw.get("posted_at", raw.get("created_at", "")),
            main_content=body_text[:8000],
            reply_count=int(raw.get("discussion_subentry_count", raw.get("reply_count", 0))),
            point_value=float(raw.get("points_possible", 0) or 0),
            capture_completeness=CaptureCompleteness.FULL.value,
            importance_score=0.7 if DiscussionTag.GRADED_DISCUSSION.value in discussion_tags else 0.5,
            urgency_score=_urgency_from_due(due),
            study_value_score=0.65,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Announcements
    # ------------------------------------------------------------------

    def normalize_announcement(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        title = raw.get("title", "")
        url = raw.get("url", raw.get("html_url", ""))
        posted = raw.get("posted_at", raw.get("created_at", ""))
        body_html = raw.get("message", raw.get("body", ""))
        body_text = _strip_html(body_html) if body_html else ""

        action_items = _announcement_mentions_change(body_text)
        recency = _recency_score(posted)

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.ANNOUNCEMENT.value, url or title),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.ANNOUNCEMENT.value,
            educational_role=EducationalRole.ADMINISTRATIVE_NOTICE.value,
            title=title,
            source_url=url,
            canonical_url=url,
            posted_date=posted,
            main_content=body_text[:8000],
            action_items=action_items,
            capture_completeness=CaptureCompleteness.FULL.value,
            importance_score=recency,
            urgency_score=recency,
            study_value_score=0.4,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Quizzes (safe summary mode only)
    # ------------------------------------------------------------------

    def normalize_quiz(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        title = raw.get("title", "")
        url = raw.get("url", raw.get("html_url", ""))
        due = raw.get("due_at", raw.get("due_date", ""))

        quiz_tags: list[str] = []
        if raw.get("time_limit"):
            quiz_tags.append(QuizTag.TIMED_QUIZ.value)
        else:
            quiz_tags.append(QuizTag.UNTIMED_QUIZ.value)
        if raw.get("lockdown_browser") or raw.get("require_lockdown_browser"):
            quiz_tags.append(QuizTag.PROCTORED_QUIZ_CANDIDATE.value)
        if raw.get("due_date_required"):
            quiz_tags.append(QuizTag.DUE_DATE_WARNING_CAPABLE.value)

        # Always mark as summary — never attempt mode during live crawl
        quiz_mode = raw.get("quiz_mode", "summary")
        if quiz_mode == "attempt":
            # Safety override: if crawler accidentally landed on attempt, treat as unknown
            quiz_mode = "unknown"

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.QUIZ_SUMMARY.value, url or title),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.QUIZ_SUMMARY.value,
            quiz_tags=quiz_tags,
            quiz_mode=quiz_mode,
            educational_role=EducationalRole.ASSESSMENT_INSTRUCTIONS.value,
            title=title,
            source_url=url,
            canonical_url=url,
            module_name=raw.get("module_name", ""),
            due_date=due,
            available_from=raw.get("unlock_at", ""),
            available_until=raw.get("lock_at", ""),
            main_content=raw.get("description", "")[:8000],
            point_value=float(raw.get("points_possible", 0) or 0),
            question_count=int(raw.get("question_count", 0)),
            time_limit_minutes=int(raw.get("time_limit", 0) or 0),
            allowed_attempts=int(raw.get("allowed_attempts", -1) or -1),
            capture_completeness=CaptureCompleteness.FULL.value,
            importance_score=0.85,
            urgency_score=_urgency_from_due(due),
            study_value_score=0.9,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Pages / wiki pages
    # ------------------------------------------------------------------

    def normalize_page(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        title = raw.get("title", "")
        url = raw.get("url", raw.get("html_url", ""))
        body_html = raw.get("body", raw.get("content", ""))
        body_text = _strip_html(body_html) if body_html else raw.get("text", "")
        module_name = raw.get("module_name", "")

        educational_role = infer_educational_role(
            ObjectType.PAGE.value, [], title
        )

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.PAGE.value, url or title),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.PAGE.value,
            educational_role=educational_role,
            title=title,
            source_url=url,
            canonical_url=url,
            module_name=module_name,
            posted_date=raw.get("updated_at", raw.get("created_at", "")),
            main_content=body_text[:8000],
            capture_completeness=CaptureCompleteness.FULL.value,
            importance_score=0.6,
            urgency_score=0.4,
            study_value_score=0.7,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Grade signals
    # ------------------------------------------------------------------

    def normalize_grade_signal(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        url = raw.get("url", f"{course.get('url','')}/grades")
        title = f"Grades — {course.get('name', '')}"

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.GRADE_SIGNAL.value, url or title),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.GRADE_SIGNAL.value,
            educational_role=EducationalRole.SCHEDULE_SIGNAL.value,
            title=title,
            source_url=url,
            canonical_url=url,
            main_content=str(raw.get("text", raw.get("summary", "")))[:8000],
            current_grade=str(raw.get("current_grade", "")),
            total_grade=str(raw.get("total_grade", raw.get("final_grade", ""))),
            grade_weight_group=str(raw.get("grade_weight_group", "")),
            missing_risk=bool(raw.get("missing_risk", False)),
            capture_completeness=CaptureCompleteness.FULL.value,
            importance_score=0.5,
            urgency_score=0.4,
            study_value_score=0.3,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Calendar items
    # ------------------------------------------------------------------

    def normalize_calendar_item(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        title = raw.get("title", raw.get("name", ""))
        url = raw.get("url", raw.get("html_url", ""))
        start = raw.get("start_at", raw.get("start", ""))
        end = raw.get("end_at", raw.get("end", ""))

        obj_type = (
            ObjectType.CALENDAR_UNDATED.value
            if not start
            else ObjectType.CALENDAR_ITEM.value
        )

        obj = CanvasObject(
            id=make_object_id(course_id, obj_type, url or title or start),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=obj_type,
            educational_role=EducationalRole.SCHEDULE_SIGNAL.value,
            title=title,
            source_url=url,
            canonical_url=url,
            due_date=start,
            available_until=end,
            main_content=raw.get("description", "")[:8000],
            capture_completeness=CaptureCompleteness.FULL.value,
            importance_score=0.6,
            urgency_score=_urgency_from_due(start),
            study_value_score=0.5,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Course home
    # ------------------------------------------------------------------

    def normalize_course_home(self, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        url = course.get("url", "")
        title = f"Course Home — {course.get('name', '')}"

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.COURSE_HOME.value, url or course_id),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.COURSE_HOME.value,
            educational_role=EducationalRole.LOW_VALUE_NAVIGATION.value,
            title=title,
            source_url=url,
            canonical_url=url,
            capture_completeness=CaptureCompleteness.MINIMAL.value,
            importance_score=0.4,
            urgency_score=0.2,
            study_value_score=0.3,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Dashboard To-Do items
    # ------------------------------------------------------------------

    def normalize_dashboard_todo(self, raw: dict) -> CanvasObject:
        course_id = str(raw.get("course_id", ""))
        title = raw.get("title", "")
        url = raw.get("url", raw.get("html_url", ""))
        due = raw.get("due_at", raw.get("due_date", ""))

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.DASHBOARD_TODO_ITEM.value, url or title),
            course_id=course_id,
            course_name=raw.get("course_name", ""),
            object_type=ObjectType.DASHBOARD_TODO_ITEM.value,
            educational_role=EducationalRole.SCHEDULE_SIGNAL.value,
            title=title,
            source_url=url,
            canonical_url=url,
            due_date=due,
            capture_completeness=CaptureCompleteness.MINIMAL.value,
            importance_score=0.9,
            urgency_score=_urgency_from_due(due),
            study_value_score=0.6,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # External tool stubs
    # ------------------------------------------------------------------

    def normalize_external_tool_stub(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        title = raw.get("title", raw.get("name", ""))
        url = raw.get("url", raw.get("html_url", ""))

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.EXTERNAL_TOOL_STUB.value, url or title),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.EXTERNAL_TOOL_STUB.value,
            educational_role=EducationalRole.UNRESOLVED_LAUNCH.value,
            is_external_tool_stub=True,
            title=title,
            source_url=url,
            canonical_url=url,
            module_name=raw.get("module_name", ""),
            action_items=["External tool requires manual launch — content not automatically captured"],
            capture_completeness=CaptureCompleteness.PARTIAL.value,
            importance_score=0.5,
            urgency_score=0.3,
            study_value_score=0.4,
            confidence_score=0.4,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Syllabus
    # ------------------------------------------------------------------

    def normalize_syllabus(self, raw: dict, course: dict) -> CanvasObject:
        course_id = str(course.get("id", ""))
        url = raw.get("url", f"{course.get('url','')}/assignments/syllabus")
        body = raw.get("body", raw.get("text", ""))
        body_text = _strip_html(body) if "<" in body else body

        obj = CanvasObject(
            id=make_object_id(course_id, ObjectType.SYLLABUS.value, url),
            course_id=course_id,
            course_name=course.get("name", ""),
            object_type=ObjectType.SYLLABUS.value,
            educational_role=EducationalRole.SCHEDULE_SIGNAL.value,
            title=f"Syllabus — {course.get('name', '')}",
            source_url=url,
            canonical_url=url,
            main_content=body_text[:8000],
            capture_completeness=CaptureCompleteness.FULL.value,
            importance_score=0.7,
            urgency_score=0.2,
            study_value_score=0.8,
        )
        obj.change_hash = make_change_hash(obj)
        obj.created_at = now_iso()
        obj.updated_at = now_iso()
        return obj

    # ------------------------------------------------------------------
    # Bulk normalize a course dict from canvas_knowledge.json
    # ------------------------------------------------------------------

    def normalize_course(self, course: dict) -> list[CanvasObject]:
        """
        Normalize all objects from a single course dict into CanvasObjects.

        Returns a flat list suitable for graph building and storage.
        """
        objects: list[CanvasObject] = []

        objects.append(self.normalize_course_home(course))

        for raw_module in course.get("modules", []):
            objects.append(self.normalize_module(raw_module, course))
            for i, item in enumerate(raw_module.get("items", [])):
                objects.append(self.normalize_module_item(item, course, raw_module, i))

        for raw in course.get("assignments", []):
            objects.append(self.normalize_assignment(raw, course))

        for raw in course.get("discussions", []):
            objects.append(self.normalize_discussion(raw, course))

        for raw in course.get("announcements", []):
            objects.append(self.normalize_announcement(raw, course))

        for raw in course.get("quizzes", []):
            objects.append(self.normalize_quiz(raw, course))

        for raw in course.get("pages", []):
            objects.append(self.normalize_page(raw, course))

        for raw in course.get("calendar_events", []):
            objects.append(self.normalize_calendar_item(raw, course))

        if course.get("grades"):
            grade_raw = course["grades"] if isinstance(course["grades"], dict) else {"text": str(course["grades"])}
            objects.append(self.normalize_grade_signal(grade_raw, course))

        syllabus = course.get("syllabus")
        if syllabus:
            syl_raw = syllabus if isinstance(syllabus, dict) else {"text": str(syllabus)}
            objects.append(self.normalize_syllabus(syl_raw, course))

        logger.info(f"[normalizer] {course.get('name','')} → {len(objects)} CanvasObjects")
        return objects
