"""
canvas_schema.py — Canonical data model for the Canvas student learning crawler.

This module defines all enumerations, dataclasses, and helper functions used
throughout the crawler pipeline (crawl → notes → topics → concepts). It is the
single source of truth for how Canvas objects are represented, stored, and
exchanged between pipeline stages.

Design notes:
- Enum fields in dataclasses store `.value` strings (not Enum members) so that
  ChromaDB metadata serialization works without custom encoders.
- All dataclass fields have defaults so partial objects can be constructed easily.
- Helper functions handle ID generation, change detection, and round-trip
  serialization to/from plain dicts.
"""

from __future__ import annotations

import hashlib
import json
import copy
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class ObjectType(str, Enum):
    """All distinct Canvas UI object types the crawler can encounter."""

    DASHBOARD = "dashboard"
    DASHBOARD_COURSE_CARD = "dashboard_course_card"
    DASHBOARD_TODO_ITEM = "dashboard_todo_item"
    DASHBOARD_RECENT_ACTIVITY = "dashboard_recent_activity"
    COURSE_HOME = "course_home"
    COURSE_NAV_ENTRY = "course_nav_entry"
    MODULE = "module"
    MODULE_ITEM = "module_item"
    PAGE = "page"
    READING = "reading"
    LECTURE_NOTE = "lecture_note"
    ASSIGNMENT = "assignment"
    RUBRIC = "rubric"
    QUIZ = "quiz"
    QUIZ_SUMMARY = "quiz_summary"
    QUIZ_ATTEMPT = "quiz_attempt"
    ANNOUNCEMENT = "announcement"
    DISCUSSION = "discussion"
    DISCUSSION_REPLY_SURFACE = "discussion_reply_surface"
    FILE = "file"
    MEDIA = "media"
    SYLLABUS = "syllabus"
    EXTERNAL_URL = "external_url"
    EXTERNAL_TOOL = "external_tool"
    EXTERNAL_TOOL_STUB = "external_tool_stub"
    GRADE_SIGNAL = "grade_signal"
    CALENDAR_ITEM = "calendar_item"
    CALENDAR_UNDATED = "calendar_undated"
    GROUP_CONTEXT = "group_context"
    UNKNOWN = "unknown"


class EducationalRole(str, Enum):
    """High-level pedagogical role of a Canvas object."""

    WEEKLY_OVERVIEW = "weekly_overview"
    CORE_INSTRUCTION = "core_instruction"
    READING = "reading"
    LECTURE = "lecture"
    ASSIGNMENT_DIRECTIONS = "assignment_directions"
    GRADING_CRITERIA = "grading_criteria"
    ASSESSMENT_INSTRUCTIONS = "assessment_instructions"
    DISCUSSION_PROMPT = "discussion_prompt"
    ADMINISTRATIVE_NOTICE = "administrative_notice"
    SCHEDULE_SIGNAL = "schedule_signal"
    OPTIONAL_REFERENCE = "optional_reference"
    PEER_REVIEW_WORKFLOW = "peer_review_workflow"
    GROUP_WORKFLOW = "group_workflow"
    PROCTORING_NOTICE = "proctoring_notice"
    UNRESOLVED_LAUNCH = "unresolved_launch"
    LOW_VALUE_NAVIGATION = "low_value_navigation"


class AssignmentSubtype(str, Enum):
    """Submission mechanism for an assignment."""

    FILE_UPLOAD = "file_upload"
    TEXT_ENTRY = "text_entry"
    NO_SUBMISSION = "no_submission"
    WEBSITE_URL = "website_url"
    PEER_REVIEW = "peer_review"
    GROUP = "group"
    UNKNOWN = "unknown"


class DiscussionTag(str, Enum):
    """Flags that describe the state or structure of a discussion."""

    PINNED = "pinned"
    CLOSED_FOR_COMMENTS = "closed_for_comments"
    SUBSCRIBED_CAPABLE = "subscribed_capable"
    THREADED_REPLIES = "threaded_replies"
    GRADED = "graded"
    UNGRADED = "ungraded"


class QuizTag(str, Enum):
    """Flags that describe the configuration or state of a quiz."""

    TIMED = "timed"
    UNTIMED = "untimed"
    DUE_DATE_WARNING = "due_date_warning"
    AUTOSUBMIT = "autosubmit"
    FLAG_QUESTIONS = "flag_questions"
    PROCTORED_CANDIDATE = "proctored_candidate"


class SourceMode(str, Enum):
    """How the raw page content was captured."""

    LIVE = "live"
    MHTML_EXPORT = "mhtml_export"
    SAVED_PAGE_ZIP = "saved_page_zip"
    ATTACHED_FILE = "attached_file"


class CaptureCompleteness(str, Enum):
    """How completely the crawler was able to extract content from a page."""

    FULL = "full"
    PARTIAL = "partial"
    MINIMAL = "minimal"


class RelationType(str, Enum):
    """Typed directed edges in the Canvas knowledge graph."""

    COURSE_CONTAINS_MODULE = "course_contains_module"
    MODULE_CONTAINS_ITEM = "module_contains_item"
    MODULE_ITEM_RESOLVES_TO = "module_item_resolves_to"
    PAGE_SUPPORTS_ASSIGNMENT = "page_supports_assignment"
    PAGE_SUPPORTS_QUIZ = "page_supports_quiz"
    ANNOUNCEMENT_UPDATES_ASSIGNMENT = "announcement_updates_assignment"
    ANNOUNCEMENT_UPDATES_DUE_DATE = "announcement_updates_due_date"
    DISCUSSION_BELONGS_TO_MODULE = "discussion_belongs_to_module"
    RUBRIC_BELONGS_TO_ASSIGNMENT = "rubric_belongs_to_assignment"
    PEER_REVIEW_FOLLOWS_SUBMISSION = "peer_review_follows_submission"
    GROUP_CONTEXT_APPLIES_TO = "group_context_applies_to"
    CALENDAR_REFERS_TO = "calendar_refers_to"
    GRADE_SIGNAL_REFERS_TO = "grade_signal_refers_to"
    EXTERNAL_TOOL_REFERS_TO = "external_tool_refers_to"
    QUIZ_HAS_PROCTORING_NOTICE = "quiz_has_proctoring_notice"


# ---------------------------------------------------------------------------
# Subsidiary dataclasses
# ---------------------------------------------------------------------------

@dataclass
class RubricCriterion:
    """A single grading criterion within a Canvas rubric."""

    criterion_id: str = ""
    name: str = ""
    description: str = ""
    ratings: list[dict] = field(default_factory=list)  # [{description, points}]
    max_points: float = 0.0
    free_form: bool = False


@dataclass
class StructuredSection:
    """A headed section extracted from a Canvas page body."""

    heading: str = ""
    level: int = 1          # heading depth: 1 = h1, 2 = h2, …
    body: str = ""
    items: list[str] = field(default_factory=list)  # bullet / numbered list items


@dataclass
class StatusSignal:
    """A completion or risk signal observed on a Canvas page or module."""

    signal_type: str = ""
    # "completed" | "overdue" | "not_started" | "locked" | "missing" | "graded" | "ungraded"
    value: str = ""
    source: str = ""
    # "module_icon" | "grade_page" | "assignment_page" | etc.


# ---------------------------------------------------------------------------
# Main record
# ---------------------------------------------------------------------------

@dataclass
class CanvasObject:
    """
    Normalized representation of any object the Canvas crawler can capture.

    Fields are grouped into:
      Identity, Source provenance, Classification, Structure/position,
      Dates, Content, Assignment specifics, Quiz specifics, Discussion
      specifics, Grade signals, Priority scores, Learning outputs,
      Versioning, and Raw capture storage.
    """

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------
    id: str = ""
    course_id: str = ""
    course_name: str = ""
    term: str = ""

    # ------------------------------------------------------------------
    # Source provenance
    # ------------------------------------------------------------------
    source_mode: str = SourceMode.LIVE.value
    source_file_name: str = ""
    source_url: str = ""
    canonical_url: str = ""
    referrer_url: str = ""

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------
    object_type: str = ObjectType.UNKNOWN.value
    secondary_tags: list[str] = field(default_factory=list)
    educational_role: str = ""
    assignment_subtype: str = ""
    discussion_tags: list[str] = field(default_factory=list)
    quiz_tags: list[str] = field(default_factory=list)
    quiz_mode: str = ""         # "summary" | "attempt" | "unknown"
    is_external_tool_stub: bool = False
    capture_completeness: str = CaptureCompleteness.FULL.value

    # ------------------------------------------------------------------
    # Structure / position
    # ------------------------------------------------------------------
    title: str = ""
    module_name: str = ""
    module_order: int = -1
    item_order: int = -1
    week_label: str = ""

    # ------------------------------------------------------------------
    # Dates
    # ------------------------------------------------------------------
    posted_date: str = ""       # ISO8601 or ""
    due_date: str = ""
    available_from: str = ""
    available_until: str = ""

    # ------------------------------------------------------------------
    # Content
    # ------------------------------------------------------------------
    main_content: str = ""
    structured_sections: list[StructuredSection] = field(default_factory=list)
    attachments: list[dict] = field(default_factory=list)
    rubric: list[RubricCriterion] = field(default_factory=list)
    status_signals: list[StatusSignal] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Assignment / submission specifics
    # ------------------------------------------------------------------
    point_value: float = 0.0
    submission_attempts: int = 0
    submission_state: str = ""
    # "submitted" | "unsubmitted" | "graded" | "pending_review"

    # ------------------------------------------------------------------
    # Quiz specifics
    # ------------------------------------------------------------------
    question_count: int = 0
    time_limit_minutes: int = 0
    allowed_attempts: int = 0

    # ------------------------------------------------------------------
    # Discussion specifics
    # ------------------------------------------------------------------
    reply_count: int = 0
    unread_count: int = 0

    # ------------------------------------------------------------------
    # Grade signals
    # ------------------------------------------------------------------
    current_grade: str = ""
    total_grade: str = ""
    grade_weight_group: str = ""
    missing_risk: bool = False

    # ------------------------------------------------------------------
    # Scores (set by priority engine)
    # ------------------------------------------------------------------
    importance_score: float = 0.0
    urgency_score: float = 0.0
    study_value_score: float = 0.0
    risk_score: float = 0.0
    confidence_score: float = 1.0

    # ------------------------------------------------------------------
    # Learning outputs (set by learning layer)
    # ------------------------------------------------------------------
    summary_short: str = ""
    summary_detailed: str = ""
    key_concepts: list[str] = field(default_factory=list)
    important_terms: list[dict] = field(default_factory=list)   # [{term, definition}]
    action_items: list[str] = field(default_factory=list)
    study_questions: list[str] = field(default_factory=list)
    ambiguity_notes: list[str] = field(default_factory=list)
    dates_and_deadlines: list[dict] = field(default_factory=list)  # [{label, date}]
    likely_linked_assignments: list[str] = field(default_factory=list)
    likely_exam_relevance: str = ""

    # ------------------------------------------------------------------
    # Versioning / change detection
    # ------------------------------------------------------------------
    change_hash: str = ""
    version: int = 1
    created_at: str = ""
    updated_at: str = ""

    # ------------------------------------------------------------------
    # Raw capture storage
    # ------------------------------------------------------------------
    raw_html: str = ""          # capped at 100 KB by the crawler
    page_title: str = ""


# ---------------------------------------------------------------------------
# Graph edge
# ---------------------------------------------------------------------------

@dataclass
class GraphEdge:
    """A directed, typed relationship between two CanvasObjects in the knowledge graph."""

    edge_id: str = ""
    from_id: str = ""
    to_id: str = ""
    relation_type: str = ""     # RelationType value
    confidence: float = 1.0
    evidence: list[str] = field(default_factory=list)
    created_at: str = ""


# ---------------------------------------------------------------------------
# Change record
# ---------------------------------------------------------------------------

@dataclass
class ChangeRecord:
    """
    A record of a detected change between two crawl snapshots of the same object.

    Used by the change-detection layer to flag items needing restudy or replanning.
    """

    change_id: str = ""
    object_id: str = ""
    course_id: str = ""
    change_type: str = ""
    # "new_item" | "edited_content" | "due_date_changed" | "rubric_changed"
    # "new_announcement" | "module_reordered" | "new_file" | "proctoring_notice"
    change_severity: str = ""   # "critical" | "high" | "medium" | "low"
    before_hash: str = ""
    after_hash: str = ""
    before_snapshot: str = ""   # JSON of changed fields, before
    after_snapshot: str = ""    # JSON of changed fields, after
    restudy_flag: bool = False
    replan_flag: bool = False
    detected_at: str = ""


# ---------------------------------------------------------------------------
# Helper functions — ID generation
# ---------------------------------------------------------------------------

def make_object_id(course_id: str, object_type: str, url: str) -> str:
    """Return a stable deterministic ID for a Canvas object."""
    key = f"{course_id}:{object_type}:{url}"
    return f"co_{hashlib.sha256(key.encode()).hexdigest()[:20]}"


def make_edge_id(from_id: str, to_id: str, relation_type: str) -> str:
    """Return a stable deterministic ID for a graph edge."""
    key = f"{from_id}:{to_id}:{relation_type}"
    return f"edge_{hashlib.sha256(key.encode()).hexdigest()[:16]}"


# ---------------------------------------------------------------------------
# Helper functions — change detection
# ---------------------------------------------------------------------------

def make_change_hash(obj: CanvasObject) -> str:
    """
    Return a short hash of the fields that matter for change detection.

    Only a 2000-character prefix of main_content is used to keep the hash
    stable across minor whitespace differences in long bodies.
    """
    tracked = {
        "main_content": obj.main_content[:2000],
        "due_date": obj.due_date,
        "available_from": obj.available_from,
        "available_until": obj.available_until,
        "point_value": obj.point_value,
        "title": obj.title,
        "rubric_count": len(obj.rubric),
    }
    return hashlib.sha256(json.dumps(tracked, sort_keys=True).encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Helper functions — serialization round-trip
# ---------------------------------------------------------------------------

# Fields whose list values are JSON-stringified when stored as ChromaDB metadata.
_LIST_FIELDS_FOR_CHROMA = (
    "structured_sections",
    "attachments",
    "rubric",
    "status_signals",
    "key_concepts",
    "important_terms",
    "action_items",
    "study_questions",
    "ambiguity_notes",
    "dates_and_deadlines",
    "likely_linked_assignments",
    "secondary_tags",
    "discussion_tags",
    "quiz_tags",
)


def canvas_object_to_dict(obj: CanvasObject) -> dict:
    """
    Convert a CanvasObject to a flat dict suitable for JSON serialization
    and ChromaDB metadata storage.

    Lists of dataclasses / dicts / strings are JSON-stringified so that
    ChromaDB (which requires scalar metadata values) can store them without
    a custom encoder.
    """
    d = dataclasses.asdict(obj)
    for key in _LIST_FIELDS_FOR_CHROMA:
        if isinstance(d.get(key), list):
            d[key] = json.dumps(d[key])
    return d


def canvas_object_from_dict(d: dict) -> CanvasObject:
    """
    Reconstruct a CanvasObject from a stored dict (inverse of canvas_object_to_dict).

    Unknown keys are silently dropped for forward-compatibility when new fields
    are added in later schema versions.
    """
    d = copy.deepcopy(d)
    for key in _LIST_FIELDS_FOR_CHROMA:
        if isinstance(d.get(key), str):
            try:
                d[key] = json.loads(d[key])
            except Exception:
                d[key] = []
    known = {f.name for f in dataclasses.fields(CanvasObject)}
    d = {k: v for k, v in d.items() if k in known}
    return CanvasObject(**d)


# ---------------------------------------------------------------------------
# Helper functions — timestamps
# ---------------------------------------------------------------------------

def now_iso() -> str:
    """Return the current UTC time as an ISO8601 string."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Helper functions — heuristic role inference
# ---------------------------------------------------------------------------

def infer_educational_role(
    obj_type: str,
    secondary_tags: list[str],
    title: str = "",
) -> str:
    """
    Quick heuristic to assign an EducationalRole value from object type and tags.

    This is used when the crawler cannot determine the role from richer signals.
    The learning layer may override this value after content analysis.
    """
    t = obj_type.lower()
    title_l = title.lower()

    if t == ObjectType.ASSIGNMENT.value:
        return EducationalRole.ASSIGNMENT_DIRECTIONS.value
    if t in (ObjectType.QUIZ.value, ObjectType.QUIZ_SUMMARY.value):
        return EducationalRole.ASSESSMENT_INSTRUCTIONS.value
    if t == ObjectType.DISCUSSION.value:
        return EducationalRole.DISCUSSION_PROMPT.value
    if t == ObjectType.ANNOUNCEMENT.value:
        return EducationalRole.ADMINISTRATIVE_NOTICE.value
    if t == ObjectType.RUBRIC.value:
        return EducationalRole.GRADING_CRITERIA.value
    if t == ObjectType.SYLLABUS.value:
        return EducationalRole.SCHEDULE_SIGNAL.value
    if t in (ObjectType.EXTERNAL_TOOL.value, ObjectType.EXTERNAL_TOOL_STUB.value):
        return EducationalRole.UNRESOLVED_LAUNCH.value
    if t in (ObjectType.DASHBOARD.value, ObjectType.COURSE_NAV_ENTRY.value):
        return EducationalRole.LOW_VALUE_NAVIGATION.value
    if any(kw in title_l for kw in ("week", "module", "overview", "introduction", "intro")):
        return EducationalRole.WEEKLY_OVERVIEW.value
    if any(kw in title_l for kw in ("reading", "read", "textbook", "chapter")):
        return EducationalRole.READING.value
    if any(kw in title_l for kw in ("lecture", "notes", "slides", "slide")):
        return EducationalRole.LECTURE.value

    return EducationalRole.CORE_INSTRUCTION.value
