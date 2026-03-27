"""
change_detector.py — Detects and records changes between crawl snapshots.

On every recrawl, each CanvasObject is compared to its previously-stored
version using change_hash and key field diffs.  Detected changes are returned
as ChangeRecord instances so the learning layer can flag items for restudy.

Usage:
    detector = ChangeDetector()
    record = detector.detect(new_obj, kb)
    if record:
        # Store change record; the learning layer will re-process flagged items
        kb.upsert_canvas_object(new_obj)  # upsert handles versioning
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Optional

from agent.canvas_schema import (
    CanvasObject,
    ChangeRecord,
    make_change_hash,
    now_iso,
)

logger = logging.getLogger(__name__)


def _change_id(object_id: str, detected_at: str) -> str:
    key = f"{object_id}:{detected_at}"
    return f"chg_{hashlib.sha256(key.encode()).hexdigest()[:16]}"


def _snapshot(obj: CanvasObject) -> str:
    """Return a compact JSON snapshot of change-tracked fields."""
    return json.dumps({
        "title": obj.title,
        "due_date": obj.due_date,
        "main_content_prefix": obj.main_content[:500],
        "point_value": obj.point_value,
        "rubric_count": len(obj.rubric),
        "available_from": obj.available_from,
        "available_until": obj.available_until,
    }, sort_keys=True)


class ChangeDetector:
    """
    Compares incoming CanvasObjects against the previously-stored version
    in ChromaDB and emits ChangeRecord instances for detected differences.

    The returned ChangeRecord (if any) should be stored separately by the
    caller; this class does NOT write to the knowledge base.
    """

    def detect(self, new_obj: CanvasObject, kb) -> Optional[ChangeRecord]:
        """
        Compare new_obj against the stored version.

        Returns:
          - None if the object is unchanged
          - ChangeRecord if the object is new or has changed
        """
        existing = kb.get_canvas_object(new_obj.id)

        if existing is None:
            return self._record_new(new_obj)

        # Object exists — check change_hash first (fast path)
        new_hash = make_change_hash(new_obj)
        if existing.change_hash == new_hash:
            return None  # No detectable change

        # Hashes differ — determine what changed
        return self._record_diff(existing, new_obj, new_hash)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record_new(self, obj: CanvasObject) -> ChangeRecord:
        detected_at = now_iso()
        return ChangeRecord(
            change_id=_change_id(obj.id, detected_at),
            object_id=obj.id,
            course_id=obj.course_id,
            change_type="new_item",
            change_severity="medium",
            before_hash="",
            after_hash=make_change_hash(obj),
            before_snapshot="{}",
            after_snapshot=_snapshot(obj),
            restudy_flag=False,
            replan_flag=False,
            detected_at=detected_at,
        )

    def _record_diff(
        self,
        old_obj: CanvasObject,
        new_obj: CanvasObject,
        new_hash: str,
    ) -> ChangeRecord:
        detected_at = now_iso()

        change_type = "edited_content"
        severity = "medium"
        restudy = False
        replan = False

        # Check specific high-signal fields in priority order
        if old_obj.due_date != new_obj.due_date:
            change_type = "due_date_changed"
            severity = "critical"
            restudy = True
            replan = True
            logger.info(
                f"[change_detector] due_date_changed: {old_obj.id} "
                f"'{old_obj.due_date}' → '{new_obj.due_date}'"
            )
        elif len(old_obj.rubric) != len(new_obj.rubric):
            change_type = "rubric_changed"
            severity = "high"
            restudy = True
            logger.info(f"[change_detector] rubric_changed: {old_obj.id}")
        elif old_obj.main_content[:500] != new_obj.main_content[:500]:
            change_type = "edited_content"
            severity = "high"
            restudy = True
            logger.info(f"[change_detector] edited_content: {old_obj.id}")
        elif old_obj.title != new_obj.title:
            change_type = "edited_content"
            severity = "medium"
            restudy = True
        elif old_obj.point_value != new_obj.point_value:
            change_type = "edited_content"
            severity = "high"
            restudy = True
            replan = True
        elif old_obj.module_order != new_obj.module_order:
            change_type = "module_reordered"
            severity = "low"

        return ChangeRecord(
            change_id=_change_id(new_obj.id, detected_at),
            object_id=new_obj.id,
            course_id=new_obj.course_id,
            change_type=change_type,
            change_severity=severity,
            before_hash=old_obj.change_hash,
            after_hash=new_hash,
            before_snapshot=_snapshot(old_obj),
            after_snapshot=_snapshot(new_obj),
            restudy_flag=restudy,
            replan_flag=replan,
            detected_at=detected_at,
        )

    def detect_batch(
        self, objects: list[CanvasObject], kb
    ) -> list[ChangeRecord]:
        """
        Run detect() for each object and return all non-None results.

        Updates each object's change_hash before returning so callers
        don't need to recompute it.
        """
        records: list[ChangeRecord] = []
        for obj in objects:
            obj.change_hash = make_change_hash(obj)
            record = self.detect(obj, kb)
            if record:
                records.append(record)
        logger.info(
            f"[change_detector] {len(records)} change(s) detected "
            f"from {len(objects)} objects"
        )
        return records
