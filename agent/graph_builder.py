"""
graph_builder.py — Infers typed graph edges between CanvasObjects.

After normalization, every Canvas object lives in isolation.  This module
builds the knowledge graph by connecting objects via typed GraphEdge instances.

Edge inference is purely heuristic (no AI): it matches IDs, URLs, titles, and
structural signals already captured by the normalizer.

Usage:
    builder = GraphBuilder()
    edges = builder.build_from_objects(all_canvas_objects)
    for edge in edges:
        kb.upsert_graph_edge(edge)
"""

from __future__ import annotations

import logging
from typing import Sequence

from agent.canvas_schema import (
    CanvasObject,
    GraphEdge,
    ObjectType,
    RelationType,
    make_edge_id,
    now_iso,
)

logger = logging.getLogger(__name__)


def _make_edge(
    from_id: str,
    to_id: str,
    relation_type: str,
    confidence: float = 0.9,
    evidence: list[str] | None = None,
) -> GraphEdge:
    return GraphEdge(
        edge_id=make_edge_id(from_id, to_id, relation_type),
        from_id=from_id,
        to_id=to_id,
        relation_type=relation_type,
        confidence=confidence,
        evidence=evidence or [],
        created_at=now_iso(),
    )


class GraphBuilder:
    """
    Builds GraphEdge relationships from a list of CanvasObjects.

    The input list should contain all objects for one or more courses.
    Edges are inferred per-course (objects from different courses are
    never linked unless a calendar item explicitly references them).
    """

    def build_from_objects(self, objects: Sequence[CanvasObject]) -> list[GraphEdge]:
        """
        Infer all edges from the provided object list.
        Returns a deduplicated list of GraphEdge instances.
        """
        edges: dict[str, GraphEdge] = {}  # edge_id → edge (dedup by ID)

        # Index objects for fast lookup
        by_id: dict[str, CanvasObject] = {o.id: o for o in objects}
        by_url: dict[str, CanvasObject] = {}
        for o in objects:
            if o.canonical_url:
                by_url[o.canonical_url.rstrip("/")] = o
            if o.source_url:
                by_url[o.source_url.rstrip("/")] = o

        # Group by course for within-course relationship inference
        by_course: dict[str, list[CanvasObject]] = {}
        for o in objects:
            by_course.setdefault(o.course_id, []).append(o)

        for course_id, course_objects in by_course.items():
            new_edges = self._build_course_edges(course_objects, by_id, by_url)
            for e in new_edges:
                edges[e.edge_id] = e

        result = list(edges.values())
        logger.info(f"[graph_builder] Built {len(result)} edges from {len(objects)} objects")
        return result

    def _build_course_edges(
        self,
        objects: list[CanvasObject],
        by_id: dict[str, CanvasObject],
        by_url: dict[str, CanvasObject],
    ) -> list[GraphEdge]:
        edges: list[GraphEdge] = []

        # Indexes for this course
        modules: dict[str, CanvasObject] = {}   # module_name → module object
        assignments: dict[str, CanvasObject] = {}  # title (lower) → assignment
        course_home: CanvasObject | None = None

        for o in objects:
            if o.object_type == ObjectType.MODULE.value:
                modules[o.title.lower()] = o
                if o.course_id:
                    # Will emit COURSE_CONTAINS_MODULE after we find course home
                    pass
            elif o.object_type == ObjectType.ASSIGNMENT.value:
                assignments[o.title.lower()] = o
            elif o.object_type == ObjectType.COURSE_HOME.value:
                course_home = o

        # ------------------------------------------------------------------
        # 1. COURSE_CONTAINS_MODULE
        # ------------------------------------------------------------------
        if course_home:
            for mod_obj in modules.values():
                edges.append(_make_edge(
                    course_home.id, mod_obj.id,
                    RelationType.COURSE_CONTAINS_MODULE.value,
                    confidence=1.0,
                    evidence=["course_home → module structural relationship"],
                ))

        # ------------------------------------------------------------------
        # 2. MODULE_CONTAINS_ITEM  (module item → item's parent module)
        # ------------------------------------------------------------------
        for o in objects:
            if o.object_type == ObjectType.MODULE_ITEM.value and o.module_name:
                parent_mod = modules.get(o.module_name.lower())
                if parent_mod:
                    edges.append(_make_edge(
                        parent_mod.id, o.id,
                        RelationType.MODULE_CONTAINS_ITEM.value,
                        confidence=0.95,
                        evidence=[f"module_name='{o.module_name}'"],
                    ))

        # ------------------------------------------------------------------
        # 3. MODULE_ITEM_RESOLVES_TO_OBJECT
        #    Match module items by canonical_url to assignments/quizzes/etc.
        # ------------------------------------------------------------------
        for o in objects:
            if o.object_type == ObjectType.MODULE_ITEM.value and o.canonical_url:
                resolved = by_url.get(o.canonical_url.rstrip("/"))
                if resolved and resolved.id != o.id:
                    edges.append(_make_edge(
                        o.id, resolved.id,
                        RelationType.MODULE_ITEM_RESOLVES_TO.value,
                        confidence=0.95,
                        evidence=[f"canonical_url match: {o.canonical_url}"],
                    ))

        # ------------------------------------------------------------------
        # 4. DISCUSSION_BELONGS_TO_MODULE
        # ------------------------------------------------------------------
        for o in objects:
            if o.object_type == ObjectType.DISCUSSION.value and o.module_name:
                parent_mod = modules.get(o.module_name.lower())
                if parent_mod:
                    edges.append(_make_edge(
                        parent_mod.id, o.id,
                        RelationType.DISCUSSION_BELONGS_TO_MODULE.value,
                        confidence=0.9,
                        evidence=[f"module_name='{o.module_name}'"],
                    ))

        # ------------------------------------------------------------------
        # 5. RUBRIC_BELONGS_TO_ASSIGNMENT
        # ------------------------------------------------------------------
        for o in objects:
            if o.object_type == ObjectType.ASSIGNMENT.value and o.rubric:
                # The rubric is embedded in the assignment — emit a self-referential
                # edge marking that this assignment has an explicit grading rubric.
                edges.append(_make_edge(
                    o.id, o.id,
                    RelationType.RUBRIC_BELONGS_TO_ASSIGNMENT.value,
                    confidence=1.0,
                    evidence=[f"{len(o.rubric)} rubric criteria attached"],
                ))

        # ------------------------------------------------------------------
        # 6. ANNOUNCEMENT_UPDATES_ASSIGNMENT / ANNOUNCEMENT_UPDATES_DUE_DATE
        #    Fuzzy title match between announcement body and assignment titles
        # ------------------------------------------------------------------
        for o in objects:
            if o.object_type == ObjectType.ANNOUNCEMENT.value and o.main_content:
                body_l = o.main_content.lower()
                for atitle_l, assign_obj in assignments.items():
                    # Match if 4+ consecutive words from assignment title appear in body
                    words = atitle_l.split()
                    if len(words) >= 2 and atitle_l in body_l:
                        rel = (
                            RelationType.ANNOUNCEMENT_UPDATES_DUE_DATE.value
                            if any(kw in body_l for kw in ("due", "deadline", "extended"))
                            else RelationType.ANNOUNCEMENT_UPDATES_ASSIGNMENT.value
                        )
                        edges.append(_make_edge(
                            o.id, assign_obj.id,
                            rel,
                            confidence=0.7,
                            evidence=[f"assignment title '{assign_obj.title}' found in announcement body"],
                        ))

        # ------------------------------------------------------------------
        # 7. PAGE_SUPPORTS_ASSIGNMENT
        #    If a page's title or body mentions an assignment title
        # ------------------------------------------------------------------
        for o in objects:
            if o.object_type == ObjectType.PAGE.value and o.main_content:
                body_l = o.main_content.lower()
                for atitle_l, assign_obj in assignments.items():
                    if len(atitle_l) >= 8 and atitle_l in body_l:
                        edges.append(_make_edge(
                            o.id, assign_obj.id,
                            RelationType.PAGE_SUPPORTS_ASSIGNMENT.value,
                            confidence=0.6,
                            evidence=[f"assignment title '{assign_obj.title}' mentioned in page body"],
                        ))

        # ------------------------------------------------------------------
        # 8. CALENDAR_ITEM_REFERS_TO_OBJECT
        #    Match calendar items to assignments/quizzes by title
        # ------------------------------------------------------------------
        for o in objects:
            if o.object_type in (
                ObjectType.CALENDAR_ITEM.value,
                ObjectType.CALENDAR_UNDATED.value,
            ):
                title_l = o.title.lower()
                for atitle_l, assign_obj in assignments.items():
                    if atitle_l and title_l and (atitle_l in title_l or title_l in atitle_l):
                        edges.append(_make_edge(
                            o.id, assign_obj.id,
                            RelationType.CALENDAR_REFERS_TO.value,
                            confidence=0.8,
                            evidence=[f"title match: '{o.title}' ~ '{assign_obj.title}'"],
                        ))

        # ------------------------------------------------------------------
        # 9. GRADE_SIGNAL_REFERS_TO_ASSIGNMENT
        # ------------------------------------------------------------------
        for o in objects:
            if o.object_type == ObjectType.GRADE_SIGNAL.value and o.main_content:
                body_l = o.main_content.lower()
                for atitle_l, assign_obj in assignments.items():
                    if len(atitle_l) >= 6 and atitle_l in body_l:
                        edges.append(_make_edge(
                            o.id, assign_obj.id,
                            RelationType.GRADE_SIGNAL_REFERS_TO.value,
                            confidence=0.7,
                            evidence=[f"assignment '{assign_obj.title}' referenced in grade signal"],
                        ))

        return edges
