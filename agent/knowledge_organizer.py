"""
Knowledge Organizer
Dedicated pipeline service for the student learning system.

Runs in two sequential phases after every crawl or manual document upload:

  Phase A — Note Generation
    For each unprocessed assignment, document chunk, and grade entry:
      - Call AI to generate structured study notes
      - Store in ChromaDB ai_notes / instructor_patterns collections
      - Mark source ID in processed_docs.json so reruns are skipped

  Phase B — Knowledge Organization
    After notes exist:
      - Cluster notes into coherent topics per course (AI)
      - Synthesize a comprehensive topic overview for each topic (AI)
      - Extract atomic concept definitions per topic (AI → JSON)
      - Store topics and concepts in ChromaDB
      - Write data/knowledge/topic_map.json (hierarchical JSON index)
"""

import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agent.knowledge_base import KnowledgeBase

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
PROCESSED_DOCS_PATH = DATA_DIR / "processed_docs.json"
TOPIC_MAP_PATH = DATA_DIR / "knowledge" / "topic_map.json"

# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

_NOTE_PROMPTS = {
    "assignment_analysis": (
        "You are a diligent student. Analyze this assignment and produce structured study notes.\n\n"
        "Format your response exactly as:\n"
        "WHAT IS ASKED: (plain-English summary of the task)\n"
        "KEY REQUIREMENTS:\n- ...\n"
        "RUBRIC BREAKDOWN:\n- [criterion name]: what excellent work looks like; common mistakes to avoid\n"
        "STRATEGY: your recommended approach\n"
        "QUESTIONS TO CLARIFY: any ambiguities before starting\n\n"
        "Assignment:\n{content}"
    ),
    "document_summary": (
        "You are a diligent student. Read this course material and write comprehensive study notes.\n\n"
        "Format your response exactly as:\n"
        "MAIN TOPIC: (one sentence describing what this covers)\n"
        "KEY CONCEPTS:\n- [concept]: definition\n"
        "IMPORTANT POINTS:\n- ...\n"
        "LEARNING OBJECTIVES: what you should understand after reading this\n"
        "CONNECTIONS: how this relates to broader course themes\n\n"
        "Material:\n{content}"
    ),
    "course_content_summary": (
        "You are a student reviewing course information. Summarize what matters.\n\n"
        "Format your response exactly as:\n"
        "OVERVIEW: (what type of content this is and its purpose)\n"
        "KEY INFORMATION:\n- ...\n"
        "ACTION ITEMS: anything requiring student action or attention\n\n"
        "Content:\n{content}"
    ),
    "grade_pattern": (
        "You are analyzing a graded assignment to understand instructor preferences.\n\n"
        "Format your response exactly as:\n"
        "SCORE: (from the grade data)\n"
        "WHAT LIKELY WORKED: reasons points were probably earned\n"
        "INSTRUCTOR PREFERENCES: patterns this reveals about what the instructor values\n"
        "APPLY TO FUTURE: specific guidance for future assignments in this course\n\n"
        "Grade entry:\n{content}"
    ),
}

_CONCEPT_EXTRACTION_PROMPT = """\
You are a student indexing key academic concepts from your study notes.

Extract 3-8 concepts that are central to understanding the material.
Return a JSON array only — no other text.

Each object must have exactly these keys:
  "concept"    : the term or concept name (string)
  "definition" : one-sentence plain-English definition (string)
  "importance" : integer 1-5 (5 = most important)

Study notes:
{notes}"""

_TOPIC_CLUSTERING_PROMPT = """\
You are organizing study notes into coherent topics for a course.

Course: {course_name}
Notes available (format: [note_type] Title):
{note_list}

Group these notes into 3-8 coherent topics. Return a JSON array only — no other text.

Each object must have exactly these keys:
  "topic_name" : short descriptive topic name (string)
  "summary"    : one-sentence description of what this topic covers (string)
  "notes"      : list of note titles that belong to this topic (list of strings)"""

_TOPIC_SYNTHESIS_PROMPT = """\
You are a student synthesizing everything you know about a topic into a comprehensive study guide.

Topic: {topic_name}
Course: {course_name}

Related notes:
{related_content}

Write a thorough synthesis covering:
OVERVIEW: What this topic is and why it matters in the course
KEY CONCEPTS: The most important ideas (bullet list with brief explanations)
DETAILED UNDERSTANDING: In-depth explanation of the main ideas and how they connect
EXAM/ASSIGNMENT RELEVANCE: What aspects you are most likely to need for work or exams
OPEN QUESTIONS: Anything still unclear or worth researching further"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _note_id(source_id: str, note_type: str) -> str:
    h = hashlib.sha256(f"{source_id}:{note_type}".encode()).hexdigest()[:16]
    return f"note_{h}"


def _topic_id(course_id: str, topic_name: str) -> str:
    h = hashlib.sha256(f"{course_id}:{topic_name}".encode()).hexdigest()[:16]
    return f"topic_{h}"


def _concept_id(topic_id: str, concept_name: str) -> str:
    h = hashlib.sha256(f"{topic_id}:{concept_name}".encode()).hexdigest()[:12]
    return f"concept_{h}"


def _load_processed_ids() -> set:
    if PROCESSED_DOCS_PATH.exists():
        try:
            data = json.loads(PROCESSED_DOCS_PATH.read_text(encoding="utf-8"))
            return set(data.get("processed_ids", []))
        except Exception:
            return set()
    return set()


def _save_processed_ids(ids: set) -> None:
    PROCESSED_DOCS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = PROCESSED_DOCS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps({"processed_ids": list(ids)}, indent=2), encoding="utf-8")
    tmp.replace(PROCESSED_DOCS_PATH)


# ---------------------------------------------------------------------------
# KnowledgeOrganizer
# ---------------------------------------------------------------------------

class KnowledgeOrganizer:
    """
    Coordinates the two-phase knowledge pipeline.
    Instantiated once in main.py and reused across requests.
    """

    def __init__(self, kb: "KnowledgeBase"):
        self.kb = kb

    # ------------------------------------------------------------------
    # Internal AI call (reuses brain's _call_api / _extract_text)
    # ------------------------------------------------------------------

    def _call_ai(self, system: str, user: str, max_tokens: int = 1200) -> str:
        # Import here to avoid circular imports at module load time
        from agent.brain import _call_api, _extract_text  # noqa: PLC0415
        notes_model = os.environ.get("NOTES_MODEL", "")
        if notes_model:
            # Temporarily override the active model for this call
            import agent.brain as brain_module  # noqa: PLC0415
            original = brain_module._AI_MODEL
            brain_module._AI_MODEL = notes_model
            try:
                resp = _call_api(system=system,
                                 messages=[{"role": "user", "content": user}],
                                 max_tokens=max_tokens)
            finally:
                brain_module._AI_MODEL = original
        else:
            resp = _call_api(system=system,
                             messages=[{"role": "user", "content": user}],
                             max_tokens=max_tokens)
        return _extract_text(resp)

    # ------------------------------------------------------------------
    # Phase A — Note generation
    # ------------------------------------------------------------------

    def _generate_note(self, source_id: str, content: str, note_type: str,
                       title: str, course_name: str) -> str:
        """Synchronous note generation — called via run_in_executor."""
        prompt_template = _NOTE_PROMPTS.get(note_type, _NOTE_PROMPTS["document_summary"])
        prompt = prompt_template.format(content=content[:4000])
        system = (
            f"You are a student at Wilmington University enrolled in {course_name!r}. "
            "Write clear, organized study notes for your personal knowledge base. "
            "Be thorough but concise."
        )
        return self._call_ai(system=system, user=prompt, max_tokens=1400)

    async def run_note_generation(self, loop: asyncio.AbstractEventLoop | None = None) -> dict:
        """
        Phase A: scan all collections for unprocessed source documents,
        generate AI study notes for each one, persist to ChromaDB.
        Returns a stats dict.
        """
        if loop is None:
            loop = asyncio.get_running_loop()

        processed_ids = _load_processed_ids()
        newly_processed: set = set()
        stats = {"assignments": 0, "documents": 0, "grades": 0, "errors": 0}

        # --- Assignments ---
        for item in self.kb.get_all_assignments():
            doc_id = item.get("id", "")
            if not doc_id or doc_id in processed_ids:
                continue
            meta = item.get("metadata", {})
            try:
                note_text = await loop.run_in_executor(None, partial(
                    self._generate_note,
                    doc_id, item.get("document", ""),
                    "assignment_analysis",
                    meta.get("title", "Unknown Assignment"),
                    meta.get("course_name", ""),
                ))
                self.kb.save_ai_note(_note_id(doc_id, "assignment_analysis"), note_text, {
                    "note_type": "assignment_analysis",
                    "source_doc_id": doc_id,
                    "source_collection": "assignments",
                    "course_id": meta.get("course_id", ""),
                    "course_name": meta.get("course_name", ""),
                    "title": meta.get("title", ""),
                    "generated_at": datetime.now().isoformat(),
                })
                newly_processed.add(doc_id)
                stats["assignments"] += 1
                logger.debug(f"Generated assignment note for: {meta.get('title', doc_id)}")
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Assignment note generation failed [{doc_id}]: {e}")
                stats["errors"] += 1

        # --- Documents (first chunk per document only) ---
        for item in self.kb.get_documents_first_chunks():
            doc_id = item.get("id", "")
            if not doc_id or doc_id in processed_ids:
                continue
            meta = item.get("metadata", {})
            try:
                note_text = await loop.run_in_executor(None, partial(
                    self._generate_note,
                    doc_id, item.get("document", ""),
                    "document_summary",
                    meta.get("title", "Unknown Document"),
                    meta.get("course_name", ""),
                ))
                self.kb.save_ai_note(_note_id(doc_id, "document_summary"), note_text, {
                    "note_type": "document_summary",
                    "source_doc_id": doc_id,
                    "source_collection": "documents",
                    "course_id": meta.get("course_id", ""),
                    "course_name": meta.get("course_name", ""),
                    "title": meta.get("title", ""),
                    "generated_at": datetime.now().isoformat(),
                })
                newly_processed.add(doc_id)
                stats["documents"] += 1
                logger.debug(f"Generated document note for: {meta.get('title', doc_id)}")
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Document note generation failed [{doc_id}]: {e}")
                stats["errors"] += 1

        # --- Grade entries → instructor_patterns ---
        try:
            grade_results = self.kb.course_content.get(
                where={"type": "grade"},
                include=["documents", "metadatas"],
            )
            for i, doc in enumerate(grade_results.get("documents") or []):
                meta = grade_results["metadatas"][i]
                doc_id = grade_results["ids"][i]
                if doc_id in processed_ids:
                    continue
                if str(meta.get("score", "-")).strip() == "-":
                    continue  # skip ungraded
                try:
                    note_text = await loop.run_in_executor(None, partial(
                        self._generate_note,
                        doc_id, doc,
                        "grade_pattern",
                        meta.get("assignment", "Grade Entry"),
                        meta.get("course_name", ""),
                    ))
                    self.kb.instructor_patterns.upsert(
                        ids=[f"pattern_{doc_id}"],
                        documents=[note_text],
                        metadatas=[{**meta, "pattern_type": "grade_based",
                                    "generated_at": datetime.now().isoformat()}],
                    )
                    newly_processed.add(doc_id)
                    stats["grades"] += 1
                    await asyncio.sleep(1.0)
                except Exception as e:
                    logger.error(f"Grade pattern generation failed [{doc_id}]: {e}")
                    stats["errors"] += 1
        except Exception as e:
            logger.error(f"Grade fetch failed: {e}")

        _save_processed_ids(processed_ids | newly_processed)
        logger.info(f"Phase A complete — notes generated: {stats}")
        return stats

    # ------------------------------------------------------------------
    # Phase B — Knowledge organization
    # ------------------------------------------------------------------

    async def run_organization(self, loop: asyncio.AbstractEventLoop | None = None) -> dict:
        """
        Phase B: cluster notes into topics, synthesize topic overviews,
        extract concepts, write topic_map.json.
        """
        if loop is None:
            loop = asyncio.get_running_loop()

        stats = {"courses_processed": 0, "topics_created": 0, "concepts_extracted": 0, "errors": 0}

        all_notes = self.kb.get_all_notes()
        if not all_notes:
            logger.info("Phase B skipped — no notes available yet")
            return stats

        # Group notes by course
        courses: dict = {}
        for note in all_notes:
            meta = note.get("metadata", {})
            cname = meta.get("course_name", "")
            cid = meta.get("course_id", "")
            if not cname:
                continue
            if cname not in courses:
                courses[cname] = {"course_id": cid, "notes": []}
            courses[cname]["notes"].append(note)

        topic_map: dict = {"generated_at": datetime.now().isoformat(), "courses": {}}

        for course_name, course_data in courses.items():
            notes = course_data["notes"]
            course_id = course_data["course_id"]

            if len(notes) < 2:
                # Not enough notes to meaningfully cluster
                continue

            # Build the note list string for clustering
            note_list = "\n".join(
                f"[{n['metadata'].get('note_type', 'note')}] {n['metadata'].get('title', 'Untitled')}"
                for n in notes
            )

            # --- Cluster notes into topics ---
            try:
                cluster_json = await loop.run_in_executor(None, partial(
                    self._call_ai,
                    f"You are organizing study materials for the course: {course_name!r}.",
                    _TOPIC_CLUSTERING_PROMPT.format(
                        course_name=course_name, note_list=note_list
                    ),
                    800,
                ))
                # Strip potential markdown code fences
                cluster_json = cluster_json.strip()
                if cluster_json.startswith("```"):
                    cluster_json = cluster_json.split("```")[1]
                    if cluster_json.startswith("json"):
                        cluster_json = cluster_json[4:]
                clusters = json.loads(cluster_json)
            except Exception as e:
                logger.error(f"Topic clustering failed for {course_name!r}: {e}")
                stats["errors"] += 1
                continue

            await asyncio.sleep(1.0)
            topic_map["courses"][course_name] = {"topics": {}}

            for cluster in clusters:
                topic_name = cluster.get("topic_name", "General")
                topic_summary = cluster.get("summary", "")
                topic_note_titles = set(cluster.get("notes", []))

                # Match titles to actual note objects
                related_notes = [
                    n for n in notes
                    if n["metadata"].get("title", "") in topic_note_titles
                ]
                if not related_notes:
                    related_notes = notes[:4]  # fallback

                # Build content block for synthesis
                related_content = "\n\n---\n\n".join(
                    f"[{n['metadata'].get('note_type', '')}] "
                    f"{n['metadata'].get('title', 'Untitled')}\n"
                    f"{n['document'][:900]}"
                    for n in related_notes[:6]
                )

                # --- Synthesize topic overview ---
                try:
                    synthesis = await loop.run_in_executor(None, partial(
                        self._call_ai,
                        (f"You are a student at Wilmington University synthesizing your knowledge "
                         f"about {topic_name!r} from the course {course_name!r}. "
                         "Write a comprehensive, well-organized study guide."),
                        _TOPIC_SYNTHESIS_PROMPT.format(
                            topic_name=topic_name,
                            course_name=course_name,
                            related_content=related_content,
                        ),
                        1600,
                    ))
                except Exception as e:
                    logger.error(f"Topic synthesis failed [{topic_name}]: {e}")
                    synthesis = topic_summary
                    stats["errors"] += 1

                tid = _topic_id(course_id, topic_name)
                self.kb.save_topic(tid, synthesis, {
                    "topic_name": topic_name,
                    "course_name": course_name,
                    "course_id": course_id,
                    "summary": topic_summary,
                    "note_count": len(related_notes),
                    "generated_at": datetime.now().isoformat(),
                })
                stats["topics_created"] += 1
                await asyncio.sleep(1.0)

                # --- Extract concepts for this topic ---
                notes_text = "\n\n".join(
                    n["document"][:700] for n in related_notes[:4]
                )
                try:
                    concepts_json = await loop.run_in_executor(None, partial(
                        self._call_ai,
                        "You extract key academic concepts from student study notes. Return valid JSON only.",
                        _CONCEPT_EXTRACTION_PROMPT.format(notes=notes_text[:3500]),
                        700,
                    ))
                    concepts_json = concepts_json.strip()
                    if concepts_json.startswith("```"):
                        concepts_json = concepts_json.split("```")[1]
                        if concepts_json.startswith("json"):
                            concepts_json = concepts_json[4:]
                    concepts = json.loads(concepts_json)

                    for c in concepts:
                        concept_name = c.get("concept", "").strip()
                        if not concept_name:
                            continue
                        cid_str = _concept_id(tid, concept_name)
                        text = f"{concept_name}: {c.get('definition', '')}"
                        self.kb.save_concept(cid_str, text, {
                            "concept_name": concept_name,
                            "topic_id": tid,
                            "topic_name": topic_name,
                            "course_name": course_name,
                            "course_id": course_id,
                            "importance": int(c.get("importance", 3)),
                            "generated_at": datetime.now().isoformat(),
                        })
                        stats["concepts_extracted"] += 1

                    await asyncio.sleep(0.8)
                except Exception as e:
                    logger.error(f"Concept extraction failed [{topic_name}]: {e}")
                    stats["errors"] += 1

                topic_map["courses"][course_name]["topics"][topic_name] = {
                    "summary": topic_summary,
                    "note_count": len(related_notes),
                    "topic_id": tid,
                }

            stats["courses_processed"] += 1

        # Write topic map JSON
        TOPIC_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
        TOPIC_MAP_PATH.write_text(
            json.dumps(topic_map, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info(f"Phase B complete — {stats}")
        return stats

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    async def run_full_pipeline(self) -> dict:
        """
        Run Phase A then Phase B sequentially.
        Designed to be launched as a fire-and-forget asyncio task.
        """
        loop = asyncio.get_running_loop()
        logger.info("Knowledge pipeline started")
        try:
            phase_a = await self.run_note_generation(loop=loop)
        except Exception as e:
            logger.error(f"Phase A failed: {e}")
            phase_a = {"error": str(e)}

        await asyncio.sleep(2.0)

        try:
            phase_b = await self.run_organization(loop=loop)
        except Exception as e:
            logger.error(f"Phase B failed: {e}")
            phase_b = {"error": str(e)}

        logger.info("Knowledge pipeline complete")
        return {"phase_a": phase_a, "phase_b": phase_b}
