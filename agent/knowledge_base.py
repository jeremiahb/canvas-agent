"""
Knowledge Base
Stores and retrieves Canvas content using ChromaDB.

Review fixes applied:
  - RF-chr92      : obfuscated chr(92) regex replaced with proper raw string
  - RF-ImpLog     : improvement log persistence handled via load/save helpers
                    (called by main.py on startup and after every log_event)
  - RF-UpcomingDB : get_upcoming_assignments uses a ChromaDB where-clause
                    instead of fetching all documents and filtering in Python
  - RF-IngestPath : ingest_knowledge default path resolves via DATA_DIR
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Optional

import chromadb
from chromadb.utils import embedding_functions

logger = logging.getLogger(__name__)


def _default_chroma_dir() -> str:
    data_dir = os.environ.get("DATA_DIR", "data")
    return str(Path(data_dir) / "knowledge" / "chroma")


def _build_where(course_name=None, note_type=None) -> dict:
    """Build a ChromaDB where-filter supporting optional course_name and note_type."""
    if course_name and note_type:
        return {"$and": [{"course_name": course_name}, {"note_type": note_type}]}
    if course_name:
        return {"course_name": course_name}
    if note_type:
        return {"note_type": note_type}
    return {}


def _zip_results(r: dict) -> list:
    """Zip ChromaDB get() ids/documents/metadatas into a list of dicts."""
    ids = r.get("ids") or []
    docs = r.get("documents") or []
    metas = r.get("metadatas") or []
    return [{"id": ids[i], "document": docs[i], "metadata": metas[i]} for i in range(len(ids))]


def _structured_to_text(s: dict) -> str:
    """Convert structured_elements dict to a searchable plain-text summary."""
    parts = []
    for h in s.get("headings", []):
        level = h.get("level", 2) if isinstance(h, dict) else 2
        text = h.get("text", h) if isinstance(h, dict) else str(h)
        parts.append(f"{'#' * level} {text}")
    for tbl in s.get("tables", []):
        if isinstance(tbl, list):
            parts.append("[TABLE]\n" + "\n".join(" | ".join(str(c) for c in row) for row in tbl))
        else:
            parts.append(f"[TABLE]\n{tbl}")
    for item in s.get("lists", []):
        parts.append(f"• {item}")
    for code in s.get("code_blocks", []):
        parts.append(f"```\n{code[:500]}\n```")
    # Slides
    for slide in s.get("slides", []):
        if isinstance(slide, dict):
            parts.append(f"[Slide {slide.get('slide','')}] {slide.get('title','')} — {slide.get('body','')[:300]}")
    # Spreadsheet schema
    schema = s.get("schema", {})
    if isinstance(schema, dict):
        for sheet, info in schema.items():
            cols = info.get("columns", []) if isinstance(info, dict) else []
            parts.append(f"[Sheet: {sheet}] Columns: {', '.join(str(c) for c in cols)}")
    return "\n\n".join(p for p in parts if p.strip())


def _chunk_text(text: str, max_chars: int = 6000) -> list[str]:
    """
    Split text into semantically coherent chunks of at most max_chars.
    Breaks on paragraph boundaries where possible; hard-splits oversized
    paragraphs.
    """
    if len(text) <= max_chars:
        return [text]

    chunks: list[str] = []
    paragraphs = text.split("\n\n")
    current = ""

    for para in paragraphs:
        if len(current) + len(para) + 2 <= max_chars:
            current = (current + "\n\n" + para).strip()
        else:
            if current:
                chunks.append(current)
            if len(para) > max_chars:
                for i in range(0, len(para), max_chars):
                    chunks.append(para[i:i + max_chars])
                current = ""
            else:
                current = para

    if current:
        chunks.append(current)

    return chunks


class KnowledgeBase:
    def __init__(self, persist_dir: str = ""):
        """
        persist_dir: path to ChromaDB storage directory.
        Defaults to DATA_DIR/knowledge/chroma.
        """
        if not persist_dir:
            persist_dir = _default_chroma_dir()
        Path(persist_dir).mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=persist_dir)
        self.ef = embedding_functions.DefaultEmbeddingFunction()

        self.assignments = self._get_or_create("assignments")
        self.course_content = self._get_or_create("course_content")
        self.documents = self._get_or_create("documents")
        self.flagged_links = self._get_or_create("flagged_links")
        self.voice_profile = self._get_or_create("voice_profile")
        self.instructor_patterns = self._get_or_create("instructor_patterns")
        # Learning system collections
        self.ai_notes = self._get_or_create("ai_notes")
        self.topics = self._get_or_create("topics")
        self.concepts = self._get_or_create("concepts")
        self.chat_history = self._get_or_create("chat_history")

    def _get_or_create(self, name: str):
        return self.client.get_or_create_collection(
            name=name,
            embedding_function=self.ef,
        )

    # ------------------------------------------------------------------ #
    #  Ingest from crawler output                                          #
    # ------------------------------------------------------------------ #

    def ingest_knowledge(self, knowledge_path: str = "") -> int:
        """
        Parse the crawler JSON snapshot and upsert all content into ChromaDB.
        RF-IngestPath: default path resolves via DATA_DIR env var.
        """
        if not knowledge_path:
            data_dir = os.environ.get("DATA_DIR", "data")
            knowledge_path = str(
                Path(data_dir) / "knowledge" / "canvas_knowledge.json"
            )

        logger.debug(f"[ingest_knowledge] Loading knowledge snapshot from: {knowledge_path}")
        with open(knowledge_path) as f:
            data = json.load(f)

        courses = data.get("courses", [])
        logger.debug(f"[ingest_knowledge] Snapshot contains {len(courses)} courses — ingesting into ChromaDB")
        total_docs = 0

        for course in courses:
            course_id = str(course.get("id", "unknown"))
            course_name = course.get("name", "Unknown Course")
            logger.debug(f"[ingest_knowledge] Processing course: {course_name} (id={course_id})")

            # --- Course content batch ---
            content_ids: list[str] = []
            content_docs: list[str] = []
            content_metas: list[dict] = []

            syllabus = course.get("syllabus", "").strip()
            if syllabus:
                content_ids.append(f"syllabus_{course_id}")
                content_docs.append(syllabus)
                content_metas.append({
                    "type": "syllabus",
                    "course_id": course_id,
                    "course_name": course_name,
                })

            for i, ann in enumerate(course.get("announcements", [])):
                title = ann.get("title", "").strip()
                if not title:
                    continue
                content_ids.append(f"announcement_{course_id}_{i}")
                content_docs.append(title)
                content_metas.append({
                    "type": "announcement",
                    "course_id": course_id,
                    "course_name": course_name,
                    "date": ann.get("date", ""),
                })

            for idx, mod in enumerate(course.get("modules", [])):
                # RF-chr92: raw string, not chr(92) + 'W+'
                slug = re.sub(r"\W+", "_", mod.get("name", ""))[:30]
                mod_id = f"module_{course_id}_{idx}_{slug}"
                items_str = ", ".join(
                    item.get("title", "") for item in mod.get("items", [])
                )
                content_ids.append(mod_id)
                content_docs.append(f"Module: {mod['name']}\nItems: {items_str}")
                content_metas.append({
                    "type": "module",
                    "course_id": course_id,
                    "course_name": course_name,
                    "module_name": mod.get("name", ""),
                })

            for grade in course.get("grades", []):
                assignment_name = grade.get("assignment", "").strip()
                if not assignment_name:
                    continue
                # RF-chr92: raw string throughout
                slug = re.sub(r"\W+", "_", assignment_name)[:40]
                content_ids.append(f"grade_{course_id}_{slug}")
                content_docs.append(
                    f"Grade for {assignment_name}: "
                    f"{grade.get('score', '-')} / {grade.get('possible', '-')}"
                )
                content_metas.append({
                    "type": "grade",
                    "course_id": course_id,
                    "course_name": course_name,
                    "assignment": assignment_name,
                    "score": grade.get("score", "-"),
                    "possible": grade.get("possible", "-"),
                })

            if content_ids:
                logger.debug(f"[ingest_knowledge] Upserting {len(content_ids)} course_content docs for {course_name}")
                self.course_content.upsert(
                    ids=content_ids,
                    documents=content_docs,
                    metadatas=content_metas,
                )
                total_docs += len(content_ids)

            # --- Documents batch ---
            doc_ids: list[str] = []
            doc_texts: list[str] = []
            doc_metas: list[dict] = []

            for idx, doc in enumerate(course.get("documents", [])):
                text = doc.get("text", "").strip()
                if not text:
                    continue

                chunks = _chunk_text(text, max_chars=6000)
                for chunk_idx, chunk in enumerate(chunks):
                    # RF-chr92: raw string slug
                    slug = re.sub(r"\W+", "_", doc.get("title", ""))[:30]
                    doc_id = f"doc_{course_id}_{idx}_{chunk_idx}_{slug}"
                    doc_ids.append(doc_id)
                    doc_texts.append(chunk)
                    doc_metas.append({
                        "type": "document",
                        "doc_type": doc.get("doc_type", "unknown"),
                        "title": doc.get("title", ""),
                        "url": doc.get("url", ""),
                        "course_id": course_id,
                        "course_name": course_name,
                        "source": doc.get("source", ""),
                        "module_name": doc.get("module_name", ""),
                        "chunk": chunk_idx,
                        "chunk_count": len(chunks),
                        # Intake pipeline classification metadata (populated when available)
                        "content_type": doc.get("content_type", ""),
                        "input_type": doc.get("input_type", ""),
                        "intent": doc.get("intent", ""),
                        "classifier_confidence": float(doc.get("classifier_confidence", 0.0)),
                        "source_label": doc.get("source", ""),
                        "canonical_url": doc.get("url", ""),
                    })

            if doc_ids:
                logger.debug(f"[ingest_knowledge] Upserting {len(doc_ids)} document chunks for {course_name}")
                self.documents.upsert(
                    ids=doc_ids,
                    documents=doc_texts,
                    metadatas=doc_metas,
                )
                total_docs += len(doc_ids)

            # --- Flagged external links ---
            flag_ids: list[str] = []
            flag_texts: list[str] = []
            flag_metas: list[dict] = []

            for idx, flag in enumerate(course.get("flagged_external", [])):
                # RF-chr92: raw string slug
                slug = re.sub(r"\W+", "_", flag.get("title", ""))[:40]
                flag_ids.append(f"flag_{course_id}_{idx}_{slug}")
                flag_texts.append(
                    f"External resource: {flag.get('title', '')}\n"
                    f"Platform: {flag.get('platform', '')}\n"
                    f"URL: {flag.get('url', '')}\n"
                    f"Note: {flag.get('note', '')}"
                )
                flag_metas.append({
                    "type": "flagged_external",
                    "title": flag.get("title", ""),
                    "url": flag.get("url", ""),
                    "platform": flag.get("platform", ""),
                    "course_id": course_id,
                    "course_name": course_name,
                    "uploaded": False,
                })

            if flag_ids:
                self.flagged_links.upsert(
                    ids=flag_ids,
                    documents=flag_texts,
                    metadatas=flag_metas,
                )
                total_docs += len(flag_ids)

            # --- Assignments batch ---
            assign_ids: list[str] = []
            assign_docs: list[str] = []
            assign_metas: list[dict] = []

            for assignment in course.get("assignments", []):
                aid = str(assignment.get("id", ""))
                if not aid:
                    continue

                details = assignment.get("details") or {}
                rubric_json = json.dumps(details.get("rubric", []))
                submission_types = ", ".join(details.get("submission_types", []))

                doc = (
                    f"Assignment: {assignment.get('title', '')}\n"
                    f"Course: {course_name}\n"
                    f"Due: {assignment.get('due', 'No due date')}\n"
                    f"Points: {assignment.get('points', 'N/A')}\n"
                    f"Description: {details.get('description', '')}\n"
                    f"Submission Types: {submission_types}\n"
                    f"Rubric: {rubric_json}"
                ).strip()

                # Preserve existing status so approved/submitted work survives re-crawls
                existing = self.get_assignment_by_id(aid)
                current_status = existing["metadata"].get("status", "pending") if existing else "pending"

                assign_ids.append(f"assignment_{course_id}_{aid}")
                assign_docs.append(doc)
                assign_metas.append({
                    "type": "assignment",
                    "assignment_id": aid,
                    "course_id": course_id,
                    "course_name": course_name,
                    "title": assignment.get("title", ""),
                    "due": assignment.get("due", "No due date"),
                    "points": assignment.get("points", ""),
                    "url": assignment.get("url", ""),
                    "status": current_status,
                })

            if assign_ids:
                logger.debug(f"[ingest_knowledge] Upserting {len(assign_ids)} assignments for {course_name}")
                self.assignments.upsert(
                    ids=assign_ids,
                    documents=assign_docs,
                    metadatas=assign_metas,
                )
                total_docs += len(assign_ids)

            logger.debug(f"[ingest_knowledge] Course {course_name} done — running total: {total_docs} docs")

        logger.info(f"Ingested {total_docs} documents into knowledge base")
        return total_docs

    # ------------------------------------------------------------------ #
    #  Query                                                               #
    # ------------------------------------------------------------------ #

    def search_documents(self, query: str, n: int = 5) -> list:
        """Search full reading and document content."""
        count = self.documents.count()
        if count == 0:
            return []
        results = self.documents.query(
            query_texts=[query],
            n_results=min(n, count),
        )
        return self._format_results(results)

    def get_flagged_links(self, course_id: Optional[str] = None) -> list:
        """Return all flagged external links, optionally filtered by course."""
        count = self.flagged_links.count()
        if count == 0:
            return []
        if course_id:
            results = self.flagged_links.get(
                where={"course_id": course_id},
                include=["documents", "metadatas"],
            )
        else:
            results = self.flagged_links.get(include=["documents", "metadatas"])
        return [
            {"document": doc, "metadata": results["metadatas"][i]}
            for i, doc in enumerate(results.get("documents", []))
        ]

    def add_manual_document(
        self,
        title: str,
        text: str,
        course_name: str,
        course_id: str = "",
        url: str = "",
    ) -> str:
        """
        Store a manually uploaded document (e.g. a VitalSource reading).
        Returns the base document ID.
        """
        chunks = _chunk_text(text, max_chars=6000)
        # RF-chr92: raw string slug
        slug = re.sub(r"\W+", "_", title)[:40]
        base_id = f"manual_{course_id}_{slug}"

        ids, texts, metas = [], [], []
        for i, chunk in enumerate(chunks):
            ids.append(f"{base_id}_{i}")
            texts.append(chunk)
            metas.append({
                "type": "document",
                "doc_type": "manual_upload",
                "title": title,
                "url": url,
                "course_id": course_id,
                "course_name": course_name,
                "source": "manual",
                "chunk": i,
                "total_chunks": len(chunks),
            })

        self.documents.upsert(ids=ids, documents=texts, metadatas=metas)
        logger.info(f"Stored manual document '{title}' in {len(chunks)} chunks")
        return base_id

    def get_documents_by_course(self, course_name: Optional[str] = None) -> list:
        """
        Return all indexed documents, optionally filtered by course name.
        Returns deduplicated list (only chunk 0 per document) for display.
        """
        count = self.documents.count()
        if count == 0:
            return []
        try:
            if course_name:
                results = self.documents.get(
                    where={"course_name": course_name},
                    include=["documents", "metadatas"],
                )
            else:
                results = self.documents.get(include=["documents", "metadatas"])
        except Exception as e:
            logger.error(f"Error fetching documents: {e}")
            return []

        docs = []
        seen_titles = set()
        for i, doc in enumerate(results.get("documents", [])):
            meta = results["metadatas"][i] if results.get("metadatas") else {}
            # Only show first chunk per document to avoid duplicates
            if meta.get("chunk", 0) != 0:
                continue
            key = (meta.get("title", ""), meta.get("course_name", ""))
            if key in seen_titles:
                continue
            seen_titles.add(key)
            docs.append({"document": doc, "metadata": meta})
        return docs

    def get_course_names(self) -> list[str]:
        """Return sorted list of all unique course names across all collections."""
        names = set()
        for collection in [self.documents, self.assignments, self.course_content]:
            try:
                count = collection.count()
                if count == 0:
                    continue
                results = collection.get(include=["metadatas"])
                for meta in results.get("metadatas", []):
                    name = meta.get("course_name", "")
                    if name:
                        names.add(name)
            except Exception:
                pass
        return sorted(names)

    def search_documents_by_course(self, query: str, course_name: Optional[str] = None, n: int = 5) -> list:
        """Search documents, optionally scoped to one course."""
        count = self.documents.count()
        if count == 0:
            return []
        try:
            if course_name:
                results = self.documents.query(
                    query_texts=[query],
                    n_results=min(n, count),
                    where={"course_name": course_name},
                )
            else:
                results = self.documents.query(
                    query_texts=[query],
                    n_results=min(n, count),
                )
            return self._format_results(results)
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            return []

    def search_course_content_by_course(self, query: str, course_name: Optional[str] = None, n: int = 5) -> list:
        """Search course content (syllabi, modules, grades), optionally scoped to one course."""
        count = self.course_content.count()
        if count == 0:
            return []
        try:
            if course_name:
                results = self.course_content.query(
                    query_texts=[query],
                    n_results=min(n, count),
                    where={"course_name": course_name},
                )
            else:
                results = self.course_content.query(
                    query_texts=[query],
                    n_results=min(n, count),
                )
            return self._format_results(results)
        except Exception as e:
            logger.error(f"Error searching course content: {e}")
            return []

    def search_assignments(self, query: str, course_name: Optional[str] = None, n: int = 5) -> list:
        """Semantic search over assignment documents, optionally scoped to one course."""
        count = self.assignments.count()
        if count == 0:
            return []
        try:
            if course_name:
                results = self.assignments.query(
                    query_texts=[query],
                    n_results=min(n, count),
                    where={"course_name": course_name},
                )
            else:
                results = self.assignments.query(
                    query_texts=[query],
                    n_results=min(n, count),
                )
            return self._format_results(results)
        except Exception as e:
            logger.error(f"Error searching assignments: {e}")
            return []

    def search_course_content(self, query: str, n: int = 5) -> list:
        """Semantic search over syllabi, modules, announcements, and grades."""
        count = self.course_content.count()
        if count == 0:
            return []
        results = self.course_content.query(
            query_texts=[query],
            n_results=min(n, count),
        )
        return self._format_results(results)

    def get_assignment_by_id(self, assignment_id: str) -> Optional[dict]:
        """
        Exact lookup by assignment_id metadata field.
        Never uses semantic search so the correct assignment is always returned.
        Returns the document, metadata, AND the actual ChromaDB document ID so
        callers (e.g. update_assignment_status) never need to reconstruct it.
        """
        try:
            results = self.assignments.get(
                where={"assignment_id": str(assignment_id)},
                include=["documents", "metadatas"],
            )
        except Exception as e:
            logger.error(f"Error fetching assignment {assignment_id}: {e}")
            return None

        if not results.get("documents"):
            return None

        return {
            "document": results["documents"][0],
            "metadata": results["metadatas"][0],
            # RF-StatusID: return the real Chroma document ID so update_assignment_status
            # never has to reconstruct it and risk a format mismatch
            "chroma_id": results["ids"][0],
        }

    def get_all_assignments(self) -> list:
        """Return all assignment documents and metadata (includes ChromaDB id)."""
        count = self.assignments.count()
        if count == 0:
            return []
        results = self.assignments.get(include=["documents", "metadatas", "ids"])
        return [
            {"id": results["ids"][i], "document": doc, "metadata": results["metadatas"][i]}
            for i, doc in enumerate(results["documents"])
        ]

    def get_upcoming_assignments(self) -> list:
        """
        Return all non-submitted assignments.
        RF-UpcomingDB: ChromaDB server-side filter instead of Python-side filter
        over a full collection fetch, which avoids deserialising discarded docs.
        """
        count = self.assignments.count()
        if count == 0:
            return []
        try:
            results = self.assignments.get(
                where={"status": {"$ne": "submitted"}},
                include=["documents", "metadatas"],
            )
            return [
                {"document": doc, "metadata": results["metadatas"][i]}
                for i, doc in enumerate(results.get("documents", []))
            ]
        except Exception as e:
            # Some ChromaDB versions don't support $ne on all backends
            # fall back to Python-side filter rather than crashing
            logger.warning(f"ChromaDB where-filter failed, using Python filter: {e}")
            return [
                a for a in self.get_all_assignments()
                if a["metadata"].get("status") != "submitted"
            ]

    def update_assignment_status(self, assignment_id: str, status: str) -> None:
        """
        Persist a status change (pending -> approved -> submitted) to ChromaDB.
        RF-StatusID: uses the actual ChromaDB document ID returned by
        get_assignment_by_id() so the upsert is guaranteed to target the correct
        document regardless of any future ID format changes.
        """
        existing = self.get_assignment_by_id(assignment_id)
        if not existing:
            logger.warning(f"Cannot update status: assignment {assignment_id} not found")
            return

        meta = {**existing["metadata"], "status": status}
        # RF-StatusID: use the real ID, never reconstruct it
        self.assignments.upsert(
            ids=[existing["chroma_id"]],
            documents=[existing["document"]],
            metadatas=[meta],
        )

    def _format_results(self, results) -> list:
        """Normalise ChromaDB query results into a consistent list of dicts."""
        if not results.get("documents") or not results["documents"][0]:
            return []
        return [
            {
                "document": doc,
                "metadata": results["metadatas"][0][i] if results.get("metadatas") else {},
                "distance": results["distances"][0][i] if results.get("distances") else None,
            }
            for i, doc in enumerate(results["documents"][0])
        ]

    # ------------------------------------------------------------------ #
    #  Voice profile                                                       #
    # ------------------------------------------------------------------ #

    def add_voice_sample(self, sample_id: str, text: str, metadata: Optional[dict] = None) -> None:
        """Store a writing sample or style rule for voice matching."""
        safe_meta = metadata if metadata is not None else {}
        self.voice_profile.upsert(
            ids=[sample_id],
            documents=[text],
            metadatas=[{**safe_meta, "type": "voice_sample"}],
        )

    def get_voice_samples(self) -> list:
        """Return all stored voice samples as plain text strings."""
        count = self.voice_profile.count()
        if count == 0:
            return []
        results = self.voice_profile.get(include=["documents"])
        return results.get("documents", [])

    # ------------------------------------------------------------------ #
    #  Stats                                                               #
    # ------------------------------------------------------------------ #

    def stats(self) -> dict:
        """Return document counts for each collection."""
        return {
            "assignments": self.assignments.count(),
            "course_content": self.course_content.count(),
            "documents": self.documents.count(),
            "flagged_external": self.flagged_links.count(),
            "voice_samples": self.voice_profile.count(),
            "instructor_patterns": self.instructor_patterns.count(),
            "ai_notes": self.ai_notes.count(),
            "topics": self.topics.count(),
            "concepts": self.concepts.count(),
            "chat_history": self.chat_history.count(),
        }

    # ------------------------------------------------------------------ #
    #  AI Notes                                                            #
    # ------------------------------------------------------------------ #

    def save_ai_note(self, note_id: str, text: str, metadata: dict) -> None:
        """Upsert an AI-generated note linked to a source document."""
        self.ai_notes.upsert(ids=[note_id], documents=[text], metadatas=[metadata])

    def get_all_notes(self, course_name: Optional[str] = None, note_type: Optional[str] = None) -> list:
        """Return all AI notes, optionally filtered by course or note_type."""
        count = self.ai_notes.count()
        if count == 0:
            return []
        where = _build_where(course_name=course_name, note_type=note_type)
        kwargs: dict = {"include": ["documents", "metadatas"]}
        if where:
            kwargs["where"] = where
        try:
            return _zip_results(self.ai_notes.get(**kwargs))
        except Exception as e:
            logger.error(f"Error fetching notes: {e}")
            return []

    def search_ai_notes(self, query: str, course_name: Optional[str] = None,
                        note_type: Optional[str] = None, n: int = 10) -> list:
        """Semantic search over AI notes."""
        count = self.ai_notes.count()
        if count == 0:
            return []
        where = _build_where(course_name=course_name, note_type=note_type)
        kwargs: dict = {"query_texts": [query], "n_results": min(n, count)}
        if where:
            kwargs["where"] = where
        try:
            return self._format_results(self.ai_notes.query(**kwargs))
        except Exception as e:
            logger.error(f"Error searching notes: {e}")
            return []

    def get_notes_for_source(self, source_doc_id: str) -> list:
        """Return all AI notes linked to a specific source document."""
        try:
            r = self.ai_notes.get(
                where={"source_doc_id": source_doc_id},
                include=["documents", "metadatas"],
            )
            return _zip_results(r)
        except Exception as e:
            logger.error(f"Error fetching notes for {source_doc_id}: {e}")
            return []

    def get_documents_first_chunks(self, course_name: Optional[str] = None) -> list:
        """Return chunk=0 entries only — one per source document — with ChromaDB ids."""
        count = self.documents.count()
        if count == 0:
            return []
        try:
            if course_name:
                where = {"$and": [{"chunk": 0}, {"course_name": course_name}]}
            else:
                where = {"chunk": 0}
            r = self.documents.get(where=where, include=["documents", "metadatas"])
            return _zip_results(r)
        except Exception as e:
            logger.error(f"Error fetching document first chunks: {e}")
            return []

    # ------------------------------------------------------------------ #
    #  Topics                                                              #
    # ------------------------------------------------------------------ #

    def save_topic(self, topic_id: str, text: str, metadata: dict) -> None:
        """Upsert a synthesized topic overview."""
        self.topics.upsert(ids=[topic_id], documents=[text], metadatas=[metadata])

    def get_all_topics(self, course_name: Optional[str] = None) -> list:
        """Return all topics, optionally filtered by course."""
        count = self.topics.count()
        if count == 0:
            return []
        kwargs: dict = {"include": ["documents", "metadatas"]}
        if course_name:
            kwargs["where"] = {"course_name": course_name}
        try:
            return _zip_results(self.topics.get(**kwargs))
        except Exception as e:
            logger.error(f"Error fetching topics: {e}")
            return []

    def search_topics(self, query: str, course_name: Optional[str] = None, n: int = 5) -> list:
        """Semantic search over synthesized topic overviews."""
        count = self.topics.count()
        if count == 0:
            return []
        kwargs: dict = {"query_texts": [query], "n_results": min(n, count)}
        if course_name:
            kwargs["where"] = {"course_name": course_name}
        try:
            return self._format_results(self.topics.query(**kwargs))
        except Exception as e:
            logger.error(f"Error searching topics: {e}")
            return []

    # ------------------------------------------------------------------ #
    #  Concepts                                                            #
    # ------------------------------------------------------------------ #

    def save_concept(self, concept_id: str, text: str, metadata: dict) -> None:
        """Upsert an atomic concept definition."""
        self.concepts.upsert(ids=[concept_id], documents=[text], metadatas=[metadata])

    def get_concepts_for_topic(self, topic_id: str) -> list:
        """Return all concepts linked to a topic."""
        try:
            r = self.concepts.get(
                where={"topic_id": topic_id},
                include=["documents", "metadatas"],
            )
            return _zip_results(r)
        except Exception as e:
            logger.error(f"Error fetching concepts for {topic_id}: {e}")
            return []

    def search_concepts(self, query: str, course_name: Optional[str] = None, n: int = 8) -> list:
        """Semantic search over extracted concepts."""
        count = self.concepts.count()
        if count == 0:
            return []
        kwargs: dict = {"query_texts": [query], "n_results": min(n, count)}
        if course_name:
            kwargs["where"] = {"course_name": course_name}
        try:
            return self._format_results(self.concepts.query(**kwargs))
        except Exception as e:
            logger.error(f"Error searching concepts: {e}")
            return []

    # ------------------------------------------------------------------ #
    #  Chat History                                                        #
    # ------------------------------------------------------------------ #

    def save_chat_message(self, message_id: str, role: str, content: str, metadata: dict) -> None:
        """Persist a single chat turn permanently."""
        self.chat_history.upsert(
            ids=[message_id],
            documents=[content],
            metadatas=[{"role": role, **metadata}],
        )

    def get_recent_chat_history(self, n: int = 40) -> list:
        """Return the n most recent chat messages, sorted newest first."""
        count = self.chat_history.count()
        if count == 0:
            return []
        try:
            r = self.chat_history.get(
                include=["documents", "metadatas"],
                limit=min(n * 2, count),  # fetch extra then sort+trim
            )
            items = _zip_results(r)
            items.sort(key=lambda x: x["metadata"].get("timestamp", ""), reverse=True)
            return items[:n]
        except Exception as e:
            logger.error(f"Error fetching chat history: {e}")
            return []

    # ------------------------------------------------------------------ #
    #  Intake Pipeline Integration                                         #
    # ------------------------------------------------------------------ #

    def store_intake_record(self, record) -> str:
        """
        Store a normalized IntakeRecord from the intake pipeline.

        - Skips 'ignore' records (login walls, media stubs, etc.)
        - Skips exact duplicates (returns canonical ID)
        - Chunks cleaned_text into the documents collection
        - Also upserts a separate structured-elements summary document
        - Returns the base unique_id
        """
        from agent.intake_pipeline import IntakeRecord  # lazy import avoids circular

        if record.recommended_use == "ignore":
            logger.debug(f"[kb.store] Skipping ignore-tagged record {record.unique_id}")
            return record.unique_id

        if record.dedup_result == "exact_duplicate":
            logger.debug(f"[kb.store] Exact dup — skipping {record.unique_id}")
            return record.dedup_canonical_id or record.unique_id

        text_to_chunk = record.cleaned_text or record.raw_text
        chunks = _chunk_text(text_to_chunk)

        # Build the base metadata dict — ChromaDB requires scalar values only
        base_meta: dict = {
            "source_url":    record.canonical_source,
            "canonical_url": record.canonical_source,
            "referring_page": record.metadata.get("referring_page", ""),
            "anchor_text":   record.metadata.get("anchor_text", ""),
            "title":         str(record.metadata.get("title", "")),
            "course_name":   record.metadata.get("course_name", ""),
            "course_id":     record.metadata.get("course_id", ""),
            "module_name":   record.metadata.get("module_name", ""),
            "source_label":  record.metadata.get("source_label", ""),
            "content_type":  record.content_type,
            "input_type":    record.input_type,
            "intent":        record.intent,
            "processing_route": record.processing_route,
            "quality_score": float(round(record.quality_score, 3)),
            "trust_score":   float(round(record.trust_score, 3)),
            "dedup_result":  record.dedup_result,
            "word_count":    int(record.metadata.get("word_count", 0)),
            "page_count":    int(record.metadata.get("page_count", 0)),
            "slide_count":   int(record.metadata.get("slide_count", 0)),
            "is_image_heavy": bool(record.metadata.get("is_image_heavy", False)),
            "access_status": record.metadata.get("access_status", "public"),
            "recommended_use": record.recommended_use,
            "errors":        "; ".join(record.errors) if record.errors else "",
        }

        # Store text chunks
        for i, chunk in enumerate(chunks):
            chunk_id = f"{record.unique_id}_c{i}"
            meta = {**base_meta, "chunk": i, "chunk_count": len(chunks)}
            self.documents.upsert(ids=[chunk_id], documents=[chunk], metadatas=[meta])

        # Store structured-elements summary as a separate searchable document
        struct_text = _structured_to_text(record.structured_elements)
        if struct_text.strip():
            self.documents.upsert(
                ids=[f"{record.unique_id}_struct"],
                documents=[struct_text[:6000]],
                metadatas=[{**base_meta, "chunk": -1, "chunk_count": 1, "is_structured": True}],
            )

        logger.debug(
            f"[kb.store] Stored {record.unique_id} "
            f"({len(chunks)} chunk(s), content_type={record.content_type}, "
            f"quality={record.quality_score:.2f})"
        )
        return record.unique_id

    def find_by_canonical_url(self, canonical_url: str) -> Optional[dict]:
        """
        Return the first document matching canonical_url, or None.
        Used by the Deduplicator to detect previously-indexed content.
        """
        try:
            r = self.documents.get(
                where={"canonical_url": canonical_url},
                include=["documents", "metadatas"],
                limit=1,
            )
            if r.get("ids"):
                return {
                    "id": r["ids"][0],
                    "cleaned_text": r["documents"][0] if r.get("documents") else "",
                }
        except Exception as e:
            logger.debug(f"[kb.find_by_canonical_url] {e}")
        return None
