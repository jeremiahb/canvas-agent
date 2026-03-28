"""
FastAPI Backend
All API routes for the Canvas Agent dashboard.

Review fixes applied:
  - RF-Auth        : authentication middleware — every /api/* route requires
                     X-Api-Key header matching API_SECRET env var
  - RF-RateLimit   : slowapi rate limiting on chat and generation endpoints
  - RF-ImpLog      : improvement log loaded from disk on startup and saved
                     after every log_event call so it survives restarts
  - RF-ModelConst  : AI_MODEL and API_SECRET added to env var documentation
  - RF-MaxBytes    : MAX_DOCUMENT_UPLOAD_BYTES promoted to module-level constant
  - RF-InlineImport: extract_text_from_bytes imported at module level
  - RF-DraftUUID   : draft IDs include UUID fragment to prevent collisions
  - RF-SampleUUID  : voice sample / style rule IDs include UUID fragment
  - RF-EventLoop   : asyncio.get_event_loop() replaced with get_running_loop()
  - RF-AtomicWrite : queue and status writes use a temp-file + atomic rename
  - RF-VoiceDirty  : mark_voice_dirty() called instead of full update_voice_profile()
"""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from functools import partial
from pathlib import Path
import io
import zipfile
from typing import List, Optional

import re

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from agent.brain import AgentBrain
import agent.brain as brain_module
from agent.crawler import CanvasCrawler
from agent.document_ingester import extract_text_from_bytes  # RF-InlineImport
from agent.file_generator import generate_file
from agent.knowledge_base import KnowledgeBase
from agent.knowledge_organizer import KnowledgeOrganizer, TOPIC_MAP_PATH

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
# Quiet down noisy third-party libraries at DEBUG level
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("chromadb").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("anthropic").setLevel(logging.WARNING)
logging.getLogger("playwright").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  Constants                                                           #
# ------------------------------------------------------------------ #

DATA_DIR = Path(os.environ.get("DATA_DIR", "data")).resolve()

ASSIGNMENTS_DIR              = (DATA_DIR / "assignments").resolve()
COOKIE_PATH                  = DATA_DIR / "cookies" / "canvas_cookies.json"
QUEUE_PATH                   = DATA_DIR / "queue.json"
STATUS_PATH                  = DATA_DIR / "crawl_status.json"
IMPROVEMENT_LOG_PATH         = DATA_DIR / "improvement_log.json"
MAX_COOKIE_UPLOAD_BYTES      = 1 * 1024 * 1024   # 1 MB
MAX_DOCUMENT_UPLOAD_BYTES    = 20 * 1024 * 1024  # 20 MB
MAX_IMPROVEMENT_LOG_ENTRIES  = 200               # RF-ImpLogCap: cap before pruning
CRAWL_TIMEOUT_SECONDS        = 7200             # RF-CrawlTimeout: 2-hour ceiling

for _d in [ASSIGNMENTS_DIR, DATA_DIR / "cookies", DATA_DIR / "knowledge"]:
    _d.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------ #
#  Rate limiter (RF-RateLimit)                                         #
# ------------------------------------------------------------------ #

# RF-RateLimit: slowapi uses in-memory counters by default, which reset on restart.
# This is acceptable for a single-user personal agent. For multi-user deployments,
# swap in a Redis storage backend:
#   from slowapi.storage import RedisStorage
#   limiter = Limiter(key_func=get_remote_address, storage_uri="redis://localhost:6379")
limiter = Limiter(key_func=get_remote_address)

# ------------------------------------------------------------------ #
#  App                                                                 #
# ------------------------------------------------------------------ #

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run startup checks when the app starts."""
    if not _API_SECRET:
        logger.error(
            "=" * 70 + "\n"
            "SECURITY WARNING: API_SECRET environment variable is not set.\n"
            "All /api/* routes are publicly accessible to anyone who knows\n"
            "your Railway URL. Set API_SECRET in Railway Variables NOW.\n"
            "Generate a strong secret with: python -c \"import secrets; print(secrets.token_hex(32))\"\n"
            + "=" * 70
        )
    else:
        logger.info("API_SECRET is set. Authentication middleware active.")
    yield


app = FastAPI(title="Canvas AI Student Agent", version="1.0.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "http://localhost:3000")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-Api-Key"],
)

# ------------------------------------------------------------------ #
#  RF-Auth: authentication middleware                                  #
# ------------------------------------------------------------------ #

_API_SECRET = os.environ.get("API_SECRET", "").strip()



@app.middleware("http")
async def require_auth(request: Request, call_next):
    """
    Reject any /api/* request that does not carry a valid X-Api-Key header.
    Set API_SECRET in Railway Variables to a long random string.
    Health check is exempt so Railway can verify the service is alive.
    Static dashboard assets are exempt (no /api/ prefix).
    """
    if request.url.path.startswith("/api/") and request.url.path != "/api/health":
        if not _API_SECRET:
            # No secret configured — warn loudly but allow through so the
            # service is still usable during initial setup
            logger.warning(
                "API_SECRET is not set. All /api/* routes are publicly accessible. "
                "Set API_SECRET in your Railway Variables immediately."
            )
        elif request.headers.get("X-Api-Key") != _API_SECRET:
            return _json_401()

    return await call_next(request)


def _json_401():
    return JSONResponse(status_code=401, content={"detail": "Unauthorized"})


# ------------------------------------------------------------------ #
#  Global state — persisted to disk                                   #
# ------------------------------------------------------------------ #

kb = KnowledgeBase()
brain = AgentBrain(kb)
organizer = KnowledgeOrganizer(kb)
_pipeline_task = None  # module-level ref prevents GC of fire-and-forget tasks


def _atomic_write(path: Path, data: str) -> None:
    """
    Write data to a temp file then atomically rename to path.
    RF-AtomicWrite: prevents corrupt files if the process is killed mid-write.
    On POSIX, os.replace() is an atomic syscall.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(data)
    tmp.replace(path)


def _load_queue() -> dict:
    if QUEUE_PATH.exists():
        try:
            return json.loads(QUEUE_PATH.read_text())
        except Exception as e:
            logger.warning(f"Could not load queue from disk: {e}")
    return {}


def _save_queue(queue: dict) -> None:
    _atomic_write(QUEUE_PATH, json.dumps(queue, indent=2))  # RF-AtomicWrite


def _load_status() -> dict:
    if STATUS_PATH.exists():
        try:
            return json.loads(STATUS_PATH.read_text())
        except Exception as e:
            logger.warning(f"Could not load crawl status from disk: {e}")
    return {"running": False, "last_run": None, "message": "Never crawled"}


def _save_status(status: dict) -> None:
    _atomic_write(STATUS_PATH, json.dumps(status, indent=2))  # RF-AtomicWrite


def _load_improvement_log() -> list:
    """RF-ImpLog: load persisted improvement log from disk on startup."""
    if IMPROVEMENT_LOG_PATH.exists():
        try:
            return json.loads(IMPROVEMENT_LOG_PATH.read_text())
        except Exception as e:
            logger.warning(f"Could not load improvement log from disk: {e}")
    return []


def _save_improvement_log(log: list) -> None:
    """
    Atomically persist improvement log after every update.
    RF-ImpLogCap: prune to the most recent MAX_IMPROVEMENT_LOG_ENTRIES entries
    before writing so the file never grows without bound across a full semester.
    propose_improvements() only ever reads the last 20 entries anyway.
    """
    pruned = log[-MAX_IMPROVEMENT_LOG_ENTRIES:] if len(log) > MAX_IMPROVEMENT_LOG_ENTRIES else log
    _atomic_write(IMPROVEMENT_LOG_PATH, json.dumps(pruned, indent=2))


# Load persisted state on startup
assignment_queue: dict = _load_queue()
crawl_status: dict = _load_status()
brain.improvement_log = _load_improvement_log()  # RF-ImpLog

if crawl_status.get("running"):
    crawl_status["running"] = False
    crawl_status["message"] = "Crawl interrupted by restart"
    _save_status(crawl_status)

# ------------------------------------------------------------------ #
#  Pydantic models                                                     #
# ------------------------------------------------------------------ #


class ChatMessage(BaseModel):
    message: str
    conversation_id: Optional[str] = None
    course_name: Optional[str] = None


class VoiceSample(BaseModel):
    text: str
    label: Optional[str] = None


class AssignmentApproval(BaseModel):
    assignment_id: str
    approved: bool
    feedback: Optional[str] = None


class StyleRule(BaseModel):
    rule: str


class ManualDocument(BaseModel):
    title: str
    text: str
    course_name: str
    course_id: Optional[str] = ""
    url: Optional[str] = ""


class ResetConversation(BaseModel):
    confirm: bool = True


# ------------------------------------------------------------------ #
#  Health (auth exempt)                                               #
# ------------------------------------------------------------------ #


@app.get("/api/health")
async def health():
    """Public health check — exempt from authentication."""
    return {
        "status": "running",
        "knowledge_base": kb.stats(),
        "crawl_status": crawl_status,
        "queue_size": len(assignment_queue),
    }


# ------------------------------------------------------------------ #
#  Cookies                                                             #
# ------------------------------------------------------------------ #


@app.post("/api/cookies/upload")
async def upload_cookies(file: UploadFile = File(...)):
    """Upload canvas_cookies.json exported from the local machine."""
    content = await file.read(MAX_COOKIE_UPLOAD_BYTES + 1)
    if len(content) > MAX_COOKIE_UPLOAD_BYTES:
        raise HTTPException(400, "Cookie file exceeds 1 MB limit")

    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        raise HTTPException(400, "File is not valid JSON")

    if not isinstance(data.get("cookies"), list):
        raise HTTPException(400, "Invalid cookie file: 'cookies' must be a list")

    if len(data["cookies"]) == 0:
        raise HTTPException(400, "Cookie list is empty -- log into Canvas first")

    COOKIE_PATH.parent.mkdir(parents=True, exist_ok=True)
    COOKIE_PATH.write_bytes(content)

    return {"message": f"Cookies uploaded successfully ({len(data['cookies'])} cookies)"}


@app.get("/api/cookies/status")
async def cookie_status():
    if not COOKIE_PATH.exists():
        return {"valid": False, "message": "No cookies uploaded"}

    try:
        data = json.loads(COOKIE_PATH.read_text())
    except Exception:
        return {"valid": False, "message": "Cookie file is corrupt -- re-upload"}

    return {
        "valid": True,
        "exported_at": data.get("exported_at"),
        "cookie_count": len(data.get("cookies", [])),
        "canvas_url": data.get("canvas_url"),
    }


# ------------------------------------------------------------------ #
#  Crawl                                                               #
# ------------------------------------------------------------------ #


async def run_crawl_task() -> None:
    """Background task: crawl Canvas, ingest knowledge, bust cache."""
    global crawl_status
    logger.debug("[crawl_task] Background crawl task started")

    crawl_status = {"running": True, "last_run": None, "message": "Crawling Canvas..."}
    _save_status(crawl_status)

    try:
        timed_out = False

        logger.debug("[crawl_task] Launching CanvasCrawler")
        async with CanvasCrawler() as crawler:
            logger.debug("[crawl_task] Browser started — verifying Canvas session")
            if not await crawler.verify_session():
                crawl_status = {
                    "running": False,
                    "last_run": datetime.now().isoformat(),
                    "message": "Session invalid -- re-upload cookies",
                }
                _save_status(crawl_status)
                logger.debug("[crawl_task] Session invalid — aborting crawl")
                return

            logger.debug(f"[crawl_task] Session OK — starting crawl (timeout={CRAWL_TIMEOUT_SECONDS}s)")
            crawler.kb = kb  # Inject KB so normalization pass can store CanvasObjects
            # RF-CrawlTimeout: a hard ceiling prevents an infinitely stalled crawl
            # from holding the running lock forever. 2 hours is generous for any
            # realistic Canvas course load.
            try:
                await asyncio.wait_for(
                    crawler.crawl_all(),
                    timeout=CRAWL_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                timed_out = True
                logger.error(
                    f"Crawl timed out after {CRAWL_TIMEOUT_SECONDS // 3600}h. "
                    "Saving partial results."
                )
                crawl_status = {
                    "running": False,
                    "last_run": datetime.now().isoformat(),
                    "message": (
                        f"Crawl timed out after {CRAWL_TIMEOUT_SECONDS // 3600}h "
                        "-- partial data saved"
                    ),
                }
                _save_status(crawl_status)
                # Still save whatever was crawled before timeout
                logger.debug("[crawl_task] Saving partial knowledge after timeout")
                await crawler.save_knowledge()

            if not timed_out:
                logger.debug("[crawl_task] Crawl finished — saving knowledge JSON")
                await crawler.save_knowledge()

        logger.debug("[crawl_task] Ingesting knowledge into ChromaDB")
        count = kb.ingest_knowledge()
        logger.debug(f"[crawl_task] Ingested {count} documents — busting upcoming cache")
        brain.invalidate_upcoming_cache()

        if not timed_out:
            crawl_status = {
                "running": False,
                "last_run": datetime.now().isoformat(),
                "message": f"Crawl complete -- {count} documents indexed. Knowledge pipeline running in background.",
                "stats": kb.stats(),
            }
            # Fire-and-forget: generate notes + organize knowledge after every crawl
            global _pipeline_task
            if _pipeline_task is None or _pipeline_task.done():
                logger.info("Crawl complete — launching knowledge pipeline")
                _pipeline_task = asyncio.create_task(organizer.run_full_pipeline())
            else:
                logger.info("Crawl complete — pipeline already running, skipping auto-trigger")

    except Exception as e:
        logger.error(f"Crawl error: {e}", exc_info=True)
        crawl_status = {
            "running": False,
            "last_run": datetime.now().isoformat(),
            "message": "Crawl failed -- check server logs",
        }
    finally:
        _save_status(crawl_status)


@app.post("/api/crawl/start")
async def start_crawl(background_tasks: BackgroundTasks):
    logger.debug(f"[POST /api/crawl/start] crawl_status.running={crawl_status.get('running')} cookie_exists={COOKIE_PATH.exists()}")
    if crawl_status.get("running"):
        raise HTTPException(409, "A crawl is already running")
    if not COOKIE_PATH.exists():
        raise HTTPException(400, "No cookies uploaded. Upload cookies first.")
    logger.debug("[POST /api/crawl/start] Queueing background crawl task")
    background_tasks.add_task(run_crawl_task)
    return {"message": "Crawl started"}


@app.get("/api/crawl/status")
async def get_crawl_status():
    return crawl_status


# ------------------------------------------------------------------ #
#  Knowledge base                                                      #
# ------------------------------------------------------------------ #


@app.get("/api/knowledge/stats")
async def knowledge_stats():
    return kb.stats()


@app.get("/api/knowledge/courses")
async def get_course_names():
    """Return all unique course names across the knowledge base."""
    return {"courses": kb.get_course_names()}


@app.get("/api/knowledge/assignments")
async def get_assignments():
    return kb.get_all_assignments()


@app.get("/api/knowledge/upcoming")
async def get_upcoming():
    return kb.get_upcoming_assignments()


@app.get("/api/knowledge/briefing")
async def get_briefing():
    briefing = await asyncio.get_running_loop().run_in_executor(
        None, brain.generate_daily_briefing
    )
    return {"briefing": briefing, "generated_at": datetime.now().isoformat()}


@app.post("/api/knowledge/search")
async def search_knowledge(body: ChatMessage):
    course = getattr(body, "course_name", None)
    assignments = kb.search_assignments(body.message, course_name=course)
    content = kb.search_course_content_by_course(body.message, course_name=course)
    return {"assignments": assignments, "content": content}


# ------------------------------------------------------------------ #
#  Chat (rate limited)                                                 #
# ------------------------------------------------------------------ #


@app.post("/api/chat")
@limiter.limit("20/minute")  # RF-RateLimit
async def chat(request: Request, body: ChatMessage):
    """Chat with the agent. Limited to 20 requests/minute per IP."""
    logger.debug(f"[POST /api/chat] message={body.message[:80]!r} course={body.course_name!r}")
    try:
        # RF-EventLoop: get_running_loop()
        reply = await asyncio.get_running_loop().run_in_executor(
            None, partial(brain.chat, body.message, body.course_name)
        )
        logger.debug(f"[POST /api/chat] Reply generated: {len(reply)} chars")
        return {"reply": reply, "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Chat error: {e}", exc_info=True)
        raise HTTPException(500, "An internal error occurred")


@app.post("/api/chat/reset")
async def reset_chat(body: ResetConversation):
    brain.reset_conversation()
    return {"message": "Conversation reset"}


# ------------------------------------------------------------------ #
#  Assignments (generation rate limited)                               #
# ------------------------------------------------------------------ #


@app.post("/api/assignments/analyze/{assignment_id}")
async def analyze_assignment(assignment_id: str):
    logger.debug(f"[POST /api/assignments/analyze] assignment_id={assignment_id!r}")
    result = kb.get_assignment_by_id(assignment_id)
    if not result:
        logger.debug(f"[POST /api/assignments/analyze] Assignment not found: {assignment_id}")
        raise HTTPException(404, f"Assignment '{assignment_id}' not found")

    logger.debug(f"[POST /api/assignments/analyze] Found assignment: {result['metadata'].get('title')!r} — sending to brain")
    # RF-EventLoop
    analysis = await asyncio.get_running_loop().run_in_executor(
        None, partial(brain.analyze_assignment, result["document"])
    )
    logger.debug(f"[POST /api/assignments/analyze] Analysis complete: type={analysis.get('ESTIMATED_TYPE')!r} confidence={analysis.get('CONFIDENCE')}")
    return {"assignment_id": assignment_id, "analysis": analysis}


@app.post("/api/assignments/generate/{assignment_id}")
@limiter.limit("5/minute")  # RF-RateLimit: generation is expensive
async def generate_assignment(request: Request, assignment_id: str):
    """Generate a draft for an assignment. Limited to 5 requests/minute per IP."""
    logger.debug(f"[POST /api/assignments/generate] assignment_id={assignment_id!r}")
    result = kb.get_assignment_by_id(assignment_id)
    if not result:
        logger.debug(f"[POST /api/assignments/generate] Assignment not found: {assignment_id}")
        raise HTTPException(404, f"Assignment '{assignment_id}' not found")

    doc = result["document"]
    meta = result["metadata"]
    logger.debug(f"[POST /api/assignments/generate] Analyzing: {meta.get('title')!r} for {meta.get('course_name')!r}")

    # RF-EventLoop
    analysis = await asyncio.get_running_loop().run_in_executor(
        None, partial(brain.analyze_assignment, doc)
    )
    file_type = (analysis.get("ESTIMATED_TYPE") or "docx").lower()
    logger.debug(f"[POST /api/assignments/generate] Analysis done — file_type={file_type!r} confidence={analysis.get('CONFIDENCE')}")

    if file_type in ("manual", "code"):
        logger.debug(f"[POST /api/assignments/generate] Copilot mode required for file_type={file_type!r}")
        return {
            "assignment_id": assignment_id,
            "mode": "copilot",
            "analysis": analysis,
            "message": "This assignment requires co-pilot mode. Start a chat to work through it.",
        }

    logger.debug(f"[POST /api/assignments/generate] Generating {file_type} content via brain")
    # RF-EventLoop
    content = await asyncio.get_running_loop().run_in_executor(
        None, partial(brain.generate_content, doc, file_type)
    )
    logger.debug(f"[POST /api/assignments/generate] Content generated: {len(content)} chars — writing file")

    try:
        filepath = generate_file(content, meta.get("title", "Assignment"), file_type)
        logger.debug(f"[POST /api/assignments/generate] File written: {filepath}")
    except Exception as e:
        logger.error(f"File generation error: {e}", exc_info=True)
        raise HTTPException(500, "File generation failed -- check server logs")

    # RF-DraftUUID: UUID suffix prevents second-precision collisions
    draft_id = f"draft_{assignment_id}_{uuid.uuid4().hex[:8]}"
    assignment_queue[draft_id] = {
        "id": draft_id,
        "assignment_id": assignment_id,
        "title": meta.get("title"),
        "course": meta.get("course_name"),
        "due": meta.get("due"),
        "file_type": file_type,
        "filepath": filepath,
        "status": "awaiting_review",
        "generated_at": datetime.now().isoformat(),
        "analysis": analysis,
    }
    _save_queue(assignment_queue)

    return {
        "draft_id": draft_id,
        "file_type": file_type,
        "status": "awaiting_review",
        "analysis": analysis,
    }


@app.get("/api/assignments/queue")
async def get_queue():
    return list(assignment_queue.values())


@app.get("/api/assignments/download/{draft_id}")
async def download_draft(draft_id: str):
    if draft_id not in assignment_queue:
        raise HTTPException(404, "Draft not found")

    draft = assignment_queue[draft_id]

    try:
        filepath = Path(draft["filepath"]).resolve()
    except Exception:
        raise HTTPException(500, "Invalid file path in draft record")

    if not str(filepath).startswith(str(ASSIGNMENTS_DIR)):
        logger.warning(f"Path traversal attempt blocked: {filepath}")
        raise HTTPException(403, "Access denied")

    if not filepath.exists():
        raise HTTPException(404, "File not found -- it may have been cleaned up")

    return FileResponse(str(filepath), filename=filepath.name)


@app.post("/api/assignments/approve")
async def approve_assignment(body: AssignmentApproval):
    if body.assignment_id not in assignment_queue:
        raise HTTPException(404, "Draft not found")

    draft = assignment_queue[body.assignment_id]

    if body.approved:
        draft["status"] = "approved"
        kb.update_assignment_status(draft["assignment_id"], "approved")
        brain.invalidate_upcoming_cache()
        brain.log_event("approval", {"draft_id": body.assignment_id, "feedback": body.feedback})
        msg = "Assignment approved. Ready for submission."
    else:
        draft["status"] = "rejected"
        draft["feedback"] = body.feedback
        kb.update_assignment_status(draft["assignment_id"], "rejected")
        brain.invalidate_upcoming_cache()
        brain.log_event("rejection", {"draft_id": body.assignment_id, "feedback": body.feedback})
        msg = "Rejected. Provide feedback and regenerate."

    _save_queue(assignment_queue)
    # RF-ImpLog: persist log after every event
    _save_improvement_log(brain.improvement_log)
    return {"message": msg, "draft": draft}


# ------------------------------------------------------------------ #
#  Documents & readings                                                #
# ------------------------------------------------------------------ #


@app.get("/api/documents/flagged")
async def get_flagged_documents():
    """Return all external platform links that need manual upload."""
    return {"flagged": kb.get_flagged_links()}


@app.get("/api/documents/flagged/{course_id}")
async def get_flagged_by_course(course_id: str):
    return {"flagged": kb.get_flagged_links(course_id=course_id)}


@app.post("/api/documents/upload")
async def upload_document_text(body: ManualDocument):
    """
    Manually add a reading or document to the knowledge base by pasting text.
    Use this for VitalSource, Pearson, or any resource the agent could not access.
    """
    logger.debug(f"[POST /api/documents/upload] title={body.title!r} course={body.course_name!r} text_len={len(body.text)}")
    if not body.text.strip():
        raise HTTPException(400, "Document text cannot be empty")
    if len(body.text) > 500_000:
        raise HTTPException(400, "Document too large -- maximum 500,000 characters")

    doc_id = kb.add_manual_document(
        title=body.title,
        text=body.text,
        course_name=body.course_name,
        course_id=body.course_id or "",
        url=body.url or "",
    )
    logger.debug(f"[POST /api/documents/upload] Stored with id={doc_id!r}")
    brain.invalidate_upcoming_cache()
    global _pipeline_task
    if _pipeline_task is None or _pipeline_task.done():
        _pipeline_task = asyncio.create_task(organizer.run_full_pipeline())
    return {
        "message": f"Document '{body.title}' added to knowledge base",
        "doc_id": doc_id,
    }


@app.post("/api/documents/upload-file")
async def upload_document_file(
    course_name: str,
    file: UploadFile = File(...),
    title: Optional[str] = None,
    course_id: Optional[str] = None,
):
    """
    Upload a PDF or Word document directly for the agent to read.
    Use this for textbook chapters, instructor PDFs, or any file the crawler could not access.
    RF-MaxBytes: limit constant now at module level.
    """
    logger.debug(f"[POST /api/documents/upload-file] filename={file.filename!r} content_type={file.content_type!r} course={course_name!r}")
    data = await file.read(MAX_DOCUMENT_UPLOAD_BYTES + 1)
    logger.debug(f"[POST /api/documents/upload-file] Read {len(data):,} bytes from upload")
    if len(data) > MAX_DOCUMENT_UPLOAD_BYTES:
        raise HTTPException(400, "File too large -- maximum 20 MB")

    content_type = file.content_type or ""
    filename = file.filename or ""
    doc_title = title or Path(filename).stem or "Uploaded Document"

    logger.debug(f"[POST /api/documents/upload-file] Extracting text from {filename!r} ({content_type})")
    text = extract_text_from_bytes(data, content_type, filename)  # RF-InlineImport
    if not text.strip():
        logger.debug(f"[POST /api/documents/upload-file] No text extracted from {filename!r}")
        raise HTTPException(422, "Could not extract text -- try pasting the text instead")

    logger.debug(f"[POST /api/documents/upload-file] Extracted {len(text):,} chars — storing as {doc_title!r}")
    doc_id = kb.add_manual_document(
        title=doc_title,
        text=text,
        course_name=course_name,
        course_id=course_id or "",
        url="",
    )
    logger.debug(f"[POST /api/documents/upload-file] Stored with id={doc_id!r}")
    brain.invalidate_upcoming_cache()
    global _pipeline_task
    if _pipeline_task is None or _pipeline_task.done():
        _pipeline_task = asyncio.create_task(organizer.run_full_pipeline())
    return {
        "message": f"'{doc_title}' ingested ({len(text):,} characters extracted)",
        "doc_id": doc_id,
        "char_count": len(text),
    }


_MAX_BULK_FILES = 50
_MAX_ZIP_COMPRESSED_BYTES = 50 * 1024 * 1024   # 50 MB compressed read limit
_MAX_ZIP_UNCOMPRESSED = 200 * 1024 * 1024       # 200 MB uncompressed guard


@app.post("/api/documents/upload-bulk")
async def upload_documents_bulk(
    course_name: str,
    files: List[UploadFile] = File(...),
    course_id: Optional[str] = None,
):
    """
    Upload multiple files at once. Accepts up to 50 files (PDF, Word, PPTX, TXT).
    Returns a summary of processed and failed files.
    """
    if len(files) > _MAX_BULK_FILES:
        raise HTTPException(400, f"Too many files — maximum {_MAX_BULK_FILES} per upload")

    results = []
    processed = 0
    failed = 0

    for upload in files:
        filename = upload.filename or ""
        doc_title = Path(filename).stem or "Uploaded Document"
        try:
            data = await upload.read(MAX_DOCUMENT_UPLOAD_BYTES + 1)
            if len(data) > MAX_DOCUMENT_UPLOAD_BYTES:
                results.append({"file": filename, "status": "failed", "reason": "file too large (max 20 MB)"})
                failed += 1
                continue

            content_type = upload.content_type or ""
            text = extract_text_from_bytes(data, content_type, filename)
            if not text.strip():
                results.append({"file": filename, "status": "failed", "reason": "no text could be extracted"})
                failed += 1
                continue

            doc_id = kb.add_manual_document(
                title=doc_title,
                text=text,
                course_name=course_name,
                course_id=course_id or "",
                url="",
            )
            results.append({"file": filename, "status": "ok", "doc_id": doc_id, "chars": len(text)})
            processed += 1
        except Exception as _e:
            logger.warning(f"[upload-bulk] Failed to process {filename!r}: {_e}")
            results.append({"file": filename, "status": "failed", "reason": str(_e)})
            failed += 1

    if processed > 0:
        brain.invalidate_upcoming_cache()
        global _pipeline_task
        if _pipeline_task is None or _pipeline_task.done():
            _pipeline_task = asyncio.create_task(organizer.run_full_pipeline())

    return {"processed": processed, "failed": failed, "results": results}


@app.post("/api/documents/upload-zip")
async def upload_documents_zip(
    course_name: str,
    file: UploadFile = File(...),
    course_id: Optional[str] = None,
):
    """
    Upload a ZIP file containing course materials. All PDF, Word, PPTX, and TXT files
    inside will be extracted and indexed. Max 200 MB uncompressed total, 20 MB per file.
    """
    filename = file.filename or ""
    content_type = file.content_type or ""
    is_zip = filename.lower().endswith(".zip") or "zip" in content_type
    if not is_zip:
        raise HTTPException(400, "File must be a .zip archive")

    data = await file.read(_MAX_ZIP_COMPRESSED_BYTES + 1)
    if len(data) > _MAX_ZIP_COMPRESSED_BYTES:
        raise HTTPException(400, "ZIP file too large — maximum 50 MB compressed")

    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise HTTPException(400, "Invalid or corrupted ZIP file")

    # Guard against zip bombs before extracting anything
    total_uncompressed = sum(info.file_size for info in zf.infolist())
    if total_uncompressed > _MAX_ZIP_UNCOMPRESSED:
        raise HTTPException(400, f"ZIP contents too large — maximum 200 MB uncompressed")

    results = []
    processed = 0
    failed = 0

    for info in zf.infolist():
        entry_name = info.filename
        # Skip directories and macOS metadata
        if entry_name.endswith("/") or entry_name.startswith("__MACOSX/") or "/__MACOSX/" in entry_name:
            continue
        if info.file_size > MAX_DOCUMENT_UPLOAD_BYTES:
            results.append({"file": entry_name, "status": "failed", "reason": "file too large (max 20 MB)"})
            failed += 1
            continue

        doc_title = Path(entry_name).stem or entry_name
        try:
            entry_data = zf.read(entry_name)
            ext = Path(entry_name).suffix.lower()
            text = extract_text_from_bytes(entry_data, "", entry_name)
            if not text.strip():
                results.append({"file": entry_name, "status": "skipped", "reason": "no text extracted"})
                continue

            doc_id = kb.add_manual_document(
                title=doc_title,
                text=text,
                course_name=course_name,
                course_id=course_id or "",
                url="",
            )
            results.append({"file": entry_name, "status": "ok", "doc_id": doc_id, "chars": len(text)})
            processed += 1
        except Exception as _e:
            logger.warning(f"[upload-zip] Failed to process {entry_name!r}: {_e}")
            results.append({"file": entry_name, "status": "failed", "reason": str(_e)})
            failed += 1

    if processed > 0:
        brain.invalidate_upcoming_cache()
        global _pipeline_task
        if _pipeline_task is None or _pipeline_task.done():
            _pipeline_task = asyncio.create_task(organizer.run_full_pipeline())

    return {"processed": processed, "failed": failed, "results": results}


@app.post("/api/crawl/reindex")
async def reindex_knowledge():
    """
    Re-run ingest_knowledge() from the existing canvas_knowledge.json snapshot
    without re-crawling Canvas. Useful after deploying new ingest paths.
    """
    snapshot_path = Path(os.environ.get("DATA_DIR", "/data")) / "knowledge" / "canvas_knowledge.json"
    if not snapshot_path.exists():
        raise HTTPException(404, "No knowledge snapshot found — run a crawl first")

    async def _run_reindex():
        try:
            total = kb.ingest_knowledge(str(snapshot_path))
            logger.info(f"[reindex] Done — {total} documents indexed from snapshot")
        except Exception as _e:
            logger.error(f"[reindex] Failed: {_e}")

    asyncio.create_task(_run_reindex())
    return {"message": "Re-indexing started from existing snapshot"}


@app.get("/api/documents/list")
async def list_documents(course_name: Optional[str] = None):
    """List all indexed documents, optionally filtered by course."""
    docs = kb.get_documents_by_course(course_name=course_name)
    return {"documents": docs, "total": len(docs)}


@app.post("/api/documents/search")
async def search_documents(body: ChatMessage):
    """Search the full document / readings knowledge base."""
    course = getattr(body, "course_name", None)
    results = kb.search_documents_by_course(body.message, course_name=course, n=8)
    return {"results": results}


# ------------------------------------------------------------------ #
#  AI Notes                                                            #
# ------------------------------------------------------------------ #


@app.get("/api/notes/list")
async def list_notes(course_name: Optional[str] = None, note_type: Optional[str] = None):
    """List all AI-generated study notes, optionally filtered by course or note_type."""
    notes = kb.get_all_notes(course_name=course_name, note_type=note_type)
    return {"notes": notes, "total": len(notes)}


@app.post("/api/notes/search")
async def search_notes(body: ChatMessage):
    """Semantic search over AI-generated study notes."""
    course = getattr(body, "course_name", None)
    results = kb.search_ai_notes(body.message, course_name=course, n=20)
    return {"results": results}


@app.post("/api/notes/generate")
async def trigger_note_generation():
    """Manually trigger the full knowledge pipeline for all unprocessed documents."""
    global _pipeline_task
    if _pipeline_task and not _pipeline_task.done():
        raise HTTPException(409, "Pipeline already running")
    _pipeline_task = asyncio.create_task(organizer.run_full_pipeline())
    return {"message": "Knowledge pipeline started in background"}


@app.get("/api/notes/stats")
async def notes_stats():
    """Return counts for the knowledge learning system."""
    from agent.knowledge_organizer import _load_processed_ids
    processed_ids = _load_processed_ids()
    return {
        "total_notes": kb.ai_notes.count(),
        "total_topics": kb.topics.count(),
        "total_concepts": kb.concepts.count(),
        "chat_history_entries": kb.chat_history.count(),
        "processed_source_docs": len(processed_ids),
        "total_assignments": kb.assignments.count(),
        "total_documents": kb.documents.count(),
        "pipeline_running": _pipeline_task is not None and not _pipeline_task.done(),
    }


# ------------------------------------------------------------------ #
#  Topics                                                              #
# ------------------------------------------------------------------ #


@app.get("/api/topics/list")
async def list_topics(course_name: Optional[str] = None):
    """List all synthesized topic overviews, optionally filtered by course."""
    topics = kb.get_all_topics(course_name=course_name)
    return {"topics": topics, "total": len(topics)}


@app.post("/api/topics/search")
async def search_topics(body: ChatMessage):
    """Semantic search over synthesized topic overviews."""
    course = getattr(body, "course_name", None)
    results = kb.search_topics(body.message, course_name=course, n=10)
    return {"results": results}


@app.get("/api/topics/{topic_id}/concepts")
async def get_topic_concepts(topic_id: str):
    """Get all concepts extracted for a specific topic."""
    concepts = kb.get_concepts_for_topic(topic_id)
    return {"concepts": concepts, "total": len(concepts)}


# ------------------------------------------------------------------ #
#  Concepts                                                            #
# ------------------------------------------------------------------ #


@app.get("/api/concepts/search")
async def search_concepts(query: str, course_name: Optional[str] = None):
    """Semantic search over extracted concept definitions."""
    results = kb.search_concepts(query, course_name=course_name, n=12)
    return {"results": results}


# ------------------------------------------------------------------ #
#  Knowledge map                                                       #
# ------------------------------------------------------------------ #


@app.get("/api/knowledge/topic-map")
async def get_topic_map():
    """Return the full hierarchical topic map JSON."""
    if TOPIC_MAP_PATH.exists():
        try:
            return json.loads(TOPIC_MAP_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"generated_at": None, "courses": {}}


# ------------------------------------------------------------------ #
#  Chat history                                                        #
# ------------------------------------------------------------------ #


@app.get("/api/chat/history")
async def get_chat_history(n: int = 60):
    """Return the n most recent persisted chat messages, newest first."""
    history = kb.get_recent_chat_history(n=n)
    return {"history": history, "total": len(history)}


# ------------------------------------------------------------------ #
#  Voice profile                                                       #
# ------------------------------------------------------------------ #


@app.post("/api/voice/sample")
async def add_voice_sample(body: VoiceSample):
    # RF-SampleUUID: UUID suffix prevents second-precision ID collisions
    sample_id = f"sample_{uuid.uuid4().hex[:8]}"
    kb.add_voice_sample(sample_id, body.text, {"label": body.label or ""})
    brain.mark_voice_dirty()  # RF-VoiceDirty: flag not full reload
    return {"message": "Voice sample added", "sample_id": sample_id}


@app.get("/api/voice/samples")
async def get_voice_samples():
    return {"samples": kb.get_voice_samples()}


@app.post("/api/voice/style-rule")
async def add_style_rule(body: StyleRule):
    # RF-SampleUUID: UUID suffix
    sample_id = f"rule_{uuid.uuid4().hex[:8]}"
    kb.add_voice_sample(sample_id, f"STYLE RULE: {body.rule}", {"label": "rule"})
    brain.mark_voice_dirty()  # RF-VoiceDirty
    return {"message": "Style rule added"}


# ------------------------------------------------------------------ #
#  Self-improvement                                                    #
# ------------------------------------------------------------------ #


@app.get("/api/improvements/proposals")
async def get_improvement_proposals():
    # RF-EventLoop
    proposals = await asyncio.get_running_loop().run_in_executor(
        None, brain.propose_improvements
    )
    return {"proposals": proposals}


@app.get("/api/improvements/log")
async def get_improvement_log():
    return {"log": brain.improvement_log}


# ------------------------------------------------------------------ #
#  Model settings                                                      #
# ------------------------------------------------------------------ #


class ModelSwitch(BaseModel):
    model_id: str


@app.get("/api/settings/model")
async def get_current_model():
    """Return the currently active model and the list of known models."""
    return {
        "current": brain_module._AI_MODEL,
        "models": brain_module.FREE_MODELS,
    }


@app.post("/api/settings/model")
async def switch_model(body: ModelSwitch):
    """Switch the active AI model at runtime. Takes effect immediately."""
    if not body.model_id.strip():
        raise HTTPException(400, "model_id cannot be empty")
    brain_module.set_model(body.model_id.strip())
    logger.info(f"Model switched to: {body.model_id}")
    return {"message": f"Model switched to {body.model_id}", "current": brain_module._AI_MODEL}


# ------------------------------------------------------------------ #
#  Debug snapshots                                                     #
# ------------------------------------------------------------------ #


@app.get("/api/snapshots")
async def list_snapshots():
    """List all saved page snapshots from the last crawl."""
    snap_dir = DATA_DIR / "debug_snapshots"
    if not snap_dir.exists():
        return {"snapshots": []}
    files = sorted(snap_dir.glob("*.html"))
    return {
        "snapshots": [
            {"name": f.stem, "size_kb": round(f.stat().st_size / 1024, 1)}
            for f in files
        ]
    }


@app.get("/api/snapshots/{name}")
async def get_snapshot(name: str):
    """Serve a specific HTML snapshot file."""
    # Sanitize — only allow safe filename characters
    safe = re.sub(r"[^\w\-]", "", name)
    snap_path = (DATA_DIR / "debug_snapshots" / f"{safe}.html").resolve()
    if not str(snap_path).startswith(str(DATA_DIR)):
        raise HTTPException(403, "Access denied")
    if not snap_path.exists():
        raise HTTPException(404, "Snapshot not found")
    return HTMLResponse(content=snap_path.read_text(encoding="utf-8", errors="replace"))


# ------------------------------------------------------------------ #
#  Canvas Objects, Graph, Changes, Study Guide                        #
# ------------------------------------------------------------------ #


@app.get("/api/objects")
async def get_canvas_objects(course_id: str = "", type: str = ""):
    """Return CanvasObjects filtered by course_id and/or object type."""
    try:
        from agent.canvas_schema import canvas_object_to_dict
        objects = kb.get_objects_by_course(course_id, object_type=type)
        results = [canvas_object_to_dict(o) for o in objects]
        return {"objects": results, "count": len(results)}
    except Exception as e:
        logger.error(f"[GET /api/objects] {e}", exc_info=True)
        raise HTTPException(500, "Failed to fetch canvas objects")


@app.get("/api/objects/{obj_id}")
async def get_canvas_object(obj_id: str):
    """Return a single CanvasObject with its graph edges."""
    try:
        from agent.canvas_schema import canvas_object_to_dict
        obj = kb.get_canvas_object(obj_id)
        if obj is None:
            raise HTTPException(404, "Object not found")
        edges = kb.get_edges_for_object(obj_id)
        return {"object": canvas_object_to_dict(obj), "edges": edges}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET /api/objects/{obj_id}] {e}", exc_info=True)
        raise HTTPException(500, "Failed to fetch canvas object")


@app.get("/api/graph")
async def get_graph_edges(from_id: str = "", relation_type: str = ""):
    """Return graph edges filtered by from_id and/or relation_type."""
    try:
        if relation_type:
            edges = kb.get_edges_by_relation(relation_type, from_id=from_id)
        elif from_id:
            edges = kb.get_edges_for_object(from_id)
        else:
            # Return a sample — full graph dump is not useful without filters
            edges = kb.get_edges_by_relation("", from_id="")
        return {"edges": edges, "count": len(edges)}
    except Exception as e:
        logger.error(f"[GET /api/graph] {e}", exc_info=True)
        raise HTTPException(500, "Failed to fetch graph edges")


@app.get("/api/changes")
async def get_change_records(course_id: str = "", severity: str = ""):
    """Return recent ChangeRecords, optionally filtered by course_id and severity."""
    try:
        records = kb.get_change_records(course_id=course_id, limit=100)
        if severity:
            records = [r for r in records if r.get("change_severity") == severity]
        return {"changes": records, "count": len(records)}
    except Exception as e:
        logger.error(f"[GET /api/changes] {e}", exc_info=True)
        raise HTTPException(500, "Failed to fetch change records")


@app.get("/api/study-guide")
async def get_study_guide(course_id: str = ""):
    """
    Return a weekly study guide: due-soon items + recent announcements + priority list.
    Sorted by urgency_score descending.
    """
    try:
        from agent.knowledge_organizer import _compute_scores
        from agent.canvas_schema import ObjectType
        import datetime as _dt
        now = _dt.datetime.now(tz=_dt.timezone.utc)

        # Collect assignments and quizzes with due dates
        actionable_types = [
            ObjectType.ASSIGNMENT.value,
            ObjectType.QUIZ_SUMMARY.value,
            ObjectType.DISCUSSION.value,
        ]
        items = []
        for obj_type in actionable_types:
            objects = kb.get_objects_by_course(course_id, object_type=obj_type)
            items.extend(objects)

        # Recent announcements (last 30 days)
        announcements = kb.get_objects_by_course(course_id, object_type=ObjectType.ANNOUNCEMENT.value)
        items.extend(announcements)

        # Attach scores and sort (items are already CanvasObject instances)
        scored = []
        for obj in items:
            scores = _compute_scores(obj, now=now)
            scored.append({
                "id": obj.id,
                "title": obj.title,
                "object_type": obj.object_type,
                "course_id": obj.course_id,
                "due_date": obj.due_date,
                "point_value": obj.point_value,
                "submission_state": obj.submission_state,
                **scores,
            })

        scored.sort(key=lambda x: x.get("urgency_score", 0), reverse=True)
        return {"study_guide": scored, "count": len(scored), "generated_at": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"[GET /api/study-guide] {e}", exc_info=True)
        raise HTTPException(500, "Failed to generate study guide")


@app.get("/api/next-actions")
async def get_next_actions():
    """
    Cross-course priority list sorted by urgency_score descending.
    Returns top 20 items needing student attention.
    """
    try:
        from agent.knowledge_organizer import _compute_scores
        from agent.canvas_schema import ObjectType
        import datetime as _dt
        now = _dt.datetime.now(tz=_dt.timezone.utc)

        actionable_types = [
            ObjectType.ASSIGNMENT.value,
            ObjectType.QUIZ_SUMMARY.value,
            ObjectType.DISCUSSION.value,
            ObjectType.ANNOUNCEMENT.value,
        ]
        all_items = []
        for obj_type in actionable_types:
            all_items.extend(kb.get_objects_by_course("", object_type=obj_type))

        scored = []
        for obj in all_items:
            scores = _compute_scores(obj, now=now)
            urgency = scores.get("urgency_score", 0)
            if urgency < 0.3:
                continue  # Skip low-urgency items
            scored.append({
                "id": obj.id,
                "title": obj.title,
                "object_type": obj.object_type,
                "course_id": obj.course_id,
                "due_date": obj.due_date,
                "point_value": obj.point_value,
                **scores,
            })

        scored.sort(key=lambda x: x.get("urgency_score", 0), reverse=True)
        return {"next_actions": scored[:20], "total_flagged": len(scored)}
    except Exception as e:
        logger.error(f"[GET /api/next-actions] {e}", exc_info=True)
        raise HTTPException(500, "Failed to compute next actions")


# ------------------------------------------------------------------ #
#  Static dashboard (served last so API routes take priority)         #
# ------------------------------------------------------------------ #

_dashboard_path = Path("dashboard/build")
if _dashboard_path.exists():
    app.mount("/", StaticFiles(directory=str(_dashboard_path), html=True), name="dashboard")
