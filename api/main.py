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
from typing import Optional

from fastapi import FastAPI, Request, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from agent.brain import AgentBrain
from agent.crawler import CanvasCrawler
from agent.document_ingester import extract_text_from_bytes  # RF-InlineImport
from agent.file_generator import generate_file
from agent.knowledge_base import KnowledgeBase

logging.basicConfig(level=logging.INFO)
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

app = FastAPI(title="Canvas AI Student Agent", version="1.0.0")
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


@app.on_event("startup")
async def _startup_checks() -> None:
    """
    RF-AuthStartup: run security and configuration checks at startup so
    misconfigurations are logged immediately rather than discovered later.
    A missing API_SECRET means every /api/* route is publicly accessible —
    this must never be silent on a live Railway deployment.
    """
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
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=401, content={"detail": "Unauthorized"})


# ------------------------------------------------------------------ #
#  Global state — persisted to disk                                   #
# ------------------------------------------------------------------ #

kb = KnowledgeBase()
brain = AgentBrain(kb)


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

    crawl_status = {"running": True, "last_run": None, "message": "Crawling Canvas..."}
    _save_status(crawl_status)

    try:
        async with CanvasCrawler() as crawler:
            if not await crawler.verify_session():
                crawl_status = {
                    "running": False,
                    "last_run": datetime.now().isoformat(),
                    "message": "Session invalid -- re-upload cookies",
                }
                _save_status(crawl_status)
                return

            # RF-CrawlTimeout: a hard ceiling prevents an infinitely stalled crawl
            # from holding the running lock forever. 2 hours is generous for any
            # realistic Canvas course load.
            try:
                await asyncio.wait_for(
                    crawler.crawl_all(),
                    timeout=CRAWL_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
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
                await crawler.save_knowledge()

            await crawler.save_knowledge()

        count = kb.ingest_knowledge()
        brain.invalidate_upcoming_cache()

        crawl_status = {
            "running": False,
            "last_run": datetime.now().isoformat(),
            "message": f"Crawl complete -- {count} documents indexed",
            "stats": kb.stats(),
        }
        logger.info("Crawl complete")

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
    if crawl_status.get("running"):
        raise HTTPException(409, "A crawl is already running")
    if not COOKIE_PATH.exists():
        raise HTTPException(400, "No cookies uploaded. Upload cookies first.")
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


@app.get("/api/knowledge/assignments")
async def get_assignments():
    return kb.get_all_assignments()


@app.get("/api/knowledge/upcoming")
async def get_upcoming():
    return kb.get_upcoming_assignments()


@app.get("/api/knowledge/briefing")
async def get_briefing():
    # RF-EventLoop: get_running_loop() not get_event_loop()
    briefing = await asyncio.get_running_loop().run_in_executor(
        None, brain.generate_daily_briefing
    )
    return {"briefing": briefing, "generated_at": datetime.now().isoformat()}


@app.post("/api/knowledge/search")
async def search_knowledge(body: ChatMessage):
    assignments = kb.search_assignments(body.message)
    content = kb.search_course_content(body.message)
    return {"assignments": assignments, "content": content}


# ------------------------------------------------------------------ #
#  Chat (rate limited)                                                 #
# ------------------------------------------------------------------ #


@app.post("/api/chat")
@limiter.limit("20/minute")  # RF-RateLimit
async def chat(request: Request, body: ChatMessage):
    """Chat with the agent. Limited to 20 requests/minute per IP."""
    try:
        # RF-EventLoop: get_running_loop()
        reply = await asyncio.get_running_loop().run_in_executor(
            None, partial(brain.chat, body.message)
        )
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
    result = kb.get_assignment_by_id(assignment_id)
    if not result:
        raise HTTPException(404, f"Assignment '{assignment_id}' not found")

    # RF-EventLoop
    analysis = await asyncio.get_running_loop().run_in_executor(
        None, partial(brain.analyze_assignment, result["document"])
    )
    return {"assignment_id": assignment_id, "analysis": analysis}


@app.post("/api/assignments/generate/{assignment_id}")
@limiter.limit("5/minute")  # RF-RateLimit: generation is expensive
async def generate_assignment(request: Request, assignment_id: str):
    """Generate a draft for an assignment. Limited to 5 requests/minute per IP."""
    result = kb.get_assignment_by_id(assignment_id)
    if not result:
        raise HTTPException(404, f"Assignment '{assignment_id}' not found")

    doc = result["document"]
    meta = result["metadata"]

    # RF-EventLoop
    analysis = await asyncio.get_running_loop().run_in_executor(
        None, partial(brain.analyze_assignment, doc)
    )
    file_type = (analysis.get("ESTIMATED_TYPE") or "docx").lower()

    if file_type in ("manual", "code"):
        return {
            "assignment_id": assignment_id,
            "mode": "copilot",
            "analysis": analysis,
            "message": "This assignment requires co-pilot mode. Start a chat to work through it.",
        }

    # RF-EventLoop
    content = await asyncio.get_running_loop().run_in_executor(
        None, partial(brain.generate_content, doc, file_type)
    )

    try:
        filepath = generate_file(content, meta.get("title", "Assignment"), file_type)
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
        brain.log_event("approval", {"draft_id": body.assignment_id, "feedback": body.feedback})
        msg = "Assignment approved. Ready for submission."
    else:
        draft["status"] = "rejected"
        draft["feedback"] = body.feedback
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
    brain.invalidate_upcoming_cache()
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
    data = await file.read(MAX_DOCUMENT_UPLOAD_BYTES + 1)
    if len(data) > MAX_DOCUMENT_UPLOAD_BYTES:
        raise HTTPException(400, "File too large -- maximum 20 MB")

    content_type = file.content_type or ""
    filename = file.filename or ""
    doc_title = title or Path(filename).stem or "Uploaded Document"

    text = extract_text_from_bytes(data, content_type, filename)  # RF-InlineImport
    if not text.strip():
        raise HTTPException(422, "Could not extract text -- try pasting the text instead")

    doc_id = kb.add_manual_document(
        title=doc_title,
        text=text,
        course_name=course_name,
        course_id=course_id or "",
        url="",
    )
    brain.invalidate_upcoming_cache()
    return {
        "message": f"'{doc_title}' ingested ({len(text):,} characters extracted)",
        "doc_id": doc_id,
        "char_count": len(text),
    }


@app.post("/api/documents/search")
async def search_documents(body: ChatMessage):
    """Search the full document / readings knowledge base."""
    results = kb.search_documents(body.message, n=5)
    return {"results": results}


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
#  Static dashboard (served last so API routes take priority)         #
# ------------------------------------------------------------------ #

_dashboard_path = Path("dashboard/build")
if _dashboard_path.exists():
    app.mount("/", StaticFiles(directory=str(_dashboard_path), html=True), name="dashboard")
