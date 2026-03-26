"""
Agent Brain
Claude-powered intelligence: reads the knowledge base, understands assignments,
generates work in the user's voice, and runs the self-improvement loop.

Review fixes applied:
  - RF-LazyClient  : Anthropic client is lazily initialised on first use,
                     not at module import time — avoids crashing during build
                     or testing when the API key is not available
  - RF-ModelConst  : model name driven by AI_MODEL env var, not hardcoded
                     in five separate call sites
  - RF-ContentIdx  : safe _extract_text() helper guards against empty
                     response.content before indexing [0]
  - RF-VoiceDirty  : voice profile only re-fetched from ChromaDB when a new
                     sample has been added (dirty flag), not on every call
  - RF-DraftUUID   : draft/sample IDs include a UUID fragment to prevent
                     second-precision collisions
"""

import base64
import json
import logging
import os
import time
from typing import Optional

from anthropic import Anthropic

from agent.knowledge_base import KnowledgeBase

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  RF-LazyClient: lazy singleton — built on first use, not at import  #
# ------------------------------------------------------------------ #

_client: Optional[Anthropic] = None


def _get_client():
    """
    Return an AI client on first call, lazy-initialised.
    Supports both Anthropic and OpenRouter. OpenRouter is checked first
    since that is the recommended starting point for new deployments.

    OpenRouter: set OPENROUTER_API_KEY and AI_MODEL in Railway Variables.
    Anthropic:  set ANTHROPIC_API_KEY and AI_MODEL in Railway Variables.
    """
    global _client
    if _client is not None:
        return _client

    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()

    if openrouter_key:
        # OpenRouter exposes an OpenAI-compatible API so we use the openai package.
        # It is already installed as a transitive dependency of chromadb.
        try:
            from openai import OpenAI
        except ImportError:
            raise RuntimeError(
                "The 'openai' package is required for OpenRouter. "
                "Add openai>=1.0.0 to requirements.txt."
            )
        _client = OpenAI(
            api_key=openrouter_key,
            base_url="https://openrouter.ai/api/v1",
        )
        logger.info(f"AI client: OpenRouter / {_AI_MODEL}")
        return _client

    if anthropic_key:
        _client = Anthropic(api_key=anthropic_key)
        logger.info(f"AI client: Anthropic / {_AI_MODEL}")
        return _client

    raise RuntimeError(
        "No AI API key found. Set either OPENROUTER_API_KEY (recommended) "
        "or ANTHROPIC_API_KEY in your Railway Variables."
    )


# RF-ModelConst: single source of truth for the model string
_AI_MODEL = os.environ.get("AI_MODEL", "claude-sonnet-4-20250514")

# Known models shown in the dashboard switcher.
# Note: ALL OpenRouter models (even free ones) require a credit-enabled account.
# Add $5 at openrouter.ai/settings/credits — free models cost $0 but credits
# must be present for the account to be activated.
FREE_MODELS = [
    {"id": "openrouter/auto", "label": "OpenRouter Auto · picks best available free model"},
    {"id": "google/gemini-2.5-pro-exp-03-25:free", "label": "Gemini 2.5 Pro (free) · 1M ctx · best quality"},
    {"id": "meta-llama/llama-4-maverick:free", "label": "Llama 4 Maverick (free) · 1M ctx · strong all-rounder"},
    {"id": "meta-llama/llama-4-scout:free", "label": "Llama 4 Scout (free) · 10M ctx · massive context"},
    {"id": "deepseek/deepseek-r1:free", "label": "DeepSeek R1 (free) · 164K ctx · strong reasoning"},
    {"id": "mistralai/mistral-small-3.1-24b-instruct:free", "label": "Mistral Small 3.1 (free) · 128K ctx"},
    {"id": "meta-llama/llama-3.3-70b-instruct:free", "label": "Llama 3.3 70B (free) · 131K ctx · reliable"},
    {"id": "anthropic/claude-sonnet-4-5", "label": "Claude Sonnet 4.5 (paid) · best quality"},
]


# Default free vision model on OpenRouter — swap via VISION_MODEL env var
_VISION_MODEL_DEFAULT = "meta-llama/llama-3.2-11b-vision-instruct:free"


async def describe_page_visuals(screenshot_bytes: bytes, context: str = "") -> str:
    """
    Send a full-page screenshot to a vision model and return extracted content.

    Only runs when VISION_ENABLED=true and OPENROUTER_API_KEY is set.
    Model is controlled by the VISION_MODEL env var so you can swap to any
    vision-capable model on OpenRouter without a redeploy.

    Returns an empty string (silently) if vision is disabled or unavailable,
    so callers can always do: text += await describe_page_visuals(...) safely.
    """
    if os.environ.get("VISION_ENABLED", "").lower() not in ("1", "true"):
        return ""

    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not openrouter_key:
        logger.warning("VISION_ENABLED=true but OPENROUTER_API_KEY not set — skipping vision")
        return ""

    vision_model = os.environ.get("VISION_MODEL", _VISION_MODEL_DEFAULT)

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(
            api_key=openrouter_key,
            base_url="https://openrouter.ai/api/v1",
        )

        b64 = base64.b64encode(screenshot_bytes).decode()
        prompt = (
            "Extract all meaningful content from this Canvas LMS page screenshot. "
            "Focus on: tables and their full data, rubric criteria and point values, "
            "diagrams or charts with their labels, any text embedded in images, "
            "and structured content that plain HTML scraping would miss. "
            "Be concise — only report content that adds information beyond plain text."
        )
        if context:
            prompt = f"Page: {context}\n\n{prompt}"

        response = await client.chat.completions.create(
            model=vision_model,
            max_tokens=1024,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                    {"type": "text", "text": prompt},
                ],
            }],
        )

        result = (response.choices[0].message.content or "").strip()
        if result:
            logger.info(f"Vision: {len(result)} chars extracted from {context or 'page'}")
        return result

    except Exception as e:
        logger.warning(f"Vision extraction failed for {context!r}: {e}")
        return ""


def set_model(model_id: str) -> None:
    """
    Switch the active AI model at runtime without restarting the server.
    The new model takes effect on the next API call.
    """
    global _AI_MODEL, _client
    _AI_MODEL = model_id
    # Reset client so it is rebuilt with the correct provider on next call
    _client = None
    logger.info(f"AI model switched to: {model_id}")


MAX_HISTORY_TURNS = 20
_UPCOMING_CACHE_TTL = 300  # seconds

SYSTEM_PROMPT = """You are an AI student agent enrolled at Wilmington University (wilmu.instructure.com).

Your role:
- You operate as a real student — reading assignments, understanding course material, generating academic work
- You write in the user's personal voice and style at all times
- You are meticulous about rubrics — every piece of work must address every rubric criterion
- You are honest about what you can and cannot do
- You flag uncertainty rather than guessing
- You never submit anything without explicit user approval

Your capabilities:
- Read and interpret any Canvas assignment, rubric, or course material
- Generate Word documents, PowerPoint presentations, Excel spreadsheets, discussion posts, short answers
- Walk users through code assignments and quizzes interactively
- Build and maintain a knowledge base of course content
- Learn from instructor feedback and grades to improve future work
- Propose system improvements and track performance over time

Always:
- Reference specific rubric criteria when generating work
- Cite course materials where relevant
- Flag if you don't have enough information to complete a task
- Ask clarifying questions before starting complex assignments
- Give a confidence score (1-10) on every generated draft

Current date context and course knowledge will be provided in each message.
"""


def _extract_text(response) -> str:
    """
    Safely extract text from either an Anthropic or OpenRouter/OpenAI response.
    - Anthropic: response.content is a list of blocks with .type and .text
    - OpenRouter/OpenAI: response.choices[0].message.content is a string
    """
    # OpenAI / OpenRouter format
    if hasattr(response, "choices"):
        content = response.choices[0].message.content
        if not content:
            raise ValueError(
                f"Empty content in OpenRouter response. "
                f"finish_reason={response.choices[0].finish_reason}"
            )
        return content

    # Anthropic format
    text_blocks = [b for b in response.content if b.type == "text"]
    if not text_blocks:
        raise ValueError(
            f"No text block in Anthropic response. "
            f"stop_reason={response.stop_reason}, "
            f"content_types={[b.type for b in response.content]}"
        )
    return text_blocks[0].text


def _call_api(system: str, messages: list, max_tokens: int = 2048):
    """
    Make a completion call to whichever provider is configured.
    Anthropic and OpenRouter have different call signatures — this
    normalises them so callers don't need to care.
    """
    client = _get_client()

    # OpenRouter / OpenAI format
    if hasattr(client, "chat"):
        return client.chat.completions.create(
            model=_AI_MODEL,
            max_tokens=max_tokens,
            messages=[{"role": "system", "content": system}] + messages,
        )

    # Anthropic format
    return client.messages.create(
        model=_AI_MODEL,
        max_tokens=max_tokens,
        system=system,
        messages=messages,
    )


class AgentBrain:
    def __init__(self, kb: KnowledgeBase):
        self.kb = kb
        self.conversation_history: list[dict] = []
        self.improvement_log: list[dict] = []
        self.voice_profile: str = ""
        self._voice_dirty: bool = True       # RF-VoiceDirty: fetch on first use
        self._upcoming_cache: tuple[float, list] = (0.0, [])

    # ------------------------------------------------------------------ #
    #  Voice profile                                                       #
    # ------------------------------------------------------------------ #

    def mark_voice_dirty(self) -> None:
        """
        Signal that voice samples have changed and should be re-fetched.
        RF-VoiceDirty: call this whenever a sample or rule is added so
        the next generation refreshes the profile exactly once.
        """
        self._voice_dirty = True

    def update_voice_profile(self) -> None:
        """
        Re-fetch voice samples from ChromaDB only when the dirty flag is set.
        RF-VoiceDirty: avoids a ChromaDB round-trip on every chat message
        and generation call when nothing has changed.
        """
        if not self._voice_dirty:
            return
        samples = self.kb.get_voice_samples()
        if samples:
            self.voice_profile = "\n\n---\n\n".join(samples[:10])
        self._voice_dirty = False

    def get_voice_context(self) -> str:
        """Return the voice context block for injection into prompts."""
        if not self.voice_profile:
            return "No voice samples provided yet. Write in a clear, academic style."
        return (
            "The user's writing voice (match this style closely):\n\n"
            f"{self.voice_profile[:3000]}\n\n"
            "Key patterns to replicate: sentence length, vocabulary level, tone, "
            "how they structure arguments, transition phrases, level of formality."
        )

    # ------------------------------------------------------------------ #
    #  Context building                                                    #
    # ------------------------------------------------------------------ #

    def _get_upcoming_cached(self) -> list:
        """Return upcoming assignments from a TTL cache to avoid per-call DB scans."""
        now = time.monotonic()
        fetched_at, data = self._upcoming_cache
        if now - fetched_at > _UPCOMING_CACHE_TTL:
            data = self.kb.get_upcoming_assignments()
            self._upcoming_cache = (now, data)
        return data

    def invalidate_upcoming_cache(self) -> None:
        """Bust the cache after a crawl or status change."""
        self._upcoming_cache = (0.0, [])

    def build_context(self, query: str = "", course_name: Optional[str] = None) -> str:
        """Build the context block injected into every prompt."""
        parts: list[str] = []

        parts.append(self.get_voice_context())

        upcoming = self._get_upcoming_cached()
        if upcoming:
            lines = ["UPCOMING ASSIGNMENTS:"]
            for a in upcoming[:10]:
                m = a["metadata"]
                lines.append(
                    f"- [{m.get('course_name', 'Unknown')}] "
                    f"{m.get('title', 'Unknown')} -- Due: {m.get('due', 'Unknown')}"
                )
            parts.append("\n".join(lines))

        if query:
            if course_name:
                relevant_content = self.kb.search_course_content_by_course(query, course_name=course_name, n=3)
                relevant_docs = self.kb.search_documents_by_course(query, course_name=course_name, n=4)
            else:
                relevant_content = self.kb.search_course_content(query, n=3)
                relevant_docs = self.kb.search_documents(query, n=4)

            if relevant_content:
                lines = ["\nRELEVANT COURSE CONTENT:"]
                for r in relevant_content:
                    lines.append(r["document"][:500])
                parts.append("\n".join(lines))

            if relevant_docs:
                lines = ["\nRELEVANT READINGS & DOCUMENTS:"]
                for r in relevant_docs:
                    meta = r.get("metadata", {})
                    header = f"[{meta.get('title', 'Document')} -- {meta.get('course_name', '')}]"
                    lines.append(header)
                    lines.append(r["document"][:800])
                parts.append("\n".join(lines))

        return "\n\n".join(parts)

    # ------------------------------------------------------------------ #
    #  Conversation history                                                #
    # ------------------------------------------------------------------ #

    def _trim_history(self) -> None:
        """
        Keep only the most recent MAX_HISTORY_TURNS pairs.
        Retains the oldest message for session context.
        """
        max_messages = MAX_HISTORY_TURNS * 2
        if len(self.conversation_history) <= max_messages:
            return
        self.conversation_history = (
            self.conversation_history[:1]
            + self.conversation_history[-(max_messages - 1):]
        )

    # ------------------------------------------------------------------ #
    #  Chat                                                                #
    # ------------------------------------------------------------------ #

    def chat(self, user_message: str, course_name: Optional[str] = None) -> str:
        """Multi-turn conversation with the agent."""
        self.update_voice_profile()  # RF-VoiceDirty: no-op unless dirty
        context = self.build_context(user_message, course_name=course_name)
        system = SYSTEM_PROMPT + f"\n\n=== CURRENT CONTEXT ===\n{context}"

        self.conversation_history.append({"role": "user", "content": user_message})
        self._trim_history()

        response = _call_api(  # RF-LazyClient, RF-ModelConst
            system=system,
            messages=self.conversation_history,
            max_tokens=4096,
        )

        reply = _extract_text(response)  # RF-ContentIdx
        self.conversation_history.append({"role": "assistant", "content": reply})
        return reply

    def reset_conversation(self) -> None:
        """Clear the conversation history."""
        self.conversation_history = []

    # ------------------------------------------------------------------ #
    #  Assignment operations                                               #
    # ------------------------------------------------------------------ #

    def analyze_assignment(self, assignment_doc: str) -> dict:
        """
        Break down an assignment: requirements, rubric, strategy, confidence.
        JSON parse failures are logged and returned as structured errors.
        """
        prompt = (
            f"Analyze this assignment thoroughly:\n\n{assignment_doc}\n\n"
            "Provide:\n"
            "1. SUMMARY: What exactly needs to be done\n"
            "2. REQUIREMENTS: Bullet list of every requirement\n"
            "3. RUBRIC_BREAKDOWN: Each criterion and how to maximize points\n"
            "4. STRATEGY: How you will approach this\n"
            "5. QUESTIONS: Any clarifications needed before starting\n"
            "6. CONFIDENCE: Score 1-10 on how well you understand this assignment\n"
            "7. ESTIMATED_TYPE: File type to generate (docx/pptx/xlsx/text/code/manual)\n\n"
            "Respond ONLY with valid JSON, no markdown fences."
        )

        response = _call_api(
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2048,
        )

        raw = _extract_text(response)
        clean = raw.replace("```json", "").replace("```", "").strip()

        try:
            return json.loads(clean)
        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to parse analysis JSON: {e}\n"
                f"Raw response (first 500 chars): {raw[:500]}"
            )
            return {
                "parse_error": True,
                "raw": raw,
                "SUMMARY": "Analysis parsing failed -- review raw output before proceeding",
                "REQUIREMENTS": [],
                "RUBRIC_BREAKDOWN": [],
                "STRATEGY": "",
                "QUESTIONS": [],
                "CONFIDENCE": 0,
                "ESTIMATED_TYPE": "docx",
            }

    def generate_content(self, assignment_doc: str, file_type: str) -> str:
        """Generate complete content for an assignment in the user's voice."""
        self.update_voice_profile()  # RF-VoiceDirty
        voice_ctx = self.get_voice_context()

        prompt = (
            f"Generate complete content for this assignment.\n\n"
            f"VOICE & STYLE:\n{voice_ctx}\n\n"
            f"ASSIGNMENT:\n{assignment_doc}\n\n"
            f"Generate the full content needed for a {file_type} submission.\n"
            "- Address every rubric criterion explicitly\n"
            "- Write entirely in the user's voice\n"
            "- Include all required sections\n"
            "- Be thorough and complete\n"
            "- End with: CONFIDENCE: X/10 -- [brief reason]\n\n"
            "For DOCX: Write full essay/report content with clear section headers\n"
            "For PPTX: Write slide-by-slide content (Slide 1: Title, Slide 2: ...)\n"
            "For XLSX: Describe the spreadsheet structure and all data/formulas needed\n"
            "For TEXT: Write the complete response"
        )

        response = _call_api(
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=8096,
        )

        return _extract_text(response)

    def generate_daily_briefing(self) -> str:
        """Generate a personalised daily briefing: priorities, upcoming, recommendations."""
        upcoming = self._get_upcoming_cached()
        context = self.build_context()

        assignment_list = "\n".join(
            f"- {a['metadata'].get('title')} ({a['metadata'].get('course_name')}) "
            f"-- Due: {a['metadata'].get('due')}"
            for a in upcoming[:15]
        ) or "No pending assignments found."

        prompt = (
            f"Generate a concise daily briefing for the student.\n\n"
            f"UPCOMING ASSIGNMENTS:\n{assignment_list}\n\n"
            f"CONTEXT:\n{context}\n\n"
            "Include:\n"
            "1. TODAY'S FOCUS -- top 1-2 priorities\n"
            "2. THIS WEEK -- what's coming up\n"
            "3. URGENT -- anything due very soon\n"
            "4. RECOMMENDATION -- what to work on first and why\n\n"
            "Keep it actionable and concise."
        )

        response = _call_api(
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1024,
        )

        return _extract_text(response)

    # ------------------------------------------------------------------ #
    #  Self-improvement                                                    #
    # ------------------------------------------------------------------ #

    def propose_improvements(self) -> list:
        """Review the improvement log and propose specific system changes."""
        if not self.improvement_log:
            return []

        log_text = json.dumps(self.improvement_log[-20:], indent=2)

        prompt = (
            f"Review this improvement log and propose specific system improvements:\n\n"
            f"{log_text}\n\n"
            "For each proposal include:\n"
            "- WHAT: The specific change\n"
            "- WHY: What problem it solves\n"
            "- HOW: How to implement it\n"
            "- PRIORITY: high/medium/low\n\n"
            "Return ONLY a valid JSON array of proposal objects, no markdown fences."
        )

        response = _call_api(
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2048,
        )

        raw = _extract_text(response)
        clean = raw.replace("```json", "").replace("```", "").strip()

        try:
            return json.loads(clean)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse improvement proposals JSON: {e}\nRaw: {raw[:500]}")
            return [{"parse_error": True, "raw": raw}]

    def log_event(self, event_type: str, details: dict) -> None:
        """Append an event to the improvement log with a wall-clock timestamp."""
        self.improvement_log.append({
            "type": event_type,
            "details": details,
            "timestamp": time.time(),
        })
