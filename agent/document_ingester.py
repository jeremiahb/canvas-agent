"""
Document Ingester
Downloads and extracts readable text from every file and link attached to
Canvas pages, modules, and assignments.

Handles three cases:
  Case 1: Canvas-hosted files (PDF, Word, PowerPoint, plain text)
          Downloaded via Playwright's API client (shares authenticated cookies,
          avoids slow CDP integer-array serialisation).
  Case 2: Embedded / linked documents (Google Docs, OneDrive, SharePoint)
          Fetched via httpx using session cookies where possible.
  Case 3: External platform links (VitalSource, Pearson, McGraw-Hill, etc.)
          Cannot be automated — flagged so the user can upload manually.

Review fixes applied:
  - RF-Ingester-bypass : accepts goto_fn callable so rate limiting is shared
                         with the crawler, not bypassed
  - RF-Dedup           : global _seen_urls set prevents the same URL from being
                         processed by multiple discovery passes
  - RF-Recursion       : global dedup also prevents page-embed infinite loops
  - RF-CDP-bytes       : uses page.context.request.get() instead of CDP
                         integer-array transfer for file downloads
  - RF-httpx-import    : httpx imported at module level, not inside methods
  - RF-FileSizeLimit   : HEAD request checks Content-Length before downloading
"""

import asyncio
import io
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Callable, Coroutine, Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# ── External platform detection ──────────────────────────────────────

EXTERNAL_PLATFORMS = {
    "vitalsource.com":   "VitalSource",
    "chegg.com":         "Chegg",
    "pearson.com":       "Pearson",
    "mheducation.com":   "McGraw-Hill",
    "cengage.com":       "Cengage",
    "oup.com":           "Oxford University Press",
    "sagepub.com":       "SAGE",
    "wiley.com":         "Wiley",
    "taylorfrancis.com": "Taylor & Francis",
    "springer.com":      "Springer",
    "elsevier.com":      "Elsevier",
    "amazon.com":        "Amazon (textbook)",
}

GOOGLE_DOCS_HOSTS = {"docs.google.com", "drive.google.com"}
MICROSOFT_HOSTS   = {"onedrive.live.com", "sharepoint.com", "1drv.ms"}

# Hard cap on individual file downloads to protect against OOM
MAX_FILE_BYTES = 15 * 1024 * 1024  # 15 MB


def classify_url(url: str) -> str:
    """
    Return one of: 'canvas_file' | 'google_doc' | 'microsoft_doc' |
                   'external_platform' | 'web_page' | 'unknown'
    """
    if not url:
        return "unknown"
    parsed = urlparse(url.lower())
    host = parsed.netloc.replace("www.", "")

    for domain in EXTERNAL_PLATFORMS:
        if domain in host:
            return "external_platform"

    if any(h in host for h in GOOGLE_DOCS_HOSTS):
        return "google_doc"

    if any(h in host for h in MICROSOFT_HOSTS):
        return "microsoft_doc"

    if "/files/" in parsed.path or "/download" in parsed.path:
        return "canvas_file"

    return "web_page"


def detect_external_platform(url: str) -> Optional[str]:
    """Return the platform name if this URL is a known gated platform, else None."""
    host = urlparse(url.lower()).netloc.replace("www.", "")
    for domain, name in EXTERNAL_PLATFORMS.items():
        if domain in host:
            return name
    return None


# ── Text extractors ──────────────────────────────────────────────────

def extract_pdf_text(data: bytes) -> str:
    """Extract all text from a PDF byte stream. Tries pdfplumber first, falls back to pypdf."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
        text = "\n\n".join(pages)
        if text.strip():
            return text.strip()
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")

    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
        pages = [page.extract_text() or "" for page in reader.pages]
        return "\n\n".join(pages).strip()
    except Exception as e:
        logger.debug(f"pypdf failed: {e}")

    return ""


def extract_docx_text(data: bytes) -> str:
    """Extract text from a Word document byte stream."""
    try:
        import docx2txt
        with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp:
            tmp.write(data)
            tmp_path = tmp.name
        text = docx2txt.process(tmp_path)
        Path(tmp_path).unlink(missing_ok=True)
        return text.strip() if text else ""
    except Exception as e:
        logger.debug(f"docx2txt failed: {e}")

    try:
        from docx import Document
        doc = Document(io.BytesIO(data))
        return "\n".join(p.text for p in doc.paragraphs if p.text.strip())
    except Exception as e:
        logger.debug(f"python-docx fallback failed: {e}")

    return ""


def extract_pptx_text(data: bytes) -> str:
    """Extract all text from a PowerPoint byte stream."""
    try:
        from pptx import Presentation
        prs = Presentation(io.BytesIO(data))
        slides = []
        for i, slide in enumerate(prs.slides, 1):
            parts = [f"Slide {i}:"]
            for shape in slide.shapes:
                if hasattr(shape, "text") and shape.text.strip():
                    parts.append(shape.text.strip())
            slides.append("\n".join(parts))
        return "\n\n".join(slides).strip()
    except Exception as e:
        logger.debug(f"pptx extraction failed: {e}")
    return ""


def extract_html_text(html: str) -> str:
    """Strip HTML tags and return clean readable text."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        lines = [l.strip() for l in soup.get_text(separator="\n").splitlines() if l.strip()]
        return "\n".join(lines)
    except Exception as e:
        logger.debug(f"HTML extraction failed: {e}")
    return html[:5000]


def extract_text_from_bytes(data: bytes, content_type: str, filename: str = "") -> str:
    """Route to the right extractor based on content type and filename."""
    ct = content_type.lower()
    fn = filename.lower()

    if "pdf" in ct or fn.endswith(".pdf"):
        return extract_pdf_text(data)
    if "wordprocessingml" in ct or fn.endswith(".docx"):
        return extract_docx_text(data)
    if "presentationml" in ct or fn.endswith(".pptx"):
        return extract_pptx_text(data)
    if "text/plain" in ct or fn.endswith(".txt"):
        return data.decode("utf-8", errors="replace")
    if "html" in ct:
        return extract_html_text(data.decode("utf-8", errors="replace"))

    text = extract_pdf_text(data)
    if text:
        return text
    text = extract_docx_text(data)
    if text:
        return text

    logger.warning(f"Could not extract text from {filename} ({content_type})")
    return ""


# ── DocumentIngester ─────────────────────────────────────────────────

# Type alias for the rate-limited goto callable passed in from the crawler
GotoFn = Callable[..., Coroutine]


class DocumentIngester:
    """
    Downloads and extracts text from all documents linked in Canvas.
    Uses the Playwright browser session (already authenticated) for Canvas files
    and httpx for external resources.
    """

    def __init__(self, page, base_url: str, goto_fn: Optional[GotoFn] = None):
        """
        page:     Playwright Page object (authenticated Canvas session)
        base_url: Canvas base URL e.g. https://wilmu.instructure.com
        goto_fn:  Rate-limited navigation coroutine (page, url) -> None.
                  If omitted, falls back to a plain page.goto with a 1s delay.
                  RF-Ingester-bypass: always pass this from CanvasCrawler._polite_goto
                  so the same rate-limiting applies to document ingestion.
        """
        self.page = page
        self.base_url = base_url
        self._goto_fn = goto_fn
        self.results: list[dict] = []
        self.flagged: list[dict] = []
        # RF-Dedup + RF-Recursion: single set shared across all discovery passes
        self._seen_urls: set[str] = set()

    async def _goto(self, url: str) -> None:
        """Navigate using the provided rate-limited helper, or a safe fallback."""
        if self._goto_fn:
            await self._goto_fn(self.page, url)
        else:
            await asyncio.sleep(1.0)
            await self.page.goto(url, wait_until="networkidle")

    # ------------------------------------------------------------------ #
    #  Public entry point                                                  #
    # ------------------------------------------------------------------ #

    async def ingest_course_documents(self, course_id: str, course_name: str) -> dict:
        """
        Discover and ingest all documents for one course.
        Returns dict with 'ingested' (list of text results) and 'flagged' (external links).
        """
        self.results = []
        self.flagged = []
        self._seen_urls = set()  # reset per course

        await self._ingest_files_page(course_id, course_name)
        await self._ingest_pages(course_id, course_name)
        await self._ingest_module_items(course_id, course_name)

        logger.info(
            f"Course {course_name}: ingested {len(self.results)} documents, "
            f"flagged {len(self.flagged)} external links"
        )
        return {"ingested": self.results, "flagged": self.flagged}

    # ------------------------------------------------------------------ #
    #  Discovery methods                                                   #
    # ------------------------------------------------------------------ #

    async def _ingest_files_page(self, course_id: str, course_name: str) -> None:
        """Download every file listed in the Canvas Files section."""
        try:
            await self._goto(f"{self.base_url}/courses/{course_id}/files")
            try:
                await self.page.wait_for_selector(
                    "a.ef-name-col__link, tr.ef-item-row, .ef-directory", timeout=10000
                )
            except Exception:
                await self.page.wait_for_timeout(3000)

            file_links = await self.page.query_selector_all(
                "a.ef-name-col__link, tr.ef-item-row a[href*='/files/'], a[href*='/download']"
            )

            seen_hrefs: set[str] = set()
            for link in file_links:
                href = await link.get_attribute("href") or ""
                name = (await link.inner_text()).strip()
                if not href or href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                full_url = href if href.startswith("http") else f"{self.base_url}{href}"
                await self._process_url(full_url, name, course_name, source="files")

        except Exception as e:
            logger.warning(f"Error ingesting files page for {course_name}: {e}")

    async def _ingest_pages(self, course_id: str, course_name: str) -> None:
        """Read all instructor-created Canvas pages for embedded links and content."""
        try:
            await self._goto(f"{self.base_url}/courses/{course_id}/pages")
            try:
                await self.page.wait_for_selector(
                    "a.wiki-page-link, .pages-index, table.index_content", timeout=8000
                )
            except Exception:
                await self.page.wait_for_timeout(2000)

            page_links = await self.page.query_selector_all(
                "a.wiki-page-link, "
                "table.index_content a[href*='/pages/'], "
                "a[href*='/courses/'][href*='/pages/']"
            )

            seen_hrefs: set[str] = set()
            for link in page_links:
                href = await link.get_attribute("href") or ""
                if not href or href in seen_hrefs:
                    continue
                # Skip template/placeholder hrefs
                if "{{" in href or "}}" in href:
                    continue
                seen_hrefs.add(href)
                full_url = href if href.startswith("http") else f"{self.base_url}{href}"
                # RF-Dedup: checked inside _scrape_canvas_page
                await self._scrape_canvas_page(full_url, course_name)

        except Exception as e:
            logger.warning(f"Error ingesting pages for {course_name}: {e}")

    async def _ingest_module_items(self, course_id: str, course_name: str) -> None:
        """Walk all module items and process external URLs and file links."""
        try:
            await self._goto(f"{self.base_url}/courses/{course_id}/modules")
            try:
                await self.page.wait_for_selector(".context_module_item", timeout=8000)
            except Exception:
                await self.page.wait_for_timeout(2000)
            # Extra settle time for React to finish populating item hrefs.
            # Without this, ExternalTool items have {{ id }} Handlebars placeholders.
            await self.page.wait_for_timeout(3000)

            items = await self.page.query_selector_all(".context_module_item")

            for item in items:
                link = await item.query_selector("a.external_url_link, a[href*='external']")
                if not link:
                    link = await item.query_selector("a.title")
                if not link:
                    continue

                href = await link.get_attribute("href") or ""
                title = (await link.inner_text()).strip()

                if not href:
                    continue

                # Skip unrendered Handlebars template hrefs — React hasn't populated them yet
                if "{{" in href or "}}" in href:
                    logger.debug(f"Skipping unrendered module item href: {href!r}")
                    continue

                full_url = href if href.startswith("http") else f"{self.base_url}{href}"

                if self.base_url in full_url and not any(
                    k in full_url for k in ["/files/", "/download", "/pages/", "/external"]
                ):
                    continue

                await self._process_url(full_url, title, course_name, source="module")

        except Exception as e:
            logger.warning(f"Error ingesting module items for {course_name}: {e}")

    # ------------------------------------------------------------------ #
    #  Processing                                                          #
    # ------------------------------------------------------------------ #

    async def _process_url(self, url: str, title: str, course_name: str, source: str) -> None:
        """
        Classify a URL and route it to the appropriate handler.
        RF-Dedup: skip any URL already processed this course to prevent
        duplicate ingestion and break page-embed cycles.
        """
        # RF-Dedup + RF-Recursion: global guard for this course's crawl
        if url in self._seen_urls:
            return
        self._seen_urls.add(url)

        url_type = classify_url(url)

        if url_type == "external_platform":
            platform = detect_external_platform(url)
            logger.info(f"Flagging external platform: {platform} -- {title}")
            self.flagged.append({
                "title": title,
                "url": url,
                "platform": platform,
                "course_name": course_name,
                "source": source,
                "note": f"Manual upload required -- {platform} requires separate login",
            })

        elif url_type == "canvas_file":
            await self._download_canvas_file(url, title, course_name, source)

        elif url_type in ("google_doc", "microsoft_doc"):
            await self._fetch_embedded_doc(url, title, course_name, source)

        elif url_type == "web_page":
            await self._fetch_web_page(url, title, course_name, source)

    async def _scrape_canvas_page(self, url: str, course_name: str) -> None:
        """
        Scrape a Canvas wiki page: extract text and find embedded links.
        RF-Recursion: _seen_urls prevents cycles when pages link to each other.
        """
        # RF-Dedup: guard at entry so repeated calls from different discovery
        # passes are all caught in one place
        if url in self._seen_urls:
            return
        self._seen_urls.add(url)

        try:
            await self._goto(url)
            await asyncio.sleep(0.5)

            title_el = await self.page.query_selector("h1.page-title, h1")
            title = (await title_el.inner_text()).strip() if title_el else "Canvas Page"

            body_el = await self.page.query_selector(".show-content, #wiki_page_show")
            if body_el:
                text = await body_el.inner_text()
                if text.strip():
                    self._store_result(title, text.strip(), url, course_name, "page", "html_page")

            links = await self.page.query_selector_all(
                ".show-content a[href], #wiki_page_show a[href]"
            )
            for link in links:
                href = await link.get_attribute("href") or ""
                link_title = (await link.inner_text()).strip() or title
                if not href or href.startswith("#"):
                    continue
                full_url = href if href.startswith("http") else f"{self.base_url}{href}"
                await self._process_url(full_url, link_title, course_name, source="page_embed")

        except Exception as e:
            logger.warning(f"Error scraping Canvas page {url}: {e}")

    async def _download_canvas_file(
        self, url: str, title: str, course_name: str, source: str
    ) -> None:
        """
        Download a Canvas-hosted file using Playwright's API client.

        RF-CDP-bytes: uses page.context.request.get() instead of JS
        Array.from(new Uint8Array(buffer)) via CDP. The CDP approach
        serialises the entire file as a JSON integer array, which is
        ~3x the file size in memory and extremely slow for PDFs > a few MB.
        Playwright's API client shares the browser's authenticated cookies
        without going through the CDP protocol.

        RF-FileSizeLimit: a HEAD request checks Content-Length before
        downloading, skipping files larger than MAX_FILE_BYTES (15 MB).
        """
        try:
            logger.info(f"Downloading Canvas file: {title}")

            # RF-FileSizeLimit: check size before committing to download.
            # timeout=10000 ms prevents indefinite hangs on slow Canvas responses.
            head_resp = await self.page.context.request.fetch(
                url, method="HEAD", timeout=10000
            )
            content_length = int(head_resp.headers.get("content-length", "0") or "0")
            if content_length > MAX_FILE_BYTES:
                logger.warning(
                    f"Skipping {title}: file is {content_length / 1024 / 1024:.1f} MB "
                    f"(limit {MAX_FILE_BYTES // 1024 // 1024} MB)"
                )
                return

            # RF-CDP-bytes: Playwright API client download — no CDP overhead.
            # RF-DownloadTimeout: 30 s ceiling so a stalled file server can't
            # hang the entire crawl indefinitely.
            response = await self.page.context.request.get(url, timeout=30000)
            if not response.ok:
                logger.warning(f"Failed to download {title}: HTTP {response.status}")
                return

            data = await response.body()
            if not data:
                logger.warning(f"Empty file downloaded: {title}")
                return

            content_type = response.headers.get("content-type", "")
            filename = Path(urlparse(url).path).name
            text = extract_text_from_bytes(data, content_type, filename)

            if text:
                self._store_result(title, text, url, course_name, source, "canvas_file")
                logger.info(f"Extracted {len(text):,} chars from: {title}")
            else:
                logger.warning(f"No text extracted from: {title} ({content_type})")

        except Exception as e:
            logger.warning(f"Error downloading Canvas file {title}: {e}", exc_info=True)

    async def _fetch_embedded_doc(
        self, url: str, title: str, course_name: str, source: str
    ) -> None:
        """Attempt to fetch a Google Doc or Microsoft document."""
        try:
            if "docs.google.com" in url:
                doc_id_match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
                if doc_id_match:
                    doc_id = doc_id_match.group(1)
                    export_url = (
                        f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
                    )
                    text = await self._http_get_text(export_url)
                    if text:
                        self._store_result(title, text, url, course_name, source, "google_doc")
                        return

            if "drive.google.com" in url:
                file_id_match = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
                if file_id_match:
                    file_id = file_id_match.group(1)
                    export_url = (
                        f"https://drive.google.com/uc?export=download&id={file_id}"
                    )
                    data, ct = await self._http_get_bytes(export_url)
                    if data:
                        text = extract_text_from_bytes(data, ct)
                        if text:
                            self._store_result(
                                title, text, url, course_name, source, "google_drive"
                            )
                            return

            if any(h in url for h in MICROSOFT_HOSTS):
                await self._goto(url)
                await asyncio.sleep(2.0)
                body_el = await self.page.query_selector("body")
                if body_el:
                    text = await body_el.inner_text()
                    if len(text.strip()) > 100:
                        self._store_result(
                            title, text.strip(), url, course_name, source, "microsoft_doc"
                        )
                        return

            logger.info(f"Could not auto-extract embedded doc: {title} -- flagging")
            self.flagged.append({
                "title": title,
                "url": url,
                "platform": "Embedded Document",
                "course_name": course_name,
                "source": source,
                "note": "Could not auto-extract -- upload manually if critical",
            })

        except Exception as e:
            logger.warning(f"Error fetching embedded doc {title}: {e}")

    async def _fetch_web_page(
        self, url: str, title: str, course_name: str, source: str
    ) -> None:
        """Fetch and extract text from a generic web page."""
        try:
            text = await self._http_get_text(url)
            if text and len(text) > 200:
                self._store_result(title, text[:50000], url, course_name, source, "web_page")
        except Exception as e:
            logger.debug(f"Could not fetch web page {url}: {e}")

    # ------------------------------------------------------------------ #
    #  HTTP helpers                                                        #
    # ------------------------------------------------------------------ #

    async def _http_get_text(self, url: str) -> str:
        """GET a URL and return its text content. RF-httpx-import: httpx at module level."""
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                if resp.status_code == 200:
                    ct = resp.headers.get("content-type", "")
                    return extract_html_text(resp.text) if "html" in ct else resp.text
        except Exception as e:
            logger.debug(f"HTTP GET failed for {url}: {e}")
        return ""

    async def _http_get_bytes(self, url: str) -> tuple[bytes, str]:
        """GET a URL and return raw bytes plus content-type. RF-httpx-import: module level."""
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                if resp.status_code == 200:
                    return resp.content, resp.headers.get("content-type", "")
        except Exception as e:
            logger.debug(f"HTTP bytes GET failed for {url}: {e}")
        return b"", ""

    # ------------------------------------------------------------------ #
    #  Storage                                                             #
    # ------------------------------------------------------------------ #

    def _store_result(
        self,
        title: str,
        text: str,
        url: str,
        course_name: str,
        source: str,
        doc_type: str,
    ) -> None:
        """Append an extracted document to the results list."""
        self.results.append({
            "title": title,
            "text": text,
            "url": url,
            "course_name": course_name,
            "source": source,
            "doc_type": doc_type,
            "char_count": len(text),
        })
