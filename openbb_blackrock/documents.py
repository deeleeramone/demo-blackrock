"""Fund-document persistence and PDF retrieval.

Two responsibilities:

* Offline (ingestion-time): walk the iShares US document library and write
  per-ticker ``(slug, label, url)`` rows into the ``fund_documents`` table.
  This makes request-time lookups instant — only the actual PDF download
  is on demand.

* Request-time: ``fetch_pdf_bytes`` downloads a single PDF (with on-disk
  cache) and returns raw bytes ready for base64 encoding.

Crawling the BlackRock site is a heavy operation handled in ``app.py``;
ingestion reuses ``_build_and_cache_us_index`` for that one-shot work and
then projects the in-memory index into the SQLite table.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from urllib.parse import parse_qs, unquote, urlparse

import httpx

from .db import DB_PATH, replace_documents_for_fund

log = logging.getLogger(__name__)

_PDF_CACHE_DIR = DB_PATH.parent / "pdf_cache"
_PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_HDRS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


def _extract_fund_documents(html: str) -> dict[str, dict]:
    """Extract literature PDF links from a raw iShares product page.

    Returns ``{slug: {"label": str, "url": str}}``, e.g.::

        {"fact-sheet": {"label": "Fact Sheet", "url": "https://..."}}
    """
    docs: dict[str, dict] = {}
    for m in re.finditer(
        r'<a\b[^>]*href=["\']('
        r'https?://www\.ishares\.com/us/literature/[^"\']+\.pdf[^"\']*'
        r')["\'][^>]*>(.*?)</a>',
        html,
        re.IGNORECASE | re.DOTALL,
    ):
        url = m.group(1).replace("&amp;", "&")
        raw = re.sub(r"<[^>]+>", "", m.group(2))
        raw = re.sub(r"\s+", " ", raw).strip()
        label = (
            re.sub(
                r"\s*PDF\s*,?\s*opens\s+in\s+a\s+new\s+tab\s*$",
                "",
                raw,
                flags=re.IGNORECASE,
            ).strip()
            or raw
            or "Document"
        )
        slug_m = re.search(r"/literature/([^/?#]+)/", url)
        slug = slug_m.group(1) if slug_m else "document"
        if slug in docs:
            slug = f"{slug}-{len(docs)}"
        docs[slug] = {"label": label, "url": url}
    return docs


def populate_us_documents(conn: sqlite3.Connection) -> int:
    """Fetch each iShares fund product page and persist its document links.

    Iterates every iShares fund row in the ``funds`` table that has a
    ``product_page_url``, GETs the page, extracts PDF links from the
    ``/us/literature/`` section, and writes them to ``fund_documents``.

    Returns the total number of document rows written.
    """
    cur = conn.execute(
        "SELECT portfolio_id, ticker, product_page_url FROM funds "
        "WHERE portfolio = 'iShares' AND ticker IS NOT NULL "
        "  AND product_page_url IS NOT NULL"
    )
    all_funds = cur.fetchall()
    log.info("populating documents for %d iShares funds ...", len(all_funds))
    written = 0
    with httpx.Client(headers=_HDRS, follow_redirects=True, timeout=30.0) as client:
        for i, (pid, ticker, page_url) in enumerate(all_funds, 1):
            if not page_url.startswith("http"):
                if "/us/individual" in page_url:
                    page_url = "https://www.ishares.com" + page_url.replace(
                        "/us/individual", "/us"
                    )
                else:
                    page_url = "https://www.ishares.com" + page_url
            try:
                resp = client.get(page_url)
                if resp.status_code != 200:
                    log.warning(
                        "  [%d/%d] %s: HTTP %s — skipping",
                        i,
                        len(all_funds),
                        ticker,
                        resp.status_code,
                    )
                    continue
                docs = _extract_fund_documents(resp.text)
            except Exception as exc:
                log.warning("  [%d/%d] %s: %s", i, len(all_funds), ticker, exc)
                continue
            n = replace_documents_for_fund(conn, pid, ticker, docs)
            written += n
            log.info("  [%d/%d] %s: %d documents", i, len(all_funds), ticker, n)
    log.info("populated %d fund_documents rows", written)
    return written


def _direct_pdf_url(url: str) -> str:
    """For stream=reg URLs, extract the direct PDF path from iframeUrlOverride."""
    if "stream=reg" not in url:
        return url
    params = parse_qs(urlparse(url).query)
    override = params.get("iframeUrlOverride", [None])[0]
    if override:
        path = unquote(override).replace("//", "/")
        return f"https://www.ishares.com{path}"
    return url


async def fetch_pdf_bytes(url: str, cache_key: str) -> bytes:
    """Download a PDF (with on-disk cache) and return raw bytes."""
    p = _PDF_CACHE_DIR / f"{cache_key}.pdf"
    if p.exists():
        data = p.read_bytes()
        if data[:4] == b"%PDF":
            return data
        p.unlink()
    url = _direct_pdf_url(url)
    async with httpx.AsyncClient(
        headers=_HDRS, follow_redirects=True, timeout=60.0
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "")
        if "pdf" not in ct:
            raise ValueError(f"Expected PDF, got {ct!r}")
        p.write_bytes(resp.content)
        return resp.content
