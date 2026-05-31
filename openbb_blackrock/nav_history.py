"""Daily NAV history + Distributions fetcher.

For each iShares fund, BlackRock's ``1521942788811.ajax`` endpoint serves
an Office 2003 SpreadsheetML workbook with multiple sheets, including:

* ``Historical`` — daily NAV per share, shares outstanding, ex-dividends.
  Typically ~5,000 rows / ~20 years.
* ``Distributions`` — every distribution paid to shareholders, broken
  down by type (Income / ST Cap Gains / LT Cap Gains / Return of Capital).

This module downloads the workbook once and parses both sheets in a
single pass, populating both ``nav_history`` and ``distributions``.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import httpx

log = logging.getLogger(__name__)

NAV_HISTORY_AJAX_ID = "1521942788811"
_HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "*/*",
}
_TIMEOUT = 60.0


@dataclass(frozen=True)
class NavRow:
    portfolio_id: str
    as_of_date: str  # ISO YYYY-MM-DD
    nav_per_share: float
    shares_outstanding: float | None
    ex_dividends: float | None


@dataclass(frozen=True)
class DistributionRow:
    portfolio_id: str
    ex_date: str  # ISO
    record_date: str | None
    payable_date: str | None
    total_distribution: float
    income: float | None
    st_cap_gains: float | None
    lt_cap_gains: float | None
    return_of_capital: float | None


_DATE_FMTS = ("%b %d, %Y", "%d-%b-%Y", "%d/%b/%Y", "%Y-%m-%d")


def _parse_date(s: str) -> str | None:
    s = (s or "").strip()
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _to_float(s: str | None) -> float | None:
    if s is None:
        return None
    s = s.strip().replace(",", "")
    if not s or s in ("-", "--", "—"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


_ROW_RE = re.compile(r"<ss:Row[^>]*>(.*?)</ss:Row>", re.DOTALL)
_DATA_RE = re.compile(r"<ss:Data[^>]*>([^<]*)</ss:Data>")
_HISTORICAL_RE = re.compile(
    r'ss:Name="Historical"[^>]*>(.*?)</ss:Worksheet>', re.DOTALL
)
_DISTRIBUTIONS_RE = re.compile(
    r'ss:Name="Distributions"[^>]*>(.*?)</ss:Worksheet>', re.DOTALL
)
_FUND_DOWNLOAD_HREF_RE = re.compile(
    r'href="([^"]*get-fund-document[^"]*component=fundDownload[^"]*)"',
    re.IGNORECASE,
)


def _build_fund_download_url(portfolio_id: str) -> str:
    return (
        "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document"
        f"?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares"
        f"&locale=en_US&portfolioId={portfolio_id}&component=fundDownload&userType=individual"
    )


def _resolve_nav_url(client: httpx.Client, product_page_url: str) -> str | None:
    """Scrape the product page to find the fundDownload API href."""
    try:
        r = client.get(product_page_url, timeout=_TIMEOUT)
    except httpx.HTTPError:
        return None
    if r.status_code != 200:
        return None
    m = _FUND_DOWNLOAD_HREF_RE.search(r.text)
    if not m:
        return None
    return m.group(1).replace("&amp;", "&")


def _parse_historical_sheet(portfolio_id: str, sheet: str) -> list[NavRow]:
    out: list[NavRow] = []
    seen_header = False
    for row_xml in _ROW_RE.findall(sheet):
        cells = _DATA_RE.findall(row_xml)
        if not cells:
            continue
        if not seen_header:
            if any("nav" in c.lower() and "share" in c.lower() for c in cells):
                seen_header = True
            continue
        iso = _parse_date(cells[0]) if cells else None
        nav = _to_float(cells[1] if len(cells) > 1 else None)
        if iso is None or nav is None:
            continue
        ex_div = _to_float(cells[2] if len(cells) > 2 else None)
        shares = _to_float(cells[3] if len(cells) > 3 else None)
        out.append(
            NavRow(
                portfolio_id=portfolio_id,
                as_of_date=iso,
                nav_per_share=nav,
                shares_outstanding=shares,
                ex_dividends=ex_div,
            )
        )
    return out


def _parse_distributions_sheet(portfolio_id: str, sheet: str) -> list[DistributionRow]:
    """Distributions sheet schema:

    Record Date | Ex-Date | Payable Date | Total Distribution |
    Income | ST Cap Gains | LT Cap Gains | Return of Capital
    """
    out: list[DistributionRow] = []
    seen_header = False
    for row_xml in _ROW_RE.findall(sheet):
        cells = _DATA_RE.findall(row_xml)
        if not cells:
            continue
        if not seen_header:
            joined = " ".join(c.lower() for c in cells)
            if "ex-date" in joined or "ex date" in joined:
                seen_header = True
            continue
        if len(cells) < 4:
            continue
        record = _parse_date(cells[0])
        ex_date = _parse_date(cells[1])
        payable = _parse_date(cells[2]) if len(cells) > 2 else None
        if ex_date is None:
            continue
        total = _to_float(cells[3]) if len(cells) > 3 else None
        if total is None:
            continue
        out.append(
            DistributionRow(
                portfolio_id=portfolio_id,
                ex_date=ex_date,
                record_date=record,
                payable_date=payable,
                total_distribution=total,
                income=_to_float(cells[4]) if len(cells) > 4 else None,
                st_cap_gains=_to_float(cells[5]) if len(cells) > 5 else None,
                lt_cap_gains=_to_float(cells[6]) if len(cells) > 6 else None,
                return_of_capital=_to_float(cells[7]) if len(cells) > 7 else None,
            )
        )
    return out


def fetch_workbook(
    portfolio_id: str,
    product_page_url: str,
    *,
    client: httpx.Client | None = None,
) -> tuple[list[NavRow], list[DistributionRow]]:
    """Fetch the XLS workbook once and parse both Historical + Distributions
    sheets.  Returns ``(nav_rows, distribution_rows)``.
    """
    own = client is None
    if own:
        client = httpx.Client(headers=_HDRS, follow_redirects=True, timeout=_TIMEOUT)
    try:
        # Discover the XLS URL from the product page (and warm session cookies),
        # the same way holdings.py discovers the CSV URL.
        url = _resolve_nav_url(client, product_page_url)
        if url is None:
            url = _build_fund_download_url(portfolio_id)
        try:
            r = client.get(url, headers={"Referer": product_page_url})
        except httpx.HTTPError as exc:
            log.warning("workbook fetch %s failed: %s", portfolio_id, exc)
            return [], []
        if r.status_code != 200 or not r.text:
            log.warning(
                "workbook %s: status=%s len=%s url=%s — skipped",
                portfolio_id,
                r.status_code,
                len(r.text or ""),
                url,
            )
            return [], []
        text = r.text.lstrip("﻿").lstrip("﻿")

        if not _HISTORICAL_RE.search(text) and not _DISTRIBUTIONS_RE.search(text):
            log.warning(
                "workbook %s: 200 OK but no Historical/Distributions worksheets found; first 500 chars: %r",
                portfolio_id,
                text[:500],
            )
            return [], []

        nav_rows: list[NavRow] = []
        h = _HISTORICAL_RE.search(text)
        if h:
            nav_rows = _parse_historical_sheet(portfolio_id, h.group(1))

        dist_rows: list[DistributionRow] = []
        d = _DISTRIBUTIONS_RE.search(text)
        if d:
            dist_rows = _parse_distributions_sheet(portfolio_id, d.group(1))

        return nav_rows, dist_rows
    finally:
        if own and client is not None:
            client.close()


# Backward-compatible alias kept for any existing callers.
def fetch_nav_history(
    portfolio_id: str,
    product_page_url: str,
    *,
    client: httpx.Client | None = None,
) -> list[NavRow]:
    nav, _ = fetch_workbook(portfolio_id, product_page_url, client=client)
    return nav


# ---------------------------------------------------------------------------
# DB write helpers
# ---------------------------------------------------------------------------


def replace_nav_history(
    conn: sqlite3.Connection, portfolio_id: str, rows: Iterable[NavRow]
) -> int:
    """Replace ``nav_history`` rows for a fund.  Computes daily_return_pct
    based on consecutive NAV values (sorted ascending).
    """
    rs = sorted(rows, key=lambda r: r.as_of_date)
    if not rs:
        return 0

    # Compute daily total return adjusted for ex-dividends:
    #   r_t = (NAV_t + div_t) / NAV_{t-1} - 1
    enriched: list[tuple] = []
    prev_nav: float | None = None
    for r in rs:
        if prev_nav is None or prev_nav == 0:
            ret = None
        else:
            div = r.ex_dividends or 0.0
            ret = ((r.nav_per_share + div) / prev_nav - 1.0) * 100.0
        enriched.append(
            (
                r.portfolio_id,
                r.as_of_date,
                r.nav_per_share,
                r.shares_outstanding,
                r.ex_dividends,
                ret,
            )
        )
        prev_nav = r.nav_per_share

    conn.execute("DELETE FROM nav_history WHERE portfolio_id = ?", (portfolio_id,))
    conn.executemany(
        """INSERT INTO nav_history (
              portfolio_id, as_of_date, nav_per_share,
              shares_outstanding, ex_dividends, daily_return_pct
           ) VALUES (?,?,?,?,?,?)""",
        enriched,
    )
    return len(enriched)


def replace_distributions_for_fund(
    conn: sqlite3.Connection, portfolio_id: str, rows: Iterable[DistributionRow]
) -> int:
    rs = list(rows)
    conn.execute("DELETE FROM distributions WHERE portfolio_id = ?", (portfolio_id,))
    if not rs:
        return 0
    conn.executemany(
        """INSERT OR REPLACE INTO distributions (
              portfolio_id, ex_date, record_date, payable_date,
              total_distribution, income, st_cap_gains,
              lt_cap_gains, return_of_capital
           ) VALUES (?,?,?,?,?,?,?,?,?)""",
        [
            (
                r.portfolio_id,
                r.ex_date,
                r.record_date,
                r.payable_date,
                r.total_distribution,
                r.income,
                r.st_cap_gains,
                r.lt_cap_gains,
                r.return_of_capital,
            )
            for r in rs
        ],
    )
    return len(rs)


def ingest_nav_history_batch(
    conn: sqlite3.Connection,
    funds: Iterable[tuple[str, str]],  # (portfolio_id, product_page_url)
    *,
    progress_every: int = 25,
) -> dict:
    """Pull NAV history + distributions for many funds in one XLS fetch each."""
    n_funds = 0
    n_nav_rows = 0
    n_dist_rows = 0
    n_failed = 0
    with httpx.Client(headers=_HDRS, follow_redirects=True, timeout=_TIMEOUT) as client:
        for pid, url in funds:
            n_funds += 1
            try:
                nav_rows, dist_rows = fetch_workbook(pid, url, client=client)
            except Exception as exc:
                log.warning("workbook fetch %s failed: %s", pid, exc)
                nav_rows, dist_rows = [], []
            if not nav_rows and not dist_rows:
                n_failed += 1
                continue
            if nav_rows:
                n_nav_rows += replace_nav_history(conn, pid, nav_rows)
            if dist_rows:
                n_dist_rows += replace_distributions_for_fund(conn, pid, dist_rows)
            if n_funds % progress_every == 0:
                log.info(
                    "nav_history progress: %d funds processed, %d nav rows, %d distributions",
                    n_funds,
                    n_nav_rows,
                    n_dist_rows,
                )
    return {
        "funds_processed": n_funds,
        "nav_rows_written": n_dist_rows if False else n_nav_rows,
        "distribution_rows_written": n_dist_rows,
        "failed": n_failed,
    }
