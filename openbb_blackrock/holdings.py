"""Holdings fetcher + parser for BlackRock public fund pages.

Three portfolios are supported (matching ``_HOLDINGS_URL_TEMPLATES`` in
``app.py``):

* **iShares** — CSV holdings download from ishares.com/us product pages.
* **US Onshore** — CSV holdings download from blackrock.com/us non-iShares
  product pages (mutual funds + active ETFs; mutual funds list top-N only).
* **US Offshore** — Office 2003 XML SpreadsheetML workbook with a
  ``Holdings`` worksheet, served from blackrock.com/uk, /lu, and /cn.

Every original CSV / XLS column is preserved verbatim in :attr:`Holding.raw`
(JSON-serialized in the DB) so no information is lost.  Indexed Holding
fields cover the most common queries; ad-hoc analytics can read the raw
dict directly.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from urllib.parse import urlencode
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable, Iterator

import httpx

log = logging.getLogger(__name__)

_HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
}
_TIMEOUT = 60.0
_SS_NS = "urn:schemas-microsoft-com:office:spreadsheet"

# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------


@dataclass
class Holding:
    parent_portfolio_id: str
    parent_ticker: str | None
    portfolio: str  # iShares | US Onshore | US Offshore
    holding_id: str | None
    holding_ticker: str | None
    holding_name: str
    holding_type: str
    holding_isin: str | None
    holding_cusip: str | None
    holding_sedol: str | None
    sector: str | None
    country: str | None  # Location column
    exchange: str | None
    currency: str  # Market Currency (where security trades)
    report_currency: str  # Currency column (fund's reporting ccy)
    shares_or_par: float | None
    price: float | None
    market_value_local: float
    notional_value: float | None
    market_value_usd: float
    weight_pct: float
    fx_rate: float | None
    # Bond-specific fields (None for equities)
    coupon_pct: float | None
    maturity_date: str | None
    duration: float | None
    mod_duration: float | None
    ytm_pct: float | None
    yield_to_call_pct: float | None
    yield_to_worst_pct: float | None
    real_duration: float | None
    real_ytm_pct: float | None
    accrual_date: str | None
    effective_date: str | None
    as_of_date: date
    # Verbatim copy of every CSV/XLS column for this row, header → cell.
    raw: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Discovery: locate the holdings-download href on a product page
# ---------------------------------------------------------------------------


def _build_href_re(ajax_ids: list[str]) -> re.Pattern[str]:
    ids = "|".join(re.escape(i) for i in ajax_ids)
    return re.compile(
        rf'href="([^"]+/(?:{ids})\.ajax\?[^"]+)"',
        re.IGNORECASE,
    )


def _fetch_text(client: httpx.Client, url: str, *, retries: int = 5) -> str | None:
    delay = 5.0
    for attempt in range(retries):
        try:
            r = client.get(url, timeout=_TIMEOUT)
        except httpx.HTTPError as exc:
            if attempt < retries - 1:
                log.warning(
                    "fetch %s failed (attempt %d/%d): %s — retrying in %.0fs",
                    url,
                    attempt + 1,
                    retries,
                    exc,
                    delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 60.0)
                continue
            log.warning("fetch %s failed: %s", url, exc)
            return None
        if r.status_code != 200 or not r.text:
            log.warning("fetch %s status=%s len=%s", url, r.status_code, len(r.text))
            return None
        return r.text
    return None


def _resolve_holdings_url(
    client: httpx.Client,
    product_page_url: str,
    ajax_ids: list[str],
    parent_ticker: str | None,
    expected_filename_suffix: str,
) -> str | None:
    """Return the absolute holdings-download URL or ``None``."""
    page = _fetch_text(client, product_page_url)
    if not page:
        return None

    href_re = _build_href_re(ajax_ids)
    candidates = href_re.findall(page)
    if not candidates:
        return None

    # Prefer a candidate whose `fileName` matches our parent ticker (avoids
    # iShares "related funds" cross-links leaking into the wrong holdings).
    if parent_ticker:
        scoped = [c for c in candidates if parent_ticker.upper() in c.upper()]
        if scoped:
            candidates = scoped
        else:
            # Refusing rather than picking the wrong fund's CSV.
            log.warning(
                "no holdings candidate matches ticker %s on %s — skipping",
                parent_ticker,
                product_page_url,
            )
            return None

    # Prefer a candidate whose fileName ends with our expected suffix.
    suffixed = [c for c in candidates if expected_filename_suffix.lower() in c.lower()]
    if suffixed:
        candidates = suffixed

    href = candidates[0]
    if href.startswith("http"):
        return href
    # Relative — derive base from the product page URL
    base = re.match(r"(https?://[^/]+)", product_page_url)
    return (base.group(1) if base else "") + href


# ---------------------------------------------------------------------------
# CSV parser (iShares + US Onshore)
# ---------------------------------------------------------------------------


_NUM_RE = re.compile(r"^-?[\d,]+(\.\d+)?$")


def _to_float(s: str | None) -> float | None:
    if s is None:
        return None
    s = s.strip().strip('"')
    if not s or s in ("-", "—"):
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _clean(s: str | None) -> str | None:
    if s is None:
        return None
    s = s.strip().strip('"')
    return s or None


def _parse_ishares_csv_as_of_date(csv_text: str) -> date | None:
    for line in csv_text.replace("\r\n", "\n").split("\n")[:20]:
        if not line.lower().startswith("fund holdings as of"):
            continue
        row = next(csv.reader([line]), [])
        if len(row) < 2:
            return None
        try:
            return datetime.strptime(row[1].strip(), "%B %d, %Y").date()
        except ValueError:
            return None
    return None


def _build_ishares_holdings_url(portfolio_id: str, as_of_date: date | None) -> str:
    params = {
        "appType": "PRODUCT_PAGE",
        "appSubType": "ISHARES",
        "targetSite": "us-ishares",
        "locale": "en_US",
        "portfolioId": portfolio_id,
        "userType": "individual",
        "component": "holdings",
    }
    if as_of_date is not None:
        params["asOfDate"] = as_of_date.strftime("%Y%m%d")
    return (
        "https://www.blackrock.com/varnish-api/blk-one01-product-data/"
        "product-data/api/v1/get-fund-document?"
        f"{urlencode(params)}"
    )


def _parse_csv_holdings(
    csv_text: str,
    parent_portfolio_id: str,
    parent_ticker: str | None,
    portfolio: str,
    parent_currency: str,
    as_of_date: date,
) -> list[Holding]:
    """iShares + US Onshore CSV.

    Captures every column verbatim into ``Holding.raw``.  Common columns
    are also exposed as typed dataclass fields for fast querying.

    Header schemas observed (from probe survey):

    * Equity ETF: Ticker, Name, Sector, Asset Class, Market Value,
      Weight (%), Notional Value, Quantity, Price, Location, Exchange,
      Currency, FX Rate, Market Currency, Accrual Date.
    * Bond ETF: Name, Sector, Asset Class, Market Value, Weight (%),
      Notional Value, Par Value, CUSIP, ISIN, SEDOL, [Price], Location,
      Exchange, Currency, Duration, YTM (%), FX Rate, Maturity,
      Coupon (%), Mod. Duration, Yield to Call (%), Yield to Worst (%),
      Real Duration, Real YTM (%), Market Currency, Accrual Date,
      Effective Date.
    """
    lines = csv_text.replace("\r\n", "\n").split("\n")
    header_idx = -1
    for i, line in enumerate(lines):
        low = line.lower()
        if "weight" in low and ("name" in low or "ticker" in low or "cusip" in low):
            header_idx = i
            break
    if header_idx == -1:
        return []

    reader = csv.reader(io.StringIO("\n".join(lines[header_idx:])))
    rows = list(reader)
    if len(rows) < 2:
        return []

    header_raw = [h.strip().strip('"') for h in rows[0]]
    header_lc = [h.lower() for h in header_raw]
    n_cols = len(header_raw)

    def col(*names: str) -> int:
        for n in names:
            if n in header_lc:
                return header_lc.index(n)
        return -1

    # Common columns
    i_name = col("name")
    i_tkr = col("ticker")
    i_sec = col("sector")
    i_ac = col("asset class")
    i_mv = col("market value", "market value (usd)", "market value (gbp)")
    i_wt = col("weight (%)", "weight(%)", "weight")
    i_qty = col("quantity", "shares")
    i_par = col("par value")
    i_price = col("price")
    i_notional = col("notional value")
    i_cusip = col("cusip")
    i_isin = col("isin")
    i_sedol = col("sedol")
    # Currency column = fund's reporting currency.  Market Currency =
    # holding's local trading currency.
    i_report_ccy = col("currency")
    i_market_ccy = col("market currency", "exchange rate currency")
    i_cty = col("location", "country")
    i_exch = col("exchange")
    i_fx = col("fx rate")
    i_coupon = col("coupon (%)", "coupon")
    i_maturity = col("maturity")
    i_duration = col("duration")
    i_modd = col("mod. duration", "mod duration", "modified duration")
    i_ytm = col("ytm (%)", "ytm")
    i_ytc = col("yield to call (%)", "yield to call")
    i_ytw = col("yield to worst (%)", "yield to worst")
    i_realdur = col("real duration")
    i_realytm = col("real ytm (%)", "real ytm")
    i_accrual = col("accrual date")
    i_effective = col("effective date")

    def cell(idx: int, row: list[str]) -> str | None:
        if 0 <= idx < len(row):
            return _clean(row[idx])
        return None

    out: list[Holding] = []
    for r in rows[1:]:
        if not r or all(not c.strip() for c in r):
            continue
        if i_name < 0 or i_name >= len(r):
            continue
        # Pad / trim to header length so raw dict has all keys.
        rr = list(r) + [""] * max(0, n_cols - len(r))
        rr = rr[:n_cols]
        raw = {h: rr[i] for i, h in enumerate(header_raw)}

        name = _clean(rr[i_name])
        if not name:
            continue
        mv_local = _to_float(rr[i_mv]) if 0 <= i_mv else None
        wt = _to_float(rr[i_wt]) if 0 <= i_wt else None
        if mv_local is None and wt is None:
            continue

        report_ccy = (
            cell(i_report_ccy, rr) or parent_currency or ""
        ).upper() or parent_currency
        market_ccy = (cell(i_market_ccy, rr) or report_ccy or "").upper()

        ticker = cell(i_tkr, rr)
        if ticker == "-":
            ticker = None

        shares = (_to_float(rr[i_qty]) if 0 <= i_qty else None) or (
            _to_float(rr[i_par]) if 0 <= i_par else None
        )

        mv_usd = (
            float(mv_local) if mv_local is not None and report_ccy == "USD" else 0.0
        )

        out.append(
            Holding(
                parent_portfolio_id=parent_portfolio_id,
                parent_ticker=parent_ticker,
                portfolio=portfolio,
                holding_id=cell(i_isin, rr) or cell(i_cusip, rr) or ticker,
                holding_ticker=ticker,
                holding_name=name,
                holding_type=cell(i_ac, rr) or "Other",
                holding_isin=cell(i_isin, rr),
                holding_cusip=cell(i_cusip, rr),
                holding_sedol=cell(i_sedol, rr),
                sector=cell(i_sec, rr),
                country=cell(i_cty, rr),
                exchange=cell(i_exch, rr),
                currency=market_ccy,
                report_currency=report_ccy,
                shares_or_par=shares,
                price=_to_float(rr[i_price]) if 0 <= i_price else None,
                market_value_local=mv_local or 0.0,
                notional_value=_to_float(rr[i_notional]) if 0 <= i_notional else None,
                market_value_usd=mv_usd,
                weight_pct=wt or 0.0,
                fx_rate=_to_float(rr[i_fx]) if 0 <= i_fx else None,
                coupon_pct=_to_float(rr[i_coupon]) if 0 <= i_coupon else None,
                maturity_date=cell(i_maturity, rr),
                duration=_to_float(rr[i_duration]) if 0 <= i_duration else None,
                mod_duration=_to_float(rr[i_modd]) if 0 <= i_modd else None,
                ytm_pct=_to_float(rr[i_ytm]) if 0 <= i_ytm else None,
                yield_to_call_pct=_to_float(rr[i_ytc]) if 0 <= i_ytc else None,
                yield_to_worst_pct=_to_float(rr[i_ytw]) if 0 <= i_ytw else None,
                real_duration=_to_float(rr[i_realdur]) if 0 <= i_realdur else None,
                real_ytm_pct=_to_float(rr[i_realytm]) if 0 <= i_realytm else None,
                accrual_date=cell(i_accrual, rr),
                effective_date=cell(i_effective, rr),
                as_of_date=as_of_date,
                raw=raw,
            )
        )
        if len(out) % 5000 == 0:
            log.info("    ... %d rows parsed", len(out))
    return out


# ---------------------------------------------------------------------------
# SpreadsheetML parser (US Offshore + CN offshore)
# ---------------------------------------------------------------------------


def _parse_spreadsheetml_holdings(
    xml_text: str,
    parent_portfolio_id: str,
    parent_ticker: str | None,
    portfolio: str,
    parent_currency: str,
    as_of_date_fallback: date,
) -> list[Holding]:
    """Parse the ``Holdings`` worksheet of a BlackRock fund-data XLS.

    The workbook is BOM-prefixed Office 2003 XML.  We read the ``Holdings``
    worksheet only and discover columns by header keyword.
    """
    text = xml_text.lstrip("﻿")
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        log.warning("XLS parse failed for %s: %s", parent_portfolio_id, exc)
        return []

    ws = None
    for w in root.iter(f"{{{_SS_NS}}}Worksheet"):
        if w.attrib.get(f"{{{_SS_NS}}}Name") == "Holdings":
            ws = w
            break
    if ws is None:
        return []

    rows: list[list[str]] = []
    as_of = as_of_date_fallback
    for row in ws.iter(f"{{{_SS_NS}}}Row"):
        cells = []
        for cell in row.findall(f"{{{_SS_NS}}}Cell"):
            data_el = cell.find(f"{{{_SS_NS}}}Data")
            cells.append((data_el.text or "").strip() if data_el is not None else "")
        if cells:
            rows.append(cells)

    # Find the header row and possibly an "as of" date row above it.
    header_idx = -1
    for i, r in enumerate(rows):
        low = " ".join(r).lower()
        if "weight" in low and ("name" in low or "issuer" in low or "security" in low):
            header_idx = i
            break
        # Capture an as-of date if present anywhere above the header.
        if i < 6 and len(r) == 1:
            try:
                as_of = datetime.strptime(r[0], "%d-%b-%Y").date()
            except ValueError:
                pass
    if header_idx == -1:
        return []

    header_raw = [c.strip() for c in rows[header_idx]]
    header_lc = [c.lower() for c in header_raw]
    n_cols = len(header_raw)

    def col(*names: str) -> int:
        for n in names:
            for j, h in enumerate(header_lc):
                if n in h:
                    return j
        return -1

    i_name = col("name", "issuer name", "security name", "持股名称")
    i_isin = col("isin")
    i_sedol = col("sedol")
    i_cusip = col("cusip")
    i_tkr = col("ticker")
    i_sec = col("sector", "industry")
    i_ac = col("asset class")
    i_mv = col("market value")
    i_wt = col("weight", "比重")
    i_qty = col("quantity", "shares", "nominal", "par value")
    i_ccy = col("market currency", "currency")
    i_cty = col("country", "location")

    def cell(idx: int, row: list[str]) -> str | None:
        if 0 <= idx < len(row):
            v = row[idx].strip()
            return v or None
        return None

    out: list[Holding] = []
    for r in rows[header_idx + 1 :]:
        if not r or all(not c for c in r):
            continue
        rr = list(r) + [""] * max(0, n_cols - len(r))
        rr = rr[:n_cols]
        raw = {h: rr[i] for i, h in enumerate(header_raw)}

        name = cell(i_name, rr)
        if not name:
            continue
        mv = _to_float(rr[i_mv]) if 0 <= i_mv else None
        wt = _to_float(rr[i_wt]) if 0 <= i_wt else None
        if mv is None and wt is None:
            continue
        ccy = (cell(i_ccy, rr) or parent_currency or "").upper()

        ticker = cell(i_tkr, rr)
        if ticker == "-":
            ticker = None

        out.append(
            Holding(
                parent_portfolio_id=parent_portfolio_id,
                parent_ticker=parent_ticker,
                portfolio=portfolio,
                holding_id=cell(i_isin, rr) or cell(i_cusip, rr) or ticker,
                holding_ticker=ticker,
                holding_name=name,
                holding_type=cell(i_ac, rr) or "Other",
                holding_isin=cell(i_isin, rr),
                holding_cusip=cell(i_cusip, rr),
                holding_sedol=cell(i_sedol, rr),
                sector=cell(i_sec, rr),
                country=cell(i_cty, rr),
                exchange=None,
                currency=ccy,
                report_currency=parent_currency,
                shares_or_par=_to_float(rr[i_qty]) if 0 <= i_qty else None,
                price=None,
                market_value_local=mv or 0.0,
                notional_value=None,
                market_value_usd=float(mv)
                if mv is not None and parent_currency == "USD"
                else 0.0,
                weight_pct=wt or 0.0,
                fx_rate=None,
                coupon_pct=None,
                maturity_date=None,
                duration=None,
                mod_duration=None,
                ytm_pct=None,
                yield_to_call_pct=None,
                yield_to_worst_pct=None,
                real_duration=None,
                real_ytm_pct=None,
                accrual_date=None,
                effective_date=None,
                as_of_date=as_of,
                raw=raw,
            )
        )
        if len(out) % 5000 == 0:
            log.info("    ... %d rows parsed", len(out))
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FundRef:
    """A fund whose holdings we want to ingest."""

    portfolio_id: str
    ticker: str | None
    name: str
    currency: str
    portfolio: str
    product_page_url: str  # absolute URL


def fetch_fund_holdings(
    fund: FundRef,
    *,
    template: dict[str, Any],
    client: httpx.Client | None = None,
    today: date | None = None,
    as_of_date: date | None = None,
) -> list[Holding]:
    """Fetch and parse holdings for a single fund.

    Returns an empty list if the holdings link is missing or unreadable —
    callers decide whether to log/skip/retry.

    When ``as_of_date`` is supplied, the BlackRock CSV endpoint is invoked
    with ``&asOfDate=YYYYMMDD`` to retrieve a historical snapshot.  Missing
    snapshots come back as ~3-byte stubs and are returned as ``[]``.
    """
    today = today or date.today()
    as_of = as_of_date or today
    own_client = client is None
    if own_client:
        client = httpx.Client(headers=_HDRS, follow_redirects=True, timeout=_TIMEOUT)
    try:
        if fund.portfolio.lower() == "ishares" and template["format"] == "csv":
            body = _fetch_text(
                client, _build_ishares_holdings_url(fund.portfolio_id, as_of_date)
            )
            if not body or len(body) < 1024:
                return []
            log.info("    downloaded %.1f KB, parsing ...", len(body) / 1024)
            served_as_of = _parse_ishares_csv_as_of_date(body) or as_of
            return _parse_csv_holdings(
                body,
                fund.portfolio_id,
                fund.ticker,
                fund.portfolio,
                fund.currency,
                served_as_of,
            )

        ajax_ids = template["ajax_id"]
        if isinstance(ajax_ids, str):
            ajax_ids = [ajax_ids]
        href = _resolve_holdings_url(
            client,
            fund.product_page_url,
            ajax_ids,
            fund.ticker,
            template["filename_suffix"],
        )
        if not href:
            log.info("no holdings link for %s (%s)", fund.portfolio_id, fund.ticker)
            return []
        if as_of_date is not None:
            sep = "&" if "?" in href else "?"
            href = f"{href}{sep}asOfDate={as_of_date.strftime('%Y%m%d')}"
        body = _fetch_text(client, href)
        if not body or len(body) < 1024:
            return []
        log.info("    downloaded %.1f KB, parsing ...", len(body) / 1024)

        if template["format"] == "csv":
            return _parse_csv_holdings(
                body,
                fund.portfolio_id,
                fund.ticker,
                fund.portfolio,
                fund.currency,
                as_of,
            )
        if template["format"] == "xls":
            return _parse_spreadsheetml_holdings(
                body,
                fund.portfolio_id,
                fund.ticker,
                fund.portfolio,
                fund.currency,
                as_of,
            )
        log.warning("unknown format %s for %s", template["format"], fund.portfolio_id)
        return []
    finally:
        if own_client and client is not None:
            client.close()


def fetch_holdings_batch(
    funds: Iterable[FundRef],
    *,
    template: dict[str, Any],
    today: date | None = None,
) -> Iterator[tuple[FundRef, list[Holding]]]:
    """Yield ``(fund, holdings)`` pairs sequentially with a shared client."""
    with httpx.Client(headers=_HDRS, follow_redirects=True, timeout=_TIMEOUT) as client:
        for f in funds:
            yield (
                f,
                fetch_fund_holdings(f, template=template, client=client, today=today),
            )
