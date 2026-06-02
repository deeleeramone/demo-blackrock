"""Async holdings + NAV/distributions ingestion.

Every fund's download URL is derivable from its ``portfolio_id`` (already
stored per fund), so the slow part of the pipeline — 486 sequential CSV /
workbook downloads — is run concurrently here with a bounded semaphore.
This is a drop-in faster alternative to :func:`ingest.ingest_portfolio`
for the nightly refresh.  Parsers and DB writers are reused unchanged.

    python -m openbb_blackrock.ingest_async --portfolio iShares --concurrency 16
    python -m openbb_blackrock.ingest_async --tickers IVV,AGG,LDRH   # quick test
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import date

import httpx

from . import nav_history as navh
from .db import (
    compute_dedupe_metadata,
    get_conn,
    init_db,
    rebuild_fund_links,
    rebuild_lookthrough,
    replace_holdings_for_fund,
    seed_fx_rates,
    upsert_fund,
)
from .documents import _extract_fund_documents
from .db import replace_documents_for_fund
from .holdings import (
    _build_ishares_holdings_url,
    _parse_csv_holdings,
    _parse_ishares_csv_as_of_date,
)
from .key_facts import KEY_FACT_COLUMNS, parse_key_facts
from .ingest import (
    PORTFOLIOS,
    _compound_num,
    _compound_str,
    _iter_funds_for_portfolio,
)
from .nav_history import replace_distributions_for_fund, replace_nav_history

log = logging.getLogger(__name__)

_HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}


async def _fetch(
    client: httpx.AsyncClient,
    url: str,
    sem: asyncio.Semaphore,
    *,
    referer: str | None = None,
    retries: int = 4,
) -> tuple[str | None, int | None]:
    """Return ``(body, status)``. ``status`` is the last HTTP code seen (or
    ``None`` on a network error) so callers can tell an expected 4xx
    'no such document' apart from a transient failure."""
    headers = {"Referer": referer} if referer else None
    delay = 3.0
    async with sem:
        for attempt in range(retries):
            try:
                r = await client.get(url, headers=headers, timeout=60)
            except httpx.HTTPError as exc:
                if attempt == retries - 1:
                    log.warning("fetch failed %s: %s", url, exc)
                    return None, None
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue
            if r.status_code == 200 and r.text:
                return r.text, 200
            # 4xx (except 429 rate-limit) is a permanent client error — the
            # fund simply doesn't serve this document. Don't waste retries;
            # return the status so the caller can classify it.
            if 400 <= r.status_code < 500 and r.status_code != 429:
                return None, r.status_code
            # 429 / 5xx are transient — back off and retry.
            if attempt == retries - 1:
                log.warning(
                    "fetch %s -> HTTP %d (gave up after %d attempts)",
                    url,
                    r.status_code,
                    retries,
                )
                return None, r.status_code
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)
    return None, None


async def _gather(refs, concurrency):
    """Concurrently fetch, per fund: holdings CSV + NAV workbook + product
    page (Key Facts and document links come from the same page download)."""
    sem = asyncio.Semaphore(concurrency)
    done = {"n": 0}
    total = len(refs)

    async with httpx.AsyncClient(headers=_HDRS, follow_redirects=True, timeout=60) as client:

        async def page(ref):
            # FundRef.product_page_url is already the absolute ishares URL.
            # _extract_fund_documents also surfaces the metal-trust bar-list
            # PDF as a "Bar List" document entry.
            body, _status = await _fetch(client, ref.product_page_url, sem)
            page_ok = bool(body)
            kf, docs = {}, {}
            if page_ok:
                kf = parse_key_facts(body)
                docs = _extract_fund_documents(body)
            return kf, docs, page_ok

        async def holdings(ref):
            body, status = await _fetch(
                client,
                _build_ishares_holdings_url(ref.portfolio_id, None),
                sem,
                referer=ref.product_page_url,
            )
            # csv_ok = a real CSV came back (vs network failure / stub).
            csv_ok = bool(body and len(body) >= 1024)
            hs = []
            if csv_ok:
                as_of = _parse_ishares_csv_as_of_date(body) or date.today()
                hs = _parse_csv_holdings(body, ref.portfolio_id, ref.ticker, ref.portfolio, ref.currency, as_of)
            return hs, csv_ok, status

        async def workbook(ref):
            body, _status = await _fetch(
                client,
                navh._build_fund_download_url(ref.portfolio_id),
                sem,
                referer=ref.product_page_url,
            )
            wb_ok = bool(body)
            nav, dist = [], []
            if wb_ok:
                text = body.lstrip("﻿")
                h = navh._HISTORICAL_RE.search(text)
                if h:
                    nav = navh._parse_historical_sheet(ref.portfolio_id, h.group(1))
                d = navh._DISTRIBUTIONS_RE.search(text)
                if d:
                    dist = navh._parse_distributions_sheet(ref.portfolio_id, d.group(1))
            return nav, dist, wb_ok

        async def one(ref):
            (hs, csv_ok, h_status), (nav, dist, wb_ok), (kf, docs, page_ok) = (
                await asyncio.gather(holdings(ref), workbook(ref), page(ref))
            )
            done["n"] += 1
            if done["n"] % 25 == 0 or done["n"] == total:
                log.info("  fetched %d/%d funds", done["n"], total)
            return ref, hs, csv_ok, h_status, nav, dist, wb_ok, kf, docs, page_ok

        return await asyncio.gather(*(one(r) for r in refs))


def _upsert_fund_from_rec(conn, ref, rec, isin, hs, key_facts=None) -> None:
    """Mirror of ingest.ingest_portfolio's fund upsert (verbatim field map),
    plus the product-page Key Facts. ``key_facts`` is the parsed page metrics;
    when empty (page fetch failed) the prior values are read back so a failure
    never blanks them."""
    aum_obj = rec.get("totalNetAssets") or rec.get("totalNetAssetsFund")
    total_aum_usd = None
    if isinstance(aum_obj, dict):
        aum_r = aum_obj.get("r")
        if isinstance(aum_r, (int, float)) and ref.currency == "USD":
            total_aum_usd = float(aum_r)
        elif isinstance(aum_r, (int, float)):
            total_aum_usd = 0.0
    inv_style = rec.get("investmentStyle")
    if isinstance(inv_style, list):
        inv_style = ", ".join(str(x) for x in inv_style if x)
    prod_view = rec.get("productView")
    if isinstance(prod_view, list):
        prod_view = ", ".join(str(x) for x in prod_view if x and x != "all")
    # Preserve the prior holdings_as_of_date when this run had no holdings,
    # so a skipped/failed fetch doesn't blank the fund's metadata either.
    if hs:
        holdings_as_of = hs[0].as_of_date
    else:
        prior = conn.execute(
            "SELECT holdings_as_of_date FROM funds WHERE portfolio_id = ?",
            (ref.portfolio_id,),
        ).fetchone()
        holdings_as_of = prior[0] if prior else None
    fields = dict(
        portfolio_id=ref.portfolio_id,
        ticker=ref.ticker,
        isin=isin,
        name=ref.name,
        portfolio=ref.portfolio,
        currency=ref.currency,
        asset_class=rec.get("aladdinAssetClass"),
        sub_asset_class=rec.get("aladdinSubAssetClass"),
        investment_style=inv_style,
        market_type=rec.get("aladdinMarketType"),
        region=rec.get("aladdinRegion"),
        product_view=prod_view,
        country=rec.get("aladdinCountry"),
        share_class=rec.get("investorClassName"),
        esg_classification=rec.get("aladdinEsgClassification"),
        sfdr_classification=rec.get("aladdinSfdr"),
        inception_date=_compound_str(rec.get("inceptionDate")),
        product_page_url=rec.get("productPageUrl"),
        nav_ytd_pct=_compound_num(rec.get("navYearToDate")),
        nav_1y_pct=_compound_num(rec.get("navOneYearAnnualized")),
        nav_3y_pct=_compound_num(rec.get("navThreeYearAnnualized")),
        nav_5y_pct=_compound_num(rec.get("navFiveYearAnnualized")),
        nav_10y_pct=_compound_num(rec.get("navTenYearAnnualized")),
        nav_inception_pct=_compound_num(rec.get("navSinceInceptionAnnualized")),
        nav_perf_as_of=_compound_str(rec.get("navPerfAsOf")),
        price_ytd_pct=_compound_num(rec.get("priceYearToDate")),
        price_1y_pct=_compound_num(rec.get("priceOneYearAnnualized")),
        price_3y_pct=_compound_num(rec.get("priceThreeYearAnnualized")),
        price_5y_pct=_compound_num(rec.get("priceFiveYearAnnualized")),
        price_10y_pct=_compound_num(rec.get("priceTenYearAnnualized")),
        price_inception_pct=_compound_num(rec.get("priceSinceInceptionAnnualized")),
        sec_yield_30d_pct=_compound_num(rec.get("thirtyDaySecYield")),
        twelve_month_yield_pct=_compound_num(rec.get("twelveMonTrlYield")),
        unsubsidized_yield_pct=_compound_num(rec.get("unsubsidizedYield")),
        distribution_yield_pct=_compound_num(rec.get("distYieldMkt")),
        premium_discount_pct=_compound_num(rec.get("premiumDiscount")),
        raw_json=json.dumps(rec, ensure_ascii=False),
        holdings_as_of_date=holdings_as_of,
        total_aum_usd=total_aum_usd,
    )
    # Key Facts (product page) override the thinner screener values
    # (premium/discount, yields) and fill the new columns. On a failed page
    # fetch, read prior values back so a failure never blanks them.
    kf = key_facts
    if not kf:
        prior = conn.execute(
            f"SELECT {','.join(KEY_FACT_COLUMNS)} FROM funds WHERE portfolio_id = ?",
            (ref.portfolio_id,),
        ).fetchone()
        kf = dict(zip(KEY_FACT_COLUMNS, prior)) if prior else {}
    fields.update({k: v for k, v in kf.items() if v is not None})
    upsert_fund(conn, **fields)


def ingest_portfolio_async(
    portfolio: str = "iShares",
    *,
    concurrency: int = 16,
    tickers: list[str] | None = None,
    limit: int | None = None,
) -> dict:
    if portfolio not in PORTFOLIOS:
        raise ValueError(f"unknown portfolio {portfolio!r}")
    init_db()
    conn = get_conn()
    seed_fx_rates(conn)

    tk = {t.upper() for t in tickers} if tickers else None
    log.info("=== async ingest: %s (concurrency=%d) ===", portfolio, concurrency)
    with httpx.Client(headers=_HDRS, follow_redirects=True, timeout=60) as sc:
        items = list(_iter_funds_for_portfolio(portfolio, sc, tickers=tk, limit=limit))
    refs = [it[0] for it in items]
    rec_by_pid = {it[0].portfolio_id: (it[1], it[2]) for it in items}
    log.info("discovered %d funds; fetching concurrently ...", len(refs))

    results = asyncio.run(_gather(refs, concurrency))

    holdings_written = nav_written = dist_written = docs_written = 0
    holdings_retained = nav_retained = holdings_no_doc = key_facts_funds = 0
    for (
        ref, hs, csv_ok, h_status, nav, dist, wb_ok, kf, docs, page_ok
    ) in results:
        rec, isin = rec_by_pid[ref.portfolio_id]
        tk = ref.ticker or ref.portfolio_id
        # Key Facts come from the product page; pass them (or {} to preserve
        # prior values when the page fetch failed) into the fund upsert.
        _upsert_fund_from_rec(conn, ref, rec, isin, hs, key_facts=kf)
        if kf:
            key_facts_funds += 1

        # Documents are parsed from the same page download — only refresh when
        # the page actually loaded, so a failure retains prior document rows.
        if page_ok and ref.ticker:
            docs_written += replace_documents_for_fund(
                conn, ref.portfolio_id, ref.ticker, docs
            )
        elif not page_ok:
            log.warning("product page fetch FAILED for %s — key facts/docs RETAINED", tk)

        if hs:
            holdings_written += replace_holdings_for_fund(conn, ref.portfolio_id, hs)
        elif h_status is not None and 400 <= h_status < 500 and h_status != 429:
            holdings_no_doc += 1
            log.info("%s has no holdings document (HTTP %d) — expected", tk, h_status)
        else:
            holdings_retained += 1
            log.warning(
                "holdings %s for %s — RETAINED prior rows",
                "fetch FAILED" if not csv_ok else "empty download",
                tk,
            )

        if nav:
            nav_written += replace_nav_history(conn, ref.portfolio_id, nav)
        elif not wb_ok:
            nav_retained += 1
            log.warning("nav workbook fetch FAILED for %s — RETAINED prior rows", tk)
        if wb_ok:
            dist_written += replace_distributions_for_fund(conn, ref.portfolio_id, dist)

    log.info("rebuilding fund links / dedupe / lookthrough ...")
    links = rebuild_fund_links(conn)
    compute_dedupe_metadata(conn)
    lt = rebuild_lookthrough(conn)
    conn.execute("DELETE FROM holdings_lt_latest")
    conn.execute(
        """INSERT INTO holdings_lt_latest
            (parent_portfolio_id, portfolio, leaf_holding_name, leaf_holding_ticker,
             leaf_holding_isin, leaf_holding_cusip, holding_type, sector, country,
             currency, market_value_usd, weight_pct, path_depth, as_of_date)
        SELECT parent_portfolio_id, portfolio, leaf_holding_name, leaf_holding_ticker,
             leaf_holding_isin, leaf_holding_cusip, holding_type, sector, country,
             currency, market_value_usd, weight_pct, path_depth, as_of_date
        FROM holdings_lookthrough
        WHERE (parent_portfolio_id, as_of_date) IN (
            SELECT parent_portfolio_id, MAX(as_of_date)
            FROM holdings_lookthrough GROUP BY parent_portfolio_id)"""
    )

    result = {
        "portfolio": portfolio,
        "funds": len(refs),
        "holdings_written": holdings_written,
        "holdings_no_document": holdings_no_doc,
        "holdings_retained_on_failure": holdings_retained,
        "nav_rows_written": nav_written,
        "nav_retained_on_failure": nav_retained,
        "dist_rows_written": dist_written,
        "fund_links": links,
        "lookthrough_rows": lt,
        "documents_written": docs_written,
        "funds_with_key_facts": key_facts_funds,
    }
    log.info("done: %s", result)
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--portfolio", default="iShares", choices=list(PORTFOLIOS))
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--tickers", help="Comma-separated ticker filter (testing)")
    ap.add_argument("--limit", type=int, help="Stop after N funds (testing)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
    print(
        ingest_portfolio_async(
            args.portfolio,
            concurrency=args.concurrency,
            tickers=tickers,
            limit=args.limit,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
