"""Orchestrator for the holdings ingestion pipeline.

Discovers candidate funds per portfolio from the existing BlackRock product
screeners (US, OFFSHORE, CN), normalizes each into a :class:`FundRef`, and
runs the holdings fetcher + DB writer.

Usage::

    python -m openbb_blackrock.ingest --portfolio iShares --tickers IVV,AGG
    python -m openbb_blackrock.ingest --portfolio "US Onshore" --limit 10
    python -m openbb_blackrock.ingest --all --limit 50
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Iterator

import httpx

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
from .holdings import FundRef, fetch_fund_holdings
from .nav_history import (
    fetch_workbook,
    replace_distributions_for_fund,
    replace_nav_history,
)

log = logging.getLogger(__name__)

_HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}


def _compound_num(v) -> float | None:
    """Extract the numeric ``r`` value from BlackRock's compound
    ``{"d": display, "r": raw}`` field shape.  Returns None for absent /
    sentinel ('-') / non-numeric values.
    """
    if isinstance(v, dict):
        r = v.get("r")
        if isinstance(r, (int, float)):
            return float(r)
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _compound_str(v) -> str | None:
    """Extract the display ``d`` value from BlackRock's compound field."""
    if isinstance(v, dict):
        d = v.get("d")
        if isinstance(d, str) and d and d != "-":
            return d
    if isinstance(v, str) and v and v != "-":
        return v
    return None


# Screener URLs (from app.py:_REGION_CONFIGS, hard-coded here to avoid
# circular import with the FastAPI app).
_BASE = "https://www.blackrock.com"
_SCREENERS = {
    "US": (
        f"{_BASE}/us/individual/product-screener/product-screener-v3.1.jsn",
        "/templatedata/config/product-screener-v3/data/en/one/v4/product-screener-backend-config",
    ),
    "OFFSHORE": (
        f"{_BASE}/americas-offshore/en/product-screener/product-screener-v3.1.jsn",
        "/templatedata/config/product-screener-v3/data/en/Americas-offshore/product-screener-backend-config",
    ),
    "CN": (
        f"{_BASE}/cn/product-screener/product-screener-v3.1.jsn",
        "/templatedata/config/product-screener-v3/data/zh/cn-retail/product-screener-backend-config",
    ),
}

# Portfolio routing — which screener regions feed which portfolio.
PORTFOLIOS: dict[str, dict] = {
    "iShares": {
        "regions": ["US"],
        "filter": lambda rec: "ishares" in (rec.get("productView") or []),
        "ajax_id": ["1467271812596"],
        "format": "csv",
        "filename_suffix": "_holdings",
        "page_base": "https://www.ishares.com",
    },
    "US Onshore": {
        "regions": ["US"],
        "filter": lambda rec: "ishares" not in (rec.get("productView") or []),
        "ajax_id": ["1464253357814", "1500962885783"],
        "format": "csv",
        "filename_suffix": "_holdings",
        "page_base": "https://www.blackrock.com",
    },
    "US Offshore": {
        # Americas Offshore + China offshore merged
        "regions": ["OFFSHORE", "CN"],
        "filter": lambda rec: True,
        "ajax_id": ["1527484370694", "1512667833328"],
        "format": "xls",
        "filename_suffix": "_fund",
        "page_base": "https://www.blackrock.com",
    },
}


def _fetch_screener(region: str, client: httpx.Client) -> dict[str, dict]:
    url, dcr = _SCREENERS[region]
    full = f"{url}?dcrPath={dcr}&siteEntryPassthrough=true"
    log.info("fetching screener [%s] ...", region)
    r = client.get(full)
    r.raise_for_status()
    catalog = r.json()
    log.info("  screener [%s]: %d products found", region, len(catalog))
    return catalog


def _build_product_url(rec: dict, page_base: str) -> str | None:
    """Turn a catalog record's productPageUrl into an absolute URL.

    iShares records on the US screener point at /us/individual/products/...
    but the actual holdings link lives under www.ishares.com — we rewrite.
    """
    p = rec.get("productPageUrl")
    if not p:
        return None
    if p.startswith("http"):
        return p
    if "ishares" in (rec.get("productView") or []):
        return "https://www.ishares.com" + p.replace("/us/individual", "/us")
    return page_base + p


def _iter_funds_for_portfolio(
    portfolio: str,
    client: httpx.Client,
    *,
    tickers: set[str] | None,
    limit: int | None,
) -> Iterator[FundRef]:
    cfg = PORTFOLIOS[portfolio]
    seen_pids: set[str] = set()
    yielded = 0
    for region in cfg["regions"]:
        try:
            catalog = _fetch_screener(region, client)
        except Exception as exc:
            log.warning("screener %s failed: %s", region, exc)
            continue
        for pid, rec in catalog.items():
            if pid in seen_pids:
                continue
            if not cfg["filter"](rec):
                continue
            ticker = rec.get("localExchangeTicker")
            if ticker == "-" or not ticker:
                ticker = None
            if tickers and (not ticker or ticker.upper() not in tickers):
                continue
            url = _build_product_url(rec, cfg["page_base"])
            if not url:
                continue
            ccy = rec.get("seriesBaseCurrencyCode") or "USD"
            isin = rec.get("isin")
            seen_pids.add(pid)
            yielded += 1
            yield (
                FundRef(
                    portfolio_id=pid,
                    ticker=ticker,
                    name=rec.get("fundName") or pid,
                    currency=ccy,
                    portfolio=portfolio,
                    product_page_url=url,
                ),
                rec,
                isin,
            )
            if limit and yielded >= limit:
                return


def ingest_portfolio(
    portfolio: str,
    *,
    tickers: list[str] | None = None,
    limit: int | None = None,
) -> dict:
    cfg = PORTFOLIOS[portfolio]
    template = {
        "ajax_id": cfg["ajax_id"],
        "format": cfg["format"],
        "filename_suffix": cfg["filename_suffix"],
    }
    log.info("=== ingesting portfolio: %s ===", portfolio)
    init_db()
    conn = get_conn()
    seeded = seed_fx_rates(conn)
    log.info("seeded %d new FX rate rows", seeded)

    tk_filter = {t.upper() for t in tickers} if tickers else None

    funds_processed = 0
    holdings_written = 0
    funds_with_no_holdings = 0
    nav_written = 0
    dist_written = 0
    with httpx.Client(headers=_HDRS, follow_redirects=True, timeout=60) as client:
        for ref, rec, isin in _iter_funds_for_portfolio(
            portfolio, client, tickers=tk_filter, limit=limit
        ):
            funds_processed += 1
            log.info(
                "[fund #%d] %s (%s): fetching ...",
                funds_processed,
                ref.portfolio_id,
                ref.ticker,
            )
            try:
                hs = fetch_fund_holdings(ref, template=template, client=client)
            except Exception as exc:
                log.warning("holdings fetch failed for %s: %s", ref.portfolio_id, exc)
                hs = []
            # Reported AUM (USD): US iShares exposes it under totalNetAssets.r;
            # offshore screeners use totalNetAssetsFund / totalFundSizeInMillions
            # in fund currency.
            aum_obj = rec.get("totalNetAssets") or rec.get("totalNetAssetsFund")
            total_aum_usd = None
            if isinstance(aum_obj, dict):
                aum_r = aum_obj.get("r")
                if isinstance(aum_r, (int, float)) and ref.currency == "USD":
                    total_aum_usd = float(aum_r)
                elif isinstance(aum_r, (int, float)) and ref.currency != "USD":
                    total_aum_usd = 0.0  # non-USD: needs FX

            inv_style = rec.get("investmentStyle")
            if isinstance(inv_style, list):
                inv_style = ", ".join(str(x) for x in inv_style if x)
            prod_view = rec.get("productView")
            if isinstance(prod_view, list):
                prod_view = ", ".join(str(x) for x in prod_view if x and x != "all")

            upsert_fund(
                conn,
                portfolio_id=ref.portfolio_id,
                ticker=ref.ticker,
                isin=isin,
                name=ref.name,
                portfolio=portfolio,
                currency=ref.currency,
                # Classification
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
                # Lifecycle / link
                inception_date=_compound_str(rec.get("inceptionDate")),
                product_page_url=rec.get("productPageUrl"),
                # Total return — NAV-based (annualized %)
                nav_ytd_pct=_compound_num(rec.get("navYearToDate")),
                nav_1y_pct=_compound_num(rec.get("navOneYearAnnualized")),
                nav_3y_pct=_compound_num(rec.get("navThreeYearAnnualized")),
                nav_5y_pct=_compound_num(rec.get("navFiveYearAnnualized")),
                nav_10y_pct=_compound_num(rec.get("navTenYearAnnualized")),
                nav_inception_pct=_compound_num(rec.get("navSinceInceptionAnnualized")),
                nav_perf_as_of=_compound_str(rec.get("navPerfAsOf")),
                # Total return — price-based
                price_ytd_pct=_compound_num(rec.get("priceYearToDate")),
                price_1y_pct=_compound_num(rec.get("priceOneYearAnnualized")),
                price_3y_pct=_compound_num(rec.get("priceThreeYearAnnualized")),
                price_5y_pct=_compound_num(rec.get("priceFiveYearAnnualized")),
                price_10y_pct=_compound_num(rec.get("priceTenYearAnnualized")),
                price_inception_pct=_compound_num(
                    rec.get("priceSinceInceptionAnnualized")
                ),
                # Yields & pricing dynamics
                sec_yield_30d_pct=_compound_num(rec.get("thirtyDaySecYield")),
                twelve_month_yield_pct=_compound_num(rec.get("twelveMonTrlYield")),
                unsubsidized_yield_pct=_compound_num(rec.get("unsubsidizedYield")),
                distribution_yield_pct=_compound_num(rec.get("distYieldMkt")),
                premium_discount_pct=_compound_num(rec.get("premiumDiscount")),
                # Verbatim screener record — preserves every field BlackRock
                # returned (region-specific fees, fund-size, inception NAV, etc.)
                raw_json=json.dumps(rec, ensure_ascii=False),
                holdings_as_of_date=hs[0].as_of_date if hs else None,
                total_aum_usd=total_aum_usd,
            )
            n = replace_holdings_for_fund(conn, ref.portfolio_id, hs)
            holdings_written += n
            if n == 0:
                funds_with_no_holdings += 1
                log.info(
                    "[fund #%d] %s (%s): no holdings",
                    funds_processed,
                    ref.portfolio_id,
                    ref.ticker,
                )
            else:
                log.info(
                    "[fund #%d] %s (%s): wrote %d rows, total USD ~%.0f",
                    funds_processed,
                    ref.portfolio_id,
                    ref.ticker,
                    n,
                    sum(h.market_value_usd for h in hs),
                )
            nav_rows, dist_rows = fetch_workbook(
                ref.portfolio_id, ref.product_page_url, client=client
            )
            nav_n = replace_nav_history(conn, ref.portfolio_id, nav_rows)
            dist_n = replace_distributions_for_fund(conn, ref.portfolio_id, dist_rows)
            nav_written += nav_n
            dist_written += dist_n
            if nav_n:
                log.info(
                    "[fund #%d] %s (%s): wrote %d nav rows, %d dist rows",
                    funds_processed,
                    ref.portfolio_id,
                    ref.ticker,
                    nav_n,
                    dist_n,
                )

    log.info("rebuilding fund links ...")
    links = rebuild_fund_links(conn)
    log.info("  %d fund links built", links)
    log.info("computing dedupe metadata ...")
    compute_dedupe_metadata(conn)
    log.info("rebuilding lookthrough (this can take several minutes) ...")
    lt_rows = rebuild_lookthrough(conn)
    log.info("  %d lookthrough rows written", lt_rows)
    docs_written = 0
    if portfolio == "iShares":
        from .documents import populate_us_documents

        log.info("ingesting documents ...")
        try:
            docs_written = populate_us_documents(conn)
            log.info("  %d documents written", docs_written)
        except Exception as exc:
            log.warning("document ingestion failed: %s", exc)
    return {
        "portfolio": portfolio,
        "funds_processed": funds_processed,
        "funds_with_no_holdings": funds_with_no_holdings,
        "holdings_written": holdings_written,
        "nav_rows_written": nav_written,
        "dist_rows_written": dist_written,
        "fund_links": links,
        "lookthrough_rows": lt_rows,
        "documents_written": docs_written,
    }


def ingest_nav_history(
    *, tickers: list[str] | None = None, limit: int | None = None
) -> dict:
    """Pull daily NAV history for every fund currently in the DB."""
    from .nav_history import ingest_nav_history_batch

    init_db()
    conn = get_conn()
    rows = conn.execute(
        """SELECT portfolio_id, ticker, product_page_url
           FROM funds
           WHERE product_page_url IS NOT NULL"""
    ).fetchall()
    if tickers:
        tk = {t.upper() for t in tickers}
        rows = [r for r in rows if r[1] and r[1].upper() in tk]
    if limit:
        rows = rows[:limit]

    funds = []
    for pid, _ticker, page_url in rows:
        if not page_url:
            continue
        if not page_url.startswith("http"):
            # iShares product pages live under www.ishares.com but the
            # screener stores a /us/individual/... path.  Rewrite once.
            if "/us/individual" in page_url:
                page_url = "https://www.ishares.com" + page_url.replace(
                    "/us/individual", "/us"
                )
            else:
                page_url = "https://www.blackrock.com" + page_url
        funds.append((pid, page_url))
    return ingest_nav_history_batch(conn, funds)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--portfolio", choices=list(PORTFOLIOS) + ["all"])
    ap.add_argument(
        "--nav-history",
        action="store_true",
        help="After (or instead of) holdings ingest, pull daily NAV history for every fund",
    )
    ap.add_argument("--tickers", help="Comma-separated ticker filter")
    ap.add_argument("--limit", type=int, help="Stop after N funds per portfolio")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.portfolio:
        targets = list(PORTFOLIOS) if args.portfolio == "all" else [args.portfolio]
        tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
        for p in targets:
            result = ingest_portfolio(p, tickers=tickers, limit=args.limit)
            print(result)
    if args.nav_history:
        tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
        result = ingest_nav_history(tickers=tickers, limit=args.limit)
        print({"nav_history": result})
    return 0


if __name__ == "__main__":
    sys.exit(main())
