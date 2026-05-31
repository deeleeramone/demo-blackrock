from __future__ import annotations

import bisect
import html as _html_mod
import json
import math
import re
import sqlite3
import time
import urllib.request as _urllib_req
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .db import DB_PATH, get_conn, init_db

app = FastAPI(title="BlackRock Portfolio API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


_PKG_DIR = Path(__file__).resolve().parent


def _load_json_file(name: str) -> Any:
    for candidate in (_PKG_DIR / name, _PKG_DIR.parent / name):
        if candidate.exists():
            return json.loads(candidate.read_text())
    raise HTTPException(status_code=404, detail=f"{name} not found")


@app.get("/widgets.json")
def widgets_json() -> JSONResponse:
    return JSONResponse(content=_load_json_file("widgets.json"))


@app.get("/apps.json")
def apps_json() -> JSONResponse:
    return JSONResponse(content=_load_json_file("apps.json"))


@contextmanager
def _conn() -> Iterator[sqlite3.Connection]:
    p = Path(DB_PATH)
    if not p.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Holdings DB not found at {p}. Run `python -m openbb_blackrock.ingest --portfolio iShares` first.",
        )
    uri = f"file:{p}?mode=ro"
    c = sqlite3.connect(uri, uri=True)
    c.row_factory = sqlite3.Row
    try:
        yield c
    finally:
        c.close()


def _read_sql(sql: str, params: tuple = ()) -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql_query(sql, c, params=params)


def _scalar(sql: str, params: tuple = ()) -> Any:
    with _conn() as c:
        cur = c.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def _compute_total_return(
    portfolio_id: str,
    start_date: str,
    *,
    annualize_years: float | None = None,
    end_date: str | None = None,
) -> float | None:
    """Total return (NAV + reinvested dividends) from start_date to the latest
    available nav_history row (or end_date if specified).  Optionally
    annualizes over *annualize_years*.
    Returns a percentage (e.g. -20.51) or None if data is insufficient.
    """
    with _conn() as conn:
        start_row = conn.execute(
            "SELECT nav_per_share FROM nav_history "
            "WHERE portfolio_id = ? AND as_of_date <= ? AND nav_per_share IS NOT NULL "
            "ORDER BY as_of_date DESC LIMIT 1",
            (portfolio_id, start_date),
        ).fetchone()
        if not start_row:
            return None
        start_nav = start_row[0]

        if end_date:
            end_row = conn.execute(
                "SELECT nav_per_share FROM nav_history "
                "WHERE portfolio_id = ? AND as_of_date <= ? AND nav_per_share IS NOT NULL "
                "ORDER BY as_of_date DESC LIMIT 1",
                (portfolio_id, end_date),
            ).fetchone()
        else:
            end_row = conn.execute(
                "SELECT nav_per_share FROM nav_history "
                "WHERE portfolio_id = ? AND nav_per_share IS NOT NULL "
                "ORDER BY as_of_date DESC LIMIT 1",
                (portfolio_id,),
            ).fetchone()
        if not end_row:
            return None
        latest_nav = end_row[0]

        div_date_cutoff = end_date or "9999-12-31"
        div_rows = conn.execute(
            "SELECT nav_per_share, ex_dividends FROM nav_history "
            "WHERE portfolio_id = ? AND ex_dividends > 0 AND as_of_date > ? "
            "AND as_of_date <= ? AND nav_per_share IS NOT NULL "
            "ORDER BY as_of_date",
            (portfolio_id, start_date, div_date_cutoff),
        ).fetchall()

    cum_factor = 1.0
    for ex_nav, div in div_rows:
        if ex_nav and ex_nav > 0 and div:
            cum_factor *= (ex_nav + div) / ex_nav

    raw = cum_factor * latest_nav / start_nav - 1.0
    if annualize_years and annualize_years > 0:
        try:
            raw = (1.0 + raw) ** (1.0 / annualize_years) - 1.0
        except (ValueError, ZeroDivisionError):
            return None
    return raw * 100.0


@app.on_event("startup")
def _startup() -> None:
    if Path(DB_PATH).exists():
        init_db()
        conn = get_conn()
        conn.execute("DELETE FROM holdings_lt_latest")
        conn.execute("""
            INSERT INTO holdings_lt_latest
                (parent_portfolio_id, portfolio, leaf_holding_name, leaf_holding_ticker,
                 leaf_holding_isin, leaf_holding_cusip, holding_type, sector, country,
                 currency, market_value_usd, weight_pct, path_depth, as_of_date)
            SELECT parent_portfolio_id, portfolio, leaf_holding_name, leaf_holding_ticker,
                   leaf_holding_isin, leaf_holding_cusip, holding_type, sector, country,
                   currency, market_value_usd, weight_pct, path_depth, as_of_date
            FROM holdings_lookthrough
            WHERE (parent_portfolio_id, as_of_date) IN (
                SELECT parent_portfolio_id, MAX(as_of_date)
                FROM holdings_lookthrough
                GROUP BY parent_portfolio_id
            )
        """)


class Overview(BaseModel):
    portfolios: list[str]
    fund_count: int
    holding_count: int
    lookthrough_count: int
    total_aum_usd: float
    external_aum_usd: float
    internally_held_usd: float
    holdings_as_of_max: str | None


class BreakdownRow(BaseModel):
    label: str
    market_value_usd: float
    weight_pct: float


class FundSummary(BaseModel):
    portfolio_id: str
    ticker: str | None
    isin: str | None
    name: str
    portfolio: str
    asset_class: str | None
    sub_asset_class: str | None
    investment_style: str | None
    market_type: str | None
    region: str | None
    product_view: str | None
    country: str | None
    currency: str
    total_aum_usd: float | None
    external_aum_usd: float | None
    internally_held_usd: float
    holdings_as_of_date: str | None


class HoldingRow(BaseModel):
    parent_ticker: str | None
    holding_name: str
    holding_ticker: str | None
    holding_isin: str | None
    holding_cusip: str | None
    holding_sedol: str | None
    holding_type: str | None
    sector: str | None
    country: str | None
    exchange: str | None
    currency: str | None
    market_value_usd: float
    weight_pct: float
    price: float | None
    notional_value: float | None
    coupon_pct: float | None
    maturity_date: str | None
    duration: float | None
    ytm_pct: float | None
    as_of_date: str


class LookthroughRow(BaseModel):
    parent_ticker: str | None
    leaf_holding_name: str
    leaf_holding_ticker: str | None
    leaf_holding_isin: str | None
    holding_type: str | None
    sector: str | None
    country: str | None
    currency: str
    market_value_usd: float
    weight_pct: float
    path_depth: int


class TotalRow(BaseModel):
    label: str
    market_value_usd: float
    weight_pct: float


def _frame_to_records(df: pd.DataFrame) -> list[dict]:
    df = df.replace([float("inf"), float("-inf")], 0.0)
    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")


def _drop_empty_columns(df: pd.DataFrame, *, keep: set[str] | None = None) -> pd.DataFrame:
    keep = keep or set()
    cols: list[str] = []
    for c in df.columns:
        if c in keep:
            cols.append(c)
            continue
        s = df[c]
        if s.isna().all():
            continue
        if s.dtype == object:
            sv = s.dropna().astype(str).str.strip()
            if (sv == "").all() or (sv == "-").all() or (sv == "—").all():
                continue
        cols.append(c)
    return df[cols]


@app.get("/overview", response_model=Overview)
def overview() -> Overview:
    df = _read_sql(
        """
        SELECT
            (SELECT COUNT(*) FROM funds)                     AS fund_count,
            (SELECT COUNT(*) FROM (
                SELECT DISTINCT COALESCE(NULLIF(holding_isin,''), holding_name)
                FROM holdings
                WHERE COALESCE(NULLIF(holding_isin,''), holding_name) IS NOT NULL
            ))                                               AS holding_count,
            (SELECT COUNT(*) FROM (
                SELECT DISTINCT COALESCE(NULLIF(leaf_holding_isin,''), leaf_holding_name)
                FROM holdings_lookthrough
                WHERE COALESCE(NULLIF(leaf_holding_isin,''), leaf_holding_name) IS NOT NULL
            ))                                               AS lookthrough_count,
            (SELECT COALESCE(SUM(total_aum_usd), 0) FROM funds)        AS total_aum_usd,
            (SELECT COALESCE(SUM(external_aum_usd), 0) FROM funds)     AS external_aum_usd,
            (SELECT COALESCE(SUM(internally_held_usd), 0) FROM funds)  AS internally_held_usd,
            (SELECT MAX(holdings_as_of_date) FROM funds)               AS holdings_as_of_max
        """
    )
    portfolios = _read_sql("SELECT DISTINCT portfolio FROM funds ORDER BY portfolio")["portfolio"].tolist()
    row = df.iloc[0]
    return Overview(
        portfolios=portfolios,
        fund_count=int(row.fund_count),
        holding_count=int(row.holding_count),
        lookthrough_count=int(row.lookthrough_count),
        total_aum_usd=float(row.total_aum_usd or 0),
        external_aum_usd=float(row.external_aum_usd or 0),
        internally_held_usd=float(row.internally_held_usd or 0),
        holdings_as_of_max=row.holdings_as_of_max or None,
    )


_BREAKDOWN_AXES = {
    "sector": "sector",
    "asset_class": "holding_type",
    "holding_type": "holding_type",
    "country": "country",
    "portfolio": "portfolio",
    "currency": "currency",
}


_FUND_AXES = {
    "fund_asset_class": "asset_class",
    "strategy": "sub_asset_class",
    "fund_strategy": "sub_asset_class",
    "investment_style": "investment_style",
    "market_type": "market_type",
    "region": "region",
    "product_view": "product_view",
    "fund_country": "country",
}


def _normalize_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    df["label"] = df["label"].apply(lambda l: "Other" if l is None or _GARBAGE_LABEL_RE.match(str(l).strip()) else l)
    df = df.groupby("label", as_index=False)["market_value_usd"].sum().sort_values("market_value_usd", ascending=False)
    total = float(df["market_value_usd"].sum())
    df["weight_pct"] = df["market_value_usd"] / total * 100 if total else 0
    return df


def _breakdown(
    axis_col: str,
    *,
    portfolio: str | None,
    fund_ticker: str | None,
) -> dict:
    where = ["1=1"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    if fund_ticker:
        where.append("parent_portfolio_id = (SELECT portfolio_id FROM funds WHERE ticker = ?)")
        params.append(fund_ticker)
    if axis_col == "currency":
        where.append("currency IS NOT NULL AND LENGTH(currency) = 3 AND currency GLOB '[A-Z][A-Z][A-Z]'")
    df = _read_sql(
        f"""SELECT COALESCE({axis_col}, 'Unknown') AS label,
                   SUM(market_value_usd)           AS market_value_usd
            FROM holdings_lookthrough
            WHERE {" AND ".join(where)}
            GROUP BY label
            ORDER BY market_value_usd DESC""",
        tuple(params),
    )
    df = _normalize_breakdown(df)
    total = float(df["market_value_usd"].sum())
    return {
        "rows": [BreakdownRow(**r).model_dump() for r in _frame_to_records(df)],
        "total": TotalRow(
            label="Total",
            market_value_usd=total,
            weight_pct=100.0 if total else 0.0,
        ).model_dump(),
    }


_REGION_REMAP = {
    "Kuwait": "Middle East and Africa",
}


def _fund_breakdown(
    axis_col: str,
    *,
    portfolio: str | None,
) -> dict:
    where = ["external_aum_usd IS NOT NULL"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    df = _read_sql(
        f"""SELECT COALESCE({axis_col}, 'Unknown') AS label,
                   SUM(external_aum_usd)            AS market_value_usd
            FROM funds WHERE {" AND ".join(where)}
            GROUP BY label
            ORDER BY market_value_usd DESC""",
        tuple(params),
    )
    if axis_col == "region":
        df["label"] = df["label"].replace(_REGION_REMAP)
    df = _normalize_breakdown(df)
    total = float(df["market_value_usd"].sum())
    return {
        "rows": [BreakdownRow(**r).model_dump() for r in _frame_to_records(df)],
        "total": TotalRow(
            label="Total",
            market_value_usd=total,
            weight_pct=100.0 if total else 0.0,
        ).model_dump(),
    }


@app.get("/breakdown/{axis}")
def breakdown(
    axis: str,
    portfolio: str | None = None,
    fund_ticker: str | None = None,
    ticker: str | None = None,
) -> dict:
    fund_ticker = fund_ticker or ticker
    if axis in _FUND_AXES:
        return _fund_breakdown(_FUND_AXES[axis], portfolio=portfolio)
    if axis not in _BREAKDOWN_AXES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown axis '{axis}'; choose from {sorted(_BREAKDOWN_AXES) + sorted(_FUND_AXES)}",
        )
    return _breakdown(
        _BREAKDOWN_AXES[axis],
        portfolio=portfolio,
        fund_ticker=fund_ticker,
    )


@app.get("/top_funds")
def top_funds(
    portfolio: str | None = None,
    asset_class: str | None = None,
    limit: int = Query(50, ge=1, le=1000),
) -> dict:
    where = ["external_aum_usd IS NOT NULL"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    if asset_class:
        where.append("asset_class = ?")
        params.append(asset_class)
    df = _read_sql(
        f"""SELECT portfolio_id, ticker, isin, name, portfolio,
                   asset_class, sub_asset_class, investment_style,
                   market_type, region, product_view,
                   country, currency,
                   total_aum_usd, external_aum_usd, internally_held_usd,
                   holdings_as_of_date
            FROM funds WHERE {" AND ".join(where)}
            ORDER BY external_aum_usd DESC
            LIMIT ?""",
        tuple(params) + (limit,),
    )
    return {
        "rows": [FundSummary(**r).model_dump() for r in _frame_to_records(df)],
        "total_external_aum_usd": float(df["external_aum_usd"].fillna(0).sum()),
    }


@app.get("/top_holdings")
def top_holdings(
    portfolio: str | None = None,
    sector: str | None = None,
    asset_class: str | None = None,
    country: str | None = None,
    group_by: str = Query("name+isin"),
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    where = ["1=1"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    if sector:
        where.append("sector = ?")
        params.append(sector)
    if asset_class:
        where.append("holding_type = ?")
        params.append(asset_class)
    if country:
        where.append("country = ?")
        params.append(country)

    if group_by == "isin":
        group_cols = ["leaf_holding_isin"]
    elif group_by == "name":
        group_cols = ["leaf_holding_name"]
    else:
        group_cols = ["leaf_holding_isin", "leaf_holding_name"]

    extra_cols = ["leaf_holding_ticker"]
    cat_cols = ["holding_type", "sector", "country", "currency"]

    where_sql = " AND ".join(where)
    group_sql = ", ".join(group_cols)
    join_on_tmpl = " AND ".join(f"COALESCE(t.{g},'') = COALESCE(__A__.{g},'')" for g in group_cols)

    dom_ctes = []
    dom_joins = []
    dom_selects = []
    for col in cat_cols + extra_cols:
        cte = (
            f"dom_{col} AS ("
            f"SELECT {group_sql}, {col}, "
            f"ROW_NUMBER() OVER (PARTITION BY {group_sql} "
            f"ORDER BY SUM(market_value_usd) DESC) AS rn "
            f"FROM base "
            f"WHERE {col} IS NOT NULL AND {col} != '' "
            f"GROUP BY {group_sql}, {col})"
        )
        dom_ctes.append(cte)
        alias = f"d_{col}"
        on_clause = join_on_tmpl.replace("__A__", alias)
        dom_joins.append(f"LEFT JOIN dom_{col} {alias} ON {on_clause} AND {alias}.rn = 1")
        dom_selects.append(f"{alias}.{col} AS {col}")

    sql = (
        f"WITH base AS ("
        f"SELECT {group_sql}, leaf_holding_ticker, holding_type, sector, "
        f"country, currency, market_value_usd "
        f"FROM holdings_lookthrough WHERE {where_sql}), "
        f"totals AS ("
        f"SELECT {group_sql}, SUM(market_value_usd) AS market_value_usd, "
        f"COUNT(*) AS appearances FROM base GROUP BY {group_sql} "
        f"ORDER BY market_value_usd DESC LIMIT ?), "
        f"{', '.join(dom_ctes)} "
        f"SELECT {', '.join('t.' + g for g in group_cols)}, "
        f"{', '.join(dom_selects)}, "
        f"t.market_value_usd, t.appearances "
        f"FROM totals t {' '.join(dom_joins)} "
        f"ORDER BY t.market_value_usd DESC"
    )
    df = _read_sql(sql, tuple(params) + (limit,))
    universe_total = (
        _scalar(
            f"SELECT SUM(market_value_usd) FROM holdings_lookthrough WHERE {where_sql}",
            tuple(params),
        )
        or 0.0
    )
    df["weight_pct"] = df["market_value_usd"] / universe_total * 100 if universe_total else 0
    return {
        "rows": _frame_to_records(df),
        "universe_total_usd": float(universe_total),
    }


@app.get("/funds")
def list_funds(
    portfolio: str | None = None,
    asset_class: str | None = None,
    country: str | None = None,
    currency: str | None = None,
    name_contains: str | None = None,
    min_aum_usd: float = Query(0.0, ge=0.0),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    where = ["1=1"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    if asset_class:
        where.append("asset_class = ?")
        params.append(asset_class)
    if country:
        where.append("country = ?")
        params.append(country)
    if currency:
        where.append("currency = ?")
        params.append(currency)
    if name_contains:
        where.append("name LIKE ?")
        params.append(f"%{name_contains}%")
    if min_aum_usd:
        where.append("total_aum_usd >= ?")
        params.append(min_aum_usd)

    total = _scalar(f"SELECT COUNT(*) FROM funds WHERE {' AND '.join(where)}", tuple(params)) or 0
    df = _read_sql(
        f"""SELECT portfolio_id, ticker, isin, name, portfolio,
                   asset_class, sub_asset_class, investment_style,
                   market_type, region, product_view,
                   country, currency,
                   total_aum_usd, external_aum_usd, internally_held_usd,
                   holdings_as_of_date
            FROM funds WHERE {" AND ".join(where)}
            ORDER BY total_aum_usd DESC NULLS LAST
            LIMIT ? OFFSET ?""",
        tuple(params) + (limit, offset),
    )
    return {
        "total": int(total),
        "limit": limit,
        "offset": offset,
        "rows": [FundSummary(**r).model_dump() for r in _frame_to_records(df)],
    }


def _resolve_fund(ticker_or_id: str) -> sqlite3.Row:
    with _conn() as c:
        cur = c.execute(
            """SELECT * FROM funds
               WHERE ticker = ? OR portfolio_id = ? OR isin = ?""",
            (ticker_or_id.upper(), ticker_or_id, ticker_or_id.upper()),
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"fund '{ticker_or_id}' not found")
    return row


@app.get("/fund/{ident}/metrics")
def fund_metrics(ident: str) -> list[dict]:
    f = dict(_resolve_fund(ident))

    def fmt_aum(v: float | None) -> str:
        if v is None:
            return "—"
        if abs(v) >= 1e12:
            return f"${v / 1e12:,.2f}T"
        if abs(v) >= 1e9:
            return f"${v / 1e9:,.2f}B"
        if abs(v) >= 1e6:
            return f"${v / 1e6:,.1f}M"
        return f"${v:,.0f}"

    def fmt_pct(v: float | None) -> str:
        return f"{v:+.2f}%" if isinstance(v, (int, float)) else "—"

    from datetime import date as _date, timedelta as _td

    # 1Y AUM delta from holdings snapshots already in cache (no live fetch
    # here — keeps the metrics endpoint snappy; live fetch happens only on
    # the breakdown/top10 compare widgets).
    aum_delta = ""
    pid = f["portfolio_id"]
    with _conn() as conn:
        rows = conn.execute(
            "SELECT as_of_date, SUM(market_value_usd) "
            "FROM holdings WHERE parent_portfolio_id = ? "
            "GROUP BY as_of_date ORDER BY as_of_date DESC",
            (pid,),
        ).fetchall()
    if len(rows) >= 2:
        try:
            cur_date = _date.fromisoformat(rows[0][0])
            target = cur_date - _td(days=365)
            # Closest at-or-before target within 60 days.
            best = None
            for ad, mv in rows[1:]:
                d = _date.fromisoformat(ad)
                if abs((d - target).days) <= 60:
                    if best is None or abs((d - target).days) < abs(_date.fromisoformat(best[0]) - target).days:
                        best = (ad, mv)
            cur_mv = float(rows[0][1] or 0)
            if best and best[1] and cur_mv:
                prior_mv = float(best[1])
                pct = (cur_mv - prior_mv) / prior_mv * 100 if prior_mv else None
                if pct is not None:
                    aum_delta = f"{pct:+.1f}% vs {best[0]}"
        except (ValueError, ZeroDivisionError):
            pass

    _today = _date.today()
    _ytd_start = f"{_today.year - 1}-12-31"
    _y1_start = _today.replace(year=_today.year - 1).isoformat()
    _y3_start = _today.replace(year=_today.year - 3).isoformat()
    _y5_start = _today.replace(year=_today.year - 5).isoformat()
    _y10_start = _today.replace(year=_today.year - 10).isoformat()

    _ytd = _compute_total_return(pid, _ytd_start)
    _y1 = _compute_total_return(pid, _y1_start)
    _y3 = _compute_total_return(pid, _y3_start, annualize_years=3.0)
    _y5 = _compute_total_return(pid, _y5_start, annualize_years=5.0)
    _y10 = _compute_total_return(pid, _y10_start, annualize_years=10.0)

    tiles = [
        {
            "label": f["ticker"] or f["portfolio_id"],
            "value": fmt_aum(f.get("total_aum_usd")),
            "delta": aum_delta,
        },
        {"label": "YTD Return", "value": fmt_pct(_ytd if _ytd is not None else f.get("nav_ytd_pct")), "delta": ""},
        {"label": "1Y Return", "value": fmt_pct(_y1 if _y1 is not None else f.get("nav_1y_pct")), "delta": ""},
        {"label": "3Y Annualized", "value": fmt_pct(_y3 if _y3 is not None else f.get("nav_3y_pct")), "delta": ""},
        {"label": "5Y Annualized", "value": fmt_pct(_y5 if _y5 is not None else f.get("nav_5y_pct")), "delta": ""},
        {
            "label": "10Y Annualized",
            "value": fmt_pct(_y10 if _y10 is not None else f.get("nav_10y_pct")),
            "delta": "",
        },
        {
            "label": "30-Day SEC Yield",
            "value": fmt_pct(f.get("sec_yield_30d_pct")),
            "delta": "",
        },
        {
            "label": "12M Trailing Yield",
            "value": fmt_pct(f.get("twelve_month_yield_pct")),
            "delta": "",
        },
    ]
    return [t for t in tiles if t["value"] not in ("—", "")]


def _ensure_snapshot_cached(
    fund: dict,
    target_date,
    *,
    max_walk_back: int = 30,
):
    """Return the as_of_date string of the cached snapshot at-or-before target_date.

    Looks in the local DB first; if none within ``max_walk_back`` business
    days exists, performs a live fetch (iShares only) and writes the result
    to the holdings table.  Returns ``None`` if no snapshot is available
    (e.g. unsupported portfolio, fund predates target, or live fetch
    exhausts).
    """
    from .holdings_history import (
        fetch_with_fallback,
        is_us_business_day,
        previous_business_day,
    )

    pid = fund["portfolio_id"]
    target = target_date
    # Snap target to a business day first (walking back).
    while not is_us_business_day(target):
        target = previous_business_day(target)
    earliest = target
    for _ in range(max_walk_back):
        earliest = previous_business_day(earliest)

    with _conn() as conn:
        row = conn.execute(
            "SELECT as_of_date FROM holdings "
            "WHERE parent_portfolio_id = ? AND as_of_date BETWEEN ? AND ? "
            "ORDER BY ABS(julianday(as_of_date) - julianday(?)) LIMIT 1",
            (pid, earliest.isoformat(), target.isoformat(), target.isoformat()),
        ).fetchone()
    if row:
        return row[0]

    if (fund.get("portfolio") or "").lower() != "ishares":
        return None
    page_url = fund.get("product_page_url") or ""
    if not page_url:
        return None
    if not page_url.startswith("http"):
        if "/us/individual" in page_url:
            page_url = "https://www.ishares.com" + page_url.replace("/us/individual", "/us")
        else:
            page_url = "https://www.blackrock.com" + page_url

    import httpx
    from .holdings import FundRef
    from .ingest import PORTFOLIOS, _HDRS
    from .db import replace_holdings_for_fund_date

    cfg = PORTFOLIOS["iShares"]
    template = {
        "ajax_id": cfg["ajax_id"],
        "format": cfg["format"],
        "filename_suffix": cfg["filename_suffix"],
    }
    ref = FundRef(
        portfolio_id=pid,
        ticker=fund.get("ticker"),
        name=fund.get("name") or pid,
        currency=fund.get("currency") or "USD",
        portfolio="iShares",
        product_page_url=page_url,
    )
    try:
        with httpx.Client(headers=_HDRS, follow_redirects=True, timeout=60) as client:
            result = fetch_with_fallback(
                ref,
                template=template,
                client=client,
                requested=target,
                max_walk_back=max_walk_back,
            )
    except Exception:
        return None
    if result is None:
        return None
    holdings, served = result
    write_conn = sqlite3.connect(str(DB_PATH), isolation_level=None)
    try:
        write_conn.execute("PRAGMA foreign_keys = ON")
        write_conn.execute("PRAGMA journal_mode = WAL")
        replace_holdings_for_fund_date(write_conn, pid, served.isoformat(), holdings)
    finally:
        write_conn.close()
    return served.isoformat()


def _grouped_bar_figure(
    categories: list[str],
    series: list[tuple[str, list[float], str]],
    *,
    title: str,
    value_axis_title: str = "",
    value_fmt: str = ",.2f",
    value_suffix: str = "",
    value_prefix: str = "",
    theme: str = "light",
    orientation: str = "v",
) -> dict:
    """Grouped bar chart with value labels on each bar.

    ``series`` is a list of ``(name, values, color)`` tuples. ``orientation``
    is "v" (vertical, categories on x) or "h" (horizontal, categories on y).
    For horizontal, categories render top-to-bottom in the order given.
    """
    is_dark = (theme or "").lower() == "dark"
    paper_bg = "rgba(0,0,0,0)"
    plot_bg = "rgba(0,0,0,0)"
    font_color = "#e5e7eb" if is_dark else "#111827"
    grid_color = "rgba(255,255,255,0.08)" if is_dark else "rgba(0,0,0,0.08)"
    horiz = orientation == "h"

    def _fmt(v: float) -> str:
        if v is None:
            return ""
        try:
            return f"{value_prefix}{format(float(v), value_fmt)}{value_suffix}"
        except (TypeError, ValueError):
            return ""

    data = []
    for name, vals, color in series:
        text_labels = [_fmt(v) for v in vals]
        trace = {
            "type": "bar",
            "name": name,
            "marker": {"color": color, "line": {"width": 0}},
            "text": text_labels,
            "textposition": "outside",
            "textfont": {"color": font_color, "size": 11},
            "cliponaxis": False,
            "hovertemplate": (
                f"<b>%{{{'y' if horiz else 'x'}}}</b><br>"
                f"{name}: {value_prefix}%{{{'x' if horiz else 'y'}:{value_fmt}}}{value_suffix}"
                "<extra></extra>"
            ),
        }
        if horiz:
            trace["orientation"] = "h"
            trace["x"] = vals
            trace["y"] = categories
        else:
            trace["x"] = categories
            trace["y"] = vals
        data.append(trace)

    cat_axis = {
        "tickfont": {"color": font_color, "size": 12},
        "automargin": True,
        "showgrid": False,
        "zeroline": False,
    }
    val_axis = {
        "title": {"text": value_axis_title, "font": {"color": font_color, "size": 12}},
        "tickfont": {"color": font_color, "size": 11},
        "showgrid": True,
        "gridcolor": grid_color,
        "zeroline": False,
        "automargin": True,
    }
    if horiz:
        cat_axis["autorange"] = "reversed"
        xaxis, yaxis = val_axis, cat_axis
        margin = {"t": 64, "b": 50, "l": 110, "r": 40}
    else:
        xaxis, yaxis = cat_axis, val_axis
        margin = {"t": 64, "b": 90, "l": 70, "r": 30}

    return {
        "data": data,
        "layout": {
            "barmode": "group",
            "bargap": 0.30,
            "bargroupgap": 0.08,
            "xaxis": xaxis,
            "yaxis": yaxis,
            "legend": {
                "orientation": "h",
                "x": 0.5,
                "xanchor": "center",
                "y": 1.04,
                "yanchor": "bottom",
                "font": {"color": font_color, "size": 12},
                "bgcolor": "rgba(0,0,0,0)",
            },
            "margin": margin,
            "template": "plotly_dark" if is_dark else "plotly_white",
            "paper_bgcolor": paper_bg,
            "plot_bgcolor": plot_bg,
            "font": {"color": font_color, "family": "Inter, system-ui, sans-serif"},
            "hoverlabel": {"bgcolor": "#111827" if is_dark else "#ffffff"},
        },
        "config": {"responsive": True, "displayModeBar": False},
    }


def _side_by_side_hbar_figure(
    *,
    left: tuple[str, list[str], list[float], str],
    right: tuple[str, list[str], list[float], str],
    value_axis_title: str = "",
    value_fmt: str = ",.2f",
    value_prefix: str = "",
    value_suffix: str = "",
    theme: str = "light",
) -> dict:
    """Two independent horizontal-bar subplots side by side.

    Each panel has its own y-categories and its own x-axis range — the two
    panels show different rankings, not the same items in two periods.
    """
    is_dark = (theme or "").lower() == "dark"
    font_color = "#e5e7eb" if is_dark else "#111827"
    grid_color = "rgba(255,255,255,0.08)" if is_dark else "rgba(0,0,0,0.08)"
    bg = "rgba(0,0,0,0)"

    def _fmt(v: float) -> str:
        try:
            return f"{value_prefix}{format(float(v or 0), value_fmt)}{value_suffix}"
        except (TypeError, ValueError):
            return ""

    left_name, left_cats, left_vals, left_color = left
    right_name, right_cats, right_vals, right_color = right

    def _trace(name, cats, vals, color, xaxis, yaxis):
        return {
            "type": "bar",
            "name": name,
            "orientation": "h",
            "x": vals,
            "y": cats,
            "xaxis": xaxis,
            "yaxis": yaxis,
            "marker": {"color": color, "line": {"width": 0}},
            "text": [_fmt(v) if (v or 0) > 0 else "" for v in vals],
            "textposition": "outside",
            "textfont": {"color": font_color, "size": 11},
            "constraintext": "none",
            "cliponaxis": False,
            "hovertemplate": (f"<b>%{{y}}</b><br>{name}: {value_prefix}%{{x:{value_fmt}}}{value_suffix}<extra></extra>"),
            "showlegend": False,
        }

    def _max(vals):
        m = max((v or 0) for v in vals) if vals else 0
        return m if m > 0 else 1

    left_range = [0, _max(left_vals) * 1.20]
    right_range = [0, _max(right_vals) * 1.20]

    def _xaxis(domain, rng):
        return {
            "title": {
                "text": value_axis_title,
                "font": {"color": font_color, "size": 12},
            },
            "tickfont": {"color": font_color, "size": 11},
            "showgrid": True,
            "gridcolor": grid_color,
            "zeroline": False,
            "automargin": True,
            "domain": domain,
            "range": rng,
            "fixedrange": True,
        }

    def _yaxis(anchor, domain):
        return {
            "tickfont": {"color": font_color, "size": 12},
            "automargin": True,
            "showgrid": False,
            "zeroline": False,
            "autorange": "reversed",
            "anchor": anchor,
            "domain": domain,
            "ticklabelposition": "outside left",
            "fixedrange": True,
        }

    return {
        "data": [
            _trace(left_name, left_cats, left_vals, left_color, "x", "y"),
            _trace(right_name, right_cats, right_vals, right_color, "x2", "y2"),
        ],
        "layout": {
            "xaxis": _xaxis([0.04, 0.48], left_range),
            "xaxis2": _xaxis([0.56, 1.0], right_range),
            "yaxis": _yaxis("x", [0.0, 1.0]),
            "yaxis2": _yaxis("x2", [0.0, 1.0]),
            "annotations": [
                {
                    "text": f"<b>{left_name}</b>",
                    "x": 0.26,
                    "xref": "paper",
                    "y": 1.02,
                    "yref": "paper",
                    "xanchor": "center",
                    "yanchor": "bottom",
                    "showarrow": False,
                    "font": {"color": left_color, "size": 13},
                },
                {
                    "text": f"<b>{right_name}</b>",
                    "x": 0.78,
                    "xref": "paper",
                    "y": 1.02,
                    "yref": "paper",
                    "xanchor": "center",
                    "yanchor": "bottom",
                    "showarrow": False,
                    "font": {"color": right_color, "size": 13},
                },
            ],
            "bargap": 0.30,
            "margin": {"t": 44, "b": 56, "l": 24, "r": 24},
            "template": "plotly_dark" if is_dark else "plotly_white",
            "paper_bgcolor": bg,
            "plot_bgcolor": bg,
            "font": {"color": font_color, "family": "Inter, system-ui, sans-serif"},
            "hoverlabel": {"bgcolor": "#111827" if is_dark else "#ffffff"},
            "showlegend": False,
        },
        "config": {"responsive": True, "displayModeBar": False},
    }


@app.get("/fund/{ident}/holdings")
def fund_holdings(ident: str, as_of_date: str | None = None) -> dict:
    fund_row = _resolve_fund(ident)
    fund = dict(fund_row)
    pid = fund["portfolio_id"]
    cols = (
        "holding_ticker, holding_name, holding_isin, holding_cusip, "
        "holding_sedol, holding_type, sector, country, exchange, "
        "currency, fx_rate, market_value_usd, weight_pct, price, "
        "coupon_pct, maturity_date, duration, "
        "ytm_pct, yield_to_call_pct, yield_to_worst_pct, mod_duration, "
        "as_of_date"
    )

    if as_of_date is None:
        df = _read_sql(
            f"SELECT {cols} FROM holdings WHERE parent_portfolio_id = ? "
            "AND as_of_date = (SELECT MAX(as_of_date) FROM holdings WHERE parent_portfolio_id = ?) "
            "ORDER BY market_value_usd DESC",
            (pid, pid),
        )
        df = _drop_empty_columns(df, keep={"holding_name", "market_value_usd", "weight_pct"})
        return {
            "fund": dict(fund),
            "total": int(df.shape[0]),
            "rows": _frame_to_records(df),
        }

    # Date-scoped path: validate, check cache, live-fetch + walk-back, persist.
    from datetime import date as _date
    from .holdings_history import (
        fetch_with_fallback,
        is_us_business_day,
        previous_business_day,
    )

    try:
        requested = _date.fromisoformat(as_of_date)
    except ValueError:
        return {
            "fund": dict(fund),
            "error": f"invalid date '{as_of_date}' — use YYYY-MM-DD",
            "rows": [],
        }
    if not is_us_business_day(requested):
        return {
            "fund": dict(fund),
            "requested_date": requested.isoformat(),
            "error": f"{requested.isoformat()} is not a US trading day",
            "rows": [],
        }

    def _serve(served: _date, cached: bool) -> dict:
        df = _read_sql(
            f"SELECT {cols} FROM holdings "
            "WHERE parent_portfolio_id = ? AND as_of_date = ? "
            "ORDER BY market_value_usd DESC",
            (pid, served.isoformat()),
        )
        df = _drop_empty_columns(df, keep={"holding_name", "market_value_usd", "weight_pct"})
        return {
            "fund": dict(fund),
            "requested_date": requested.isoformat(),
            "served_date": served.isoformat(),
            "cached": cached,
            "total": int(df.shape[0]),
            "rows": _frame_to_records(df),
        }

    # Cache probe — also accepts a prior business day if the requested date
    # itself isn't published (so we don't re-hit BlackRock for known walk-backs).
    with _conn() as conn:
        row = conn.execute(
            "SELECT as_of_date FROM holdings "
            "WHERE parent_portfolio_id = ? AND as_of_date <= ? "
            "ORDER BY as_of_date DESC LIMIT 1",
            (pid, requested.isoformat()),
        ).fetchone()
    if row:
        cached_dt = _date.fromisoformat(row[0])
        # Only treat as a hit if cached date is within our walk-back window
        # of the request — otherwise the user likely wants a fresh fetch.
        delta_bdays = 0
        cur = requested
        while cur > cached_dt and delta_bdays <= 5:
            cur = previous_business_day(cur)
            delta_bdays += 1
        if cur == cached_dt:
            return _serve(cached_dt, cached=True)

    # Live fetch via the iShares template.
    if (fund.get("portfolio") or "").lower() != "ishares":
        return {
            "fund": dict(fund),
            "requested_date": requested.isoformat(),
            "error": (f"historical holdings unavailable: portfolio '{fund.get('portfolio')}' not yet supported"),
            "rows": [],
        }

    import httpx
    from .holdings import FundRef
    from .ingest import PORTFOLIOS, _HDRS

    cfg = PORTFOLIOS["iShares"]
    template = {
        "ajax_id": cfg["ajax_id"],
        "format": cfg["format"],
        "filename_suffix": cfg["filename_suffix"],
    }
    page_url = fund.get("product_page_url") or ""
    if page_url and not page_url.startswith("http"):
        if "/us/individual" in page_url:
            page_url = "https://www.ishares.com" + page_url.replace("/us/individual", "/us")
        else:
            page_url = "https://www.blackrock.com" + page_url
    if not page_url:
        return {
            "fund": dict(fund),
            "requested_date": requested.isoformat(),
            "error": "fund has no product_page_url; cannot fetch history",
            "rows": [],
        }

    ref = FundRef(
        portfolio_id=pid,
        ticker=fund.get("ticker"),
        name=fund.get("name") or pid,
        currency=fund.get("currency") or "USD",
        portfolio="iShares",
        product_page_url=page_url,
    )
    try:
        with httpx.Client(headers=_HDRS, follow_redirects=True, timeout=60) as client:
            result = fetch_with_fallback(
                ref,
                template=template,
                client=client,
                requested=requested,
            )
    except Exception as exc:
        return {
            "fund": dict(fund),
            "requested_date": requested.isoformat(),
            "error": f"fetch failed: {exc}",
            "rows": [],
        }
    if result is None:
        return {
            "fund": dict(fund),
            "requested_date": requested.isoformat(),
            "error": (f"no holdings available within 5 business days of {requested.isoformat()}"),
            "rows": [],
        }
    holdings, served = result
    from .db import replace_holdings_for_fund_date

    write_conn = sqlite3.connect(str(DB_PATH), isolation_level=None)
    try:
        write_conn.execute("PRAGMA foreign_keys = ON")
        write_conn.execute("PRAGMA journal_mode = WAL")
        replace_holdings_for_fund_date(write_conn, pid, served.isoformat(), holdings)
    finally:
        write_conn.close()
    return _serve(served, cached=False)


_COMPARE_AXES = {
    "asset_class": ("holding_type", "Asset Class"),
    "sector": ("sector", "Sector"),
    "country": ("country", "Country"),
}


def _holdings_at_date(pid: str, as_of_date: str, axis_col: str | None = None) -> pd.DataFrame:
    """Aggregate one snapshot's holdings by axis (or per-row if axis None).

    Returns DataFrame with columns ``label, market_value_usd, weight_pct``
    when ``axis_col`` is set, else raw rows with at least
    ``holding_ticker, holding_name, market_value_usd, weight_pct``.
    """
    if axis_col:
        df = _read_sql(
            f"""SELECT COALESCE({axis_col}, 'Unknown') AS label,
                       SUM(market_value_usd)           AS market_value_usd,
                       SUM(weight_pct)                 AS weight_pct
                FROM holdings
                WHERE parent_portfolio_id = ? AND as_of_date = ?
                GROUP BY label
                ORDER BY market_value_usd DESC""",
            (pid, as_of_date),
        )
        df["label"] = df["label"].apply(lambda l: "Other" if l is None or _GARBAGE_LABEL_RE.match(str(l).strip()) else l)
        df = (
            df.groupby("label", as_index=False)
            .agg({"market_value_usd": "sum", "weight_pct": "sum"})
            .sort_values("market_value_usd", ascending=False)
        )
        return df
    return _read_sql(
        """SELECT holding_ticker, holding_name, market_value_usd, weight_pct
           FROM holdings
           WHERE parent_portfolio_id = ? AND as_of_date = ?
           ORDER BY market_value_usd DESC""",
        (pid, as_of_date),
    )


def _resolve_compare_dates(fund: dict) -> tuple[str | None, str | None]:
    """Pick (current, ~1y-ago) as_of_date strings for a fund.

    Current = most recent snapshot in DB.  1Y-ago = nearest cached snapshot
    to current-365d, or live-fetched on miss.  Either may be ``None`` if
    unavailable.
    """
    from datetime import date as _date, timedelta as _td

    pid = fund["portfolio_id"]
    with _conn() as conn:
        row = conn.execute(
            "SELECT MAX(as_of_date) FROM holdings WHERE parent_portfolio_id = ?",
            (pid,),
        ).fetchone()
    current = row[0] if row else None
    if not current:
        return None, None
    try:
        current_dt = _date.fromisoformat(current)
    except ValueError:
        return current, None
    target = current_dt - _td(days=365)
    prior = _ensure_snapshot_cached(fund, target, max_walk_back=30)
    return current, prior


@app.get("/fund/{ident}/top10_compare")
def fund_top10_compare(
    ident: str,
    top: int = Query(10, ge=3, le=30),
    metric: str = Query("weight_pct", pattern="^(weight_pct|market_value_usd)$"),
    raw: bool = Query(False),
    theme: str = "light",
) -> Any:
    """Top-N holdings of the current snapshot beside the top-N of ~1y ago.

    The two panels are independent rankings — the right panel shows the
    top-N *as of the prior date*, not the current top-N's prior values.
    """
    fund = dict(_resolve_fund(ident))
    pid = fund["portfolio_id"]
    current_date, prior_date = _resolve_compare_dates(fund)
    if not current_date:
        return _grouped_bar_figure([], [], theme=theme)

    def _topn(as_of: str) -> tuple[list[str], list[float]]:
        df = _holdings_at_date(pid, as_of).head(top)
        if df.empty:
            return [], []
        cats = []
        for tk, name in zip(df["holding_ticker"].tolist(), df["holding_name"].tolist()):
            tk_clean = str(tk).strip().upper() if tk and str(tk).strip() not in ("-", "") else None
            cats.append(tk_clean or (str(name)[:18] if name else "?"))
        vals = [float(v or 0) for v in df[metric].tolist()]
        return cats, vals

    cur_cats, cur_vals = _topn(current_date)
    prior_cats, prior_vals = ([], [])
    if prior_date:
        prior_cats, prior_vals = _topn(prior_date)

    if raw:
        rows: list[dict] = []
        for rank, (cat, val) in enumerate(zip(cur_cats, cur_vals), 1):
            rows.append(
                {
                    "side": "current",
                    "as_of_date": current_date,
                    "rank": rank,
                    "label": cat,
                    metric: val,
                }
            )
        for rank, (cat, val) in enumerate(zip(prior_cats, prior_vals), 1):
            rows.append(
                {
                    "side": "prior",
                    "as_of_date": prior_date,
                    "rank": rank,
                    "label": cat,
                    metric: val,
                }
            )
        return rows

    if metric == "weight_pct":
        value_axis_title, value_fmt, value_prefix, value_suffix = (
            "Weight (%)",
            ".2f",
            "",
            "%",
        )
    else:
        value_axis_title, value_fmt, value_prefix, value_suffix = (
            "Market Value (USD)",
            ",.0f",
            "$",
            "",
        )
    return _side_by_side_hbar_figure(
        left=(f"Current ({current_date})", cur_cats, cur_vals, "#1f6feb"),
        right=(f"1Y Ago ({prior_date or 'n/a'})", prior_cats, prior_vals, "#f59e0b"),
        value_axis_title=value_axis_title,
        value_fmt=value_fmt,
        value_prefix=value_prefix,
        value_suffix=value_suffix,
        theme=theme,
    )


@app.get("/fund/{ident}/breakdown_compare")
def fund_breakdown_compare(
    ident: str,
    axis: str = Query(..., pattern="^(asset_class|sector|country)$"),
    top: int = Query(10, ge=3, le=30),
    metric: str = Query("weight_pct", pattern="^(weight_pct|market_value_usd)$"),
    raw: bool = Query(False),
    theme: str = "light",
) -> Any:
    """Allocation by axis, current vs ~1y ago, as two side-by-side hbars.

    Each panel ranks its own snapshot independently. ``weight_pct`` values are
    normalized to sum to 100% within each side so periods are comparable even
    when raw rows total >100% (lookthrough/derivatives); ``market_value_usd``
    is shown as raw dollar amounts.
    """
    fund = dict(_resolve_fund(ident))
    pid = fund["portfolio_id"]
    axis_col, axis_label = _COMPARE_AXES[axis]
    current_date, prior_date = _resolve_compare_dates(fund)
    if not current_date:
        return _grouped_bar_figure([], [], theme=theme)

    def _side(as_of: str) -> tuple[list[str], list[float]]:
        df = _holdings_at_date(pid, as_of, axis_col=axis_col).head(top)
        if df.empty:
            return [], []
        labels = [str(l) for l in df["label"].tolist()]
        vals = [float(v or 0) for v in df[metric].tolist()]
        if metric == "weight_pct":
            total = sum(vals)
            if total > 0:
                vals = [v * 100.0 / total for v in vals]
        return labels, vals

    cur_cats, cur_vals = _side(current_date)
    prior_cats, prior_vals = ([], [])
    if prior_date:
        prior_cats, prior_vals = _side(prior_date)

    if raw:
        rows: list[dict] = []
        for cat, val in zip(cur_cats, cur_vals):
            rows.append(
                {
                    "side": "current",
                    "as_of_date": current_date,
                    "axis": axis,
                    "label": cat,
                    metric: val,
                }
            )
        for cat, val in zip(prior_cats, prior_vals):
            rows.append(
                {
                    "side": "prior",
                    "as_of_date": prior_date,
                    "axis": axis,
                    "label": cat,
                    metric: val,
                }
            )
        return rows

    if metric == "weight_pct":
        value_axis_title, value_fmt, value_prefix, value_suffix = (
            "Weight (%)",
            ".2f",
            "",
            "%",
        )
    else:
        value_axis_title, value_fmt, value_prefix, value_suffix = (
            "Market Value (USD)",
            ",.0f",
            "$",
            "",
        )
    return _side_by_side_hbar_figure(
        left=(f"Current ({current_date})", cur_cats, cur_vals, "#1f6feb"),
        right=(f"1Y Ago ({prior_date or 'n/a'})", prior_cats, prior_vals, "#f59e0b"),
        value_axis_title=value_axis_title,
        value_fmt=value_fmt,
        value_prefix=value_prefix,
        value_suffix=value_suffix,
        theme=theme,
    )


@app.get("/fund/{ident}/lookthrough")
def fund_lookthrough(
    ident: str,
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> dict:
    fund = _resolve_fund(ident)
    df = _read_sql(
        """SELECT leaf_holding_ticker, leaf_holding_name, leaf_holding_isin,
                  holding_type, sector, country, currency,
                  market_value_usd, weight_pct, path_depth
           FROM holdings_lookthrough
           WHERE parent_portfolio_id = ?
           ORDER BY market_value_usd DESC
           LIMIT ? OFFSET ?""",
        (fund["portfolio_id"], limit, offset),
    )
    if (df["path_depth"] == 0).all():
        df = df.drop(columns=["path_depth"])
    df = _drop_empty_columns(df, keep={"leaf_holding_name", "market_value_usd", "weight_pct"})
    return {
        "fund": dict(fund),
        "rows": _frame_to_records(df),
    }


@app.get("/fund/{ident}/distributions")
def fund_distributions(ident: str, limit: int = Query(120, ge=1, le=2000)) -> dict:
    fund = dict(_resolve_fund(ident))
    df = _read_sql(
        """SELECT ex_date, record_date, payable_date,
                  total_distribution, income, st_cap_gains, lt_cap_gains,
                  return_of_capital
           FROM distributions WHERE portfolio_id = ?
           ORDER BY ex_date DESC LIMIT ?""",
        (fund["portfolio_id"], limit),
    )
    return {
        "fund": fund,
        "rows": _frame_to_records(df),
        "count": int(df.shape[0]),
    }


@app.get("/fund_metrics")
def fund_metrics_qs(ticker: str = Query(...)) -> list[dict]:
    return fund_metrics(ticker)


@app.get("/fund_holdings")
def fund_holdings_qs(
    ticker: str = Query(...),
    as_of_date: str | None = Query(None),
) -> dict:
    return fund_holdings(ticker, as_of_date=as_of_date)


@app.get("/fund_top10_compare")
def fund_top10_compare_qs(
    ticker: str = Query(...),
    top: int = Query(10, ge=3, le=30),
    metric: str = Query("weight_pct"),
    raw: bool = Query(False),
    theme: str = Query("light"),
) -> Any:
    return fund_top10_compare(ticker, top=top, metric=metric, raw=raw, theme=theme)


@app.get("/fund_breakdown_compare")
def fund_breakdown_compare_qs(
    ticker: str = Query(...),
    axis: str = Query(...),
    top: int = Query(10, ge=3, le=30),
    metric: str = Query("weight_pct"),
    raw: bool = Query(False),
    theme: str = Query("light"),
) -> Any:
    return fund_breakdown_compare(ticker, axis=axis, top=top, metric=metric, raw=raw, theme=theme)


@app.get("/fund_lookthrough")
def fund_lookthrough_qs(
    ticker: str = Query(...),
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> dict:
    return fund_lookthrough(ticker, limit=limit, offset=offset)


@app.get("/fund/{ident}/nav_history")
def fund_nav_history(ident: str) -> dict:
    fund = dict(_resolve_fund(ident))
    df = _read_sql(
        """SELECT as_of_date, nav_per_share, shares_outstanding,
                  ex_dividends, daily_return_pct
           FROM nav_history WHERE portfolio_id = ?
           ORDER BY as_of_date ASC""",
        (fund["portfolio_id"],),
    )
    return {
        "fund": fund,
        "rows": _frame_to_records(df),
        "count": int(df.shape[0]),
    }


@app.get("/fund_nav_history")
def fund_nav_history_qs(ticker: str = Query(...)) -> dict:
    return fund_nav_history(ticker)


@app.get("/fund_distributions")
def fund_distributions_qs(
    ticker: str = Query(...),
    limit: int = Query(120, ge=1, le=2000),
) -> dict:
    return fund_distributions(ticker, limit=limit)


# ---------------------------------------------------------------------------
# Performance chart data — fetched from iShares product page and cached
# ---------------------------------------------------------------------------

_PERF_CHART_CACHE: dict[str, tuple[float, dict]] = {}
_PERF_CHART_TTL = 3600  # seconds

_PD_CACHE: dict[str, tuple[float, dict]] = {}
_PD_TTL = 3600  # seconds


def _utm_source_url(fund: dict) -> str | None:
    page_url = (fund.get("product_page_url") or "").strip()
    if not page_url:
        return None
    page_url = page_url.replace("/individual/products/", "/products/")
    if not page_url.startswith("http"):
        page_url = "https://www.ishares.com" + page_url
    if "?" in page_url:
        page_url = page_url.split("?")[0]
    return page_url + "?utm_source=openai"


def _pd_base_url(fund: dict) -> str | None:
    page_url = (fund.get("product_page_url") or "").strip()
    if not page_url:
        return None
    page_url = page_url.replace("/individual/products/", "/products/")
    if not page_url.startswith("http"):
        page_url = "https://www.ishares.com" + page_url
    if "?" in page_url:
        page_url = page_url.split("?")[0]
    return page_url + "/1467271812596.ajax?fileType=csv&dataType=premiumDiscount"


def _fetch_premium_discount(portfolio_id: str, pd_url: str) -> dict | None:
    cached = _PD_CACHE.get(portfolio_id)
    if cached and (time.time() - cached[0]) < _PD_TTL:
        return cached[1]

    r = _urllib_req.Request(
        pd_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "*/*",
            "Referer": "https://www.ishares.com/",
        },
    )
    try:
        with _urllib_req.urlopen(r, timeout=45) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    decoded = _html_mod.unescape(body)

    chart_parent = decoded.find('"premium-discount-chart"')
    if chart_parent < 0:
        return None
    pd_idx = decoded.find('"name":"premiumDiscountChartData"', chart_parent)
    if pd_idx < 0:
        return None

    date_m = re.search(r'"asOfDate":\[([0-9,]+)\]', decoded[chart_parent : pd_idx + 10000])
    if not date_m:
        return None
    dates = [int(x) for x in date_m.group(1).split(",")]

    val_m = re.search(
        r'"name":"premiumDiscountChartData"[^[]+\[([^\]]+)\]',
        decoded[pd_idx : pd_idx + 50000],
    )
    if not val_m:
        return None
    vals: list[float | None] = []
    for x in val_m.group(1).split(","):
        try:
            vals.append(float(x.strip().strip('"')))
        except ValueError:
            vals.append(None)

    quarters: dict[str, dict] = {}
    qidx = decoded.find('"name":"premiumDiscountsByQuarters"')
    if qidx >= 0:
        qval_m = re.search(r'"value":(\{.*?\}),"visible"', decoded[qidx : qidx + 10000], re.DOTALL)
        if qval_m:
            try:
                for v in json.loads(qval_m.group(1)).values():
                    start = v.get("startDate")
                    if start:
                        quarters[str(start)] = {
                            "start": start,
                            "end": v.get("endDate"),
                            "premium_days": v.get("premiumDays"),
                            "nav_days": v.get("navDays"),
                            "discount_days": v.get("discountDays"),
                        }
            except Exception:
                pass

    annual: dict = {}
    yidx = decoded.find('"name":"previousYearEndDisc"')
    if yidx >= 0:
        chunk = decoded[yidx : yidx + 2000]
        for field in ["startDate", "endDate", "premiumDays", "navDays", "discountDays"]:
            fm = re.search(f'"{field}":([0-9-]+)', chunk)
            if fm:
                try:
                    annual[field] = int(fm.group(1))
                except ValueError:
                    pass

    data: dict = {"dates": dates, "vals": vals, "quarters": quarters, "annual": annual}
    _PD_CACHE[portfolio_id] = (time.time(), data)
    return data


def _fetch_perf_chart_data(portfolio_id: str, utm_url: str) -> dict | None:
    cached = _PERF_CHART_CACHE.get(portfolio_id)
    if cached and (time.time() - cached[0]) < _PERF_CHART_TTL:
        return cached[1]

    req = _urllib_req.Request(
        utm_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with _urllib_req.urlopen(req, timeout=45) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    decoded = _html_mod.unescape(body)

    perf_idx = decoded.find('"name":"performanceData"')
    if perf_idx < 0:
        return None
    perf_obj_start = decoded.rfind("{", 0, perf_idx)
    if perf_obj_start < 0:
        return None

    m = re.search(r'"asOfDate":\[([0-9,]+)\]', decoded[perf_obj_start : perf_obj_start + 300000])
    if not m:
        return None
    fund_dates = [int(x) for x in m.group(1).split(",")]

    pm = re.search(r'"name":"performanceData"[^[]+\[([^\]]+)\]', decoded[perf_idx : perf_idx + 200000])
    if not pm:
        return None
    fund_vals = [float(x.strip('" ')) for x in pm.group(1).split(",")]

    bm_idx = decoded.find('"name":"benchmarkData"')
    if bm_idx < 0:
        return None
    bm_obj_start = decoded.rfind("{", 0, bm_idx)
    if bm_obj_start < 0:
        return None

    mb = re.search(r'"asOfDate":\[([0-9,]+)\]', decoded[bm_obj_start : bm_obj_start + 300000])
    if not mb:
        return None
    bm_dates = [int(x) for x in mb.group(1).split(",")]

    bmv = re.search(r'"name":"benchmarkData"[^[]+\[([^\]]+)\]', decoded[bm_idx : bm_idx + 200000])
    if not bmv:
        return None
    bm_vals = [float(x.strip('" ')) for x in bmv.group(1).split(",")]

    bm_name = "Benchmark"
    nm_idx = decoded.find('"benchmarkName"', bm_obj_start, bm_obj_start + 400000)
    if nm_idx > 0:
        nm_val = re.search(r'"formattedValue":"([^"]+)"', decoded[nm_idx : nm_idx + 500])
        if nm_val:
            bm_name = nm_val.group(1)

    data: dict = {
        "fund_dates": fund_dates,
        "fund_vals": fund_vals,
        "bm_dates": bm_dates,
        "bm_vals": bm_vals,
        "bm_name": bm_name,
    }
    _PERF_CHART_CACHE[portfolio_id] = (time.time(), data)
    return data


def _ymd_int_to_str(ymd: int) -> str:
    y, md = divmod(ymd, 10000)
    m, d = divmod(md, 100)
    return f"{y:04d}-{m:02d}-{d:02d}"


def _date_to_ymd(d: "date") -> int:
    return d.year * 10000 + d.month * 100 + d.day


def _bm_return_from_series(
    dates: list[int],
    vals: list[float],
    start_ymd: int,
    end_ymd: int | None = None,
    *,
    annualize_years: float | None = None,
) -> float | None:
    if not dates or not vals:
        return None
    if end_ymd is None:
        end_ymd = dates[-1]
    si = bisect.bisect_right(dates, start_ymd) - 1
    if si < 0:
        return None
    ei = bisect.bisect_right(dates, end_ymd) - 1
    if ei <= si:
        return None
    sv, ev = vals[si], vals[ei]
    if sv <= 0:
        return None
    raw = ev / sv - 1.0
    if annualize_years and annualize_years > 0:
        try:
            raw = (1.0 + raw) ** (1.0 / annualize_years) - 1.0
        except (ValueError, ZeroDivisionError):
            return None
    return raw * 100.0


@app.get("/fund/{ident}/growth_10k")
def fund_growth_10k(
    ident: str,
    theme: str = Query("light"),
    raw: bool = Query(False),
) -> Any:
    fund = dict(_resolve_fund(ident))
    utm_url = _utm_source_url(fund)
    if not utm_url:
        raise HTTPException(status_code=404, detail="No product page URL for this fund")

    data = _fetch_perf_chart_data(fund["portfolio_id"], utm_url)
    if not data:
        raise HTTPException(status_code=502, detail="Could not retrieve chart data from iShares")

    fund_date_strs = [_ymd_int_to_str(d) for d in data["fund_dates"]]
    bm_date_strs = [_ymd_int_to_str(d) for d in data["bm_dates"]]

    fund_name = fund.get("name") or ident.upper()
    bm_name = data["bm_name"]

    if raw:
        fund_col = fund.get("ticker") or ident.upper()
        fund_map = dict(zip(fund_date_strs, data["fund_vals"]))
        bm_map = dict(zip(bm_date_strs, data["bm_vals"]))
        all_dates = sorted(set(fund_date_strs) | set(bm_date_strs))
        return [{"date": d, fund_col: fund_map.get(d), bm_name: bm_map.get(d)} for d in all_dates]

    is_dark = (theme or "").lower() == "dark"
    paper_bg = "rgba(0,0,0,0)"
    plot_bg = "rgba(0,0,0,0)"
    font_color = "#e5e7eb" if is_dark else "#111827"
    grid_color = "rgba(255,255,255,0.08)" if is_dark else "rgba(0,0,0,0.08)"

    return {
        "data": [
            {
                "type": "scatter",
                "mode": "lines",
                "name": fund.get("ticker") or ident.upper(),
                "x": fund_date_strs,
                "y": data["fund_vals"],
                "line": {"color": "#003c7e", "width": 2},
                "hovertemplate": "%{x}<br>$%{y:,.0f}<extra></extra>",
            },
            {
                "type": "scatter",
                "mode": "lines",
                "name": bm_name,
                "x": bm_date_strs,
                "y": data["bm_vals"],
                "line": {"color": "#f59e0b", "width": 2},
                "hovertemplate": "%{x}<br>$%{y:,.0f}<extra></extra>",
            },
        ],
        "layout": {
            "xaxis": {
                "type": "date",
                "tickfont": {"color": font_color, "size": 11},
                "showgrid": False,
                "zeroline": False,
            },
            "yaxis": {
                "tickprefix": "$",
                "tickformat": ",.0f",
                "tickfont": {"color": font_color, "size": 11},
                "showgrid": True,
                "gridcolor": grid_color,
                "zeroline": False,
            },
            "legend": {
                "orientation": "h",
                "x": 0.5,
                "xanchor": "center",
                "y": 1.02,
                "yanchor": "bottom",
                "font": {"color": font_color, "size": 12},
                "bgcolor": "rgba(0,0,0,0)",
            },
            "margin": {"t": 40, "b": 50, "l": 70, "r": 20},
            "template": "plotly_dark" if is_dark else "plotly_white",
            "paper_bgcolor": paper_bg,
            "plot_bgcolor": plot_bg,
            "font": {"color": font_color, "family": "Inter, system-ui, sans-serif"},
            "hoverlabel": {"bgcolor": "#111827" if is_dark else "#ffffff"},
        },
        "config": {"responsive": True, "displayModeBar": False},
    }


@app.get("/fund/{ident}/performance")
def fund_performance(
    ident: str,
    tab: str = Query("average_annual"),
) -> dict:
    from datetime import date as _date

    fund = dict(_resolve_fund(ident))
    pid = fund["portfolio_id"]
    today = _date.today()

    utm_url = _utm_source_url(fund)
    chart_data = _fetch_perf_chart_data(pid, utm_url) if utm_url else None

    bm_dates = chart_data["bm_dates"] if chart_data else []
    bm_vals = chart_data["bm_vals"] if chart_data else []
    bm_name = (chart_data["bm_name"] if chart_data else "Benchmark") or "Benchmark"

    today_ymd = _date_to_ymd(today)

    def _fund_ret(start: str, *, ann: float | None = None, end: str | None = None) -> float | None:
        return _compute_total_return(pid, start, annualize_years=ann, end_date=end)

    def _bm_ret(start_ymd: int, *, ann: float | None = None, end_ymd: int | None = None) -> float | None:
        return _bm_return_from_series(bm_dates, bm_vals, start_ymd, end_ymd, annualize_years=ann)

    def _fmt(v: float | None) -> float | None:
        if v is None:
            return None
        return round(v, 2)

    if tab == "average_annual":
        ytd_start = f"{today.year - 1}-12-31"
        y1_start = today.replace(year=today.year - 1).isoformat()
        y3_start = today.replace(year=today.year - 3).isoformat()
        y5_start = today.replace(year=today.year - 5).isoformat()
        y10_start = today.replace(year=today.year - 10).isoformat()

        with _conn() as conn:
            inc_row = conn.execute(
                "SELECT MIN(as_of_date) FROM nav_history WHERE portfolio_id = ? AND nav_per_share IS NOT NULL",
                (pid,),
            ).fetchone()
        inception_date = inc_row[0] if inc_row and inc_row[0] else None
        inc_years: float | None = None
        if inception_date:
            try:
                inc_dt = _date.fromisoformat(inception_date)
                inc_years = max((today - inc_dt).days / 365.25, 0.01)
            except ValueError:
                pass

        rows = [
            {
                "period": "YTD",
                "fund_nav_pct": _fmt(_fund_ret(ytd_start)),
                "benchmark_pct": _fmt(_bm_ret(_date_to_ymd(today.replace(year=today.year - 1, month=12, day=31)))),
            },
            {
                "period": "1 Year",
                "fund_nav_pct": _fmt(_fund_ret(y1_start, ann=1.0)),
                "benchmark_pct": _fmt(_bm_ret(int(y1_start.replace("-", "")), ann=1.0)),
            },
            {
                "period": "3 Year",
                "fund_nav_pct": _fmt(_fund_ret(y3_start, ann=3.0)),
                "benchmark_pct": _fmt(_bm_ret(int(y3_start.replace("-", "")), ann=3.0)),
            },
            {
                "period": "5 Year",
                "fund_nav_pct": _fmt(_fund_ret(y5_start, ann=5.0)),
                "benchmark_pct": _fmt(_bm_ret(int(y5_start.replace("-", "")), ann=5.0)),
            },
            {
                "period": "10 Year",
                "fund_nav_pct": _fmt(_fund_ret(y10_start, ann=10.0)),
                "benchmark_pct": _fmt(_bm_ret(int(y10_start.replace("-", "")), ann=10.0)),
            },
        ]
        if inception_date and inc_years is not None:
            rows.append(
                {
                    "period": "Since Inception",
                    "fund_nav_pct": _fmt(_fund_ret(inception_date, ann=inc_years)),
                    "benchmark_pct": _fmt(_bm_ret(int(inception_date.replace("-", "")), ann=inc_years)),
                }
            )

    elif tab == "cumulative":
        ytd_start = f"{today.year - 1}-12-31"
        y1_start = today.replace(year=today.year - 1).isoformat()
        y3_start = today.replace(year=today.year - 3).isoformat()
        y5_start = today.replace(year=today.year - 5).isoformat()
        y10_start = today.replace(year=today.year - 10).isoformat()

        def _months_ago(n: int) -> str:
            m = today.month - n
            y = today.year
            while m <= 0:
                m += 12
                y -= 1
            import calendar

            last_day = calendar.monthrange(y, m)[1]
            return f"{y}-{m:02d}-{min(today.day, last_day):02d}"

        with _conn() as conn:
            inc_row = conn.execute(
                "SELECT MIN(as_of_date) FROM nav_history WHERE portfolio_id = ? AND nav_per_share IS NOT NULL",
                (pid,),
            ).fetchone()
        inception_date = inc_row[0] if inc_row and inc_row[0] else None

        rows = [
            {
                "period": "YTD",
                "fund_nav_pct": _fmt(_fund_ret(ytd_start)),
                "benchmark_pct": _fmt(_bm_ret(_date_to_ymd(today.replace(year=today.year - 1, month=12, day=31)))),
            },
            {
                "period": "1 Month",
                "fund_nav_pct": _fmt(_fund_ret(_months_ago(1))),
                "benchmark_pct": _fmt(_bm_ret(int(_months_ago(1).replace("-", "")))),
            },
            {
                "period": "3 Month",
                "fund_nav_pct": _fmt(_fund_ret(_months_ago(3))),
                "benchmark_pct": _fmt(_bm_ret(int(_months_ago(3).replace("-", "")))),
            },
            {
                "period": "6 Month",
                "fund_nav_pct": _fmt(_fund_ret(_months_ago(6))),
                "benchmark_pct": _fmt(_bm_ret(int(_months_ago(6).replace("-", "")))),
            },
            {
                "period": "1 Year",
                "fund_nav_pct": _fmt(_fund_ret(y1_start)),
                "benchmark_pct": _fmt(_bm_ret(int(y1_start.replace("-", "")))),
            },
            {
                "period": "3 Year",
                "fund_nav_pct": _fmt(_fund_ret(y3_start)),
                "benchmark_pct": _fmt(_bm_ret(int(y3_start.replace("-", "")))),
            },
            {
                "period": "5 Year",
                "fund_nav_pct": _fmt(_fund_ret(y5_start)),
                "benchmark_pct": _fmt(_bm_ret(int(y5_start.replace("-", "")))),
            },
            {
                "period": "10 Year",
                "fund_nav_pct": _fmt(_fund_ret(y10_start)),
                "benchmark_pct": _fmt(_bm_ret(int(y10_start.replace("-", "")))),
            },
        ]
        if inception_date:
            rows.append(
                {
                    "period": "Since Inception",
                    "fund_nav_pct": _fmt(_fund_ret(inception_date)),
                    "benchmark_pct": _fmt(_bm_ret(int(inception_date.replace("-", "")))),
                }
            )

    elif tab == "calendar_year":
        with _conn() as conn:
            inc_row = conn.execute(
                "SELECT MIN(as_of_date) FROM nav_history WHERE portfolio_id = ? AND nav_per_share IS NOT NULL",
                (pid,),
            ).fetchone()
        inception_date = inc_row[0] if inc_row and inc_row[0] else None
        if not inception_date:
            return {"as_of": today.isoformat(), "tab": tab, "benchmark_name": bm_name, "rows": []}

        try:
            inception_year = int(inception_date[:4])
        except ValueError:
            inception_year = today.year

        rows = []
        for yr in range(today.year, inception_year - 1, -1):
            start_str = f"{yr - 1}-12-31"
            if yr < today.year:
                end_str = f"{yr}-12-31"
            else:
                end_str = None
            end_ymd = int(end_str.replace("-", "")) if end_str else None
            rows.append(
                {
                    "period": str(yr),
                    "fund_nav_pct": _fmt(_fund_ret(start_str, end=end_str)),
                    "benchmark_pct": _fmt(_bm_ret(int(start_str.replace("-", "")), end_ymd=end_ymd)),
                }
            )

    else:
        raise HTTPException(
            status_code=400,
            detail=f"unknown tab '{tab}'; choose from average_annual, cumulative, calendar_year",
        )

    return {
        "as_of": today.isoformat(),
        "tab": tab,
        "benchmark_name": bm_name,
        "rows": rows,
    }


@app.get("/fund_growth_10k")
def fund_growth_10k_qs(
    ticker: str = Query(...),
    theme: str = Query("light"),
    raw: bool = Query(False),
) -> Any:
    return fund_growth_10k(ticker, theme=theme, raw=raw)


@app.get("/fund/{ident}/premium_discount")
def fund_premium_discount(
    ident: str,
    theme: str = Query("light"),
    raw: bool = Query(False),
) -> Any:
    fund = dict(_resolve_fund(ident))
    pd_url = _pd_base_url(fund)
    if not pd_url:
        raise HTTPException(status_code=404, detail="No product page URL for this fund")

    data = _fetch_premium_discount(fund["portfolio_id"], pd_url)
    if not data:
        raise HTTPException(status_code=502, detail="Could not retrieve premium/discount data from iShares")

    date_strs = [_ymd_int_to_str(d) for d in data["dates"]]
    vals = data["vals"]

    if raw:
        return [{"date": d, "premium_discount_pct": v} for d, v in zip(date_strs, vals) if v is not None]

    is_dark = (theme or "").lower() == "dark"
    paper_bg = "rgba(0,0,0,0)"
    plot_bg = "rgba(0,0,0,0)"
    font_color = "#e5e7eb" if is_dark else "#111827"
    grid_color = "rgba(255,255,255,0.08)" if is_dark else "rgba(0,0,0,0.08)"

    return {
        "data": [
            {
                "type": "scatter",
                "mode": "lines",
                "name": "Premium/Discount",
                "x": date_strs,
                "y": vals,
                "line": {"color": "#16a34a", "width": 1.5},
                "hovertemplate": "%{x}<br>%{y:.4f}%<extra></extra>",
            }
        ],
        "layout": {
            "xaxis": {
                "type": "date",
                "tickfont": {"color": font_color, "size": 11},
                "showgrid": False,
                "zeroline": False,
            },
            "yaxis": {
                "ticksuffix": "%",
                "tickformat": ".2f",
                "tickfont": {"color": font_color, "size": 11},
                "showgrid": True,
                "gridcolor": grid_color,
                "zeroline": True,
                "zerolinecolor": grid_color,
                "zerolinewidth": 1,
            },
            "legend": {
                "orientation": "h",
                "x": 0.5,
                "xanchor": "center",
                "y": 1.02,
                "yanchor": "bottom",
                "font": {"color": font_color, "size": 12},
                "bgcolor": "rgba(0,0,0,0)",
            },
            "margin": {"t": 40, "b": 50, "l": 70, "r": 20},
            "template": "plotly_dark" if is_dark else "plotly_white",
            "paper_bgcolor": paper_bg,
            "plot_bgcolor": plot_bg,
            "font": {"color": font_color, "family": "Inter, system-ui, sans-serif"},
            "hoverlabel": {"bgcolor": "#111827" if is_dark else "#ffffff"},
        },
        "config": {"responsive": True, "displayModeBar": False},
    }


@app.get("/fund_premium_discount")
def fund_premium_discount_qs(
    ticker: str = Query(...),
    theme: str = Query("light"),
    raw: bool = Query(False),
) -> Any:
    return fund_premium_discount(ticker, theme=theme, raw=raw)


@app.get("/fund_performance")
def fund_performance_qs(
    ticker: str = Query(...),
    tab: str = Query("average_annual"),
) -> dict:
    return fund_performance(ticker, tab=tab)


def opt_portfolios() -> list[dict]:
    df = _read_sql("SELECT DISTINCT portfolio FROM funds ORDER BY portfolio")
    return [{"label": "All Portfolios", "value": ""}] + [{"label": p, "value": p} for p in df["portfolio"].tolist()]


@app.get("/options/investment_styles")
def opt_investment_styles(portfolio: str | None = None) -> list[dict]:
    if portfolio:
        df = _read_sql(
            "SELECT DISTINCT investment_style FROM funds "
            "WHERE portfolio = ? AND investment_style IS NOT NULL "
            "AND investment_style != '' ORDER BY investment_style",
            (portfolio,),
        )
    else:
        df = _read_sql(
            "SELECT DISTINCT investment_style FROM funds "
            "WHERE investment_style IS NOT NULL AND investment_style != '' "
            "ORDER BY investment_style"
        )
    return [{"label": "All Investment Styles", "value": ""}] + [
        {"label": s, "value": s} for s in df["investment_style"].tolist()
    ]


def _fund_distinct(col: str, portfolio: str | None, all_label: str) -> list[dict]:
    where = [f"{col} IS NOT NULL", f"{col} != ''"]
    args: list = []
    if portfolio:
        where.append("portfolio = ?")
        args.append(portfolio)
    df = _read_sql(
        f"SELECT DISTINCT {col} FROM funds WHERE {' AND '.join(where)} ORDER BY {col}",
        tuple(args),
    )
    return [{"label": all_label, "value": ""}] + [{"label": v, "value": v} for v in df[col].tolist()]


@app.get("/options/fund_asset_classes")
def opt_fund_asset_classes(portfolio: str | None = None) -> list[dict]:
    return _fund_distinct("asset_class", portfolio, "All Asset Classes")


@app.get("/options/fund_sub_asset_classes")
def opt_fund_sub_asset_classes(portfolio: str | None = None) -> list[dict]:
    return _fund_distinct("sub_asset_class", portfolio, "All Sub Asset Classes")


@app.get("/options/fund_countries")
def opt_fund_countries(portfolio: str | None = None) -> list[dict]:
    return _fund_distinct("country", portfolio, "All Countries")


@app.get("/options/asset_classes")
def opt_asset_classes(
    portfolio: str | None = None,
    ticker: str | None = None,
) -> list[dict]:
    if ticker:
        df = _read_sql(
            """SELECT DISTINCT holding_type AS asset_class
               FROM holdings_lookthrough
               WHERE parent_portfolio_id = (SELECT portfolio_id FROM funds WHERE ticker=?)
                 AND holding_type IS NOT NULL AND holding_type != ''
               ORDER BY SUM(market_value_usd) DESC""",
            (ticker,),
        )
    else:
        where = ["asset_class IS NOT NULL"]
        params: list = []
        if portfolio:
            where.append("portfolio = ?")
            params.append(portfolio)
        df = _read_sql(
            f"""SELECT asset_class, COUNT(*) n FROM funds
                WHERE {" AND ".join(where)}
                GROUP BY asset_class ORDER BY n DESC""",
            tuple(params),
        )
    return [{"label": "All Asset Classes", "value": ""}] + [
        {"label": r["asset_class"], "value": r["asset_class"]} for _, r in df.iterrows()
    ]


@app.get("/options/holding_types")
def opt_holding_types() -> list[dict]:
    _KNOWN = {
        "Equity",
        "Fixed Income",
        "Alternative",
        "Money Market",
        "Commodity",
        "Real Estate",
        "Other",
    }
    df = _read_sql(
        """SELECT holding_type, SUM(market_value_usd) AS mv
           FROM holdings_lt_latest
           WHERE holding_type IS NOT NULL AND holding_type != ''
           GROUP BY holding_type
           ORDER BY mv DESC""",
        (),
    )
    rows = [
        {"label": r["holding_type"], "value": r["holding_type"]} for _, r in df.iterrows() if r["holding_type"] in _KNOWN
    ]
    return rows


@app.get("/options/sectors")
def opt_sectors(
    portfolio: str | None = None,
    asset_class: str | None = None,
    ticker: str | None = None,
    country: str | None = None,
) -> list[dict]:
    where = ["sector IS NOT NULL", "sector != ''"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    if asset_class:
        where.append("holding_type = ?")
        params.append(asset_class)
    if country:
        where.append("country = ?")
        params.append(country)
    if ticker:
        where.append("parent_portfolio_id = (SELECT portfolio_id FROM funds WHERE ticker = ?)")
        params.append(ticker)
    df = _read_sql(
        f"""SELECT sector, SUM(market_value_usd) AS mv
            FROM holdings_lookthrough
            WHERE {" AND ".join(where)}
            GROUP BY sector ORDER BY mv DESC""",
        tuple(params),
    )
    return [{"label": "All Sectors", "value": ""}] + [{"label": s, "value": s} for s in df["sector"].tolist()]


@app.get("/options/countries")
def opt_countries(
    portfolio: str | None = None,
    asset_class: str | None = None,
    sector: str | None = None,
    ticker: str | None = None,
) -> list[dict]:
    where = ["country IS NOT NULL", "country != ''"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    if asset_class:
        where.append("holding_type = ?")
        params.append(asset_class)
    if sector:
        where.append("sector = ?")
        params.append(sector)
    if ticker:
        where.append("parent_portfolio_id = (SELECT portfolio_id FROM funds WHERE ticker = ?)")
        params.append(ticker)
    df = _read_sql(
        f"""SELECT country, SUM(market_value_usd) AS mv
            FROM holdings_lookthrough
            WHERE {" AND ".join(where)}
            GROUP BY country ORDER BY mv DESC""",
        tuple(params),
    )
    return [{"label": "All Countries", "value": ""}] + [{"label": c, "value": c} for c in df["country"].tolist()]


@app.get("/options/funds")
def opt_funds(
    portfolio: str | None = None,
    asset_class: str | None = None,
    limit: int = Query(500, ge=1, le=2000),
) -> list[dict]:
    where = ["ticker IS NOT NULL"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    if asset_class:
        where.append("asset_class = ?")
        params.append(asset_class)
    df = _read_sql(
        f"""SELECT ticker, name, total_aum_usd
            FROM funds WHERE {" AND ".join(where)}
            ORDER BY total_aum_usd DESC NULLS LAST LIMIT ?""",
        tuple(params) + (limit,),
    )
    return [
        {
            "label": f"{r['ticker']} — {r['name']}",
            "value": r["ticker"],
            "extraInfo": {
                "description": r["name"],
                "rightOfDescription": (f"${r['total_aum_usd'] / 1e9:.1f}B" if r["total_aum_usd"] else ""),
            },
        }
        for _, r in df.iterrows()
    ]


@app.get("/options/holdings")
def opt_holdings(limit: int = Query(500, ge=1, le=2000)) -> list[dict]:
    df = _read_sql(
        """SELECT lt.leaf_holding_ticker AS ticker,
                  MIN(lt.leaf_holding_name) AS name,
                  SUM(lt.market_value_usd) AS mv
           FROM holdings_lt_latest lt
           WHERE lt.leaf_holding_ticker IS NOT NULL AND lt.leaf_holding_ticker != ''
             AND lt.holding_type = 'Equity'
           GROUP BY lt.leaf_holding_ticker
           ORDER BY mv DESC
           LIMIT ?""",
        (limit,),
    )
    return [{"label": f"{r['ticker']} — {r['name']}", "value": r["ticker"]} for _, r in df.iterrows()]


@app.get("/holding_funds")
def holding_funds_table(ticker: str = Query(...)) -> dict:
    res = holding_detail(ticker)
    return {"rows": res["funds"]}


@app.get("/top_securities/by_asset_class")
def top_securities_by_asset_class(
    asset_class: str = Query("Equity"),
    limit: int = Query(25, ge=1, le=100),
) -> dict:
    if asset_class == "Equity":
        id_expr = "COALESCE(NULLIF(lt.leaf_holding_ticker,''), lt.leaf_holding_isin, lt.leaf_holding_cusip, lt.leaf_holding_name)"
        extra = "AND lt.leaf_holding_ticker IS NOT NULL AND lt.leaf_holding_ticker != ''"
    else:
        id_expr = "COALESCE(NULLIF(lt.leaf_holding_isin,''), NULLIF(lt.leaf_holding_cusip,''), lt.leaf_holding_name)"
        extra = ""
    df = _read_sql(
        f"""SELECT {id_expr} AS ticker,
                  REPLACE(MAX(lt.leaf_holding_name), ' (fund-as-leaf)', '') AS name,
                  SUM(lt.market_value_usd) AS total_exposure_usd,
                  COUNT(DISTINCT lt.parent_portfolio_id) AS fund_count
           FROM holdings_lt_latest lt
           WHERE lt.holding_type = ? {extra}
           GROUP BY {id_expr}
           ORDER BY total_exposure_usd DESC
           LIMIT ?""",
        (asset_class, limit),
    )
    return {"rows": df.to_dict("records")}


@app.get("/top_securities/by_sector")
def top_securities_by_sector(
    sector: str = Query("Information Technology"),
    limit: int = Query(25, ge=1, le=100),
) -> dict:
    id_expr = (
        "CASE WHEN lt.holding_type = 'Equity' AND lt.leaf_holding_ticker IS NOT NULL AND lt.leaf_holding_ticker != '' "
        "THEN lt.leaf_holding_ticker "
        "ELSE COALESCE(NULLIF(lt.leaf_holding_isin,''), NULLIF(lt.leaf_holding_cusip,''), lt.leaf_holding_name) END"
    )
    df = _read_sql(
        f"""SELECT {id_expr} AS ticker,
                  REPLACE(MAX(lt.leaf_holding_name), ' (fund-as-leaf)', '') AS name,
                  SUM(lt.market_value_usd) AS total_exposure_usd,
                  COUNT(DISTINCT lt.parent_portfolio_id) AS fund_count
           FROM holdings_lt_latest lt
           WHERE lt.sector = ?
             AND lt.holding_type NOT IN ('Other Derivatives', 'Forwards', 'Swaps', 'FX', 'Cash', 'Cash Collateral and Margins')
           GROUP BY {id_expr}
           ORDER BY total_exposure_usd DESC
           LIMIT ?""",
        (sector, limit),
    )
    return {"rows": df.to_dict("records")}


@app.get("/top_securities/by_country")
def top_securities_by_country(
    country: str = Query("United States"),
    limit: int = Query(25, ge=1, le=100),
) -> dict:
    id_expr = (
        "CASE WHEN lt.holding_type = 'Equity' AND lt.leaf_holding_ticker IS NOT NULL AND lt.leaf_holding_ticker != '' "
        "THEN lt.leaf_holding_ticker "
        "ELSE COALESCE(NULLIF(lt.leaf_holding_isin,''), NULLIF(lt.leaf_holding_cusip,''), lt.leaf_holding_name) END"
    )
    df = _read_sql(
        f"""SELECT {id_expr} AS ticker,
                  REPLACE(MAX(lt.leaf_holding_name), ' (fund-as-leaf)', '') AS name,
                  SUM(lt.market_value_usd) AS total_exposure_usd,
                  COUNT(DISTINCT lt.parent_portfolio_id) AS fund_count
           FROM holdings_lt_latest lt
           WHERE lt.country = ?
             AND lt.holding_type NOT IN ('Other Derivatives', 'Forwards', 'Swaps', 'FX', 'Cash', 'Cash Collateral and Margins')
           GROUP BY {id_expr}
           ORDER BY total_exposure_usd DESC
           LIMIT ?""",
        (country, limit),
    )
    return {"rows": df.to_dict("records")}


_PALETTE = [
    "#003c7e",
    "#0066cc",
    "#5fa3d4",
    "#7ac8e3",
    "#c2dff0",
    "#cc7a00",
    "#e69500",
    "#f1b95e",
    "#9c8a3a",
    "#5e9c5e",
    "#0e7c66",
    "#4cb594",
    "#7d3c98",
    "#c2185b",
    "#7f8c8d",
]


_GARBAGE_LABEL_RE = re.compile(r"^[\d.,\-\s]+$")


def _clean_pie_rows(rows: list[dict], *, min_pct: float = 0.5, max_slices: int = 12) -> list[dict]:
    cleaned: list[dict] = []
    for r in rows:
        label = r.get("label") or "Unknown"
        v = r.get("market_value_usd") or 0
        if v <= 0:
            continue
        if _GARBAGE_LABEL_RE.match(str(label).strip()):
            label = "Other"
        cleaned.append({**r, "label": label})
    merged: dict[str, dict] = {}
    for r in cleaned:
        k = r["label"]
        if k in merged:
            merged[k]["market_value_usd"] += r["market_value_usd"]
        else:
            merged[k] = dict(r)
    out = sorted(merged.values(), key=lambda r: r["market_value_usd"], reverse=True)
    total = sum(r["market_value_usd"] for r in out) or 1.0
    big, small = [], []
    for r in out:
        ((big if r["market_value_usd"] / total * 100 >= min_pct else small).append(r))
    if len(big) > max_slices:
        small = big[max_slices:] + small
        big = big[:max_slices]
    if small:
        big.append(
            {
                "label": "Other",
                "market_value_usd": sum(r["market_value_usd"] for r in small),
            }
        )
    return big


def _pie_figure(
    labels: list[str],
    values: list[float],
    *,
    title: str,
    hole: float = 0.55,
    theme: str = "light",
) -> dict:
    is_dark = (theme or "").lower() == "dark"
    paper_bg = "rgba(0,0,0,0)" if is_dark else "#ffffff"
    plot_bg = "rgba(0,0,0,0)" if is_dark else "#ffffff"
    font_color = "#e5e7eb" if is_dark else "#111827"
    return {
        "data": [
            {
                "type": "pie",
                "labels": labels,
                "values": values,
                "hole": hole,
                "textinfo": "percent",
                "textposition": "inside",
                "insidetextorientation": "horizontal",
                "automargin": True,
                "marker": {
                    "colors": _PALETTE * (1 + len(labels) // len(_PALETTE)),
                    "line": {"color": "#ffffff", "width": 1},
                },
                "hovertemplate": "<b>%{label}</b><br>$%{value:,.0f}<br>%{percent}<extra></extra>",
                "sort": True,
            }
        ],
        "layout": {
            "title": {
                "text": title,
                "x": 0.5,
                "xanchor": "center",
                "font": {"color": font_color},
            },
            "showlegend": True,
            "legend": {
                "orientation": "v",
                "y": 0.5,
                "yanchor": "middle",
                "x": 1.02,
                "xanchor": "left",
                "font": {"size": 11, "color": font_color},
            },
            "margin": {"t": 60, "b": 30, "l": 30, "r": 180},
            "template": "plotly_dark" if is_dark else "plotly_white",
            "paper_bgcolor": paper_bg,
            "plot_bgcolor": plot_bg,
            "font": {"color": font_color},
        },
        "config": {"responsive": True, "displayModeBar": False},
    }


def _fmt_money_short(v: float) -> str:
    a = abs(v)
    if a >= 1e12:
        s = f"${v / 1e12:.2f}T"
    elif a >= 1e9:
        s = f"${v / 1e9:.2f}B"
    elif a >= 1e6:
        s = f"${v / 1e6:.1f}M"
    elif a >= 1e3:
        s = f"${v / 1e3:.0f}K"
    else:
        s = f"${v:.0f}"
    return s.replace(".00T", "T").replace(".00B", "B").replace(".0M", "M")


def _nice_money_ticks(vmax: float, n_target: int = 6) -> tuple[list[float], list[str]]:
    if vmax <= 0:
        return [0.0], ["$0"]
    raw = vmax / n_target
    mag = 10 ** math.floor(math.log10(raw))
    step = mag
    for f in (1, 2, 2.5, 5, 10):
        step = f * mag
        if vmax / step <= n_target:
            break
    ticks = []
    v = 0.0
    while v <= vmax * 1.0001:
        ticks.append(v)
        v += step
    return ticks, [_fmt_money_short(t) for t in ticks]


def _hbar_figure(
    labels: list[str],
    values: list[float],
    *,
    title: str,
    color: str = "#003c7e",
    theme: str = "light",
) -> dict:
    pairs = sorted(zip(labels, values), key=lambda p: p[1])
    labs = [p[0] for p in pairs]
    vals = [p[1] for p in pairs]
    vmax = max(vals) if vals else 0
    tickvals, ticktext = _nice_money_ticks(vmax)
    is_dark = (theme or "").lower() == "dark"
    paper_bg = "rgba(0,0,0,0)" if is_dark else "#ffffff"
    plot_bg = "rgba(0,0,0,0)" if is_dark else "#ffffff"
    font_color = "#e5e7eb" if is_dark else "#111827"
    return {
        "data": [
            {
                "type": "bar",
                "orientation": "h",
                "x": vals,
                "y": labs,
                "marker": {"color": color},
                "hovertemplate": "<b>%{y}</b><br>$%{x:,.0f}<extra></extra>",
            }
        ],
        "layout": {
            "title": {
                "text": title,
                "x": 0.5,
                "xanchor": "center",
                "font": {"color": font_color},
            },
            "xaxis": {
                "tickvals": tickvals,
                "ticktext": ticktext,
                "showgrid": True,
                "zeroline": True,
                "tickfont": {"color": font_color},
            },
            "yaxis": {
                "automargin": True,
                "tickfont": {"size": 11, "color": font_color},
            },
            "margin": {"t": 60, "b": 50, "l": 220, "r": 40},
            "template": "plotly_dark" if is_dark else "plotly_white",
            "paper_bgcolor": paper_bg,
            "plot_bgcolor": plot_bg,
            "font": {"color": font_color},
        },
        "config": {"responsive": True, "displayModeBar": False},
    }


_ALLOC_AXES = {
    "asset_class": ("holding_type", "Asset Class Allocation", "lt"),
    "sector": ("sector", "Sector Allocation", "lt"),
    "country": ("country", "Geographic Allocation", "lt"),
    "currency": ("currency", "Currency Exposure", "lt"),
    "fund_asset_class": ("asset_class", "Fund Asset Class Allocation", "fund"),
    "strategy": ("sub_asset_class", "Investment Strategy Allocation", "fund"),
    "investment_style": ("investment_style", "Active vs Index", "fund"),
    "market_type": ("market_type", "Developed vs Emerging Market", "fund"),
    "region": ("region", "Regional Allocation", "fund"),
    "product_view": ("product_view", "Product Type Allocation", "fund"),
}


@app.get("/chart/allocation/{axis}")
def chart_allocation(
    axis: str,
    portfolio: str | None = None,
    fund_ticker: str | None = None,
    raw: bool = Query(False),
    theme: str = Query("light"),
) -> Any:
    if axis not in _ALLOC_AXES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown allocation axis '{axis}'; choose one of {sorted(_ALLOC_AXES)}",
        )
    col, title, source = _ALLOC_AXES[axis]
    if source == "fund":
        data = _fund_breakdown(col, portfolio=portfolio)
    else:
        data = _breakdown(col, portfolio=portfolio, fund_ticker=fund_ticker)
    if raw:
        return data["rows"]
    if fund_ticker:
        title = f"{title} — {fund_ticker}"
    cleaned = _clean_pie_rows(data["rows"], min_pct=0.5, max_slices=12)
    labels = [r["label"] for r in cleaned]
    vals = [r["market_value_usd"] for r in cleaned]
    return _pie_figure(labels, vals, title=title, theme=theme)


@app.get("/chart/top_holdings_bar")
def chart_top_holdings_bar(
    portfolio: str | None = None,
    sector: str | None = None,
    asset_class: str | None = None,
    country: str | None = None,
    fund_ticker: str | None = None,
    limit: int = Query(20, ge=5, le=100),
    raw: bool = Query(False),
    theme: str = Query("light"),
) -> Any:
    where = ["1=1"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    if sector:
        where.append("sector = ?")
        params.append(sector)
    if asset_class:
        where.append("holding_type = ?")
        params.append(asset_class)
    if country:
        where.append("country = ?")
        params.append(country)
    if fund_ticker:
        where.append("parent_portfolio_id = (SELECT portfolio_id FROM funds WHERE ticker = ?)")
        params.append(fund_ticker)
    df = _read_sql(
        f"""SELECT leaf_holding_name, leaf_holding_ticker,
                   SUM(market_value_usd) AS market_value_usd
            FROM holdings_lookthrough
            WHERE {" AND ".join(where)}
            GROUP BY leaf_holding_isin, leaf_holding_name
            ORDER BY market_value_usd DESC LIMIT ?""",
        tuple(params) + (limit,),
    )
    if raw:
        return _frame_to_records(df)
    labels = [
        f"{r['leaf_holding_ticker']} — {r['leaf_holding_name'][:40]}"
        if r["leaf_holding_ticker"]
        else r["leaf_holding_name"][:50]
        for _, r in df.iterrows()
    ]
    vals = df["market_value_usd"].tolist()
    title = f"Top {limit} Holdings" + (f" — {fund_ticker}" if fund_ticker else "") + (f" — {sector}" if sector else "")
    return _hbar_figure(labels, vals, title=title, theme=theme)


@app.get("/chart/top_funds_bar")
def chart_top_funds_bar(
    portfolio: str | None = None,
    asset_class: str | None = None,
    limit: int = Query(15, ge=3, le=50),
    raw: bool = Query(False),
    theme: str = Query("light"),
) -> Any:
    res = top_funds(portfolio=portfolio, asset_class=asset_class, limit=limit)
    rows = res["rows"]
    if raw:
        return rows
    labels = [f"{r['ticker']} — {r['name'][:35]}" if r["ticker"] else r["name"][:50] for r in rows]
    vals = [r["external_aum_usd"] or 0 for r in rows]
    return _hbar_figure(labels, vals, title=f"Top {limit} Funds by Net Assets", theme=theme)


_RETURN_PERIODS = {
    "ytd": "nav_ytd_pct",
    "1y": "nav_1y_pct",
    "3y": "nav_3y_pct",
    "5y": "nav_5y_pct",
    "10y": "nav_10y_pct",
    "inception": "nav_inception_pct",
}


def _resolve_period(period: str) -> str:
    if period not in _RETURN_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown period '{period}'; choose one of {sorted(_RETURN_PERIODS)}",
        )
    return _RETURN_PERIODS[period]


@app.get("/returns/total")
def returns_total(portfolio: str | None = None) -> dict:
    where = ["external_aum_usd > 0"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    cols = ", ".join(
        f"SUM(external_aum_usd * {col}) / NULLIF(SUM(CASE WHEN {col} IS NOT NULL THEN external_aum_usd ELSE 0 END), 0) AS {col}"
        for col in _RETURN_PERIODS.values()
    )
    cov = ", ".join(
        f"SUM(CASE WHEN {col} IS NOT NULL THEN external_aum_usd ELSE 0 END) AS coverage_{col}"
        for col in _RETURN_PERIODS.values()
    )
    df = _read_sql(
        f"""SELECT SUM(external_aum_usd) AS total_aum_usd,
                   COUNT(*)              AS fund_count,
                   {cols},
                   {cov}
            FROM funds WHERE {" AND ".join(where)}""",
        tuple(params),
    )
    row = df.iloc[0].to_dict()
    return {
        "portfolio": portfolio or "all",
        "total_aum_usd": float(row.get("total_aum_usd") or 0),
        "fund_count": int(row.get("fund_count") or 0),
        "returns_pct": {k: row.get(col) for k, col in _RETURN_PERIODS.items()},
        "coverage_aum_usd": {k: float(row.get(f"coverage_{col}") or 0) for k, col in _RETURN_PERIODS.items()},
    }


@app.get("/returns/summary")
def returns_summary(portfolio: str | None = None) -> dict:
    rt = returns_total(portfolio=portfolio)
    label_map = [
        ("ytd", "YTD"),
        ("1y", "1 Year"),
        ("3y", "3 Year (annlz.)"),
        ("5y", "5 Year (annlz.)"),
        ("10y", "10 Year (annlz.)"),
        ("inception", "Since Inception"),
    ]
    rows = [
        {
            "period": label,
            "return_pct": rt["returns_pct"].get(key),
            "coverage_pct": (
                rt["coverage_aum_usd"].get(key, 0) / rt["total_aum_usd"] * 100 if rt["total_aum_usd"] else None
            ),
        }
        for key, label in label_map
    ]
    return {
        "rows": rows,
        "total_aum_usd": rt["total_aum_usd"],
        "fund_count": rt["fund_count"],
    }


@app.get("/returns/performers")
def returns_performers(
    portfolio: str | None = None,
    direction: str = Query("top", pattern="^(top|bottom)$"),
    limit: int = Query(5, ge=1, le=50),
) -> dict:
    where = ["nav_1y_pct IS NOT NULL", "external_aum_usd > 0"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    order = "DESC" if direction == "top" else "ASC"
    df = _read_sql(
        f"""SELECT ticker, name, asset_class, nav_1y_pct, external_aum_usd
            FROM funds
            WHERE {" AND ".join(where)}
            ORDER BY nav_1y_pct {order}
            LIMIT ?""",
        tuple(params) + (limit,),
    )
    rows = [
        {
            "rank": i + 1,
            "ticker": r.get("ticker") or "",
            "name": r.get("name") or "",
            "asset_class": r.get("asset_class") or "",
            "return_1y_pct": r.get("nav_1y_pct"),
            "aum_usd": r.get("external_aum_usd"),
        }
        for i, r in enumerate(_frame_to_records(df))
    ]
    return {"direction": direction, "rows": rows}


_RETURN_FUND_AXES = {
    "asset_class": "asset_class",
    "strategy": "sub_asset_class",
    "investment_style": "investment_style",
    "market_type": "market_type",
    "region": "region",
    "fund_country": "country",
    "product_view": "product_view",
}


_RETURN_SEC_AXES = {
    "sector": "sector",
    "country": "country",
    "holding_type": "holding_type",
    "currency": "currency",
}


@app.get("/returns/attribution")
def returns_attribution(
    dim: str = Query("asset_class"),
    period: str = Query("1y"),
    portfolio: str | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> dict:
    col = _resolve_period(period)
    fund_where = ["external_aum_usd > 0", f"{col} IS NOT NULL"]
    fund_params: list = []
    if portfolio:
        fund_where.append("portfolio = ?")
        fund_params.append(portfolio)

    if dim in _RETURN_FUND_AXES:
        axis_col = _RETURN_FUND_AXES[dim]
        df = _read_sql(
            f"""SELECT COALESCE({axis_col}, 'Unknown') AS label,
                       SUM(external_aum_usd) AS aum_usd,
                       SUM(external_aum_usd * {col}) / SUM(external_aum_usd) AS return_pct,
                       COUNT(*) AS fund_count
                FROM funds WHERE {" AND ".join(fund_where)}
                GROUP BY label
                ORDER BY aum_usd DESC""",
            tuple(fund_params),
        )
    elif dim in _RETURN_SEC_AXES:
        axis_col = _RETURN_SEC_AXES[dim]
        lt_where = ["1=1"]
        lt_params: list = []
        if portfolio:
            lt_where.append("lt.portfolio = ?")
            lt_params.append(portfolio)
        df = _read_sql(
            f"""SELECT COALESCE(lt.{axis_col}, 'Unknown') AS label,
                       SUM(lt.market_value_usd) AS aum_usd,
                       SUM(lt.market_value_usd * f.{col}) / SUM(CASE WHEN f.{col} IS NOT NULL THEN lt.market_value_usd ELSE 0 END) AS return_pct,
                       COUNT(DISTINCT lt.parent_portfolio_id) AS fund_count
                FROM holdings_lt_latest lt
                JOIN funds f ON f.portfolio_id = lt.parent_portfolio_id
                WHERE {" AND ".join(lt_where)} AND f.{col} IS NOT NULL
                GROUP BY label
                ORDER BY aum_usd DESC""",
            tuple(lt_params),
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"unknown dim '{dim}'; choose from {sorted(_RETURN_FUND_AXES) + sorted(_RETURN_SEC_AXES)}",
        )

    total_aum = float(df["aum_usd"].sum())
    df["weight_pct"] = df["aum_usd"] / total_aum * 100 if total_aum else 0
    df["contribution_pct"] = df["weight_pct"] * df["return_pct"] / 100
    df = df.head(limit)
    return {
        "dim": dim,
        "period": period,
        "portfolio_total_aum_usd": total_aum,
        "portfolio_total_return_pct": float(df["contribution_pct"].sum()),
        "rows": _frame_to_records(df),
    }


@app.get("/chart/returns_attribution")
def chart_returns_attribution(
    dim: str = Query("asset_class"),
    period: str = Query("1y"),
    portfolio: str | None = None,
    raw: bool = Query(False),
    limit: int = Query(20, ge=3, le=100),
    theme: str = Query("light"),
) -> Any:
    res = returns_attribution(dim=dim, period=period, portfolio=portfolio, limit=limit)
    rows = res["rows"]
    if raw:
        return res
    pairs = sorted([(r["label"], r["contribution_pct"]) for r in rows], key=lambda p: p[1])
    labels = [p[0] for p in pairs]
    contribs = [p[1] for p in pairs]
    colors = ["#0e7c66" if v >= 0 else "#c0392b" for v in contribs]
    title = f"{dim.replace('_', ' ').title()} Return Contribution — {period.upper()}"
    is_dark = (theme or "").lower() == "dark"
    font_color = "#e5e7eb" if is_dark else "#111827"
    return {
        "data": [
            {
                "type": "bar",
                "orientation": "h",
                "x": contribs,
                "y": labels,
                "marker": {"color": colors},
                "hovertemplate": "%{y}<br>%{x:.2f}%<extra></extra>",
            }
        ],
        "layout": {
            "title": {
                "text": title,
                "x": 0.5,
                "xanchor": "center",
                "font": {"color": font_color},
            },
            "xaxis": {
                "title": "Contribution (%)",
                "ticksuffix": "%",
                "tickfont": {"color": font_color},
            },
            "yaxis": {"automargin": True, "tickfont": {"color": font_color}},
            "margin": {"t": 60, "b": 50, "l": 200, "r": 30},
            "template": "plotly_dark" if is_dark else "plotly_white",
            "paper_bgcolor": "rgba(0,0,0,0)" if is_dark else "#ffffff",
            "plot_bgcolor": "rgba(0,0,0,0)" if is_dark else "#ffffff",
            "font": {"color": font_color},
        },
        "config": {"responsive": True, "displayModeBar": False},
    }


_YIELD_BASES = {
    "twelve_month": "twelve_month_yield_pct",
    "sec_30d": "sec_yield_30d_pct",
    "distribution": "distribution_yield_pct",
    "unsubsidized": "unsubsidized_yield_pct",
}


def _resolve_yield_basis(basis: str) -> str:
    if basis not in _YIELD_BASES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown yield_basis '{basis}'; choose one of {sorted(_YIELD_BASES)}",
        )
    return _YIELD_BASES[basis]


@app.get("/income/total")
def income_total(portfolio: str | None = None) -> dict:
    where = ["external_aum_usd > 0"]
    params: list = []
    if portfolio:
        where.append("portfolio = ?")
        params.append(portfolio)
    parts = []
    for key, col in _YIELD_BASES.items():
        parts.append(
            f"SUM(CASE WHEN {col} IS NOT NULL THEN external_aum_usd * {col} / 100 ELSE 0 END) AS annual_income_{col}"
        )
        parts.append(f"SUM(CASE WHEN {col} IS NOT NULL THEN external_aum_usd ELSE 0 END) AS coverage_{col}")
    df = _read_sql(
        f"""SELECT SUM(external_aum_usd) AS total_aum_usd,
                   COUNT(*)              AS fund_count,
                   {", ".join(parts)}
            FROM funds WHERE {" AND ".join(where)}""",
        tuple(params),
    )
    row = df.iloc[0].to_dict()
    total_aum = float(row.get("total_aum_usd") or 0)
    out = {
        "portfolio": portfolio or "all",
        "total_aum_usd": total_aum,
        "fund_count": int(row.get("fund_count") or 0),
        "by_yield_basis": {},
    }
    for key, col in _YIELD_BASES.items():
        income = float(row.get(f"annual_income_{col}") or 0)
        cov = float(row.get(f"coverage_{col}") or 0)
        out["by_yield_basis"][key] = {
            "annual_income_usd": income,
            "coverage_aum_usd": cov,
            "coverage_pct_of_total": (cov / total_aum * 100) if total_aum else 0,
            "weighted_yield_pct": (income / cov * 100) if cov else None,
        }
    return out


@app.get("/income/attribution")
def income_attribution(
    dim: str = Query("asset_class"),
    yield_basis: str = Query("twelve_month"),
    portfolio: str | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> dict:
    col = _resolve_yield_basis(yield_basis)
    fund_where = ["external_aum_usd > 0", f"{col} IS NOT NULL"]
    fund_params: list = []
    if portfolio:
        fund_where.append("portfolio = ?")
        fund_params.append(portfolio)

    if dim in _RETURN_FUND_AXES:
        axis_col = _RETURN_FUND_AXES[dim]
        df = _read_sql(
            f"""SELECT COALESCE({axis_col}, 'Unknown') AS label,
                       SUM(external_aum_usd) AS aum_usd,
                       SUM(external_aum_usd * {col}) / SUM(external_aum_usd) AS yield_pct,
                       SUM(external_aum_usd * {col}) / 100 AS annual_income_usd,
                       COUNT(*) AS fund_count
                FROM funds WHERE {" AND ".join(fund_where)}
                GROUP BY label
                ORDER BY annual_income_usd DESC""",
            tuple(fund_params),
        )
    elif dim in _RETURN_SEC_AXES:
        axis_col = _RETURN_SEC_AXES[dim]
        lt_where = [f"f.{col} IS NOT NULL"]
        lt_params: list = []
        if portfolio:
            lt_where.append("lt.portfolio = ?")
            lt_params.append(portfolio)
        df = _read_sql(
            f"""SELECT COALESCE(lt.{axis_col}, 'Unknown') AS label,
                       SUM(lt.market_value_usd) AS aum_usd,
                       SUM(lt.market_value_usd * f.{col}) / SUM(lt.market_value_usd) AS yield_pct,
                       SUM(lt.market_value_usd * f.{col}) / 100 AS annual_income_usd,
                       COUNT(DISTINCT lt.parent_portfolio_id) AS fund_count
                FROM holdings_lt_latest lt
                JOIN funds f ON f.portfolio_id = lt.parent_portfolio_id
                WHERE {" AND ".join(lt_where)}
                GROUP BY label
                ORDER BY annual_income_usd DESC""",
            tuple(lt_params),
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"unknown dim '{dim}'; choose from {sorted(_RETURN_FUND_AXES) + sorted(_RETURN_SEC_AXES)}",
        )

    total_aum = float(df["aum_usd"].sum())
    total_income = float(df["annual_income_usd"].sum())
    df["weight_pct"] = df["aum_usd"] / total_aum * 100 if total_aum else 0
    df["income_share_pct"] = df["annual_income_usd"] / total_income * 100 if total_income else 0
    df = df.head(limit)
    return {
        "dim": dim,
        "yield_basis": yield_basis,
        "total_aum_usd": total_aum,
        "total_annual_income_usd": total_income,
        "portfolio_weighted_yield_pct": ((total_income / total_aum * 100) if total_aum else None),
        "rows": _frame_to_records(df),
    }


@app.get("/fund/{ident}/income")
def fund_income(ident: str) -> dict:
    f = dict(_resolve_fund(ident))
    aum = f.get("total_aum_usd") or 0.0

    def annual(v: float | None) -> float | None:
        if v is None or aum <= 0:
            return None
        return aum * v / 100

    yields = {
        "twelve_month": {
            "yield_pct": f.get("twelve_month_yield_pct"),
            "annual_income_usd": annual(f.get("twelve_month_yield_pct")),
        },
        "sec_30d": {
            "yield_pct": f.get("sec_yield_30d_pct"),
            "annual_income_usd": annual(f.get("sec_yield_30d_pct")),
        },
        "distribution": {
            "yield_pct": f.get("distribution_yield_pct"),
            "annual_income_usd": annual(f.get("distribution_yield_pct")),
        },
        "unsubsidized": {
            "yield_pct": f.get("unsubsidized_yield_pct"),
            "annual_income_usd": annual(f.get("unsubsidized_yield_pct")),
        },
    }
    return {
        "fund": {
            "portfolio_id": f["portfolio_id"],
            "ticker": f.get("ticker"),
            "name": f.get("name"),
            "asset_class": f.get("asset_class"),
            "sub_asset_class": f.get("sub_asset_class"),
        },
        "total_aum_usd": aum,
        "by_yield_basis": yields,
    }


_DIST_LOOKBACKS = {
    "1y": 365,
    "3y": 365 * 3,
    "5y": 365 * 5,
    "10y": 365 * 10,
    "ytd": None,
    "all": -1,
}


def _dist_window_clause(period: str) -> tuple[str, tuple]:
    if period == "ytd":
        return ("date(d.ex_date) >= date('now', 'start of year')", ())
    if period == "all":
        return ("1=1", ())
    days = _DIST_LOOKBACKS.get(period)
    if days is None:
        raise HTTPException(
            status_code=400,
            detail=f"unknown period '{period}'; choose from {sorted(_DIST_LOOKBACKS)}",
        )
    return ("date(d.ex_date) >= date('now', ?)", (f"-{days} days",))


def _scaled_distributions_subquery(period: str) -> tuple[str, tuple]:
    cond, params = _dist_window_clause(period)
    sub = f"""
        SELECT d.portfolio_id,
               SUM(d.total_distribution) AS dist_per_share_period,
               SUM(d.income) AS income_per_share_period,
               SUM(d.st_cap_gains) AS st_cg_period,
               SUM(d.lt_cap_gains) AS lt_cg_period,
               SUM(d.return_of_capital) AS roc_period,
               COUNT(*) AS distribution_count,
               MAX(d.ex_date) AS most_recent_ex_date
        FROM distributions d
        WHERE {cond}
        GROUP BY d.portfolio_id
    """
    return sub, params


@app.get("/income/distributions/total")
def distributions_total(
    period: str = Query("1y"),
    portfolio: str | None = None,
) -> dict:
    sub, sub_params = _scaled_distributions_subquery(period)
    where = ["1=1"]
    params: list = list(sub_params)
    if portfolio:
        where.append("f.portfolio = ?")
        params.append(portfolio)

    df = _read_sql(
        f"""SELECT SUM(d.dist_per_share_period * latest_shares.shares) AS total_dist_usd,
                   SUM(d.income_per_share_period * latest_shares.shares) AS total_income_usd,
                   SUM(d.st_cg_period * latest_shares.shares) AS total_st_cg_usd,
                   SUM(d.lt_cg_period * latest_shares.shares) AS total_lt_cg_usd,
                   SUM(d.roc_period * latest_shares.shares) AS total_roc_usd,
                   COUNT(DISTINCT d.portfolio_id) AS funds_with_distributions,
                   SUM(f.external_aum_usd) AS coverage_aum_usd,
                   MAX(d.most_recent_ex_date) AS latest_ex_date
            FROM funds f
            JOIN ({sub}) d ON d.portfolio_id = f.portfolio_id
            JOIN (
              SELECT portfolio_id, shares_outstanding AS shares
              FROM nav_history nh
              WHERE shares_outstanding IS NOT NULL
                AND as_of_date = (
                    SELECT MAX(as_of_date) FROM nav_history nh2
                    WHERE nh2.portfolio_id = nh.portfolio_id
                      AND nh2.shares_outstanding IS NOT NULL
                )
            ) latest_shares ON latest_shares.portfolio_id = f.portfolio_id
            WHERE {" AND ".join(where)}""",
        tuple(params),
    )
    row = df.iloc[0].to_dict()
    return {
        "period": period,
        "portfolio": portfolio or "all",
        "total_distribution_usd": float(row.get("total_dist_usd") or 0),
        "income_usd": float(row.get("total_income_usd") or 0),
        "st_cap_gains_usd": float(row.get("total_st_cg_usd") or 0),
        "lt_cap_gains_usd": float(row.get("total_lt_cg_usd") or 0),
        "return_of_capital_usd": float(row.get("total_roc_usd") or 0),
        "funds_with_distributions": int(row.get("funds_with_distributions") or 0),
        "coverage_aum_usd": float(row.get("coverage_aum_usd") or 0),
        "latest_ex_date": row.get("latest_ex_date"),
    }


@app.get("/income/distributions/attribution")
def distributions_attribution(
    dim: str = Query("asset_class"),
    period: str = Query("1y"),
    portfolio: str | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> dict:
    if dim not in _RETURN_FUND_AXES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown dim '{dim}'; choose from {sorted(_RETURN_FUND_AXES)}",
        )
    axis_col = _RETURN_FUND_AXES[dim]
    sub, sub_params = _scaled_distributions_subquery(period)
    where = ["1=1"]
    params: list = list(sub_params)
    if portfolio:
        where.append("f.portfolio = ?")
        params.append(portfolio)

    df = _read_sql(
        f"""SELECT COALESCE(f.{axis_col}, 'Unknown') AS label,
                   SUM(f.external_aum_usd) AS aum_usd,
                   SUM(d.dist_per_share_period * ls.shares) AS distributions_usd,
                   SUM(d.income_per_share_period * ls.shares) AS income_usd,
                   COUNT(DISTINCT f.portfolio_id) AS fund_count
            FROM funds f
            JOIN ({sub}) d ON d.portfolio_id = f.portfolio_id
            JOIN (
              SELECT portfolio_id, shares_outstanding AS shares
              FROM nav_history nh
              WHERE shares_outstanding IS NOT NULL
                AND as_of_date = (
                    SELECT MAX(as_of_date) FROM nav_history nh2
                    WHERE nh2.portfolio_id = nh.portfolio_id
                      AND nh2.shares_outstanding IS NOT NULL
                )
            ) ls ON ls.portfolio_id = f.portfolio_id
            WHERE {" AND ".join(where)}
            GROUP BY label
            ORDER BY distributions_usd DESC""",
        tuple(params),
    )
    total_aum = float(df["aum_usd"].sum())
    total_dist = float(df["distributions_usd"].sum())
    df["weight_pct"] = df["aum_usd"] / total_aum * 100 if total_aum else 0
    df["distribution_share_pct"] = df["distributions_usd"] / total_dist * 100 if total_dist else 0
    df["realized_yield_pct"] = (df["distributions_usd"] / df["aum_usd"] * 100).fillna(0)
    df = df.head(limit)
    return {
        "dim": dim,
        "period": period,
        "total_aum_usd": total_aum,
        "total_distributions_usd": total_dist,
        "rows": _frame_to_records(df),
    }


_CORR_PERIODS = {
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "3y": 365 * 3,
    "5y": 365 * 5,
    "10y": 365 * 10,
}


def _correlation_frame(
    period_days: int,
    *,
    portfolio: str | None,
    top: int,
    investment_styles: list[str] | None = None,
    asset_classes: list[str] | None = None,
    sub_asset_classes: list[str] | None = None,
    countries: list[str] | None = None,
    sectors: list[str] | None = None,
) -> tuple[pd.DataFrame, list[str], list[float]]:
    where = ["f.external_aum_usd > 0"]
    params: list = []
    if portfolio:
        where.append("f.portfolio = ?")
        params.append(portfolio)
    if investment_styles:
        ph = ",".join(["?"] * len(investment_styles))
        where.append(f"f.investment_style IN ({ph})")
        params.extend(investment_styles)
    if asset_classes:
        ph = ",".join(["?"] * len(asset_classes))
        where.append(f"f.asset_class IN ({ph})")
        params.extend(asset_classes)
    if sub_asset_classes:
        ph = ",".join(["?"] * len(sub_asset_classes))
        where.append(f"f.sub_asset_class IN ({ph})")
        params.extend(sub_asset_classes)

    # Holdings-level slice (sectors / countries) — when active, rank funds by
    # actual USD exposure to that slice rather than by total external AUM, so
    # the matrix shows the funds with the heaviest exposure to e.g. "Energy".
    exposure_where: list[str] = []
    exposure_params: list = []
    if countries:
        ph = ",".join(["?"] * len(countries))
        exposure_where.append(f"country IN ({ph})")
        exposure_params.extend(countries)
    if sectors:
        ph = ",".join(["?"] * len(sectors))
        exposure_where.append(f"sector IN ({ph})")
        exposure_params.extend(sectors)

    if exposure_where:
        exp_sql = (
            "WITH exposure AS ("
            "  SELECT parent_portfolio_id, "
            "         SUM(market_value_usd) AS exposure_usd, "
            "         SUM(weight_pct)       AS exposure_weight "
            f"  FROM holdings_lookthrough WHERE {' AND '.join(exposure_where)} "
            "  GROUP BY parent_portfolio_id"
            ") "
            "SELECT f.portfolio_id, f.ticker, f.name, f.external_aum_usd, "
            "       f.investment_style, e.exposure_usd, e.exposure_weight "
            "FROM funds f JOIN exposure e ON e.parent_portfolio_id = f.portfolio_id "
            f"WHERE {' AND '.join(where)} AND e.exposure_usd > 0 "
            "ORDER BY e.exposure_weight DESC LIMIT ?"
        )
        funds_df = _read_sql(exp_sql, tuple(exposure_params) + tuple(params) + (top,))
        weight_col = "exposure_usd"
    else:
        funds_df = _read_sql(
            f"""SELECT portfolio_id, ticker, name, external_aum_usd, investment_style,
                       external_aum_usd AS exposure_usd
                FROM funds f
                WHERE {" AND ".join(where)}
                ORDER BY external_aum_usd DESC LIMIT ?""",
            tuple(params) + (top,),
        )
        weight_col = "external_aum_usd"
    if funds_df.empty:
        raise HTTPException(404, "no funds found")
    pids = tuple(funds_df["portfolio_id"].tolist())
    placeholders = ",".join(["?"] * len(pids))
    nav_df = _read_sql(
        f"""SELECT portfolio_id, as_of_date, daily_return_pct
            FROM nav_history
            WHERE portfolio_id IN ({placeholders})
              AND daily_return_pct IS NOT NULL
              AND date(as_of_date) >= date('now', ?)""",
        pids + (f"-{period_days} days",),
    )
    if nav_df.empty:
        raise HTTPException(
            404,
            "no NAV history rows yet — run `python -m openbb_blackrock.ingest --nav-history` first",
        )
    pivot = nav_df.pivot(index="as_of_date", columns="portfolio_id", values="daily_return_pct")
    min_valid = max(10, int(len(pivot) * 0.60))
    pivot = pivot.dropna(thresh=min_valid, axis=1)
    pid_to_label = {
        r["portfolio_id"]: (r["ticker"] or r["name"])
        + (f" [{r['investment_style']}]" if investment_styles and r["investment_style"] else "")
        for _, r in funds_df.iterrows()
    }
    pid_to_weight = {r["portfolio_id"]: float(r[weight_col]) for _, r in funds_df.iterrows()}
    keep_pids = [pid for pid in funds_df["portfolio_id"].tolist() if pid in pivot.columns and pid in pid_to_label]
    pivot = pivot[keep_pids]
    labels = [pid_to_label[pid] for pid in keep_pids]
    pivot.columns = labels
    weights = [pid_to_weight[pid] for pid in keep_pids]
    return pivot, labels, weights


@app.get("/correlations/matrix")
def correlations_matrix(
    period: str = Query("1y"),
    portfolio: str | None = None,
    top: int = Query(25, ge=2, le=100),
    investment_style: str | None = Query(None),
    asset_class: str | None = Query(None),
    sub_asset_class: str | None = Query(None),
    country: str | None = Query(None),
    sector: str | None = Query(None),
) -> dict:
    if period not in _CORR_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown period '{period}'; choose from {sorted(_CORR_PERIODS)}",
        )

    def _split(v: str | None) -> list[str] | None:
        if not v or not isinstance(v, str):
            return None
        s = v.strip()
        if s.startswith("[") and s.endswith("]"):
            s = s[1:-1]
        out = [t.strip().strip('"').strip("'") for t in s.split(",")]
        out = [t for t in out if t]
        return out or None

    pivot, labels, weights = _correlation_frame(
        _CORR_PERIODS[period],
        portfolio=portfolio,
        top=top,
        investment_styles=_split(investment_style),
        asset_classes=_split(asset_class),
        sub_asset_classes=_split(sub_asset_class),
        countries=_split(country),
        sectors=_split(sector),
    )
    corr = pivot.corr(min_periods=20).round(4)
    matrix = corr.where(pd.notna(corr), None).values.tolist()
    return {
        "period": period,
        "labels": labels,
        "n_observations": int(pivot.shape[0]),
        "weights_usd": weights,
        "matrix": matrix,
    }


def _lower_triangle_heatmap(
    matrix: list[list[float | None]],
    labels: list[str],
    *,
    title: str,
    decimals: int = 4,
    theme: str = "light",
) -> dict:
    n = len(labels)
    z: list[list[float | None]] = []
    text: list[list[str]] = []
    for i in range(n):
        z_row: list[float | None] = []
        t_row: list[str] = []
        for j in range(n):
            if j > i:
                z_row.append(None)
                t_row.append("")
                continue
            v = matrix[i][j] if matrix and i < len(matrix) and j < len(matrix[i]) else None
            z_row.append(v)
            t_row.append(f"{v:.{decimals}f}" if isinstance(v, (int, float)) else "")
        z.append(z_row)
        text.append(t_row)

    is_dark = (theme or "").lower() == "dark"
    paper_bg = "rgba(0,0,0,0)" if is_dark else "#ffffff"
    plot_bg = "rgba(0,0,0,0)" if is_dark else "#ffffff"
    font_color = "#e5e7eb" if is_dark else "#111827"

    return {
        "data": [
            {
                "type": "heatmap",
                "z": z,
                "x": labels,
                "y": labels,
                "text": text,
                "texttemplate": "%{text}",
                "textfont": {"size": 9, "color": "#ffffff"},
                "colorscale": [
                    [0.0, "#b22222"],
                    [0.25, "#c0392b"],
                    [0.5, "#6b7280"],
                    [0.75, "#1a6aad"],
                    [1.0, "#003c7e"],
                ],
                "reversescale": False,
                "zmid": 0,
                "zmin": -1,
                "zmax": 1,
                "xgap": 1,
                "ygap": 1,
                "hovertemplate": "<b>%{y}</b> vs <b>%{x}</b><br>r = %{z:.4f}<extra></extra>",
                "colorbar": {
                    "title": {"text": ""},
                    "tickfont": {"color": font_color},
                    "thickness": 14,
                    "outlinewidth": 0,
                },
            }
        ],
        "layout": {
            "title": {
                "text": title,
                "x": 0.5,
                "xanchor": "center",
                "font": {"color": font_color},
            },
            "xaxis": {
                "automargin": True,
                "tickangle": -45,
                "tickfont": {"size": 10, "color": font_color},
                "side": "bottom",
                "showgrid": False,
                "zeroline": False,
            },
            "yaxis": {
                "automargin": True,
                "autorange": "reversed",
                "tickfont": {"size": 10, "color": font_color},
                "showgrid": False,
                "zeroline": False,
            },
            "margin": {"t": 60, "b": 100, "l": 130, "r": 30},
            "template": "plotly_dark" if is_dark else "plotly_white",
            "paper_bgcolor": paper_bg,
            "plot_bgcolor": plot_bg,
            "font": {"color": font_color},
        },
        "config": {"displayModeBar": False},
    }


@app.get("/chart/correlation_heatmap")
def chart_correlation_heatmap(
    period: str = Query("1y"),
    portfolio: str | None = None,
    top: int = Query(25, ge=2, le=100),
    investment_style: str | None = Query(None),
    asset_class: str | None = Query(None),
    sub_asset_class: str | None = Query(None),
    country: str | None = Query(None),
    sector: str | None = Query(None),
    theme: str = Query("light"),
    raw: bool = Query(False),
) -> Any:
    res = correlations_matrix(
        period=period,
        portfolio=portfolio,
        top=top,
        investment_style=investment_style,
        asset_class=asset_class,
        sub_asset_class=sub_asset_class,
        country=country,
        sector=sector,
    )
    if raw:
        rows: list[dict] = []
        labels = res["labels"]
        matrix = res["matrix"]
        for i, fa in enumerate(labels):
            for j, fb in enumerate(labels):
                if j > i:
                    continue
                v = matrix[i][j] if matrix and i < len(matrix) and j < len(matrix[i]) else None
                rows.append(
                    {
                        "period": res["period"],
                        "n_observations": res["n_observations"],
                        "fund_a": fa,
                        "fund_b": fb,
                        "correlation": v,
                    }
                )
        return rows
    bits = [v for v in (investment_style, asset_class, sub_asset_class, country, sector) if v and isinstance(v, str)]
    suffix = (" — " + " · ".join(bits)) if bits else ""
    return _lower_triangle_heatmap(
        res["matrix"],
        res["labels"],
        title=f"Daily-return Correlation — Top {len(res['labels'])} Funds ({period.upper()}){suffix}",
        theme=theme,
    )


def _dominant(filter_sql: str, params: tuple, col: str) -> str | None:
    df = _read_sql(
        f"""SELECT {col} AS v, SUM(lt.market_value_usd) AS mv
            FROM holdings_lt_latest lt
            WHERE ({filter_sql}) AND {col} IS NOT NULL AND {col} != ''
            GROUP BY {col}
            ORDER BY mv DESC LIMIT 1""",
        params,
    )
    if df.empty:
        return None
    return df.iloc[0]["v"]


def _resolve_holding(ident: str) -> dict:
    qq = ident.upper()
    where = "UPPER(lt.leaf_holding_ticker)=? OR UPPER(lt.leaf_holding_isin)=? OR UPPER(lt.leaf_holding_cusip)=? OR UPPER(lt.leaf_holding_name)=?"
    params: tuple = (qq, qq, qq, qq)
    df = _read_sql(
        f"""SELECT
              MAX(lt.leaf_holding_name)   AS holding_name,
              MAX(lt.leaf_holding_ticker) AS holding_ticker,
              MAX(lt.leaf_holding_isin)   AS holding_isin,
              MAX(lt.leaf_holding_cusip)  AS holding_cusip,
              SUM(lt.market_value_usd)    AS total_exposure_usd,
              COUNT(DISTINCT lt.parent_portfolio_id) AS fund_count
            FROM holdings_lt_latest lt
            WHERE {where}""",
        params,
    )
    row = df.iloc[0].to_dict() if not df.empty else {}
    if not row.get("holding_name"):
        like_where = "lt.leaf_holding_name LIKE ?"
        like_params = (f"%{ident}%",)
        df = _read_sql(
            f"""SELECT
                  MAX(lt.leaf_holding_name)   AS holding_name,
                  MAX(lt.leaf_holding_ticker) AS holding_ticker,
                  MAX(lt.leaf_holding_isin)   AS holding_isin,
                  MAX(lt.leaf_holding_cusip)  AS holding_cusip,
                  SUM(lt.market_value_usd)    AS total_exposure_usd,
                  COUNT(DISTINCT lt.parent_portfolio_id) AS fund_count
                FROM holdings_lt_latest lt
                WHERE {like_where}""",
            like_params,
        )
        row = df.iloc[0].to_dict() if not df.empty else {}
        where, params = like_where, like_params
    if not row.get("holding_name"):
        raise HTTPException(404, f"holding '{ident}' not found")
    for col in ("holding_type", "sector", "country", "currency"):
        row[col] = _dominant(where, params, col)
    return row


@app.get("/holding/{ident}")
def holding_detail(ident: str) -> dict:
    h = _resolve_holding(ident)
    funds_df = _read_sql(
        """SELECT f.ticker AS fund_ticker,
                  f.name AS fund_name,
                  f.asset_class,
                  f.sub_asset_class,
                  lt.weight_pct AS weight_in_fund_pct,
                  lt.market_value_usd AS exposure_usd,
                  f.nav_ytd_pct, f.nav_1y_pct, f.nav_3y_pct, f.nav_5y_pct,
                  f.nav_10y_pct, f.nav_inception_pct,
                  f.twelve_month_yield_pct, f.sec_yield_30d_pct
           FROM holdings_lt_latest lt
           JOIN funds f ON f.portfolio_id = lt.parent_portfolio_id
           WHERE UPPER(lt.leaf_holding_ticker)=?
              OR UPPER(lt.leaf_holding_isin)=?
              OR UPPER(lt.leaf_holding_cusip)=?
              OR lt.leaf_holding_name=?
           GROUP BY f.ticker
           ORDER BY MAX(lt.market_value_usd) DESC""",
        (
            (h.get("holding_ticker") or "").upper(),
            (h.get("holding_isin") or "").upper(),
            (h.get("holding_cusip") or "").upper(),
            h.get("holding_name"),
        ),
    )
    universe_total = _scalar("SELECT SUM(market_value_usd) FROM holdings_lookthrough") or 0
    portfolio_weight_pct = h["total_exposure_usd"] / universe_total * 100 if universe_total else None
    return {
        "holding": h,
        "portfolio_weight_pct": portfolio_weight_pct,
        "funds": _frame_to_records(funds_df),
    }


@app.get("/holding_detail")
def holding_detail_qs(ticker: str = Query(...)) -> dict:
    return holding_detail(ticker)


@app.get("/metrics/holding")
def metrics_holding(ticker: str = Query(...)) -> list[dict]:
    res = holding_detail(ticker)
    h = res["holding"]
    exp = h.get("total_exposure_usd") or 0

    def fmt_usd(v: float) -> str:
        if v is None:
            return "—"
        for unit, scale in (("T", 1e12), ("B", 1e9), ("M", 1e6)):
            if abs(v) >= scale:
                return f"${v / scale:,.2f}{unit}"
        return f"${v:,.0f}"

    return [
        {
            "label": h.get("holding_ticker") or h.get("holding_isin") or h.get("holding_name") or ticker,
            "value": fmt_usd(exp),
            "delta": "",
        },
        {
            "label": "Brand Weight",
            "value": f"{res['portfolio_weight_pct']:.3f}%" if res.get("portfolio_weight_pct") is not None else "—",
            "delta": "",
        },
        {"label": "Funds Holding", "value": str(len(res["funds"])), "delta": ""},
        {"label": "Asset Class", "value": h.get("holding_type") or "—", "delta": ""},
        {"label": "Sector", "value": h.get("sector") or "—", "delta": ""},
    ]


def _fmt_aum(v: float | None) -> str:
    if v is None:
        return "—"
    if abs(v) >= 1e12:
        return f"${v / 1e12:,.2f}T"
    if abs(v) >= 1e9:
        return f"${v / 1e9:,.1f}B"
    return f"${v:,.0f}"


def _fmt_pct(v: float | None) -> str:
    return f"{v:+.2f}%" if isinstance(v, (int, float)) else "—"


def _fmt_yield(v: float | None) -> str:
    return f"{v:.2f}%" if isinstance(v, (int, float)) else "—"


@app.get("/metrics/overview")
def metrics_overview() -> list[dict]:
    o = overview()
    inc = income_total()
    income = inc["by_yield_basis"]["twelve_month"]["annual_income_usd"]
    yield_12m = inc["by_yield_basis"]["twelve_month"]["weighted_yield_pct"]
    sec_30d = inc["by_yield_basis"]["sec_30d"]["weighted_yield_pct"]
    return [
        {"label": "Total Net Assets", "value": _fmt_aum(o.total_aum_usd), "delta": ""},
        {"label": "Funds", "value": f"{o.fund_count:,}", "delta": ""},
        {"label": "Distinct Securities", "value": f"{o.holding_count:,}", "delta": ""},
        {"label": "Annual Income (12M)", "value": _fmt_aum(income), "delta": ""},
        {"label": "Weighted Yield (12M)", "value": _fmt_yield(yield_12m), "delta": ""},
        {"label": "30-Day SEC Yield", "value": _fmt_yield(sec_30d), "delta": ""},
    ]


@app.get("/metrics/returns")
def metrics_returns() -> list[dict]:
    rt = returns_total()
    return [
        {"label": "YTD", "value": _fmt_pct(rt["returns_pct"].get("ytd")), "delta": ""},
        {"label": "1Y", "value": _fmt_pct(rt["returns_pct"].get("1y")), "delta": ""},
        {
            "label": "3Y Annualized",
            "value": _fmt_pct(rt["returns_pct"].get("3y")),
            "delta": "",
        },
        {
            "label": "5Y Annualized",
            "value": _fmt_pct(rt["returns_pct"].get("5y")),
            "delta": "",
        },
        {
            "label": "10Y Annualized",
            "value": _fmt_pct(rt["returns_pct"].get("10y")),
            "delta": "",
        },
        {
            "label": "Since Inception",
            "value": _fmt_pct(rt["returns_pct"].get("inception")),
            "delta": "",
        },
    ]


@app.get("/metrics/income")
def metrics_income(portfolio: str | None = None) -> list[dict]:
    res = income_total(portfolio=portfolio)
    aum = res["total_aum_usd"]

    def fmt_usd(v: float) -> str:
        if v is None:
            return "—"
        for unit, scale in (("T", 1e12), ("B", 1e9), ("M", 1e6)):
            if abs(v) >= scale:
                return f"${v / scale:,.2f}{unit}"
        return f"${v:,.0f}"

    def fmt_yield(v):
        return f"{v:.2f}%" if isinstance(v, (int, float)) else "—"

    tm = res["by_yield_basis"]["twelve_month"]
    sec = res["by_yield_basis"]["sec_30d"]
    return [
        {"label": "Total Net Assets", "value": fmt_usd(aum), "delta": ""},
        {
            "label": "Annual Income (12M)",
            "value": fmt_usd(tm["annual_income_usd"]),
            "delta": "",
        },
        {
            "label": "Weighted Yield (12M)",
            "value": fmt_yield(tm["weighted_yield_pct"]),
            "delta": "",
        },
        {
            "label": "Forward Income (30D SEC)",
            "value": fmt_usd(sec["annual_income_usd"]),
            "delta": "",
        },
        {
            "label": "30-Day SEC Yield",
            "value": fmt_yield(sec["weighted_yield_pct"]),
            "delta": "",
        },
    ]


@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(25, ge=1, le=200),
) -> dict:
    qq = f"%{q}%"
    funds_df = _read_sql(
        """SELECT portfolio_id, ticker, isin, name, portfolio,
                  asset_class, sub_asset_class, investment_style,
                  market_type, region, product_view,
                  country, currency,
                  total_aum_usd, external_aum_usd, internally_held_usd,
                  holdings_as_of_date
           FROM funds
           WHERE ticker LIKE ?
              OR isin LIKE ?
              OR name LIKE ?
              OR portfolio_id LIKE ?
           ORDER BY total_aum_usd DESC NULLS LAST
           LIMIT ?""",
        (qq, qq, qq, qq, limit),
    )
    holdings_df = _read_sql(
        """SELECT leaf_holding_name AS name, leaf_holding_ticker AS ticker,
                  leaf_holding_isin AS isin, holding_type, sector, country,
                  SUM(market_value_usd) AS market_value_usd,
                  COUNT(*) AS appearances
           FROM holdings_lookthrough
           WHERE leaf_holding_name LIKE ? OR leaf_holding_ticker LIKE ? OR leaf_holding_isin LIKE ?
           GROUP BY leaf_holding_isin, leaf_holding_name
           ORDER BY market_value_usd DESC
           LIMIT ?""",
        (qq, qq, qq, limit),
    )
    return {
        "query": q,
        "funds": [FundSummary(**r).model_dump() for r in _frame_to_records(funds_df)],
        "holdings": _frame_to_records(holdings_df),
    }


# ---------------------------------------------------------------------------
# Regulatory documents (iShares US) — backed by the fund_documents table
# populated at ingestion time.  Lookups are pure SQL; only the actual PDF
# download hits the network.
# ---------------------------------------------------------------------------
import base64  # noqa: E402

from fastapi import Body  # noqa: E402

from . import documents as _documents  # noqa: E402


@app.get("/blackrock/doc_options")
def doc_options(
    ticker: str,
    region: str | None = None,  # noqa: ARG001  kept for widget cascade compat
    fund_type: str | None = None,  # noqa: ARG001  kept for widget cascade compat
) -> list[dict]:
    tk = ticker.upper().strip()
    df = _read_sql(
        "SELECT slug, label FROM fund_documents WHERE ticker = ? ORDER BY label",
        (tk,),
    )
    return [{"label": f"{tk} - {r['label']}", "value": f"US|{tk}|{r['slug']}"} for _, r in df.iterrows()]


async def _resolve_documents(doc_name: list[str]) -> list[dict]:
    files: list = []
    for name in doc_name[:1]:
        parts = name.split("|", 2)
        if len(parts) < 3:
            files.append({"error_type": "invalid", "content": f"Bad key: {name!r}"})
            continue
        _region, ticker, slug = parts
        tk = ticker.upper().strip()
        df = _read_sql(
            "SELECT label, url FROM fund_documents WHERE ticker = ? AND slug = ? LIMIT 1",
            (tk, slug),
        )
        if df.empty:
            files.append({"error_type": "not_found", "content": f"{tk}/{slug}"})
            continue
        url = df["url"].iloc[0].replace("&amp;", "&")
        label = df["label"].iloc[0]
        try:
            pdf = await _documents.fetch_pdf_bytes(url, f"{tk}_{slug}")
            b64 = base64.b64encode(pdf).decode("utf-8")
            files.append(
                {
                    "content": b64,
                    "data_format": {
                        "data_type": "pdf",
                        "filename": f"{tk} - {label}.pdf",
                    },
                }
            )
        except Exception as exc:
            files.append({"error_type": "download_error", "content": str(exc)})
    return files


@app.get("/blackrock/view_documents")
async def view_documents_get(doc_name: list[str] = Query(...)) -> list[dict]:
    files = await _resolve_documents(doc_name)
    return JSONResponse(headers={"Content-Type": "application/json"}, content=files)


@app.post("/blackrock/view_documents")
async def view_documents(doc_name: list[str] = Body(..., embed=True)) -> list[dict]:
    files = await _resolve_documents(doc_name)
    return JSONResponse(headers={"Content-Type": "application/json"}, content=files)
