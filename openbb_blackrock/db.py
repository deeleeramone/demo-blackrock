"""SQLite persistence for the BlackRock holdings ingestion pipeline.

Uses Python's stdlib ``sqlite3`` — no external ORM dependency.

Schema:
* ``funds``               — one row per parent fund
* ``holdings``            — one row per (parent_portfolio_id, holding_id, as_of_date)
* ``fx_rates``            — daily EUR-base reference rates copied from ECB
* ``fund_links``          — pre-resolved fund-of-fund edges
* ``holdings_lookthrough``— materialized view of all holdings with
  fund-of-fund legs recursively expanded; downstream aggregation reads
  *only* this table to guarantee no double counting.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Iterator

from .holdings import Holding

log = logging.getLogger(__name__)

_DEFAULT_DB_DIR = Path(
    os.environ.get("BLACKROCK_DB_DIR", Path.home() / ".openbb-blackrock")
)
_DEFAULT_DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = _DEFAULT_DB_DIR / "holdings.db"

_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS funds (
        portfolio_id        TEXT PRIMARY KEY,
        ticker              TEXT,
        isin                TEXT,
        name                TEXT NOT NULL,
        portfolio           TEXT NOT NULL,
        currency            TEXT NOT NULL DEFAULT 'USD',
        nav_date            TEXT,
        nav_local           REAL,
        nav_usd             REAL,
        total_aum_usd       REAL,
        internally_held_usd REAL NOT NULL DEFAULT 0,
        external_aum_usd    REAL,
        -- Classification
        asset_class             TEXT,
        sub_asset_class         TEXT,
        investment_style        TEXT,
        market_type             TEXT,
        region                  TEXT,
        product_view            TEXT,
        country                 TEXT,
        share_class             TEXT,
        esg_classification      TEXT,
        sfdr_classification     TEXT,
        -- Lifecycle / links
        inception_date          TEXT,
        product_page_url        TEXT,
        -- NAV-based total return (annualized %, BlackRock-published)
        nav_ytd_pct             REAL,
        nav_1y_pct              REAL,
        nav_3y_pct              REAL,
        nav_5y_pct              REAL,
        nav_10y_pct             REAL,
        nav_inception_pct       REAL,
        nav_perf_as_of          TEXT,
        -- Price-based total return (market price annualized %)
        price_ytd_pct           REAL,
        price_1y_pct            REAL,
        price_3y_pct            REAL,
        price_5y_pct            REAL,
        price_10y_pct           REAL,
        price_inception_pct     REAL,
        -- Yields and pricing dynamics
        sec_yield_30d_pct       REAL,
        twelve_month_yield_pct  REAL,
        unsubsidized_yield_pct  REAL,
        distribution_yield_pct  REAL,
        premium_discount_pct    REAL,
        -- Key Facts (scraped from the product page; vary by asset class)
        expense_ratio_pct           REAL,
        management_fee_pct          REAL,
        acquired_fund_fees_pct      REAL,
        other_expenses_pct          REAL,
        sponsor_fee_pct             REAL,
        closing_price               REAL,
        mid_point_price             REAL,
        daily_volume                INTEGER,
        avg_volume_30d              INTEGER,
        median_bid_ask_spread_30d_pct REAL,
        shares_outstanding          INTEGER,
        net_assets_usd              REAL,
        number_of_holdings          INTEGER,
        equity_beta_3y              REAL,
        std_dev_3y_pct              REAL,
        pe_ratio                    REAL,
        pb_ratio                    REAL,
        effective_duration          REAL,
        convexity                   REAL,
        avg_ytm_pct                 REAL,
        option_adjusted_spread_bps  REAL,
        weighted_avg_coupon_pct     REAL,
        weighted_avg_maturity_yrs   REAL,
        ounces_in_trust             REAL,
        tonnes_in_trust             REAL,
        basket_amount               REAL,
        indicative_basket_amount    REAL,
        cusip                       TEXT,
        exchange                    TEXT,
        benchmark_index             TEXT,
        bloomberg_index_ticker      TEXT,
        distribution_frequency      TEXT,
        -- Verbatim copy of every field BlackRock returned (for future use)
        raw_json                TEXT,
        holdings_as_of_date     TEXT,
        last_fetched_at         TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_funds_portfolio ON funds(portfolio)",
    "CREATE INDEX IF NOT EXISTS ix_funds_ticker    ON funds(ticker)",
    "CREATE INDEX IF NOT EXISTS ix_funds_isin      ON funds(isin)",
    """
    CREATE TABLE IF NOT EXISTS holdings (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        parent_portfolio_id TEXT NOT NULL REFERENCES funds(portfolio_id),
        parent_ticker       TEXT,
        portfolio           TEXT NOT NULL,
        holding_id          TEXT,
        holding_ticker      TEXT,
        holding_name        TEXT NOT NULL,
        holding_type        TEXT,
        holding_isin        TEXT,
        holding_cusip       TEXT,
        holding_sedol       TEXT,
        sector              TEXT,
        country             TEXT,
        exchange            TEXT,
        currency            TEXT NOT NULL,
        report_currency     TEXT,
        shares_or_par       REAL,
        price               REAL,
        market_value_local  REAL NOT NULL DEFAULT 0,
        notional_value      REAL,
        market_value_usd    REAL NOT NULL DEFAULT 0,
        weight_pct          REAL NOT NULL DEFAULT 0,
        fx_rate             REAL,
        coupon_pct          REAL,
        maturity_date       TEXT,
        duration            REAL,
        mod_duration        REAL,
        ytm_pct             REAL,
        yield_to_call_pct   REAL,
        yield_to_worst_pct  REAL,
        real_duration       REAL,
        real_ytm_pct        REAL,
        accrual_date        TEXT,
        effective_date      TEXT,
        as_of_date          TEXT NOT NULL,
        raw_json            TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_holdings_parent     ON holdings(parent_portfolio_id)",
    "CREATE INDEX IF NOT EXISTS ix_holdings_portfolio  ON holdings(portfolio)",
    "CREATE INDEX IF NOT EXISTS ix_holdings_isin       ON holdings(holding_isin)",
    "CREATE INDEX IF NOT EXISTS ix_holdings_ticker     ON holdings(holding_ticker)",
    "CREATE INDEX IF NOT EXISTS ix_holdings_as_of      ON holdings(as_of_date)",
    "CREATE INDEX IF NOT EXISTS ix_holdings_parent_date ON holdings(parent_portfolio_id, as_of_date)",
    """
    CREATE TABLE IF NOT EXISTS fx_rates (
        rate_date TEXT NOT NULL,
        ccy       TEXT NOT NULL,
        eur_rate  REAL NOT NULL,
        PRIMARY KEY (rate_date, ccy)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fund_links (
        parent_portfolio_id TEXT NOT NULL REFERENCES funds(portfolio_id),
        child_portfolio_id  TEXT NOT NULL REFERENCES funds(portfolio_id),
        weight_pct          REAL NOT NULL,
        as_of_date          TEXT NOT NULL,
        PRIMARY KEY (parent_portfolio_id, child_portfolio_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS holdings_lookthrough (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        parent_portfolio_id TEXT NOT NULL,
        portfolio           TEXT NOT NULL,
        leaf_holding_name   TEXT NOT NULL,
        leaf_holding_ticker TEXT,
        leaf_holding_isin   TEXT,
        leaf_holding_cusip  TEXT,
        holding_type        TEXT,
        sector              TEXT,
        country             TEXT,
        currency            TEXT NOT NULL,
        market_value_usd    REAL NOT NULL,
        weight_pct          REAL NOT NULL,
        path_depth          INTEGER NOT NULL DEFAULT 0,
        as_of_date          TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_lt_parent      ON holdings_lookthrough(parent_portfolio_id)",
    "CREATE INDEX IF NOT EXISTS ix_lt_portfolio   ON holdings_lookthrough(portfolio)",
    "CREATE INDEX IF NOT EXISTS ix_lt_isin        ON holdings_lookthrough(leaf_holding_isin)",
    "CREATE INDEX IF NOT EXISTS ix_lt_parent_date ON holdings_lookthrough(parent_portfolio_id, as_of_date)",
    "CREATE INDEX IF NOT EXISTS ix_lt_ticker      ON holdings_lookthrough(leaf_holding_ticker)",
    "CREATE INDEX IF NOT EXISTS ix_lt_type        ON holdings_lookthrough(holding_type)",
    "CREATE INDEX IF NOT EXISTS ix_lt_sector      ON holdings_lookthrough(sector)",
    "CREATE INDEX IF NOT EXISTS ix_lt_country     ON holdings_lookthrough(country)",
    "CREATE INDEX IF NOT EXISTS ix_lt_type_ticker ON holdings_lookthrough(holding_type, leaf_holding_ticker)",
    """
    CREATE TABLE IF NOT EXISTS holdings_lt_latest (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        parent_portfolio_id TEXT NOT NULL,
        portfolio           TEXT NOT NULL,
        leaf_holding_name   TEXT NOT NULL,
        leaf_holding_ticker TEXT,
        leaf_holding_isin   TEXT,
        leaf_holding_cusip  TEXT,
        holding_type        TEXT,
        sector              TEXT,
        country             TEXT,
        currency            TEXT NOT NULL,
        market_value_usd    REAL NOT NULL,
        weight_pct          REAL NOT NULL,
        path_depth          INTEGER NOT NULL DEFAULT 0,
        as_of_date          TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_ltl_parent      ON holdings_lt_latest(parent_portfolio_id)",
    "CREATE INDEX IF NOT EXISTS ix_ltl_ticker      ON holdings_lt_latest(leaf_holding_ticker)",
    "CREATE INDEX IF NOT EXISTS ix_ltl_isin        ON holdings_lt_latest(leaf_holding_isin)",
    "CREATE INDEX IF NOT EXISTS ix_ltl_type        ON holdings_lt_latest(holding_type)",
    "CREATE INDEX IF NOT EXISTS ix_ltl_sector      ON holdings_lt_latest(sector)",
    "CREATE INDEX IF NOT EXISTS ix_ltl_country     ON holdings_lt_latest(country)",
    "CREATE INDEX IF NOT EXISTS ix_ltl_type_ticker ON holdings_lt_latest(holding_type, leaf_holding_ticker)",
    """
    CREATE TABLE IF NOT EXISTS nav_history (
        portfolio_id        TEXT NOT NULL REFERENCES funds(portfolio_id),
        as_of_date          TEXT NOT NULL,
        nav_per_share       REAL NOT NULL,
        shares_outstanding  REAL,
        ex_dividends        REAL,
        daily_return_pct    REAL,
        PRIMARY KEY (portfolio_id, as_of_date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_nav_date ON nav_history(as_of_date)",
    """
    CREATE TABLE IF NOT EXISTS distributions (
        portfolio_id        TEXT NOT NULL REFERENCES funds(portfolio_id),
        ex_date             TEXT NOT NULL,
        record_date         TEXT,
        payable_date        TEXT,
        total_distribution  REAL NOT NULL,
        income              REAL,
        st_cap_gains        REAL,
        lt_cap_gains        REAL,
        return_of_capital   REAL,
        PRIMARY KEY (portfolio_id, ex_date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_dist_ex_date ON distributions(ex_date)",
    """
    CREATE TABLE IF NOT EXISTS fund_documents (
        portfolio_id        TEXT NOT NULL,
        ticker              TEXT NOT NULL,
        slug                TEXT NOT NULL,
        label               TEXT NOT NULL,
        url                 TEXT NOT NULL,
        PRIMARY KEY (portfolio_id, ticker, slug)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fund_documents_ticker ON fund_documents(ticker)",
]


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


_conn: sqlite3.Connection | None = None


def init_db(path: Path | str | None = None) -> sqlite3.Connection:
    """Create tables if they do not exist.  Idempotent.  Returns the
    process-wide connection."""
    global _conn
    p = Path(path) if path else DB_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    _conn = sqlite3.connect(str(p), isolation_level=None)  # autocommit
    _conn.execute("PRAGMA foreign_keys = ON")
    _conn.execute("PRAGMA journal_mode = WAL")
    for stmt in _SCHEMA:
        _conn.execute(stmt)
    log.info("DB ready at %s", p)
    return _conn


def get_conn() -> sqlite3.Connection:
    if _conn is None:
        init_db()
    assert _conn is not None
    return _conn


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    conn = get_conn()
    conn.execute("BEGIN")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


# ---------------------------------------------------------------------------
# Ingestion helpers
# ---------------------------------------------------------------------------


_FUND_COLUMNS = [
    "portfolio_id",
    "ticker",
    "isin",
    "name",
    "portfolio",
    "currency",
    "nav_date",
    "nav_local",
    "nav_usd",
    "total_aum_usd",
    "asset_class",
    "sub_asset_class",
    "investment_style",
    "market_type",
    "region",
    "product_view",
    "country",
    "share_class",
    "esg_classification",
    "sfdr_classification",
    "inception_date",
    "product_page_url",
    "nav_ytd_pct",
    "nav_1y_pct",
    "nav_3y_pct",
    "nav_5y_pct",
    "nav_10y_pct",
    "nav_inception_pct",
    "nav_perf_as_of",
    "price_ytd_pct",
    "price_1y_pct",
    "price_3y_pct",
    "price_5y_pct",
    "price_10y_pct",
    "price_inception_pct",
    "sec_yield_30d_pct",
    "twelve_month_yield_pct",
    "unsubsidized_yield_pct",
    "distribution_yield_pct",
    "premium_discount_pct",
    "expense_ratio_pct",
    "management_fee_pct",
    "acquired_fund_fees_pct",
    "other_expenses_pct",
    "sponsor_fee_pct",
    "closing_price",
    "mid_point_price",
    "daily_volume",
    "avg_volume_30d",
    "median_bid_ask_spread_30d_pct",
    "shares_outstanding",
    "net_assets_usd",
    "number_of_holdings",
    "equity_beta_3y",
    "std_dev_3y_pct",
    "pe_ratio",
    "pb_ratio",
    "effective_duration",
    "convexity",
    "avg_ytm_pct",
    "option_adjusted_spread_bps",
    "weighted_avg_coupon_pct",
    "weighted_avg_maturity_yrs",
    "ounces_in_trust",
    "tonnes_in_trust",
    "basket_amount",
    "indicative_basket_amount",
    "cusip",
    "exchange",
    "benchmark_index",
    "bloomberg_index_ticker",
    "distribution_frequency",
    "raw_json",
    "holdings_as_of_date",
    "last_fetched_at",
]


def upsert_fund(conn: sqlite3.Connection, **fields) -> None:
    """Insert or update a fund row.  Every keyword argument must match a
    column name in :data:`_FUND_COLUMNS`; absent fields default to NULL.
    """
    fields.setdefault(
        "last_fetched_at", datetime.utcnow().isoformat(timespec="seconds")
    )
    nav_d = fields.get("nav_date")
    if isinstance(nav_d, date):
        fields["nav_date"] = nav_d.isoformat()
    h_d = fields.get("holdings_as_of_date")
    if isinstance(h_d, date):
        fields["holdings_as_of_date"] = h_d.isoformat()

    placeholders = ",".join("?" for _ in _FUND_COLUMNS)
    cols = ",".join(_FUND_COLUMNS)
    update_cols = ",".join(
        f"{c}=excluded.{c}" for c in _FUND_COLUMNS if c != "portfolio_id"
    )
    values = [fields.get(c) for c in _FUND_COLUMNS]
    conn.execute(
        f"""INSERT INTO funds ({cols}) VALUES ({placeholders})
            ON CONFLICT(portfolio_id) DO UPDATE SET {update_cols}""",
        values,
    )


def replace_documents_for_fund(
    conn: sqlite3.Connection,
    portfolio_id: str,
    ticker: str,
    docs: dict,
) -> int:
    """Replace fund_documents rows for one (portfolio_id, ticker) pair.

    ``docs`` is the per-ticker mapping ``{slug: {"label": ..., "url": ...}}``
    as produced by ``app._build_and_cache_us_index`` for iShares US funds.
    """
    conn.execute(
        "DELETE FROM fund_documents WHERE portfolio_id = ? AND ticker = ?",
        (portfolio_id, ticker),
    )
    if not docs:
        return 0
    rows = [
        (portfolio_id, ticker, slug, info.get("label") or slug, info["url"])
        for slug, info in docs.items()
        if info and info.get("url")
    ]
    if not rows:
        return 0
    conn.executemany(
        "INSERT INTO fund_documents (portfolio_id, ticker, slug, label, url) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def replace_holdings_for_fund(
    conn: sqlite3.Connection,
    parent_portfolio_id: str,
    holdings: Iterable[Holding],
) -> int:
    conn.execute(
        "DELETE FROM holdings WHERE parent_portfolio_id = ?",
        (parent_portfolio_id,),
    )
    return _insert_holdings(conn, holdings)


def replace_holdings_for_fund_date(
    conn: sqlite3.Connection,
    parent_portfolio_id: str,
    as_of_date: str,
    holdings: Iterable[Holding],
) -> int:
    """Replace only the (fund, as_of_date) slice — preserves other snapshots."""
    conn.execute(
        "DELETE FROM holdings WHERE parent_portfolio_id = ? AND as_of_date = ?",
        (parent_portfolio_id, as_of_date),
    )
    return _insert_holdings(conn, holdings)


def _insert_holdings(
    conn: sqlite3.Connection,
    holdings: Iterable[Holding],
) -> int:
    rows = [
        (
            h.parent_portfolio_id,
            h.parent_ticker,
            h.portfolio,
            h.holding_id,
            h.holding_ticker,
            h.holding_name,
            h.holding_type,
            h.holding_isin,
            h.holding_cusip,
            h.holding_sedol,
            h.sector,
            h.country,
            h.exchange,
            h.currency,
            h.report_currency,
            h.shares_or_par,
            h.price,
            h.market_value_local,
            h.notional_value,
            h.market_value_usd,
            h.weight_pct,
            h.fx_rate,
            h.coupon_pct,
            h.maturity_date,
            h.duration,
            h.mod_duration,
            h.ytm_pct,
            h.yield_to_call_pct,
            h.yield_to_worst_pct,
            h.real_duration,
            h.real_ytm_pct,
            h.accrual_date,
            h.effective_date,
            h.as_of_date.isoformat(),
            json.dumps(h.raw, ensure_ascii=False),
        )
        for h in holdings
    ]
    if rows:
        conn.executemany(
            """
            INSERT INTO holdings (
                parent_portfolio_id, parent_ticker, portfolio,
                holding_id, holding_ticker, holding_name, holding_type,
                holding_isin, holding_cusip, holding_sedol, sector,
                country, exchange, currency, report_currency,
                shares_or_par, price, market_value_local, notional_value,
                market_value_usd, weight_pct, fx_rate,
                coupon_pct, maturity_date, duration, mod_duration,
                ytm_pct, yield_to_call_pct, yield_to_worst_pct,
                real_duration, real_ytm_pct,
                accrual_date, effective_date, as_of_date, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
    return len(rows)


def seed_fx_rates(conn: sqlite3.Connection) -> int:
    """No-op for now — FX rate sourcing is deferred.  The ``fx_rates``
    table stays empty until the FX source is decided.  Kept as a stub so
    callers don't change.
    """
    return 0


# ---------------------------------------------------------------------------
# Fund-of-fund link resolution
# ---------------------------------------------------------------------------


def compute_dedupe_metadata(conn: sqlite3.Connection) -> None:
    """Populate ``funds.internally_held_usd`` and ``funds.external_aum_usd``.

    For each fund F:
      ``internally_held_usd`` = sum of ``holdings.market_value_usd`` across
      every parent fund whose holding row resolves (by ISIN or ticker) to F.
      This is the dollar value of F that is held *inside* our universe.

      ``external_aum_usd`` = ``max(0, total_aum_usd - internally_held_usd)``.
      For top-level funds (held by nothing in the universe) this equals
      total_aum_usd.  For purely-internal funds it tends toward zero.

    Aggregation that wants to avoid fund-of-fund double counting must
    anchor each parent's lookthrough at its ``external_aum_usd`` rather
    than its full NAV — that is exactly what :func:`rebuild_lookthrough`
    does after this function runs.
    """
    # Build ISIN/ticker → portfolio_id lookups
    isin_to_pid: dict[str, str] = {}
    ticker_to_pid: dict[str, str] = {}
    all_funds = conn.execute("SELECT portfolio_id, isin, ticker FROM funds").fetchall()
    log.info("dedupe metadata: updating %d funds ...", len(all_funds))
    for pid, isin, tk in all_funds:
        if isin:
            isin_to_pid[isin.upper()] = pid
        if tk:
            ticker_to_pid[tk.upper()] = pid

    # Sum holdings.market_value_usd grouped by which fund the holding
    # represents (when it represents one).
    internally_held: dict[str, float] = {}
    for parent_pid, h_isin, h_tk, mv in conn.execute(
        """SELECT parent_portfolio_id, holding_isin, holding_ticker,
                  market_value_usd FROM holdings"""
    ):
        child_pid = None
        if h_isin and h_isin.upper() in isin_to_pid:
            child_pid = isin_to_pid[h_isin.upper()]
        elif h_tk and h_tk.upper() in ticker_to_pid:
            child_pid = ticker_to_pid[h_tk.upper()]
        if not child_pid or child_pid == parent_pid:
            continue
        internally_held[child_pid] = internally_held.get(child_pid, 0.0) + (mv or 0.0)

    # Update funds rows
    rows = conn.execute("SELECT portfolio_id, total_aum_usd FROM funds").fetchall()
    updates = []
    for pid, aum in rows:
        ih = internally_held.get(pid, 0.0)
        # Bound external to [0, aum] — guards against rounding when our
        # holdings sum exceeds reported AUM.
        external = max(0.0, (aum or 0.0) - ih)
        updates.append((ih, external, pid))
    conn.executemany(
        "UPDATE funds SET internally_held_usd = ?, external_aum_usd = ? WHERE portfolio_id = ?",
        updates,
    )


def rebuild_fund_links(conn: sqlite3.Connection) -> int:
    total_h = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    total_f = conn.execute("SELECT COUNT(*) FROM funds").fetchone()[0]
    log.info(
        "fund links: scanning %d holdings rows across %d funds ...", total_h, total_f
    )
    conn.execute("DELETE FROM fund_links")

    isin_to_pid: dict[str, str] = {}
    ticker_to_pid: dict[str, str] = {}
    for pid, isin, tk in conn.execute("SELECT portfolio_id, isin, ticker FROM funds"):
        if isin:
            isin_to_pid[isin.upper()] = pid
        if tk:
            ticker_to_pid[tk.upper()] = pid

    seen: set[tuple[str, str]] = set()
    rows = []
    for parent_pid, isin, ticker, weight, as_of in conn.execute(
        """SELECT parent_portfolio_id, holding_isin, holding_ticker,
                  weight_pct, as_of_date FROM holdings"""
    ):
        child = None
        if isin and isin.upper() in isin_to_pid:
            child = isin_to_pid[isin.upper()]
        elif ticker and ticker.upper() in ticker_to_pid:
            child = ticker_to_pid[ticker.upper()]
        if not child or child == parent_pid:
            continue
        key = (parent_pid, child)
        if key in seen:
            continue
        seen.add(key)
        rows.append((parent_pid, child, weight or 0.0, as_of))

    if rows:
        conn.executemany(
            """INSERT INTO fund_links
               (parent_portfolio_id, child_portfolio_id, weight_pct, as_of_date)
               VALUES (?,?,?,?)""",
            rows,
        )
    return len(rows)


# ---------------------------------------------------------------------------
# Look-through view (no double counting)
# ---------------------------------------------------------------------------


def rebuild_lookthrough(conn: sqlite3.Connection) -> int:
    """Build ``holdings_lookthrough`` so each leaf row carries an
    *absolute USD* market value attributable to **external investors of
    the root parent fund**, with no double-counting across the iShares
    universe.

    Algorithm:

    * Each fund F is walked as a root, anchored at
      ``F.external_aum_usd`` — the dollars external investors put into
      F.  That anchor flows down the holdings tree:

        leg_dollars = anchor × (h.weight_pct / 100)

      (i.e. the dollar amount *of root's external NAV* that ends up
      flowing through this leg).

    * If a holding row maps to another fund in our universe (ISIN/ticker
      match), we recurse into that fund, carrying ``leg_dollars`` as the
      new anchor.  This is the look-through expansion.

    * Otherwise it's a leaf — emit a row with ``market_value_usd =
      leg_dollars`` and ``weight_pct = leg_dollars / root.external × 100``.

    * **Funds with no decomposable holdings** (e.g. SLV physical silver,
      a fund whose CSV failed) get a *synthetic* leaf representing the
      fund itself: ``leaf_name = fund.name``, ``holding_type =
      fund.asset_class``, ``market_value_usd = anchor``.  This way every
      external dollar is represented in lookthrough — even when the
      underlying isn't broken out.

    Summing ``market_value_usd`` over the entire ``holdings_lookthrough``
    table therefore equals ``Σ funds.external_aum_usd`` — the total iShares
    AUM held by external investors, with no double counting.

    Cycles abort the offending branch with a logged warning.
    """
    conn.execute("DELETE FROM holdings_lookthrough")

    # ── Lookups ──────────────────────────────────────────────────────
    isin_to_pid: dict[str, str] = {}
    ticker_to_pid: dict[str, str] = {}
    fund_meta: dict[str, dict] = {}
    for (
        pid,
        ticker,
        isin,
        name,
        asset_class,
        sub_asset_class,
        country,
        ccy,
        total_aum,
        ext_aum,
    ) in conn.execute(
        """SELECT portfolio_id, ticker, isin, name, asset_class,
                  sub_asset_class, country, currency,
                  total_aum_usd, external_aum_usd FROM funds"""
    ):
        if isin:
            isin_to_pid[isin.upper()] = pid
        if ticker:
            ticker_to_pid[ticker.upper()] = pid
        fund_meta[pid] = {
            "ticker": ticker,
            "isin": isin,
            "name": name,
            "asset_class": asset_class or "Other",
            "sub_asset_class": sub_asset_class,
            "country": country,
            "currency": ccy,
            "total_aum_usd": total_aum or 0.0,
            "external_aum_usd": ext_aum if ext_aum is not None else (total_aum or 0.0),
        }

    holdings_by_parent: dict[str, list[tuple]] = {}
    for row in conn.execute(
        """SELECT parent_portfolio_id, portfolio, holding_name,
                  holding_ticker, holding_isin, holding_cusip,
                  holding_type, sector, country, currency,
                  market_value_local, weight_pct, as_of_date
           FROM holdings"""
    ):
        holdings_by_parent.setdefault(row[0], []).append(row)

    def child_of(isin: str | None, ticker: str | None) -> str | None:
        if isin and isin.upper() in isin_to_pid:
            return isin_to_pid[isin.upper()]
        if ticker and ticker.upper() in ticker_to_pid:
            return ticker_to_pid[ticker.upper()]
        return None

    out_rows: list[tuple] = []

    today_iso = date.today().isoformat()

    def emit_synthetic_leaf(
        root_pid: str, fund_pid: str, anchor_dollars: float, depth: int
    ) -> None:
        """Represent ``fund_pid`` itself as a single leaf row.

        Used for funds that have an AUM but no decomposable holdings
        (physical commodity ETFs, recently-launched funds, fetch
        failures).  ``anchor_dollars`` is the dollar slice of root that
        flows here.
        """
        meta = fund_meta.get(fund_pid, {})
        ext_aum = meta.get("external_aum_usd", 0.0) or 0.0
        if anchor_dollars <= 0:
            return
        sector = meta.get("sub_asset_class") or meta.get("asset_class")
        out_rows.append(
            (
                root_pid,
                "iShares",
                f"{meta.get('name') or fund_pid} (fund-as-leaf)",
                meta.get("ticker"),
                meta.get("isin"),
                None,
                meta.get("asset_class") or "Fund",
                sector,
                meta.get("country"),
                meta.get("currency") or "USD",
                anchor_dollars,
                (anchor_dollars / ext_aum * 100.0)
                if root_pid == fund_pid and ext_aum
                else 0.0,
                depth,
                today_iso,
            )
        )

    def walk(
        root_pid: str,
        cur_pid: str,
        anchor_dollars: float,
        root_external: float,
        depth: int,
        visited: frozenset[str],
    ) -> None:
        if cur_pid in visited:
            log.warning(
                "lookthrough cycle: %s already in %s",
                cur_pid,
                "->".join(sorted(visited)),
            )
            return
        if anchor_dollars <= 0:
            return
        visited = visited | {cur_pid}

        rows = holdings_by_parent.get(cur_pid, [])
        if not rows:
            # No decomposable holdings → fund-as-leaf
            emit_synthetic_leaf(root_pid, cur_pid, anchor_dollars, depth)
            return

        # ── Detect pre-expanded duplication ─────────────────────────────
        # BlackRock's allocation-fund CSVs (AOA, AOR, LifePath, etc.)
        # include BOTH the high-level fund holdings AND the look-through
        # constituents in the same file.  Sum-of-weights ≈ 184%.  If the
        # fund-rows alone account for ~100% of weight, treat them as the
        # authoritative layer and drop the constituent overlay — we'll
        # re-derive constituents by recursing into each child fund.
        fund_rows = []
        for r in rows:
            if child_of(r[4], r[3]) is not None and child_of(r[4], r[3]) != cur_pid:
                fund_rows.append(r)
        fund_wt_sum = sum((r[11] or 0.0) for r in fund_rows)
        if fund_wt_sum >= 95.0 and fund_wt_sum <= 105.0:
            rows = fund_rows

        # Use mv_local-based shares so a row's leg ratio is robust to
        # weight-column gaps (bond CSVs sometimes leave cash/futures at
        # 0%).  Use the SIGNED sum so positive and negative positions
        # both flow through; short / negative-MV legs are emitted as
        # negative-USD leaves (correct for forwards / swaps).
        sum_mv = sum((r[10] or 0.0) for r in rows) or 0.0

        for (
            _parent_pid,
            portfolio,
            h_name,
            h_tk,
            h_isin,
            h_cusip,
            h_type,
            sector,
            country,
            ccy,
            mv_local,
            weight,
            as_of,
        ) in rows:
            if sum_mv != 0:
                leg_share = (mv_local or 0.0) / sum_mv
            elif weight is not None:
                leg_share = (weight or 0.0) / 100.0
            else:
                leg_share = 0.0
            leg_dollars = anchor_dollars * leg_share
            if leg_dollars == 0:
                continue

            child = child_of(h_isin, h_tk)
            if child and child != cur_pid:
                walk(root_pid, child, leg_dollars, root_external, depth + 1, visited)
            else:
                weight_of_root = (
                    leg_dollars / root_external * 100.0 if root_external else 0.0
                )
                out_rows.append(
                    (
                        root_pid,
                        portfolio,
                        h_name,
                        h_tk,
                        h_isin,
                        h_cusip,
                        h_type,
                        sector,
                        country,
                        ccy or "USD",
                        leg_dollars,
                        weight_of_root,
                        depth,
                        as_of,
                    )
                )

    _INSERT_SQL = """INSERT INTO holdings_lookthrough (
                parent_portfolio_id, portfolio, leaf_holding_name,
                leaf_holding_ticker, leaf_holding_isin, leaf_holding_cusip,
                holding_type, sector, country, currency,
                market_value_usd, weight_pct, path_depth, as_of_date
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

    # Walk every fund as a root.  Anchor = its external AUM.  Funds with
    # 0 external AUM (entirely internally held) contribute nothing.
    active = [
        (pid, m)
        for pid, m in fund_meta.items()
        if (m.get("external_aum_usd") or 0.0) > 0
    ]
    log.info("lookthrough: walking %d funds ...", len(active))
    total_inserted = 0
    for i, (root_pid, meta) in enumerate(active, 1):
        ext = meta.get("external_aum_usd") or 0.0
        ticker = meta.get("ticker") or root_pid
        walk(root_pid, root_pid, ext, ext, 0, frozenset())
        added = len(out_rows)
        if out_rows:
            conn.executemany(_INSERT_SQL, out_rows)
            total_inserted += added
            out_rows.clear()
        log.info(
            "  [%d/%d] %s: inserted %d rows (%d total)",
            i,
            len(active),
            ticker,
            added,
            total_inserted,
        )
    return total_inserted
