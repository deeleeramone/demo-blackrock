-- Dolt (MySQL-dialect) schema for the iShares public holdings dataset.
-- Translated from the SQLite source: TEXT->VARCHAR (sized from observed maxima),
-- REAL->DOUBLE, INTEGER PK AUTOINCREMENT->BIGINT PK. raw_json columns dropped.
-- ISO date strings kept as VARCHAR to stay lossless against partial/blank values.

CREATE TABLE funds (
    portfolio_id            VARCHAR(64)  NOT NULL,
    ticker                  VARCHAR(64),
    isin                    VARCHAR(32),
    name                    VARCHAR(255) NOT NULL,
    portfolio               VARCHAR(128) NOT NULL,
    currency                VARCHAR(32)   NOT NULL DEFAULT 'USD',
    nav_date                VARCHAR(32),
    nav_local               DOUBLE,
    nav_usd                 DOUBLE,
    total_aum_usd           DOUBLE,
    internally_held_usd     DOUBLE       NOT NULL DEFAULT 0,
    external_aum_usd        DOUBLE,
    asset_class             VARCHAR(128),
    sub_asset_class         VARCHAR(128),
    investment_style        VARCHAR(128),
    market_type             VARCHAR(128),
    region                  VARCHAR(128),
    product_view            VARCHAR(128),
    country                 VARCHAR(128),
    share_class             VARCHAR(128),
    esg_classification      VARCHAR(128),
    sfdr_classification     VARCHAR(128),
    inception_date          VARCHAR(32),
    product_page_url        VARCHAR(512),
    nav_ytd_pct             DOUBLE,
    nav_1y_pct              DOUBLE,
    nav_3y_pct              DOUBLE,
    nav_5y_pct              DOUBLE,
    nav_10y_pct             DOUBLE,
    nav_inception_pct       DOUBLE,
    nav_perf_as_of          VARCHAR(32),
    price_ytd_pct           DOUBLE,
    price_1y_pct            DOUBLE,
    price_3y_pct            DOUBLE,
    price_5y_pct            DOUBLE,
    price_10y_pct           DOUBLE,
    price_inception_pct     DOUBLE,
    sec_yield_30d_pct       DOUBLE,
    twelve_month_yield_pct  DOUBLE,
    unsubsidized_yield_pct  DOUBLE,
    distribution_yield_pct  DOUBLE,
    premium_discount_pct    DOUBLE,
    -- Key Facts (scraped from the product page; vary by asset class)
    expense_ratio_pct             DOUBLE,
    management_fee_pct            DOUBLE,
    acquired_fund_fees_pct        DOUBLE,
    other_expenses_pct            DOUBLE,
    sponsor_fee_pct               DOUBLE,
    closing_price                 DOUBLE,
    mid_point_price               DOUBLE,
    daily_volume                  BIGINT,
    avg_volume_30d                BIGINT,
    median_bid_ask_spread_30d_pct DOUBLE,
    shares_outstanding            BIGINT,
    net_assets_usd                DOUBLE,
    number_of_holdings            BIGINT,
    equity_beta_3y                DOUBLE,
    std_dev_3y_pct                DOUBLE,
    pe_ratio                      DOUBLE,
    pb_ratio                      DOUBLE,
    effective_duration            DOUBLE,
    convexity                     DOUBLE,
    avg_ytm_pct                   DOUBLE,
    option_adjusted_spread_bps    DOUBLE,
    weighted_avg_coupon_pct       DOUBLE,
    weighted_avg_maturity_yrs     DOUBLE,
    ounces_in_trust               DOUBLE,
    tonnes_in_trust               DOUBLE,
    basket_amount                 DOUBLE,
    indicative_basket_amount      DOUBLE,
    cusip                         VARCHAR(32),
    exchange                      VARCHAR(64),
    benchmark_index               VARCHAR(255),
    bloomberg_index_ticker        VARCHAR(64),
    distribution_frequency        VARCHAR(32),
    holdings_as_of_date     VARCHAR(32),
    last_fetched_at         VARCHAR(32),
    PRIMARY KEY (portfolio_id),
    KEY ix_funds_portfolio (portfolio),
    KEY ix_funds_ticker (ticker),
    KEY ix_funds_isin (isin)
);

CREATE TABLE holdings (
    id                  BIGINT       NOT NULL,
    parent_portfolio_id VARCHAR(64)  NOT NULL,
    parent_ticker       VARCHAR(64),
    portfolio           VARCHAR(128) NOT NULL,
    holding_id          VARCHAR(64),
    holding_ticker      VARCHAR(64),
    holding_name        VARCHAR(255) NOT NULL,
    holding_type        VARCHAR(64),
    holding_isin        VARCHAR(32),
    holding_cusip       VARCHAR(32),
    holding_sedol       VARCHAR(32),
    sector              VARCHAR(128),
    country             VARCHAR(128),
    exchange            VARCHAR(128),
    currency            VARCHAR(32)   NOT NULL,
    report_currency     VARCHAR(32),
    shares_or_par       DOUBLE,
    price               DOUBLE,
    market_value_local  DOUBLE       NOT NULL DEFAULT 0,
    notional_value      DOUBLE,
    market_value_usd    DOUBLE       NOT NULL DEFAULT 0,
    weight_pct          DOUBLE       NOT NULL DEFAULT 0,
    fx_rate             DOUBLE,
    coupon_pct          DOUBLE,
    maturity_date       VARCHAR(32),
    duration            DOUBLE,
    mod_duration        DOUBLE,
    ytm_pct             DOUBLE,
    yield_to_call_pct   DOUBLE,
    yield_to_worst_pct  DOUBLE,
    real_duration       DOUBLE,
    real_ytm_pct        DOUBLE,
    accrual_date        VARCHAR(32),
    effective_date      VARCHAR(32),
    as_of_date          VARCHAR(32)  NOT NULL,
    PRIMARY KEY (id),
    KEY ix_holdings_parent (parent_portfolio_id),
    KEY ix_holdings_portfolio (portfolio),
    KEY ix_holdings_isin (holding_isin),
    KEY ix_holdings_ticker (holding_ticker),
    KEY ix_holdings_as_of (as_of_date),
    KEY ix_holdings_parent_date (parent_portfolio_id, as_of_date)
);

CREATE TABLE holdings_lookthrough (
    id                  BIGINT       NOT NULL,
    parent_portfolio_id VARCHAR(64)  NOT NULL,
    portfolio           VARCHAR(128) NOT NULL,
    leaf_holding_name   VARCHAR(255) NOT NULL,
    leaf_holding_ticker VARCHAR(64),
    leaf_holding_isin   VARCHAR(32),
    leaf_holding_cusip  VARCHAR(32),
    holding_type        VARCHAR(64),
    sector              VARCHAR(128),
    country             VARCHAR(128),
    currency            VARCHAR(32)   NOT NULL,
    market_value_usd    DOUBLE       NOT NULL,
    weight_pct          DOUBLE       NOT NULL,
    path_depth          INT          NOT NULL DEFAULT 0,
    as_of_date          VARCHAR(32)  NOT NULL,
    PRIMARY KEY (id),
    KEY ix_lt_parent (parent_portfolio_id),
    KEY ix_lt_portfolio (portfolio),
    KEY ix_lt_isin (leaf_holding_isin),
    KEY ix_lt_parent_date (parent_portfolio_id, as_of_date),
    KEY ix_lt_ticker (leaf_holding_ticker),
    KEY ix_lt_type (holding_type),
    KEY ix_lt_sector (sector),
    KEY ix_lt_country (country)
);

CREATE TABLE holdings_lt_latest (
    id                  BIGINT       NOT NULL,
    parent_portfolio_id VARCHAR(64)  NOT NULL,
    portfolio           VARCHAR(128) NOT NULL,
    leaf_holding_name   VARCHAR(255) NOT NULL,
    leaf_holding_ticker VARCHAR(64),
    leaf_holding_isin   VARCHAR(32),
    leaf_holding_cusip  VARCHAR(32),
    holding_type        VARCHAR(64),
    sector              VARCHAR(128),
    country             VARCHAR(128),
    currency            VARCHAR(32)   NOT NULL,
    market_value_usd    DOUBLE       NOT NULL,
    weight_pct          DOUBLE       NOT NULL,
    path_depth          INT          NOT NULL DEFAULT 0,
    as_of_date          VARCHAR(32)  NOT NULL,
    PRIMARY KEY (id),
    KEY ix_ltl_parent (parent_portfolio_id),
    KEY ix_ltl_ticker (leaf_holding_ticker),
    KEY ix_ltl_isin (leaf_holding_isin),
    KEY ix_ltl_type (holding_type),
    KEY ix_ltl_sector (sector),
    KEY ix_ltl_country (country)
);

CREATE TABLE nav_history (
    portfolio_id        VARCHAR(64)  NOT NULL,
    as_of_date          VARCHAR(32)  NOT NULL,
    nav_per_share       DOUBLE       NOT NULL,
    shares_outstanding  DOUBLE,
    ex_dividends        DOUBLE,
    daily_return_pct    DOUBLE,
    PRIMARY KEY (portfolio_id, as_of_date),
    KEY ix_nav_date (as_of_date)
);

CREATE TABLE distributions (
    portfolio_id        VARCHAR(64)  NOT NULL,
    ex_date             VARCHAR(32)  NOT NULL,
    record_date         VARCHAR(32),
    payable_date        VARCHAR(32),
    total_distribution  DOUBLE       NOT NULL,
    income              DOUBLE,
    st_cap_gains        DOUBLE,
    lt_cap_gains        DOUBLE,
    return_of_capital   DOUBLE,
    PRIMARY KEY (portfolio_id, ex_date),
    KEY ix_dist_ex_date (ex_date)
);

CREATE TABLE fund_documents (
    portfolio_id        VARCHAR(64)  NOT NULL,
    ticker              VARCHAR(64)  NOT NULL,
    slug                VARCHAR(128) NOT NULL,
    label               VARCHAR(255) NOT NULL,
    url                 VARCHAR(512) NOT NULL,
    PRIMARY KEY (portfolio_id, ticker, slug),
    KEY ix_fund_documents_ticker (ticker)
);

CREATE TABLE fund_links (
    parent_portfolio_id VARCHAR(64)  NOT NULL,
    child_portfolio_id  VARCHAR(64)  NOT NULL,
    weight_pct          DOUBLE       NOT NULL,
    as_of_date          VARCHAR(32)  NOT NULL,
    PRIMARY KEY (parent_portfolio_id, child_portfolio_id)
);

CREATE TABLE fx_rates (
    rate_date VARCHAR(32) NOT NULL,
    ccy       VARCHAR(16) NOT NULL,
    eur_rate  DOUBLE      NOT NULL,
    PRIMARY KEY (rate_date, ccy)
);

-- Daily premium/discount to NAV (%), scraped from the iShares chart data.
CREATE TABLE premium_discount_history (
    portfolio_id          VARCHAR(64) NOT NULL,
    as_of_date            VARCHAR(32) NOT NULL,
    premium_discount_pct  DOUBLE,
    PRIMARY KEY (portfolio_id, as_of_date),
    KEY ix_pdh_date (as_of_date)
);

-- Fund + benchmark growth-of-$10,000 series from the product page chart.
CREATE TABLE performance_history (
    portfolio_id     VARCHAR(64) NOT NULL,
    as_of_date       VARCHAR(32) NOT NULL,
    fund_value       DOUBLE,
    benchmark_value  DOUBLE,
    PRIMARY KEY (portfolio_id, as_of_date),
    KEY ix_perf_date (as_of_date)
);
-- Note: currency/report_currency widened to VARCHAR(32): some source rows carry
-- dirty values (e.g. a date string up to 12 chars) in those fields.
