"""Parse the 'Key Facts' metrics embedded in an iShares product page.

The product page carries the Key Facts as HTML-entity-encoded JSON objects
of the form ``{"label": "...", ..., "value": ...}``.  The available labels
vary by asset class (equity / fixed income / commodity); we map the known
ones to ``funds`` columns and ignore the rest.

``parse_key_facts(html)`` returns ``{column_name: value}`` with numeric
values coerced to ``float`` / ``int`` and string values left as-is.
"""

from __future__ import annotations

import html as _html
import re

# Normalised label (lower-cased, trailing ':' stripped) -> (column, kind).
# kind: "num" float, "int" integer count, "str" verbatim string.
_LABELS: dict[str, tuple[str, str]] = {
    "expense ratio": ("expense_ratio_pct", "num"),
    "management fee": ("management_fee_pct", "num"),
    "acquired fund fees and expenses": ("acquired_fund_fees_pct", "num"),
    "other expenses": ("other_expenses_pct", "num"),
    "sponsor fee": ("sponsor_fee_pct", "num"),
    "closing price": ("closing_price", "num"),
    "mid-point price": ("mid_point_price", "num"),
    "daily volume": ("daily_volume", "int"),
    "30 day avg. volume": ("avg_volume_30d", "int"),
    "30 day median bid/ask spread": ("median_bid_ask_spread_30d_pct", "num"),
    "cusip": ("cusip", "str"),
    "exchange": ("exchange", "str"),
    "benchmark index": ("benchmark_index", "str"),
    "reference benchmark": ("benchmark_index", "str"),
    "bloomberg index ticker": ("bloomberg_index_ticker", "str"),
    "shares outstanding": ("shares_outstanding", "int"),
    "net assets of fund": ("net_assets_usd", "num"),
    "premium/discount": ("premium_discount_pct", "num"),
    "equity beta (3y)": ("equity_beta_3y", "num"),
    "standard deviation (3y)": ("std_dev_3y_pct", "num"),
    "number of holdings": ("number_of_holdings", "int"),
    "distribution frequency": ("distribution_frequency", "str"),
    "30 day sec yield": ("sec_yield_30d_pct", "num"),
    "12m trailing yield": ("twelve_month_yield_pct", "num"),
    "p/e ratio": ("pe_ratio", "num"),
    "p/b ratio": ("pb_ratio", "num"),
    "effective duration": ("effective_duration", "num"),
    "convexity": ("convexity", "num"),
    "average yield to maturity": ("avg_ytm_pct", "num"),
    "option adjusted spread": ("option_adjusted_spread_bps", "num"),
    "weighted avg coupon": ("weighted_avg_coupon_pct", "num"),
    "weighted avg maturity": ("weighted_avg_maturity_yrs", "num"),
    "ounces in trust": ("ounces_in_trust", "num"),
    "tonnes in trust": ("tonnes_in_trust", "num"),
    "basket amount": ("basket_amount", "num"),
    "indicative basket amount": ("indicative_basket_amount", "num"),
}

# Every column this module can populate (used by the schema + ingest).
KEY_FACT_COLUMNS: list[str] = sorted({c for c, _ in _LABELS.values()})

_PAIR = re.compile(
    r'"label":"([^"]{2,40})"(?:[^{}]*?)"value":((?:"[^"]*")|[-0-9.eE]+|null)'
)


def parse_key_facts(html_text: str) -> dict[str, object]:
    txt = _html.unescape(html_text)
    out: dict[str, object] = {}
    for label, val in _PAIR.findall(txt):
        norm = label.strip().rstrip(":").strip().lower()
        # "<metric> as of" carries the as-of date, not the value — skip.
        if norm.endswith(" as of"):
            continue
        mapping = _LABELS.get(norm)
        if not mapping or val == "null":
            continue
        col, kind = mapping
        if col in out:  # first occurrence wins
            continue
        s = val[1:-1] if val.startswith('"') else val
        if kind in ("num", "int"):
            try:
                f = float(s.replace(",", ""))
            except ValueError:
                continue
            out[col] = int(f) if kind == "int" else f
        else:
            s = s.strip()
            if s and s not in ("-", "—"):
                out[col] = s
    return out
