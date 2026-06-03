"""Parse iShares chart-data series so they can be persisted on ingest.

Two datasets are captured:

* **premium/discount** daily series — from the ``dataType=premiumDiscount``
  ajax download.
* **performance / benchmark** growth series ("growth of $10,000") — embedded
  in the product page HTML (the same page the ingest already downloads).

Each series object lists its arrays in a fixed order: ``asOfDate``,
``formattedAsOfDate``, ``formattedValue``, ``value``.  We read ``asOfDate``
(YYYYMMDD ints) and the real ``value`` array.  (The app previously grabbed
the *first* array after the series name — ``asOfDate`` — which is why charts
rendered date integers as the y-values.)
"""

from __future__ import annotations

import html as _html
import re


def pd_ajax_url(product_page_url: str | None) -> str | None:
    """Build the premium/discount ajax URL from a product page URL."""
    page = (product_page_url or "").strip()
    if not page:
        return None
    page = page.replace("/individual/products/", "/products/")
    if not page.startswith("http"):
        page = "https://www.ishares.com" + page
    if "?" in page:
        page = page.split("?")[0]
    return page + "/1467271812596.ajax?fileType=csv&dataType=premiumDiscount"


def _array_after(decoded: str, key: str, start: int) -> str | None:
    """Return the raw contents of the first ``key[...]`` array at/after
    ``start`` (window-free, so it works for very long arrays)."""
    i = decoded.find(key, start)
    if i < 0:
        return None
    lb = decoded.find("[", i)
    rb = decoded.find("]", lb)
    if lb < 0 or rb < 0:
        return None
    return decoded[lb + 1 : rb]


def _ymd_to_iso(ymd: str) -> str | None:
    ymd = ymd.strip().strip('"')
    if len(ymd) != 8 or not ymd.isdigit():
        return None
    return f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"


def _to_float(s: str) -> float | None:
    s = s.strip().strip('"')
    if not s or s.lower() in ("null", "--", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _series(decoded: str, name: str) -> list[tuple[str, float]]:
    """Return ``[(iso_date, value), ...]`` for a named chart series."""
    i = decoded.find(f'"name":"{name}"')
    if i < 0:
        return []
    raw_dates = _array_after(decoded, '"asOfDate":[', i)
    raw_vals = _array_after(decoded, '"value":[', i)
    if not raw_dates or not raw_vals:
        return []
    dates = [_ymd_to_iso(x) for x in raw_dates.split(",")]
    vals = [_to_float(x) for x in raw_vals.split(",")]
    out = []
    for d, v in zip(dates, vals):
        if d is not None and v is not None:
            out.append((d, v))
    return out


def parse_premium_discount(text: str) -> list[tuple[str, float]]:
    """Daily premium/discount percentages: ``[(iso_date, pct), ...]``."""
    decoded = _html.unescape(text)
    if '"premium-discount-chart"' not in decoded:
        return []
    return _series(decoded, "premiumDiscountChartData")


def parse_performance(text: str) -> dict:
    """Fund + benchmark growth series from the product page.

    Returns ``{"fund": [(iso, val)], "benchmark": [(iso, val)],
    "benchmark_name": str|None}`` (growth of a hypothetical $10,000).
    """
    decoded = _html.unescape(text)
    fund = _series(decoded, "performanceData")
    benchmark = _series(decoded, "benchmarkData")
    # The benchmark label is its own object: "benchmarkName":{...,"value":"S&P 500 Index (USD)"}
    bm_name = None
    nm = re.search(r'"benchmarkName":\{[^{}]*?"value":"([^"]+)"', decoded)
    if nm:
        bm_name = nm.group(1)
    return {"fund": fund, "benchmark": benchmark, "benchmark_name": bm_name}
