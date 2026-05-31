"""US business-day check + walk-back historical holdings fetcher.

Used by the on-demand date picker on the Holdings widget: a user picks a
date, we validate it's a US trading day, fetch the snapshot from BlackRock
(walking back to the nearest valid prior trading day if the requested date
has no published data), and return the served date alongside the rows so
the caller can cache it.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import httpx
import pandas as pd
from pandas.tseries.holiday import (
    GoodFriday,
    USFederalHolidayCalendar,
)
from pandas.tseries.offsets import CustomBusinessDay

from .holdings import FundRef, Holding, fetch_fund_holdings

log = logging.getLogger(__name__)


class _NYSECalendar(USFederalHolidayCalendar):
    """Federal calendar plus Good Friday — close enough to NYSE for our use."""

    rules = [*USFederalHolidayCalendar.rules, GoodFriday]


_NYSE_BDAY = CustomBusinessDay(calendar=_NYSECalendar())


def is_us_business_day(d: date) -> bool:
    """True if ``d`` is a US trading day (Mon–Fri excluding NYSE holidays)."""
    ts = pd.Timestamp(d)
    return bool((ts + 0 * _NYSE_BDAY) == ts)


def previous_business_day(d: date) -> date:
    return (pd.Timestamp(d) - _NYSE_BDAY).date()


def fetch_with_fallback(
    fund: FundRef,
    *,
    template: dict[str, Any],
    client: httpx.Client,
    requested: date,
    max_walk_back: int = 5,
) -> tuple[list[Holding], date] | None:
    """Fetch a historical snapshot, walking back to the nearest valid day.

    Returns ``(holdings, served_date)`` on the first non-empty response, or
    ``None`` if ``max_walk_back`` consecutive prior business days all miss.
    """
    target = requested
    for attempt in range(max_walk_back + 1):
        hs = fetch_fund_holdings(
            fund,
            template=template,
            client=client,
            as_of_date=target,
        )
        if hs:
            log.info(
                "historical fetch %s @ %s → %d rows (attempt %d)",
                fund.ticker,
                target,
                len(hs),
                attempt + 1,
            )
            return hs, target
        target = previous_business_day(target)
    log.info(
        "historical fetch %s exhausted %d walk-back attempts from %s",
        fund.ticker,
        max_walk_back,
        requested,
    )
    return None
