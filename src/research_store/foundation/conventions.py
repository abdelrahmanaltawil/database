from __future__ import annotations

import calendar
from collections.abc import Iterable
from datetime import date, datetime

import numpy as np
import pandas as pd

from research_store.foundation.models import SentinelRule


def valid_calendar_day(year: int, month: int, day: int) -> bool:
    """Drop only arithmetically impossible month-wide cells.

    Invalid year/month values are not swallowed: they indicate corrupt source
    structure and must surface to the caller.
    """

    if not 1 <= month <= 12:
        raise ValueError(f"Invalid month: {month}")
    if year < 1:
        raise ValueError(f"Invalid year: {year}")
    return 1 <= day <= calendar.monthrange(year, month)[1]


def signed_longitude(value: float, source_convention: str) -> float:
    if source_convention == "0_360":
        result = ((float(value) + 180.0) % 360.0) - 180.0
    elif source_convention == "signed":
        result = float(value)
    else:
        raise ValueError(f"Unknown longitude convention: {source_convention!r}")
    if not -180.0 <= result <= 180.0:
        raise ValueError(f"Longitude is outside [-180, 180]: {result}")
    return result


def utc_timestamps(
    values: pd.Series,
    *,
    source_timezone: str,
    ambiguous: str | bool | np.ndarray = "raise",
    nonexistent: str = "raise",
) -> pd.Series:
    """Convert source-local clock readings to aware UTC timestamps."""

    parsed = pd.to_datetime(values, errors="raise")
    if getattr(parsed.dt, "tz", None) is None:
        parsed = parsed.dt.tz_localize(
            source_timezone,
            ambiguous=ambiguous,
            nonexistent=nonexistent,
        )
    return parsed.dt.tz_convert("UTC")


def apply_sentinel(
    raw_value: str,
    observed_at: date | datetime | pd.Timestamp,
    rules: Iterable[SentinelRule],
) -> float | None:
    """Apply exactly one era-aware rule, or parse the value normally."""

    instant = pd.Timestamp(observed_at)

    def boundary(value: str | None) -> pd.Timestamp | None:
        if value is None:
            return None
        result = pd.Timestamp(value)
        if instant.tzinfo is not None and result.tzinfo is None:
            result = result.tz_localize("UTC")
        elif instant.tzinfo is None and result.tzinfo is not None:
            result = result.tz_convert("UTC").tz_localize(None)
        return result

    matching: list[SentinelRule] = []
    marker_rules: list[SentinelRule] = []
    for rule in rules:
        if raw_value != rule.marker:
            continue
        marker_rules.append(rule)
        rule_start = boundary(rule.start)
        rule_end = boundary(rule.end)
        start_ok = rule_start is None or instant >= rule_start
        end_ok = rule_end is None or instant < rule_end
        if start_ok and end_ok:
            matching.append(rule)
    if len(matching) > 1:
        raise ValueError(f"Overlapping sentinel rules for {raw_value!r} at {instant}")
    if matching:
        return matching[0].replacement
    if marker_rules:
        raise ValueError(
            f"Sentinel {raw_value!r} has no era rule covering {instant}; refusing to store it"
        )
    return float(raw_value)
