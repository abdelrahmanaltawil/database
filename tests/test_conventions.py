from __future__ import annotations

import pandas as pd
import pytest

from research_store.foundation.conventions import (
    apply_sentinel,
    signed_longitude,
    utc_timestamps,
    valid_calendar_day,
)
from research_store.foundation.models import SentinelRule


def test_sentinel_meaning_changes_at_era_boundary() -> None:
    rules = (
        SentinelRule(
            marker="-9999.9",
            meaning="missing",
            replacement=None,
            end="2020-01-01",
            evidence="publisher data dictionary, pre-2020 edition",
        ),
        SentinelRule(
            marker="-9999.9",
            meaning="measured_zero",
            replacement=0.0,
            start="2020-01-01",
            evidence="publisher data dictionary, 2020 edition",
        ),
    )
    assert apply_sentinel("-9999.9", "2019-12-31T23:00:00Z", rules) is None
    assert apply_sentinel("-9999.9", "2020-01-01T00:00:00Z", rules) == 0.0
    assert apply_sentinel("12.5", "2020-01-01", rules) == 12.5


def test_uncovered_sentinel_era_fails_loudly() -> None:
    rules = (
        SentinelRule(
            "-9", "missing", None, start="2010", end="2015", evidence="manual"
        ),
    )
    with pytest.raises(ValueError, match="no era rule"):
        apply_sentinel("-9", "2020", rules)


def test_impossible_month_wide_dates_are_dropped_arithmetically() -> None:
    assert valid_calendar_day(2024, 2, 29)
    assert not valid_calendar_day(2023, 2, 29)
    assert not valid_calendar_day(2024, 2, 30)
    with pytest.raises(ValueError, match="Invalid month"):
        valid_calendar_day(2024, 13, 1)


def test_longitude_convention_is_explicit() -> None:
    assert signed_longitude(350.0, "0_360") == -10.0
    assert signed_longitude(-79.9, "signed") == -79.9
    with pytest.raises(ValueError, match="Unknown longitude"):
        signed_longitude(10, "guess")


def test_dst_ambiguity_is_not_guessed() -> None:
    values = pd.Series(["2024-11-03 01:30:00"])
    with pytest.raises(Exception, match="Cannot infer dst time"):
        utc_timestamps(values, source_timezone="America/Toronto")
