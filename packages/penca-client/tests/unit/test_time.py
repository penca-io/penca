"""Unit tests for datetime ↔ microsecond-epoch conversion."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from penca_client._time import datetime_to_micros, micros_to_datetime


def test_round_trip_preserves_aware_datetime() -> None:
    dt = datetime(2026, 5, 7, 12, 34, 56, 789012, tzinfo=timezone.utc)
    assert micros_to_datetime(datetime_to_micros(dt)) == dt


def test_naive_datetime_assumed_utc() -> None:
    naive = datetime(2026, 5, 7, 12, 0, 0)
    aware = naive.replace(tzinfo=timezone.utc)
    assert datetime_to_micros(naive) == datetime_to_micros(aware)


def test_non_utc_offset_normalized_to_utc() -> None:
    """An offset-aware datetime in a non-UTC zone must serialize to the
    same micros as its UTC equivalent — the wire format is offset-naive."""
    plus_two = timezone(timedelta(hours=2))
    dt_local = datetime(2026, 5, 7, 14, 0, 0, tzinfo=plus_two)
    dt_utc = datetime(2026, 5, 7, 12, 0, 0, tzinfo=timezone.utc)
    assert datetime_to_micros(dt_local) == datetime_to_micros(dt_utc)


def test_micros_to_datetime_returns_utc_aware() -> None:
    dt = micros_to_datetime(0)
    assert dt.tzinfo is timezone.utc
    assert dt == datetime(1970, 1, 1, tzinfo=timezone.utc)
