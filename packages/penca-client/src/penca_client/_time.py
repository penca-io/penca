"""Datetime ↔ microsecond epoch conversion utilities."""

from __future__ import annotations

from datetime import datetime, timezone


def datetime_to_micros(dt: datetime) -> int:
    """Convert a datetime to microseconds since Unix epoch.

    If the datetime is naive (no tzinfo), it is assumed to be UTC.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp() * 1_000_000)


def micros_to_datetime(us: int) -> datetime:
    """Convert microseconds since Unix epoch to a UTC datetime."""
    return datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc)
