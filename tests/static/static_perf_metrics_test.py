"""Static checks for the shared perf-metrics helpers (CHA-438).

``scripts/perf/metrics.py`` is the single home for per-operation
normalization, the legacy-row backfill predicate, and the human formatting
the report/trends/dashboard render. The formatters are pure primitives, so
their band boundaries (1 / 100 / 1000 / 1e6) are pinned exhaustively here —
including the round-into-band rule at each promotion edge (999.9 ms is
'1.00 s', never '1000 ms'). Loaded off the scripts/perf sys.path entry the
way the consumers resolve it. No Docker. Runs under ``just static-test
perf_metrics`` and ``just check``.
"""

from __future__ import annotations

import importlib
import math
import sys
from pathlib import Path

import pytest

SCRIPTS_PERF = Path(__file__).parents[2] / "scripts/perf"
if str(SCRIPTS_PERF) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_PERF))

metrics = importlib.import_module("metrics")


@pytest.mark.parametrize(
    ("value_ms", "expected"),
    [
        (0.06, "0.060 ms"),
        (0.9, "0.900 ms"),
        (1.0, "1.0 ms"),
        (52.3, "52.3 ms"),
        (99.9, "99.9 ms"),
        (100.0, "100 ms"),
        (500.0, "500 ms"),
        (999.4, "999 ms"),
        # Round-into-band: the .0f display would read "1000 ms" — promote.
        (999.9, "1.00 s"),
        (1000.0, "1.00 s"),
        (2000.0, "2.00 s"),
        (11482.0, "11.48 s"),
    ],
)
def test_format_ms_bands(value_ms, expected):
    assert metrics.format_ms(value_ms) == expected


@pytest.mark.parametrize(
    ("value_per_sec", "expected"),
    [
        (0.0, "0.0"),
        (19.12, "19.1"),
        (999.4, "999.4"),
        # Round-into-band: 999.6 displays as 1k, not "999.6".
        (999.6, "1k"),
        (1000.0, "1k"),
        (48_191.0, "48k"),
        (999_499.0, "999k"),
        # Round-into-band: the k display would read "1000k" — promote.
        (999_999.0, "1.0M"),
        (1_000_000.0, "1.0M"),
        (5_000_000.0, "5.0M"),
        (33_465_007.0, "33.5M"),
    ],
)
def test_format_rate_bands(value_per_sec, expected):
    assert metrics.format_rate(value_per_sec) == expected


@pytest.mark.parametrize(
    ("count", "expected"),
    [
        (100, "100"),
        (999, "999"),
        (1_000, "1k"),
        (100_011, "100k"),
        (999_499, "999k"),
        # Round-into-band: 999_999 is the 1M scale marker, not "1000k".
        (999_999, "1M"),
        (1_000_000, "1M"),
        (1_500_000, "1.5M"),
    ],
)
def test_format_count_bands(count, expected):
    assert metrics.format_count(count) == expected


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"operations": 100}, 100),
        ({"operations": 1}, 1),
        ({"operations": 0}, None),
        ({"operations": -5}, None),
        ({"operations": None}, None),
        ({}, None),
        ({"operations": math.nan}, None),
        # pandas float passthrough is normalized to int inside the predicate.
        ({"operations": 100.0}, 100),
    ],
)
def test_recorded_operations_usable_predicate(row, expected):
    assert metrics.recorded_operations(row) == expected
