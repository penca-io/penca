"""Unit tests pinning ``scripts/telemetry/spanlog.py`` parsing primitives.

``spanlog.parse_close`` is the load-bearing parser both telemetry span
tables import (CHA-417); these pin the Penca-owned parsing logic —
including the three behavior fixes the consolidation made over the old
per-script parsers — so a future "simplify back to a regex" edit can't
silently re-introduce parser skew:

- bare final chain segments are counted (depth was undercounted before),
- adjacent bare span names are both kept (the old regex consumed the
  separating colon and skipped every other name),
- ``ns``-unit closes parse instead of being dropped.

Lives in ``tests/static`` because it needs no Docker and no services —
pure functions over literal lines. Runs under ``just static-test
spanlog`` and ``just check``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

REPO = Path(__file__).parents[2]

_spec = importlib.util.spec_from_file_location(
    "spanlog", REPO / "scripts" / "telemetry" / "spanlog.py"
)
assert _spec is not None and _spec.loader is not None
spanlog = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(spanlog)


def test_full_close_line_with_idle():
    line = (
        "2026-06-10T20:09:59.052289Z DEBUG ipc_encode{batches=2 rows=1 bytes=1488}: "
        "penca_server_grpc::ipc: close time.busy=1.45ms time.idle=53.7ms"
    )
    parsed = spanlog.parse_close(line)
    assert parsed is not None
    assert parsed["ts"] == "2026-06-10T20:09:59.052289Z"
    assert parsed["names"] == ["ipc_encode"]
    assert parsed["target"] == "penca_server_grpc::ipc"
    assert parsed["busy_ms"] == 1.45
    assert parsed["idle_ms"] == 53.7


def test_close_line_without_idle_is_zero():
    line = "2026-06-10T20:00:00.000000Z INFO connect: penca_db::driver::pg: close time.busy=1.83ms"
    parsed = spanlog.parse_close(line)
    assert parsed is not None
    assert parsed["idle_ms"] == 0.0


def test_all_four_units_convert_to_ms():
    for unit, expected in [("ns", 2e-6), ("µs", 2e-3), ("ms", 2.0), ("s", 2e3)]:
        line = f"t DEBUG s: tgt: close time.busy=2{unit}"
        parsed = spanlog.parse_close(line)
        assert parsed is not None, unit
        assert parsed["busy_ms"] == expected, unit


def test_ansi_colored_line_parses():
    line = (
        "\x1b[2m2026-06-10T20:09:59.052289Z\x1b[0m \x1b[34mDEBUG\x1b[0m "
        "\x1b[1mipc_encode\x1b[0m\x1b[2m:\x1b[0m \x1b[2mpenca_server_grpc::ipc\x1b[2m:\x1b[0m "
        "close \x1b[3mtime.busy\x1b[2m=\x1b[0m1.45ms \x1b[3mtime.idle\x1b[2m=\x1b[0m53.7ms"
    )
    parsed = spanlog.parse_close(line)
    assert parsed is not None
    assert parsed["names"] == ["ipc_encode"]
    assert parsed["busy_ms"] == 1.45


def test_colons_inside_brace_fields_do_not_split_segments():
    chain = 'read_data:execute{db.statement=SELECT a::bigint FROM "t" WHERE x = $1}'
    assert spanlog.span_names(chain) == ["read_data", "execute"]


def test_bare_final_segment_is_counted():
    # The old SPAN regex required a trailing colon or braces and dropped a
    # bare final segment, undercounting depth.
    assert spanlog.span_names("outer{f=1}:inner") == ["outer", "inner"]


def test_adjacent_bare_names_are_all_kept():
    # The old regex consumed the separating colon, skipping every other
    # bare name (`a:b:c:` lost `b`).
    assert spanlog.span_names("a:b:c:") == ["a", "b", "c"]


def test_non_close_lines_return_none():
    assert (
        spanlog.parse_close("2026-06-10T20:00:00Z INFO read_data: tgt: enter") is None
    )
    assert spanlog.parse_close("garbage") is None

    # Passes the cheap `time.busy=` pre-filter but CLOSE_RE rejects it
    # (no `close` keyword) — must fall through to None, not crash.
    assert spanlog.parse_close("t DEBUG s: tgt: time.busy=1.0ms") is None
