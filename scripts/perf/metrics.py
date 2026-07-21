"""Derived perf metrics shared by the report, trends, and dashboard (CHA-438).

A measurement row records ``elapsed_seconds`` spanning ``operations``
repetitions of one ``unit`` (see ``tests/performance/performance_helpers.py``
``PerfResult``). This module is the single home for everything derived from
that: per-operation normalization (``ms_per_op`` / ``ops_per_sec``), the
legacy-row backfill rule (``resolve_operations``), human formatting, and the
headline-number selection the HTML report leads with. It sits below
``comparison`` and ``trends`` (which both import it) so no consumer ever
reimplements a formula. stdlib only.
"""

from __future__ import annotations

# The externally-quotable numbers the report leads with, in display order.
# ``operation`` matches the recorded operation name; ``kind`` picks the value
# shape (latency -> ms/unit, throughput -> rows/s + total time at scale);
# ``per_state`` emits one row per system_state present (point reads read very
# differently per tier). Operations absent from a run are skipped — a scoped
# run renders only the headlines it measured.
HEADLINES = [
    {
        "title": "Point lookup",
        "operation": "oltp_point_read",
        "per_state": True,
        "kind": "latency",
    },
    {
        "title": "Point lookup (ids pushdown)",
        "operation": "oltp_point_read_ids",
        "per_state": True,
        "kind": "latency",
    },
    {"title": "Single-row insert", "operation": "oltp_insert", "kind": "latency"},
    {"title": "Full scan", "operation": "olap_full_scan", "kind": "throughput"},
    {"title": "Filtered scan", "operation": "olap_filtered_scan", "kind": "throughput"},
    {"title": "Bulk write", "operation": "write_empty_table", "kind": "throughput"},
    {"title": "TPC-B transaction mix", "operation": "pgbench_tpcb", "kind": "latency"},
    {"title": "Bulk load (pgbench)", "operation": "pgbench_load", "kind": "throughput"},
    {"title": "Persist (hot to cold)", "operation": "persist", "kind": "throughput"},
    {"title": "Snapshot", "operation": "snapshot", "kind": "throughput"},
]


def recorded_operations(row: dict) -> int | None:
    """The row's usable recorded count: positive, else None.

    NULL, an absent key, a pandas NaN, and a zero/negative mis-recording all
    read as unrecorded. This predicate is the one spelling of "is this count
    usable" — both per-row resolution and every fallback selector go through
    it, so the rule can't drift between consumers. The int() cast makes the
    annotation unconditionally true (a NaN-bearing pandas column delivers
    counts as floats like 100.0; counts are integral by construction).
    """
    operations = row.get("operations")
    if operations and operations > 0:
        return int(operations)

    return None


def resolve_operations(row: dict, fallback: int) -> int:
    """A row's recorded operation count, or ``fallback`` for legacy rows.

    Stored operations win; ``fallback`` is the count carried by newer rows of
    the SAME series (the count is a code constant per series), never the row's
    ``row_count`` — for scan-shaped series row_count is rows touched, not
    operations performed.
    """
    operations = recorded_operations(row)
    return operations if operations is not None else fallback


def ms_per_op(elapsed_seconds: float, operations: int) -> float:
    """The one per-operation normalization formula, in milliseconds."""
    return elapsed_seconds / operations * 1000.0


def ops_per_sec(elapsed_seconds: float, operations: int) -> float:
    """Operation rate; 0.0 on non-positive elapsed (mirrors rows_per_second)."""
    if elapsed_seconds <= 0:
        return 0.0

    return operations / elapsed_seconds


def format_ms(value_ms: float) -> str:
    """Human-readable duration from milliseconds: '2.00 s', '500 ms',
    '52.3 ms', '0.060 ms'.

    Band selection rounds the same way the band's format does, so a value
    whose display rounds across the boundary promotes into the next band —
    999.9 reads '1.00 s', never '1000 ms'.
    """
    if round(value_ms) >= 1000:
        return f"{value_ms / 1000:.2f} s"

    if value_ms >= 100:
        return f"{value_ms:.0f} ms"

    if value_ms >= 1:
        return f"{value_ms:.1f} ms"

    return f"{value_ms:.3f} ms"


def format_rate(value_per_sec: float) -> str:
    """Human-readable rate magnitude: '5.0M', '48k', '19.1'.

    Same round-into-band rule as ``format_ms``: 999_999/s reads '1.0M',
    never '1000k'.
    """
    if round(value_per_sec / 1_000) >= 1_000:
        return f"{value_per_sec / 1_000_000:.1f}M"

    if round(value_per_sec) >= 1_000:
        return f"{value_per_sec / 1_000:.0f}k"

    return f"{value_per_sec:.1f}"


def format_count(count: int) -> str:
    """Human-readable count at scale-marker altitude: '1M', '100k', '500'
    (100_011 reads as '100k', not '100.011k' — these label the workload's
    order of magnitude, not its exact size). Round-into-band: 999_999 reads
    '1M', never '1000k'."""
    if round(count / 1_000) >= 1_000:
        return f"{round(count / 1_000_000, 1):g}M"

    if count >= 1_000:
        return f"{count / 1_000:.0f}k"

    return str(count)


def _unit_suffix(entry: dict) -> str:
    """'/query'-style suffix; empty for rows with no recorded unit (never
    '/None')."""
    unit = entry.get("unit")
    return f"/{unit}" if unit else ""


def _latency_values(entry: dict) -> tuple[str, str | None]:
    penca = format_ms(entry["run_ms_per_op"]) + _unit_suffix(entry)
    postgres_ms = entry.get("postgres_ms_per_op")
    postgres = (
        None if postgres_ms is None else format_ms(postgres_ms) + _unit_suffix(entry)
    )
    return penca, postgres


def _throughput_values(entry: dict) -> tuple[str, str | None]:
    # The scale fragment is omitted when no run row recorded a row_count (a
    # pre-row_count-column DB) — mirroring _unit_suffix's never-'/None' rule.
    scale = (
        ""
        if entry.get("row_count") is None
        else f" @ {format_count(entry['row_count'])} rows"
    )
    penca = (
        f"{format_rate(entry['run_rows_per_second'])} rows/s"
        f" ({format_ms(entry['run_ms_per_op'])}{scale})"
    )
    postgres_ms = entry.get("postgres_ms_per_op")
    postgres = None if postgres_ms is None else format_ms(postgres_ms)
    return penca, postgres


def select_headlines(entries: list[dict]) -> list[dict]:
    """Pick the headline rows out of comparison entries.

    Returns ``[{title, penca, postgres}, ...]`` in HEADLINES order; per-state
    specs emit one row per system_state, other specs pick the largest scale
    (max row_count) when a run measured several.
    """
    rows: list[dict] = []
    for spec in HEADLINES:
        matches = [e for e in entries if e["operation"] == spec["operation"]]
        if not matches:
            continue

        if spec.get("per_state"):
            chosen = sorted(matches, key=lambda e: e["system_state"])
        else:
            chosen = [max(matches, key=lambda e: e["row_count"] or 0)]

        for entry in chosen:
            title = spec["title"]
            if spec.get("per_state"):
                title = f"{title} [{entry['system_state']}]"

            if spec["kind"] == "latency":
                penca, postgres = _latency_values(entry)
            else:
                penca, postgres = _throughput_values(entry)

            rows.append({"title": title, "penca": penca, "postgres": postgres})

    return rows
