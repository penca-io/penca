"""Compare one perf run against the accumulated SQLite history (CHA-423).

The shared kernel behind both the recipe-fired static HTML report
(``render_report.py``) and the interactive Streamlit dashboard (``--run_id``).
A run's measurements come from either the run's JSONL (end-of-run) or the
SQLite ``measurements`` table keyed by ``run_id``; ``resolve_run`` picks the
source with a fixed precedence — SQLite, then JSONL, then error. Series are
identified by ``trends._SERIES_KEYS`` so the grouping matches the rest of the
perf tooling (no parallel series-key definition). stdlib only.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from metrics import ms_per_op, recorded_operations, resolve_operations
from trends import (
    _SERIES_KEYS,
    _has_measurements,
    fill_missing_columns,
    select_present_columns,
)

# Columns pulled for each measurement row (run + history): the series-key fields
# plus the metrics, the work-unit fields, run_id, and ts_utc used for ordering.
_ROW_COLUMNS = [
    *_SERIES_KEYS,
    "row_count",
    "elapsed_seconds",
    "postgres_baseline_seconds",
    "rows_per_second",
    "operations",
    "unit",
    "run_id",
    "ts_utc",
]


def _series_key(row: dict) -> tuple:
    """The (type, file, test, operation, system_state, params_json) identity."""
    return tuple(row[name] for name in _SERIES_KEYS)


def series_label(entry: dict) -> str:
    """Human-readable ``file.test.operation [system_state]`` label for a row /
    comparison entry — shared by the HTML report and the dashboard."""
    return (
        f"{entry['file']}.{entry['test']}.{entry['operation']}"
        f" [{entry['system_state']}]"
    )


def load_run(json_path: str) -> list[dict]:
    """Read a single run's measurements from its JSONL file."""
    rows: list[dict] = []
    with open(json_path) as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                rows.append(json.loads(stripped))

    return rows


def load_run_from_db(db_path: str, run_id: str) -> list[dict]:
    """Return one run's measurement rows from the SQLite history by run_id."""
    if not _has_measurements(db_path):
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        clause, _ = select_present_columns(conn, _ROW_COLUMNS)
        rows = conn.execute(
            f"SELECT {clause} FROM measurements WHERE run_id = ?",
            (run_id,),
        ).fetchall()
    finally:
        conn.close()

    return fill_missing_columns([dict(row) for row in rows], _ROW_COLUMNS)


def resolve_run(run_id: str, db_path: str, json_path: str | None = None) -> list[dict]:
    """Resolve a run's rows by id: SQLite first, then JSONL, else error.

    SQLite wins when the run is in both stores — it is the durable, recorded
    copy. The JSONL fallback lets an unrecorded run still be compared from the
    file it just wrote. Raising keeps a typo'd run_id from silently comparing
    against nothing.
    """
    db_rows = load_run_from_db(db_path, run_id)
    if db_rows:
        return db_rows

    if json_path is not None and Path(json_path).exists():
        json_rows = [row for row in load_run(json_path) if row.get("run_id") == run_id]
        if json_rows:
            return json_rows

    raise ValueError(f"run_id {run_id!r} not found in {db_path} or {json_path}")


def load_history(
    db_path: str, exclude_run_id: str | None = None
) -> dict[tuple, list[dict]]:
    """Group the recorded history into ``{series_key: [rows oldest..newest]}``.

    Ordered by ts_utc (a zero-padded ISO-8601 UTC string sorts chronologically).
    ``exclude_run_id`` drops one run from the baseline so a recorded run is not
    compared against itself. A missing DB / table yields an empty history.
    """
    if not _has_measurements(db_path):
        return {}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        clause, _ = select_present_columns(conn, _ROW_COLUMNS)
        query = f"SELECT {clause} FROM measurements"
        params: tuple = ()
        if exclude_run_id is not None:
            query += " WHERE run_id != ?"
            params = (exclude_run_id,)

        query += " ORDER BY ts_utc"
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    groups: dict[tuple, list[dict]] = {}
    for record in fill_missing_columns([dict(row) for row in rows], _ROW_COLUMNS):
        groups.setdefault(_series_key(record), []).append(record)

    return groups


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _delta_pct(run_value: float, baseline: float | None) -> float | None:
    """Percent change of the run vs a baseline (positive = run is slower).

    Both sides arrive in normalized ms-per-operation space. Shared by the
    history-mean and Postgres-baseline comparisons. A missing baseline (no
    history / no recorded Postgres time) or a non-positive one yields None,
    which both consumers surface as "no-baseline".
    """
    if baseline is None or baseline <= 0:
        return None

    return (run_value - baseline) / baseline * 100.0


def compare_run_to_history(
    run_rows: list[dict], history: dict[tuple, list[dict]]
) -> list[dict]:
    """Per-series comparison of this run against the historical mean.

    All comparisons are computed in normalized ms-per-operation space
    (CHA-438): the run's operation count for the series (a code constant;
    1 for rows that never recorded one) divides both sides, and legacy
    history rows with NULL ``operations`` are backfilled with that same
    count — stored counts win where present. Delta% is ``None`` when the
    series has no history -> "no-baseline".
    """
    by_series: dict[tuple, list[dict]] = {}
    for row in run_rows:
        by_series.setdefault(_series_key(row), []).append(row)

    entries: list[dict] = []
    for key, rows in by_series.items():
        run_elapsed = _mean([row["elapsed_seconds"] for row in rows])
        run_rows_per_second = _mean([row["rows_per_second"] for row in rows])
        run_ops = next(
            (
                operations
                for row in rows
                if (operations := recorded_operations(row)) is not None
            ),
            1,
        )
        unit = next((row["unit"] for row in rows if row.get("unit") is not None), None)
        row_count = next(
            (row["row_count"] for row in rows if row.get("row_count") is not None),
            None,
        )
        run_ms_per_op = ms_per_op(run_elapsed, run_ops)

        history_ms_per_ops = [
            ms_per_op(row["elapsed_seconds"], resolve_operations(row, run_ops))
            for row in history.get(key, [])
        ]
        if history_ms_per_ops:
            history_mean_ms: float | None = _mean(history_ms_per_ops)
            delta_pct: float | None = _delta_pct(run_ms_per_op, history_mean_ms)
        else:
            history_mean_ms = None
            delta_pct = None

        # The Postgres baseline is a per-run, per-measurement value carried on
        # the run's rows (only the gRPC / write / pgbench suites record one), so
        # it comes from THIS run — not from history. It spans the SAME operation
        # count as the run's elapsed, so it normalizes by run_ops. A series
        # whose rows have no baseline yields None ("no-baseline"), mirroring
        # the history column.
        pg_baselines = [
            row["postgres_baseline_seconds"]
            for row in rows
            if row.get("postgres_baseline_seconds") is not None
        ]
        postgres_baseline = _mean(pg_baselines) if pg_baselines else None
        postgres_ms_per_op = (
            None if postgres_baseline is None else ms_per_op(postgres_baseline, run_ops)
        )
        postgres_delta_pct = _delta_pct(run_ms_per_op, postgres_ms_per_op)

        entry = dict(zip(_SERIES_KEYS, key, strict=True))
        entry.update(
            unit=unit,
            row_count=row_count,
            operations=run_ops,
            run_rows_per_second=run_rows_per_second,
            run_ms_per_op=run_ms_per_op,
            history_ms_per_ops=history_ms_per_ops,
            history_mean_ms_per_op=history_mean_ms,
            delta_pct=delta_pct,
            postgres_ms_per_op=postgres_ms_per_op,
            postgres_delta_pct=postgres_delta_pct,
        )
        entries.append(entry)

    return entries
