"""Summarize + graph perf trends from the SQLite history (CHA-419).

Reads the ``measurements`` table written by
``scripts/perf/results_to_sqlite.py`` and produces, per **series** — one
(type, file, test, operation, system_state) tuple tracked over time —
summary stats (run count, latest-vs-previous delta in normalized
ms-per-operation, regression flag) and a trend PNG of ms-per-operation over
runs. "Latest" is by ``ts_utc``, so re-ingested or backfilled runs sort
correctly regardless of insertion order. stdlib + matplotlib (headless).

Usage:
    python scripts/perf/trends.py --db .perf/perf.db --out-dir .perf/graphs
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sqlite3
import sys
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
from metrics import format_ms, ms_per_op, recorded_operations, resolve_operations

# Headless backend — set before any figure is created so this runs in CI / over
# SSH with no display.
matplotlib.use("Agg")

# A run is flagged as a regression when its normalized ms-per-operation is more
# than this many percent slower than the immediately preceding run of the same
# series.
REGRESSION_THRESHOLD_PCT = 5.0

# The tuple that identifies one series tracked over time. params_json is part of
# the identity: two runs that differ only in parametrization are distinct series
# (without it, same-run rows that vary only by params would share a ts_utc and
# couldn't be ordered for the latest-vs-previous comparison).
_SERIES_KEYS = [
    "type",
    "file",
    "test",
    "operation",
    "system_state",
    "params_json",
]


def select_present_columns(
    conn: sqlite3.Connection, columns: list[str]
) -> tuple[str, list[str]]:
    """SELECT clause projecting only the ``columns`` present in this DB.

    A ``.perf/perf.db`` created before a column existed is only migrated when
    an ingest runs (``--record``); read paths must still work against it, so
    absent columns are skipped and defaulted via ``fill_missing_columns``.
    Shared by this module and ``comparison``.
    """
    present = {row[1] for row in conn.execute("PRAGMA table_info(measurements)")}
    available = [name for name in columns if name in present]
    return ", ".join(available), available


def fill_missing_columns(rows: list[dict], columns: list[str]) -> list[dict]:
    """Default any ``columns`` absent from the projection to None."""
    for row in rows:
        for name in columns:
            row.setdefault(name, None)

    return rows


def _has_measurements(db_path: str) -> bool:
    """True if ``db_path`` exists and holds a ``measurements`` table."""
    if not Path(db_path).exists():
        return False

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='measurements'"
        ).fetchone()
    finally:
        conn.close()

    return row is not None


def _load_series(db_path: str) -> dict:
    """Return ``{series_key_tuple: [rows oldest..newest]}`` from the DB.

    A single SQL read ordered by ``ts_utc`` gives chronological order within
    each series. This relies on ``ts_utc`` sorting lexicographically into
    chronological order, which holds because the ingest writes a uniform,
    zero-padded ISO-8601 string with a fixed ``+00:00`` UTC offset.
    """
    columns = [
        *_SERIES_KEYS,
        "elapsed_seconds",
        "rows_per_second",
        "operations",
        "unit",
        "ts_utc",
    ]
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        clause, _ = select_present_columns(conn, columns)
        rows = conn.execute(
            f"SELECT {clause} FROM measurements ORDER BY ts_utc"
        ).fetchall()
    finally:
        conn.close()

    groups: dict = {}
    for record in fill_missing_columns([dict(row) for row in rows], columns):
        key = tuple(record[name] for name in _SERIES_KEYS)
        groups.setdefault(key, []).append(record)

    return groups


def _series_unit(series: list[dict]) -> str | None:
    """The newest non-NULL unit across a series' rows, else None.

    The work-unit fields are code constants per series, so the latest recorded
    value is the right reading for legacy rows that predate the columns.
    """
    for row in reversed(series):
        if row.get("unit") is not None:
            return row["unit"]

    return None


def _series_operations_fallback(series: list[dict]) -> int:
    """The newest usable recorded count, else 1.

    A mis-recorded 0/negative is unrecorded for fallback selection too —
    otherwise one degenerate newest row collapses the whole series' backfill
    to 1 while an older row still holds the real count.
    """
    for row in reversed(series):
        operations = recorded_operations(row)
        if operations is not None:
            return operations

    return 1


def _series_ms_per_ops(series: list[dict]) -> list[float]:
    """Each row's normalized ms/op, backfilling unrecorded operations with the
    series' newest known count (1 for a series that never recorded one)."""
    fallback_ops = _series_operations_fallback(series)
    return [
        ms_per_op(row["elapsed_seconds"], resolve_operations(row, fallback_ops))
        for row in series
    ]


def summarize(db_path: str) -> list:
    """Per-series summary stats, including a latest-vs-previous regression flag.

    The latest-vs-previous comparison is computed in normalized ms-per-operation
    space (CHA-438) so series whose runs differ in recorded operation counts —
    or that straddle the legacy NULL-operations boundary — compare
    like-for-like. Raw elapsed extrema stay alongside.
    """
    summary = []
    for key, series in _load_series(db_path).items():
        elapseds = [row["elapsed_seconds"] for row in series]
        ms_per_ops = _series_ms_per_ops(series)
        latest = series[-1]
        latest_ms = ms_per_ops[-1]
        previous_ms = ms_per_ops[-2] if len(series) > 1 else None

        pct_change = None
        regressed = False
        if previous_ms is not None and previous_ms > 0:
            pct_change = (latest_ms - previous_ms) / previous_ms * 100.0
            regressed = pct_change > REGRESSION_THRESHOLD_PCT

        entry = dict(zip(_SERIES_KEYS, key, strict=True))
        entry.update(
            run_count=len(series),
            latest_elapsed=latest["elapsed_seconds"],
            latest_rows_per_second=latest["rows_per_second"],
            latest_ms_per_op=latest_ms,
            previous_ms_per_op=previous_ms,
            unit=_series_unit(series),
            min_elapsed=min(elapseds),
            max_elapsed=max(elapseds),
            pct_change=pct_change,
            regressed=regressed,
        )
        summary.append(entry)

    return summary


def render_summary_markdown(summary: list) -> str:
    """Render the per-series summary as a GitHub-flavored markdown table."""
    headers = [
        "Type",
        "File",
        "Test",
        "Operation",
        "System State",
        "Unit",
        "Runs",
        "Latest (ms/op)",
        "Δ% vs prev",
        "Regressed",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
    ]
    ordered = sorted(
        summary,
        key=lambda entry: (
            entry["type"],
            entry["file"],
            entry["test"],
            entry["operation"],
            entry["system_state"],
        ),
    )
    for entry in ordered:
        pct = "n/a" if entry["pct_change"] is None else f"{entry['pct_change']:+.1f}%"
        lines.append(
            "| "
            + " | ".join(
                [
                    _md_cell(entry["type"]),
                    _md_cell(entry["file"]),
                    _md_cell(entry["test"]),
                    _md_cell(entry["operation"]),
                    _md_cell(entry["system_state"]),
                    _md_cell(entry["unit"] or ""),
                    str(entry["run_count"]),
                    format_ms(entry["latest_ms_per_op"]),
                    pct,
                    "yes" if entry["regressed"] else "no",
                ]
            )
            + " |"
        )

    return "\n".join(lines)


def _md_cell(value) -> str:
    """Escape pipe so an arbitrary identifier can't break the table layout."""
    return str(value).replace("|", "\\|")


def generate_graphs(db_path: str, out_dir: str) -> list:
    """Write one ms-per-operation trend PNG per series into ``out_dir``."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    written = []
    for key, series in _load_series(db_path).items():
        # params_json is part of the key (folded into the filename hash below)
        # but not the human-readable title/stem.
        type_name, file_name, test_name, operation, system_state, _params_json = key
        timestamps = [row["ts_utc"] for row in series]
        ms_per_ops = _series_ms_per_ops(series)
        unit = _series_unit(series)

        figure, axes = plt.subplots(figsize=(8, 4))
        axes.plot(range(len(ms_per_ops)), ms_per_ops, marker="o")
        axes.set_xticks(range(len(timestamps)))
        axes.set_xticklabels(timestamps, rotation=45, ha="right", fontsize=6)
        axes.set_ylabel(f"ms per {unit}" if unit else "ms/op")
        axes.set_title(f"{file_name}.{test_name}.{operation} [{system_state}]")
        figure.tight_layout()

        # Sanitize each key component so a "/" can't escape the directory, then
        # append a short hash of the raw key so distinct series can never
        # collide on one filename (sanitization alone maps e.g. "a/b" and "a_b"
        # to the same stem).
        parts = [
            re.sub(r"[^0-9A-Za-z._-]", "_", component)
            for component in (type_name, file_name, test_name, operation, system_state)
        ]
        digest = hashlib.sha1("\x00".join(key).encode()).hexdigest()[:8]
        path = out / ("__".join(parts) + "__" + digest + ".png")
        figure.savefig(path)
        plt.close(figure)
        written.append(path)

    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize + graph perf trends.")
    parser.add_argument("--db", required=True, help="path to the SQLite database")
    parser.add_argument(
        "--out-dir",
        default=".perf/graphs",
        help="directory for trend PNGs",
    )
    args = parser.parse_args(argv)

    if not _has_measurements(args.db):
        print(f"[perf] no measurements at {args.db} — run `just perf-test` first")
        return 1

    summary = summarize(args.db)
    print(render_summary_markdown(summary))
    graphs = generate_graphs(args.db, args.out_dir)
    print(f"\n[perf] wrote {len(graphs)} trend graph(s) to {args.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
