"""Interactive Streamlit dashboard over the perf-results history (CHA-419).

Launch with ``just perf-dashboard`` (``uv run streamlit run
scripts/perf/dashboard.py``). Reads the gitignored SQLite ``measurements`` table
written by ``scripts/perf/results_to_sqlite.py`` and lets you explore the
normalized per-operation metrics (ms/op, ops/s — toggled via the Metric
selector) plus raw elapsed time / throughput over time, filtered by run_id /
type / file / test / params_json / branch / host. The measurements table leads
with run_id, the ms/op reading + unit, and a Penca-vs-Postgres
``pg_delta_pct`` column (the report's "Δ% vs Postgres").

``load_dataframe`` is import-safe (no Streamlit side effects), so the data
access is usable and testable on its own; the ``st.*`` UI only runs when the
module is executed as the Streamlit entry point.

An optional ``--run_id`` (passed after ``--`` to ``streamlit run``, or typed in
the sidebar) switches to a comparison view that overlays that run against the
recorded history via the shared ``comparison`` kernel.
"""

from __future__ import annotations

import argparse
import sqlite3

import pandas as pd
import streamlit as st
from comparison import (
    _SERIES_KEYS,
    compare_run_to_history,
    load_history,
    resolve_run,
    series_label,
)
from metrics import ms_per_op, ops_per_sec, recorded_operations, resolve_operations
from trends import select_present_columns

_DEFAULT_DB = ".perf/perf.db"

# Dimensions offered as sidebar multiselect filters, in display order. run_id
# leads (it's also the leading table column); params_json lets a single
# parametrization be isolated out of a test's matrix.
_FILTER_COLUMNS = (
    "run_id",
    "type",
    "file",
    "test",
    "params_json",
    "branch",
    "hostname",
)

# Explicit projection (never SELECT *) — the UI depends on this column set.
_COLUMNS = [
    "type",
    "file",
    "test",
    "operation",
    "system_state",
    "row_count",
    "result_rows",
    "elapsed_seconds",
    "postgres_baseline_seconds",
    "rows_per_second",
    "operations",
    "unit",
    "params_json",
    "run_id",
    "branch",
    "commit_sha",
    "hostname",
    "ts_utc",
]

# Metrics the Postgres baseline (an elapsed time spanning the same operation
# count) can be converted into for the trend overlay; rows_per_second is
# throughput over a different denominator and stays overlay-less.
_NORMALIZED_METRICS = ("ms_per_op", "ops_per_sec")
_BASELINE_METRICS = (*_NORMALIZED_METRICS, "elapsed_seconds")


def load_dataframe(db_path: str) -> pd.DataFrame:
    """Load the ``measurements`` table into a DataFrame ordered by ``ts_utc``.

    Tolerates a pre-migration DB the same way comparison/trends do: columns
    the DB predates (the CHA-438 work-unit fields) load as NULL — the
    backfill already handles them — instead of failing the whole dashboard
    with a misleading "no measurements" warning.
    """
    conn = sqlite3.connect(db_path)
    try:
        clause, available = select_present_columns(conn, _COLUMNS)
        frame = pd.read_sql_query(
            f"SELECT {clause} FROM measurements ORDER BY ts_utc", conn
        )
    finally:
        conn.close()

    for column in _COLUMNS:
        if column not in available:
            frame[column] = None

    return frame[_COLUMNS]


def _resolved_operations(frame: pd.DataFrame) -> list[int]:
    """Per-row operation counts via the shared backfill policy.

    Every row routes through ``metrics.resolve_operations`` (NULL and
    non-positive stored counts are unrecorded — one parallel pandas policy
    already drifted, so the dict-based helper is THE implementation). The
    fallback is the series' newest positive count when the frame carries
    series identity (rows arrive ts-ordered from ``load_dataframe``); a frame
    without series identity has no backfill source, so unrecorded resolves to
    one operation. Dashboard frames are hundreds of rows — per-row dict
    access costs nothing here.
    """
    rows = frame.to_dict("records")
    fallbacks: dict[str, int] = {}
    if "series" in frame.columns:
        for row in rows:
            operations = recorded_operations(row)
            if operations is not None:
                fallbacks[str(row["series"])] = operations

    return [
        resolve_operations(row, fallbacks.get(str(row.get("series")), 1))
        for row in rows
    ]


def _with_normalized_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    """Add ``ms_per_op`` / ``ops_per_sec`` columns, derived per row from the
    shared ``metrics`` formulas (never a vectorized reimplementation). The
    resolved counts ride along as ``_resolved_ops`` so the baseline overlay
    normalizes with the SAME denominators as the series values — re-resolving
    over a baseline-bearing subset could pick a different fallback."""
    frame = frame.copy()
    resolved = _resolved_operations(frame)
    frame["_resolved_ops"] = resolved
    frame["ms_per_op"] = [
        ms_per_op(elapsed, operations)
        for elapsed, operations in zip(frame["elapsed_seconds"], resolved, strict=True)
    ]
    frame["ops_per_sec"] = [
        ops_per_sec(elapsed, operations)
        for elapsed, operations in zip(frame["elapsed_seconds"], resolved, strict=True)
    ]
    return frame


def _baseline_values(baselines: pd.DataFrame, metric: str) -> list[float]:
    """The Postgres baseline converted into ``metric``'s space, using the
    already-resolved per-row counts (``_resolved_ops``) so it divides by
    exactly what the Penca series beside it divided by."""
    if metric == "elapsed_seconds":
        return list(baselines["postgres_baseline_seconds"])

    formula = ms_per_op if metric == "ms_per_op" else ops_per_sec
    return [
        formula(baseline, operations)
        for baseline, operations in zip(
            baselines["postgres_baseline_seconds"],
            baselines["_resolved_ops"],
            strict=True,
        )
    ]


def build_trend_chart_data(
    frame: pd.DataFrame, metric: str, show_baselines: bool
) -> pd.DataFrame:
    """Pivot the filtered measurements into a ``ts_utc`` × ``series`` chart frame.

    Import-safe (no Streamlit) so the overlay logic is testable on its own. The
    caller supplies ``frame`` with a ``series`` column already built. The
    normalized metrics (``ms_per_op`` / ``ops_per_sec``) are derived here from
    operations + elapsed, with legacy NULL-operations rows backfilled per
    series.

    When ``show_baselines`` is set, add a parallel ``pg_baseline / {series}``
    column per series, sourced from ``postgres_baseline_seconds`` (only where
    a baseline was recorded) and converted into the selected metric's space.
    ``rows_per_second`` gets no overlay — the baseline is elapsed-time over a
    different denominator and isn't comparable.
    """
    if metric in _NORMALIZED_METRICS:
        frame = _with_normalized_metrics(frame)

    chart_data = frame.pivot_table(
        index="ts_utc", columns="series", values=metric, aggfunc="mean"
    )
    if not (show_baselines and metric in _BASELINE_METRICS):
        return chart_data

    baselines = frame[frame["postgres_baseline_seconds"].notna()]
    if baselines.empty:
        return chart_data

    baselines = baselines.copy()
    baselines["pg_metric"] = _baseline_values(baselines, metric)
    baselines["series"] = "pg_baseline / " + baselines["series"]
    baseline_pivot = baselines.pivot_table(
        index="ts_utc",
        columns="series",
        values="pg_metric",
        aggfunc="mean",
    )
    return chart_data.join(baseline_pivot, how="outer")


def build_measurements_table(frame: pd.DataFrame) -> pd.DataFrame:
    """Shape the filtered measurements for the table: derived ms_per_op +
    Penca-vs-Postgres Δ% columns, with ``run_id`` and the normalized reading
    led to the front.

    Import-safe (no Streamlit) so the column math + ordering are testable on
    their own. ``pg_delta_pct`` mirrors the report's "Δ% vs Postgres" (positive
    = Penca slower than Postgres); a row with no recorded baseline — or a
    non-positive one — is NaN ("no baseline"), never ±inf from a zero divide.
    """
    table = _with_normalized_metrics(frame).drop(columns=["_resolved_ops"])
    baseline = table["postgres_baseline_seconds"]
    table["pg_delta_pct"] = (
        ((table["elapsed_seconds"] - baseline) / baseline * 100.0)
        .where(baseline > 0)
        .round(1)
    )
    # run_id leads (each row's run is identifiable at a glance), then the
    # normalized reading + unit, then the raw elapsed / baseline / Δ% trio so
    # the comparison reads without scrolling past identity + context columns.
    lead = [
        "run_id",
        "ms_per_op",
        "unit",
        "elapsed_seconds",
        "postgres_baseline_seconds",
        "pg_delta_pct",
    ]
    rest = [column for column in table.columns if column not in lead]
    return table[lead + rest]


def _series_label(row: pd.Series) -> str:
    # Pivot-column identity for the trend chart: joins ALL of _SERIES_KEYS
    # (incl. type + params_json) so series differing only in parametrization
    # stay distinct columns. Deliberately fuller than comparison.series_label
    # (a 4-field human label for the flat comparison table) — collapsing to it
    # would merge distinct params into one averaged pivot column.
    return " / ".join(str(row[key]) for key in _SERIES_KEYS)


def _requested_run_id() -> str | None:
    """Read an optional ``--run_id`` passed after ``--`` to ``streamlit run``.

    Uses ``parse_known_args`` so Streamlit's own CLI args don't trip parsing.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--run_id", default=None)
    args, _ = parser.parse_known_args()
    return args.run_id


def _render_run_comparison(db_path: str, run_id: str) -> None:
    """Overlay one run against history via the shared ``comparison`` kernel.

    The dashboard resolves the run from the SQLite history by run_id
    (``resolve_run`` with no JSONL — the dashboard has no run file), and compares
    it against the history with that run excluded so it isn't compared against
    itself. Reuses the same ``compare_run_to_history`` the static HTML report
    uses — no parallel diff logic in the dashboard.
    """
    st.subheader(f"Run {run_id} vs history")
    try:
        run_rows = resolve_run(run_id, db_path, json_path=None)
    except ValueError as error:
        st.warning(str(error))
        return

    entries = compare_run_to_history(
        run_rows, load_history(db_path, exclude_run_id=run_id)
    )
    # ms/op columns, matching the space delta_pct is computed in — raw seconds
    # beside a normalized delta would contradict each other whenever the run's
    # operation count differs from legacy history rows.
    table = pd.DataFrame(
        [
            {
                "series": series_label(entry),
                "unit": entry["unit"],
                "run (ms/op)": entry["run_ms_per_op"],
                "history mean (ms/op)": entry["history_mean_ms_per_op"],
                "delta_pct": entry["delta_pct"],
            }
            for entry in entries
        ]
    )
    st.dataframe(table, width="stretch")


def main() -> None:
    st.set_page_config(page_title="Penca perf trends", layout="wide")
    st.title("Penca perf trends")

    db_path = st.sidebar.text_input("SQLite DB", _DEFAULT_DB)

    # Optional run-vs-history comparison: the --run_id CLI flag seeds a sidebar
    # input; when set, show the comparison view instead of the overall trend.
    run_id = st.sidebar.text_input("Compare run_id", _requested_run_id() or "")
    if run_id:
        _render_run_comparison(db_path, run_id)
        return

    try:
        frame = load_dataframe(db_path)
    except (sqlite3.OperationalError, pd.errors.DatabaseError):
        st.warning(f"No measurements at {db_path} — run `just perf-test` first.")
        return

    if frame.empty:
        st.warning("The measurements table is empty — run `just perf-test` first.")
        return

    # Optional filters over run_id, the type/file/test + params identity, and
    # the run-context dimensions.
    for column in _FILTER_COLUMNS:
        options = sorted(frame[column].dropna().unique())
        chosen = st.sidebar.multiselect(column, options)
        if chosen:
            frame = frame[frame[column].isin(chosen)]

    frame = frame.copy()
    frame["series"] = frame.apply(_series_label, axis=1)

    metric = st.sidebar.selectbox(
        "Metric",
        ["ms_per_op", "ops_per_sec", "elapsed_seconds", "rows_per_second"],
        index=0,
    )
    show_baselines = st.sidebar.checkbox("Show Postgres baselines", value=False)

    st.subheader("Trend")
    if show_baselines and metric not in _BASELINE_METRICS:
        st.caption(
            "Postgres baselines overlay on ms_per_op, ops_per_sec, or "
            "elapsed_seconds — not rows_per_second, which isn't comparable."
        )

    chart_data = build_trend_chart_data(frame, metric, show_baselines)
    st.line_chart(chart_data)

    st.subheader("Measurements")
    st.dataframe(build_measurements_table(frame), width="stretch")


if __name__ == "__main__":
    main()
