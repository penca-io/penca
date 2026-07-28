"""Static wiring checks for the streamlit perf dashboard (CHA-419, stretch).

The dashboard is the optional interactive surface over the SQLite history:
``streamlit run scripts/perf/dashboard.py``. These structural guards pin that
streamlit/pandas are *dev* dependencies (scoped to the dependency group, so they
can't pass as runtime deps — they're heavy and dashboard-only), the
``perf-dashboard`` recipe exists, and the script exposes a
``load_dataframe(db_path)`` data accessor distinct from the streamlit UI entry
point. No Docker, no penca services. Runs under ``just static-test
perf_dashboard`` and ``just check``.
"""

from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).parents[2]

SCRIPTS_PERF = REPO / "scripts/perf"
if str(SCRIPTS_PERF) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_PERF))

# Imported off the scripts/perf sys.path entry the way `streamlit run` resolves
# it — same convention as the report/comparison static tests.
dashboard = importlib.import_module("dashboard")


def _read(rel: str) -> str:
    return (REPO / rel).read_text()


def _trend_frame() -> pd.DataFrame:
    """Two series (one with a Postgres baseline, one without) across two runs."""
    return pd.DataFrame(
        [
            {
                "ts_utc": "2026-06-09T00:00:00+00:00",
                "series": "olap.full_scan",
                "elapsed_seconds": 0.07,
                "rows_per_second": 100.0,
                "postgres_baseline_seconds": 0.04,
            },
            {
                "ts_utc": "2026-06-10T00:00:00+00:00",
                "series": "olap.full_scan",
                "elapsed_seconds": 0.08,
                "rows_per_second": 110.0,
                "postgres_baseline_seconds": 0.05,
            },
            {
                "ts_utc": "2026-06-09T00:00:00+00:00",
                "series": "lifecycle.persist",
                "elapsed_seconds": 0.50,
                "rows_per_second": 10.0,
                "postgres_baseline_seconds": None,
            },
        ]
    )


def _toml_section(toml_text: str, header: str) -> str:
    """Return the lines of the ``[header]`` table up to the next ``[`` section."""
    marker = f"[{header}]"
    start = toml_text.find(marker)
    assert start != -1, f"missing {marker} in pyproject.toml"
    rest = toml_text[start + len(marker) :]
    end = rest.find("\n[")
    return rest if end == -1 else rest[:end]


def test_the_dashboards_deps_are_declared_where_they_belong():
    """streamlit stays dev-only; pandas is now a real runtime dependency.

    The dashboard is a dev tool, so streamlit must not ship. pandas backs it
    too, but `examples/` also print through `to_pandas().to_markdown()`, so it
    moved to the root project's dependencies (CHA-517) — it was previously
    declared in both places, which meant the fresh-clone CI job could not
    actually pin it: a plain `uv sync` installs the default dev group as well,
    so the dev copy masked a missing runtime declaration.
    """
    pyproject = _read("pyproject.toml")
    dev = _toml_section(pyproject, "dependency-groups")
    runtime = _toml_section(pyproject, "project")

    assert "streamlit" in dev, "the dashboard is a dev tool and must not ship"
    assert "streamlit" not in runtime
    assert "pandas" in runtime, (
        "examples/ import pandas at runtime, so it belongs in dependencies"
    )
    assert "pandas" not in dev, (
        "declared twice, the dev copy masks a missing runtime declaration"
    )


def test_perf_dashboard_recipe_exists():
    justfile = _read("Justfile")
    assert "perf-dashboard" in justfile
    assert "streamlit run scripts/perf/dashboard.py" in justfile


def test_dashboard_script_exposes_load_dataframe_with_db_path():
    text = _read("scripts/perf/dashboard.py")
    # Pin the db_path parameter the data accessor takes (not just the name).
    assert "def load_dataframe(db_path" in text


def test_dashboard_supports_run_id_comparison_via_shared_helper():
    text = _read("scripts/perf/dashboard.py")
    # The optional --run_id comparison overlays one run against history using
    # the shared comparison kernel (not a reimplemented diff in the dashboard).
    # Pin the CLI flag (not the bare "run_id" token, which already appears as a
    # _COLUMNS entry) so this gates the actual comparison code path.
    assert "--run_id" in text
    assert "comparison" in text
    assert "compare_run_to_history" in text


def test_show_baselines_off_leaves_only_penca_series():
    chart = dashboard.build_trend_chart_data(
        _trend_frame(), "elapsed_seconds", show_baselines=False
    )
    # No pg_baseline overlay when the toggle is off — just the Penca series.
    assert not any(str(col).startswith("pg_baseline") for col in chart.columns)
    assert "olap.full_scan" in chart.columns


def test_show_baselines_adds_pg_series_only_where_recorded():
    chart = dashboard.build_trend_chart_data(
        _trend_frame(), "elapsed_seconds", show_baselines=True
    )
    # The series WITH a baseline gets a parallel pg_baseline column carrying the
    # postgres_baseline_seconds values...
    assert "pg_baseline / olap.full_scan" in chart.columns
    assert chart["pg_baseline / olap.full_scan"].dropna().tolist() == [0.04, 0.05]
    # ...while the series with no recorded baseline gets none.
    assert "pg_baseline / lifecycle.persist" not in chart.columns


def test_show_baselines_ignored_for_rows_per_second():
    # The baseline is elapsed-time (s); it is not comparable to throughput, so
    # the overlay is suppressed for the rows_per_second metric.
    chart = dashboard.build_trend_chart_data(
        _trend_frame(), "rows_per_second", show_baselines=True
    )
    assert not any(str(col).startswith("pg_baseline") for col in chart.columns)


def _measurements_frame() -> pd.DataFrame:
    """Two rows: a 100-op measurement with a Postgres baseline, and a legacy
    row (NULL operations/unit) without one — the shape a migrated pre-CHA-438
    DB loads as."""
    return pd.DataFrame(
        [
            {
                "run_id": "run-a",
                "elapsed_seconds": 0.08,
                "postgres_baseline_seconds": 0.04,
                "rows_per_second": 100.0,
                "operations": 100,
                "unit": "query",
                "params_json": '{"num_rows": 1000}',
            },
            {
                "run_id": "run-a",
                "elapsed_seconds": 0.50,
                "postgres_baseline_seconds": None,
                "rows_per_second": 10.0,
                "operations": None,
                "unit": None,
                "params_json": "{}",
            },
        ]
    )


def test_measurements_table_leads_with_run_id_and_pg_delta():
    table = dashboard.build_measurements_table(_measurements_frame())
    # run_id first, then the normalized reading + unit, then the raw elapsed /
    # baseline / Δ% trio (CHA-438 lead order).
    assert list(table.columns[:6]) == [
        "run_id",
        "ms_per_op",
        "unit",
        "elapsed_seconds",
        "postgres_baseline_seconds",
        "pg_delta_pct",
    ]
    # Δ% mirrors the report: (0.08 - 0.04) / 0.04 * 100 = +100% (Penca slower).
    assert table["pg_delta_pct"].iloc[0] == 100.0
    # Every original column is preserved (the reorder drops nothing).
    assert set(_measurements_frame().columns).issubset(set(table.columns))


def test_measurements_table_pg_delta_is_nan_without_baseline():
    table = dashboard.build_measurements_table(_measurements_frame())
    # A row with no recorded baseline is NaN ("no baseline"), never ±inf.
    assert pd.isna(table["pg_delta_pct"].iloc[1])


def _oltp_trend_frame() -> pd.DataFrame:
    """One point-read series (100 ops per measurement) across three runs; the
    oldest is a legacy row (NULL operations) that must be backfilled from the
    series' known count — not read as 1 op (the ~100x cliff) nor dropped."""
    return pd.DataFrame(
        [
            {
                "ts_utc": "2026-06-08T00:00:00+00:00",
                "series": "oltp.point_read",
                # rows_per_second deliberately inconsistent with the series'
                # 100-op count (legacy throughput is row-count-based), so a
                # backfill derived from the row's own throughput fails.
                "elapsed_seconds": 5.27,
                "rows_per_second": 1000 / 5.27,
                "postgres_baseline_seconds": 0.006,
                "operations": None,
                "unit": None,
            },
            {
                "ts_utc": "2026-06-09T00:00:00+00:00",
                "series": "oltp.point_read",
                "elapsed_seconds": 5.25,
                "rows_per_second": 100 / 5.25,
                "postgres_baseline_seconds": 0.006,
                "operations": 100,
                "unit": "query",
            },
            {
                "ts_utc": "2026-06-10T00:00:00+00:00",
                "series": "oltp.point_read",
                "elapsed_seconds": 5.23,
                "rows_per_second": 100 / 5.23,
                "postgres_baseline_seconds": 0.006,
                "operations": 100,
                "unit": "query",
            },
        ]
    )


def test_load_dataframe_includes_operations_unit(tmp_path):
    # CHA-438: the explicit work-unit columns flow through the dashboard's
    # projection so the normalized metrics can be derived. The seeded column
    # list is the full post-CHA-438 measurements registry.
    db = tmp_path / "perf.db"
    columns = [
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
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE measurements (" + ", ".join(columns) + ")")
    values: dict[str, object] = dict.fromkeys(columns, "x")
    values.update(
        operation="oltp_point_read",
        row_count=100,
        result_rows=1,
        elapsed_seconds=5.23,
        postgres_baseline_seconds=0.006,
        rows_per_second=100 / 5.23,
        operations=100,
        unit="query",
        ts_utc="2026-06-09T00:00:00+00:00",
    )
    conn.execute(
        "INSERT INTO measurements ("
        + ", ".join(columns)
        + ") VALUES ("
        + ", ".join("?" for _ in columns)
        + ")",
        [values[c] for c in columns],
    )
    conn.commit()
    conn.close()

    frame = dashboard.load_dataframe(str(db))
    assert frame["operations"].tolist() == [100]
    assert frame["unit"].tolist() == ["query"]


def test_measurements_table_leads_with_ms_per_op():
    # CHA-438: the table derives ms_per_op (elapsed / operations, in ms) the
    # same way it already derives pg_delta_pct, and leads with it so the
    # normalized read is first. A frame without series-identity columns has no
    # backfill source, so the legacy NULL-operations row reads as one op.
    table = dashboard.build_measurements_table(_measurements_frame())
    assert table["ms_per_op"].iloc[0] == pytest.approx(0.08 / 100 * 1000.0)
    assert table["ms_per_op"].iloc[1] == pytest.approx(500.0)
    assert list(table.columns).index("ms_per_op") < list(table.columns).index(
        "elapsed_seconds"
    )

    # When the frame DOES carry series identity (main() builds the series
    # column before the table), legacy NULL-ops rows backfill from the series'
    # known count — the production path for migrated history.
    with_series = dashboard.build_measurements_table(
        _oltp_trend_frame().assign(run_id="run-b")
    )
    assert with_series["ms_per_op"].iloc[0] == pytest.approx(52.7)


def test_trend_chart_ms_per_op_metric():
    # CHA-438: the trend chart plots normalized ms/op, deriving the column from
    # operations + elapsed; the legacy NULL-ops point is backfilled from the
    # series (52.7, not 5270 or absent); the Postgres baseline overlays in the
    # SAME normalized space (0.006s for 100 ops -> 0.06 ms/op).
    chart = dashboard.build_trend_chart_data(
        _oltp_trend_frame(), "ms_per_op", show_baselines=True
    )
    assert chart["oltp.point_read"].dropna().tolist() == pytest.approx(
        [52.7, 52.5, 52.3]
    )
    assert chart["pg_baseline / oltp.point_read"].dropna().tolist() == pytest.approx(
        [0.06, 0.06, 0.06]
    )


def test_trend_chart_ops_per_sec_metric():
    # CHA-438: the rate toggle — operations/s as the plotted metric, with the
    # baseline converted to the same space (100 ops over 0.006s -> ~16667/s).
    chart = dashboard.build_trend_chart_data(
        _oltp_trend_frame(), "ops_per_sec", show_baselines=True
    )
    assert chart["oltp.point_read"].dropna().tolist() == pytest.approx(
        [100 / 5.27, 100 / 5.25, 100 / 5.23]
    )
    assert chart["pg_baseline / oltp.point_read"].dropna().tolist() == pytest.approx(
        [100 / 0.006] * 3
    )


def test_zero_operations_row_backfills_from_series():
    # A mis-recorded operations=0 row is unrecorded (shared resolve_operations
    # rule) — it backfills from the series' known count instead of
    # zero-dividing the whole chart.
    frame = pd.DataFrame(
        [
            {
                "ts_utc": "2026-06-09T00:00:00+00:00",
                "series": "oltp.point_read",
                "elapsed_seconds": 5.27,
                "rows_per_second": 100 / 5.27,
                "postgres_baseline_seconds": None,
                "operations": 0,
                "unit": "query",
            },
            {
                "ts_utc": "2026-06-10T00:00:00+00:00",
                "series": "oltp.point_read",
                "elapsed_seconds": 5.23,
                "rows_per_second": 100 / 5.23,
                "postgres_baseline_seconds": None,
                "operations": 100,
                "unit": "query",
            },
        ]
    )
    chart = dashboard.build_trend_chart_data(frame, "ms_per_op", show_baselines=False)
    assert chart["oltp.point_read"].dropna().tolist() == pytest.approx([52.7, 52.3])


def test_baseline_overlay_uses_series_denominator():
    # Only the legacy NULL-ops row carries a baseline: the overlay must divide
    # by the SAME series-backfilled count as the Penca line beside it (100),
    # not re-resolve over the baseline-bearing subset where every row is
    # NULL-ops and the fallback would collapse to 1 (0.006s -> 6 ms, ~100x off).
    frame = pd.DataFrame(
        [
            {
                "ts_utc": "2026-06-09T00:00:00+00:00",
                "series": "oltp.point_read",
                "elapsed_seconds": 5.27,
                "rows_per_second": 1000 / 5.27,
                "postgres_baseline_seconds": 0.006,
                "operations": None,
                "unit": None,
            },
            {
                "ts_utc": "2026-06-10T00:00:00+00:00",
                "series": "oltp.point_read",
                "elapsed_seconds": 5.23,
                "rows_per_second": 100 / 5.23,
                "postgres_baseline_seconds": None,
                "operations": 100,
                "unit": "query",
            },
        ]
    )
    chart = dashboard.build_trend_chart_data(frame, "ms_per_op", show_baselines=True)
    assert chart["oltp.point_read"].dropna().tolist() == pytest.approx([52.7, 52.3])
    assert chart["pg_baseline / oltp.point_read"].dropna().tolist() == pytest.approx(
        [0.06]
    )


def test_load_dataframe_tolerates_pre_migration_db(tmp_path):
    # A DB last written before CHA-438 lacks operations/unit; the dashboard
    # loads it with NULL work-unit columns (same stance as comparison/trends)
    # instead of an OperationalError misreported as "no measurements".
    db = tmp_path / "perf.db"
    legacy_columns = [
        name for name in dashboard._COLUMNS if name not in ("operations", "unit")
    ]
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE measurements (" + ", ".join(legacy_columns) + ")")
    conn.execute(
        "INSERT INTO measurements ("
        + ", ".join(legacy_columns)
        + ") VALUES ("
        + ", ".join("?" for _ in legacy_columns)
        + ")",
        ["x"] * len(legacy_columns),
    )
    conn.commit()
    conn.close()

    frame = dashboard.load_dataframe(str(db))
    assert len(frame) == 1
    assert frame["operations"].isna().all()
    assert frame["unit"].isna().all()


def test_dashboard_run_comparison_renders_ms_per_op_columns():
    # The run-vs-history table renders in the same normalized space as the
    # delta_pct beside it — raw-seconds columns next to a normalized delta
    # would contradict each other whenever op counts differ across runs.
    text = _read("scripts/perf/dashboard.py")
    assert '"run (ms/op)"' in text
    assert '"history mean (ms/op)"' in text
    assert '"unit": entry["unit"]' in text
    assert "run elapsed (s)" not in text


def test_dashboard_filters_lead_with_run_id_and_include_params_json():
    # The sidebar filters must lead with run_id and offer params_json alongside
    # the existing type/file/test/branch/hostname dimensions.
    assert dashboard._FILTER_COLUMNS[0] == "run_id"
    assert "params_json" in dashboard._FILTER_COLUMNS
    # Every filtered dimension is a real measurements column.
    assert set(dashboard._FILTER_COLUMNS).issubset(set(dashboard._COLUMNS))
