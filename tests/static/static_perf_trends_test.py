"""Static checks for the perf-trends summary + graphing tool (CHA-419).

``scripts/perf/trends.py`` reads the SQLite ``measurements`` history and produces
(a) per-series summary stats with a regression flag and (b) trend PNGs. A series
is one (type, file, test, operation, system_state) tuple tracked over time
(ts_utc); each row is one run of that series.

These tests load the script by path, seed a tmp ``measurements`` table directly
(decoupled from the ingest tool), and pin the Penca-owned summary arithmetic +
that a graph file is written. matplotlib renders headless (Agg) in the script.
Runs under ``just static-test perf_trends`` and ``just check``.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

# trends.py imports the shared metrics module off the scripts/perf sys.path
# entry (the way `python scripts/perf/trends.py` runs, where the script dir is
# sys.path[0]); the by-path load below doesn't add it, so do it explicitly —
# the same self-sufficiency the report/dashboard test modules have. Without
# this, the file only passes when collected after a sibling that already
# mutated sys.path.
SCRIPTS_PERF = Path(__file__).parents[2] / "scripts/perf"
if str(SCRIPTS_PERF) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_PERF))

TRENDS = Path(__file__).parents[2] / "scripts/perf/trends.py"

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
    "branch",
    "commit_sha",
    "hostname",
    "ts_utc",
]


def _load_trends():
    """Load the trends script by path (FileNotFoundError is the red state)."""
    spec = importlib.util.spec_from_file_location("perf_trends", TRENDS)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _insert_runs(db_path: Path, operation: str, runs) -> None:
    """Insert ``runs`` (ts_utc, elapsed, rows_per_second[, operations, unit])
    for one series.

    Rows are inserted in the order given — callers pass a NON-chronological
    order so a summarize() that reads "latest" by rowid/insertion order instead
    of sorting on ts_utc gets the wrong answer. The optional trailing
    operations/unit pair seeds the CHA-438 work-unit columns and must be
    passed together; 3-tuples leave both NULL (a legacy row).
    """
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS measurements (" + ", ".join(_COLUMNS) + ")"
    )
    for run in runs:
        ts_utc, elapsed, rps = run[:3]
        operations, unit = run[3:] if len(run) > 3 else (None, None)
        values = dict.fromkeys(_COLUMNS)
        values.update(
            type="performance",
            file="write",
            test="write_into_empty_table",
            operation=operation,
            system_state="all_hot",
            row_count=100_000,
            elapsed_seconds=elapsed,
            rows_per_second=rps,
            operations=operations,
            unit=unit,
            params_json="{}",
            branch="main",
            commit_sha="abc",
            hostname="h",
            ts_utc=ts_utc,
        )
        conn.execute(
            "INSERT INTO measurements ("
            + ", ".join(_COLUMNS)
            + ") VALUES ("
            + ", ".join("?" for _ in _COLUMNS)
            + ")",
            [values[c] for c in _COLUMNS],
        )

    conn.commit()
    conn.close()


# Chronological elapsed 2.0 -> 1.5 -> 3.0, but inserted shuffled so "latest"
# can only be 3.0 if summarize sorts on ts_utc (not insertion order).
_REGRESSION_RUNS = [
    ("2026-06-02T00:00:00+00:00", 1.5, 66_666.0),
    ("2026-06-03T00:00:00+00:00", 3.0, 33_333.0),
    ("2026-06-01T00:00:00+00:00", 2.0, 50_000.0),
]
# Chronological 2.0 -> 3.0 -> 2.5 (shuffled): latest (2.5) is BETTER than the
# previous run (3.0) but still worse than min (2.0). Pins regression as
# "latest vs previous", not "latest vs min/mean".
_RECOVERED_RUNS = [
    ("2026-06-02T00:00:00+00:00", 3.0, 33_333.0),
    ("2026-06-01T00:00:00+00:00", 2.0, 50_000.0),
    ("2026-06-03T00:00:00+00:00", 2.5, 40_000.0),
]


def test_summarize_sorts_by_ts_and_flags_regression(tmp_path):
    mod = _load_trends()
    db_path = tmp_path / "perf.db"
    _insert_runs(db_path, "write_empty_table", _REGRESSION_RUNS)

    summary = mod.summarize(str(db_path))
    assert len(summary) == 1
    series = summary[0]
    assert series["operation"] == "write_empty_table"
    assert series["system_state"] == "all_hot"
    assert series["run_count"] == 3
    # latest is by ts_utc (06-03 -> 3.0), not insertion order (06-01 -> 2.0).
    assert series["latest_elapsed"] == 3.0
    assert series["min_elapsed"] == 1.5
    assert series["max_elapsed"] == 3.0
    # latest (3.0) slower than previous run (1.5) -> regression.
    assert series["regressed"] is True
    # A fully-legacy series (no row ever recorded a count) reads as one op per
    # measurement — 3.0s -> 3000 ms/op — with no unit.
    assert series["latest_ms_per_op"] == pytest.approx(3000.0)
    assert series["unit"] is None


def test_summarize_no_regression_when_latest_beats_previous(tmp_path):
    mod = _load_trends()
    db_path = tmp_path / "perf.db"
    _insert_runs(db_path, "stable_op", _RECOVERED_RUNS)

    series = next(
        s for s in mod.summarize(str(db_path)) if s["operation"] == "stable_op"
    )
    assert series["latest_elapsed"] == 2.5
    # latest (2.5) faster than previous (3.0) -> NOT a regression, even though
    # 2.5 is still worse than min (2.0).
    assert series["regressed"] is False


def test_summarize_normalized_ms_per_op(tmp_path):
    # CHA-438: the summary reads in ms-per-operation, not raw loop totals —
    # 100 point reads over 5.23s is 52.3 ms/query.
    mod = _load_trends()
    db_path = tmp_path / "perf.db"
    _insert_runs(
        db_path,
        "oltp_point_read",
        [
            ("2026-06-01T00:00:00+00:00", 5.25, 100 / 5.25, 100, "query"),
            ("2026-06-02T00:00:00+00:00", 5.23, 100 / 5.23, 100, "query"),
        ],
    )

    series = next(
        s for s in mod.summarize(str(db_path)) if s["operation"] == "oltp_point_read"
    )
    assert series["latest_ms_per_op"] == pytest.approx(52.3)
    assert series["unit"] == "query"


def test_pct_change_across_mixed_stored_op_counts(tmp_path):
    # latest (100 ops over 5.23s) vs previous (a DIFFERENT stored count: 50 ops
    # over 2.625s) — both really ~52.5/52.3 ms/op. latest-vs-previous on raw
    # elapsed would read +99.2% (5.23 vs 2.625) and flag a regression; only a
    # comparison computed in normalized ms/op space yields the true -0.4%, and
    # only an implementation honoring the stored counts (not 1, not a series
    # backfill of 100) reads the previous row as 52.5.
    mod = _load_trends()
    db_path = tmp_path / "perf.db"
    _insert_runs(
        db_path,
        "oltp_point_read",
        [
            ("2026-06-02T00:00:00+00:00", 2.625, 50 / 2.625, 50, "query"),
            ("2026-06-03T00:00:00+00:00", 5.23, 100 / 5.23, 100, "query"),
        ],
    )

    series = next(
        s for s in mod.summarize(str(db_path)) if s["operation"] == "oltp_point_read"
    )
    assert series["latest_ms_per_op"] == pytest.approx(52.3)
    # The previous row's stored count (50) is honored — a 1-op or backfilled-100
    # reading would yield 2625 or 26.25, not 52.5.
    assert series["previous_ms_per_op"] == pytest.approx(52.5)
    assert series["pct_change"] == pytest.approx((52.3 - 52.5) / 52.5 * 100.0, abs=0.02)
    assert series["regressed"] is False
    assert series["unit"] == "query"


def test_legacy_null_ops_backfilled_with_latest_count(tmp_path):
    # A legacy previous row (NULL operations, raw 5.25s for what was really
    # 100 ops) must be backfilled with the series' latest known operation
    # count: a NULL-as-1 reading would yield 5250 ms/op (the ~100x cliff), and
    # a backfill from the row's own row_count (the helper seeds 100_000) would
    # yield 0.0525 — only the latest non-NULL count (100) reads 52.5.
    mod = _load_trends()
    db_path = tmp_path / "perf.db"
    _insert_runs(
        db_path,
        "oltp_point_read",
        [
            ("2026-06-01T00:00:00+00:00", 5.25, 100 / 5.25),
            ("2026-06-02T00:00:00+00:00", 5.23, 100 / 5.23, 100, "query"),
        ],
    )

    series = next(
        s for s in mod.summarize(str(db_path)) if s["operation"] == "oltp_point_read"
    )
    assert series["previous_ms_per_op"] == pytest.approx(52.5)
    assert series["pct_change"] == pytest.approx((52.3 - 52.5) / 52.5 * 100.0, abs=0.02)
    assert series["regressed"] is False
    # unit comes from the latest (non-NULL) row, not the legacy NULL one.
    assert series["unit"] == "query"


def test_newest_degenerate_count_does_not_poison_backfill(tmp_path):
    # The newest row carries a mis-recorded operations=0: it must resolve via
    # the series' newest POSITIVE count (100) — and the fallback selection
    # must skip the 0 too, or the legacy NULL row would normalize by 1.
    mod = _load_trends()
    db_path = tmp_path / "perf.db"
    _insert_runs(
        db_path,
        "oltp_point_read",
        [
            ("2026-06-01T00:00:00+00:00", 5.25, 100 / 5.25),
            ("2026-06-02T00:00:00+00:00", 5.23, 100 / 5.23, 100, "query"),
            ("2026-06-03T00:00:00+00:00", 5.27, 100 / 5.27, 0, "query"),
        ],
    )

    series = next(
        s for s in mod.summarize(str(db_path)) if s["operation"] == "oltp_point_read"
    )
    assert series["latest_ms_per_op"] == pytest.approx(52.7)
    assert series["previous_ms_per_op"] == pytest.approx(52.3)
    assert series["regressed"] is False


def test_render_summary_markdown_includes_derived_values(tmp_path):
    mod = _load_trends()
    db_path = tmp_path / "perf.db"
    _insert_runs(db_path, "write_empty_table", _REGRESSION_RUNS)

    markdown = mod.render_summary_markdown(mod.summarize(str(db_path)))
    assert "write_empty_table" in markdown
    # A real table, not a header-only stub: the separator row + a derived value.
    assert "---" in markdown
    assert "3" in markdown  # run_count and/or latest_elapsed surfaced


def test_generate_graphs_writes_png(tmp_path):
    mod = _load_trends()
    db_path = tmp_path / "perf.db"
    _insert_runs(db_path, "write_empty_table", _REGRESSION_RUNS)
    out_dir = tmp_path / "graphs"

    mod.generate_graphs(str(db_path), str(out_dir))
    assert list(out_dir.glob("*.png"))


def test_generate_graphs_one_png_per_series_no_collision(tmp_path):
    # Two series whose sanitized stems would collide ("a/b" -> "a_b" == "a_b");
    # the hash suffix keeps them distinct -> two files, neither overwritten.
    mod = _load_trends()
    db_path = tmp_path / "perf.db"
    _insert_runs(db_path, "a/b", _REGRESSION_RUNS)
    _insert_runs(db_path, "a_b", _REGRESSION_RUNS)
    out_dir = tmp_path / "graphs"

    mod.generate_graphs(str(db_path), str(out_dir))
    assert len(list(out_dir.glob("*.png"))) == 2


def test_main_reports_no_measurements(tmp_path, capsys):
    mod = _load_trends()
    # Missing DB file.
    assert mod.main(["--db", str(tmp_path / "absent.db")]) == 1
    assert "no measurements" in capsys.readouterr().out

    # Existing DB file but no measurements table.
    other_db = tmp_path / "other.db"
    conn = sqlite3.connect(other_db)
    conn.execute("CREATE TABLE unrelated (x INTEGER)")
    conn.commit()
    conn.close()
    assert mod.main(["--db", str(other_db)]) == 1


def test_md_cell_escapes_pipe():
    mod = _load_trends()
    assert mod._md_cell("a|b") == "a\\|b"
    assert mod._md_cell(3) == "3"
