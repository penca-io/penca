"""Static checks for the per-run comparison helper + HTML report (CHA-423).

``scripts/perf/comparison.py`` is the shared kernel that compares one perf run
against the accumulated SQLite history — consumed by both the recipe-fired
static HTML report (``scripts/perf/render_report.py``) and the interactive
Streamlit dashboard (``--run_id``). It reuses ``trends._SERIES_KEYS`` so a
series is identified the same way everywhere (no parallel series-grouping).

These tests add ``scripts/perf`` to ``sys.path`` and import the modules the way
the recipe runs them (``python scripts/perf/render_report.py`` puts that dir on
``sys.path``), seed a tmp ``measurements`` DB via the real ingest, and pin the
Penca-owned comparison arithmetic + that an HTML file is written. matplotlib
renders headless (Agg). Runs under ``just static-test perf_report`` and
``just check``.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPTS_PERF = Path(__file__).parents[2] / "scripts/perf"
if str(SCRIPTS_PERF) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_PERF))

# Imported dynamically (not a top-level `import comparison`) so the modules
# resolve off the scripts/perf sys.path entry the way the recipe runs them, and
# the type checker doesn't flag them. They don't exist until IMPL4 — the
# ModuleNotFoundError raised here at collection is the red state for this gate.
comparison = importlib.import_module("comparison")
render_report = importlib.import_module("render_report")

INGEST = Path(__file__).parents[2] / "scripts/perf/results_to_sqlite.py"


def _load_ingest():
    spec = importlib.util.spec_from_file_location("perf_results_to_sqlite", INGEST)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _record(**overrides):
    base = {
        "type": "performance",
        "file": "query",
        "test": "read",
        "operation": "read_data",
        "system_state": "all_hot",
        "row_count": 100_000,
        "result_rows": None,
        "elapsed_seconds": 1.0,
        "postgres_baseline_seconds": None,
        "rows_per_second": 100_000.0,
        "params_json": "{}",
        "run_id": "11111111-1111-1111-1111-111111111111",
        "branch": "main",
        "commit_sha": "abc123",
        "hostname": "host.local",
        "ts_utc": "2026-06-09T00:00:00+00:00",
    }
    base.update(overrides)
    return base


def _seed_jsonl(path: Path, records) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def _ingest(db_path: Path, records, name: str = "seed.jsonl") -> None:
    jsonl = db_path.parent / name
    _seed_jsonl(jsonl, records)
    _load_ingest().ingest_jsonl(str(jsonl), str(db_path))


def test_compare_run_to_history_delta_and_no_baseline(tmp_path):
    db = tmp_path / "perf.db"
    # History: one prior run of the read_data series at 1.0s.
    _ingest(
        db,
        [_record(run_id="hist", operation="read_data", elapsed_seconds=1.0)],
    )
    history = comparison.load_history(str(db))

    # This run: read_data is slower (2.0s); write_data has no history.
    run_rows = [
        _record(run_id="now", operation="read_data", elapsed_seconds=2.0),
        _record(run_id="now", operation="write_data", elapsed_seconds=5.0),
    ]
    entries = comparison.compare_run_to_history(run_rows, history)
    by_op = {entry["operation"]: entry for entry in entries}

    # Slower-than-history run yields a positive delta vs the historical mean.
    assert by_op["read_data"]["delta_pct"] is not None
    assert by_op["read_data"]["delta_pct"] > 0
    # A series with no history is flagged no-baseline (None delta).
    assert by_op["write_data"]["delta_pct"] is None


def test_compare_run_to_history_postgres_baseline(tmp_path):
    # Postgres baseline is a per-run value off the run's rows, NOT history — so
    # it's surfaced even with no SQLite history at all.
    history = comparison.load_history(str(tmp_path / "absent.db"))

    run_rows = [
        # read_data: two rows at 2.0s vs Postgres baselines of 1.0s/3.0s
        # (mean 2.0s) -> Penca matches the mean baseline, 0% delta.
        _record(
            run_id="now",
            operation="read_data",
            elapsed_seconds=2.0,
            postgres_baseline_seconds=1.0,
        ),
        _record(
            run_id="now",
            operation="read_data",
            elapsed_seconds=2.0,
            postgres_baseline_seconds=3.0,
        ),
        # write_data: no Postgres baseline recorded -> None ("no-baseline").
        _record(
            run_id="now",
            operation="write_data",
            elapsed_seconds=5.0,
            postgres_baseline_seconds=None,
        ),
    ]
    by_op = {
        entry["operation"]: entry
        for entry in comparison.compare_run_to_history(run_rows, history)
    }

    # Baseline is the mean of the run's non-null Postgres times, normalized to
    # ms/op (2.0s over one op -> 2000 ms), and the delta is the run vs that
    # baseline (here equal -> ~0%).
    assert by_op["read_data"]["postgres_ms_per_op"] == pytest.approx(2000.0)
    assert by_op["read_data"]["postgres_delta_pct"] == pytest.approx(0.0)
    # A series with no recorded Postgres time is no-baseline (None), independent
    # of whether it had SQLite history.
    assert by_op["write_data"]["postgres_ms_per_op"] is None
    assert by_op["write_data"]["postgres_delta_pct"] is None


def test_compare_entries_carry_ms_per_op(tmp_path):
    # CHA-438: entries normalize elapsed by the measurement's explicit
    # operation count — 100 point reads over 5.23s is 52.3 ms/query, and the
    # Postgres baseline normalizes by the SAME count so the columns compare
    # like-for-like.
    history = comparison.load_history(str(tmp_path / "absent.db"))
    run_rows = [
        _record(
            run_id="now",
            operation="oltp_point_read",
            row_count=100,
            operations=100,
            unit="query",
            elapsed_seconds=5.23,
            postgres_baseline_seconds=0.006,
            rows_per_second=100 / 5.23,
        )
    ]

    (entry,) = comparison.compare_run_to_history(run_rows, history)

    assert entry["run_ms_per_op"] == pytest.approx(52.3)
    assert entry["postgres_ms_per_op"] == pytest.approx(0.06)
    assert entry["unit"] == "query"


def test_history_ops_backfill(tmp_path):
    # Two history rows, both really 52.5 ms/op: a legacy row (NULL operations,
    # raw 5.25s for what was 100 ops — backfilled with the run's count for the
    # series) and a row with a DIFFERENT stored count (50 ops over 2.625s —
    # stored operations win over backfill). The raw-elapsed mean would be
    # 3.9375s (+32.8% vs the run's 5.23s); only a comparison computed in
    # normalized ms/op space yields the true small drift vs 52.5. The legacy
    # row's row_count (500) deliberately differs from every operation count, so
    # a backfill sourced from the row's own row_count (10.5 ms/op) fails too —
    # the run's operation count for the series is the only basis that passes.
    db = tmp_path / "perf.db"
    _ingest(
        db,
        [
            _record(
                run_id="hist-legacy",
                operation="oltp_point_read",
                row_count=500,
                elapsed_seconds=5.25,
                rows_per_second=500 / 5.25,
            ),
            _record(
                run_id="hist-halved",
                operation="oltp_point_read",
                row_count=50,
                operations=50,
                unit="query",
                elapsed_seconds=2.625,
                rows_per_second=50 / 2.625,
                ts_utc="2026-06-10T00:00:00+00:00",
            ),
        ],
    )
    history = comparison.load_history(str(db))
    # The run row's row_count (500) also differs from its operations (100), so
    # a backfill sourced from the RUN rows' row_count fails just like one
    # sourced from the legacy row's own.
    run_rows = [
        _record(
            run_id="now",
            operation="oltp_point_read",
            row_count=500,
            operations=100,
            unit="query",
            elapsed_seconds=5.23,
            rows_per_second=500 / 5.23,
        )
    ]

    (entry,) = comparison.compare_run_to_history(run_rows, history)

    assert entry["history_mean_ms_per_op"] == pytest.approx(52.5)
    assert entry["delta_pct"] == pytest.approx((52.3 - 52.5) / 52.5 * 100.0, abs=0.02)
    # A legacy NULL-unit history row must not poison the entry's unit to None
    # (the entry's unit is sourced from the run's rows).
    assert entry["unit"] == "query"


def test_zero_operations_treated_as_unrecorded(tmp_path):
    # A mis-recorded operations=0 must not ZeroDivisionError the whole report —
    # it reads as unrecorded, i.e. one operation.
    history = comparison.load_history(str(tmp_path / "absent.db"))
    run_rows = [
        _record(
            run_id="now",
            operation="oltp_point_read",
            row_count=100,
            operations=0,
            unit="query",
            elapsed_seconds=5.23,
            rows_per_second=100 / 5.23,
        )
    ]

    (entry,) = comparison.compare_run_to_history(run_rows, history)

    assert entry["run_ms_per_op"] == pytest.approx(5230.0)


def test_missing_operations_defaults_to_single_op(tmp_path):
    # Run rows with no operations/unit at all (a pre-CHA-438 JSONL): one
    # measurement is one operation, so ms/op is just elapsed in ms — for the
    # row itself and for its Postgres baseline.
    history = comparison.load_history(str(tmp_path / "absent.db"))
    run_rows = [
        _record(
            run_id="now",
            operation="read_data",
            elapsed_seconds=2.0,
            postgres_baseline_seconds=0.5,
        )
    ]

    (entry,) = comparison.compare_run_to_history(run_rows, history)

    assert entry["run_ms_per_op"] == pytest.approx(2000.0)
    assert entry["postgres_ms_per_op"] == pytest.approx(500.0)
    # No unit recorded -> None; rendering omits the "/unit" suffix ("500 ms",
    # never "500 ms/None").
    assert entry["unit"] is None


def test_resolve_run_precedence_sqlite_then_jsonl_then_error(tmp_path):
    db = tmp_path / "perf.db"
    _ingest(db, [_record(run_id="a", operation="read_data", elapsed_seconds=1.0)])
    # A JSONL that ALSO contains run "a" with a DISTINGUISHABLE payload (9.0s vs
    # SQLite's 1.0s), plus a run "b" that exists only in the JSONL.
    jsonl = tmp_path / "run.jsonl"
    _seed_jsonl(
        jsonl,
        [
            _record(run_id="a", operation="read_data", elapsed_seconds=9.0),
            _record(run_id="b", operation="read_data", elapsed_seconds=2.0),
        ],
    )

    # Arm 1 — CONFLICT: run "a" is in BOTH stores. SQLite must win the tie, so
    # the returned row carries the SQLite payload (1.0s), not the JSONL's 9.0s.
    rows_a = comparison.resolve_run("a", str(db), json_path=str(jsonl))
    assert rows_a and all(row["run_id"] == "a" for row in rows_a)
    assert rows_a[0]["elapsed_seconds"] == 1.0

    # Arm 1 (user case) — no JSONL at all, run_id is in SQLite -> resolves fine.
    rows_no_jsonl = comparison.resolve_run("a", str(db), json_path=None)
    assert rows_no_jsonl and all(row["run_id"] == "a" for row in rows_no_jsonl)
    assert rows_no_jsonl[0]["elapsed_seconds"] == 1.0

    # Arm 2 — run "b" is absent from SQLite but present in the JSONL -> fallback.
    rows_b = comparison.resolve_run("b", str(db), json_path=str(jsonl))
    assert rows_b and all(row["run_id"] == "b" for row in rows_b)

    # Arm 3 — found in neither -> ValueError.
    with pytest.raises(ValueError):
        comparison.resolve_run("missing", str(db), json_path=str(jsonl))


def test_load_history_excludes_run_id(tmp_path):
    db = tmp_path / "perf.db"
    _ingest(
        db,
        [
            _record(
                run_id="a",
                operation="read_data",
                elapsed_seconds=1.0,
                ts_utc="2026-06-09T00:00:00+00:00",
            ),
            _record(
                run_id="b",
                operation="read_data",
                elapsed_seconds=3.0,
                ts_utc="2026-06-10T00:00:00+00:00",
            ),
        ],
    )

    full = comparison.load_history(str(db))
    excl_b = comparison.load_history(str(db), exclude_run_id="b")
    # Same single series in both, but excluding run b drops its row.
    (full_rows,) = list(full.values())
    (excl_rows,) = list(excl_b.values())
    assert len(full_rows) == 2
    assert len(excl_rows) == 1

    run_a = [_record(run_id="a", operation="read_data", elapsed_seconds=1.0)]

    # Excluding run a from this DB still leaves run b as the baseline, so a
    # comparison of run a has a non-None delta.
    history_excl_a = comparison.load_history(str(db), exclude_run_id="a")
    assert (
        comparison.compare_run_to_history(run_a, history_excl_a)[0]["delta_pct"]
        is not None
    )

    # But when the DB holds ONLY run a, excluding it leaves no baseline at all
    # -> None delta (the self-exclusion case that keeps a recorded run from
    # being compared against itself).
    db_solo = tmp_path / "solo.db"
    _ingest(db_solo, [_record(run_id="a", operation="read_data", elapsed_seconds=1.0)])
    solo_excl_self = comparison.load_history(str(db_solo), exclude_run_id="a")
    assert (
        comparison.compare_run_to_history(run_a, solo_excl_self)[0]["delta_pct"] is None
    )


def test_write_report_emits_html_with_comparison(tmp_path):
    db = tmp_path / "perf.db"
    _ingest(db, [_record(run_id="hist", operation="read_data", elapsed_seconds=1.0)])
    run_jsonl = tmp_path / "results.jsonl"
    _seed_jsonl(
        run_jsonl,
        [
            _record(
                run_id="now",
                operation="read_data",
                elapsed_seconds=2.0,
                postgres_baseline_seconds=0.5,
            )
        ],
    )
    out = tmp_path / "report.html"

    render_report.write_report(str(run_jsonl), str(db), str(out))

    assert out.exists()
    text = out.read_text()
    assert text.strip()
    assert "read_data" in text
    # Comparison is surfaced (a delta percentage), not just raw numbers.
    assert "%" in text
    # The Postgres baseline is surfaced as its own column + value, normalized
    # to ms/op (CHA-438; this record has no operations -> one op, 0.5s -> 500 ms).
    assert "Postgres" in text
    assert "500 ms" in text
    # A record with no unit renders without a suffix — never "500 ms/None".
    assert "/None" not in text


def test_render_html_headline_section(tmp_path):
    # CHA-438: the report leads with externally-quotable headline numbers a
    # reader can lift with zero knowledge of the test configuration — point
    # lookup in ms/query, scans as humanized rows/s.
    run_jsonl = tmp_path / "results.jsonl"
    _seed_jsonl(
        run_jsonl,
        [
            _record(
                run_id="now",
                operation="oltp_point_read",
                system_state="all_hot",
                row_count=100,
                result_rows=1,
                operations=100,
                unit="query",
                elapsed_seconds=5.23,
                postgres_baseline_seconds=0.006,
                rows_per_second=100 / 5.23,
            ),
            _record(
                run_id="now",
                operation="olap_full_scan",
                system_state="all_cold_snapshotted",
                row_count=1_000_000,
                operations=1,
                unit="query",
                elapsed_seconds=0.2,
                rows_per_second=5_000_000.0,
                params_json='{"olap_setup": 1000000}',
            ),
            # The same scan at a smaller scale (distinct params -> distinct
            # series): the headline must quote the LARGEST scale measured.
            _record(
                run_id="now",
                operation="olap_full_scan",
                system_state="all_cold_snapshotted",
                row_count=100_000,
                operations=1,
                unit="query",
                elapsed_seconds=0.073,
                rows_per_second=100_000 / 0.073,
                params_json='{"olap_setup": 100000}',
            ),
        ],
    )
    out = tmp_path / "report.html"

    render_report.write_report(str(run_jsonl), str(tmp_path / "absent.db"), str(out))

    text = out.read_text()
    assert "Headline numbers" in text
    assert "Point lookup" in text
    # Composite value+unit strings the summary table won't contain (its cells
    # render the value and the unit in separate columns), so these pin the
    # headline's own numbers — not a coincidental substring elsewhere.
    assert "52.3 ms/query" in text
    assert "Full scan" in text
    assert "5.0M rows/s" in text
    # The 100k-scale scan (~1.4M rows/s) appears in the summary table but must
    # not be the quoted headline — the positive scale fragment pins the
    # selection independent of rate formatting. The bare "1.4M" pins the
    # rendered branch of the rows/s cell (a row-counting series keeps its
    # rate in the table even though only the largest scale is headlined).
    assert "@ 1M rows" in text
    assert "@ 100k rows" not in text
    assert "1.4M rows/s" not in text
    assert "1.4M" in text


def test_headline_skips_absent_operations(tmp_path):
    # A scoped run (e.g. `just perf-test performance_query_test.py`) records
    # none of the headline operations — the report still renders, just without
    # those headline rows.
    run_jsonl = tmp_path / "results.jsonl"
    _seed_jsonl(
        run_jsonl,
        [
            _record(
                run_id="now",
                operation="read_data",
                operations=1,
                unit="query",
                elapsed_seconds=2.0,
            )
        ],
    )
    out = tmp_path / "report.html"

    render_report.write_report(str(run_jsonl), str(tmp_path / "absent.db"), str(out))

    text = out.read_text()
    assert "read_data" in text
    assert "Point lookup" not in text
    # No matching headline operations -> the section is omitted entirely, not
    # rendered as an empty table.
    assert "Headline numbers" not in text


def test_summary_table_renders_ms_per_op(tmp_path):
    # The per-series summary table reads in normalized ms/op — the raw rep-loop
    # total ("5.230s") must not appear anywhere as a table value.
    run_jsonl = tmp_path / "results.jsonl"
    _seed_jsonl(
        run_jsonl,
        [
            _record(
                run_id="now",
                operation="oltp_point_read",
                row_count=100,
                operations=100,
                unit="query",
                elapsed_seconds=5.23,
                rows_per_second=100 / 5.23,
            )
        ],
    )
    out = tmp_path / "report.html"

    render_report.write_report(str(run_jsonl), str(tmp_path / "absent.db"), str(out))

    text = out.read_text()
    assert "52.3" in text
    assert "5.230s" not in text
    # An op-counting series (row_count == operations) gets NO rows/s cell —
    # 100 reps / 5.23s would render "19.1", which is queries/sec mislabeled
    # as a row rate.
    assert "19.1" not in text


def test_throughput_headline_without_row_count():
    # A run whose rows never recorded a row_count (pre-row_count-column DB
    # resolved by run_id) still gets its throughput headline — the scale
    # fragment is simply omitted, mirroring the never-"/None" unit rule.
    metrics = importlib.import_module("metrics")
    entry = {
        "operation": "olap_full_scan",
        "system_state": "all_cold_snapshotted",
        "row_count": None,
        "unit": "query",
        "run_ms_per_op": 200.0,
        "run_rows_per_second": 5_000_000.0,
        "postgres_ms_per_op": None,
    }

    (row,) = metrics.select_headlines([entry])

    assert row["penca"] == "5.0M rows/s (200 ms)"


def test_write_report_no_history_is_no_baseline(tmp_path):
    # A default (--record-less) run has no SQLite history; the report must still
    # render, marking every series no-baseline rather than raising.
    missing_db = tmp_path / "absent.db"
    run_jsonl = tmp_path / "results.jsonl"
    _seed_jsonl(
        run_jsonl, [_record(run_id="now", operation="read_data", elapsed_seconds=2.0)]
    )
    out = tmp_path / "report.html"

    render_report.write_report(str(run_jsonl), str(missing_db), str(out))

    assert out.exists()
    assert "no-baseline" in out.read_text().lower()
