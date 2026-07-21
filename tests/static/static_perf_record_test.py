"""Static checks for the perf result recorder (CHA-419).

``PerfRecorder`` is the per-test seam that replaces the old class-var
``results`` list + ``teardown_class`` markdown flush: each measurement is
tagged with its tags (type/file/test), its pytest parametrization,
and the run context (branch/commit/hostname/UTC timestamp), then emitted as one
JSON object per line to ``$PERF_RESULTS_JSON``. A standalone ingest script later
loads that JSONL into SQLite.

These assertions load ``tests/performance/perf_record.py`` by path and pin the
Penca-owned recorder logic only — no Docker, no penca_client, no running
services. The measurement object is duck-typed (a ``SimpleNamespace``) so the
recorder never has to import ``performance_helpers.PerfResult`` (which would
pull in penca_client at module load). Runs under ``just static-test
perf_record`` and ``just check``.
"""

from __future__ import annotations

import datetime
import importlib.util
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

PERF_RECORD = Path(__file__).parents[2] / "tests/performance/perf_record.py"

EXPECTED_COLUMNS = {
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
    "run_id",
}


def _load_perf_record():
    """Load perf_record.py by path (FileNotFoundError is the red state)."""
    spec = importlib.util.spec_from_file_location("perf_record", PERF_RECORD)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _measurement(**overrides):
    base = {
        "operation": "write_empty_table",
        "system_state": "all_hot",
        "row_count": 100_000,
        "elapsed_seconds": 2.0,
        "postgres_baseline_seconds": 1.0,
        "result_rows": None,
        # CHA-438 work-unit fields: one bulk write is a single operation.
        "operations": 1,
        "unit": "write",
    }
    base.update(overrides)
    # operations/unit are mandatory on every measurement by contract (the real
    # PerfResult carries defaults, so they are always present); the recorder
    # reads them via direct attribute access — AttributeError on a
    # nonconforming measurement is intended fail-fast, not a case to default.
    # The real PerfResult exposes rows_per_second as a @property; the recorder
    # READS that value rather than recomputing row_count/elapsed itself (no
    # duplicated throughput formula). Mirror the property here so the test pins
    # "recorder copies measurement.rows_per_second".
    elapsed = base["elapsed_seconds"]
    base["rows_per_second"] = base["row_count"] / elapsed if elapsed > 0 else 0.0
    return SimpleNamespace(**base)


def test_run_context_keys_and_iso_timestamp():
    mod = _load_perf_record()
    ctx = mod.run_context()
    assert set(ctx) >= {"branch", "commit_sha", "hostname", "ts_utc", "run_id"}
    assert all(
        isinstance(ctx[k], str) for k in ("branch", "commit_sha", "hostname", "ts_utc")
    )
    # ts_utc parses as ISO-8601 and is timezone-aware UTC.
    parsed = datetime.datetime.fromisoformat(ctx["ts_utc"])
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == datetime.timedelta(0)
    # run_id is a UUID stamped once per session so a single run can be sliced
    # out of the accumulated history.
    assert uuid.UUID(ctx["run_id"])


def test_record_emits_one_jsonl_object_with_all_columns(tmp_path):
    mod = _load_perf_record()
    json_path = tmp_path / "results.jsonl"
    ctx = {
        "branch": "feature-x",
        "commit_sha": "deadbeef",
        "hostname": "host.local",
        "ts_utc": "2026-06-09T00:00:00+00:00",
        "run_id": "11111111-1111-1111-1111-111111111111",
    }
    sink = mod.PerfSink(str(json_path), ctx)
    recorder = mod.PerfRecorder(
        "performance", "write", "write_into_empty_table", {"batch_count": 4}, sink
    )
    recorder.record(_measurement())

    lines = json_path.read_text().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert set(row) == EXPECTED_COLUMNS
    assert row["run_id"] == "11111111-1111-1111-1111-111111111111"
    assert row["type"] == "performance"
    assert row["file"] == "write"
    assert row["test"] == "write_into_empty_table"
    assert row["operation"] == "write_empty_table"
    assert row["row_count"] == 100_000
    assert row["branch"] == "feature-x"
    assert row["commit_sha"] == "deadbeef"
    assert json.loads(row["params_json"]) == {"batch_count": 4}
    # rows_per_second = row_count / elapsed_seconds.
    assert row["rows_per_second"] == 50_000.0
    # The stub defaults (1, "write") differ from the 100/"query" pair asserted
    # in test_record_emits_operations_and_unit — two value points pin that the
    # recorder copies the measurement's values rather than hardcoding either.
    assert row["operations"] == 1
    assert row["unit"] == "write"


def test_record_rows_per_second_zero_when_no_elapsed(tmp_path):
    mod = _load_perf_record()
    json_path = tmp_path / "results.jsonl"
    sink = mod.PerfSink(
        str(json_path),
        {"branch": "b", "commit_sha": "c", "hostname": "h", "ts_utc": "t"},
    )
    recorder = mod.PerfRecorder("performance", "lifecycle", "persist", {}, sink)
    recorder.record(_measurement(elapsed_seconds=0.0, postgres_baseline_seconds=None))

    row = json.loads(json_path.read_text().splitlines()[0])
    assert row["rows_per_second"] == 0.0
    assert row["postgres_baseline_seconds"] is None


def test_record_emits_operations_and_unit(tmp_path):
    # CHA-438: the work-unit is explicit on every measurement — `operations`
    # (how many operations elapsed_seconds spans; 100 for the point-read rep
    # loop) and `unit` (what one operation is). The recorder copies them off
    # the measurement the same way it copies rows_per_second.
    mod = _load_perf_record()
    json_path = tmp_path / "results.jsonl"
    ctx = {"branch": "b", "commit_sha": "c", "hostname": "h", "ts_utc": "t"}
    sink = mod.PerfSink(str(json_path), ctx)
    recorder = mod.PerfRecorder("grpc", "oltp", "point_read", {}, sink)
    recorder.record(
        _measurement(operation="oltp_point_read", operations=100, unit="query")
    )

    row = json.loads(json_path.read_text().splitlines()[0])
    assert row["operations"] == 100
    assert row["unit"] == "query"


def test_record_appends_one_line_per_measurement(tmp_path):
    # The contract is "one JSONL object per measurement" — record() must APPEND,
    # not truncate/overwrite. Two records into the same sink -> two parseable
    # lines.
    mod = _load_perf_record()
    json_path = tmp_path / "results.jsonl"
    ctx = {"branch": "b", "commit_sha": "c", "hostname": "h", "ts_utc": "t"}
    sink = mod.PerfSink(str(json_path), ctx)
    mod.PerfRecorder("performance", "write", "write_into_empty_table", {}, sink).record(
        _measurement()
    )
    mod.PerfRecorder(
        "performance", "write", "write_into_populated_table", {}, sink
    ).record(_measurement(operation="write_populated_table"))

    lines = json_path.read_text().splitlines()
    assert len(lines) == 2
    operations = sorted(json.loads(line)["operation"] for line in lines)
    assert operations == ["write_empty_table", "write_populated_table"]
