"""Static checks for the JSONL -> SQLite perf-results ingest (CHA-419).

``scripts/perf/results_to_sqlite.py`` is the interim sink that loads the JSONL
emitted by ``PerfRecorder`` into a gitignored SQLite ``measurements`` table (one
row per measurement). Eventually this is replaced by storing results in Penca
itself, so the table is a single flat shape with type/file/test as columns
— the namespacing that makes that migration mechanical.

The script is a committed tool, not a package on ``sys.path``; these tests load
it by path and exercise ``ingest_jsonl`` against a tmp DB — stdlib only, no
Docker. Runs under ``just static-test perf_ingest`` and ``just check``.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

INGEST = Path(__file__).parents[2] / "scripts/perf/results_to_sqlite.py"

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


def _load_ingest():
    """Load the ingest script by path (FileNotFoundError is the red state)."""
    spec = importlib.util.spec_from_file_location("perf_results_to_sqlite", INGEST)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _record(**overrides):
    base = {
        "type": "performance",
        "file": "write",
        "test": "write_into_empty_table",
        "operation": "write_empty_table",
        "system_state": "all_hot",
        "row_count": 100_000,
        "result_rows": None,
        "elapsed_seconds": 2.0,
        "postgres_baseline_seconds": 1.0,
        "rows_per_second": 50_000.0,
        "params_json": "{}",
        "branch": "main",
        "commit_sha": "abc123",
        "hostname": "host.local",
        "ts_utc": "2026-06-09T00:00:00+00:00",
        "run_id": "11111111-1111-1111-1111-111111111111",
    }
    base.update(overrides)
    return base


def _seed_jsonl(path: Path, records) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def _seed_legacy_db(mod, db_path: Path, missing_columns: tuple[str, ...]) -> None:
    """Create an old-schema ``measurements`` table (every ingest column except
    ``missing_columns``) and seed one legacy row keyed ``legacykey``."""
    legacy_cols = [name for name in mod._COLUMN_NAMES if name not in missing_columns]
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE measurements (dedupe_key TEXT PRIMARY KEY, "
        + ", ".join(f"{name} TEXT" for name in legacy_cols)
        + ")"
    )
    conn.execute(
        "INSERT INTO measurements (dedupe_key, "
        + ", ".join(legacy_cols)
        + ") VALUES (?"
        + ", ?" * len(legacy_cols)
        + ")",
        ["legacykey"] + ["old"] * len(legacy_cols),
    )
    conn.commit()
    conn.close()


def test_ingest_creates_measurements_table_with_columns(tmp_path):
    mod = _load_ingest()
    json_path = tmp_path / "results.jsonl"
    db_path = tmp_path / "perf.db"
    _seed_jsonl(json_path, [_record(), _record(operation="write_populated_table")])

    mod.ingest_jsonl(str(json_path), str(db_path))

    conn = sqlite3.connect(db_path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(measurements)")}
    assert EXPECTED_COLUMNS <= cols
    rows = conn.execute(
        "SELECT operation, row_count, branch FROM measurements ORDER BY operation"
    ).fetchall()
    conn.close()
    assert rows == [
        ("write_empty_table", 100_000, "main"),
        ("write_populated_table", 100_000, "main"),
    ]


def test_ingest_stores_run_id(tmp_path):
    # run_id identifies which perf run a measurement belongs to, so a single
    # run can be sliced out of the accumulated history. It must round-trip
    # JSONL -> measurements column.
    mod = _load_ingest()
    json_path = tmp_path / "results.jsonl"
    db_path = tmp_path / "perf.db"
    run_id = "abcdef00-0000-4000-8000-000000000001"
    _seed_jsonl(json_path, [_record(run_id=run_id)])

    mod.ingest_jsonl(str(json_path), str(db_path))

    conn = sqlite3.connect(db_path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(measurements)")}
    stored = conn.execute("SELECT run_id FROM measurements").fetchall()
    conn.close()
    assert "run_id" in cols
    assert stored == [(run_id,)]


def test_ingest_migrates_legacy_db_missing_run_id(tmp_path):
    # A .perf/perf.db created before the run_id column existed must keep
    # working: ingest additively ALTERs in the missing column rather than
    # crashing with "no such column: run_id". Legacy rows keep NULL run_id;
    # the freshly ingested row carries its real run_id.
    mod = _load_ingest()
    db_path = tmp_path / "perf.db"
    _seed_legacy_db(mod, db_path, missing_columns=("run_id",))

    json_path = tmp_path / "results.jsonl"
    _seed_jsonl(json_path, [_record(run_id="new-run")])
    mod.ingest_jsonl(str(json_path), str(db_path))

    conn = sqlite3.connect(db_path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(measurements)")}
    rows = dict(conn.execute("SELECT dedupe_key, run_id FROM measurements"))
    conn.close()
    assert "run_id" in cols
    assert rows["legacykey"] is None  # legacy row backfilled to NULL
    assert rows[mod._dedupe_key(_record(run_id="new-run"))] == "new-run"


def test_ingest_persists_operations_and_unit(tmp_path):
    # CHA-438: the explicit work-unit fields round-trip JSONL -> measurements
    # columns, so report/dashboard consumers can normalize elapsed into
    # ms-per-operation without per-test knowledge.
    mod = _load_ingest()
    json_path = tmp_path / "results.jsonl"
    db_path = tmp_path / "perf.db"
    _seed_jsonl(
        json_path,
        [_record(operation="oltp_point_read", operations=100, unit="query")],
    )

    mod.ingest_jsonl(str(json_path), str(db_path))

    conn = sqlite3.connect(db_path)
    stored = conn.execute("SELECT operations, unit FROM measurements").fetchall()
    conn.close()
    assert stored == [(100, "query")]


def test_ingest_migrates_legacy_db_missing_operations_unit(tmp_path):
    # A .perf/perf.db created before the operations/unit columns existed must
    # keep working: ingest additively ALTERs in the missing columns rather
    # than crashing. Legacy rows keep NULL; the fresh row carries its values.
    mod = _load_ingest()
    db_path = tmp_path / "perf.db"
    _seed_legacy_db(mod, db_path, missing_columns=("operations", "unit"))

    json_path = tmp_path / "results.jsonl"
    new_record = _record(operations=100, unit="query")
    _seed_jsonl(json_path, [new_record])
    mod.ingest_jsonl(str(json_path), str(db_path))

    conn = sqlite3.connect(db_path)
    rows = {
        key: (operations, unit)
        for key, operations, unit in conn.execute(
            "SELECT dedupe_key, operations, unit FROM measurements"
        )
    }
    conn.close()
    assert rows["legacykey"] == (None, None)  # legacy row backfilled to NULL
    assert rows[mod._dedupe_key(new_record)] == (100, "query")


def test_ingest_is_idempotent_on_reingest(tmp_path):
    mod = _load_ingest()
    json_path = tmp_path / "results.jsonl"
    db_path = tmp_path / "perf.db"
    _seed_jsonl(json_path, [_record(), _record(operation="write_populated_table")])

    mod.ingest_jsonl(str(json_path), str(db_path))
    mod.ingest_jsonl(str(json_path), str(db_path))

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]
    conn.close()
    assert count == 2


def test_ingest_is_additive_across_files_then_dedups(tmp_path):
    # The real use case accumulates: each run emits its own JSONL into one
    # shared DB. Ingest must be additive across distinct files AND dedup a
    # re-ingested file — a truncate-and-reload impl (which would pass the
    # single-file idempotency test) silently wipes prior runs and is caught
    # here.
    mod = _load_ingest()
    db_path = tmp_path / "perf.db"
    file_a = tmp_path / "a.jsonl"
    file_b = tmp_path / "b.jsonl"
    _seed_jsonl(file_a, [_record(), _record(operation="write_populated_table")])
    _seed_jsonl(
        file_b,
        [
            _record(ts_utc="2026-06-10T00:00:00+00:00"),
            _record(
                operation="write_populated_table", ts_utc="2026-06-10T00:00:00+00:00"
            ),
        ],
    )

    mod.ingest_jsonl(str(file_a), str(db_path))
    mod.ingest_jsonl(str(file_b), str(db_path))
    conn = sqlite3.connect(db_path)
    after_both = conn.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]
    # Re-ingesting A must not duplicate its rows nor drop B's.
    mod.ingest_jsonl(str(file_a), str(db_path))
    after_reingest = conn.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]
    conn.close()
    assert after_both == 4
    assert after_reingest == 4


def test_ingest_skips_malformed_and_non_object_lines(tmp_path):
    # A corrupt line and a valid-but-non-object line are both skipped (not
    # aborting the run); the two good rows still land.
    mod = _load_ingest()
    json_path = tmp_path / "results.jsonl"
    db_path = tmp_path / "perf.db"
    json_path.write_text(
        json.dumps(_record())
        + "\nNOT JSON {{{\n"
        + "42\n"
        + json.dumps(_record(operation="write_populated_table"))
        + "\n"
    )

    mod.ingest_jsonl(str(json_path), str(db_path))

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]
    conn.close()
    assert count == 2
