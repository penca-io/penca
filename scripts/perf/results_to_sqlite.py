"""Ingest perf-result JSONL into a SQLite ``measurements`` table (CHA-419).

``just perf-test`` emits one JSON object per measurement (see
``tests/performance/perf_record.py``) to a JSONL file; this script loads those
rows into a gitignored SQLite DB so perf history accumulates across runs,
branches, and hosts. It is the interim backend — eventually the results live in
Penca itself, which is why the table is one flat shape with type/file/test as columns (the namespacing that makes that migration mechanical).

Ingest is **additive across files** and **idempotent per row**: each row carries
a content-hash ``dedupe_key`` (PRIMARY KEY), so re-ingesting the same JSONL is a
no-op while distinct runs accumulate. stdlib only.

Usage:
    python scripts/perf/results_to_sqlite.py --json .perf/results.jsonl --db .perf/perf.db
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

# Column name -> SQLite type. Order is the canonical column order used for the
# dedupe hash and the INSERT.
_COLUMNS: dict[str, str] = {
    "type": "TEXT NOT NULL",
    "file": "TEXT NOT NULL",
    "test": "TEXT NOT NULL",
    "operation": "TEXT NOT NULL",
    "system_state": "TEXT NOT NULL",
    "row_count": "INTEGER NOT NULL",
    "result_rows": "INTEGER",
    "elapsed_seconds": "REAL NOT NULL",
    "postgres_baseline_seconds": "REAL",
    "rows_per_second": "REAL NOT NULL",
    # CHA-438 work-unit fields; nullable because pre-existing JSONL rows lack
    # them (record.get degrades to NULL).
    "operations": "INTEGER",
    "unit": "TEXT",
    "params_json": "TEXT NOT NULL",
    "run_id": "TEXT NOT NULL",
    "branch": "TEXT NOT NULL",
    "commit_sha": "TEXT NOT NULL",
    "hostname": "TEXT NOT NULL",
    "ts_utc": "TEXT NOT NULL",
}
_COLUMN_NAMES = list(_COLUMNS)


def _dedupe_key(record: dict) -> str:
    """Content hash over the canonical column values — identical rows collide.

    The hash spans ``_COLUMN_NAMES``, so growing the schema reshapes the keys:
    re-ingesting a pre-upgrade JSONL into an already-populated DB duplicates
    those rows instead of deduping (same accepted behavior as the ``run_id``
    addition — per-run JSONLs are truncated each run, so only a manual
    re-ingest hits it).
    """
    payload = json.dumps(
        {name: record.get(name) for name in _COLUMN_NAMES},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _create_table(conn: sqlite3.Connection) -> None:
    columns_ddl = ",\n    ".join(f"{name} {ddl}" for name, ddl in _COLUMNS.items())
    conn.execute(
        "CREATE TABLE IF NOT EXISTS measurements (\n"
        "    dedupe_key TEXT PRIMARY KEY,\n"
        f"    {columns_ddl}\n"
        ")"
    )
    _migrate_columns(conn)


def _migrate_columns(conn: sqlite3.Connection) -> None:
    """Additively add any ``_COLUMNS`` missing from a pre-existing table.

    ``.perf/perf.db`` accumulates across runs, so a DB created before a column
    was introduced (e.g. ``run_id``) must not break ingest — ``CREATE TABLE IF
    NOT EXISTS`` is a no-op on an existing table, so the INSERT would otherwise
    hit ``no such column``. New columns are added nullable (SQLite can't add a
    ``NOT NULL`` column without a default); only legacy rows predating the
    column carry NULL, while every fresh ingest writes the real value.
    """
    existing = {row[1] for row in conn.execute("PRAGMA table_info(measurements)")}
    for name, ddl in _COLUMNS.items():
        if name not in existing:
            nullable_ddl = ddl.replace(" NOT NULL", "")
            conn.execute(f"ALTER TABLE measurements ADD COLUMN {name} {nullable_ddl}")


def ingest_jsonl(json_path: str, db_path: str) -> int:
    """Load each JSONL line into ``measurements``; return the rows inserted.

    Additive across distinct files; idempotent on re-ingest (INSERT OR IGNORE
    on the content-hash primary key).
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    placeholders = ", ".join("?" for _ in range(len(_COLUMN_NAMES) + 1))
    insert_sql = (
        "INSERT OR IGNORE INTO measurements (dedupe_key, "
        + ", ".join(_COLUMN_NAMES)
        + f") VALUES ({placeholders})"
    )

    conn = sqlite3.connect(db_path)
    try:
        _create_table(conn)
        inserted = 0
        with open(json_path) as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue

                # Isolate a bad line so one corrupt row can't discard the whole
                # run's valid measurements. (NOT NULL violations are skipped
                # silently by INSERT OR IGNORE; this guards the parse step.)
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as error:
                    print(
                        f"[perf] skipping malformed JSON on line {line_number}: {error}",
                        file=sys.stderr,
                    )
                    continue

                # Valid JSON that isn't an object (42, [1,2], "foo") would blow
                # up _dedupe_key/.get below — skip it too.
                if not isinstance(record, dict):
                    print(
                        f"[perf] skipping non-object JSON on line {line_number}",
                        file=sys.stderr,
                    )
                    continue

                values = [_dedupe_key(record)] + [
                    record.get(name) for name in _COLUMN_NAMES
                ]
                cursor = conn.execute(insert_sql, values)
                inserted += cursor.rowcount

        conn.commit()
    finally:
        conn.close()

    return inserted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingest perf-result JSONL into SQLite."
    )
    parser.add_argument("--json", required=True, help="path to the JSONL results file")
    parser.add_argument("--db", required=True, help="path to the SQLite database")
    args = parser.parse_args(argv)

    inserted = ingest_jsonl(args.json, args.db)
    print(f"[perf] ingested {inserted} new measurement(s) into {args.db}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
