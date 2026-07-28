#!/usr/bin/env python3
"""A primary-key point lookup, and what happens to it when the data goes cold.

One feature: fetch a single row by its primary key. The interesting part is the
tier. Rows start in Penca's hot tier; the lifecycle pipeline persists and
snapshots them into open columnar files on object storage. A columnar layout is
built for scans, so the fair question to ask of a lakehouse is whether a
single-row seek survives the trip — this script times the same lookup before and
after, and prints what it measured on your machine.

It runs the lookup twice over on each tier: once through the gRPC client, once
as ordinary SQL over Flight SQL. The two are not the same amount of work per
call — `ReadData` is a single RPC, while the SQL arm parses, plans, and takes
the ADBC driver's prepared-statement round trip before it reaches the same read
path. Expect the gRPC arm to show less fixed overhead today; closing that gap is
active work. The numbers below are yours, not ours: nothing here is baked in.

Setup and the tier change use the gRPC client, because bulk-seeding a table and
driving persist/snapshot are not things SQL can express. The lookup itself is
shown both ways, because that is the subject.

Requires Docker services running: just penca-up
"""

from __future__ import annotations

import argparse
import time
from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation, PencaClient

AUTHOR = "penca-demo"
SCHEMA_NAME = "ledger"
TABLE_NAME = "accounts"

DEFAULT_ROWS = 100_000
# A point read is sub-millisecond of real work dominated by the round trip, so
# one measurement is mostly noise. Average over repetitions instead.
DEFAULT_REPS = 50

ACCOUNTS_SCHEMA = pa.schema(
    [
        pa.field("account_id", pa.int64()),
        pa.field("owner", pa.utf8()),
        pa.field("balance", pa.float64()),
    ]
)


def seed_rows(rows: int) -> pa.Table:
    """``rows`` accounts, ids ``0..rows-1``, owners zero-padded so they sort."""
    return pa.table(
        {
            "account_id": list(range(rows)),
            "owner": [f"owner_{account_id:06d}" for account_id in range(rows)],
            "balance": [round(account_id * 1.1, 2) for account_id in range(rows)],
        },
        schema=ACCOUNTS_SCHEMA,
    )


def time_lookups(lookup, reps: int) -> tuple[float, pa.Table]:
    """Mean milliseconds per call, plus the last result for inspection.

    Returns the result as well as the timing so the caller can show *what* the
    seek found — a latency figure for a lookup that quietly returned nothing
    would be worse than no figure at all.
    """
    start = time.perf_counter()
    for _ in range(reps):
        found = lookup()

    elapsed = time.perf_counter() - start

    return (elapsed / reps) * 1000, found


def print_table(columns: dict[str, list]) -> None:
    print(pa.table(columns).to_pandas().to_markdown(index=False))


def discard_catalog(client: PencaClient, catalog_uuid: str) -> None:
    """Best-effort: report a failed cleanup rather than raising over it.

    The catalog is pure scaffolding — the demo has nothing to show once the
    numbers are printed — so it deletes what it created instead of leaving a
    `demo_*` behind on every run.
    """
    try:
        client.delete_catalog(catalog_uuid=catalog_uuid)
    except Exception as exc:
        print(f"\n(could not delete catalog {catalog_uuid}: {exc})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="A primary-key point lookup, hot tier and cold columnar."
    )
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--reps", type=int, default=DEFAULT_REPS)
    args = parser.parse_args()
    # Validate before main() opens a client: otherwise a typo'd flag fails only
    # after the catalog exists, leaving debris behind a stack trace.
    if args.rows < 2:
        parser.error("--rows must be at least 2")

    if args.reps < 1:
        parser.error("--reps must be at least 1")

    return args


def main() -> None:
    args = parse_args()
    # The middle of the key range, so the seek is not trivially the first or
    # last row in any layout the storage happens to choose.
    target_id = args.rows // 2

    client = PencaClient.from_settings()
    catalog_name = f"demo_{uuid4().hex[:8]}"
    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, AUTHOR)
    # Rebind the client's default catalog so the calls below target it rather
    # than falling back to the bootstrap "public" catalog.
    client.catalog = catalog_name

    sql = None
    try:
        schema_uuid = client.create_schema(
            SCHEMA_NAME,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
            author=AUTHOR,
            comment="create ledger schema",
        )
        table_uuid = client.create_table(
            TABLE_NAME,
            ACCOUNTS_SCHEMA,
            primary_keys=["account_id"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author=AUTHOR,
            comment="create accounts table",
        )

        print(f"Seeding {args.rows} accounts into {catalog_name}.{SCHEMA_NAME}...")
        client.write_data(
            None,
            Mutation(table_uuid=table_uuid, upserts=seed_rows(args.rows)),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author=AUTHOR,
            comment="seed accounts",
        )

        fqn = f"{catalog_name}.{SCHEMA_NAME}.{TABLE_NAME}"
        select = (
            f"SELECT account_id, owner, balance FROM {fqn} "
            f"WHERE account_id = {target_id}"
        )

        def grpc_lookup() -> pa.Table:
            return client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch_uuid,
                filter=f"account_id = {target_id}",
            )

        # One Flight SQL connection, pinned to this catalog at handshake the way
        # a Postgres connection is pinned to one database.
        sql = PencaClient.from_settings(catalog=catalog_name, branch="main")

        print(f"Looking up account {target_id} ({args.reps}x per arm)...\n")
        hot_grpc, found = time_lookups(grpc_lookup, args.reps)
        hot_sql, _ = time_lookups(lambda: sql.execute_query(select), args.reps)

        print("Persisting and snapshotting to cold columnar storage...")
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )

        cold_grpc, _ = time_lookups(grpc_lookup, args.reps)
        cold_sql, _ = time_lookups(lambda: sql.execute_query(select), args.reps)

        print("\n--- The row we looked up ---")
        print(found.to_pandas().to_markdown(index=False))

        print("\n--- Point lookup latency (mean per seek) ---")
        print_table(
            {
                "path": ["gRPC", "SQL", "gRPC", "SQL"],
                "tier": ["hot", "hot", "cold", "cold"],
                "ms": [
                    round(hot_grpc, 3),
                    round(hot_sql, 3),
                    round(cold_grpc, 3),
                    round(cold_sql, 3),
                ],
            }
        )
        print(
            f"\nSame single-row seek, {args.rows} rows, before and after the data "
            f"moved into open columnar files."
        )
    finally:
        # finally, not straight-line: a failed run is exactly when leaving a
        # catalog behind hurts most.
        if sql is not None:
            try:
                sql.close()
            except Exception as exc:
                print(f"(could not close the SQL connection: {exc})")

        discard_catalog(client, catalog_uuid)


if __name__ == "__main__":
    main()
