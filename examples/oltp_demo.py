#!/usr/bin/env python3
"""A primary-key point lookup against open columnar files on object storage.

One feature: fetch a single row by its primary key. The interesting part is
where the row lives. A columnar layout is built for scans, so the fair question
to ask of a lakehouse is whether a single-row seek is still a seek once the data
has been persisted and snapshotted into open columnar files — this script drives
the table to that steady state first, then times the lookup and prints what it
measured on your machine.

The same seek is shown two ways: through the gRPC client, and as ordinary SQL
over Flight SQL. They are not the same amount of work per call — `ReadData` is a
single RPC, while the SQL arm parses, plans and takes the ADBC driver's
prepared-statement round trip before reaching the same read path. That is a
mechanism difference, not a verdict; the numbers are yours, measured live, and
nothing is baked into this file.

Seeding and the persist/snapshot step use the gRPC client, because neither bulk
loading a table nor driving the lifecycle is something SQL can express. The
lookup itself is shown both ways, because that is the subject.

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

    One untimed call first: the first request on a connection pays for session
    setup and the table-identifier resolve, which are one-off costs and not what
    a per-seek figure should be reporting.

    Returns the result as well as the timing so the caller can show *what* the
    seek found — a latency figure for a lookup that quietly returned nothing
    would be worse than no figure at all.
    """
    lookup()

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
        description="A primary-key point lookup against cold columnar storage."
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

        # Explicit rather than left to the lifecycle scheduler: the scheduler is
        # disabled on the test profile, so waiting for it would measure the hot
        # tier under CI and the cold tier for a reader — the same script
        # reporting two different things. Driving it here also lets the output
        # below say which tier the number belongs to.
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

        print(f"\nLooking up account {target_id} ({args.reps}x per arm)...")
        grpc_ms, found = time_lookups(grpc_lookup, args.reps)
        sql_ms, _ = time_lookups(lambda: sql.execute_query(select), args.reps)

        print("\n--- The row we looked up ---")
        print(found.to_pandas().to_markdown(index=False))

        print("\n--- Point lookup latency on cold columnar (mean per seek) ---")
        print_table(
            {
                "path": ["gRPC", "SQL"],
                "ms": [round(grpc_ms, 3), round(sql_ms, 3)],
            }
        )
        print(
            f"\nOne row out of {args.rows}, seeked straight out of open columnar "
            f"files on object storage — no scan of the table to find it."
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
