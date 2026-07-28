#!/usr/bin/env python3
"""One analytical question, asked two ways against the same columnar copy.

One feature: a `GROUP BY` over the whole table, after the rows have been
persisted and snapshotted into open columnar files on object storage. No
branching, no time travel — just the analytical query, on the same copy of the
data that took the row-level writes a moment earlier. That is the "one copy,
both workloads" claim with nothing else attached.

The same aggregate is computed twice, and the difference is where the work
happens rather than how fast it is:

  * gRPC `ReadData` has no server-side aggregate, so every row travels to the
    client and the grouping happens here, in pyarrow.
  * Flight SQL pushes the `GROUP BY` into the engine, so only the grouped rows
    come back.

Both results are printed so you can see they agree — a timing comparison means
nothing unless both arms answered the same question. The timings are measured
live on your machine; nothing is baked into this file.

Setup and the tier change use the gRPC client, because bulk-seeding a table and
driving persist/snapshot are not things SQL can express.

Requires Docker services running: just penca-up
"""

from __future__ import annotations

import argparse
import random
import time
from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation, PencaClient

AUTHOR = "penca-demo"
SCHEMA_NAME = "analytics"
TABLE_NAME = "events"

DEFAULT_ROWS = 100_000
DEFAULT_SEED = 20260728

REGIONS = ("us-east", "us-west", "eu-central", "ap-south")
DEVICES = ("desktop", "mobile", "tablet")

EVENTS_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.int64()),
        pa.field("region", pa.utf8()),
        pa.field("device", pa.utf8()),
        # Cents, not a float. The two arms sum in different engines and in
        # different orders, and float addition is not associative — an integer
        # column is what lets the printed totals be compared exactly.
        pa.field("amount_cents", pa.int64()),
    ]
)


def seed_rows(rows: int, seed: int) -> pa.Table:
    """``rows`` events spread over the regions and devices, reproducibly."""
    rng = random.Random(seed)

    return pa.table(
        {
            "event_id": list(range(rows)),
            "region": [rng.choice(REGIONS) for _ in range(rows)],
            "device": [rng.choice(DEVICES) for _ in range(rows)],
            "amount_cents": [rng.randrange(50, 50_000) for _ in range(rows)],
        },
        schema=EVENTS_SCHEMA,
    )


def group_client_side(scanned: pa.Table) -> pa.Table:
    """The aggregate gRPC cannot do for us, done here over the shipped rows."""
    grouped = scanned.group_by("region").aggregate(
        [("event_id", "count"), ("amount_cents", "sum")]
    )

    return pa.table(
        {
            "region": grouped.column("region"),
            "events": grouped.column("event_id_count"),
            "total_cents": grouped.column("amount_cents_sum"),
        }
    ).sort_by("region")


def print_table(table: pa.Table) -> None:
    print(table.to_pandas().to_markdown(index=False))


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
        description="One GROUP BY, computed two ways on one columnar copy."
    )
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    # Validate before main() opens a client: otherwise a typo'd flag fails only
    # after the catalog exists, leaving debris behind a stack trace.
    if args.rows < len(REGIONS):
        parser.error(f"--rows must be at least {len(REGIONS)}")

    return args


def main() -> None:
    args = parse_args()

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
            comment="create analytics schema",
        )
        table_uuid = client.create_table(
            TABLE_NAME,
            EVENTS_SCHEMA,
            primary_keys=["event_id"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author=AUTHOR,
            comment="create events table",
        )

        print(f"Seeding {args.rows} events into {catalog_name}.{SCHEMA_NAME}...")
        client.write_data(
            None,
            Mutation(table_uuid=table_uuid, upserts=seed_rows(args.rows, args.seed)),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author=AUTHOR,
            comment="seed events",
        )

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

        # The scan and the client-side grouping are timed together on purpose:
        # both are work the caller has to do to get an answer out of ReadData,
        # and charging only the scan would flatter the arm.
        print("\nAsking for revenue by region, twice...")
        grpc_start = time.perf_counter()
        scanned = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        grpc_groups = group_client_side(scanned)
        grpc_ms = (time.perf_counter() - grpc_start) * 1000

        fqn = f"{catalog_name}.{SCHEMA_NAME}.{TABLE_NAME}"
        # One Flight SQL connection, pinned to this catalog at handshake the way
        # a Postgres connection is pinned to one database.
        sql = PencaClient.from_settings(catalog=catalog_name, branch="main")
        sql_start = time.perf_counter()
        sql_groups = sql.execute_query(
            "SELECT region, count(*) AS events, sum(amount_cents) AS total_cents "
            f"FROM {fqn} GROUP BY region ORDER BY region"
        )
        sql_ms = (time.perf_counter() - sql_start) * 1000

        print("\n--- Aggregate via gRPC ReadData (rows shipped, grouped here) ---")
        print_table(grpc_groups)

        print("\n--- Aggregate via Flight SQL (GROUP BY pushed into the engine) ---")
        print_table(sql_groups)

        print("\n--- Analytical query latency ---")
        print(
            pa.table(
                {
                    "path": ["gRPC", "SQL"],
                    "ms": [round(grpc_ms, 3), round(sql_ms, 3)],
                }
            )
            .to_pandas()
            .to_markdown(index=False)
        )
        print(
            f"\nSame answer, same {args.rows} rows, same columnar copy. The gRPC "
            f"arm moved every row to get it; the SQL arm moved "
            f"{sql_groups.num_rows}."
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
