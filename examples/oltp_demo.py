#!/usr/bin/env python3
"""A primary-key point lookup against open columnar files on object storage.

One feature: fetch a single row by its primary key. The interesting part is
where the row lives. A columnar layout is built for scans, so the fair question
to ask of a lakehouse is whether fetching one row is still cheap once the data
has been persisted, snapshotted AND purged into open columnar files — all three,
because persist leaves the rows physically in the hot tier, so the read plan
still attaches a hot arm to every lookup. Purge is what deletes them, and only
then is the read all-cold, which is the shape worth timing. Purge can advance no
further than the snapshot, which is why snapshot sits between the two. This
script drives the table to that steady state first, then times the lookup.

The same row is fetched two ways, and they converge. The gRPC arm sends `ids=`,
a primary-key restriction the server resolves to a row identity. The SQL arm
sends `WHERE account_id = ...` over Flight SQL, and the gateway extracts that
primary-key equality into the *same* `ids` restriction — the WHERE fragment is
then not pushed with the read, so nothing evaluates it over the columnar files —
so both arms land on the same keyed read. Neither one scans.

What the SQL arm pays on top is the SQL itself: parsing and logical planning on
each execution, and the driver's extra round trips to get the plan and then the
data.

Both numbers are measured live on your machine; nothing is baked into this file.

Seeding and the lifecycle steps use the gRPC client, because neither bulk
loading a table nor driving persist/snapshot/purge is something SQL can express.
The lookup itself is shown both ways, because that is the subject.

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
    a per-lookup figure should be reporting.

    Returns the result as well as the timing so the caller can check *what* came
    back — a latency figure for a lookup that quietly returned nothing would be
    worse than no figure at all.
    """
    lookup()

    start = time.perf_counter()
    for _ in range(reps):
        found = lookup()

    elapsed = time.perf_counter() - start

    return (elapsed / reps) * 1000, found


def check_found(found: pa.Table, target_id: int, arm: str) -> None:
    """Fail the run unless this arm returned exactly the row it asked for."""
    got = found.column("account_id").to_pylist()
    if got != [target_id]:
        msg = f"the {arm} lookup returned account_ids {got}, expected [{target_id}]"
        raise RuntimeError(msg)


def _watermark(response, field: str) -> str:
    """A lifecycle watermark, or ``none`` when the call was a no-op."""
    return str(getattr(response, field)) if response.HasField(field) else "none"


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
    # The middle of the key range, so the lookup is not trivially the first or
    # last row in any layout the storage happens to choose.
    target_id = args.rows // 2

    client = PencaClient.from_settings()
    catalog_name = f"demo_{uuid4().hex[:8]}"
    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, AUTHOR)

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
        #
        # Purge is the load-bearing third step, not a tidy-up. Persist copies
        # rows to cold but leaves them in the hot tables, and the plan attaches a
        # hot arm whenever any hot row exists — so without this the reads below
        # would be hot+cold and "cold" would be a mislabel. It is the delete that
        # matters here, not the watermark: the hot/cold fence is already at the
        # snapshot before purge runs. Purge can advance no further than the
        # snapshot, hence the order.
        print("Persisting, snapshotting and purging to cold columnar storage...")
        persisted = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        snapshotted = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        purged = client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        # Printed, not asserted. Each of these is no-op-capable, and on a stack
        # whose lifecycle scheduler is running it may legitimately have done the
        # work already — so an unset watermark here is not proof of failure. The
        # smoke test runs against a profile with the scheduler idle, where these
        # calls do the work and the values must be present.
        print(
            f"  persisted_at={_watermark(persisted, 'persisted_at_micros')}"
            f"  snapshotted_at={_watermark(snapshotted, 'snapshotted_at_micros')}"
            f"  purged_at={_watermark(purged, 'purged_at_micros')}"
        )

        fqn = f"{catalog_name}.{SCHEMA_NAME}.{TABLE_NAME}"
        select = (
            f"SELECT account_id, owner, balance FROM {fqn} "
            f"WHERE account_id = {target_id}"
        )

        # `ids`, not `filter`. This is the primary-key point-lookup restriction:
        # the server derives the row identity itself and probes for it. A
        # `filter="account_id = N"` would instead read the columnar data and
        # evaluate a predicate over it — a scan, however small, which is the
        # opposite of what this script claims to show. On a snapshot-only plan
        # with no value filter this is the shape that skips query planning
        # altogether.
        ids = pa.table(
            {"account_id": [target_id]},
            schema=pa.schema([ACCOUNTS_SCHEMA.field("account_id")]),
        )

        def grpc_lookup() -> pa.Table:
            return client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch_uuid,
                ids=ids,
            )

        # One Flight SQL connection, pinned to this catalog at handshake the way
        # a Postgres connection is pinned to one database.
        sql = PencaClient.from_settings(catalog=catalog_name, branch="main")

        print(f"\nLooking up account {target_id} ({args.reps}x per arm)...")
        grpc_ms, found = time_lookups(grpc_lookup, args.reps)
        sql_ms, sql_found = time_lookups(lambda: sql.execute_query(select), args.reps)
        # Every arm, not just the one printed below. A lookup that quietly
        # returned nothing would otherwise post a flatteringly fast number and
        # exit 0 — and the faster it got, the more wrong it would be.
        check_found(found, target_id, "gRPC")
        check_found(sql_found, target_id, "SQL")

        print("\n--- The row we looked up ---")
        print(found.to_pandas().to_markdown(index=False))

        print("\n--- Point lookup latency on cold columnar (mean per lookup) ---")
        print_table(
            {
                "path": ["gRPC", "SQL"],
                "ms": [round(grpc_ms, 3), round(sql_ms, 3)],
            }
        )
        print(
            f"\nOne row out of {args.rows}, out of open columnar files on object "
            f"storage. Both arms made the same keyed read — the SQL arm's "
            f"primary-key equality is extracted into the same restriction the "
            f"gRPC arm sends. What the SQL arm pays on top is the SQL itself: "
            f"parsing and planning on each execution, plus the driver's extra "
            f"round trips."
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
