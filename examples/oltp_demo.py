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
Measuring them honestly takes some care, because a point lookup is around a
millisecond of real work spread across four hops with idle gaps in between —
low, bursty utilization, which is the shape a laptop's CPU governor is slowest
to clock up for. Untreated, that ramp is worth a factor of four and it lands
inside the measurement window, so the run reports where on the ramp it happened
to sit. Hence three things this script does before it believes a number. It
spends a fixed stretch of wall clock warming up, discarded, because the ramp is
a function of elapsed time rather than of request count. It reports percentiles
over a large sample instead of a mean, which no single slow request can drag.
And it interleaves the two arms rather than running them back to back, so that
neither one absorbs the machine's drift on the other's behalf.

Seeding and the lifecycle steps use the gRPC client, because neither bulk
loading a table nor driving persist/snapshot/purge is something SQL can express.
The lookup itself is shown both ways, because that is the subject.

Requires Docker services running: just penca-up
"""

from __future__ import annotations

import argparse
import math
import time
from collections.abc import Callable, Mapping
from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation, PencaClient

AUTHOR = "penca-demo"
SCHEMA_NAME = "ledger"
TABLE_NAME = "accounts"

DEFAULT_ROWS = 100_000
# A point read is sub-millisecond of real work dominated by the round trip, so
# one measurement is mostly noise. Take a distribution over repetitions instead.
# Enough of them that the tail percentile below rests on more than one or two
# samples, and few enough that the whole script still runs in well under a
# minute — the two arms together cost roughly 30ms per rep on a warm machine.
DEFAULT_REPS = 300
# Wall clock, not a repetition count, because the dominant thing being warmed is
# the CPU's clock ramp and that is measured in seconds however many requests it
# takes to get there. Long enough to hold the ramp through the measurement that
# follows; short enough to stay a rounding error in the script's runtime.
DEFAULT_WARMUP_SECONDS = 1.5
# The median says what a lookup normally costs, and the two tail figures say what
# it costs when something goes wrong — which for an OLTP claim is the more
# interesting half. p99 at DEFAULT_REPS leaves only three samples above it, so it
# moves around between runs; it is here to show the shape of the tail, not to be
# quoted as a number.
PERCENTILES = (50, 90, 99)

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


# Mapping rather than dict: both functions below only read the arms, and dict is
# invariant in its value type, so a dict of lambdas built at the call site does
# not satisfy a dict[str, Callable[[], pa.Table]] parameter.
Arms = Mapping[str, Callable[[], pa.Table]]


def warm_up(arms: Arms, seconds: float) -> None:
    """Drive both arms for ``seconds`` of wall clock, discarding everything.

    Three costs get paid here rather than inside the measurement. The first
    request on a connection pays for session setup and the table-identifier
    resolve. The server-side caches behind a keyed read fill on first touch. And
    the machine itself clocks up: a lookup is a millisecond of work per hop with
    the CPU idle in between, so a governor sees almost no load and takes seconds
    to leave its lowest frequency — which is worth about a factor of four here,
    dwarfing everything this script is trying to show.

    Both arms together, because that last cost is a property of the machine and
    not of either arm. Warming them one after the other would ramp the clock on
    the first arm's time and hand the second one a machine already at speed.
    """
    deadline = time.perf_counter() + seconds
    while time.perf_counter() < deadline:
        for lookup in arms.values():
            lookup()


def measure(
    arms: Arms, reps: int
) -> tuple[dict[str, list[float]], dict[str, pa.Table]]:
    """Per-call milliseconds and the last result, per arm.

    Round-robin rather than one arm then the other: thermal and frequency state
    drifts over the seconds this takes, and running the arms back to back would
    hand that drift to whichever went second as if it were a property of the
    surface being measured. Interleaved, both arms see the same machine.

    Returns the results alongside the timings so the caller can check *what* came
    back — a latency figure for a lookup that quietly returned nothing would be
    worse than no figure at all.
    """
    samples = {arm: [] for arm in arms}
    found = {}

    for _ in range(reps):
        for arm, lookup in arms.items():
            start = time.perf_counter()
            found[arm] = lookup()
            samples[arm].append((time.perf_counter() - start) * 1000)

    return samples, found


def percentile(ordered_ms: list[float], p: int) -> float:
    """The p-th percentile of an already-sorted sample, by nearest rank.

    Nearest rank rather than an interpolating definition: every figure this
    prints is then a request that actually happened and could be gone looking for
    in a trace, which is worth more on a latency table than the third decimal
    place of a value nothing observed.
    """
    return ordered_ms[math.ceil(p / 100 * len(ordered_ms)) - 1]


def check_found(found: pa.Table, target_id: int, arm: str) -> None:
    """Fail the run unless this arm returned exactly the row it asked for."""
    got = found.column("account_id").to_pylist()
    if got != [target_id]:
        msg = f"the {arm} lookup returned account_ids {got}, expected [{target_id}]"
        raise RuntimeError(msg)


def _watermark(response, field: str) -> str:
    """A lifecycle watermark, or ``none`` when the call was a no-op."""
    return str(getattr(response, field)) if response.HasField(field) else "none"


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
        description="A primary-key point lookup against cold columnar storage."
    )
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--reps", type=int, default=DEFAULT_REPS)
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=DEFAULT_WARMUP_SECONDS,
        help="wall clock spent on discarded lookups before measuring",
    )
    args = parser.parse_args()
    # Validate before main() opens a client: otherwise a typo'd flag fails only
    # after the catalog exists, leaving debris behind a stack trace.
    if args.rows < 2:
        parser.error("--rows must be at least 2")

    if args.reps < 1:
        parser.error("--reps must be at least 1")

    # Zero is allowed, and means "measure cold" — a legitimate thing to ask for,
    # and the only way to see from this script what the warm-up is worth.
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds cannot be negative")

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
        # with no value filter this is the shape that takes the
        # DataFusion-free snapshot seek.
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

        arms = {"gRPC": grpc_lookup, "SQL": lambda: sql.execute_query(select)}

        print(f"\nWarming up for {args.warmup_seconds}s...")
        warm_up(arms, args.warmup_seconds)

        print(f"Looking up account {target_id} ({args.reps}x per arm)...")
        samples, found = measure(arms, args.reps)
        # Every arm, not just the one printed below. A lookup that quietly
        # returned nothing would otherwise post a flatteringly fast number and
        # exit 0 — and the faster it got, the more wrong it would be.
        for arm, result in found.items():
            check_found(result, target_id, arm)

        print("\n--- The row we looked up ---")
        print_table(found["gRPC"])

        ordered = {arm: sorted(ms) for arm, ms in samples.items()}
        print(
            f"\n--- Point lookup latency on cold columnar "
            f"({args.reps} reps per arm) ---"
        )
        print_table(
            pa.table(
                {
                    "path": list(ordered),
                    **{
                        f"p{p} ms": [
                            round(percentile(ms, p), 3) for ms in ordered.values()
                        ]
                        for p in PERCENTILES
                    },
                }
            )
        )
        print(
            f"\nOne row out of {args.rows}, out of open columnar files on object "
            f"storage. Both arms made the same keyed read — the SQL arm's "
            f"primary-key equality is extracted into the same restriction the "
            f"gRPC arm sends. What the SQL arm pays on top is the SQL itself: "
            f"parsing and planning on each execution, plus the driver's extra "
            f"round trips. Both arms were interleaved on a warmed-up machine, so "
            f"the figures are steady-state; a laptop on a power-saving governor "
            f"reads several times slower until its clock ramps."
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
