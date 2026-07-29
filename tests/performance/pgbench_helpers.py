"""pgbench (TPC-B) benchmark helpers — Penca load/workload + Postgres baseline.

Dedicated module (kept out of ``performance_helpers.py`` to avoid bloating
it) holding the four pgbench table schemas, the Penca bulk-load + TPC-B
workload drivers, and the hand-rolled ``psycopg`` baseline.
"""

from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass
from uuid import uuid4

import psycopg
import pyarrow as pa
from penca_client.types import Mutation

from .performance_helpers import _drive_system_tables_cold

# Table names — shared by the Penca and Postgres sides so the workload
# builds identical access patterns against both engines.
PGBENCH_TABLES = (
    "pgbench_accounts",
    "pgbench_branches",
    "pgbench_tellers",
    "pgbench_history",
)

# Rows per write_data call for the large accounts table — bounds the Arrow
# IPC payload. At scale 1 accounts fit in a single chunk; chunking only kicks
# in at higher scale factors.
_LOAD_CHUNK = 100_000

# Fixed epoch-micros origin so mtime is deterministic across runs and engines.
_BASE_MTIME = 1_700_000_000_000_000

_ACCOUNTS_SCHEMA = pa.schema(
    [
        pa.field("aid", pa.int64()),
        pa.field("bid", pa.int64()),
        pa.field("abalance", pa.int64()),
        pa.field("filler", pa.utf8()),
    ]
)
_BRANCHES_SCHEMA = pa.schema(
    [
        pa.field("bid", pa.int64()),
        pa.field("bbalance", pa.int64()),
        pa.field("filler", pa.utf8()),
    ]
)
_TELLERS_SCHEMA = pa.schema(
    [
        pa.field("tid", pa.int64()),
        pa.field("bid", pa.int64()),
        pa.field("tbalance", pa.int64()),
        pa.field("filler", pa.utf8()),
    ]
)
_HISTORY_SCHEMA = pa.schema(
    [
        pa.field("hid", pa.int64()),
        pa.field("tid", pa.int64()),
        pa.field("bid", pa.int64()),
        pa.field("aid", pa.int64()),
        pa.field("delta", pa.int64()),
        pa.field("mtime", pa.int64()),
        pa.field("filler", pa.utf8()),
    ]
)

# table name -> (arrow schema, primary keys). pgbench_history's ``hid`` is a
# synthetic PK; real pgbench's history table is a PK-less append log.
_TABLE_DEFS = {
    "pgbench_accounts": (_ACCOUNTS_SCHEMA, ["aid"]),
    "pgbench_branches": (_BRANCHES_SCHEMA, ["bid"]),
    "pgbench_tellers": (_TELLERS_SCHEMA, ["tid"]),
    "pgbench_history": (_HISTORY_SCHEMA, ["hid"]),
}


# The seven Flight SQL statements issued per TPC-B transaction, in order.
# Each is its own round trip; ``run_pgbench_tpcb`` times them separately so the
# per-statement latency breakdown shows where a transaction's wall time goes.
_TPCB_STMTS = (
    "begin",
    "update_accounts",
    "select_abalance",
    "update_tellers",
    "update_branches",
    "insert_history",
    "commit",
)


@dataclass(frozen=True, slots=True)
class TpcbOutcome:
    """What ``run_pgbench_tpcb`` reports back for the test's correctness asserts.

    ``tracked_aid`` is one account the runner followed across the workload;
    ``expected_abalance`` is the sum of deltas it applied to that account,
    which the test compares against ``SELECT abalance ... WHERE aid =
    tracked_aid`` to prove the ``UPDATE += delta`` path actually landed.
    ``stmt_mean_ms`` is the mean wall-clock latency of each of the seven
    per-transaction statements (keys = ``_TPCB_STMTS``) — the per-statement
    profile that explains the per-transaction total. ``stmt_secs`` is the raw
    accumulated wall-clock per statement across the whole run (same keys), so the
    caller can persist each statement as its own recorded series (CHA-501).
    ``drain_secs`` is the wall-clock spent in the periodic hot-log drain
    (persist→snapshot→purge); the caller EXCLUDES it from the headline TPS since
    production runs that GC in the background, not serially in the txn path.
    """

    history_rows: int
    tracked_aid: int
    expected_abalance: int
    stmt_mean_ms: dict[str, float]
    stmt_secs: dict[str, float]
    drain_secs: float


def _timed_update(stmt_secs: dict[str, float], key: str, client, sql: str) -> None:
    """Run a Flight SQL update and add its round-trip time to ``stmt_secs[key]``."""
    start = time.perf_counter()
    client.execute_update(sql)
    stmt_secs[key] += time.perf_counter() - start


def _timed_query(stmt_secs: dict[str, float], key: str, client, sql: str):
    """Run a Flight SQL query, accumulate its round-trip time, return the table."""
    start = time.perf_counter()
    result = client.execute_query(sql)
    stmt_secs[key] += time.perf_counter() - start
    return result


@dataclass(frozen=True, slots=True)
class _TpcbTxn:
    """One TPC-B transaction's parameters, drawn once and replayed by both
    engines so their access patterns stay identical."""

    aid: int
    bid: int
    tid: int
    delta: int
    hid: int
    mtime: int


def _next_tpcb_txn(rng: random.Random, scale: int, i: int) -> _TpcbTxn:
    """Draw one TPC-B transaction's parameters (pgbench tpcb-like rules).

    The four ``rng.randint`` draws here are the *single* source of the access
    pattern both the Penca and Postgres runners replay (the SELECT step takes
    no draw); ``hid`` / ``mtime`` derive from the iteration index. Centralizing
    the draw order makes cross-engine RNG parity structural rather than a
    comment each runner has to independently honor.
    """
    return _TpcbTxn(
        aid=rng.randint(1, scale * 100_000),
        bid=rng.randint(1, scale),
        tid=rng.randint(1, scale * 10),
        delta=rng.randint(-5000, 5000),
        hid=i + 1,
        mtime=_BASE_MTIME + i,
    )


def setup_pgbench_schema(client, scale: int) -> dict:
    """Create the catalog/schema + four pgbench tables; return a context dict."""
    catalog_name = f"pgbench_cat_{uuid4().hex[:8]}"
    schema_name = "pgbench_schema"
    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
    # Rebind the default catalog so create_table / write_data / Flight SQL
    # all target this catalog (mirrors setup_performance_schema).
    client.catalog = catalog_name
    schema_uuid = client.create_schema(
        schema_name,
        catalog_uuid=catalog_uuid,
        author="pgbench",
        comment="pgbench schema",
    )
    # Experiment knob (CHA segment-clustering spike): cluster pgbench_accounts
    # by the given comma-separated keys so its snapshot segments come out as
    # contiguous ranges and the snapshot-tier pruner can skip them. Empty
    # (default) keeps the pre-clustering behavior.
    accounts_cluster_keys = [
        k for k in os.environ.get("PGBENCH_ACCOUNTS_CLUSTER_KEYS", "").split(",") if k
    ]

    tables = {}
    for table_name in PGBENCH_TABLES:
        arrow_schema, primary_keys = _TABLE_DEFS[table_name]
        clustering_keys = (
            accounts_cluster_keys if table_name == "pgbench_accounts" else []
        )
        table_uuid = client.create_table(
            table_name,
            arrow_schema,
            primary_keys=primary_keys,
            clustering_keys=clustering_keys,
            schema_uuid=schema_uuid,
            author="pgbench",
            comment=f"pgbench create {table_name}",
        )
        tables[table_name] = {"name": table_name, "table_uuid": table_uuid}

    context: dict = {
        "catalog_uuid": catalog_uuid,
        "catalog_name": catalog_name,
        "main_branch_uuid": main_branch_uuid,
        "schema_uuid": schema_uuid,
        "schema_name": schema_name,
        "tables": tables,
    }
    # CHA-501: drive the system metadata tables cold AFTER the DDL (as
    # setup_performance_schema does), so every statement's table-identifier
    # resolve consults the cold snapshot segment list + the shared snapshot-list
    # cache instead of reading Postgres hot. Without this the system tables stay
    # hot (hot_min == 0) and the CHA-472 cache is never exercised by TPC-B.
    _drive_system_tables_cold(client, context)
    return context


def _accounts_table(aid_start: int, count: int) -> pa.Table:
    aids = list(range(aid_start, aid_start + count))
    return pa.table(
        {
            "aid": aids,
            "bid": [(aid - 1) // 100_000 + 1 for aid in aids],
            "abalance": [0] * count,
            "filler": [""] * count,
        },
        schema=_ACCOUNTS_SCHEMA,
    )


def _branches_table(scale: int) -> pa.Table:
    bids = list(range(1, scale + 1))
    return pa.table(
        {"bid": bids, "bbalance": [0] * scale, "filler": [""] * scale},
        schema=_BRANCHES_SCHEMA,
    )


def _tellers_table(scale: int) -> pa.Table:
    tids = list(range(1, scale * 10 + 1))
    return pa.table(
        {
            "tid": tids,
            "bid": [(tid - 1) // 10 + 1 for tid in tids],
            "tbalance": [0] * (scale * 10),
            "filler": [""] * (scale * 10),
        },
        schema=_TELLERS_SCHEMA,
    )


def _upsert(client, context: dict, table_name: str, table: pa.Table) -> None:
    client.write_data(
        None,
        Mutation(
            table_uuid=context["tables"][table_name]["table_uuid"],
            upserts=table,
        ),
        author="pgbench",
        comment=f"pgbench load {table_name}",
        schema_uuid=context["schema_uuid"],
        branch_uuid=context["main_branch_uuid"],
    )


def load_pgbench_tables(client, context: dict, scale: int) -> int:
    """Bulk-load accounts/branches/tellers at ``scale``; return total rows.

    ``pgbench_history`` starts empty (it is populated by the workload).
    """
    n_accounts = scale * 100_000
    for start in range(1, n_accounts + 1, _LOAD_CHUNK):
        count = min(_LOAD_CHUNK, n_accounts - start + 1)
        _upsert(client, context, "pgbench_accounts", _accounts_table(start, count))

    _upsert(client, context, "pgbench_branches", _branches_table(scale))
    _upsert(client, context, "pgbench_tellers", _tellers_table(scale))
    return n_accounts + scale + scale * 10


def _persist_and_snapshot(client, context: dict, table_name: str) -> None:
    ids = {
        "schema_uuid": context["schema_uuid"],
        "table_uuid": context["tables"][table_name]["table_uuid"],
        "branch_uuid": context["main_branch_uuid"],
    }
    client.persist(**ids)
    client.snapshot(**ids)


_WORKLOAD_SNAPSHOT_TABLES = ("pgbench_accounts", "pgbench_branches", "pgbench_tellers")


def snapshot_pgbench_tables(
    client, context: dict, tables: tuple[str, ...] = _WORKLOAD_SNAPSHOT_TABLES
) -> None:
    """Persist + snapshot the given tables so reads resolve against the cold
    (snapshotted Lance) tier instead of the hot upsert log, then warm the
    in-memory segment cache with a full scan of each so the caller measures
    warm-cache cold reads, not first-touch S3 fetches.

    Defaults to the three tables the TPC-B workload reads; ``pgbench_history``
    starts empty there (written only by the workload) so it stays hot. The
    OLAP test passes its own list (accounts + the pre-loaded history).
    """
    for table_name in tables:
        _persist_and_snapshot(client, context, table_name)
        # Full scan pulls the snapshotted Lance segments into the in-memory
        # cache so later point reads don't pay first-touch S3 fetch.
        client.execute_query(f"SELECT * FROM {_fqn(context, table_name)}")


def _fqn(context: dict, table_name: str) -> str:
    return f"{context['catalog_name']}.{context['schema_name']}.{table_name}"


def _drain_pgbench_tables(client, context: dict) -> None:
    """Persist → snapshot → purge the four pgbench data tables, mirroring the
    production lifecycle scheduler's continuous cadence.

    Driven from the TPC-B harness because the perf profile pins
    ``SCHEDULER_TICK_INTERVAL_SECONDS=-1`` (``docker/test.env``); without a drain
    the hot upsert log grows unbounded across the run — at scale 1 the 1-row
    ``pgbench_branches`` / 10-row ``pgbench_tellers`` accumulate one hot version
    per transaction, so every arithmetic ``UPDATE``'s read-modify-write merges a
    growing hot stack (O(N) per-txn cost, a no-GC pathology). Purge is
    committed-only CDC, so an open tx's uncommitted RMW rows (``commit_micros IS
    NULL``) are retained → read-your-own-writes stays correct; only committed data
    moves to the cold snapshot tier.
    """
    for table_name in PGBENCH_TABLES:
        _persist_and_snapshot(client, context, table_name)
        client.purge(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["tables"][table_name]["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )


def run_pgbench_tpcb(
    client,
    context: dict,
    scale: int,
    n_transactions: int,
    seed: int,
    drain_every: int = 0,
) -> TpcbOutcome:
    """Run ``n_transactions`` TPC-B transactions against Penca over Flight SQL.

    Each iteration is a real Penca transaction (Flight SQL ``BEGIN`` …
    ``COMMIT``) carrying the five pgbench statements: 3 arithmetic ``UPDATE``s
    (``SET col = col + delta``, the faithful mapping of ``UPDATE += delta``
    onto the SQL path rather than a read-modify-write upsert), a PK-point
    ``SELECT`` (RYOW inside the open tx), and an ``INSERT`` into the synthetic
    ``hid``-keyed history log. Parameters come from ``_next_tpcb_txn`` — the
    same shared draw the Postgres baseline uses — so both engines exercise the
    same access pattern.

    Returns the post-workload ``pgbench_history`` row count plus one tracked
    account's expected balance, which the test asserts against to prove the
    ``UPDATE += delta`` path landed.
    """
    if n_transactions < 1:
        raise ValueError(f"n_transactions must be >= 1, got {n_transactions}")

    rng = random.Random(seed)
    accounts = _fqn(context, "pgbench_accounts")
    tellers = _fqn(context, "pgbench_tellers")
    branches = _fqn(context, "pgbench_branches")
    history = _fqn(context, "pgbench_history")

    stmt_secs = dict.fromkeys(_TPCB_STMTS, 0.0)
    drain_secs = 0.0
    applied: dict[int, int] = {}
    tracked_aid: int | None = None
    for i in range(n_transactions):
        txn = _next_tpcb_txn(rng, scale, i)
        if tracked_aid is None:
            tracked_aid = txn.aid

        applied[txn.aid] = applied.get(txn.aid, 0) + txn.delta
        _timed_update(stmt_secs, "begin", client, "BEGIN")
        _timed_update(
            stmt_secs,
            "update_accounts",
            client,
            f"UPDATE {accounts} SET abalance = abalance + {txn.delta} WHERE aid = {txn.aid}",
        )
        # RYOW: the in-tx SELECT must observe this tx's own UPDATE — the
        # running balance for this aid. Asserting it makes the documented
        # read-your-own-write guarantee a checked invariant rather than an
        # incidental discarded call (the read cost is still in the timing).
        ryow = _timed_query(
            stmt_secs,
            "select_abalance",
            client,
            f"SELECT abalance FROM {accounts} WHERE aid = {txn.aid}",
        )
        assert ryow.column("abalance").to_pylist() == [applied[txn.aid]]
        _timed_update(
            stmt_secs,
            "update_tellers",
            client,
            f"UPDATE {tellers} SET tbalance = tbalance + {txn.delta} WHERE tid = {txn.tid}",
        )
        _timed_update(
            stmt_secs,
            "update_branches",
            client,
            f"UPDATE {branches} SET bbalance = bbalance + {txn.delta} WHERE bid = {txn.bid}",
        )
        _timed_update(
            stmt_secs,
            "insert_history",
            client,
            f"INSERT INTO {history} (hid, tid, bid, aid, delta, mtime, filler) "
            f"VALUES ({txn.hid}, {txn.tid}, {txn.bid}, {txn.aid}, {txn.delta}, {txn.mtime}, '')",
        )
        _timed_update(stmt_secs, "commit", client, "COMMIT")

        # CHA-501: drain the hot log on a cadence (mirrors production's scheduler
        # tick, which the perf profile disables). Between transactions, outside
        # the per-statement timed blocks. Its wall-clock is accumulated into
        # `drain_secs` (kept out of BOTH the per-statement means and — via the
        # caller — the headline `elapsed`/TPS), because the production scheduler
        # runs this GC in the background, not serially in the txn path.
        if drain_every and (i + 1) % drain_every == 0:
            drain_start = time.perf_counter()
            _drain_pgbench_tables(client, context)
            drain_secs += time.perf_counter() - drain_start

    stmt_mean_ms = {k: stmt_secs[k] / n_transactions * 1000 for k in _TPCB_STMTS}
    count_table = client.execute_query(f"SELECT COUNT(*) AS c FROM {history}")
    history_rows = count_table.column("c").to_pylist()[0]
    assert tracked_aid is not None  # guaranteed by the n_transactions >= 1 guard
    return TpcbOutcome(
        history_rows=history_rows,
        tracked_aid=tracked_aid,
        expected_abalance=applied[tracked_aid],
        stmt_mean_ms=stmt_mean_ms,
        stmt_secs=dict(stmt_secs),
        drain_secs=drain_secs,
    )


def print_stmt_breakdown(stmt_mean_ms: dict[str, float]) -> None:
    """Print the Penca per-statement mean latency for one TPC-B transaction.

    Each statement is its own Flight SQL round trip, so the breakdown shows
    which statement dominates the per-transaction wall time. The ``SELECT``
    takes the ADBC prepared-statement path (multiple round trips per call),
    while the updates take the single ``DoPutStatementUpdate`` path.
    """
    print("\n### pgbench Penca per-statement latency (mean)\n")
    print("| Statement | Mean latency |")
    print("|-----------|-------------:|")
    for label in _TPCB_STMTS:
        print(f"| {label} | {stmt_mean_ms[label]:.1f} ms |")

    print(f"| **transaction total** | **{sum(stmt_mean_ms.values()):.1f} ms** |")


def create_pgbench_baseline_schema(conn) -> None:
    """Create the equivalent four pgbench tables in the Postgres baseline.

    Idempotent (``DROP ... IF EXISTS`` first) so it is safe to call once per
    test against the shared Postgres instance — the Penca side gets the same
    isolation for free by minting a fresh ``uuid4`` catalog per setup. Types
    are widened (``BIGINT`` / ``TEXT``) to match the Arrow schemas, and
    ``pgbench_history`` carries the same synthetic ``hid`` PK as the Penca
    side so the comparison stays apples-to-apples.
    """
    conn.execute("DROP TABLE IF EXISTS pgbench_history")
    conn.execute("DROP TABLE IF EXISTS pgbench_accounts")
    conn.execute("DROP TABLE IF EXISTS pgbench_tellers")
    conn.execute("DROP TABLE IF EXISTS pgbench_branches")
    conn.execute(
        "CREATE TABLE pgbench_accounts "
        "(aid BIGINT PRIMARY KEY, bid BIGINT, abalance BIGINT, filler TEXT)"
    )
    conn.execute(
        "CREATE TABLE pgbench_branches "
        "(bid BIGINT PRIMARY KEY, bbalance BIGINT, filler TEXT)"
    )
    conn.execute(
        "CREATE TABLE pgbench_tellers "
        "(tid BIGINT PRIMARY KEY, bid BIGINT, tbalance BIGINT, filler TEXT)"
    )
    conn.execute(
        "CREATE TABLE pgbench_history "
        "(hid BIGINT PRIMARY KEY, tid BIGINT, bid BIGINT, aid BIGINT, "
        "delta BIGINT, mtime BIGINT, filler TEXT)"
    )


def load_pgbench_baseline(conn, scale: int) -> int:
    """Bulk-load accounts/branches/tellers at ``scale``; return total rows.

    Uses the same pipelined ``executemany`` shape as
    ``insert_postgres_baseline``; ``pgbench_history`` starts empty.
    """
    n_accounts = scale * 100_000
    accounts = [
        (aid, (aid - 1) // 100_000 + 1, 0, "") for aid in range(1, n_accounts + 1)
    ]
    branches = [(bid, 0, "") for bid in range(1, scale + 1)]
    tellers = [(tid, (tid - 1) // 10 + 1, 0, "") for tid in range(1, scale * 10 + 1)]
    with conn.cursor() as cur, conn.pipeline():
        cur.executemany(
            "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) "
            "VALUES (%s, %s, %s, %s)",
            accounts,
        )
        cur.executemany(
            "INSERT INTO pgbench_branches (bid, bbalance, filler) VALUES (%s, %s, %s)",
            branches,
        )
        cur.executemany(
            "INSERT INTO pgbench_tellers (tid, bid, tbalance, filler) "
            "VALUES (%s, %s, %s, %s)",
            tellers,
        )

    return n_accounts + scale + scale * 10


def run_pgbench_baseline_txns(conn, scale: int, n_transactions: int, seed: int) -> int:
    """Run the equivalent TPC-B workload against the Postgres baseline.

    One PG transaction per iteration (``conn`` must be ``autocommit=False``).
    Parameters come from ``_next_tpcb_txn`` — the same shared draw the Penca
    runner uses — so both engines exercise the same access pattern. Returns the
    resulting ``pgbench_history`` row count so the caller can assert the
    workload actually landed before trusting the timing.
    """
    rng = random.Random(seed)
    with conn.cursor() as cur:
        for i in range(n_transactions):
            txn = _next_tpcb_txn(rng, scale, i)
            cur.execute(
                "UPDATE pgbench_accounts SET abalance = abalance + %s WHERE aid = %s",
                (txn.delta, txn.aid),
            )
            cur.execute(
                "SELECT abalance FROM pgbench_accounts WHERE aid = %s", (txn.aid,)
            )
            cur.fetchone()
            cur.execute(
                "UPDATE pgbench_tellers SET tbalance = tbalance + %s WHERE tid = %s",
                (txn.delta, txn.tid),
            )
            cur.execute(
                "UPDATE pgbench_branches SET bbalance = bbalance + %s WHERE bid = %s",
                (txn.delta, txn.bid),
            )
            cur.execute(
                "INSERT INTO pgbench_history "
                "(hid, tid, bid, aid, delta, mtime, filler) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (txn.hid, txn.tid, txn.bid, txn.aid, txn.delta, txn.mtime, ""),
            )
            conn.commit()

        cur.execute("SELECT COUNT(*) FROM pgbench_history")
        return cur.fetchone()[0]


# An analytical query: a per-account history count + the per-branch average of
# that count, filtered + top-N — the scan/join/aggregate shape a columnar,
# vectorized engine wins on at scale. Written with explicit joins/CTEs rather
# than correlated subqueries: the natural correlated phrasing is rejected by
# Penca's Flight SQL today — the single-level COUNT subquery by CHA-402
# (a get_flight_info-vs-DoGet schema-nullability mismatch) and the doubly-nested
# branch subquery by CHA-401 (a DataFusion decorrelation gap). Both engines run
# this same decorrelated SQL so the comparison stays apples-to-apples.
# {accounts} / {history} are filled with FQNs for Penca, bare names for Postgres.
_OLAP_SQL = """\
WITH acct AS (
    SELECT a.aid,
           a.bid,
           a.abalance,
           COALESCE(h.cnt, 0) AS my_txns
    FROM {accounts} a
    LEFT JOIN (SELECT aid, count(*) AS cnt FROM {history} GROUP BY aid) h
           ON h.aid = a.aid
),
branch AS (
    SELECT bid, avg(my_txns) AS branch_avg_txns
    FROM acct
    GROUP BY bid
)
SELECT acct.aid,
       acct.bid,
       acct.abalance,
       acct.my_txns,
       branch.branch_avg_txns
FROM acct
JOIN branch ON branch.bid = acct.bid
WHERE acct.abalance > 0
ORDER BY acct.my_txns DESC, acct.aid ASC
LIMIT 20"""


# The OLAP fixture spreads accounts across this many branches (vs pgbench's
# single branch per 100k accounts at scale 1) so the query's per-branch
# `GROUP BY bid` aggregate actually discriminates between buckets — a wrong-
# bucketing regression then changes a branch average and the cross-engine
# assertion catches it.
_OLAP_BRANCHES = 16


def _olap_accounts_table(aid_start: int, count: int) -> pa.Table:
    # abalance = aid so every row passes the OLAP query's `abalance > 0` filter.
    aids = list(range(aid_start, aid_start + count))
    return pa.table(
        {
            "aid": aids,
            "bid": [(aid - 1) % _OLAP_BRANCHES + 1 for aid in aids],
            "abalance": aids,
            "filler": [""] * count,
        },
        schema=_ACCOUNTS_SCHEMA,
    )


def _history_table(
    hid_start: int, count: int, n_accounts: int, rng: random.Random
) -> pa.Table:
    hids = list(range(hid_start, hid_start + count))
    aids = [rng.randint(1, n_accounts) for _ in hids]
    return pa.table(
        {
            "hid": hids,
            "tid": [1] * count,
            "bid": [1] * count,
            "aid": aids,
            "delta": [0] * count,
            "mtime": [_BASE_MTIME] * count,
            "filler": [""] * count,
        },
        schema=_HISTORY_SCHEMA,
    )


def load_pgbench_olap_data(
    client, context: dict, scale: int, n_history: int, seed: int
) -> tuple[int, int]:
    """Load accounts (every abalance > 0) + ``n_history`` history rows for the
    OLAP query. history ``aid``s are drawn from a seed-matched RNG so the
    Penca and Postgres result sets are identical. Returns (n_accounts, n_history).
    """
    n_accounts = scale * 100_000
    for start in range(1, n_accounts + 1, _LOAD_CHUNK):
        count = min(_LOAD_CHUNK, n_accounts - start + 1)
        _upsert(client, context, "pgbench_accounts", _olap_accounts_table(start, count))

    rng = random.Random(seed)
    for start in range(1, n_history + 1, _LOAD_CHUNK):
        count = min(_LOAD_CHUNK, n_history - start + 1)
        _upsert(
            client,
            context,
            "pgbench_history",
            _history_table(start, count, n_accounts, rng),
        )

    return n_accounts, n_history


def load_pgbench_olap_baseline(
    conn, scale: int, n_history: int, seed: int
) -> tuple[int, int]:
    """Load the same accounts (abalance > 0) + history into the Postgres baseline.

    Uses an RNG seeded identically to ``load_pgbench_olap_data`` and the same
    hid order, so both engines hold the same history ``aid`` distribution.
    """
    n_accounts = scale * 100_000
    accounts = [
        (aid, (aid - 1) % _OLAP_BRANCHES + 1, aid, "")
        for aid in range(1, n_accounts + 1)
    ]
    rng = random.Random(seed)
    history = [
        (hid, 1, 1, rng.randint(1, n_accounts), 0, _BASE_MTIME, "")
        for hid in range(1, n_history + 1)
    ]
    with conn.cursor() as cur, conn.pipeline():
        cur.executemany(
            "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) "
            "VALUES (%s, %s, %s, %s)",
            accounts,
        )
        cur.executemany(
            "INSERT INTO pgbench_history (hid, tid, bid, aid, delta, mtime, filler) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            history,
        )

    return n_accounts, n_history


def run_pgbench_olap_penca(client, context: dict) -> tuple:
    """Run the OLAP query against Penca over Flight SQL; return (table, secs)."""
    sql = _OLAP_SQL.format(
        accounts=_fqn(context, "pgbench_accounts"),
        history=_fqn(context, "pgbench_history"),
    )
    start = time.perf_counter()
    table = client.execute_query(sql)
    return table, time.perf_counter() - start


def run_pgbench_olap_baseline(conn, timeout_s: int) -> tuple:
    """Run the OLAP query against Postgres under a ``statement_timeout`` so a
    pathological plan can't hang the suite. Same SQL as the Penca side.

    Returns ``(rows, secs)`` on completion, or ``(None, None)`` if it timed out.
    """
    sql = _OLAP_SQL.format(accounts="pgbench_accounts", history="pgbench_history")
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = '{timeout_s}s'")
        start = time.perf_counter()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except psycopg.errors.QueryCanceled:
            conn.rollback()
            return None, None

        return rows, time.perf_counter() - start
