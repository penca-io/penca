"""pgbench (TPC-B) benchmark — Penca vs a hand-rolled Postgres baseline.

Mirrors PostgreSQL ``pgbench``'s default ``tpcb-like`` benchmark: the
four-table schema and the five-statement TPC-B transaction (3 UPDATE +
1 SELECT + 1 INSERT), driven against Penca and against a direct
``psycopg`` connection for the Penca-vs-Postgres comparison the rest of
this suite uses. We do NOT shell out to the real ``pgbench`` binary.

Deviations from real pgbench (CHA-396):

1. ``pgbench_history`` has no primary key in real pgbench (a pure append
   log). Penca tables are PK-keyed upserts, so we add a synthetic ``hid``
   PK, monotonically incremented per INSERT. The Postgres baseline carries
   the same synthetic ``hid`` PK so the comparison stays apples-to-apples.
2. ``UPDATE balance = balance + :delta`` is issued as a Flight SQL
   ``UPDATE ... SET col = col + :delta WHERE pk = :id`` (not a
   read-modify-write upsert), so Penca exercises the same SQL UPDATE path
   the Postgres baseline does.
3. Real pgbench uses ``integer`` balances and ``char(N)`` filler; we widen
   to ``int64`` / ``utf8`` (TEXT on Postgres) consistently on both engines.
   ``filler`` content is also left empty rather than blank-padded to
   pgbench's ``char(84)`` / ``char(88)`` width, so absolute payload sizes
   understate a real pgbench load — both engines use empty filler, so the
   Penca-vs-Postgres comparison stays apples-to-apples.
4. ``mtime`` is stored as ``int64`` epoch-micros (real pgbench:
   ``timestamp``) — supplied explicitly per INSERT so neither engine relies
   on ``CURRENT_TIMESTAMP`` literal parsing.
5. Single-client / sequential. pgbench's ``-c`` / ``-j`` concurrent client
   load is out of scope (a possible follow-up).

The Flight SQL workload (UPDATE / SELECT / INSERT) requires the Rust
backend, so ``test_pgbench_tpcb`` carries the ``_requires_rust`` marker
(mirrors ``performance_query_test.py``). Configure via ``PGBENCH_SCALE``
(default 1), ``PGBENCH_TX`` (default 1000), ``PGBENCH_SEED`` (default 42),
and ``PGBENCH_STATE`` (default ``cold`` = persist+snapshot+cache-warm the
base so reads resolve against the cold snapshotted tier; ``hot`` keeps the
base in the hot upsert log). The TPC-B test prints a per-statement latency
breakdown so the per-transaction total is attributable to each of the seven
Flight SQL round trips.

``test_pgbench_olap`` adds an HTAP analytical query (per-account history
count + per-branch average, top-N) over the cold-snapshotted base, Penca vs
the same SQL on Postgres. It is written with explicit joins rather than
correlated subqueries, which Penca's Flight SQL cannot yet accept
(CHA-402 schema mismatch; the nested form also hits CHA-401).

Run via ``PENCA_BACKEND=rust just perf-test performance_pgbench_test.py``.
"""

from __future__ import annotations

import math
import os
import time

import psycopg
import pytest
from psycopg import sql

from .performance_helpers import (
    PerfResult,
    make_client,
    pg_conninfo,
)
from .pgbench_helpers import (
    create_pgbench_baseline_schema,
    load_pgbench_baseline,
    load_pgbench_olap_baseline,
    load_pgbench_olap_data,
    load_pgbench_tables,
    print_stmt_breakdown,
    run_pgbench_baseline_txns,
    run_pgbench_olap_baseline,
    run_pgbench_olap_penca,
    run_pgbench_tpcb,
    setup_pgbench_schema,
    snapshot_pgbench_tables,
)

SCALE = int(os.environ.get("PGBENCH_SCALE", "1"))
N_TRANSACTIONS = int(os.environ.get("PGBENCH_TX", "1000"))
SEED = int(os.environ.get("PGBENCH_SEED", "42"))
# CHA-501: drain the hot log every N committed transactions (persist→snapshot→
# purge the four data tables), standing in for the production scheduler tick the
# perf profile disables (SCHEDULER_{PERSIST,SNAPSHOT}_TICK_INTERVAL_SECONDS=-1)
# while keeping the
# other perf tests scheduler-off + deterministic. Default 100 keeps the per-RMW
# hot stack shallow without dominating the run; 0 disables (the raw no-GC case).
PGBENCH_DRAIN_EVERY = int(os.environ.get("PGBENCH_DRAIN_EVERY", "100"))
# Storage tier the workload's read-modify-write resolves against: "cold"
# (default) persists + snapshots + cache-warms the loaded tables so reads hit
# the cold (snapshotted Lance) tier — the representative steady-state shape;
# "hot" leaves them in the hot upsert log (the worst case for point reads).
PGBENCH_STATE = os.environ.get("PGBENCH_STATE", "cold")
_STATE_LABELS = {"hot": "all_hot", "cold": "all_cold_snapshotted"}
if PGBENCH_STATE not in _STATE_LABELS:
    raise ValueError(
        f"PGBENCH_STATE must be one of {sorted(_STATE_LABELS)}, got {PGBENCH_STATE!r}"
    )

_STATE_LABEL = _STATE_LABELS[PGBENCH_STATE]
# OLAP/HTAP query: history is pre-loaded to half the accounts row count, and the
# Postgres baseline is capped so a pathological correlated-subquery plan can't
# hang the suite.
N_HISTORY = int(os.environ.get("PGBENCH_HISTORY", str(SCALE * 50_000)))
OLAP_PG_TIMEOUT_S = int(os.environ.get("PGBENCH_OLAP_PG_TIMEOUT", "60"))

_requires_rust = pytest.mark.skipif(
    os.environ.get("PENCA_BACKEND", "") != "rust",
    reason="Flight SQL requires --backend rust",
)


class TestPgbenchPerformance:
    """pgbench TPC-B load + transaction throughput, Penca vs Postgres."""

    def test_pgbench_load(self, perf_recorder):
        """Bulk-load all four pgbench tables at the scale factor; measure it."""
        client = make_client()
        context = setup_pgbench_schema(client, SCALE)

        start = time.perf_counter()
        loaded_rows = load_pgbench_tables(client, context, SCALE)
        elapsed = time.perf_counter() - start

        expected_rows = {
            "pgbench_accounts": SCALE * 100_000,
            "pgbench_branches": SCALE,
            "pgbench_tellers": SCALE * 10,
            "pgbench_history": 0,
        }
        for table_name, want in expected_rows.items():
            table = client.read_data(
                schema_uuid=context["schema_uuid"],
                table_uuid=context["tables"][table_name]["table_uuid"],
                branch_uuid=context["main_branch_uuid"],
            )
            assert table.num_rows == want, f"{table_name}: {table.num_rows} != {want}"

        with psycopg.connect(pg_conninfo(), autocommit=True) as conn:
            create_pgbench_baseline_schema(conn)
            pg_start = time.perf_counter()
            pg_loaded_rows = load_pgbench_baseline(conn, SCALE)
            pg_elapsed = time.perf_counter() - pg_start

            # Independently verify the PG load landed via real row counts
            # (mirrors the Penca read_data checks above), rather than trusting
            # the recomputed constant load_pgbench_baseline returns.
            pg_counts = {}
            for name in expected_rows:
                row = conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(name))
                ).fetchone()
                assert row is not None
                pg_counts[name] = row[0]

        assert pg_counts == expected_rows
        assert pg_loaded_rows == sum(pg_counts.values())

        perf_recorder.record(
            PerfResult(
                "pgbench_load", "n/a", loaded_rows, elapsed, pg_elapsed, unit="load"
            )
        )

    @_requires_rust
    def test_pgbench_tpcb(self, perf_recorder):
        """Run N TPC-B transactions (3 UPDATE + 1 SELECT + 1 INSERT); measure TPS."""
        client = make_client()
        context = setup_pgbench_schema(client, SCALE)
        load_pgbench_tables(client, context, SCALE)
        if PGBENCH_STATE == "cold":
            snapshot_pgbench_tables(client, context)

        start = time.perf_counter()
        outcome = run_pgbench_tpcb(
            client, context, SCALE, N_TRANSACTIONS, SEED, PGBENCH_DRAIN_EVERY
        )
        # CHA-501: exclude the periodic hot-log drain (persist→snapshot→purge)
        # from the headline TPS — production runs that GC in the background, so
        # charging it serially here would make TPS pessimistic and drain-on vs
        # drain-off (PGBENCH_DRAIN_EVERY=0) runs non-comparable.
        elapsed = time.perf_counter() - start - outcome.drain_secs

        # The workload actually landed: history grew by N and the tracked
        # account's balance equals the deltas we applied to it.
        assert outcome.history_rows == N_TRANSACTIONS
        accounts_fqn = (
            f"{context['catalog_name']}.{context['schema_name']}.pgbench_accounts"
        )
        observed = client.execute_query(
            f"SELECT abalance FROM {accounts_fqn} WHERE aid = {outcome.tracked_aid}"
        )
        assert observed.column("abalance").to_pylist() == [outcome.expected_abalance]

        with psycopg.connect(pg_conninfo(), autocommit=False) as conn:
            create_pgbench_baseline_schema(conn)
            conn.commit()
            load_pgbench_baseline(conn, SCALE)
            conn.commit()
            pg_start = time.perf_counter()
            pg_history_rows = run_pgbench_baseline_txns(
                conn, SCALE, N_TRANSACTIONS, SEED
            )
            pg_elapsed = time.perf_counter() - pg_start

            # Same seed -> same draws, so the tracked account ends at the same
            # balance on both engines. Assert it so the PG workload's effect is
            # verified (not just its row count), symmetric with the Penca side.
            pg_balance = conn.execute(
                "SELECT abalance FROM pgbench_accounts WHERE aid = %s",
                (outcome.tracked_aid,),
            ).fetchone()

        # Parallels the Penca-side history assertion: the baseline workload
        # actually committed N transactions before we record its timing.
        assert pg_history_rows == N_TRANSACTIONS
        assert pg_balance is not None
        assert pg_balance[0] == outcome.expected_abalance

        perf_recorder.record(
            PerfResult(
                "pgbench_tpcb",
                _STATE_LABEL,
                N_TRANSACTIONS,
                elapsed,
                pg_elapsed,
                operations=N_TRANSACTIONS,
                unit="transaction",
            )
        )

        # CHA-501: persist each of the seven Flight SQL statements as its own
        # series so the per-statement attribution survives in results.jsonl / the
        # SQLite history — not just the stdout breakdown below. Each statement
        # runs once per transaction, so operations == N_TRANSACTIONS.
        for label, secs in outcome.stmt_secs.items():
            perf_recorder.record(
                PerfResult(
                    f"pgbench_tpcb_{label}",
                    _STATE_LABEL,
                    N_TRANSACTIONS,
                    secs,
                    operations=N_TRANSACTIONS,
                    unit="statement",
                )
            )

        # Per-statement profile: where the ~per-transaction wall time goes.
        print(f"\n(workload read tier: {_STATE_LABEL})")
        print_stmt_breakdown(outcome.stmt_mean_ms)

    @_requires_rust
    def test_pgbench_olap(self, perf_recorder):
        """HTAP analytical query over the cold-snapshotted base, Penca vs Postgres.

        Both engines run the same explicit-join SQL (the natural correlated
        phrasing is blocked on Penca by CHA-402 / CHA-401). Penca's columnar,
        vectorized engine carries a fixed per-query overhead, so at scale 1
        Postgres wins on this small result; the gap crosses over as data grows
        (at scale 10 / 1M rows Penca is ~2x faster). The PG baseline is capped
        at ``OLAP_PG_TIMEOUT_S`` so a pathological plan can't hang the suite.
        """
        client = make_client()
        context = setup_pgbench_schema(client, SCALE)
        n_accounts, n_history = load_pgbench_olap_data(
            client, context, SCALE, N_HISTORY, SEED
        )
        snapshot_pgbench_tables(
            client, context, tables=("pgbench_accounts", "pgbench_history")
        )

        penca_table, penca_elapsed = run_pgbench_olap_penca(client, context)
        assert penca_table.num_rows == 20

        with psycopg.connect(pg_conninfo(), autocommit=True) as conn:
            create_pgbench_baseline_schema(conn)
            load_pgbench_olap_baseline(conn, SCALE, N_HISTORY, SEED)
            pg_rows, pg_elapsed = run_pgbench_olap_baseline(conn, OLAP_PG_TIMEOUT_S)

        # Correctness: when Postgres completes, Penca's top-20 must match — a
        # fast wrong answer is worse than a slow right one. Integer (aid,
        # my_txns) exactly; the float branch_avg_txns (the join+aggregate the
        # rewrite is built around) with tolerance.
        if pg_rows is not None:
            penca_pairs = list(
                zip(
                    penca_table.column("aid").to_pylist(),
                    penca_table.column("my_txns").to_pylist(),
                    strict=True,
                )
            )
            assert penca_pairs == [(row[0], row[3]) for row in pg_rows], (
                f"OLAP (aid, my_txns) mismatch: {penca_pairs}"
            )
            penca_branch = penca_table.column("branch_avg_txns").to_pylist()
            for got, pg_row in zip(penca_branch, pg_rows, strict=True):
                assert math.isclose(got, float(pg_row[4]), rel_tol=1e-9), (
                    f"branch_avg_txns mismatch: {got} != {pg_row[4]}"
                )

        pg_desc = (
            f"{pg_elapsed:.2f}s"
            if pg_elapsed is not None
            else f">{OLAP_PG_TIMEOUT_S}s (timeout)"
        )
        print(
            f"\nOLAP query: Penca {penca_elapsed:.2f}s vs Postgres {pg_desc} "
            f"({n_accounts:,} accounts, {n_history:,} history, all_cold_snapshotted)"
        )
        perf_recorder.record(
            PerfResult(
                "olap_query",
                "all_cold_snapshotted",
                n_accounts,
                penca_elapsed,
                pg_elapsed,
                result_rows=20,
            )
        )
