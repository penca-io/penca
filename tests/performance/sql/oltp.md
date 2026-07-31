# OLTP (SQL / Flight SQL) Performance

A dead-simple, **stable** operational yardstick for the latency-sensitive OLTP
path, measured over the **Flight SQL API** (ADBC driver) — the SQL-path sibling
of the [gRPC OLTP suite](../grpc/oltp.md). It drives the *same* four point ops,
but each as a **single autocommit Flight SQL statement** rather than a native
`ReadData` / `WriteData` RPC. This is the **SQL-API later pass** that CHA-416
(the gRPC-first operational suite) explicitly parked, and the dead-simple
**single-statement** companion to the seven-statement pgbench transaction
([pgbench](../../../docs/performance.md#pgbench-tpc-b-performance) / CHA-501).

**Why a SQL mirror of the gRPC suite.** The gRPC suite deliberately measures the
engine, *not* the SQL parser / ADBC / prepared-statement machinery. Running the
identical point ops over Flight SQL and comparing **op-for-op against the gRPC
suite** isolates exactly that SQL-layer overhead — parse → plan (DataFusion) →
ADBC/prepared-statement wire actions → metadata resolve → Arrow IPC. CHA-501
inferred ~40 ms per Flight SQL statement inside a `BEGIN … COMMIT` but never
profiled a *single* one; this suite is the clean, attributable single-statement
measurement.

Four operations, each a single **autocommit** statement (no `BEGIN … COMMIT` —
that is pgbench / CHA-501):

- **`sql_point_read`** — `SELECT id, name, value FROM <t> WHERE id = <pk>`,
  repeated 100× and averaged, across the latency-relevant tiers: `all_hot`
  (merge-on-read fast path), `all_cold_snapshotted` (the steady-state production
  tier), and `hot_and_cold_mixed`. Takes the ADBC **prepared** query path
  (`ActionCreatePreparedStatement` → `DoGet(CommandPreparedStatementQuery)`).
- **`sql_insert`** — single-row `INSERT`, `_WRITE_COUNT` (default 1,000) auto-commit
  statements. Writes land hot, so no tier parametrization.
- **`sql_update`** — point `UPDATE … SET value = <lit> WHERE id = <pk>` (the
  read-modify-write write path), one per seeded row.
- **`sql_delete`** — point `DELETE … WHERE id = <pk>`, one per seeded row.

`sql_insert` / `sql_update` / `sql_delete` take the `DoPutStatementUpdate` path
(`client.execute_update`), distinct from the point read's prepared DoGet arm.

`Rows` is the repetition count, so `rows/s` reads as ops/s. The Postgres baseline
runs the equivalent single-row statements over a direct `psycopg` connection,
each committed in its own transaction (the analog of Penca autocommit).

Flight SQL requires the Rust backend, so the suite is skipped unless
`PENCA_BACKEND=rust`. Run via `PENCA_BACKEND=rust just perf-test
sql/oltp_test.py`. Absolute numbers are not committed here (host-dependent) —
each run captures to `.perf/results.jsonl`, emits an HTML report
(`.perf/report-<run_id>.html`) comparing the run against history, and with
`--record` persists to the SQLite store; view trends with `just perf-trends` /
`just perf-dashboard`. See the main
[performance notes](../../../docs/performance.md#recording--viewing-results).
Two write-count knobs (`PERF_SQL_WRITE_COUNT`, `PERF_SQL_READ_REPS`) trade run
time for averaging on a constrained host.

## Per-statement cost attribution

The point of this suite is not just the numbers but **where each statement's
wall time goes**, attributed against the gRPC op-for-op baseline. The attribution
is produced by a profiling run — `PENCA_BACKEND=rust just perf-test --trace
sql/oltp_test.py` (span busy/idle breakdown) and/or `--profile` (samply CPU
profile per servicer) — splitting each op across: Flight SQL parse/route → plan
(DataFusion) → metadata resolve (PG round trips) → the read (merge-on-read vs the
direct-seek bypass) / the write (upsert + tx bookkeeping) → Arrow IPC.

The SQL-layer overhead this isolates is the same fixed-overhead gap tracked by
the Flight SQL caching work — plan/metadata caching ([CHA-120](https://linear.app/chapala/issue/CHA-120)),
reusing the `GetFlightInfo` plan in `DoGet` ([CHA-355](https://linear.app/chapala/issue/CHA-355)),
and the per-query metadata-resolution amplification ([CHA-365](https://linear.app/chapala/issue/CHA-365));
it is the SQL-path counterpart of the gRPC point-read investigation
([CHA-414](https://linear.app/chapala/issue/CHA-414)).

### What the profiling run found

Method: one `--trace` run (per-servicer span busy/idle) plus one plain run of
this suite alongside `grpc/oltp_test.py` for the op-for-op delta. The Flight SQL
server itself emits no span timings, so its slice is the residual (client latency
− backend servicer busy). Absolute numbers below are representative of one
reference host and are **not** committed — the relative slices are the finding.

**1. The point read auto-pushes the PK as an `ids` seek (CHA-426), so it already
beats the unpushed gRPC filter path.** The SQL `SELECT … WHERE id = <lit>` latency
tracks the gRPC **`ids`-seek** shape (grows modestly with tier), not the gRPC
`filter` baseline that pays the full merge-on-read. On the reference host the SQL
point read ran ~20 ms (`all_hot`) / ~23 ms (`all_cold_snapshotted`) / ~31 ms
(`hot_and_cold_mixed`) — vs the gRPC `ids` seek at ~4 / ~8 / ~16 ms and the gRPC
`filter` baseline at ~52 / ~9 / ~118 ms. So the **SQL-layer overhead per point
read is ~15 ms** over the equivalent gRPC seek, and `hot_and_cold_mixed` is the
worst tier (the hot+cold merge fan-in) for both APIs.

**2. The dominant slice is the fixed Flight SQL per-statement pipeline, not the
data scan and not commit.** In the query servicer's span busy time, the actual
segment read (`ss_execute_stream`, `penca_dl::driver`) is ~2%; **~96% is metadata
planning** (`read_persist_segments_for_window` + `phase_one_fence_and_existence`
in `penca_api::query::meta_plan`), spread over tens of thousands of calls — the
*residual* per-query resolution amplification that survives the already-shipped
per-RPC metadata dedup of [CHA-365](https://linear.app/chapala/issue/CHA-365).
But backend servicer busy is under ~10% of client wall time; the rest is the SQL
front-end (parse → DataFusion plan → the ADBC prepared-statement path, which
plans the query **twice** — in `ActionCreatePreparedStatement` and
`GetFlightInfo`, before `DoGet` — of which the shipped
[CHA-355](https://linear.app/chapala/issue/CHA-355) already elides the third
(`DoGet`) re-plan, leaving the prepare + `GetFlightInfo` double-plan →
`DoPutStatementUpdate` wire actions → the SQL-server → servicer gRPC hop) plus
Arrow IPC — the fixed per-query gap [CHA-120](https://linear.app/chapala/issue/CHA-120)
targets. It is **not** commit/fsync: PG `begin` + `commit` summed to a fraction of
a second across a multi-minute run.

**3. The gRPC write parity localizes the SQL DML cost, and the RMW writes confirm
CHA-501's inferred ~40 ms/statement.** The native gRPC writes are uniform —
`oltp_insert` / `oltp_update` / `oltp_delete` all ran ~8.6–8.7 ms — because each is
a blind single-row `WriteData` / `Mutation` RPC with no read. The SQL writes
diverge: `sql_insert` ~29 ms, `sql_update` ~41 ms, `sql_delete` ~40 ms. Two slices
fall out cleanly:

- **SQL DML layer ≈ +20 ms** (`sql_insert` − `oltp_insert`) — the fixed Flight SQL
  per-statement pipeline over the equivalent native write.
- **Server-side read-modify-write ≈ +12 ms** (`sql_update` − `sql_insert`) — the
  SET-applied `SELECT` the DML runs before the upsert. The native path skips it:
  `oltp_update` ≈ `oltp_insert` (both ~8.7 ms) proves a native upsert is *blind*
  (no read), so the extra ~12 ms is specifically the SQL DML's RMW, not the write.

The ~40 ms RMW statements match the ~40 ms/statement
[CHA-501](https://linear.app/chapala/issue/CHA-501) inferred from the pgbench
transaction but never profiled in isolation. The autocommit point read
(~20–31 ms) is cheaper than the ~40 ms RMW statement because it is a **lighter
op** — no server-side read-modify-write — not because of transaction framing: the
suite's own *autocommit* RMW writes (`sql_update` / `sql_delete` ~40 ms) already
match the in-transaction RMW, so `OpenTx` framing adds ≈ 0.

**Dominant slice → follow-up.** The lever is the fixed Flight SQL per-statement
overhead, not the engine. The measured gap is the *residual* after the shipped
plan/metadata work — both [CHA-355](https://linear.app/chapala/issue/CHA-355)
(reuse the `GetFlightInfo` plan in `DoGet`) and
[CHA-365](https://linear.app/chapala/issue/CHA-365) (per-RPC metadata-resolution
dedup) have landed — so the sole genuinely-open lever is
[CHA-120](https://linear.app/chapala/issue/CHA-120) (metadata + plan caching). See
the run's HTML report and the
[main performance notes](../../../docs/performance.md#mechanism-and-cost-attribution).
