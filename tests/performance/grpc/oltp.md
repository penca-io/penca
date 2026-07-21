# OLTP (gRPC) Performance

A dead-simple, **stable** operational yardstick for the latency-sensitive
OLTP path, measured over the **native gRPC API** (`ReadData` / `WriteData`)
— deliberately **not** Flight SQL, so the measurement is the engine, not the
SQL parser / ADBC / prepared-statement machinery. Kept separate from the
[OLAP suite](olap.md) because the two have opposite cost structures
(fixed-overhead-dominated point ops vs throughput-dominated scans). Part of
the gRPC-first operational suite (CHA-416); the SQL-API (Flight SQL) versions
and parameterized/multi-statement workloads are explicit later passes.

Point read plus the three single-row write ops (insert / update / delete):

- **`oltp_point_read`** — a single-row point read via
  `read_data(filter="id = <pk>")` (gRPC `ReadDataRequest.filter` predicate
  pushdown), repeated 100× and averaged, across the latency-relevant tiers:
  `all_hot` (merge-on-read fast path), `all_cold_snapshotted` (the
  steady-state production tier), and `hot_and_cold_mixed`.
- **`oltp_insert`** — 1,000 single-row auto-commit `WriteData` inserts, each
  a full RPC round trip. Writes always land in hot storage, so there is no
  tier parametrization.
- **`oltp_update`** — 1,000 single-row `WriteData` upserts on *existing* PKs
  (the native update path: a latest-wins upsert, distinct from the insert
  arm's upsert of fresh ids), over a table pre-seeded all-hot.
- **`oltp_delete`** — 1,000 single-row `Mutation` delete-tombstones (a
  PK-only delete batch), over a table pre-seeded all-hot.

The native gRPC surface has no SQL-style point `UPDATE`/`DELETE`, so
`oltp_update` / `oltp_delete` measure the upsert / delete-mutation paths — the
write-op parity with the [SQL OLTP suite](../sql/oltp.md), whose `sql_update` /
`sql_delete` drive real Flight SQL DML.

`Rows` is the repetition count, so `rows/s` reads as ops/s. The Postgres
baseline runs the equivalent single-row lookups / inserts / updates / deletes
over a direct `psycopg` connection.

Run via `just perf-test grpc/oltp_test.py`. Absolute numbers are not committed here
(host-dependent) — each run captures to `.perf/results.jsonl`, emits an HTML
report (`.perf/report-<run_id>.html`) comparing the run against history, and
with `--record` persists to the SQLite store; view trends with `just
perf-trends` / `just perf-dashboard`. See the main
[performance notes](../../../docs/performance.md#recording--viewing-results).

The point read is the execution risk this yardstick exists to track: in
absolute terms it is dominated by per-query fixed overhead (Flight-SQL-free
here, but still plan resolution, metadata RPCs, and per-batch Arrow IPC), and
the `hot_and_cold_mixed` tier pays the full merge-on-read fan-in cost. The
closing levers are the same ones named in the main
[performance notes](../../../docs/performance.md) — plan/metadata caching and
a snapshot-only `stream_merged` bypass.
