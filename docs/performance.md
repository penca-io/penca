# Performance

Benchmarked across all system state permutations (hot, cold unsnapshotted,
cold snapshotted, and mixtures) with 100k rows using the **Lance** cold
storage format (default). Postgres baselines use equivalent Arrow
serialization overhead so the comparison isolates Penca's auditable
storage overhead.

The Rust backend is the only supported runtime.

## Recording & viewing results

Absolute perf numbers are **not committed** to this doc — they are
hardware-dependent and go stale. The SQLite results store is the source of
truth, and each run renders its own report:

- **Capture (always on).** Every `just perf-test [paths]` run writes one
  JSON object per measurement to `.perf/results.jsonl` — the single capture
  format, produced on every run.
- **Persist to history (opt-in).** Pass `--record` to also ingest the run into
  the gitignored SQLite history at `.perf/perf.db`, where perf accumulates
  across runs, branches, and hosts. Without `--record` the run is throwaway —
  JSONL + report only, no history accumulated.
- **Per-run HTML report (always on).** At the end of every run a self-contained
  static **HTML report** is written to `.perf/report-<run_id>.html`. It plots
  this run against the recorded SQLite history — the run's points projected at
  the end of each series' trend line, plus the average delta vs history (or
  "no-baseline" when there is no history yet). It is generated headlessly (no
  server, no browser), so it works in CI and over SSH.
- **Explore the history.** `just perf-trends` prints a per-series markdown
  summary (run counts, latest-vs-previous, regression flags) and writes trend
  PNGs; `just perf-dashboard` launches the interactive Streamlit dashboard over
  `.perf/perf.db`. The dashboard takes an optional `--run_id` to overlay one
  run against history (sourced from SQLite, falling back to the run's JSONL).

**Lifecycle note:** In the happy path, persist and snapshot are always
called atomically — data moves from hot storage directly to a
snapshotted state. Persist on its own (producing unsnapshotted cold data)
is an emergency release valve, not the normal operating mode. System
states containing unsnapshotted cold data represent a worst-case
scenario that should not occur under normal lifecycle operation.

**Resource requirements:** The benchmark spins up Postgres and SeaweedFS in
Docker and writes 100k rows to cold storage (Lance on S3). Ensure at
least **6 GB of free host memory** and **swap under 80%** before
running — the test exits early if swap is saturated. Docker Engine
(native) is preferred over Docker Desktop (which runs a ~2GB QEMU VM).
See `just docker-ensure` for automatic backend selection.

## Operational gRPC suites (OLTP / OLAP)

The **gRPC-first operational perf suites** — a dead-simple, stable yardstick we
optimize against, with OLTP and OLAP kept deliberately separate (opposite cost
structures) — live alongside their tests, with co-located result tables:

- [OLTP (gRPC)](../tests/performance/grpc/oltp.md) — single-row point read
  (`read_data` PK-filter) across hot / cold-snapshotted / mixed tiers, plus
  single-row `WriteData` insert / update (upsert-on-existing) / delete
  (delete-tombstone).
- [OLAP (gRPC)](../tests/performance/grpc/olap.md) — full + bulk filtered scan
  via `read_data` at 100k and 1M on the cold-snapshotted tier.
- [OLTP (SQL / Flight SQL)](../tests/performance/sql/oltp.md) — the same four
  point ops (read across tiers, insert, update, delete) driven as **single
  autocommit Flight SQL statements** over the ADBC driver, so the delta against
  the gRPC OLTP suite isolates the SQL-layer overhead (parse → plan → ADBC
  prepared-statement wire actions → metadata resolve). The SQL-API pass CHA-416
  parked; the single-statement companion to pgbench (CHA-501). Run via
  `PENCA_BACKEND=rust just perf-test sql/oltp_test.py`.

The gRPC pass exercises the native `ReadData` / `WriteData` surface only
(`SELECT` + non-parameterized `INSERT`), so the measurement is the engine, not
the SQL parser / ADBC / prepared-statement machinery; the Flight SQL OLTP pass
above adds the SQL-path mirror (server-side aggregates remain an explicit later
pass). This suite folds
[CHA-37](https://linear.app/chapala/issue/CHA-37) (the proto/gRPC-contract perf
suite). TPC-H / ClickBench and other industry-standard *complex* benchmarks are
a separate track under [CHA-77](https://linear.app/chapala/issue/CHA-77) — an
investor-credibility asset, not part of this operational optimization loop. Run
via `just perf-test grpc` (the whole gRPC suite directory).

## Building-blocks floors (`just perf-floor`)

Where the suites above measure the **whole stack** end-to-end, the
building-blocks floor suite ([CHA-415](https://linear.app/chapala/issue/CHA-415))
**isolates** the three storage primitives Penca does not control — the hot tier
bounded by **Postgres**, the cold tier by **Lance**, the merge by **DataFusion**
— so cost can be attributed to a tier instead of fused. These are Rust
**criterion** benches (run via `just perf-floor`; criterion writes its own
self-contained HTML report under `target/criterion/`), the first of the
perf-de-risking efforts (floor → representative suite → instrumentation). What's
durable is the *shape* of each floor and the verdict it gates:

1. **Hot-tier MVCC resolution (Postgres-bound)** — `build_merge_resolved::<PgDialect>`
   dedup (`DISTINCT ON` + `commit_tx_log` join + tombstone filter) over the real
   `upsert_log`/`commit_tx_log`/`delete_log` schema + indexes, read + write, at
   increasing log **depth** × dedup **density**. *Shape:* read cost is **O(depth)**
   — the full-log dedup scans the whole log regardless of how many distinct rows
   survive; cheap over a shallow log, prohibitive over a deep one. Append is a
   steady raw bulk-INSERT floor, flat across depth.
2. **Cold-tier read (Lance-bound)** — `FormatReader` read of a snapshot segment:
   whole-segment vs `(offset, length)` positional slice (a compact-time row
   range, **not** a predicate pushdown — Penca filters in DataFusion,
   [ADR 0023](decisions/0023-single-query-execution-engine.md)), plus
   segment-write throughput, over Lance and Parquet. *Shape:* sub-millisecond
   point read; the positional slice is far cheaper than scanning the whole
   segment. Absorbs [CHA-348](https://linear.app/chapala/issue/CHA-348);
   [CHA-61](https://linear.app/chapala/issue/CHA-61) subsumed (Lance/Parquet).
   The cached-vs-uncached + real-S3 first-touch arm is deferred to
   [CHA-422](https://linear.app/chapala/issue/CHA-422).
3. **Hot+cold merge fan-in (DataFusion-bound)** — the real `DlDriver::scan_snapshot`
   (the [CHA-411](https://linear.app/chapala/issue/CHA-411) `SnapshotTableProvider`)
   running the exclusion-set **anti-join** + snapshot scan in DataFusion, as hot
   churn (the exclusion-set size) grows over a fixed cold base. *Shape:*
   single-digit-millisecond over a 100k base, growing with hot churn.
4. **Cold point-lookup execution (DataFusion-bound)** — the #2b execution arm: the
   real `DlDriver::scan_snapshot` (the same
   [CHA-411](https://linear.app/chapala/issue/CHA-411) `SnapshotTableProvider`)
   running the **production cold point-lookup plan** — the exclusion anti-join over
   an *empty* exclusion **plus** a PK equality residual, the exact shape
   `build_cold_snapshot_scan` always emits (`NOT IN (SELECT row_uuid FROM exclusion)
   AND (row_uuid = x)`) — over a fixed in-memory cold base; the
   `cold_point_lookup_floor` bench
   ([CHA-418](https://linear.app/chapala/issue/CHA-418)) shares #3's `scan_snapshot`
   harness, varying the residual where #3 varies the exclusion set. *Shape:* O(rows)
   full-scan-and-filter today (no predicate pushdown,
   [ADR 0023](decisions/0023-single-query-execution-engine.md)); CHA-410
   (`output_ordering`) / CHA-412 (secondary index) turn it O(rows)→O(log n).

### Verdict — shared-connection MVCC vs sticky/pinned connections

**The decision this gates:** is shared-connection MVCC merge-on-read fast enough
to serve OLTP, or must we fall back to **sticky / pinned connections**?

**The bottleneck is hot-log *depth*, not the connection model.** The dominant
OLTP cost is the **O(depth) hot merge-on-read dedup** — comfortably fast over a
shallow log, prohibitively slow once the log is deep. That cost is the dedup
*scan*; it is **independent of whether the Postgres connection is shared from a
pool or pinned per session**. Sticky/pinned connections address session-state and
connection-setup overhead — neither of which is what this floor pays. So
**shared-connection MVCC is fine; do not fall back to sticky connections to fix
this.** OLTP feasibility hinges on keeping the hot log **shallow** (the
persist/snapshot lifecycle) and on **not** resolving the whole log for a point
read (PK-equality point-lookup pushdown,
[CHA-398](https://linear.app/chapala/issue/CHA-398)). The other tiers are not the
constraint: cold point lookups are sub-millisecond (improving to O(log n) via
CHA-410/411/412), and the merge fan-in is single-digit-millisecond. The cold
point-lookup *execution* floor (#2b — the DataFusion residual filter) is the
`cold_point_lookup_floor` bench ([CHA-418](https://linear.app/chapala/issue/CHA-418)),
sharing #3's `scan_snapshot` harness.

## Query Performance

Four operations across every system state:
- `read_data` — streaming gRPC read (merge-on-read).
- `read_data_time_travel` — same path, `as_of` capped to the first commit.
- `query_filter_non_pk[match_N]` — `SELECT … WHERE …` via Flight SQL (ADBC),
  parametrized over result-set size. `match_1` filters on `name = ?` for a
  single row; `match_1000` filters on `value < 1100.0` which matches 1,000
  rows out of 100k. Same scan, different return sizes — see the analysis
  for why the ratio tightens dramatically as the result set grows.
- `query_aggregate` — `SELECT COUNT(*), SUM(value), AVG(value)` via Flight SQL.

## Write Performance

Bulk-write operations: `write_empty_table` and `write_populated_table`
(100k-row `WriteData` upserts into a fresh vs. already-populated table),
and `write_multi_tx` (the same 100k rows split across 1 / 10 / 100
auto-commit transactions to expose per-tx fan-out cost).

## Lifecycle Performance

Tiering operations: `persist` (hot → cold Lance on S3), `snapshot`
(materialize a pre-deduplicated point-in-time view),
`compact_persist_segments` (merge 2 / 5 / 10 log segments), and the
end-to-end `pipeline_write` → `pipeline_persist` → `pipeline_snapshot` chain.

## pgbench (TPC-B) Performance

An industry-standard OLTP shape — PostgreSQL `pgbench`'s default `tpcb-like`
benchmark — recreated against Penca
([CHA-396](https://linear.app/chapala/issue/CHA-396)). The bulk load
(`pgbench -i`) loads the four-table schema at scale 1; the TPC-B transaction
runs its five statements (3 `UPDATE` + 1 `SELECT` +
1 `INSERT`) over `N = 1000` transactions with a fixed seed. The TPC-B
workload runs over Flight SQL (Rust backend) and each transaction is a real
Penca `BEGIN … COMMIT`; the Postgres baseline runs the equivalent SQL over a
direct `psycopg` connection. The workload defaults to the
`all_cold_snapshotted` tier (the loaded base is persisted + snapshotted +
cache-warmed first), the representative steady-state shape; set
`PGBENCH_STATE=hot` to measure against the hot upsert log instead.

Bulk load tracks the existing write path, consistent with
`write_empty_table`. The TPC-B transaction rate is Penca's worst-case access
pattern — single-row, single-client, fully synchronous OLTP — where each of
the five statements is its own Flight SQL round trip (≈7–10 RPCs per
transaction counting `BEGIN`/`COMMIT` and the ADBC prepared-statement
`SELECT`). It is a recognizable, repeatable yardstick for tracking that gap
over time, not a claim that Penca competes with Postgres on single-client
OLTP.

### Per-statement latency

The TPC-B transaction reports the mean latency of each of the seven statements so the
per-transaction total is attributable, not opaque (recorded per-statement in
the run's report; `PGBENCH_STATE=hot` gives the hot-tier contrast). Two things
the breakdown makes obvious:

- **`BEGIN`/`COMMIT` are not the cost — it is not commit/fsync.** The cost is
  the per-statement read path and the synchronous round trips.
- **The point read/update cost scales with table size, and the snapshot tier
  narrows but does not close the gap.** The *identical* `UPDATE … WHERE pk = v`
  is markedly slower on the 100k-row `accounts` table than on 1-row `branches`.
  The snapshot's columnar Lance layout + row-group stats make the large-table
  read cheaper than the hot upsert-log `DISTINCT ON`, but it stays above the
  small-table floor: the PK-equality predicate is applied *after* the
  merge-on-read resolution instead of being pushed to a `row_uuid` point
  lookup. Tracked by
  [CHA-398](https://linear.app/chapala/issue/CHA-398), which would flatten
  this across both tiers.

### HTAP analytical query

`olap_query` runs an analytical query — per-account history count + the
per-branch average of that count, filtered and top-20 — over the
cold-snapshotted base, the **same SQL on both engines**. At small scale
Postgres edges it: Penca's fixed per-query overhead (planning + Flight SQL
RPC) dominates a small result. The gap is a crossover, not a wall — Penca's
overhead is fixed while Postgres scales with rows, so at larger scale Penca
pulls ahead. The columnar/vectorized analytical advantage shows once there is
enough data to amortize the fixed cost.

The query is written with explicit joins, not the natural correlated-subquery
phrasing: Penca's Flight SQL rejects the single-level correlated `COUNT`
([CHA-402](https://linear.app/chapala/issue/CHA-402)) and the doubly-nested
branch subquery ([CHA-401](https://linear.app/chapala/issue/CHA-401)) today.
(Note: that correlated form is also a textbook *bad* benchmark — its cost is a
query-optimizer artifact, not a data-volume one; same-SQL is the fair test.)

Deviations from real pgbench (synthetic `hid` PK on `pgbench_history`,
`UPDATE += delta` mapped onto the Flight SQL `UPDATE` path, `int64`/`utf8`
widening, empty `filler`, single-client/sequential) are documented in the
`tests/performance/performance_pgbench_test.py` module docstring. Run via
`PENCA_BACKEND=rust just perf-test performance_pgbench_test.py`.

## Analysis

**SQL-path single-statement OLTP: the cost is the fixed Flight SQL pipeline, not
the scan.** The [SQL OLTP suite](../tests/performance/sql/oltp.md) drives one
`SELECT` / `INSERT` / `UPDATE` / `DELETE` point op each as a single autocommit
Flight SQL statement, profiled and compared op-for-op against the gRPC OLTP suite
(CHA-504). The point `SELECT` auto-pushes the PK as an `ids` seek
([CHA-426](https://linear.app/chapala/issue/CHA-426)), so it tracks the gRPC
`ids`-seek shape rather than the unpushed merge-on-read filter path — the
SQL-layer overhead over the equivalent gRPC seek is ~15 ms/read. Under
`--trace` the actual segment read is ~2% of query-servicer busy while metadata
planning (`meta_plan`) is ~96%, but backend busy is under ~10% of client wall
time: the dominant slice is the fixed per-statement Flight SQL pipeline (parse →
plan → ADBC prepared-statement / `DoPutStatementUpdate` wire actions → the
SQL-server hop → Arrow IPC). The measured gap is the *residual* after the shipped
[CHA-355](https://linear.app/chapala/issue/CHA-355) (elides the `DoGet` re-plan)
and [CHA-365](https://linear.app/chapala/issue/CHA-365) (per-RPC metadata dedup),
so [CHA-120](https://linear.app/chapala/issue/CHA-120) (metadata + plan caching) is
the sole open lever. Single-statement RMW writes (~40 ms) confirm the ~40 ms/statement
[CHA-501](https://linear.app/chapala/issue/CHA-501) inferred but never profiled in
isolation. Full attribution in the [suite doc](../tests/performance/sql/oltp.md).

**Read performance scales with storage tier.** Cold snapshotted data reads
at 1.4–2.6M rows/s — roughly 7–13x faster than Postgres — because snapshots
are pre-deduplicated Lance files with zero merge-on-read overhead. This is
the expected production state, since persist and snapshot are called
atomically in normal operation. Even mixed hot+cold reads stay above 990k
rows/s once snapshots are present. States with unsnapshotted cold data
represent a worst-case scenario — not normal operation.

**Hot-only reads run at ~1.1M rows/s** (~5x faster than Postgres). Merge-
on-read still runs (upsert log scan, transaction log join, delete tombstone
filtering, dedupe by `row_uuid`), but the Rust path (DataFusion + Arrow,
columnar all the way through) keeps it cheap relative to row-oriented
Postgres.

**Flight SQL `query_filter_non_pk` is pushed down end-to-end as of
[CHA-142](https://linear.app/chapala/issue/CHA-142).** The
`PencaTableProvider` translates DataFusion `Expr` filters to a bare SQL
`WHERE` fragment (via the 52.5.0 `Unparser` with a Postgres dialect) and
plumbs it into `stream_merged`. As of
[CHA-368](https://linear.app/chapala/issue/CHA-368) that fragment is **no
longer spliced into the per-tier resolve SQL** — DataFusion is the single
filter engine ([ADR 0023](decisions/0023-single-query-execution-engine.md)).
Each tier's resolve returns an unfiltered, `is_delete`-flagged two-arm
delta; the exclusion set that shadows stale snapshot rows is the full
`row_uuid` set of that resolve (derived **before** any filtering, so a
current version failing the filter can't let a stale version resurface),
and the user predicate is applied once as the `full_plan_predicate`
residual — the same one the snapshot segment scan uses. `all_hot` takes a
fast path that skips `stream_merged` and streams the unfiltered, projected
hot-tier resolve (`WHERE NOT is_delete` drops tombstones in PG), then
residual-filters each batch; its `COUNT(*)` push-down survives only for
the no-filter case.

The test is parametrized over result-set size (`match_1` vs
`match_1000`) on the same 100k-row table to make the ratio shape
visible. Penca's wall time is dominated by per-query fixed overhead
(plan resolution, metadata RPCs, Flight SQL handshake) — roughly
constant across the two cases, which is why `match_1000` throughput
scales almost linearly with rows returned (e.g. `hot_and_cold_snapshotted`
1.2M → 2.1M rows/s). The Postgres baseline moves the other way:
psycopg's per-tuple Python object construction dominates on the
1,000-row return, dragging PG from ~16M rows/s on `match_1` down to
~10M rows/s on `match_1000`. The ratio tightens accordingly:
`all_hot` 20.8x → 6.9x, `hot_and_cold_snapshotted` 13.3x → 4.8x,
`realistic_timeseries` 16.7x → 6.7x. In other words, the overhead
gap you see on `match_1` is mostly a measurement of Penca's fixed
per-query cost against PG's near-zero single-row read — it's an
honest upper bound on the ratio, not a throughput ceiling.

Cold-dominant states (`all_cold_snapshotted`,
`cold_snapshotted_and_unsnapshotted`) pay a larger relative penalty
on `match_1` (~17x) because the segment scan + per-segment filter
evaluation is the bottleneck; snapshot-tier segment pruning
([CHA-82](https://linear.app/chapala/issue/CHA-82), landed) trims
whole segment files via `PruningPredicate` before any IO. Format-internal
pushdown into Parquet/Lance ([CHA-256](https://linear.app/chapala/issue/CHA-256))
was evaluated and **dropped** ([ADR 0023](decisions/0023-single-query-execution-engine.md)
— DataFusion is the single filter-execution engine); the per-segment
predicate runs in-process on top of the CHA-82 pre-IO trim. Further gains
come from narrowing the read itself — per-tier projection pushdown
([CHA-368](https://linear.app/chapala/issue/CHA-368), landed 2026-07-13:
the Postgres tier projects `output ∪ filter ∪ group-by ∪ join-keys` and
evaluates the filter only in DataFusion) and relational
reduction across merged sources
([CHA-370](https://linear.app/chapala/issue/CHA-370)).
States with unsnapshotted cold run ~22–40x on `match_1`
because every segment read also pays the merge-on-read fan-in cost
without the pre-dedupe a snapshot provides. Closing the per-query
fixed overhead through Flight SQL metadata caching
([CHA-120](https://linear.app/chapala/issue/CHA-120)) is the
complementary lever for the selective-filter case.

**Known limitation — absolute latency for web-OLTP point lookups.**
Throughput ratios understate what matters for sub-50 ms-per-query web
workloads. In absolute terms, `match_1` runs ~85–135 ms in snapshotted
states and ~245–270 ms with unsnapshotted cold data, against a Postgres
baseline of ~6–8 ms — over the typical web-app point-lookup budget
(<50 ms wall, often <20 ms). This is a real gap for latency-sensitive
web reads today, but it is a fixed-overhead / handshake gap, not an
architectural one: Penca's per-row scan cost is ~1.2 µs, so the actual
work for a 100k-row selective scan is sub-millisecond and wall time is
dominated by Flight SQL handshake, sequential metadata RPCs, and
per-batch Arrow IPC encode. The closing levers are named:
plan / arrow-schema / catalog caching with version-ETag invalidation
([CHA-120](https://linear.app/chapala/issue/CHA-120), ~15–25 ms);
the streaming + IPC encode bucket (~60–70 ms — **exonerated on the
gRPC path** by the CHA-417
[point-read breakdown](perf-reports/oltp-point-read.md): encode busy is
~1 ms/request and the wall was the O(depth) hot dedup, CHA-398; the
Flight SQL arm still needs its own pass);
collapsing the multi-hop Flight SQL → query servicer → metadata gRPC
chain (~5–10 ms); and a snapshot-only `stream_merged` bypass when the
upsert log is empty past `snapshotted_at`. Today's `match_1` numbers
should be read as an upper bound on handshake cost, not a steady-state
ceiling.

**Flight SQL `query_aggregate` still pays the full merge-on-read cost
per tier** (~5–32x slower than Postgres across states). Unlike
filters, aggregates are not pushed through `stream_merged` yet —
aggregate pushdown over merge-on-read is tracked separately as
[CHA-143](https://linear.app/chapala/issue/CHA-143). The partial +
correction approach sketched there is the follow-up.

**Write throughput is ~115k rows/s** for bulk operations — roughly 2.3x the
equivalent Postgres baseline. Wins come from Arrow IPC over the wire,
vectorized `row_uuid` / `version_uuid` computation, and one-shot
auto-commit `WriteData` (server opens + commits a tx in one round-trip
when `tx_uuid` is unset). Per-tx fan-out hurts: 100 batches drops to
~80k rows/s, and single-row OLTP inserts pay a full RPC round trip per
commit — now tracked in the
[OLTP (gRPC) suite](../tests/performance/grpc/oltp.md).

**Persist throughput is ~144k rows/s** — moving data from Postgres (hot) to
Lance files on S3 (cold) — and **snapshot throughput is ~283k rows/s**,
materializing pre-deduplicated point-in-time views. **Log segment
compaction runs at ~430–500k rows/s** independent of group size, since
the cost is dominated by the cold-storage read+write rather than per-
segment overhead. The full **write → persist → snapshot pipeline** sustains
~117k → 231k → 293k rows/s — the snapshot stage is fastest because it
operates on already-persisted, pre-deduplicated batches.

## Future improvements

**Snapshot segment compaction** was removed (ADR 0024 — snapshot
segment files are immutable). The CHA-404 packed write makes snapshot
output born compacted: whole partitions accumulate into one file up to
`max_segment_bytes`, so there are no small snapshot segments to merge.

**Predicate / projection pushdown for `Plan`** ([CHA-65](https://linear.app/chapala/issue/CHA-65))
is partially in place on the read path but not yet plumbed through the
Rust `QueryManager::plan` call (now in-process). Closing
this gap composes with segment-level pruning
([CHA-82](https://linear.app/chapala/issue/CHA-82), landed) by pushing
the user predicate further down the planner.

**Streaming response chunking** ([CHA-136](https://linear.app/chapala/issue/CHA-136)) —
the gRPC server currently runs with `max_send_message_length = -1` as a
stop-gap so wide-schema responses don't trip the 4 MiB default cap. The
real fix is to chunk batches by `default_stream_batch_size` before
yielding so each frame stays under the default limit.

**LRU file cache for cold segments** ([CHA-74](https://linear.app/chapala/issue/CHA-74))
and **custom indices on partition metadata**
([CHA-63](https://linear.app/chapala/issue/CHA-63)) will further accelerate
filtered reads against large cold tablesets by avoiding repeat S3
round trips and skipping irrelevant segments without opening them.

**Aggregate pushdown over merge-on-read: partial + correction + segment
stats** ([CHA-143](https://linear.app/chapala/issue/CHA-143)) —
decomposable aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) are
computed as a *partial* aggregate per tier plus a *correction* step
that reconciles tombstones and overwrites from the upsert log, and
snapshot segments carry precomputed per-segment stats that short-
circuit the partial aggregate entirely when filters align with
partition columns. Cross-references CHA-82, CHA-112, CHA-124, and
CHA-142. Directly targets the ~6–32x `query_aggregate` overhead
today.
