2026-06-10 — CHA-417: OLTP point-read latency breakdown (gRPC suite)

# OLTP point-read latency breakdown

> **Dated one-time investigation record.** Per the CHA-423 convention,
> absolute numbers are not maintained in docs — this file is a snapshot
> of one measurement campaign, pinned to its host and run ids. The
> SQLite history (`.perf/perf.db`) and each run's HTML report are the
> living record.
>
> Host: `fabricdb-dev-3.exe.xyz` (7 GB RAM shared VM; Docker stack).
> Profile run `run_id d3354632-6292-411a-9431-c1de7f720fe1`
> (representative verbosity, samply attached). Trace run
> `run_id 609d26ba-1702-4513-a620-d2a781673594`
> (`--trace`: `penca=trace` + `PENCA_SPAN_TIMING=1` — wall numbers are
> Penca-under-tracing). Tree: `7c3b1f28`.
>
> **Scope:** the gRPC operational suites only
> (`tests/performance/grpc/oltp_test.py`, `olap_test.py`), per
> ticket-owner directive — the measurement is the engine, not the SQL
> front-end. The **Flight SQL profile pass is deferred** (the
> `flight_encode` span landed but went unexercised here); the ~85–135 ms
> Flight SQL `match_1` shape in `docs/performance.md` is therefore
> *inferred*, not measured, to share the root cause below.

## Measured (profile run, representative verbosity)

| operation | tier | per-op | PG baseline/op | ratio |
|---|---|---|---|---|
| `oltp_point_read` | `all_hot` | **66.9 ms** | 0.13 ms | ~515× |
| `oltp_point_read` | `all_cold_snapshotted` | **8.3 ms** | 0.28 ms | ~30× |
| `oltp_point_read` | `hot_and_cold_mixed` | **181.6 ms** | 0.12 ms | ~1500× |
| `oltp_insert` | hot | 7.6 ms | 0.9 ms | ~8× |
| `olap_full_scan` 100k | cold-snapshotted | 0.116 s (859k rows/s) | 0.128 s | 1.1× faster |
| `olap_full_scan` 1M | cold-snapshotted | 0.437 s (2.29M rows/s) | 0.527 s | 1.2× faster |
| `olap_filtered_scan` 1M | cold-snapshotted | 0.069 s (14.4M rows/s) | 0.405 s | 5.8× faster |

The OLAP side needs no de-risking at this scale. The OLTP point read is
the problem, and the tier ordering is the first surprise: **the
cold-snapshotted tier is the fastest tier by 8×** — the ticket's
hypothesis ("do cold-segment point lookups dominate?") is inverted.

## Per-tier attribution (trace run, span table)

Span timings from `PENCA_SPAN_TIMING=1` close events on the query
container, windowed per tier by table uuid
(`scripts/telemetry/span_window_table.py`). `busy` = time the span's
future was polled; `idle` = suspended (for a span awaiting Postgres,
the wait shows as **idle**). `busy` is not exclusive of children. Note:
each request also performs a metadata resolution that itself runs a
small `merge_read` over the `__penca_system__` MVCC tables — the
resolve/exclusion span rows below include that metadata read; rows
called out in the analysis are the user read's.

### `all_hot` — ~63 ms server-side, ~99 % of it one Postgres query

```
span                                n/req  busy ms/req  idle ms/req
ipc_encode                            1.0         1.12        58.64
stream_query_as_batches               1.0         0.53        58.53
read_data (entry+inner)               2.0         2.38         2.37
resolve_table_metadata                1.0         1.42         1.74
merge_read (metadata tables)          1.0         1.13         1.48
plan                                  2.0         0.48         0.49
```

Request timeline (one read, representative): handler + metadata
resolution + plan complete in ~6 ms; then a **single ~85 ms silent
gap** between plan events and `db.fetch_stream complete`; then encode
and stream-out in ~1 ms. The gap sits inside the CHA-142 all-hot fast
path's one Postgres query: `build_merge_resolved` — `DISTINCT ON
(row_uuid)` + `commit_tx_log` join + tombstone filter **over the full
100k-row upsert log**, with the PK filter applied to the *resolved*
output (dedup-then-filter). This is precisely the **O(depth) hot
dedup** the CHA-415 floor measured: cost scales with log depth, not
result size. The cold tier's prompt stream (2.9 ms `ipc_encode` idle,
same client, same tonic) rules out a consumer-side/flow-control
explanation.

### `all_cold_snapshotted` — ~6 ms server-side, healthy

```
span                                n/req  busy ms/req  idle ms/req
merge_read (incl. metadata read)      2.0         3.50         3.47
ipc_encode                            1.0         3.42         2.90
read_data (entry+inner)               2.0         1.74         2.07
read_cached_snapshot_segment          7.1         0.13         0.96   (cache hits)
```

Snapshot segments are served from the CHA-252 cache (`cache=hit`),
the exclusion anti-join runs over an empty hot log, and the whole
request is fixed overhead: metadata resolution + plan + a cache-hit
DataFusion scan. **Cold-segment point lookups do not dominate** — at
this scale they barely register.

### `hot_and_cold_mixed` — ~174 ms server-side, persist-tier amplification

```
span                                n/req  busy ms/req  idle ms/req
ipc_encode                            1.0        77.48        96.27
execute_sql (DataFusion, cold logs)   2.0         9.51       232.82
read_one_persist_segment             10.0         6.73       150.60
hot_exclusion_row_uuids               2.0        48.23        26.59
resolve_cold                          2.0         6.58       141.94
cold_exclusion_row_uuids              2.0         4.84        90.79
```

Two compounding costs, every read:

1. **Persist-log scan amplification** — each point read re-reads **10
   uncached persist log segments** (~150 ms idle, cold-storage round
   trips). Persist segments are *never* cached by design (retention
   compaction rewrites them — `penca-dl/src/cache.rs`), and the
   persist tier is deliberately read unfiltered to preserve the
   exclusion-set invariant. The cost is per-read and scales with
   persist-segment count.
2. **O(depth) hot work again** — `hot_exclusion_row_uuids` at ~48 ms
   *busy*: the DISTINCT scan over the deep hot log plus decoding the
   large shadowed-`row_uuid` result set into the exclusion set.

(Observed, unexplained in this pass: the hot *exclusion* probe is the
expensive hot query here while `resolve_hot` stays ~2.6 ms; in the
all-hot tier the asymmetry runs the other way. Likely result-set-size
vs sort-shape effects; a `pg_stat_statements` pass would settle it.)

## The streaming/IPC-encode bucket — exonerated (gRPC path)

`docs/performance.md` carried a ~60–70 ms "streaming + IPC encode"
bucket "pending a flamegraph pass". The new `ipc_encode` span brackets
exactly that loop, and its **busy time is ~1.1 ms per request** in
every tier; the large `ipc_encode` wall numbers are *idle* time spent
waiting on the upstream query (the span wraps the whole response
lifetime). The bucket was a misattribution of the O(depth) hot dedup —
on the gRPC path there is no material encode cost to optimize. The
Flight SQL path (`flight_encode` span, ADBC prepare + handshake on
top) still needs its own pass before saying the same there.

## Go/no-go

* **CHA-398 (hot PK-equality point-lookup pushdown): GO.** It is the
  direct fix for the dominant cost in both slow tiers — skip the
  O(depth) dedup when the predicate pins a PK; the floor verdict and
  this breakdown agree. This (plus lifecycle keeping the hot log
  shallow) is where the OLTP point-read milliseconds are.
* **CHA-410 → CHA-411 → CHA-412 (cold sort-order advertisement,
  SnapshotTableProvider O(log n) lookups, materialized secondary
  indexes): NO-GO for now.** The snapshot tier is the *fastest* tier
  (~6 ms server-side, cache-hit scans ~0.1 ms busy). The data does not
  support scheduling them on OLTP-latency grounds at this scale.
  Revisit with the CHA-422 cached-vs-uncached / real-S3 floor arm or
  at much larger snapshot scale.
* **New follow-up (recommend ticketing): mixed-tier persist-segment
  amplification.** 10 uncached persist-segment reads per point read is
  the mixed tier's 150 ms. Levers: snapshot cadence as the operational
  fix (post-snapshot the tier converges to the fast path above);
  persist-segment caching keyed by immutable segment uuid as the
  engineering fix (needs a compaction-invalidation story).
* **New follow-up (recommend ticketing): Flight SQL profile pass** —
  rerun this campaign over the Flight SQL suite (deferred scope) to
  attribute the `match_1` ~85–135 ms shape and exercise
  `flight_encode`.

## Exclusion queries vs pull-all + dedup

The question this section gates (CHA-417 follow-on experiment): are
the two exclusion-set queries (Query B, hot + cold) worth their round
trips vs pulling all visible hot + cold log rows in one query per tier
and deduplicating in DataFusion in memory?

**Verdict: not worth building the experiment now.** The breakdown
shows the probe *round trips* are not a material slice anywhere:

* `all_hot`: the user read's fast path issues no exclusion probes at
  all (the ~1 ms probe rows in its table belong to the metadata
  system-table read). The 58–85 ms is one resolved-upsert query.
* `all_cold_snapshotted`: all four probes together are ~2–3 ms of a
  ~6 ms request — and two of them short-circuit (CHA-352).
* `hot_and_cold_mixed`: the hot exclusion probe *is* expensive
  (~48 ms busy), but its cost is the O(depth) log scan plus shipping
  and decoding tens of thousands of shadowed `row_uuid`s — work a
  pull-all design **also pays, with strictly more bytes on the wire**
  (full rows rather than bare uuids, superseded versions included).
  Collapsing four queries to two saves round trips measured here at
  ~1–2 ms total.

The cost the question was circling is real but lives elsewhere: it is
log *depth* (hot) and *segment count* (persist), not query count.
CHA-398 + persist-segment hygiene attack it directly. Reopen the
pull-all experiment only if, after CHA-398, profile data shows the
remaining exclusion cost dominated by per-query fixed overhead rather
than scan/result size (the criterion bench harness sketch lives in the
CHA-417 kata task history).

## Reproduction

```bash
just perf-test --profile grpc        # samply CPU profiles → .perf/profile-<svc>.json
just perf-test --trace grpc          # span timing (PENCA_SPAN_TIMING=1)
# capture container logs before teardown, then:
python3 scripts/telemetry/span_window_table.py query.log
python3 scripts/telemetry/span_trace_table.py query.log --totals
```

Campaign artifacts (profiles, tier-windowed logs, JSONL) are kept off
the repo; the two `run_id`s above identify the runs in any `--record`ed
history.

---

2026-06-12 — CHA-426 follow-up: TPC-B point statements after the SQL ids pushdown

# CHA-426: per-statement breakdown after pk-equality ids pushdown

> **Dated one-time measurement record** (CHA-423 convention). Host:
> `fabricdb-dev-3.exe.xyz` (2-core shared VM; Docker stack;
> representative verbosity, no `--trace`). Cold run
> `run_id 25024c6a-982f-42b5-9140-edbf657d48ca`, hot run
> `run_id 76154787-5cbc-4323-a3c9-267cfc04b79e`. Tree: `c6288528`
> (wiring landed in `6799a317`). Workload: `PGBENCH_SCALE=1`
> (100k accounts), `PGBENCH_TX=1000`, seed 42, via
> `PENCA_BACKEND=rust just perf-test performance_pgbench_test.py`.

CHA-398 shipped the `ReadDataRequest.ids` PK-batch restriction;
CHA-426 makes `PencaTableProvider::scan` populate it from
PK-equality filter conjunctions, so SQL point statements resolve
below the merge-on-read dedup at O(versions-of-row). The CHA-396
campaign (PR #200) measured `update_accounts` ≈ 102 ms and
`select_abalance` ≈ 86 ms at this scale; the prediction was a
collapse toward the small-table floor (~25–40 ms).

## Measured (mean per statement, 1000 TPC-B transactions)

| statement | CHA-396 baseline | cold (snapshotted) | hot (upsert log) |
|---|---:|---:|---:|
| `begin` | — | 4.0 ms | 3.4 ms |
| `update_accounts` | ~102 ms | **48.6 ms** | **34.3 ms** |
| `select_abalance` | ~86 ms | **31.9 ms** | **21.0 ms** |
| `update_tellers` | — | 45.1 ms | 30.4 ms |
| `update_branches` | — | 44.0 ms | 30.6 ms |
| `insert_history` | — | 28.1 ms | 21.6 ms |
| `commit` | — | 4.8 ms | 3.6 ms |

Both point statements collapsed into the predicted band: the SELECT
half sits inside it in both tiers, and the UPDATEs (an RMW SELECT
plus a write RPC) sit at the floor plus their write cost — the
accounts-table statements are now indistinguishable from the
10-row teller / 1-row branch statements, i.e. flat across table
sizes (O(table-rows) → O(versions-of-row)).

Caveat: the hot-state `test_pgbench_olap` companion failed on a
SeaweedFS S3 PUT timeout during its persist setup (infra flake under
this VM's load; the cold-state run of the same test passed). The
TPC-B measurement above completed before that setup step and is
unaffected; the OLAP query path carries no point-lookup pushdown.
