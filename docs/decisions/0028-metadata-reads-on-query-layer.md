# ADR 0028 — Metadata reads belong to the Query layer: rehome `MetadataClient` reads onto `QueryManager` (caches shared with the write path); rename the lifecycle remainder `LifecycleManager`

## Status

Accepted ([CHA-472](https://linear.app/chapala/issue/CHA-472)). Builds on
[CHA-441](https://linear.app/chapala/issue/CHA-441) (PR #271 — the snapshot-list
cache + hot existence-gate) and follows
[CHA-445](https://linear.app/chapala/issue/CHA-445) (deletion of
`StorageMetadataService`; `Plan` made query-service-internal). Decision recorded
2026-06-23; implementation tracked by CHA-472.

## Context

`MetadataClient` (`crates/penca-storage-meta`) is a **stateless unit struct**
(`pub struct MetadataClient;`, zero fields) — a ~130-method namespace that mixes
two unrelated responsibilities:

- **Reads**: the `__penca_system__` resolves (`resolve_table_metadata`,
  `resolve_schema_metadata`, `resolve_index_metadata`), the getters (`get_table`,
  `get_table_by_uuid`, `get_schema`, `get_index`, `list_indexes`), and the read
  planner (`plan` + `read_snapshot_segments_for_table`,
  `read_persist_segments_for_window`, `phase_one_fence_and_existence`,
  `list_snapshot_segments`, …).
- **Writes / lifecycle**: `persist`, `snapshot`, `purge`, `compact`, `ddl`,
  `segment_index`, plus the `branch` / `catalog` mutators.

This shape is leftover from the deleted `StorageMetadataService` (ADR-era
pre-CHA-445): when the service went away, its methods had no stateful home and
stayed grouped under a namespace struct one crate below the services that drive
them.

CHA-441 added the in-process **snapshot-list cache** (`SnapshotListCache`, the
immutable `(segments, W_snap)` baseline keyed `(catalog, branch, table)`) on
`QueryManager` in `penca-api`, but could wire it only onto `read_data`'s
user-data `plan()`. The hottest snapshot-list read — `__penca_system__.tables`,
one entry per `(catalog, branch)`, hit on **every identifier resolution** — runs
through the system-table resolves, which pass `cache = None`. Threading the cache
down to them is awkward precisely because of the layering split: the cache
instance is on `QueryManager` (penca-api) while the reads are stateless statics
one crate down (penca-storage-meta), so the cache would have to be passed as a
parameter through `resolve.rs` and across the crate boundary into every getter.

Critically, the **write path is a first-class consumer of the same read**.
`mutate_data → apply_one_change → resolve_table → … → plan → list_snapshot_segments`
resolves the target table on **every** point write/update, and `penca_write.rs`
runs with `SegmentCache::disabled()` and no list cache — so it pays a
full Postgres round-trip per write. It is arguably the hottest consumer of the
system-table snapshot-list read and currently gets zero cache benefit.

## Decision

1. **Rehome the read methods off `MetadataClient` onto `QueryManager`** (as
   `&self` methods). They consult `self.snapshot_list_cache` / `self.snapshot_cache`
   through **one shared cache-eligibility gate**: a default-current-time request
   (no `as_of_micros` / `as_of_seq` / `open_tx_uuid`) → `Some(cache)`; any
   time-travel / open-tx read → `None` (bypass). This is the single place both the
   user-data read and the system-table resolves decide cache eligibility — no
   per-caller `Some`/`None` duplication, no cache parameter threaded through
   `resolve.rs` + `penca-storage-meta`.

2. **`WriteManager` holds a `QueryManager`.** The write path's per-write
   table/schema resolution flows through the same cache-consulting methods, so it
   gets the warm-cache benefit it lacks today. `read_data` / `audit_data` on the
   write-held `QueryManager` are nominal-only (never invoked from the write
   service) — no runtime cost.

3. **Rename the remaining write/lifecycle surface `MetadataClient` →
   `LifecycleManager`**, mirroring the service naming (`LifecycleService`,
   CHA-445). This is a rename, not a dissolution — the write/lifecycle methods
   stay grouped.

### Soundness

Sharing the snapshot-list cache with the write path is correct: the cache holds
only the **immutable cold snapshot baseline**; the hot change-log is always read
fresh; open-tx writes (read-your-own-writes) bypass via the eligibility gate
(`open_tx → None`); a stale entry is a *perf* cost only (merge against an older
`W_snap`), correctness-bounded by the retire/compact GC grace (cache TTL `<=`
`QUERY_TIMEOUT_SECONDS`, so the named files still exist). An autocommit point
write resolves under `AsOfMicros(pg_now)` → eligible → served from cache, saving
the round-trip, correctly.

## Consequences

- The CHA-441 snapshot-list cache extends to its highest-value read
  (`__penca_system__.tables`) and, for the first time, to the write path —
  turning a query-only optimization into an OLTP-wide one.
- The read/write responsibilities of the old metadata namespace are finally
  split along the service boundary that drives them; `LifecycleManager` names
  what it is.
- The write service nominally depends on `QueryManager` (it holds one). This is
  the accepted cost of sharing the read/cache layer; the alternative (a separate
  shared "resolve core" both managers embed) was considered and rejected as more
  machinery for no current benefit.
- **Acceptance is a perf gate** (CHA-472): instrumented perf tests + profiles
  must show **sub-10ms all-cold snapshotted point queries on both the query and
  the write path**, and a 2nd resolution (query and write) issuing **0**
  `…_table_snapshot_segment_metadata` reads. This is the payoff the
  commit_seq_num · incremental-snapshots · plan-caching · cold epic has been building
  toward; missing it is a blocker, not a pass.

## Alternatives considered

- **Thread an `Option<&SnapshotListCache>` parameter** from `QueryManager`
  through `resolve.rs` and across the crate boundary into the three system-table
  resolves. Smaller diff, but duplicates the eligibility decision per caller,
  leaves the read/write namespace smell in place, and gives the write path
  nothing. Rejected as a local patch over a layering defect.
- **Dissolve `MetadataClient` entirely**, distributing the ~70 write/lifecycle
  methods across `WriteManager` and the lifecycle owners. Far larger surface for
  no read-path benefit; the write/lifecycle methods are cohesive and want one
  home. Rejected in favor of the `LifecycleManager` rename.
