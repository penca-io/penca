# lifecycle-scheduler

**Port**: none (does not listen — pure gRPC client)
**Crate**: `crates/penca-lifecycle-scheduler/` (binary: `penca-lifecycle-scheduler`)

## Purpose

Drives the per-table `Persist → Snapshot → Purge` chain on a periodic
timer so the hot → cold → snapshotted → purged pipeline runs
autonomously. Without the scheduler, Penca only advances lifecycle
state when an operator (or embedded-mode caller) invokes the RPCs
directly. With it, the happy-path unattended deployment moves data
forward on its own.

This is the v0 implementation per [CHA-154](https://linear.app/chapala/issue/CHA-154):
single replica, no leader election, no compaction, no operator-facing
admin RPCs. Multi-replica safety and compaction are future work.

## Tick loop

Every `SCHEDULER_TICK_INTERVAL_SECONDS`:

```text
for each catalog (QueryService::ListCatalogs):
  for each branch in catalog (QueryService::ListBranches):
    now = system clock micros

    // Tables with new committed writes since the last tick
    modified = paginate LifecycleService::ListModifiedTables(
                 catalog, branch, modified_at=[last_modified_tick, now))
    for T in modified:
      LifecycleService::Persist(catalog, branch, T)
      LifecycleService::Snapshot(catalog, branch, T)
    last_modified_tick[catalog, branch] = now

    // Tables whose latest persist has cleared the universal grace
    // window (ADR 0019)
    purge_upper = now - QUERY_TIMEOUT_SECONDS_micros
    if purge_upper > last_purge_tick:
      persisted = paginate LifecycleService::ListPersistedTables(
                    catalog, branch, persisted_at=[last_purge_tick, purge_upper))
      for T in persisted:
        LifecycleService::Purge(catalog, branch, T)
      last_purge_tick[catalog, branch] = purge_upper

    // Branch-scoped GC of the four hot tx-log family tables
    // (CHA-221). Unconditional per tick — the RPC's own empty-set
    // fast-path is the no-op gate, no scheduler watermark needed.
    LifecycleService::PurgeTxLog(catalog, branch)

sleep(SCHEDULER_TICK_INTERVAL_SECONDS)
```

### Dual enumeration

Persist is driven by `ListModifiedTables` (settled-tx writes joined
through `commit_tx_log ∪ abort_tx_log`). CHA-221 v2.1 / ADR 0021 broadens
the listing from "committed-only" to "committed OR aborted" so the
scheduler triggers Persist on tables touched by aborted writes too —
Persist owns aborted hot-row cleanup, and without this enumeration
change aborted-only tables (no committed writes ever) would never
have Persist called, leaving their hot rows + tx-log family
metadata to leak indefinitely.

Snapshot is chained directly off each Persist on the same `(catalog,
branch, T)` within the same tick — Snapshot's input is the cold data
that Persist just wrote, so no separate enumeration is needed. For
aborted-only tables, Persist writes a `table_persist_metadata` row
with no segments; Snapshot then writes a placeholder via the
existing CHA-228 empty-merge path. Bounded overhead per
aborted-only table per tick.

Purge is driven by `ListPersistedTables` instead. The reason is the
universal grace window in [ADR 0019](../decisions/0019-plan-time-pinning-and-universal-grace-window.md):
`Purge(T)` no-ops unless `now - max_persisted_at(T) > QUERY_TIMEOUT_SECONDS`.
On the tick that fires Persist, the gap is ~0 and Purge no-ops. If the
scheduler used `ListModifiedTables` alone, subsequent ticks would return
an empty set (no new writes) and Purge would never advance. Driving
Purge off `ListPersistedTables(persisted_at=[last_purge_tick, now - grace_window))`
guarantees every returned table has a Persist watermark that has
already cleared the grace gate — Purge has work to do.

### In-memory watermarks

`last_modified_tick` and `last_purge_tick` are per-`(catalog, branch)`
and live in process memory only. Restart resets both to `0`, making
the first post-restart tick a full sweep over committed history. This
is safe because all three lifecycle ops are idempotent — each no-ops
when its watermark already covers the requested range
(see [CHA-228](https://linear.app/chapala/issue/CHA-228) / ADR 0018
on `Snapshot` / `Purge` no-ops).

Durable watermarks (Postgres-backed) are out of v0 scope. They become
load-bearing only under multi-replica deployment, which is its own
ticket when horizontal scaling is actually needed.

## Mechanism contract

The scheduler is a pure gRPC client. It does NOT:

- Import `LifecycleManager` or any other in-process metadata helper.
- Hold a Postgres connection pool.
- Read or write Postgres tables directly.
- Wrap the per-table lifecycle ops behind a new `run_full_chain` RPC.

All data access flows through `QueryServiceClient` and
`LifecycleServiceClient` (CHA-445 rehomed the `ListModifiedTables` /
`ListPersistedTables` dirty-set discovery RPCs onto `LifecycleService`,
dropping the StorageMetadataService client). The per-table chain is the
existing per-table RPCs (`Persist`, `Snapshot`, `Purge` from
[CHA-220](https://linear.app/chapala/issue/CHA-220)) invoked individually.

## Configuration

All values are required from environment variables; defaults live in
`docker/compose.yml`. Identical pattern to every other servicer
(`penca-core::config::required_env*`).

| Variable | Purpose |
|---|---|
| `QUERY_SERVICE_ADDR` | gRPC URL of the `query` service (catalog/branch discovery) |
| `LIFECYCLE_SERVICE_ADDR` | gRPC URL of the `lifecycle` service (per-table `Persist`/`Snapshot`/`Purge`) |
| `SCHEDULER_TICK_INTERVAL_SECONDS` | Time between the end of one tick and the start of the next. Compose default `5s`; the `dev` profile pins `1s` (CHA-517 — an interactive stack should not accumulate unpersisted hot data) and the `test` profile pins `-1`, i.e. boot and idle, so the tick loop cannot race a suite's manual lifecycle calls. |
| `SCHEDULER_LIST_PAGE_SIZE` | Max `table_uuid`s requested per list-tables page. The scheduler drains every page before moving on. |
| `QUERY_TIMEOUT_SECONDS` | Universal grace window in seconds. MUST equal the value the `query` + `lifecycle` services read from the same env var (ADR 0019). The scheduler uses it to bound the `ListPersistedTables` upper edge at `now - QUERY_TIMEOUT_SECONDS`. |

## Failure handling

Errors inside a single `(catalog, branch)` tick are logged at `warn` and
the loop continues. Errors on a single table within a branch are also
logged and skipped. Every lifecycle op is idempotent, so transient
failures self-heal on the next sweep that re-enumerates the table.

The scheduler does NOT use gRPC retries or backoff. The tick cadence
(`SCHEDULER_TICK_INTERVAL_SECONDS`) is the natural retry interval —
adding intra-tick retries would interfere with the tick loop's
single-replica progress guarantee.

## Single-replica only

V0 is hard-coded single-replica. The lifecycle service's per-op
advisory locks (`persist:{table}:{branch}`, etc.) would serialize
concurrent scheduler instances correctly, but the scheduler itself
has no leader election, no `wait_if_locked` / `SKIPPED_LOCKED`
semantics, and no candidate-shard hashing. Running multiple
schedulers would cause both to do redundant work (every op gets
called by both, with the loser blocking on the advisory lock). It
won't corrupt data — it just wastes Postgres connections.

When multi-replica becomes a real requirement (CHA-NNN, not filed
yet), the design extension is leader election + candidate sharding
on `hash(catalog_uuid, branch_uuid)`.

## What v0 does NOT drive

- `CompactPersistSegments` — segment-count thresholds, less
  time-sensitive. Folds in once v0 is shipping. (Snapshot segments are
  immutable and never compact — ADR 0024.)
- DML or DDL of any kind. Lifecycle ops only.

## Related

- [CHA-154](https://linear.app/chapala/issue/CHA-154) — this ticket.
- [CHA-220](https://linear.app/chapala/issue/CHA-220) — per-table Persist + Purge RPCs the scheduler invokes.
- [CHA-233](https://linear.app/chapala/issue/CHA-233) / [ADR 0019](../decisions/0019-plan-time-pinning-and-universal-grace-window.md) — universal grace window; rationale for the dual-enumeration design.
- [CHA-221](https://linear.app/chapala/issue/CHA-221) — branch-scoped `PurgeTxLog` invoked unconditionally at the end of each `tick_branch`.
- [CHA-228](https://linear.app/chapala/issue/CHA-228) / [ADR 0018](../decisions/0018-purge-as-hot-cold-visibility-cutoff.md) — Persist / Snapshot / Purge symmetric no-ops the scheduler relies on for idempotent retries.
