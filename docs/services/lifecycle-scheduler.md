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

## Tick loops

Two independently-paced loops. Persist is the hot→cold memory-relief sweep and
must not fall behind; Snapshot, Purge and tx-log GC are compaction and cleanup,
cheaper to amortize over a longer cadence. A single interval forced one
compromise between the two.

The loops share no mutable state: the persist loop is stateless because
`PersistBranch` resolves its own dirty set server-side, and the snapshot loop
owns the entire per-branch watermark map.

```text
// Persist loop — every SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS
for each catalog (QueryService::ListCatalogs):
  for each branch in catalog (QueryService::ListBranches):
    LifecycleService::PersistBranch(catalog, branch)
sleep(SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS)
```

```text
// Snapshot loop — every SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS
for each catalog (QueryService::ListCatalogs):
  for each branch in catalog (QueryService::ListBranches):
    now = system clock micros

    // Enumerates the PERSISTED set server-side (CHA-509), not the
    // hot-modified set, so a table persisted-then-purged is still
    // re-snapshotted.
    LifecycleService::SnapshotBranch(catalog, branch)

    // Tables with new committed OR aborted writes since the last sweep
    modified = paginate LifecycleService::ListModifiedTables(
                 catalog, branch, modified_at=[last_modified_tick, now))
    for T in modified:
      LifecycleService::Purge(catalog, branch, T)
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
sleep(SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS)
```

### Why Purge rides the snapshot loop

Purge's committed axis targets `Pu = W_snap`, read from the latest committed
snapshot watermark behind a strict-advance gate, so it cannot advance unless a
Snapshot has run — on a fast persist tick it would compute no advance and
early-return, costing an RPC and buying nothing.

Accepted trade-off: Purge's other two axes (expired-begin cleanup and abort
cleanup) have no dependence on `W_snap` and so now reclaim at the snapshot
cadence rather than the persist one. Both reclaim invisible garbage — aborted
rows and timed-out open txs serve no reads — and ADR 0027 §5 already gives
expired-begin ledger GC a wall-clock grace.

### Dual enumeration

Persist is driven by `ListModifiedTables` (settled-tx writes joined
through `commit_tx_log ∪ abort_tx_log`). CHA-221 v2.1 / ADR 0021 broadens
the listing from "committed-only" to "committed OR aborted" so the
scheduler triggers Persist on tables touched by aborted writes too —
Persist owns aborted hot-row cleanup, and without this enumeration
change aborted-only tables (no committed writes ever) would never
have Persist called, leaving their hot rows + tx-log family
metadata to leak indefinitely.

Snapshot is **not** chained off Persist. It runs on its own loop, on its
own cadence, and enumerates the PERSISTED set — tables carrying a
committed `table_persist_metadata` row — so its input is whatever an
earlier persist tick already made durable. That decoupling is the point
of the cadence split: a table persisted then dropped from hot is still
re-snapshotted, because Snapshot keys on persisted state rather than on
hot-modified state.

For aborted-only tables, Persist writes a `table_persist_metadata` row
with no segments; Snapshot then writes a placeholder via the existing
CHA-228 empty-merge path. Bounded overhead per aborted-only table per
snapshot tick.

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
dropping the StorageMetadataService client). Persist and Snapshot are
branch-scoped server-side RPCs — `PersistBranch` and `SnapshotBranch`,
whose per-table loops live in `LifecycleManager` — so the scheduler makes
one call per branch per loop. `Purge` is still the per-table RPC from
[CHA-220](https://linear.app/chapala/issue/CHA-220), invoked in a
client-side loop over the enumerated set (TODO(CHA-502) moves it
server-side too).

## Configuration

All values are required from environment variables; defaults live in
`docker/compose.yml`. Identical pattern to every other servicer
(`penca-core::config::required_env*`).

| Variable | Purpose |
|---|---|
| `QUERY_SERVICE_ADDR` | gRPC URL of the `query` service (catalog/branch discovery) |
| `LIFECYCLE_SERVICE_ADDR` | gRPC URL of the `lifecycle` service (per-table `Persist`/`Snapshot`/`Purge`) |
| `SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS` | Time between the end of one persist tick and the start of the next. Compose default `5s`; the `dev` profile pins `1s` (CHA-517 — an interactive stack should not accumulate unpersisted hot data). **Non-positive** disables the persist loop alone: boot and idle. |
| `SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS` | Same, for the snapshot loop (Snapshot + Purge + PurgeTxLog). Compose default `30s`; the `dev` profile pins `5s`. **Non-positive** disables the snapshot loop alone. The `test` profile pins **both** to `-1` so neither loop can race a suite's manual lifecycle calls. |
| `SCHEDULER_LIST_PAGE_SIZE` | Max `table_uuid`s requested per list-tables page. The scheduler drains every page before moving on. |
| `QUERY_TIMEOUT_SECONDS` | Universal grace window in seconds. MUST equal the value the `query` + `lifecycle` services read from the same env var (ADR 0019). The scheduler uses it to bound the `ListPersistedTables` upper edge at `now - QUERY_TIMEOUT_SECONDS`. |

## Failure handling

Errors inside a single `(catalog, branch)` tick are logged at `warn` and
the loop continues. Errors on a single table within a branch are also
logged and skipped — the branch ops are continue-on-error, which is
load-bearing rather than incidental: both dirty sets are enumerated
oldest-timestamp-first, so a table whose op keeps failing sorts first on
every subsequent sweep and would starve everything behind it forever if
the loop aborted on it.

A branch op that skipped at least one table signals partial completion by
withholding its response watermark; the scheduler logs that, and callers
needing an all-or-nothing flush (CreateBranch) treat it as an error.

Every lifecycle op is idempotent, so transient failures self-heal on the
next sweep that re-enumerates the table.

The scheduler does NOT use gRPC retries or backoff. The tick cadence
(`SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS` for Persist,
`SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS` for Snapshot, Purge and tx-log GC)
is the natural retry interval —
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
