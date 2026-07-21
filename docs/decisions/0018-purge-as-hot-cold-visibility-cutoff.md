# ADR 0018 — Purge is the hot/cold visibility cutoff; Persist, Snapshot, Purge are per-table

## Status

Accepted (CHA-220). Two decisions below are **superseded** by
[ADR 0019](0019-plan-time-pinning-and-universal-grace-window.md)
([CHA-233](https://linear.app/chapala/issue/CHA-233)):

1. The **cutoff source** for `MetadataClient::plan` (moving the
   hot/cold cutoff from `persisted_at_micros` to `purged_at_micros`)
   reverts to `persisted_at_micros` once plan-time threading
   ([CHA-227](https://linear.app/chapala/issue/CHA-227)) plus the
   universal grace window make the strict partition robust without
   relying on `purged_at` as the visibility waypoint.
2. The **shared advisory lock key**
   (`lifecycle:{table_uuid}:{branch_uuid}` taken by all three of
   `Persist(T)`, `Snapshot(T)`, `Purge(T)`) splits into three
   per-operation keys (`persist:`, `snapshot:`, `purge:`).
   Cross-operation serialization on T is no longer load-bearing
   under threading + grace; same-operation serialization still is.

The per-table decomposition (`Persist(T)`, `Snapshot(T)`, `Purge(T)`
as separate per-table RPCs, parallel across different tables) and
the strict-partition reasoning are **preserved** unchanged.

## Context

Before CHA-220, `Persist(catalog, branch)` did three things in one
branch-scoped operation: it wrote a coherent watermark of committed
data to cold storage, deleted the corresponding hot upsert/delete log
rows, and purged the hot `commit_tx_log` family past the same watermark
([CHA-168](https://linear.app/chapala/issue/CHA-168)). The hot/cold
visibility cutoff used by `MetadataClient::plan` was
`persisted_at_micros`: hot was anything strictly newer than the latest
persist; cold was everything up to it.

Two upstream changes invalidated the branch-scoped framing:

1. **[CHA-218](https://linear.app/chapala/issue/CHA-218)** made
   `commit_tx_log` hot-only. Cold rows carry the four denormalized tx
   metadata columns inline (per
   [ADR 0017](0017-cold-data-segments-pre-joined-tx-metadata.md)).
   Branch coordination on cold was load-bearing only because every
   touched table's persist emitted into one shared cold `commit_tx_log`
   segment per persist event; with no cold `commit_tx_log` artifact, there is
   no remaining write-time reason for tables to persist together.
2. **[CHA-154](https://linear.app/chapala/issue/CHA-154)** (lifecycle
   scheduler) wants to iterate per dirty table: `Persist(T) →
   Snapshot(T) → Purge(T)`. A branch-scoped persist forces the
   scheduler to either persist every dirty table on every tick or
   build per-table dirty-state filtering on top of the branch RPC —
   both worse than per-table from the start.

Coupling cold-write to hot-purge inside `Persist` also blocked atomic
persist+snapshot. [CHA-84](https://linear.app/chapala/issue/CHA-84)
sketched a `PersistAndSnapshot` RPC to close the window where a query
could see "persisted but not yet snapshotted" cold state. That framing
only exists because persist is *also* the visibility flip — the cold
rows become the source of truth as soon as persist commits.

## Decision

**Split `Persist` into `Persist(T)` and `Purge(T)`. Move the hot/cold
visibility cutoff used by `plan()` from `persisted_at_micros` to
`purged_at_micros`.**

`Persist(T)` writes T's hot upsert/delete log rows to cold and stamps
`table_persist_metadata(T).persisted_at_micros`. It does **not** delete
hot rows. Queries still serve T's data from hot.

`Purge(T)` reads `persisted_at_micros(T)`, deletes the corresponding
hot rows (`upsert_log` / `delete_log` with `commit_micros <=
persisted_at_micros(T)`), and stamps
`table_purge_metadata(T).purged_at_micros`. After purge, the same
rows are served from cold.

`MetadataClient::plan` sources the hot-side `min_micros` cutoff from
`latest_committed_table_purge_watermark(catalog, branch, table)`.
Between `Persist(T)` and `Purge(T)` the same rows exist in both tiers;
the merge layer's per-`row_uuid` latest-commit-time dedup collapses
the temporary double presence into one visible row regardless of
which tier served it.

`plan_audit` consumes the same watermark (via the shared
`MetadataClient::hot_min_commit_micros` helper, `max_purged + 1`
or `0` if no purge has committed yet) but uses it as a **strict**
tier partition rather than a lower bound. Cold per-row filters cap at
`min(user_to, hot_min)`; hot per-row filters floor at `max(user_from,
hot_min)`. The asymmetry exists because `audit_data` has no merge
dedup — `read_data` returns the latest version per `row_uuid`, but
`audit_data` returns every committed version of every row, so the
same version surfacing twice from the cross-tier overlap between
Persist and Purge would corrupt the audit horizon. The strict
partition ensures every version surfaces from exactly one tier.

All three of `Persist(T)`, `Snapshot(T)`, `Purge(T)` take the same
advisory lock key `lifecycle:{table_uuid}:{branch_uuid}` and
serialize against each other on T. `Persist(T1)` and `Persist(T2)` run
in parallel on different keys. `Persist` continues to clamp its
watermark to `min(target_micros ?? now, oldest_open_began_at(branch)
- 1)` — the open-tx invariant is structural per-table.

## Why purge as the cutoff, not persist

The invariant `Persist(T) → Snapshot(T) → Purge(T)` is the
scheduler's per-tick chain. The cutoff has to be the **last** event
in the chain — otherwise queries can see a stale view of T while
the scheduler is mid-chain:

- If the cutoff were `persisted_at_micros`, then between `Persist(T)`
  and `Snapshot(T)` queries would serve from cold raw segments
  rather than the snapshot baseline + a small tail — exactly the
  CHA-84 "no atomic persist+snapshot" window.
- With the cutoff at `purged_at_micros`, queries keep serving from
  hot until `Purge(T)` runs. `Snapshot(T)` lands a new snapshot
  baseline before purge; `Purge(T)` flips visibility to cold, at
  which point cold has both the fresh baseline and the cold raw
  segments still readable. The window closes on its own.

A scheduler crash between any two phases is recoverable:
- Crash after `Persist(T)` before `Snapshot(T)` / `Purge(T)`: next
  tick re-persists (idempotent), re-snapshots, then purges.
- Crash between `Snapshot(T)` and `Purge(T)`: next tick re-purges.
  If the prior Purge had already committed, the replay no-ops
  because `persisted_at <= last_purged` — the watermark stays put
  and the world is consistent.

`Purge(T)` no-ops when there is no committed persist newer than the
last purge — including the brand-new "never persisted" case. The
call writes no `table_purge_metadata` row and returns
`PurgeResponse.purged_at_micros` unset. The fast-path cannot safely
advance the watermark in this state: `plan()` and `plan_audit()`
filter hot by `commit_micros >= purged_at + 1`, so any value
larger than the last Persist's watermark would exclude hot rows that
were committed after Persist (or any hot rows at all if Persist
hasn't run) — they aren't in cold yet, and `audit_data` has no
merge-dedup to recover them. Branch-min consumers
([CHA-221](https://linear.app/chapala/issue/CHA-221) and friends)
must therefore special-case "table T has not contributed a purge
watermark yet" rather than treating an absent row as `0`.

`Snapshot(T)` is symmetric ([CHA-228](https://linear.app/chapala/issue/CHA-228)):
when there is no committed persist newer than the last snapshot —
the planner reports `cold_storage.persist = None` after a snapshot
has caught up because `read_and_classify_persist_segments` filters
raw segments by the prior `snapshotted_at_micros` — it writes no new
`table_snapshot_metadata` row and returns
`SnapshotResponse.snapshotted_at_micros` unset. The existing
snapshot stays in place; the cold merge-read is skipped entirely.
This collapses three "nothing to fold in" states into one exit:
never persisted, no `cold_data_max` with no prior snapshot, no
`cold_data_max` with a prior snapshot already covering all persist
(the case that pre-CHA-228 fell through to a redundant merge-read
and an ON-CONFLICT re-insert at the same deterministic snap_uuid).

The three lifecycle RPCs share one response convention:
`PersistResponse.persisted_at_micros`,
`PurgeResponse.purged_at_micros`, and
`SnapshotResponse.snapshotted_at_micros` are all proto3 `optional`.
**Unset = no-op**, set = "this is T's watermark after the call."
Distinguishes "RPC did nothing" from "watermark is at value X"
without overloading `0`. Callers test field presence
(`HasField`) rather than comparing against `0`.

## What `Persist` no longer does

- Does not delete hot upsert/delete log rows (`hot.delete_by_*`).
  Those move to `Purge`.
- Does not purge the hot `commit_tx_log` family (`commit_tx_log`, `abort_tx_log`,
  `begin_tx_log`, `tx_table_log`). Those leak until
  [CHA-221](https://linear.app/chapala/issue/CHA-221) lands a
  dedicated branch-min GC pass. Accepted as pre-1.0 dev-only data
  leakage — the leak is bounded by the cleanup ticket's scope, not
  by individual persist calls.
- Does not write a `branch_persist_metadata` row. That parent table is
  deleted; per-table `table_persist_metadata` is the recovery anchor.
  `table_persist_metadata` carries `persisted_at_micros` directly (the
  field that used to live on the deleted parent).

## What changes on the wire and on disk

- `PersistRequest` carries a per-table identifier block (catalog +
  schema + branch + table); `PersistResponse` is
  `{optional persisted_at_micros}` only. `rows_persisted` /
  `segment_uuids` are gone — no current consumer reads them and
  Snapshot re-queries `table_persist_metadata` on its own.
- New `Purge(PurgeRequest) → PurgeResponse{optional purged_at_micros}`.
- `SnapshotResponse` collapses to `{optional snapshotted_at_micros}`
  (CHA-228). The pre-CHA-228 trio (`table_snapshot_uuid`,
  `rows_in_snapshot`, `table_snapshot_segment_uuids`) is gone —
  tests that needed `table_snapshot_uuid` derive it deterministically
  from the returned watermark via `table_snapshot_uuid`.
- `CompactPersistSegmentsRequest` is per-table.
- `table_purge_metadata` is added — same shape as
  `table_persist_metadata` (phase-1/phase-2 commit, LIST partitioned
  by `branch_uuid`). `branch_persist_metadata` is dropped.

## Relationship to other ADRs

- **[ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md):**
  the auditable-store invariant continues to hold — hot rows are
  deleted strictly after they exist in cold (now in `Purge` rather
  than in `Persist`'s Phase 2). The "in at least one tier at every
  instant" invariant strengthens: between Persist and Purge the rows
  exist in *both* tiers, and the visibility cutoff makes that
  intentional rather than transient.
- **[ADR 0017](0017-cold-data-segments-pre-joined-tx-metadata.md):**
  is the prerequisite. Pre-joined cold rows are what allow
  per-table persist — without it, every persist would have to coordinate
  on writing the shared cold `commit_tx_log` segment.

## Out of scope

- Hot `commit_tx_log` family GC — [CHA-221](https://linear.app/chapala/issue/CHA-221).
- Lifecycle scheduler — [CHA-154](https://linear.app/chapala/issue/CHA-154).
- Atomic persist+snapshot — moot; supersedes
  [CHA-84](https://linear.app/chapala/issue/CHA-84).
- Capping cold-side reads at `min(as_of, max_purged)` via a new
  `ColdStoragePlan.committed_at` proto field. The merge layer's
  per-`row_uuid` dedup already collapses the temporary double
  presence; revisit only if a test surfaces a visibility regression
  the dedup can't catch.

## Pre-1.0 migration

Drop-and-recreate per the [CHA-203](https://linear.app/chapala/issue/CHA-203)
precedent. Covers both the `branch_persist_metadata` removal and the
`table_purge_metadata` addition.
