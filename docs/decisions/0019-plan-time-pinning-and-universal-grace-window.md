# ADR 0019 — Plan-time pinning + universal grace window

## Status

Proposed (CHA-233). Supersedes two decisions in
[ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md):

1. **Cutoff source** for `plan()` / `plan_audit()` — moves back from
   `purged_at_micros` to `persisted_at_micros`.
2. **Shared advisory lock key** — `Persist(T)`, `Snapshot(T)`, and
   `Purge(T)` no longer share `lifecycle:{table_uuid}:{branch_uuid}`.
   Each operation takes its own per-operation, per-table key.

The per-table decomposition (`Persist(T)`, `Snapshot(T)`, `Purge(T)`
as separate per-table RPCs, parallel across different tables) and the
strict-partition property introduced by
[CHA-227](https://linear.app/chapala/issue/CHA-227) survive unchanged.

## Context

After [ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md) and
[CHA-227](https://linear.app/chapala/issue/CHA-227) (PR #86), the
plan-time read sequence in `MetadataClient::plan` and
`MetadataClient::plan_audit` is correct as a snapshot:

1. **Cutoff read**: `hot_min = latest_committed_table_purge_watermark + 1`.
2. **Snapshot picker** bounded by `min(request.as_of, cutoff - 1)`.
3. **Persist segment fetch** bounded by `[snapshot_watermark + 1, cutoff)`.

Each step's output is the next step's bound. A concurrent
`Persist(T)` + `Purge(T)` that commits between any two reads advances
PG state, but the plan's bounds are pinned to step 1 — so the picker
and the segment list stay consistent with that pin. Plan-time
atomicity is via explicit threading, not via REPEATABLE READ.

Two TOCTOU windows remained open at execute time:

* **Hot-side ([CHA-231](https://linear.app/chapala/issue/CHA-231),
  silent data loss).** Between plan commit and the hot-side row read,
  a concurrent `Persist(T) + Purge(T)` can delete the hot rows the
  plan expected. The merge layer no longer dedup-absorbs the
  double-presence (CHA-227 made the partition strict for
  `read_data` and structural for `audit_data`), so the missing rows
  surface as silent data loss in `read_data` and a dropped version
  in `audit_data`.
* **Cold-side ([CHA-232](https://linear.app/chapala/issue/CHA-232),
  availability).** Between plan commit and the cold-side segment
  read, compaction can delete a cold file the plan captured by URI.
  The stream fails with `NotFound` from cold storage.

[CHA-231](https://linear.app/chapala/issue/CHA-231) and
[CHA-232](https://linear.app/chapala/issue/CHA-232) were originally
filed as separate fixes — a long-lived `REPEATABLE READ` transaction
threaded through the execute-time stream for the hot-side, and a
standalone compaction grace window for the cold-side. Both were
canceled in favor of one unified mechanism. The motivation: the two
windows share a root cause (destructive lifecycle steps racing
in-flight plans), and a single grace contract on the destructive side
closes both.

## Decision

**Adopt one system invariant, one config knob, and one mechanism that
spans hot and cold.**

> **System invariant.** Any `Plan + Execute` that completes within
> `query_timeout_seconds` observes a consistent view of the data.
> Every destructive lifecycle step (Purge of hot rows, GC of
> compacted-away cold files) delays its delete operation by at least
> `query_timeout_seconds` past its own commit time, so anything a
> plan captures stays valid for at least that long.

### Three pillars of the invariant

The invariant holds by the composition of three orthogonal
mechanisms; it does not hold under any one alone.

1. **Plan-time threading** ([CHA-227](https://linear.app/chapala/issue/CHA-227),
   PR #86, already in place). `plan()` and `plan_audit()` read the
   cutoff once, bound the snapshot picker by it, and bound the
   persist-segment fetch by it. The plan is self-consistent by
   construction. No REPEATABLE READ tx.
2. **Persist open-tx clamp** (existing in `lifecycle.rs::persist_locked`
   at `lifecycle.rs:202-206`, no code change in this ADR). Persist's
   `effective_target` is clamped to
   `min(requested_target, oldest_open_began_at(branch) - 1)`. Without
   this clamp, an in-flight tx could commit with `committed_at <=
   persisted_at`, which would land open-tx data in cold segments and
   break the "commit_tx_log rows below the branch-min purge watermark are
   settled" invariant downstream consumers depend on (see
   [CHA-221](https://linear.app/chapala/issue/CHA-221)). Snapshot
   inherits the safe boundary via the cutoff threading (pillar 1);
   Purge inherits via gating on `persisted_at` (pillar 3-ii below).
3. **Destructive-side grace window** (new in this ADR). Purge of hot
   rows and GC of compacted-away cold files each wait at least
   `query_timeout_seconds` past their own metadata commit before
   deleting. Combined with a hard query runtime cap, every concurrent
   plan completes before its captured state is destroyed.

Removing any one pillar reopens at least one of the TOCTOU windows.

### Four-part mechanism

1. **`persisted_at_micros` as the plan cutoff source.** Reverts the
   [CHA-220](https://linear.app/chapala/issue/CHA-220) cutoff-source
   flip (`purged_at_micros` → `persisted_at_micros`). The strict
   tier partition added by
   [CHA-227](https://linear.app/chapala/issue/CHA-227) carries over
   unchanged — only the value computed in step 1 of the plan-time
   read sequence changes. Cold serves `committed_at < cutoff`; hot
   serves `committed_at >= cutoff`. Pre-Persist (`persisted_at = 0`)
   leaves cold as `None` and hot serves everything, same shape as
   pre-Purge in the
   [ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md) world.

2. **Grace-bounded Purge gated on
   `table_persist_metadata.commit_micros`.** Purge's
   `purged_at_micros` derivation reads `MAX(persisted_at_micros)`
   over `table_persist_metadata` rows where `commit_micros IS
   NOT NULL AND now_micros() - commit_micros >
   query_timeout_micros`. If the result is `NULL` or `<=
   last_purged`, Purge writes no row and
   `PurgeResponse.purged_at_micros` stays unset — extends the
   ADR 0018 "no-op when there is no committed persist newer than the
   last purge" rule with a grace condition on top.

   Correctness argument: any query that planned at `T_q` finishes by
   `T_q + query_timeout`. A Persist that committed at `T_c >= T_q`
   can't have its hot rows purged before `T_c + query_timeout >=
   T_q + query_timeout` — the query's deadline. The rows the query's
   hot filter wants are still in hot when it reads.

3. **Grace-bounded compaction GC.** Compaction's metadata-update
   transaction stops inline-deleting the old cold files and instead
   inserts rows into a new `segment_delete_set` table inside the same
   merge tx. A separate sweep (`sweep_segments`) reads set rows
   whose `written_at_micros` is older than `query_timeout`, deletes
   the files, then deletes the set rows. Same correctness argument:
   a plan that captured `uri_old` and started before the compaction
   tx committed finishes before the file delete fires. The set is a
   single shared queue for both persist-segment and snapshot-segment
   compaction — no `kind` discriminator, since `sweep_segments`
   treats every row as an `object_uri` to delete.

   *Amended by CHA-531.* `segment_delete_uuid` is branch-keyed, so one
   file that several branches reference has one queue row per branch
   and each row ages on its own clock. Eligibility therefore takes the
   **cross-branch max**: a row is eligible only when no sibling
   branch's row for the same `object_uri` is still within grace.
   Otherwise a fork's parent could delete, inside its own expired
   window, a carried file a child had only just released. The same
   widening applies to the refcount probes; see
   [ADR 0024 §4](0024-incremental-snapshot.md).

4. **Enforced query runtime cap.** `read_data`, `audit_data`, and
   their callees wrap the returned `BatchStream` so each `next()` is
   bounded by `(T_q + query_timeout) - now`, where `T_q` is the
   start of the plan call. On elapsed, the server returns gRPC
   `RESOURCE_EXHAUSTED` with a structured retry-pattern detail
   ("query exceeded `query_timeout_seconds`; retry with a fresh
   plan"). Soft targets don't suffice — the grace argument requires
   a strict upper bound.

### Lock scoping: per-operation, per-table keys

[ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md)'s decision
that all three of `Persist(T)`, `Snapshot(T)`, `Purge(T)` take the
same `lifecycle:{table_uuid}:{branch_uuid}` advisory lock key
serialized cross-operation pairs on T (`Persist↔Snapshot`,
`Persist↔Purge`, `Snapshot↔Purge`). With pillars 1 (plan-time
threading) and 3 (grace window) in place, cross-operation
serialization on T is no longer load-bearing — every pair is now
safe lock-free. Same-operation pairs (`Persist↔Persist`,
`Snapshot↔Snapshot`, `Purge↔Purge`) still need serialization, but
each on its own key.

Split the single shared key into three per-operation keys:

- **`persist:{table_uuid}:{branch_uuid}`** — `Persist(T)` against
  `Persist(T)`. Still load-bearing for correctness, not just
  hygiene. Two concurrent Persists would each clamp to
  `oldest_open_began_at - 1`, pick overlapping read windows, and
  write overlapping cold segments. `read_data`'s per-`row_uuid`
  dedup absorbs the duplicate presence; `audit_data` does not (the
  strict tier partition introduced by
  [CHA-227](https://linear.app/chapala/issue/CHA-227) leaves
  `audit_data` with no merge-dedup safety net), so unserialized
  Persists would surface the same row version from two cold
  segments and corrupt the audit horizon.
- **`snapshot:{table_uuid}:{branch_uuid}`** — `Snapshot(T)` against
  `Snapshot(T)`. Hygiene only — race-losers already no-op via the
  deterministic `snap_uuid` ON-CONFLICT exit
  ([CHA-228](https://linear.app/chapala/issue/CHA-228)) and the
  "no committed persist newer than the last snapshot" idempotent
  early-out. The lock avoids the wasted merge-read + segment write
  the race-loser would otherwise do before reaching that exit.
- **`purge:{table_uuid}:{branch_uuid}`** — `Purge(T)` against
  `Purge(T)`. Hygiene only — race-losers already no-op via the
  deterministic `purge_uuid` and the idempotent
  `<= last_purged` early-out. The lock avoids the wasted DELETE
  the race-loser would otherwise issue against rows already removed.

Cross-operation pairs on T are now lock-free. Their independence
follows from the three pillars:

- **`Persist(T) ↔ Snapshot(T)`.** Snapshot reads `persisted_at(T)`
  once and captures candidate cold segments bounded by it (pillar 1
  threading). A concurrent Persist that commits a new
  `persisted_at` does so *after* Snapshot's bound was captured;
  Snapshot's segment list does not include the new segments, and
  the next Snapshot run picks them up. Two consecutive baselines,
  each internally consistent.
- **`Persist(T) ↔ Purge(T)`.** Purge gates its watermark choice on
  the grace window: `now - committed_at > query_timeout` (pillar 3).
  A freshly-committed Persist is structurally excluded from the
  grace-eligible set, so its `persisted_at` does not affect this
  Purge's chosen watermark. Conversely, the rows Purge deletes are
  bounded by the chosen watermark, and the concurrent Persist's
  new hot rows have `committed_at > watermark` (pillar 2's open-tx
  clamp keeps Persist from advancing `persisted_at` past any
  in-flight tx's `began_at`, and Persist's read filter caps the
  written `committed_at` values at `effective_target`), so the
  concurrent Persist's rows are untouched.
- **`Snapshot(T) ↔ Purge(T)`.** Different tiers (Snapshot writes
  cold; Purge deletes from hot), different metadata tables
  (`table_snapshot_metadata` vs `table_purge_metadata`), no
  read/write overlap. Fully orthogonal.

The scheduler's per-tick `Persist(T) → Snapshot(T) → Purge(T)`
chain was never the lock's responsibility — scheduler iteration
order is what enforces it, and that is unchanged. A late Snapshot
from tick N completing after tick N+1's Persist is safe by the
pair argument above (Snapshot's bounds were captured at tick N);
same for a late Purge.

### Reading the watermark: `purged_at` is the GC contract; `persisted_at` is internal

The two watermarks answer two different questions, and reaching for
the wrong one is the bug class this ADR exists to prevent.

* `persisted_at` answers **"where does cold take over visibility?"**
  Read only by `MetadataClient::plan` and
  `MetadataClient::plan_audit` as the hot/cold cutoff source. No
  other call site reads `persisted_at` for cleanup or scheduling.
* `purged_at` answers **"what's safe to delete from hot or from any
  consumer that depends on hot?"** Read by every GC consumer
  (hot commit_tx_log GC at [CHA-221](https://linear.app/chapala/issue/CHA-221),
  any future orphan sweeps, branch-min cleanups). Consumers read the
  *stored* `table_purge_metadata.purged_at_micros` column directly —
  via the existing `MetadataClient::latest_committed_table_purge_watermark`
  helper or its branch-min composition on
  [CHA-221](https://linear.app/chapala/issue/CHA-221) — and **must
  not** re-derive an equivalent value from
  `MAX(persisted_at) WHERE now - committed_at > query_timeout`. The
  derivation is functionally close but architecturally wrong: a
  reviewer reading the call site has to recompute the equivalence
  in their head, and the derivation is easy to get subtly wrong
  (e.g., choosing grace from `effective_target` instead of
  `commit_micros`). The grace clamp lives in `purge_locked`'s
  write path, once.

Worked example. CHA-221's hot commit_tx_log GC computes
`branch_min_purged = MIN(purged_at_micros)` across the branch's
tables and deletes commit_tx_log rows with `committed_at < branch_min_purged`.
If CHA-221 substituted
`MIN(MAX(persisted_at) WHERE grace)` it would delete commit_tx_log entries
for txs with `committed_at ∈ (P_q_for_a_live_query, persisted_at_now]`
— exactly the rows still needed by a live query whose plan was
pinned at `P_q`. The grace gap on `purge_locked`'s write side is
load-bearing for every downstream consumer; substituting a
near-equivalent on the read side bypasses it.

A long-term home for cross-cutting GC invariants like this rule
(once more accumulate) may be `docs/algorithms.md` or similar; for
now the rule lives here.

### Defaults and resolved open questions

* **`query_timeout_seconds` default.** 900 (15 min) in production;
  2 in tests, set via fixture env. The prod value tracks PG's
  typical `statement_timeout` for OLTP-leaning workloads —
  long-running analytical streams that legitimately need more will
  get a loud cancellation and a clear retry instruction rather than
  silent inconsistency. The test value keeps the integration suite
  fast while exercising the cap path.
* **Config knob naming.** `query_timeout_seconds` (not
  `max_query_runtime_micros`). Unit-explicit, PG `statement_timeout`
  lineage, `max_` redundant with `timeout`. Internally converted to
  micros at config load so the SQL math stays in micros (every
  other timestamp in the system is micros).
* **Per-query override semantics.** None in v1 — the cap is
  system-wide. Correctness requires the grace window to cover the
  longest possible concurrent query, so any per-query override
  could only ever *shorten* the cap, never extend it. Deferred as a
  follow-up; the wire format does not carry a `deadline_micros`
  field in v1.
* **Cancellation surface.** gRPC `RESOURCE_EXHAUSTED` with a
  structured detail naming the cap and the retry pattern. Mirrors
  PG's `statement_timeout` and is the same error code clients
  already handle for other resource-bound failures.

## Consequences

* **Plan-time read shape unchanged.** PR #86's threading is
  preserved; only step 1's value source flips. `read_only_snapshot`
  stays dropped.
* **Hot row count after `Persist + Purge` stabilizes at "rows
  committed within the last `query_timeout`."** Hot is no longer
  drained to its post-Persist minimum; it carries a grace tail
  whose size is bounded by ingest rate × `query_timeout`. At
  default 15 min and typical ingest, this is small relative to
  steady-state hot row count, but it is observable and
  configurable.
* **Compaction GC is delayed by `query_timeout`.** Cold storage
  cost rises by the same window-bounded factor. The
  `segment_delete_set` itself is small (one row per compacted-away
  file, deleted on sweep) and partitioned by `branch_uuid` to match
  the rest of the lifecycle tables.

  *Amended by CHA-531.* "Small" no longer holds unconditionally. A
  row whose file is still referenced across a fork edge is **not**
  deleted on sweep — it stays queued in the expired range and is
  re-scanned by every subsequent sweep on its branch. Since the
  probes are catalog-wide, each such row costs one index probe per
  branch leaf, so a standing blocked set costs
  O(blocked_rows x branches) per sweep. Two things make a set stand:
  a legitimately long-lived carried reference (bounded — it clears
  when the last referencing snapshot retires), and crash-orphaned
  uncommitted snapshot rows (unbounded until TODO(CHA-435) lands a
  reaper). The live-lock CHA-435 addresses is therefore now
  **catalog-scoped** — but along the queue axis, not the file axis. An
  orphan still pins only the URIs its own snapshot touched (that
  branch's new files plus what it carried from its parent, which under
  the CHA-515 main-only guard is always `main`). What widened is whose
  sweep those orphans stall: `segment_delete_uuid` is branch-keyed, so
  every branch that carried a URI gets its own queue row for it on
  retirement, and one branch's orphans now block all of them. Before,
  an orphan could only block its own branch's rows.
  `sweep_segments`' `eligible`/`deleted` pair is the triage signal — a
  persistent `eligible = 0` against a growing delete set reads as
  "everything still referenced".
* **The grace arm assumes a uniform `query_timeout`.** The
  cross-branch max compares `written_at_micros` across branches
  against a *single* `query_timeout` — the one belonging to the
  sweeping process (`QUERY_TIMEOUT_SECONDS`, passed down as
  `LifecycleManager::query_timeout_micros`), not one per row. Deploy
  a shorter timeout on one branch's lifecycle process and it can
  expire a sibling branch's row early, reopening the TOCTOU window
  that pillar 3 closes. Uniform deployment across a catalog is
  currently an operational convention, not an enforced invariant;
  making the knob genuinely per-branch would require the probe to
  compare each row against its own branch's timeout.
* **Long-running analytical queries hit a hard cap.** Queries that
  exceeded the default 15 min previously completed silently;
  under this ADR they get a `RESOURCE_EXHAUSTED` cancellation.
  Client behavior is symmetric with how mature systems handle
  `statement_timeout`: clients retry with a fresh plan, which
  re-establishes the threading and the grace argument from scratch.
* **Stale plans hit the cap, not ENOENT.** A client holding a plan
  past `query_timeout` and trying to execute will be cut off by
  the cap before its captured state is destroyed; the failure mode
  is "cancelled at the cap" rather than "missing rows" or
  "`NotFound` from cold." This is the failure shape the cap
  exists to produce.
* **Snapshot and Purge MUST NOT re-derive the open-tx clamp.**
  Pillar 2 lives in `persist_locked`. Snapshot relies on the
  cutoff threading; Purge relies on `persisted_at`. If a future
  contributor adds an `oldest_open_began_at` call to
  `snapshot_locked` or `purge_locked`, that is a structural
  violation of this ADR.
* **Cross-operation parallelism on T is now allowed.**
  `Persist(T)` and `Purge(T)` can run concurrently; `Snapshot(T)`
  and `Purge(T)` can run concurrently; etc. The scheduler may
  choose to keep the per-tick chain sequential for simplicity, but
  the metadata layer no longer enforces it.

## Why this supersedes ADR 0018's two decisions

### Cutoff source: `purged_at_micros` → back to `persisted_at_micros`

[ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md) moved the
plan cutoff source from `persisted_at_micros` to `purged_at_micros`
specifically to eliminate the dedup-absorbed double-presence
between `Persist(T)` and `Purge(T)` — the merge layer's per-`row_uuid`
latest-commit-time dedup was the only thing collapsing the
cross-tier overlap, and that mechanism didn't exist for
`audit_data`. Cutting the cutoff at `purged_at` made the partition
strict and unambiguous.

[CHA-227](https://linear.app/chapala/issue/CHA-227) preserved the
strict-partition property and added plan-time threading so the
partition is honored across plan-time reads even under concurrent
lifecycle activity. That made `persisted_at` viable as the cutoff
source again: the partition is now structural in the plan, not
dependent on the choice of cutoff watermark.

Once `persisted_at` is viable, the cost trade-off flips. With
`purged_at` as the cutoff:

* The plan-visible cold/hot boundary lags the actual cold contents
  by the gap between Persist and Purge — typically small, but
  unbounded under scheduler back-pressure.
* A stale plan that outlives its grace can still hit ENOENT during
  the gap between Persist and the next Purge.

With `persisted_at` as the cutoff plus the grace-bounded Purge:

* The plan-visible cold/hot boundary tracks the actual cold contents
  exactly.
* A stale plan that outlives its grace is intercepted by the hard
  cap, not by ENOENT — a clean failure surface.

### Shared advisory lock key → per-operation keys

[ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md)'s "all three
take `lifecycle:{table_uuid}:{branch_uuid}`" decision was load-bearing
in its world: with the cutoff at `purged_at` and no destructive-side
grace, the scheduler's per-tick `Persist(T) → Snapshot(T) → Purge(T)`
chain had to be observably sequential — a late Purge from tick N
overlapping tick N+1's Persist could advance `purged_at` past the new
`persisted_at` and shift visibility incorrectly. Sharing the lock
key made the chain serial on T.

With pillar 1 (plan-time threading captures `persisted_at` bounds at
plan start) and pillar 3 (Purge's grace window structurally excludes
fresh persists from the chosen watermark), the chain is now safe
even when interleaved. The shared key was over-constraining; the
narrow per-operation keys preserve the only serialization that
remains correctness-load-bearing (`Persist↔Persist` for the
`audit_data` cold-segment duplicate-presence reason).

The per-table framing of 0018 — `Persist(T)`, `Snapshot(T)`,
`Purge(T)` as separate per-table RPCs, parallel across tables, with
the scheduler iterating per dirty table — is preserved. Only the
shared-key half of the lock decision is reversed.

## Subtleties

* **Grace is measured from Persist's `commit_micros`, not
  `effective_target` or `persisted_at_micros`.** `effective_target`
  is the upper bound on the data the Persist batch *covers* — it
  can sit minutes behind wall-clock if the Persist tx is slow to
  commit. The window we need to bound is "time since readers could
  first see the new segments," which is the Persist tx's commit
  timestamp. Same for compaction: grace from the merge tx's commit,
  not from when the underlying file was written.
* **Plan-time threading carries over unchanged from
  [CHA-227](https://linear.app/chapala/issue/CHA-227).** The
  three-step plan-read order (cutoff → snapshot picker bounded by
  cutoff → persist segments bounded by cutoff) keeps the same
  shape — only step 1's value source changes. The Persist
  open-tx clamp (pillar 2) is also unchanged structurally.
* **Stale plan from client retry.** A client holding a plan past
  `query_timeout_seconds` and trying to execute will be cancelled
  at the cap with `RESOURCE_EXHAUSTED`. Same shape as any DB
  `statement_timeout`: clear error, fresh plan on retry.

## Alternatives Considered

* **Keep [CHA-231](https://linear.app/chapala/issue/CHA-231) +
  [CHA-232](https://linear.app/chapala/issue/CHA-232) as two
  separate fixes.** The hot-side fix would thread a long-lived
  `REPEATABLE READ` transaction through the execute-time stream;
  the cold-side fix would add a standalone compaction grace window
  on its own knob. Rejected here in favor of this ADR's unified
  mechanism, but documented as the fallback: if the hard runtime
  cap proves unacceptable for primary use cases (e.g., legitimate
  hour-long analytical queries), reopen both tickets and pursue
  the tx-threading path. Reasons not to pick this path first:
  * Tx-in-stream Rust gymnastics for `audit_data`, which has no
    single owner for the tx lifetime.
  * Held PG connection per stream — vacuum delays scale with the
    longest-lived stream.
  * Two scopes (hot-side, cold-side) with overlapping mechanics
    and no unifying invariant.
  * No `query_timeout` knob means `audit_data` and `read_data`
    can run indefinitely. That's a behavior change either way
    once we expose any sort of stream-level cancellation; the
    unified path makes it explicit.
* **Keep [CHA-220](https://linear.app/chapala/issue/CHA-220)'s
  `purged_at_micros` cutoff source and only grace-bound Purge.**
  Rejected: a grace-bounded Purge effectively moves the visible
  cutoff back to "persisted + grace" anyway. Keeping `purged_at`
  as the nominal cutoff would mean a stale plan can still hit
  ENOENT between Persist and the next grace-elapsed Purge — the
  cap-based failure surface is cleaner.
* **Per-query `deadline_micros` request override.** Deferred.
  Allowing clients to *extend* the deadline beyond
  `query_timeout_seconds` breaks the correctness argument (the
  grace window only bounds the longest *possible* concurrent
  query). Allowing clients to *shorten* it is benign but has no
  pressing use case in v1; revisit if a workload appears that
  benefits from tighter per-query caps.

## Out of scope

* Hot `commit_tx_log` family branch-min GC —
  [CHA-221](https://linear.app/chapala/issue/CHA-221). This ADR
  locks in the `purged_at` contract CHA-221 composes from but does
  not implement the cleanup itself.
* Per-query `deadline_micros` request override (covered above).
* User-cancellation via Ctrl-C / gRPC stream abort — a separate
  mechanism that this cap does not replace.
* Backwards-compatibility shim for clients holding pre-cap plans —
  none. Retry-with-fresh-plan is the contract.
* Folding `segment_delete_set` into a general orphan sweeper — the
  orphan sweeper for aborted-tx orphans doesn't exist yet, and
  conflating its design with the compaction grace widens the scope
  without a corresponding correctness gain. Revisit when the orphan
  sweeper is in scope.

## Relationship to other ADRs

* **[ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md):**
  two decisions are superseded — the `plan()` cutoff source
  (`purged_at_micros` → back to `persisted_at_micros`) and the
  shared advisory lock key (`lifecycle:{table_uuid}:{branch_uuid}`
  → three per-operation keys `persist:`, `snapshot:`, `purge:`).
  The per-table decomposition (`Persist(T)`, `Snapshot(T)`,
  `Purge(T)` as separate per-table RPCs, parallel across different
  tables) and the strict-partition reasoning are preserved. 0018's
  call-out at the bottom (Persist "continues to clamp its watermark
  to `min(target_micros ?? now, oldest_open_began_at(branch) - 1)`")
  is pillar 2 of this ADR's system invariant.
* **[ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md):**
  the auditable-store "in at least one tier at every instant"
  invariant strengthens. Pre-CHA-220, hot rows were deleted in
  Persist's Phase 2; CHA-220 moved that to Purge; this ADR delays
  Purge by `query_timeout_seconds`. The invariant continues to
  hold and the window of double-presence widens by the grace
  amount.
* **[ADR 0017](0017-cold-data-segments-pre-joined-tx-metadata.md):**
  the prerequisite for `audit_data`'s strict tier partition,
  which is the reason `audit_data` cannot rely on merge-layer
  dedup and therefore cannot tolerate any TOCTOU window without
  data corruption. The grace window plus the cap is what makes
  the strict partition safe under concurrent lifecycle activity.
* **[ADR 0011](0011-transactional-metadata-stores.md):**
  the retention / audit-horizon framing is unchanged; the grace
  window is orthogonal to retention.

## Pre-1.0 migration

Drop-and-recreate per the
[CHA-203](https://linear.app/chapala/issue/CHA-203) precedent.
Covers the addition of `segment_delete_set`. No metadata migration
is needed for the cutoff-source flip — the `MetadataClient::plan`
change reads an existing column from an existing table.
