# ADR 0021 — Persist owns aborted hot-row cleanup

## Status

Proposed ([CHA-221](https://linear.app/chapala/issue/CHA-221)).

> **Reversed by [ADR 0027](0027-decoupled-purge-seq-cutoff-and-split-grace.md)
> (CHA-444).** Persist becomes pure committed-only CDC, so it can no longer
> clean aborted hot rows; aborted-hot cleanup moves back to Purge, on an
> independent `aborted_at_seq_num` / `Pa` axis. The open-tx-clamp transitive
> reasoning below is obsolete (the clamp is removed). Read for history.

Builds on:

- [ADR 0019](0019-plan-time-pinning-and-universal-grace-window.md) —
  per-operation per-table lock keys, universal grace window, and the
  open-tx invariant that `persisted_at(T) < every open tx's
  began_at`. v2.1 reuses ADR 0019's existing
  `oldest_open_began_at - 1` clamp in Persist; no new clamp logic.
- [ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md) — the
  CHA-220 reshape of Persist + Purge into per-table operations.
- CHA-227 stamping rule (canonical prose in `docs/algorithms.md`
  §"Persist") — broadened by this ADR from
  `max(committed_at over persisted rows)` to
  `max(committed_at over persisted rows, aborted_at over aborted
  txs whose hot rows Persist just cleaned)`.

## Context

Pre-CHA-221, aborted-tx hot rows in `upsert_log[T,B]` /
`delete_log[T,B]` were never cleaned up:

- `Persist(T)` only moves committed rows to cold (filter:
  `commit_micros IS NOT NULL`); aborted rows with
  `committed_at = NULL` are invisible to Persist.
- `Purge(T)` deletes hot rows by joining with `commit_tx_log` (committed-tx
  index); aborted txs aren't in `commit_tx_log`, so their hot rows are
  untouched.
- The four hot tx-log family tables (`commit_tx_log`, `tx_table_log`,
  `abort_tx_log`, `begin_tx_log`) similarly accumulate aborted-tx
  metadata indefinitely.

CHA-221's `PurgeTxLog` was introduced to clean the tx-log family.
The ticket's literal step-5–8 chained `NOT IN` algorithm had two
known correctness gaps surfaced by integration testing:

1. **Open-tx race.** Step 6's
   `DELETE FROM tx_table_log WHERE tx_uuid NOT IN (commit_tx_log ∪
   abort_tx_log)` matches in-flight open writers (their tx_uuid is
   in neither at SQL-snapshot time). Their `tx_table_log` rows get
   deleted mid-transaction; step 8 then deletes their `begin_tx_log`
   row by the same logic. The next CommitTx fails with
   `NotFoundError`.

2. **Aborted-with-writes chicken-and-egg leak.** For an aborted tx X
   with writes: step 6 won't delete `tx_table_log[X]` because X is
   in `abort_tx_log`; step 7 won't delete `abort_tx_log[X]` because
   X is in `tx_table_log`. Both rows + the corresponding hot upsert
   / delete rows leak indefinitely.

The v1 implementation patched (1) with an eligibility-set CTE
gated on `NOT IN tx_table_log` — but (2) remained. A third leak
also surfaced: tables whose only activity was aborted writes
never have `Purge(T)` called by the scheduler (because they have
no committed `table_persist_metadata` rows for
`ListPersistedTables` to enumerate), so their hot rows + commit_tx_log
family metadata leak indefinitely.

## Decision

Move aborted hot-row cleanup into `Persist(T)`, leveraging
Persist's existing open-tx clamp instead of duplicating it in
`Purge(T)` or `PurgeTxLog`. `persisted_at(T)` broadens to cover
both committed and aborted timestamps. Three concrete changes:

### 1. `Persist(T)` cleans aborted hot rows

After computing `effective_target = min(target ?? now,
oldest_open_began_at - 1)`, Persist now also reads aborted
tx_uuids past that clamp via `tx_table_log[B] ⋈ abort_tx_log[B]`
filtered by `table_uuid = T` and `aborted_at_micros <=
effective_target_micros`. Inside the existing phase-2 transaction
(atomic with the per-`log_kind` `commit_table_persist` flips),
Persist DELETEs those tx_uuids' rows from `upsert_log[T,B]` and
`delete_log[T,B]`.

For the all-aborts case (no committed rows past clamp but
aborted_tx_uuids non-empty), Persist writes a single
`table_persist_metadata` row with `log_kind = upsert_log` and no
segment rows — the schema supports this directly (the NOT-NULL
columns `min_tx_commit_micros` / `max_tx_commit_micros`
live on `table_persist_segment_metadata`, which simply doesn't get
a row in this case; the parent `table_persist_metadata` only
requires `persisted_at_micros` to be non-null). No schema
migration.

### 2. CHA-227 stamping rule broadens

`persisted_at_micros` is now stamped to:

```
max(
    max(committed_at over committed rows being persisted),
    max(aborted_at over aborted txs whose hot rows Persist just cleaned)
)
```

The open-tx invariant `persisted_at < every open tx's began_at`
is preserved transitively: every `committed_at <= effective_target`
(by Persist's hot-read filter), every `aborted_at <=
effective_target` (by the aborted-tx read), and `effective_target
< oldest_open_began_at` by Persist's existing clamp.

### 3. `ListModifiedTables` enumerates aborted-tx touches

The scheduler-driving listing
`MetadataClient::list_modified_table_uuids_paginated` is extended
so its inner subquery becomes `tx_uuid IN (commit_tx_log[B] UNION ALL
abort_tx_log[B])` instead of `commit_tx_log[B]` only. Window predicate
applies to `committed_at` or `aborted_at` per row; ordering key
generalizes to `MAX(modified_at_micros)` where `modified_at` is
the per-row committed_at or aborted_at value.

This is the load-bearing change that closes the aborted-only-table
leak: the scheduler now sees aborted-only tables in the per-tick
Persist enumeration, fires Persist on them, which clears their
hot rows and advances `persisted_at(T)` past the abort. Once
`Purge(T)` then runs (because `ListPersistedTables` now finds the
table in `table_persist_metadata`), `purged_at(T)` advances past
the abort, and `PurgeTxLog` can clean the metadata.

### 4. Strict-advance no-op gate on Persist

Persist gains an explicit `persisted_at <= last_persisted_at(T)`
no-op gate (analogous to Purge's `compute_purge_watermark`'s
strict-advance check). Without this, a re-Persist on a table
whose abort_tx_log entries are still in PG but whose hot rows are
already cleaned would re-write a redundant
`table_persist_metadata` row every tick. With the gate, Persist
genuinely no-ops once it has caught up to all settled txs past
the clamp.

## Consequences

### Positive

- **No orphaned aborted hot rows.** Persist cleans them at the
  same instant it advances `persisted_at(T)`, so any abort whose
  `aborted_at <= purged_at(T)` is guaranteed cleaned in the hot
  tier by the time downstream consumers (`PurgeTxLog`) GC the
  metadata.

- **No aborted-with-writes metadata leak.** `PurgeTxLog`'s
  composite SQL drops the v1 `NOT IN tx_table_log` clause; aborted
  txs in the eligibility set get fully cleaned (`abort_tx_log` +
  `tx_table_log` + `begin_tx_log`) in one composite DELETE. The
  safety chain holds because Persist already cleaned their hot
  rows.

- **No aborted-only-table leak.** The extended
  `ListModifiedTables` enumeration ensures the scheduler fires
  Persist on aborted-only tables. After Persist, the table has a
  `table_persist_metadata` row → `ListPersistedTables` finds it
  → Purge runs → `purged_at(T)` advances → `PurgeTxLog` cleans
  metadata.

- **Open-tx race fixed (was already fixed in v1's
  eligibility-set patch, preserved here).** Open writers are not
  in `commit_tx_log` or `abort_tx_log` at SQL-snapshot time, so they're
  never in the `eligible` CTE; their `tx_table_log` and
  `begin_tx_log` state is preserved unconditionally.

- **Open-tx clamp reused, not duplicated.** Persist's existing
  `oldest_open_began_at` clamp covers both committed_at and
  aborted_at bounds. `Purge(T)` and `PurgeTxLog` never read
  `abort_tx_log` directly.

### Negative

- **Persist's scope grows.** A per-table op now reads
  branch-scoped state (`abort_tx_log[B] ⋈ tx_table_log[B]`). The
  scope is bounded: one inner join filtered by `table_uuid`,
  cardinality is `O(aborted txs that touched T)`. Acceptable.

- **Slight overhead on aborted-only tables.** Each scheduler tick
  on an aborted-only table runs Persist + Snapshot + Purge +
  PurgeTxLog (Snapshot is a no-op via the CHA-228 empty-merge
  placeholder path). Bounded by aborted-only-table count per
  branch.

- **CHA-227 stamping rule is no longer purely
  `max(committed_at)`.** Existing consumers of `persisted_at`
  (plan-time cutoff, snapshot stamping, compact) treat the
  broadened value correctly because committed-row-only segments
  still bound the read window strictly below
  `persisted_at + 1` — aborted_at contributions to the
  watermark don't introduce any rows past it. Test docstring
  updates in `test_persist_stamps_watermark_at_max_committed_at`,
  `test_persist_respects_open_tx_clamp_under_new_stamping`, and
  `test_snapshot_reads_watermarks_directly` note the broadened
  rule; assertion logic preserved (no aborts in those tests'
  setups).

## Mechanism non-goals

- **`Purge(T)` MUST NOT read `abort_tx_log[B]`.** Aborted handling
  is Persist's responsibility; Purge stays focused on committed
  hot DELETE + watermark stamping.

- **`PurgeTxLog` MUST NOT call `Purge(T)`.** Cross-layer coupling;
  scheduler triggers Purge based on `ListPersistedTables`.

- **`PurgeTxLog`'s composite SQL MUST NOT use
  `tx_uuid NOT IN tx_table_log` clauses anywhere.** Persist's
  broadened `persisted_at` + the as-of filter on
  `table_purge_metadata.commit_micros` make them
  unnecessary, and they reintroduce the v1 chicken-and-egg leak.

- **`PurgeTxLog` MUST NOT wrap multiple SQL statements in a PG
  transaction.** The as-of filter is the explicit substitute for
  transaction-isolation; one composite SQL is sufficient.

- **No new scheduler enumeration RPC for aborts.**
  `ListModifiedTables` is extended in place; the semantic
  broadens via the SQL change, not a new endpoint.

- **No new "aborted-only" fast-path in `Snapshot(T)`.** The
  existing CHA-228 empty-merge placeholder handling is
  sufficient; adding a new branch would duplicate logic.

- **No schema migration.** `table_persist_segment_metadata`'s
  `NOT NULL` columns are dodged by simply not writing a segment
  row for all-aborts persists.

## Alternatives considered

### A. Have `Purge(T)` clean aborted hot rows

Symmetrical to Persist in shape, but Purge would need to
re-implement the open-tx clamp logic that Persist already owns.
Duplicates the load-bearing safety mechanism in two places;
correctness becomes a matter of keeping the two clamps in
lock-step rather than reusing the canonical one.

### B. Have `PurgeTxLog` clean aborted hot rows directly

PurgeTxLog would enumerate `tx_table_log[B] ⋈ abort_tx_log[B]`
to find aborted hot rows to clean, gated on the same cutoff it
uses for tx-log family metadata. Pulls the open-tx clamp logic
into PurgeTxLog. Same duplication concern as (A); additionally,
PurgeTxLog would need to issue per-table hot DELETEs alongside
its branch-scoped composite DELETE, breaking the "one composite
SQL" simplicity.

### C. Add a new scheduler enumeration RPC for aborts

`ListTablesTouchedByAborts(catalog, branch, window)` joined with
`ListPersistedTables` and `ListModifiedTables`. Triples the
scheduler's per-tick listing surface area. The chosen approach
(extending `ListModifiedTables` in place) achieves the same
result with one SQL change.

Option (chosen) keeps the clamp logic in Persist where it
already lives, broadens `persisted_at` to cover both halves, and
gets the aborted-only-table coverage from a one-line listing
extension.

## Related

- [CHA-220](https://linear.app/chapala/issue/CHA-220) — per-table
  Persist + Purge reshape; introduced `table_purge_metadata`.
- [CHA-227](https://linear.app/chapala/issue/CHA-227) — CHA-227
  stamping rule (canonical prose in `docs/algorithms.md`
  §"Persist"); broadened by this ADR.
- [CHA-233](https://linear.app/chapala/issue/CHA-233) /
  [ADR 0019](0019-plan-time-pinning-and-universal-grace-window.md)
  — universal grace window; preserved.
- [CHA-154](https://linear.app/chapala/issue/CHA-154) — lifecycle
  scheduler; observable behavior broadens via the extended
  `ListModifiedTables`.
- [CHA-246](https://linear.app/chapala/issue/CHA-246) — DeleteTable
  leaks per-(table, branch) hot PG tables; independent (separate
  ticket).
