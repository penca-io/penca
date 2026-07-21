# ADR 0017 — Cold data segments pre-join tx metadata; `commit_tx_log` becomes hot-only

## Status

Accepted (CHA-218). Partially amended by
[ADR 0030](0030-cold-commit-tx-log-and-audit-join.md) (CHA-507): `author` and
`comment` are no longer denormalized onto cold data rows — they live in a slim
durable cold `tx_log` and are joined into `audit_data` on demand. The rest of
this ADR (the other tx-metadata columns pre-joined inline, `commit_tx_log`
otherwise hot-only) still holds.

## Context

Before CHA-218 each branch persist wrote three cold artifact kinds:

1. Per-table `upsert_log` segments — `(version_uuid, row_uuid, tx_uuid, <user_cols>)`.
2. Per-table `delete_log` segments — `(row_uuid, tx_uuid)`.
3. One shared `commit_tx_log` segment per branch — `(tx_uuid, branch_uuid, commit_micros)`.

Cold merge-on-read JOINed the data segments back against the cold
`commit_tx_log` segments on `tx_uuid` to recover each row's `commit_micros`
for visibility, dedup, and ordering. The snapshot watermark was computed
by scanning the cold `commit_tx_log` segments, filtering by branch, and taking
`MAX(commit_micros)`. The `LogKind` discriminator on
`table_persist_metadata` had three values: `{upsert_log, delete_log, commit_tx_log}`.

The structural cost of that arrangement:

1. **Per-tx framing leaked into cold.** Every read against cold paid a
   JOIN that hot already pays at write time (via `t.commit_micros`
   stamped at `CommitTx`). Hot's `commit_tx_log` partition is the single source
   of truth for per-tx framing while a tx is live; once a tx commits, the
   only thing cold readers ever ask about it is "what did its
   `commit_micros` end up as." That's one scalar per row — the
   JOIN was paying full table-shape costs to recover it.
2. **Cold commit_tx_log was a separate `TableProvider` registration** in
   DataFusion (`penca-dl`), with its own segment list in
   `PersistPlan.commit_tx_log_segments`, its own row in
   `table_persist_metadata` keyed off the per-catalog system commit_tx_log
   `table_uuid`, and its own match arms in the lifecycle / metadata code
   paths.
3. **Snapshot used cold commit_tx_log as its watermark source.** That tied
   snapshot's correctness to whether commit_tx_log had ever been persisted, even
   though the only thing it actually needed was "what's the latest
   committed_at this table has in cold."

`tx_uuid` itself was sticking around on cold rows only to power that
JOIN — cold readers never used `tx_uuid` for anything else. Once the
JOIN is gone, `tx_uuid` is unused on cold rows.

## Decision

**Pre-join the `commit_tx_log` metadata columns onto every cold data row at
persist time.** Each cold upsert/delete segment row carries
`(commit_micros, began_at_micros, comment, author)` inline. Two
columns were appended to this denormalized block after this ADR landed:
`written_at_micros` (CHA-243, the per-row write timestamp) and
`commit_seq_num` (CHA-430, the per-branch gapless commit-order serial from
CHA-428 — lets cold-segment selection prune on the seq axis and surfaces
a gap-tolerant `audit_data` cursor). Both trail the original four; the
trailing order is load-bearing (the cold on-disk schema tail is
projected position-for-position against the hot JOIN result tail).

- Cold upsert segment row: `(row_uuid, <user_cols>, written_at_micros, commit_micros, began_at_micros, comment, author, commit_seq_num)`.
- Cold delete segment row: `(row_uuid, <pk_cols>, written_at_micros, commit_micros, began_at_micros, comment, author, commit_seq_num)`.

Drop `version_uuid` and `tx_uuid` from cold rows. The hot upsert/delete
tables keep `version_uuid` (PK enforcing the auditable-store invariant
per ADR 0013) and `tx_uuid` (FK to `commit_tx_log`). On the cold side every
version is already deduped (one row per `(row_uuid, tx_uuid)` from
hot) and ordering / visibility is governed by `commit_micros` —
the original per-tx identity has done its job by the time data lands
on cold.

Stop emitting cold `commit_tx_log` segments. Reduce `LogKind` to
`{upsert_log, delete_log}` — drop the `TxLog` variant from the enum,
the Postgres `CHECK` constraint, and every match site. Drop
`PersistPlan.commit_tx_log_segments` from the proto. Snapshot's watermark
becomes `MAX(commit_micros)` across cold upsert + delete
segments, surfaced via `PersistPlan.persisted_at_micros` (the same value
the planner already computes from `seg.max_tx_commit_micros`).

Cold merge-on-read becomes a pure scan: no JOIN against commit_tx_log, no
shared SQL builder dialect-quirk for the cold-tier OR'd open-tx
branch. The visibility predicate collapses to
`commit_micros <= as_of` (or strict `<` for `OpenTx`). The hot
path keeps its existing `build_merge_resolved` SQL — its JOIN against
hot `commit_tx_log` is structural (`commit_micros` is stamped on the
`commit_tx_log` row at `CommitTx` time, not on the upsert/delete row).

## Join-before-purge ordering invariant

The Phase 2 hot `commit_tx_log` purge (in `lifecycle::persist_locked` —
`DELETE FROM commit_tx_log WHERE commit_micros <= effective_target`)
runs in the same persist transaction that wrote the cold segments. By
the time the hot row is deleted, Phase 1 has already JOINed against
it and projected its four metadata columns onto every cold row of
every touched table that has unpersisted data for that tx. The
information the purge drops from hot is preserved on cold. **No
information loss; the auditable-store invariant holds.**

This invariant is what makes the change a single atomic refactor.
Splitting "widen the cold-side read" and "purge hot commit_tx_log" across
two PRs would put cold readers in a window where neither tier holds
the tx metadata for already-persisted rows.

## Hot vs cold asymmetry — why hot still JOINs

Hot `upsert_log` / `delete_log` rows are written by `MutateData`
before `CommitTx` is called, so they cannot carry
`commit_micros` (which is set by Pg at `CommitTx` time, on the
`commit_tx_log` row). Hot's merge-on-read therefore still JOINs against the
hot `commit_tx_log` partition for the `commit_micros` predicate.

That asymmetry is structural: the cold-side denormalization happens
at *persist* time, when every row's tx has long since committed and the
metadata is known. Trying to extend the same denormalization to hot
would require either an UPDATE wave on every committed row (huge
write amplification) or stamping `commit_micros` on each row at
INSERT time (impossible — that timestamp is only known at commit).

## Snapshot watermark

Snapshot's watermark switches from "scan cold commit_tx_log → filter by
branch → MAX(commit_micros)" to
`PersistPlan.persisted_at_micros`. That field is already populated by the
planner from `MAX(seg.max_tx_commit_micros)` over the same
upsert + delete segments the snapshot read uses. No extra cold IO.

## Relationship to other ADRs

- **[ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md):**
  `version_uuid` enforces "one row per `(row_uuid, tx_uuid)`" on hot.
  Once a row is on cold it has already been deduped — the cold side
  doesn't need `version_uuid` or `tx_uuid` to maintain the invariant.
- **[ADR 0016](0016-canonical-uuid-construction-for-derived-rows.md):**
  the inputs to `row_uuid_for_pk` and `table_persist_uuid` were
  already independent of `tx_uuid` for the metadata-row level; this
  ADR doesn't touch the UUID construction rules.

## Out of scope

- Hot `upsert_log` / `delete_log` / `commit_tx_log` schemas — unchanged.
- A measured storage-cost comparison of the per-row denormalization.
  The four added columns are two `Int64`s + two short `Utf8`s; the
  former cold `commit_tx_log` segment was one row per tx (cheap to drop) but
  every persisted row paid a tx_uuid + version_uuid pair under the old
  layout. Net change is roughly neutral; track precise numbers as a
  follow-up benchmark.
- Async/streaming changes to `audit_data` — orthogonal to CHA-148.

## Pre-1.0 migration

Drop-and-recreate per the [CHA-203](https://linear.app/chapala/issue/CHA-203)
precedent. Covers both the cold segment shape change and the
`log_kind` `CHECK` reduction.
