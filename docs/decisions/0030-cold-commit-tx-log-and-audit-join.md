# ADR 0030 — Durable cold `tx_log`; author/comment joined, not denormalized

## Status

Accepted (CHA-507). Amends [ADR 0017](0017-cold-data-segments-pre-joined-tx-metadata.md).

## Context

ADR 0017 made `commit_tx_log` **hot-only**: each branch persist pre-joined the
tx metadata (`commit_micros`, `began_at_micros`, `comment`, `author`, and later
`commit_seq_num`) directly onto every cold data row, and dropped the separate
cold `commit_tx_log` artifact. That removed the cold read-side JOIN, but left
two gaps that surfaced in the branch-inheritance epic:

1. **Fork positions don't survive purge.** `resolve_fork_watermark` (CHA-505)
   resolves a fork point — `(commit_seq_num, commit_micros)` — by reading the
   **hot** `commit_tx_log`. `PurgeTxLog` GCs those rows once persisted, so a
   legitimately-committed fork position resolves to `INVALID_ARGUMENT` after
   purge even though the data is durably in cold. Nothing in cold records the
   commit-order → wall-clock → author/comment map independently of the data
   rows (an empty/metadata-only commit writes no data rows at all).

2. **author/comment pay for almost nothing on cold rows.** They are identical
   for every row of a tx and highly redundant, yet stored on each cold data
   row, while only `audit_data` ever reads them.

## Decision

Reintroduce a **slim, durable cold `tx_log`** and reverse the author/comment
half of ADR 0017's denormalization.

### Cold `tx_log` substrate

- `persist_tx_log` flushes the `(W_txlog, target]` slice of hot `commit_tx_log`
  — projected to `(commit_seq_num, commit_micros, author, comment)`, sorted by
  `commit_seq_num` — into per-`(catalog, branch)` cold `tx_log` segments,
  recorded in a new **unpartitioned** `tx_log_persist_segment_metadata` table
  (`branch_uuid` a column; the log is slim + low-volume). The write is
  two-phase (insert uncommitted → write file → commit) like the data persist;
  a crash leaves the segment uncommitted (invisible) and the next run redoes
  the range. The segment file lives under its own `tx_log` URI kind, kept out
  of the persist/snapshot orphan-retirement sweeps; `persist_tx_log` reclaims
  its own uncommitted orphans (file-first, then row).
- **`W_txlog`** (the tx_log persist watermark) is *derived*, not tracked:
  `MAX(max_commit_seq_num)` over committed `tx_log_persist_segment_metadata`.
- Reads seek the sorted `commit_seq_num` / `commit_micros` columns — no
  separate index (the artifact is already sorted).

### Ordering invariants (load-bearing)

- **`persist_tx_log` runs FIRST** in `persist_branch` /
  `persist_and_snapshot_branch`, before any data-table persist, and fail-fast.
  Cold data segments drop author/comment and depend on the cold tx_log join, so
  the tx_log covering `<= T` must be durable before any data segment
  referencing those seqs can flip visible — else an
  `audit_data(include_tx_metadata)` in that window joins a tx_log missing those
  rows. This reintroduces, at branch scope, the "tx_log first" ordering ADR
  0017 removed at per-table scope, for a different (visibility) reason. It is a
  deliberate exception to `persist_and_snapshot_branch`'s continue-on-error
  policy: the flush is a correctness prerequisite for every table.
- **`PurgeTxLog` clamps** its `commit_tx_log` deletion cutoff to
  `min(Pu, W_txlog)`, so a hot row is never GC'd before its cold copy exists.
  With `persist_tx_log`-first + fail-fast, `W_txlog >=` the data-persist
  watermark in the branch flow, so the clamp is non-binding there; it binds
  only when data was persisted past what the tx_log covers (mixed per-table
  persist), holding those hot rows until their cold copy exists. A branch that
  never ran `persist_tx_log` (`W_txlog` unset) is unconstrained — pre-CHA-507
  behavior.

### author/comment: joined, not denormalized

- Cold data segments no longer carry `author`/`comment`
  (`cold_tx_metadata_fields` and the persist-side JOIN drop them). The other
  axes (`commit_micros`, `began_at_micros`, `write_seq_num`, `commit_seq_num`)
  stay inline.
- `AuditDataRequest.include_tx_metadata` (a new optional field) gates
  reattachment. When set, `audit_data` reattaches `author`/`comment` by joining
  the cold `tx_log` on `commit_seq_num` (cold tier) or projecting them from the
  existing `commit_tx_log` JOIN (hot tier); when unset (default) they are
  omitted — pay-for-what-you-use. This is a pre-1.0 behavior change: callers
  that need author/comment must opt in.
- The cold audit path is **unified onto DataFusion** (register the cold data +
  cold tx_log batches, `LEFT JOIN` on `commit_seq_num`, windows/ids as SQL),
  replacing the hand-rolled Arrow filter/project.

## Consequences

- Fork positions resolve after purge (the motivating consumer): a hot miss in
  `resolve_fork_watermark` falls back to a bounded cold `tx_log` seek.
- Cold data rows shrink (two Utf8 columns gone). The audit join is the added
  cost, paid only when `include_tx_metadata` is requested.
- `resolve_fork_watermark`'s cold fallback needs cold-read capability in the
  write pod (a `FormatReader` map on `WriteServiceImpl`).
- Time-travel / audit reads that outlive hot GC are unblocked by the same
  durable cold `tx_log`.

## Alternatives considered

- **Derive seq↔micros from cold data segments** (CHA-430 stamps both): rejected
  — empty/metadata-only commits write no data rows, so their position is
  unrecoverable, and it would scan every table's segments for one seq.
- **Keep author/comment denormalized** (ADR 0017 status quo): rejected — the
  cold `tx_log` is needed for fork durability regardless, and once it exists,
  joining author/comment on demand removes per-row redundancy.
