# ADR 0026: Cold-tier indexes — index-as-table, per-segment, selection-not-filtering

## Status

Proposed (CHA-455 / CHA-412 / CHA-454, 2026-06-14). Builds on ADR 0024
(incremental snapshot), ADR 0023 (single query-execution engine), and
ADR 0012 (metadata as first-class tables). Records the corrected design
after CHA-412's first attempt (PR #245) was parked — see "the obvious
lever was wrong" below.

## Context

A Penca cold read scans immutable snapshot segments. A point or
high-selectivity lookup on an indexed column (`WHERE id = 1`, or the
system `row_uuid` PK probe behind CHA-398) is an **O(rows)
full-scan-and-filter** today: the `SnapshotTableProvider` (CHA-411)
materializes each segment and DataFusion's residual filter walks it
linearly. We want O(log n).

The obvious lever was tried first and is wrong. CHA-412 / CHA-410
originally proposed delivering point lookups by advertising the
segments' sort order to DataFusion (`output_ordering`). That advertise
**does not** become an execution-time seek for our in-memory cold
execution — it is *planner* metadata (it elides a redundant `SortExec`
and enables order-aware operators), but the scan still streams and the
operator still walks the rows. An advertised ordering never turns into a
binary search. Discovering this parked PR #245 and split the work into
an ordering ticket (CHA-410, planner-only) and a separate seek
(CHA-454).

Any index here must compose with four existing decisions:

- **Immutable, carry-forward snapshots** (ADR 0024): segments never
  mutate; an incremental snapshot rewrites only changed segments and
  carries the rest forward by reference (CHA-406).
- **One filter-execution engine** (ADR 0023): DataFusion is the sole
  place predicates run; no second engine evaluates the user filter.
- **Ref-counted cold GC + grace** (CHA-405 / ADR 0019): a cold file is
  deleted only when no snapshot references it and the grace window has
  elapsed.
- **Metadata as auditable tables** (ADR 0012): logical schema objects
  live in MVCC-versioned Postgres stores, distinct from the
  lifecycle-written physical-materialization tables.

## Decision

### 1. The seek is hand-rolled; `output_ordering` is not it

Cold point lookups are delivered by a **hand-rolled, index-driven
selective read** in the `SnapshotTableProvider` (CHA-454): probe the
index for the matching row positions, then selectively decode only those
rows (`ParquetAccessPlan` / `RowSelection`, or `arrow::compute::take` on
a cached segment). `output_ordering` (CHA-410) is a separate, still-valid
optimization — `SortExec` elision and order-aware operators — but it
delivers **no** lookup acceleration and must not be conflated with one.

### 2. Index-as-table; selection, not filtering

The index artifact is the indexed column **sorted**, paired with each
base row's **physical position** within the segment — a flat sorted
`(key, row_offset)`, one entry per base row. Duplicate keys form a
contiguous run (e.g. `age = [21, 21, 21, 22, 23]` → `offset =
[0, 1, 3, 2, 4]`); a lookup binary-searches to the first match and scans
the equal-key run. (A compact `(key, list<offset>)` encoding is an
optional builder choice, never a catalog concept.)

Per ADR 0023 the index does **selection, not filtering**: it supplies
*which rows to decode*, and DataFusion still applies the exact predicate
to the decoded rows. The index never eliminates a row and never
constitutes an answer. It is **snapshot-tier only** — the persist and hot
tiers merge normally and the exclusion set still shadows changed rows, so
a value changed by a post-snapshot persist update is handled by the
resolve, not by the index. The index is a **baseline accelerator, not a
complete answer**; the exclusion-set query (CHA-142) is built unfiltered
and is never index-pruned.

### 3. Per-segment sidecars, not one global per-snapshot artifact

Index artifacts are **per-segment** — one sidecar per `(segment, index)`
— not a single global artifact spanning the whole snapshot. Because
snapshots are immutable and carry-forward (ADR 0024) carries an unchanged
segment forward by reference, its sidecar is carried forward with it →
**incremental index maintenance**: build sidecars only for new/rewritten
segments; compaction rebuilds only the compacted outputs' sidecars. A
single global artifact would be rebuilt over *all* snapshot rows every
cycle, reintroducing exactly the full-rewrite write-amplification that
incremental snapshots exist to eliminate — for indexed tables.

The trade-off is at lookup time: a random key (notably `row_uuid`, a
uniform hash with no usable order) probes N per-segment sidecars rather
than one global index. N is bounded by `max_segment_bytes` and the
sidecars are cache-warm (CHA-252); the maintenance win dominates given
carry-forward is the system's direction. The sidecars are themselves cold
files and flow through the ref-counted GC / `segment_delete_set` sweep
(CHA-405 / ADR 0019 grace) when their base segment is retired or
compacted away.

### 4. No uniqueness flag

There is no `unique` flag on an index. These are async cold
read-accelerators built at snapshot time; they **cannot** enforce a
write-time uniqueness *constraint* (the write already committed in hot
Postgres before any cold index existed), and the flat sorted artifact
handles duplicate keys natively. The PK index is unique by construction
(recorded as `index_kind = pk`), not by a flag. Real unique constraints,
if ever wanted, are a hot-tier concern and out of scope here.

### 5. Two metadata tables: definition vs materialization

> **Materialization-side update (CHA-412, 2026-06-20).** The
> *materialization* side below was originally specced as a single
> `segment_index_metadata` table with an `index_kind` (`pk` | `secondary`)
> discriminator. As implemented it is **two** tables following the
> `table_snapshot_metadata` → `table_snapshot_segment_metadata` convention:
> **`table_snapshot_index_metadata`** (parent — one row per `(snapshot,
> index)`, a fileless header re-declared each snapshot) and
> **`table_snapshot_segment_index_metadata`** (child — one row per `(segment,
> index)` sidecar, referencing its parent via `table_snapshot_index_uuid`).
> **`index_kind` is dropped, and `index_uuid` lives only on the parent** — the
> role discriminator `index_uuid IS NULL` (NULL ⇒ the strictly-internal
> `row_uuid` identity index; non-NULL ⇒ a *declared* index) is a parent property
> the child reaches through its FK, not a duplicated column. A non-NULL
> `index_uuid` is either a **built-in system-table name index** (CHA-481 —
> deterministic via `naming::system_name_index_uuid`, auto-built on every
> `__penca_system__.{schemas,tables,indexes}` snapshot, *never* a row in the
> `__penca_system__.indexes` user-DDL registry) or a **user secondary index**
> (CHA-463, a logical reference to `index_metadata`). The built-in name index is
> deliberately non-NULL so the `row_uuid` read plan's `index_uuid IS NULL` filter
> excludes it (CHA-454 / the CHA-473 by-uuid path is unaffected). Every sidecar
> id is the system's xxh3 identity (`row_uuid_for_pk`), computed in Rust, never a
> SQL-side hash. The parent gives
> planning a direct "does snapshot S have index X?" lookup instead of a
> segment fan-out scan. The *definition* side (`index_metadata`) is unchanged.
> Persist-tier materialization is deferred to CHA-464. The prose below is the
> original CHA-455 framing, kept for the rationale.

Indexes extend the existing **auditable-store ↔ lifecycle-metadata**
seam (ADR 0012), as two tables (CHA-455):

- **`index_metadata`** — the user-facing **auditable** store written by
  `CreateIndex` / inline `CreateTable.indexes`, branch-partitioned and
  time-travelable like `table_metadata`. The *definition*:
  `(index_uuid, branch_uuid, table_uuid, index_name, columns,
  index_type)`. `index_name` is unique only within a table.
- **`segment_index_metadata`** — lifecycle-written (snapshot, compact),
  query-planning-read. The *materialization*: one row per
  `(segment_uuid, index)` sidecar, for system PK (`row_uuid`) and user
  secondary indexes, on snapshot and persist segments (the table's
  target domain; v1 materializes snapshot PK + snapshot secondary — see
  Out of scope). Shaped like the
  segment-metadata tables (an index sidecar *is* a cold file):
  `object_uri / offset / length / format / size_bytes / statistics` plus
  the `commit_micros / written_at_micros` two-phase-commit pair,
  plus `index_kind` (`pk` | `secondary`) and `index_uuid`. `index_uuid`
  is NULL for the system PK index and otherwise a logical reference to
  `index_metadata`, not an enforced FK (ADR 0015). `index_kind` is
  redundant with `index_uuid IS NULL` for the pk-vs-secondary split
  today, but is kept explicit so a future auto-built system index (which
  would also carry a NULL `index_uuid`) stays distinguishable from the PK.

Two orthogonal discriminators, easily conflated: **`index_kind`** (`pk`
vs `secondary`) is the index's *role*; **`index_type`** (`SCALAR_BTREE`,
later `SCALAR_HASH`) is its *physical layout*. They are independent — the
PK index is `kind = pk, type = SCALAR_BTREE`; a user btree secondary is
`kind = secondary, type = SCALAR_BTREE`.

**Query planning reads `segment_index_metadata`, never
`index_metadata`.** Planning needs the artifact **URIs for the segments
in the plan** — physical lifecycle outputs, not definitions. Even
time-travelling the auditable definition to "the index as defined when
snapshot S was built" would not yield artifact URIs. A consequence falls
out and is acceptable (indexes are correctness-independent
read-accelerators): a freshly-`CREATE`d index is not usable until the
**next snapshot** materializes its sidecars, and `DROP` is lazy — sidecars
linger until the next snapshot stops emitting them and GC reclaims them.

### 6. Format: uniform build, format-specific apply

The **build** is format-agnostic. The index-as-table sidecar is a sorted
`(key, row_offset)` cold file — derived data, independent of the base
segment's encoding — so we build it for **every** snapshot regardless of
storage format. The sidecar follows the table's storage format (Parquet
table → `.parquet` sidecar, Lance table → `.lance` sidecar), written via
the same `FormatWriter` as the base segments. Gating the build on Parquet
would leave Lance tables with no `row_uuid` identity index, which is wrong:
the identity index is a single uniform mechanism, not a per-format feature.

The **apply** (the CHA-454 seek) is format-specific only in how a selected
set of row offsets is read back: Parquet applies the selection via
`ParquetAccessPlan` / `RowSelection`; Lance applies it via its file
reader's take-by-offset. Both readers already support "read just these
rows," so the seek is uniform at the artifact level (binary-search the
sorted `key`, then take the offsets) and diverges only at the final
per-format read.

This is distinct from **CHA-339** (Lance native scalar indexes /
filter-aware lance-file decoders), which is a *predicate-pushdown*
optimization for user-column **filters** — a different concern from the
internal `row_uuid` identity seek, and **not** a replacement for the
hand-rolled identity index. The `index_type` knob is honored-or-ignored
per format, not assumed universal.

### 7. The point-read corollary: existence gate ≠ exclusion set (hot tier)

§2 records two choices made for the **cold baseline**: the exclusion set is
"built unfiltered and never index-pruned," and "the persist and hot tiers merge
normally." That is sound where it was measured — over cache-warm snapshot
segments an unfiltered scan feeding the DataFusion residual is cheap. It does
**not** hold for a **point read that engages the hot tier**, where "unfiltered"
means a full scan of the per-`(table, branch)` hot logs. CHA-472's OLTP
profiling made the cost concrete; it is recorded here so it is not relitigated.

**Measured root cause.** Every post-Persist resolve runs
`phase_one_fence_and_existence`, whose `hot_present` gate originally mirrored the
merge predicate: `EXISTS` over a JOIN of the hot upsert/delete logs to the
`commit_tx_log` partition, fenced by `commit_seq_num > fence ∧ committed_at ≤ as_of`.
`EXPLAIN (ANALYZE, BUFFERS)` showed this seq-scans the whole `commit_tx_log` partition
and nested-loop-probes the hot log once per row — **~1,724 shared-buffer hits
per call** to answer a boolean. `pg_stat_statements` put server-side mean exec
at 0.66 ms but `max_exec` at **28.7 ms** under the loaded run; it is **not**
planning (mean plan 0.07 ms — sqlx prepares + reuses) and **not** disk (0.004
blocks read/call) — it is an all-in-cache scan whose wall-clock balloons under
contention. This is the same shape as the exclusion-set scan (CHA-142 /
`build_exclusion_set` — since retired by CHA-368, folded into the two-arm
resolve; the table-wide-scan shape it illustrates is unchanged): **table-wide
work that ignores the point read's `row_uuid`.**

**The split.** The two consumers have different needs, so they get different
fixes:

- **Existence gate (`hot_present`)** needs only a *boolean*, never rows — so it
  must carry **no predicate at all**: `hot_present = EXISTS(upsert) OR
  EXISTS(delete)`, no `commit_tx_log` join, no fence/as_of filter. This is a safe
  over-approximation: `hot_present` only decides whether `assemble_plan`
  attaches `hot_storage`, and `plan_hot_storage` re-applies fence + as_of, so a
  spurious `true` merely runs the hot read, which re-filters (pre-Persist
  already passes `true` unconditionally). It never false-negatives — a visible
  hot row IS a log row, so an empty log ⇒ no hot rows; the open tx's own writes
  are log rows, subsuming the old RYOW arm. The bare `EXISTS` short-circuits at
  the first row and is a ~0-page scan on the post-purge empty hot log.
  **Decided and shipped (CHA-472):** fence `max_exec` 28.7 ms → 0.177 ms, mean
  0.66 ms → 0.028 ms, buffer hits 1,724 → 6.2 — under the same loaded run.
- **Exclusion set** needs the actual shadowed `row_uuid`s, so it *cannot*
  collapse to a boolean. For a point read it must instead be **restricted to the
  read's `row_uuid` predicate** — a scoped exception to "never index-pruned"
  (§2) that is valid precisely because a point read names its rows, turning the
  table-wide scan into a seek. **Open → CHA-473.**

The principle generalizes §2: *selection, not filtering* applies to the **hot**
point-read path too. A point read should resolve its rows by identity on every
tier it touches; only a full scan (OLAP) may legitimately filter the unfiltered.

### 8. User secondary indexes: materialize-on-next-snapshot (CHA-483)

The internal `row_uuid` index (§5) is built for every segment from inception, so
it never has a partially-covered snapshot. A **user** `CREATE INDEX` does not:
when an index is defined after a table already has cold segments, the next
snapshot carries those unchanged segments forward by reference (ADR 0024), and a
carried segment has no sidecar for an index that did not exist when it was last
written. Two ways to close that gap were considered.

**Decision: build the missing sidecars at the next snapshot — including for
carried segments.** When the snapshot writer encounters a carried segment that a
live user index does not yet cover, it reads that segment's base file **once**
and builds the missing `(key…, row_offset)` sidecar for it; new/rewritten
segments build from the in-memory pack batch as usual. The base data files stay
carried-by-reference — **only a small sidecar is added, never a base rewrite** —
so this is bounded read-amplification at index-creation time (the normal cost of
`CREATE INDEX` in any database), not the full-segment write-amplification ADR
0024 exists to eliminate. Once built, a sidecar carries forward by reference with
its segment like any other.

The consequence is the clean one: a declared user index is **fully materialized
at the next snapshot**, so there is no "eventual" / partial-coverage state and
**no coverage signal is needed** — the seek (CHA-485) may treat a declared index
as complete. `DROP INDEX` stays lazy: the parent header is re-derived from
`index_metadata` each snapshot, so a dropped index is simply not re-declared next
cycle and its sidecars are reclaimed by GC.

**Rejected — eventual coverage (build only on rewrite).** Leaving carried
segments unindexed until they happen to be rewritten would make an index
partially materialized for an unbounded time, forcing a coverage signal on the
parent row and partial-coverage fallback logic into the seek — downstream
complexity bought to avoid a one-time, bounded read of already-cache-warm
segments. The materialize-on-next-snapshot read is the cheaper trade.

## Alternatives considered

- **Deliver point lookups via `output_ordering`** (CHA-410's original
  claim). Rejected: planner metadata, not an execution seek — see
  Context. This is the decision the ADR exists to record so it is not
  re-attempted.
- **One global per-snapshot index artifact.** Rejected: rebuilding the
  whole index every snapshot defeats carry-forward (ADR 0024). Per-segment
  trades a single O(log n) seek for N small probes in exchange for
  incremental maintenance, which wins given immutable carry-forward
  snapshots.
- **A `unique` flag / `CREATE UNIQUE INDEX` constraint.** Rejected:
  unenforceable for an index built after the write committed; the artifact
  handles duplicates natively and the PK is unique by construction.
- **Index definitions as columns on `table_metadata`** (mirroring
  `clustering_keys` / `partition_keys`). Rejected in favor of a separate
  auditable `index_metadata` store: independent `CreateIndex` / `DropIndex`
  with per-index history, and CreateIndex does not rewrite the table row.
- **A JSON `indexes` blob on the segment-metadata row.** Rejected: an
  index sidecar is a cold file that needs its own two-phase commit and
  must participate in the ref-counted GC sweep — a blob can carry neither.

## Consequences

- Cold PK / high-selectivity lookups go O(rows) → O(log n) per segment
  (≈ N · O(log(rows/N)) for a random key over N segments).
- Index maintenance is incremental with carry-forward, preserving ADR
  0024's write-amplification win for indexed tables; compaction rebuilds
  only compacted outputs.
- Correctness is untouched: the index only selects rows to decode (ADR
  0023); the exclusion set and the DataFusion residual still own the
  answer.
- A new metadata family (`index_metadata` + `segment_index_metadata`) and
  a DDL surface (`CreateIndex` / `Drop` / `Update` / `Get` / `List`, plus
  inline `CreateTable.indexes`).
- A `CREATE INDEX` is not usable until the next snapshot; `DROP` is lazy.
  The visible-index lag is bounded by the snapshot interval.
- Storage rises by one sidecar per `(segment, index)`, GC'd with its base
  segment under the grace window.

## Out of scope / open questions

- Secondary indexes on **persist** segments (snapshot PK + snapshot
  secondary first; persist gets a `row_uuid` index later).
- A hash structure (`INDEX_TYPE_SCALAR_HASH`) and a hash-partitioned
  on-disk layout — sorted (BTree) only for v1; defer hash until profiling
  on too-big-to-cache indexes justifies it.
- Covering indexes (serve `ORDER BY` / `MIN` / `MAX` / `BETWEEN` from the
  index alone).
- Whether the explicit partition concept becomes redundant once indexes
  exist (a whiteboard open question): partitions also drive the
  carry-forward rewrite unit (ADR 0024) and `DROP partition`, so removal
  is a separate design pass, not this ADR.

## Related

- CHA-455 — `index_metadata` + `segment_index_metadata` (definition +
  materialization).
- CHA-412 — the artifact build + system PK auto-index (shipped;
  built per-segment).
- CHA-454 — the hand-rolled cold PK seek (the execution this enables).
- CHA-410 — `output_ordering` (planner ordering, **not** the seek).
- CHA-442 — index-build memory bounding (resolved by the per-segment
  build; closed).
- CHA-398 — `ids` PK point-lookup API (the request-side consumer).
- CHA-472 — §7 existence-gate fix: `hot_present` as a loose
  `EXISTS(upsert) OR EXISTS(delete)` over-approximation (the boolean half).
- CHA-473 — §7 exclusion-set restriction: scope the point-read exclusion set
  to the read's `row_uuid` predicate (the rows half).
- CHA-411 — `SnapshotTableProvider` (the attach point).
- CHA-405 — ref-counted cold-segment GC (sidecars participate).
- CHA-252 — in-process snapshot-segment cache (sidecars cache-warm).
- CHA-339 — Lance native scalar indexes (format split).
- CHA-348 — selectivity / segment-size crossover (when a scan beats the
  index).
- ADR 0024 — incremental snapshot / carry-forward (why per-segment).
- ADR 0023 — single query-execution engine (why selection, not
  filtering).
- ADR 0022 — no persist-segment pruning (the index never prunes the
  exclusion set or persist).
- ADR 0012 — metadata as first-class tables (the definition store).
- ADR 0019 — universal grace window (sidecar GC).
- ADR 0015 — no foreign keys (the `index_uuid` reference is logical).
