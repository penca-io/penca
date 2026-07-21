# ADR 0024: Incremental snapshot — immutable segments, delta-partition carry-forward

## Status

Proposed (CHA-384, 2026-06-10). Children: CHA-404 (packed streaming write), CHA-405 (reference-counted GC), CHA-406 (delta-partition carry-forward), CHA-407 (remove snapshot compaction).

The "keep full rewrite + bound storage with snapshot retention
(keep-last-N)" entry under "Alternatives considered" is amended by
[ADR 0025](0025-persist-log-retention-baseline-fold.md) (CHA-425):
bounded storage belongs to the persist log via a retention window,
not to snapshot keep-N policies.

## Context

`snapshot` materializes a read-optimized, point-in-time baseline of a table's cold data: it merge-reads the cold tier (latest-by-`row_uuid`, deletes resolved), sorts each partition by the table's clustering keys, and writes fresh cold segments. Today (`snapshot_op.rs::snapshot_locked`) this is a **full rewrite every cycle** — `collect_merge_read` pulls the *entire* table into one in-memory `RecordBatch`, then `build_segments_to_write` partitions and bin-packs it.

Two costs follow:

1. **Write amplification.** A table where only a few partitions change between snapshots still re-encodes every unchanged partition each cycle. For low-churn analytical tables (the common case) that is almost pure waste.
2. **Peak memory.** Materializing the whole table bounds snapshot memory by table size, not by partition size — a scaling ceiling and an OOM risk on large tables.

Compaction adds a third tension. `compact_snapshot_segments` exists to merge a snapshot's freshly-written small segments into larger ones. But snapshot already produces deduplicated, clustering-sorted segments, and once we carry segments forward across snapshot generations, "which files may compact together" becomes ill-defined (a snapshot is now a mix of generations with different `(offset,length)` slice addressing). The machinery's cost/benefit no longer holds.

## Decision

**A snapshot segment file is immutable, and a snapshot is built incrementally: rewrite only the partitions that changed since the prior snapshot, and carry unchanged partitions forward by reference.** Three mechanisms realize this.

### 1. Snapshot segments are immutable — no compaction (CHA-407)

Snapshot segment files are never rewritten after they are committed. `compact_snapshot_segments` is removed (CHA-407 deletes the machinery directly — snapshot segments become immutable): the `SnapshotScope` compaction impl, the op + gRPC handler + `CompactSnapshotSegments` proto RPC, the snapshot-only repoint/seal metadata fns, plus the now-dead `is_sealed` and `min_partition_value` / `max_partition_value` columns on `table_snapshot_segment_metadata` (no logic consumes the values — the compaction-eligibility query is the last functional reader; the plan-path SELECT, the `SnapshotSegment` proto fields, and the test ordering plumb them through and go with CHA-407 too. Per-segment `statistics` are the pruning input). `offset` / `length` stay — the packed write below addresses multiple segments inside one file with them, and writes them explicitly on every row (a single-segment file is the whole-file range, never NULL), so CHA-407 also makes the two columns `NOT NULL`. Per-catalog DDL only runs at CreateCatalog and pre-release there is no in-place migration path: catalogs predating these schema changes (whole-file NULL ranges, the dead compaction columns, the missing `table_snapshot_metadata` key columns) are recreated. **Persist-tier compaction (`PersistScope`) is unaffected** — persist segments are still mutable and still compact.

Immutability is the enabling invariant for everything below: a file that never changes can be safely shared by reference across snapshots, and can be addressed by stable `(segment_uuid, row_offset)` row positions (the cold secondary-index work, CHA-412, depends on exactly this).

### 2. Packed streaming write (CHA-404)

The snapshot write path merges the snapshot and compaction algorithms: partition row generation is decoupled from segment persistence exactly the way compaction's accumulate-and-flush works, so snapshot output is **born compacted** and CHA-407 loses nothing by deleting the compactor.

- **Resolve the delta once, globally** — the windowed cold merge (`build_cold_merge_resolved`) and the exclusion set (`build_cold_exclusion_set`), both O(delta). The exclusion set stays global/unfiltered (the CHA-142 invariant); that is what makes partition-moved rows correct under full rewrite: the stale copy is dropped wherever it lives, the new copy lands via the delta's own partition column. Group the resolved rows by partition label.
- **Stream the prior snapshot's segments in plan order** (`ORDER BY seg.chunk_idx` + the ordered `ByPlan` scan with bounded readahead). Segments are written label-sorted, so rows arrive as label-sorted partition runs. The exclusion set is applied per batch on this leg — an in-plan anti-join would build its hash table over the snapshot side, materializing it.
- **Merge-iterate** prior runs and delta groups in label order; per partition: combine, sort by the effective clustering keys, hand to the packer.
- **The packer accumulates whole partitions into an in-memory buffer** and flushes one segment FILE when the next partition would not fit `max_segment_bytes`. A **segment is a logical grouping of rows within a single file/uri** — multiple segments can map to the same uri: one metadata row per partition, addressed by `(offset, length)` row ranges, with `size_bytes` and `statistics` computed per partition slice so pruning stays partition-tight. A single partition over the cap still splits via `chunk_row_ranges` (the online packer).

Peak memory is bounded by `max_segment_bytes` + the largest single partition + the delta — decoupled from table size. This ships independently of carry-forward: it preserves full-rewrite semantics (every partition rewritten) and the two-phase durable-write + empty-merge watermark-commit invariants. The first snapshot of a table has delta = the whole table; accepted (new tables are small).

**Layout-key invariant: if `partition_keys` or `clustering_keys` change between snapshots, the next snapshot is a full rewrite of every segment** — carry-forward is only legal when the keys are unchanged. The write-time keys (clustering defaulting to primary keys when unset) are therefore recorded once on the snapshot parent row, `table_snapshot_metadata`, not per segment: every segment in a snapshot shares one key set by construction, and CHA-406's key-change detection reads the prior snapshot's stored keys.

### 3. Delta-partition carry-forward (CHA-406)

Between snapshots, only rewrite partitions that actually changed:

1. **Delta** = cold persist segments committed in `(prev_watermark+1, snap+1]`. **Cold-only** — snapshot must not read the hot tier; live OLTP must not leak into the baseline.
2. Merge-read the **delta only** (latest-by-`row_uuid`), not the whole prior snapshot (`snapshot baseline = None`).
3. Partition the delta rows. **The distinct partition labels present in the delta *are* the touched-partition set** — derived exactly from data, no statistics mining.
4. For each touched partition P: load the prior snapshot's segment(s) for P (label-exact — each packed segment row covers exactly one partition; locate via partition-column `statistics` or the per-partition rows directly), merge with the delta rows for P, write new **immutable** segment(s) via the CHA-404 packed writer.
5. Write metadata: new segment rows for rewritten partitions + **carried-forward** rows for untouched partitions — a new `table_snapshot_segment_uuid` under the new `table_snapshot_uuid`, pointing at the **same `object_uri`** as the prior snapshot's row.

**Granularity is partition-level**: a partition is wholly carried or wholly rewritten. This keeps every snapshot internally recency-consistent, so **the read path needs no change**. Segment-level / clustering-key-pruned carry-forward is a deferred refinement.

### 4. Reference-counted GC (CHA-405) — hard prerequisite

Carry-forward makes one physical file referenced by N snapshots (N metadata rows sharing one `object_uri`). Today `sweep_segments` deletes a cold file once its delete-set row ages past the grace window, with **no reference counting** — under carry-forward that would delete a file a younger snapshot still references. Before physically deleting an `object_uri`, confirm no live `table_snapshot_segment_metadata` row (any snapshot) references it; delete only at refcount zero. Snapshot retirement — keep the latest, plus the retention floor's baseline snapshot per [ADR 0025](0025-persist-log-retention-baseline-fold.md), which amends the keep-last-N framing here — enqueues a retired snapshot's files for GC, and the refcount check makes shared-file retirement safe. (This is the same pin shape as Iceberg-retention pinning in CHA-382, generalized to native snapshots.) **CHA-406 carry-forward must not go live before this lands.**

## Correctness frontiers

- **Partition-key mutation (gating).** A row whose partition column is updated moves partitions. The delta reveals only its *new* partition, so the stale copy in its *old* partition is never invalidated → duplicate row. Resolve before shipping carry-forward: either confirm partition columns are immutable under Penca's write-boundary contract, or attribute the old partition via a `row_uuid` → prior-partition lookup.
- **Delete attribution.** A tombstone can be placed in step 3 only if it carries the partition column. The cold delete-log carries PK columns; if partition-key ⊄ PK, deletes cannot be attributed. **v1 engages incremental only when partition-key ⊆ PK**, else falls back to full rewrite.
- **Empty-merge / watermark-commit invariant** (zero-row placeholder, parent committed last) is preserved unchanged.
- **Cold-only** read for the delta — preserved from today's snapshot.
- Unpartitioned tables get no carry-forward benefit (one partition); the CHA-404 memory win still applies.
- **Exclusion-set application is the read-amplification frontier.** Row_uuids are uniformly distributed, so per-segment stats prune ~nothing for the exclusion set; under carry-forward the rewrite pass would still read prior segments to find excluded rows. The row_uuid index (CHA-385) inverts this: look up exactly which prior segments contain excluded row_uuids and stream only those — the untouched remainder carries forward without ever being read.

## Alternatives considered

- **Keep full rewrite + bound storage with snapshot retention (keep-last-N).** This is the *current* model and what CHA-382 / CHA-383 (Iceberg export / adopt-in-place) assume. Simple and correct, but pays full write amplification every cycle. Incremental snapshot is the optimization layered on top; the full-rewrite path remains the fallback (and the v1 path whenever partition-key ⊄ PK). The bound-storage half of this framing is superseded by [ADR 0025](0025-persist-log-retention-baseline-fold.md) (CHA-425): snapshots are a read-optimization cache (this ADR's CHA-405 retires all but the latest), so bounded storage is governed by a persist-log retention window — the fold baseline registered as a snapshot at the retention horizon — not by snapshot keep-N policies.
- **Keep compaction across snapshot generations.** Rejected: with carried-forward segments a snapshot is a mix of generations, so cross-generation compaction eligibility and `(offset,length)` re-addressing become a materially harder cost model for no clear benefit over immutable + bin-packed-on-write segments.
- **Segment-level (not partition-level) carry-forward in v1.** Deferred: partition granularity keeps each snapshot internally recency-consistent and leaves the read path untouched; finer granularity would push recency reasoning into the reader.

## Consequences

- Snapshot write amplification drops to ~the changed-partition fraction for low-churn tables; unchanged data is referenced, never re-encoded.
- Snapshot peak memory is bounded by `max_segment_bytes` + the largest single partition + the delta (CHA-404), decoupled from table size (first snapshot: delta = the whole table, accepted).
- `compact_snapshot_segments` and its proto RPC are removed (cross-language stub regeneration; confirm no external consumers). Persist compaction is untouched.
- Cold-segment GC becomes reference-counted; naive age-out deletion is replaced by refcount-zero deletion.
- Immutable, stably-addressed snapshot segments are the substrate the cold secondary-index / auto-PK-index work (CHA-412, CHA-410) builds on: `(segment_uuid, row_offset)` is a stable row address only because the file never changes.
- Side benefit: the Iceberg adopt-in-place epic (CHA-383) can approach zero-copy *forever* — adopted files that never change are never re-encoded — though that is not the reason to build this.

## Related

- CHA-384 — the spike this ADR records.
- CHA-404 — packed streaming write (memory bound; ships first).
- CHA-405 — reference-counted cold-segment GC (hard prereq for carry-forward).
- CHA-406 — delta-partition carry-forward (the headline optimization).
- CHA-407 — remove snapshot-compaction machinery.
- CHA-54 — `compact_snapshot_segments` (the machinery being removed).
- CHA-410 / CHA-412 — clustering-order + secondary indexes that rely on immutable, stably-addressed snapshot segments.
- CHA-382 / CHA-383 — Iceberg export / adopt-in-place; same reference-pin retention shape.
