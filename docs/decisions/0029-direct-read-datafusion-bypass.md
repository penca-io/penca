# ADR 0029 — Direct read fast-path: bypass DataFusion for default snapshot-only point reads

## Status

Accepted (2026-06-25, CHA-476). Extends [ADR 0023](0023-single-query-execution-engine.md)
(single filter engine — selection, not filtering) and the cold-index design in
[ADR 0026](0026-cold-tier-index.md). The DataFusion-free point-read kernel it
reuses is CHA-454; the format-agnostic sidecar build it relies on is ADR 0026 §6.

Amended (CHA-482): the bypass is **lifted out of `read_data`'s inline dispatch
into a shared `stream_cold_read` helper** (`crates/penca-api/src/query/cold_read.rs`),
callable by both `read_data` and — once CHA-484 lands — the by-name metadata
resolves, so the metadata fast-path takes the same DataFusion-free arm instead of
calling `stream_merged` directly. The gate is now expressed over an internal
`MergeReadRequest.seeks` param (a single index-seek entry; the `ids` restriction
is subsumed as the identity `row_uuid` entry), and the seek generalizes from the
single-column `row_uuid` key to **composite index keys** (CHA-480) so a unique
name key like `(schema_uuid, table_name)` is bypass-eligible. The
selection-not-filtering invariant and the `filter.is_none()` gate are unchanged.

## Context

`read_data` dispatches on plan shape: `is_all_hot` → `stream_all_hot` (already
DataFusion-free); `is_all_cold` → `stream_all_cold`; mixed → `stream_merged`. A
small point read on a fully-cold, snapshot-only table (the OLTP steady state once
the hot tier has drained and the persist band has been purged into the snapshot)
still routes through `stream_all_cold`, which builds a `DatafusionDlDriver` +
`MergeReadRequest` and runs a DataFusion scan. Profiling under CHA-472 / CHA-473
measured ~1.37 ms of fixed DataFusion plan-construction + execution overhead for
such a read — pure overhead for an operation that evaluates no predicate, merges
no tier, and prunes no rows.

DataFusion is *load-bearing* for this read only if it provides an operator the
direct path cannot: **predicate evaluation**, **multi-tier merge**, or
**visibility pruning**. None of those depend on where the bytes live, so data
**residency is not a gate** — it only selects the physical read strategy.

## Decision

`read_data` gains a fourth dispatch arm, ahead of the `is_all_cold` branch, that
serves the read **without DataFusion** when **all** of the following hold (a
purely query-shape gate):

1. **Default current-time read** — `default_frontier.is_some()` (no explicit
   `as_of` / `commit_seq` / open-tx). Avoids the within-snapshot visibility prune
   a time-traveled read needs.
2. **Snapshot-only** — `is_snapshot_only(&plan)`: `is_all_cold` with no persist
   band, so there is nothing in the hot or persist tiers to merge and the
   exclusion set is empty.
3. **Point read** — `request.ids` is present (the CHA-398 identity probe set).
   Full scans (no `ids`) stay on `stream_all_cold`: the fixed DataFusion overhead
   this targets only dominates a small point read.
4. **No value predicate** — `request.filter` is `None`. Any value predicate is
   real predicate evaluation and stays on `stream_all_cold`.

The result is the **same row set** as `stream_all_cold` for this plan shape: with
no hot/persist tier the exclusion set is empty, and a `row_uuid` lookup is the
exact identity selection, so the seeked rows are exactly what the
`stream_all_cold` `row_uuid IN (…)` residual would return. Row *order* is
unspecified in both paths — `build_cold_snapshot_scan` carries no `ORDER BY` and
`SnapshotTableProvider` advertises no `output_ordering` (CHA-459), so the
equivalence is over the row set (a multiset, deduped to a set because snapshot
segments are disjoint by `row_uuid`), not positional.

### Why this does not erode the single filter engine (ADR 0023 / CHA-368/369)

The bypass sits on the *sanctioned* side of the selection/filtering line:

- `row_uuid` selection is set-membership over a precomputed identity set — the
  same non-predicate operation whether served by an index seek or a membership
  scan. **Selection, not filtering.** Reusing CHA-454's merged kernel
  *strengthens* this carve-out rather than opening a second filter path.
- Projection is column pruning — it changes the output *schema*, never *which
  rows*. Already done outside DataFusion. Not filtering.
- Any value predicate is real predicate evaluation → it stays on
  `stream_all_cold`, so evaluation is single-sourced.

> **Invariant.** The direct read arm may serve a default-current-time,
> snapshot-only, **point** read without DataFusion **iff** it evaluates no value
> predicate (`request.filter` is `None`); `row_uuid` selection (index seek, or a
> membership scan over a cached batch) and column projection are non-predicate
> operations and stay DataFusion-free **independent of cache residency**, while
> any value predicate, any time-travel, any hot/persist band, or a full-scan
> (no `ids`) read stays on `stream_all_cold` so predicate evaluation and
> visibility bounds stay single-sourced.

### Residency is a tier-selector, not a gate

Given the gate, serving the read directly beats `stream_all_cold` at every
residency level — both pay the same underlying I/O and the direct path simply
strips DataFusion's fixed overhead. Residency only picks *how* the bytes are
read, all DataFusion-free, all inside CHA-454's reused
`read_seeked_snapshot_segment` kernel:

- **Cached base + sidecar** → binary-search the `row_uuid` sidecar
  (`penca_format::index::seek_row_offsets`) and `arrow::compute::take` the
  matched rows — zero I/O.
- **Evicted sidecar** → re-GET the (small) sidecar, then `take`.
- **Uncached base** → decode the segment and `take`, populating the cache the
  way `stream_all_cold` does. True selective row-group decode (fetch only the
  matched rows' byte ranges) is the follow-up CHA-469; it is **out of scope**
  here — this reuses the merged kernel as-is rather than reimplementing it.

The sidecar is built **format-agnostically** (Parquet and Lance, following the
table's `storage_format`, atomically under the snapshot op's two-phase gate) per
**ADR 0026 §6** — this ADR references that build, it does not restate or correct
it.

A segment whose snapshot never materialized a `row_uuid` index makes the kernel
report no usable sidecar; the arm then **falls back to `stream_all_cold`**. The
fallback is a hard-resolution condition (the index does not exist), **never** a
residency state.

## Consequences

- A snapshot-only OLTP point read drops the ~1.37 ms DataFusion plan/execute
  overhead, served by an index-driven selective read.
- One new code path on the read side, gated narrowly. The SQL/Flight surface is
  unaffected: SQL `SELECT … WHERE pk = X` does not populate `ReadDataRequest.ids`,
  so it never reaches this arm; only the gRPC `ids` API does, and its result is
  the same row set as the DataFusion path either way.
- The fallback keeps correctness total: any read the arm cannot serve (including
  any future plan shape it does not recognize) reverts to `stream_all_cold`.

## Out of scope

- **Selective row-group decode on cache miss** — CHA-469 (the kernel currently
  full-decodes the base then `take`s; this arm reuses that behavior).
- **Base + sidecar cache co-residency** — CHA-477. Until it lands, an evicted
  sidecar over a cached base re-GETs the sidecar; co-residency makes
  base-resident imply sidecar-resident by construction.
- **Resolution / proto surface** — CHA-475. This arm sits at the dispatch, not
  the identifier-resolution path.

## Related

- [ADR 0023](0023-single-query-execution-engine.md) — single filter engine; selection vs
  filtering (extended here).
- [ADR 0026](0026-cold-tier-index.md) — cold-tier index; §6 is the
  format-agnostic sidecar build this arm relies on.
- CHA-454 — the hand-rolled cold `row_uuid` seek kernel reused here.
- CHA-441 — the hot existence-gate / `is_snapshot_only` plan shape this gates on.
- CHA-398 — the `ids` identity point-lookup API.
- CHA-473 — the point-read cost analysis this fast-path realizes.
- CHA-469 — selective row-group decode (deferred).
- CHA-477 — base + sidecar cache co-residency (deferred).
