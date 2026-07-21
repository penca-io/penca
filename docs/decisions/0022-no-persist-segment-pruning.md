# ADR 0022: No persist segment pruning, even though persist segments carry stats

## Status

Accepted (CHA-82, 2026-05-28).

## Context

CHA-82 wires DataFusion `PruningPredicate`-based segment pruning for
cold-tier reads. The natural-seeming design prunes both cold tiers
symmetrically: snapshot segments and persist log segments alike. This
is unsafe for persist segments under the symmetric per-tier
merge-on-read algorithm (CHA-130 / `algorithms.md` §"Phase 3").

## The unsafe trace

Merge-on-read runs two independent queries against the same persist
log segments within one cold-tier DataFusion `SessionContext`:

> **Post-CHA-368:** the two-query (Query A / Query B) model this section
> describes was collapsed into one two-arm, `is_delete`-flagged resolve
> (`build_merge_resolved` / `build_cold_merge_resolved`); the exclusion set now
> falls out of the *unfiltered* resolve output and `build_exclusion_set` is gone
> — see [algorithms.md §"Two-arm resolve"](../algorithms.md#read-path) and
> [ADR 0023](0023-single-query-execution-engine.md). The no-prune **decision**
> below is unchanged: the exclusion set is still built over the full, unfiltered
> persist log.

- **Query A (resolve)**: latest committed upsert per `row_uuid`, with
  the user filter applied at the outermost `WHERE`.
- **Query B (exclusion set)**: distinct `row_uuid`s touched in any
  persist log since snapshot. The row_uuids in this set shadow the
  snapshot scan. Per CHA-142, Query B must be built **unfiltered**
  by the user predicate — a row whose current value doesn't match
  the filter but whose snapshot value does still needs its `row_uuid`
  in the exclusion set, or the stale snapshot row leaks through.

Consider:

- Snapshot `S2` holds row `r1` at `amount=500` (passes filter
  `WHERE amount > 400`).
- A post-snapshot persist log update sets `r1`'s amount to `50` (does
  not pass filter).
- Correct answer for `SELECT … WHERE amount > 400`: empty (r1's
  current value is 50).

Today (no persist pruning) the algorithm produces the correct result:

- Resolve persist → r1 at amount=50; outer `WHERE` drops it → empty.
- Exclusion set → `{r1}`.
- Snapshot scan → r1 read from S2, dropped by exclusion check → empty.

With persist segment pruning enabled (segment's amount min/max =
`[50, 50]`, doesn't match `> 400`, so pruned):

- Resolve persist → empty (segment never read).
- Exclusion set → `{}` (same session, same pruned segment list —
  segment also dropped from Query B).
- Snapshot scan → r1 read from S2, passes filter, not in exclusion
  set → emitted.
- Final: r1 at `amount=500` ❌ stale value leaks through.

The pruning is a coarser-grained version of the same filter CHA-142
forbade applying to Query B. Pruning a persist segment based on
user-filter-derived stats is semantically a filter on the
exclusion-set query.

## The snapshot asymmetry

Snapshot segment pruning is safe under the same algorithm because
snapshot holds the *possibly stale* baseline state and persist holds
the *current* state. Pruning a snapshot segment whose min/max can't
match the filter only drops rows whose snapshot value doesn't match.
If their current (persist) value does match, the unfiltered persist
resolve picks them up independently. If their current value doesn't
match either, the row is correctly absent. Persist is the opposite:
dropping a persist segment removes both the resolve signal *and* the
exclusion-set signal at once.

## Decision

Persist segments **carry** segment-statistics bytes (same writer-side
`compute_segment_statistics` helper as snapshot) but the reader
**does not** use them for filter-based segment pruning.

- `PersistTableProvider` does not implement
  `datafusion::common::pruning::PruningStatistics`.
- `PersistTableProvider::new` does not accept a `filters` parameter.
- `prune_segments_by_stats` is invoked at exactly one call site:
  `penca_merge::merge_read` Phase 3, for snapshot segments only.

Persist stats are still computed and stored for two reasons:

1. **`TableProvider::statistics()` aggregate** — DataFusion's CBO
   consumes table-level row counts and per-column min/max for
   cardinality estimation in join planning.
2. **Writer-side uniformity** — one `compute_segment_statistics`
   helper, called identically at all persist + snapshot writer sites.

## Alternatives considered

1. **Skip persist stats entirely** — drop the `statistics` column
   from `table_persist_segment_metadata`, remove the proto field,
   skip the writer-side compute on persist sites. Strictly cleaner if
   we believe no future use will emerge. Rejected on uniformity
   grounds + CBO cardinality use.
2. **Split sessions per query** — run Query A and Query B against
   distinct `SessionContext`s, with pruned and unpruned persist
   segment lists respectively. Mechanically correct but adds
   per-cold-read machinery for marginal benefit (persist logs are
   short-lived; snapshot is where bulk pruning pays off). Deferred
   indefinitely.
3. **Filter pushdown into persist format readers** (CHA-256 Parquet
   `RowFilter` / Lance `FilterExpression`) — same correctness failure
   at row granularity instead of segment granularity. CHA-256 must be
   snapshot-only on the persist tier.

## Consequences

- Queries with selective predicates whose data sits in cold persist
  logs (not yet snapshotted) scan all persist segments. In practice
  the persist log is small relative to snapshot, so the absolute IO
  cost stays modest.
- **CHA-256** (format-internal predicate pushdown) inherits the same
  asymmetry: `RowFilter` / `FilterExpression` integration applies
  only to snapshot reads. Persist reads stay unfiltered at the format
  layer.
- **CHA-143** (aggregate pushdown over merge-on-read) inherits the
  same asymmetry: if the snapshot tier short-circuits aggregates via
  segment stats, the persist tier must continue to use the full
  merge-on-read path for its half of the aggregate.
- **If we ever revisit**: a future merge-on-read refactor that
  separates the resolve and exclusion-set queries onto distinct
  segment lists (e.g., the exclusion set is precomputed and cached,
  or stored as a separate index) could unlock safe persist pruning.
  Until then, this ADR holds.

## Related

- CHA-82 — implements segment-level pruning for the snapshot tier;
  the ticket that surfaced this analysis.
- CHA-142 — established the "exclusion set must be built unfiltered"
  invariant that this ADR generalizes to segment granularity.
- CHA-130 — defined the symmetric per-tier merge-on-read algorithm
  under which the persist-pruning hazard exists.
- CHA-256 — format-internal predicate pushdown; constrained to
  snapshot only by this decision.
- CHA-143 — aggregate pushdown over merge-on-read; persist-side
  aggregates can't shortcut via segment stats.
