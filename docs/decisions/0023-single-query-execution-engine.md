# ADR 0023: A single query-execution engine — DataFusion owns filter and projection

## Status

Accepted (CHA-369, 2026-06-01). First concrete cut of a principle that
also governs CHA-368 (Postgres tier — implemented 2026-07-13) and
CHA-370 (merged sources as relations).

## Context

A Penca read is a merge-on-read that spans storage tiers and formats:
the hot tier (Postgres), the cold persist log, and cold snapshot
segments (parquet or Lance). Historically, filtering was pushed into
more than one engine:

- The cold snapshot format readers took a filter `Expr` for
  format-internal predicate pushdown (CHA-256): the parquet reader
  lowered it to a `RowFilter`; the Lance reader passed a
  `FilterExpression` (a no-op in lance-file 4.0.0).
- The Postgres hot tier pushes a translated `WHERE` into SQL.

Each of these is a *second* executor of the user predicate, running
alongside the one DataFusion runs in the merge-on-read layer. CHA-353
made the merge-on-read residual a full-DataFusion-planned predicate
applied in-process — the exact, correctness-critical row filter. That
made the format-internal pushdown pure redundancy: a best-effort IO
optimization layered on top of the authoritative filter.

It was also a latent bug source. The parquet `build_row_filter` lowered
the *raw, uncoerced* `Expr` via `create_physical_expr`; a cross-type
predicate (e.g. an `Int32` column compared to an `Int64` literal) built
fine but errored at row-group eval, and the build-time keep-all fallback
caught only build failures, not eval failures — so the read aborted. It
went unnoticed because Lance is the default format.

## Decision

**DataFusion is the one place filter, projection, and (ultimately)
relational behavior is defined.** No filter pushdown is delegated to a
second engine — not to the format readers (parquet `RowFilter` /
Lance), not to Postgres (the hot tier). Each storage tier returns
exactly the columns DataFusion computes it needs
(`output ∪ filter ∪ group-by ∪ join-keys`); DataFusion does the
filtering and reduction.

CHA-369 applies this to the snapshot format readers:
`FormatReader::read_segment` no longer takes a filter; the parquet
`RowFilter` lowering site is deleted; the CHA-353 residual is the sole
snapshot-tier row filter. Segment-level pruning
(`prune_segments_by_stats`, ADR 0022) is unaffected — it operates on
segment min/max stats, not row-level predicate evaluation, and remains
a safe coarse-grained skip.

### Reasoning — why a second executor is forbidden

Because a merge read spans tiers and formats, any filter executed by a
*different* engine must be provably equivalent to DataFusion's. But
cross-engine SQL-semantics equivalence is **not provable** — type
coercion, null handling, collation, and operator semantics differ
between arrow-rs row filters, Postgres, and DataFusion, and equivalence
can only be *tested* over a finite input space, never proven over all
inputs. Eliminating the second executor removes the equivalence
obligation entirely rather than managing it. The parquet cross-type
abort (CHA-369) is exactly the class of bug this removes: a second
executor that disagreed with DataFusion on `Int32 == Int64`.

### Projection is part of the same principle, not just filtering

The column set a reader returns must be the union of output columns and
every column referenced by an operator above the scan — filter,
group-by, join keys — exactly what DataFusion's projection-pushdown
already computes for a `TableProvider` scan. The **correctness trap** to
name explicitly: once filtering moves out of the reader, the read
projection **must** include the filter (and group-by / join) columns, or
the in-process predicate references a column that was never read.

Current state, for the record:

- The snapshot **cacheable** path over-reads the *full* schema
  (CHA-252 — one cache entry serves any projection), so
  projection-narrowing is moot there today: it reads every column even
  for `SELECT name`. The residual therefore always has its filter
  columns in hand.
- The persist-log `PersistTableProvider` already relies on DataFusion's
  projection-pushdown to include filter columns under its
  `Unsupported`-filter scan.

The principle became load-bearing when the Postgres tier narrowed its
read: CHA-368 (implemented 2026-07-13) projects
`output ∪ filter ∪ group-by ∪ join-keys` and evaluates **no** user
`WHERE`. The per-tier merge resolves run unfiltered and return a two-arm
`is_delete`-flagged delta (visible upserts + winning tombstones), so the
exclusion set is derived from the *unfiltered* resolve output — the two
Query-B exclusion probes are retired — and the user predicate is applied
once as the shared DataFusion residual across the hot, cold-log, and
snapshot tiers. See the Consequences entry.

## Alternatives considered

**A constrained predicate AST with dual translators.** Define a
restricted predicate language and maintain one translator per engine
(DataFusion, parquet `RowFilter`, Postgres SQL), kept equivalent by a
shared conformance test suite. Rejected: the equivalence the design
hinges on is unprovable and the test space is finite, so the suite can
only ever sample the input space; the latent cross-type abort
(CHA-369) is precisely a corner the existing tests missed. The
maintenance cost of N translators plus a never-complete conformance
suite outweighs the IO-pruning benefit on a secondary tier (parquet;
Lance already no-ops). Removing the second executor is strictly simpler
and strictly safer.

## Consequences

- The parquet cross-type eval-abort bug is gone by construction — there
  is no in-reader predicate to mis-evaluate.
- One `Expr`-lowering site is deleted (`build_row_filter` /
  `create_physical_expr`); the readers are pure projected scans.
- Parquet loses row-group / page skipping, so it reads more bytes from
  the file and filters in-process. Acceptable: parquet is the secondary
  format, and Lance (the default) already worked this way. The parquet
  page index is no longer loaded by the reader.
- Lance loses nothing (its `FilterExpression` was already a no-op).
- CHA-339 (Lance filter-aware decoders) is superseded in spirit: any
  future filter-aware decoding plugs in through DataFusion, not a
  reader parameter.
- CHA-368 (implemented 2026-07-13) extends the cut to the Postgres hot
  tier: the merge resolves no longer splice the user `WHERE`, so the
  unprovable Postgres≡DataFusion filter-equivalence obligation is
  eliminated. Each tier's resolve returns an unfiltered, `is_delete`-
  flagged two-arm delta; the exclusion set is the full `row_uuid` set of
  the composed resolve (derived before any filtering — CHA-142), and the
  user predicate is applied once as the `full_plan_predicate` residual the
  snapshot tier already used. The all-hot fast path reads the unfiltered
  resolve, drops tombstones in PG (`WHERE NOT is_delete`), and residual-
  filters per batch; its `COUNT(*)` push-down survives only for the
  no-filter case. The invariant that the read projection includes every
  filter column (else the residual fails fast) is guarded, not assumed.

## Related

- CHA-369 — format readers (this ADR's concrete cut).
- CHA-368 — single filter-execution engine for the Postgres tier
  (filter **and** projection: project `output ∪ filter ∪ group-by ∪
  join-keys`, filter in DataFusion). Implemented 2026-07-13 — see the
  Consequences entry.
- CHA-370 — expose merge-on-read as DataFusion table sources so
  relational logic (joins, reductions) runs in one plan.
- CHA-256 — introduced the format-internal pushdown (now removed).
- CHA-353 — the full-plan-once residual filter this principle leans on.
- CHA-252 — the snapshot full-decode cache that side-steps
  projection-narrowing today.
- ADR 0022 — segment-level pruning stays (coarse stats skip), distinct
  from row-level predicate execution.
