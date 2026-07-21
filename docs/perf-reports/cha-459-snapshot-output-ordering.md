2026-06-20 — CHA-462: snapshot `output_ordering` — is the redundant `SortExec` on a hot path? (CHA-459 fork-3 gate)

# Snapshot `output_ordering`: SortExec hot-path measurement

> **Dated one-time investigation record.** Per the CHA-423 convention,
> absolute numbers are not maintained in docs — this is a snapshot of one
> measurement campaign.
>
> Host: `fabricdb-dev-3.exe.xyz` (shared VM). Based on `main` @ `f7e56330`.
> **This PR ships only this finding.** The measurement apparatus (a
> plan-shape capture + a floor-harness timing bench) was investigative and is
> **not committed** — it tested pre-change / upstream behavior and benched a
> win this finding recommends deferring, so it would be dead weight. It is
> fully described under [Method to reproduce](#method-to-reproduce) so
> CHA-459 Round 2 can rebuild it against the real new behavior.

## The question (CHA-459 fork-3)

CHA-459 would advertise a snapshot `output_ordering` so DataFusion can elide
a redundant `SortExec`. The gate, before paying for the writer fix +
advertisement: **does a redundant `SortExec` actually sit on a hot path for
cold snapshot reads — i.e. would the advertisement elide a sort that
matters, or is the win latent?**

## Finding 1 — the `SortExec` exists, but only for an *explicit* `ORDER BY`

Physical plans over a single-segment `SnapshotTableProvider` scan
(`ByCompletion`, the production query session config), via
`create_physical_plan` + `displayable`:

```
-- user ORDER BY on the clustering key (the elision opportunity):
SELECT l.row_uuid, l."value" FROM l ORDER BY l."value"
  SortExec: expr=[value@1 ASC NULLS LAST], preserve_partitioning=[false]
    StreamingTableExec: partition_sizes=1, projection=[row_uuid, value]

-- the order-free production merge-read snapshot leg:
SELECT l.row_uuid FROM l
  StreamingTableExec: partition_sizes=1, projection=[row_uuid]
```

A `SortExec` is planned **only** when the query carries an explicit
`ORDER BY <clustering_key>`. An advertised `output_ordering` would elide
exactly that node.

## Finding 2 — the production read path has no such `SortExec` (no live consumer)

The advertisement has **no production hot-path consumer today**:

- **The production merge-read snapshot leg is order-free.** It resolves by
  `row_uuid` and carries no `ORDER BY` (`build_cold_snapshot_scan` /
  `build_cold_snapshot_scan_plain` in `penca-merge`), so no `SortExec` is
  ever planned over it — nothing for the advertisement to elide.
- **The carry-forward (CHA-406) does not consume `output_ordering`.** Its
  ordered (`ByPlan`) path uses a *plain* SQL scan + a hand-rolled
  `PartitionMerger` (`crates/penca-api/src/lifecycle/packer.rs`) +
  `ORDER BY seg.chunk_idx` at the metadata level — not a DataFusion
  `SortPreservingMerge` over an advertised order.
- There is **zero** `SortPreservingMerge` / order-aware operator anywhere in
  the tree (`rg SortPreservingMerge` → none).
- The merged persist tier (CHA-410) already advertises its order and its own
  comment records it as **"latent today — no consumer."**

## Finding 3 — when it *does* fire, the cost is modest

A throwaway floor-harness bench (the CHA-415 `scan_snapshot` path, 100k-row
in-memory cold base, median of 10 Criterion samples) timed the order-free
`SCAN_SQL` against `ORDER BY value`:

| scenario | shape | time (median) |
| -- | -- | -- |
| `plain` | order-free `SCAN_SQL` (no `SortExec`) | ~1.33 ms (noisy: [1.13, 1.58]) |
| `ordered` | `ORDER BY value` (`SortExec`) | ~1.60 ms (tight: [1.60, 1.61]) |

The `SortExec` adds **~0.27 ms (~20%)** over a ~1.33 ms in-memory scan at
100k rows. (Host-dependent and not committed — CHA-423. The cost is
`O(n log n)` so it grows with row count, but only on the explicit-`ORDER BY`
path; against real object-store IO it would be proportionally smaller still.)

## Conclusion (fork-3)

**The redundant `SortExec` is NOT on the production hot path.** It appears
only for a *synthetic* user `SELECT … ORDER BY <clustering_key>` against a
cold table — where it is a modest ~20% overhead — and the production
merge-read path that dominates cold OLTP/OLAP plans no such sort. With no
live consumer (carry-forward is hand-rolled; no `SortPreservingMerge`
exists), advertising a snapshot `output_ordering` today would be **latent**,
exactly as the persist tier (CHA-410) already is.

## Recommendation (fork-1) for CHA-459 Round 2

1. **Land Part A (the typed-partition-order writer fix) on its own.** It is a
   latent **correctness** bug (the writer emits partitions in
   stringified-label order, wrong for non-string keys) and the honest
   prerequisite for *any* future multi-partition advertisement. It is
   valuable independent of the advertisement and carries no perf risk.
2. **Defer Parts B/C (the `output_ordering` advertisement).** There is no
   consumer to elide a sort for, and the synthetic-only cost is small. Revisit
   when a real order-aware consumer materializes — the CHA-406-v2
   `SortPreservingMerge` carry-forward, or measured evidence that user
   `ORDER BY <clustering_key>` cold queries are a hot path.
3. **If/when advertising, Option C (N contiguous ordered partitions +
   `SortPreservingMergeExec`) is the end-state** (per the CHA-459 design
   note) — but re-measure against the actual consumer then; do not adopt the
   `target_partitions=1` pin (Option A/B) speculatively.

## Method (to reproduce)

Both apparatus pieces were investigative and are not committed; Round 2
should rebuild them against the *new* behavior (the typed-order writer + the
advertisement), where they test Penca-owned logic rather than the
pre-change / upstream baseline below.

- **Plan shape.** In a `#[cfg(test)] mod` in `crates/penca-dl/src/provider.rs`
  (alongside `by_plan_order_tests`, which has the `pub(crate)` access):
  register a single-segment `SnapshotTableProvider` as `l` via
  `build_snapshot_session(…, SegmentOrder::ByCompletion)`, then
  `ctx.sql(sql).create_physical_plan()` rendered with
  `datafusion::physical_plan::displayable(plan).indent(true)`. Compare
  `SELECT l.row_uuid, l."value" FROM l ORDER BY l."value"` (SortExec present)
  against `SELECT l.row_uuid FROM l` (none).
- **Timing.** Extend the CHA-415 floor harness
  (`crates/penca-merge/benches/floor_support.rs`) with an
  `ORDER BY <clustering_key>` SQL and a Criterion bench over
  `driver_for(base_batch(100_000))` / `scan_snapshot`, comparing it to the
  order-free `SCAN_SQL`; `PERF_FLOOR_MAX=1m` for the 1M base.

## Caveats

- Single-segment, 100k, in-memory base — isolates DataFusion compute, not
  object-store/decode. The `plain` baseline is noisy ([1.13, 1.58] ms); the
  ~20% delta is the signal, not the absolute.
- Measures the planner/exec `SortExec` cost only — independent of CHA-454
  (the hand-rolled index seek) and of segment-read IO.
