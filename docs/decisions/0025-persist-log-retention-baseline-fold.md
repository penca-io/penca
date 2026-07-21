# ADR 0025: Persist-log retention — snapshot-as-floor

## Status

Proposed (CHA-425, 2026-06-11; **revised 2026-07-08** to snapshot-as-floor).
This revision **supersedes the original baseline-fold mechanism of this
same ADR**: the earliest retained *durable* snapshot is the history floor
*and* the read baseline, so there is no synthetic baseline to build and no
`table_retention_metadata` table. (The filename retains its original slug
for link stability; the ADR number is the stable identifier.) Amends the
"bound storage with snapshot retention (keep-last-N)" alternative in
[ADR 0024](0024-incremental-snapshot.md). Children: CHA-432 (durability
substrate + floor helper), CHA-55 (snapshot retention), CHA-434
(`PrunePersistSegments`), CHA-433 (plan-time floor enforcement), CHA-495
(`RetentionConfig` reshape — seconds units, drop `retain_max_versions`, add
`snapshot_density_seconds`; the chain root, blocks CHA-432). Related
read-path: CHA-457 (seq-aware snapshot picker).

## Context

The cold persist log is Penca's time-travel + audit substrate: `as_of`
reads and `audit_data` both resolve against it, and today it grows forever.
Snapshots are a **read-optimization cache** ([ADR 0024](0024-incremental-snapshot.md)):
a snapshot at watermark `W` is a *complete materialization* of resolved
state ≤ `W`. Snapshot retirement was decoupled and **disabled** (CHA-468)
to protect open-tx reads, so snapshots also grow forever. Two unbounded
stores; one mechanism should bound both.

The original form of this ADR proposed a **baseline fold**: a periodic op
that rebuilds merged state at a time horizon, registers it as a synthetic
low-watermark snapshot, and swaps out the sub-horizon log. This revision
observes we do not need to *build* a baseline — we already materialize them
continuously as snapshots. The earliest snapshot we choose to *retain* is a
valid floor. The load-bearing hazard is unchanged: pruning below a point
without a baseline at that point, and without failing reads below it,
silently corrupts time travel instead of bounding it.

## Decision

**Retention is a single durable-snapshot ladder. The newest `durable`
snapshot at or before the window start is the floor: persist below it is
deleted, and reads below it fail at plan time.** No synthetic baseline — the floor is a real,
retained snapshot.

### 1. `RetentionPolicy` = { duration, density }

`RetentionConfig` (proto, defined at catalog / schema / table and
coalesced) carries two knobs:

- `retention_duration_seconds` — the history/window span.
- `snapshot_density_seconds` — the spacing between durable ladder rungs.

Both are **seconds** — durations, not timestamps (matching Penca's
`MAX_TX_TIMEOUT_SECONDS` convention); the comparisons below convert once
against the micros timestamp axis (`× 1_000_000`).

The speculative, never-enforced `retain_max_versions` is removed (CHA-495).
Per-row version-count retention is out of scope; if ever wanted it returns
as a *real enforced* version-pruning compaction pass, not a stored-but-
ignored field.

### 2. Durable snapshots (the ladder rungs)

A `durable` boolean on `table_snapshot_metadata` marks a permanent rung.
It is assigned **once, at snapshot creation**: `durable = true` iff no prior
durable snapshot exists for `(branch, table)`, or `snapshotted_at_micros −
last_durable.snapshotted_at_micros ≥ snapshot_density_seconds × 1_000_000`.
It is sticky by
construction, which is what makes the floor **monotonic** and lets every
consumer read one flag rather than re-derive a rung set. Two independent
ops re-deriving the set could disagree — and a disagreement means the prune
deleting persist under a snapshot that retirement then sweeps, i.e. silent
data loss. The stored flag is the single source of truth
([ADR 0019](0019-plan-time-pinning-and-universal-grace-window.md)
§"Reading the watermark" discipline).

### 3. The floor

```
floor(branch, table) = commit_seq_num of the durable snapshot with
    MAX(snapshotted_at_micros)
    over table_snapshot_metadata
    WHERE durable AND snapshotted_at_micros ≤ now_micros − retention_duration_seconds × 1_000_000
```

— the **newest durable snapshot at or before the window start**. The helper
returns the floor snapshot's `(commit_seq_num, snapshotted_at_micros)` — both
coordinates, so §5 compares on whichever axis the `as_of` uses without a
micros↔seq mapping. Because its watermark is `≤ window_start`, the retained
window `[floor, now]` covers **at least** the full `retention_duration` —
retention never delivers *less* than configured, and no `as_of` inside the
window ever fails. The extra retained is ~one density window in steady state;
if snapshotting stalls the newest durable `≤ window_start` can be older, so
the upper bound is not fixed (only the "never less" direction is guaranteed). A null floor (no
durable precedes the window — the table is younger than `retention_duration`,
or retention is disabled) means retention no-ops: keep everything. The floor
read folds into the existing `hot_min` plan-time round trip as a scalar
subquery, so plan stays at its current query count. Consumers read this
value; they must **not** re-derive it from segment metadata (ADR 0019
§"Reading the watermark").

### 4. Persist prune — history depth (`PrunePersistSegments`)

A `PrunePersistSegments(table, branch)` lifecycle op deletes
`table_persist_segment_metadata` rows with `max_commit_seq_num < floor`,
drops now-childless `table_persist_metadata` parents **except** the one
carrying `MAX(persisted_at_micros)` (§6 anchor rule), and enqueues replaced
files through `segment_delete_set` + `sweep_segments` under the universal
grace window (ADR 0019 pillar 3 — the identical drain that already covers
compaction GC and CHA-405 ref-count GC). Segments **straddling** the floor
(`min_commit_seq_num < floor ≤ max_commit_seq_num`) are kept — their
sub-floor rows are unreachable (the plan floor rejects `as_of < floor`, §5),
so keeping them is harmless and no per-row splitting is needed.

This is a bounded `DELETE`, not a rebuild — no baseline build, no CHA-404
pipeline, no fold boundary arithmetic. Concurrency: a `persist_prune:{table}:
{branch}` advisory key; enumerate inside the transaction with `SELECT FOR
UPDATE` so it serializes against a concurrent compact; the floor is read
once at op start. Like Snapshot and Purge (ADR 0019), the prune must **not**
read `oldest_open_began_at` — its bound chains transitively from the
durable snapshot watermark.

### 5. Plan-time floor enforcement — correctness

Enforced at exactly two points:

- `QueryManager::plan` (`crates/penca-api/src/query/meta_plan.rs`):
  `as_of < floor` → gRPC **`FAILED_PRECONDITION`**, "as_of precedes
  retention horizon (floor=X)". An **error, not a clamp** — a clamped
  answer is data at a different instant than the user asked about, the
  exact silent wrong answer this ADR exists to prevent. The comparison is
  **on the axis the `as_of` arrives on**: a seq `as_of = N` is rejected iff
  `N < floor.commit_seq_num`; a micros `as_of = T` iff `T <
  floor.snapshotted_at_micros`. Both coordinates live on the floor snapshot
  row (§3), so there is no micros↔seq mapping and no dependency on the
  committed-at-monotonic-with-seq invariant. `as_of` exactly at the floor is
  **accepted** (the floor snapshot serves it) — the reject is a strict `<`.
- `QueryManager::plan_audit` (`crates/penca-api/src/query/mod.rs`): an
  **explicit** audit lower `from` below the floor (same per-axis rule) → the
  same error; an **unset** lower bound means "all retained history" and is
  clamped to the floor (inclusive of the floor snapshot's watermark), so
  open-ended audits keep working after retention engages. `plan()` and
  `plan_audit()` **share this exact boundary rule** — no divergence.

This is the load-bearing half: without it, the prune corrupts time travel
instead of bounding it. **Retention is not enabled in a deployment until
this check is live** — the two ship in either order; the disabled-by-default
config gate (`retention_duration` unset ⇒ null floor ⇒ prune no-ops)
enforces the ordering.

### 6. Persist-parent anchor rule (watermark-regression hazard)

A parent carrying the branch-table `MAX(persisted_at_micros)` can be left
with no live data segments (e.g. an aborts-only persist writes a
segment-less parent so the watermark advances). Deleting such a parent as
"childless" would regress `latest_committed_table_persist_watermark`,
dropping `plan()`'s `hot_min` below rows Purge already deleted from hot —
rows then served by **neither tier** (silent data loss). Rule: childless
parents delete freely **except** the row carrying the current
`MAX(persisted_at_micros)` — the persist recovery anchor and hot/cold cutoff
source. Deleting non-max childless parents never moves the MAX. (Carried
over unchanged from the original ADR — the hazard is identical whether the
sub-floor log leaves via a fold swap or a prune delete.)

### 7. Why a snapshot is a valid floor (exactness)

For any `as_of = P ≥ floor`: the plan picks the nearest snapshot ≤ P (the
floor at minimum) — a complete resolved baseline ≤ its watermark — plus the
raw persist delta `(W_snap, P]`, which is retained (≥ floor). Superseded
versions and applied tombstones below the floor are exactly what the floor
snapshot already folded into resolved form, so their deletion loses nothing
a retained read could observe. For `audit_data from ≥ floor`: the raw
persist `[floor, now]` is retained with audit columns intact. Everything
`< floor` is gone by construction and fails loudly (§5). Deleting persist
below the floor is safe **precisely because** the floor snapshot is a
complete materialization ≤ its watermark — the same guarantee the original
fold synthesized, obtained from a snapshot we already built.

### 8. Read-path: audit never reads a baseline

`audit_data` reads the raw persist log (+ hot), never a snapshot baseline —
snapshot baselines deliberately omit the audit columns (`commit_seq_num`,
`author`, `committed_at`, `comment`). So no audit-column baseline table is
needed. Time-travel `as_of` reads *do* compose over snapshot baselines; the
seq-aware picker (CHA-457) bounds them on the **existing** `commit_seq_num`
watermark column on `table_snapshot_metadata` (the CHA-443 `W_snap`) — no new
column; today's picker simply ignores it.

## Relationship to other ADRs

- **[ADR 0022](0022-no-persist-segment-pruning.md)** stays in force.
  Read-time persist pruning is per-query and filter-derived; the prune here
  is a global, filter-independent substrate delete governed by a stored
  floor that both merge queries see identically.
- **[ADR 0024](0024-incremental-snapshot.md)**: the keep-last-N
  bounded-storage alternative is superseded; snapshots gain a retention
  ladder (the `durable` rungs) rather than keep-latest-only.
- **[ADR 0019](0019-plan-time-pinning-and-universal-grace-window.md)**: the
  per-operation lock taxonomy gains `persist_prune:`; the delete rides the
  existing `segment_delete_set` + sweep grace; the no-open-tx-clamp rule
  extends to the prune.
- **[ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md)**:
  the auditable-store invariant becomes retention-bounded — every version
  *within the window* lives in at least one tier; below the floor, history
  is deliberately gone, and the audit floor (§5) makes it loud.

## Alternatives considered

- **Baseline fold (the original ADR 0025 design).** Build a synthetic
  baseline at an exact time horizon via the snapshot pipeline, swap the
  sub-horizon log in one commit-gated transaction. Rejected as
  over-engineered: it re-materializes state we already snapshot, and needs
  fold-boundary arithmetic, cross-wave recapture reasoning, compacted-input
  handling, and a `table_retention_metadata` substrate. Snapshot-as-floor
  reuses an existing snapshot as the baseline; the floor quantizes to
  snapshot-density spacing (retaining at most one density-window extra), an
  acceptable trade for collapsing the whole op to a `DELETE`.
- **Derive the rung set at prune time (no `durable` column).** A
  deterministic greedy walk over snapshot timestamps each run is stable, but
  two independent ops (prune, retire) reading a re-derived set risk
  divergence → deleting persist under a to-be-retired snapshot. A stored
  flag is the single source of truth (§2).
- **Byte-bounded rung spacing.** Size rungs by the persist bytes between
  them (bounding worst-case time-travel replay IO) rather than wall-clock.
  Deferred (v2, CHA-55) — YAGNI until profiling shows the seconds cadence is
  too coarse. Segments already carry `size_bytes` / `row_count` /
  `min,max_commit_seq_num`, so it is a no-schema follow-up.
- **Keep `retain_max_versions`.** Rejected — stored and resolved but never
  enforced; removed (CHA-495).
- **Snapshot keep-last-N as the storage bound** (ADR 0024's recorded
  alternative). Bounds the wrong tier: snapshots are a cache; the log is the
  history and grows regardless of N. Superseded.

## Consequences

- One `RetentionPolicy` (`{ duration, density }`) governs both snapshot
  storage and persist-log depth.
- The persist-retention op collapses from a fold + swap to a bounded delete.
- History depth = `[floor snapshot, now]` with the floor `≤ window_start`, so
  **at least** `retention_duration` is retained (unbounded above; steady-state
  slack ~one density window, larger if snapshotting stalls).
- Time travel and audit below the floor fail loudly (`FAILED_PRECONDITION`)
  instead of silently returning wrong point-in-time data; open-ended audits
  serve all retained history.
- Version-count retention is explicitly unsupported until built for real.
- Cross-branch file sharing remains branch-safe: persist/snapshot URIs embed
  `branch_uuid` and `create_branch` copies no segment metadata, so the
  prune's deletions are local; the shared `segment_delete_set` inherits
  CHA-405's refcount-zero-across-reference-classes generalization.

## Implementation tickets

1. **CHA-432 — durability substrate** — the `durable` column +
   assignment-at-creation, `snapshot_density_seconds` on `RetentionConfig`, the
   `retention_floor_seq` read helper (folded onto `hot_min`),
   `docs/schema-reference.md` entry. Blocks the other three.
2. **CHA-55 — snapshot retention** — retire non-durable snapshots; keep
   `{the floor durable and all newer durables} ∪ {latest} ∪ {open-tx-safe}`
   via the CHA-468 decoupled op + CHA-405 ref-count GC.
3. **CHA-434 — `PrunePersistSegments`** — delete persist below the floor +
   sweep; anchor rule (§6); `persist_prune:` lock; disabled-by-default
   config. Activation-gated on CHA-433.
4. **CHA-433 — plan-time floor enforcement** — `plan()` / `plan_audit()`
   checks riding the `hot_min` query; `FAILED_PRECONDITION` surface. Fires
   the Flight SQL driver-parity audit (error surface reaches ADBC and JDBC
   through different wire paths).
5. **CHA-495 — `RetentionConfig` reshape** — drop `retain_max_versions`,
   renumber `retention_duration_us` → `retention_duration_seconds` into
   field 1, add `snapshot_density_seconds` as field 2 (no `reserved` — we are
   pre-release, so field numbers are reshaped freely). One coordinated proto/
   column/plumbing edit; the chain root (blocks CHA-432).

## Related

- CHA-425 — the design ticket this ADR records.
- CHA-404 — packed streaming snapshot write (produces the durable baselines
  the floor reuses).
- CHA-405 — snapshot retirement + ref-counted cold-segment GC.
- CHA-468 — decoupled + disabled snapshot retirement (what CHA-55 re-enables).
- CHA-457 — seq-aware snapshot picker (read-path correctness over baselines).
