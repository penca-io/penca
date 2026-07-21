# 0001 — Unified `upsert_log` for data auditable stores

- **Status:** Accepted
- **Date:** 2026-04-19
- **Ticket:** [CHA-134](https://linear.app/chapala/issue/CHA-134)
- **Supersedes:** [CHA-118](https://linear.app/chapala/issue/CHA-118) (per-table split); obsoletes [CHA-133](https://linear.app/chapala/issue/CHA-133) (branch-merge classification rework)

## Context

The physical storage layout for a user data auditable store has flip-flopped
once already:

1. **Pre-CHA-118** (before 2026-04-15) — each physical data table had a single
   `{physical}_data_upsert_log`. Writes of new and existing rows landed in the
   same table. Reads grouped rows by `row_uuid` and picked the latest
   committed version.
2. **CHA-118** (commit `a3b4e42`, 2026-04-15) — split every auditable store
   into separate `insert_log` and `update_log` tables. The stated motivation
   was to enable the CHA-112 exclusion-set optimization: cold-tier reads
   could skip rows whose `row_uuid` only appears in `insert_log` (those rows
   cannot shadow anything in the snapshot, since the snapshot doesn't contain
   them). Read paths paid the cost of a `UNION ALL` across the two logs; the
   write path asked the client to classify each row as an insert or an update
   before calling `MutateData`.
3. **CHA-134** (this decision, 2026-04-19) — collapse back to a single
   `upsert_log` per physical data table. The `UNION ALL` folds away, the
   anti-join disappears from the exclusion set, and clients send one
   `Change(upserts=batch)` payload regardless of whether the rows are new or
   overwrite existing ones. Table-metadata auditable stores
   (`table_metadata_insert_log` / `table_metadata_update_log`) were
   initially left split — out of scope for this decision — and tracked
   separately under [CHA-149](https://linear.app/chapala/issue/CHA-149),
   which has since landed and collapsed them into a single
   `table_metadata_upsert_log` on the same rationale.

CHA-134 is not a revert of CHA-118. The cost/benefit that motivated the
split looks different today:

- **Client ergonomics were underweighted.** Under the split, a client that
  wanted to write a mixed batch had to round-trip the table first to
  discover which `row_uuid`s already existed, then partition its batch into
  `inserts=` vs `updates=`. Postgres's `INSERT ... ON CONFLICT DO UPDATE`
  exists for exactly this reason, and the equivalent reason applies here.
- **The client-side classification was already a lie at branch-merge time.**
  A row that is an insert on `source` may collide with a row that already
  exists on `target`; the classification is branch-local. The branch-merge
  algorithm had to re-classify anyway (see CHA-133, now obsolete).
- **The exclusion-set win turned out to be marginal.** The anti-join did
  strictly reduce the size of the in-memory `HashSet<row_uuid>` in Phase 2
  of merge-on-read, but pure-insert `row_uuid`s never match against the
  snapshot segments scanned in Phase 3 — they just occupy a hash slot and
  get probed once. At the volumes we read today, the hash lookup cost is
  dwarfed by the snapshot segment scan itself.
- **There was no write-time classification scan.** The server trusted the
  client's `Change.inserts` vs `Change.updates` routing and wrote to
  whichever log table it was told. No server-side `SELECT` ever checked
  whether an "insert" was actually new. Strict-INSERT semantics (fail on
  PK collision) was never implemented under the split and is now a
  separate, opt-in feature under [CHA-121](https://linear.app/chapala/issue/CHA-121).

## Decision

Collapse `{physical}_data_insert_log` and `{physical}_data_update_log` into
a single `{physical}_data_upsert_log` per physical data table. The
`{physical}_data_delete_log` and the per-catalog `commit_tx_log` (post-CHA-163;
was per-schema when this ADR was written) are unchanged.

Proto surface:

- `Change.inserts` + `Change.updates` → `Change.upserts` (single `bytes` field).
- `MutateDataResponse.inserted_row_uuids` + `.updated_row_uuids` →
  `.upserted_row_uuids`.
- `AuditDataResponse` gains a second field — `bytes upserts = 1; bytes deletes = 2;`
  — replacing the prior single `bytes data = 1;`. The audit trail now surfaces
  tombstones, which were previously invisible. `client.audit_data()` returns
  `tuple[pa.Table, pa.Table]`.

Read-path SQL (`build_merge_resolved` / `build_exclusion_set` in both Python
and Rust):

- Query A (resolved upserts): the `upserts` CTE now reads from a single
  `upsert_log` table. The `UNION ALL` is gone.
- Query B (exclusion set): drops the `LEFT ANTI JOIN insert_log` clause. The
  exclusion set grows slightly (pure-insert `row_uuid`s are now included),
  but each new entry is a single hash probe that never matches in the
  snapshot scan — negligible at the volumes we care about.

Branch-merge compaction (`merge_table_data`): drops the
`source_inserted` CTE and the two-route routing between
`target.insert_log` and `target.update_log`. A single
`INSERT INTO target.upsert_log SELECT ... FROM source.upsert_log JOIN
source.commit_tx_log ...` handles the alive-row route; the delete route no
longer needs to exclude source-inserted rows.

## Rationale

The headline win is client UX, not internal cleanup. Every data write
becomes `Change(upserts=batch)` with no prior scan and no bookkeeping.
This unlocks bulk-load patterns (`COPY`-shaped workflows, streaming
ingestion from Kafka) that required a read round-trip under the split.

Secondary: schema simplification. Each physical data table now has three
log tables instead of four. Persist, snapshot, and cold-tier plan generation
each drop one code path. The bundled audit-response change makes
tombstones visible in the audit trail, which was a latent gap (deleted
rows were simply invisible).

## Prior design (preserved for reversibility)

Preserved here so a future reader does not need to re-read CHA-118 to
understand what changed.

### DDL (prior)

```sql
CREATE TABLE "{physical}_data_insert_log" (
    version_uuid UUID PRIMARY KEY,
    row_uuid UUID NOT NULL,
    tx_uuid UUID NOT NULL,
    <user_columns>
);

CREATE TABLE "{physical}_data_update_log" (
    version_uuid UUID PRIMARY KEY,
    row_uuid UUID NOT NULL,
    tx_uuid UUID NOT NULL,
    <user_columns>
);
```

### Write-path dispatch (prior)

```python
for change in request.changes:
    if change.inserts:
        hot.insert_into(insert_log_table(phys), deserialize(change.inserts))
    if change.updates:
        hot.insert_into(update_log_table(phys), deserialize(change.updates))
    if change.deletes:
        hot.insert_tombstones(delete_log_table(phys), change.deletes, tx_uuid)
```

### `build_exclusion_set` (prior)

```sql
WITH committed_tx AS (...)
SELECT DISTINCT x.row_uuid
FROM (
    SELECT row_uuid, tx_uuid FROM update_log
    UNION ALL
    SELECT row_uuid, tx_uuid FROM delete_log
) x
JOIN committed_tx USING (tx_uuid)
LEFT JOIN insert_log i ON i.row_uuid = x.row_uuid
WHERE i.row_uuid IS NULL;
```

The anti-join was a correctness-preserving optimization: under the then-
classification invariant (every row_uuid lives in exactly one of
insert/update/delete log), a row_uuid in `insert_log` could not appear
in a cold snapshot, so excluding it bought nothing.

### Branch-merge classification (prior)

```sql
-- Shared CTEs
source_inserted AS (SELECT DISTINCT row_uuid FROM {source_insert_log}),
...

-- Route 1: alive ∈ source.insert_log → target.insert_log
INSERT INTO {target_insert_log} ...
FROM latest_upserts l JOIN source_inserted si USING (row_uuid)
WHERE (d.deleted_at IS NULL OR l.commit_micros > d.deleted_at);

-- Route 2: alive ∉ source.insert_log → target.update_log
INSERT INTO {target_update_log} ...
FROM latest_upserts l LEFT JOIN source_inserted si USING (row_uuid)
WHERE si.row_uuid IS NULL
  AND (d.deleted_at IS NULL OR l.commit_micros > d.deleted_at);

-- Route 3: tombstones ∉ source.insert_log → target.delete_log
-- (source-inserted-then-deleted rows dropped entirely)
```

CHA-133 was the planned follow-up that would have replaced membership in
`source.insert_log` with membership in `target.snapshot`. Under CHA-134
there is no routing decision left to make, so CHA-133 is closed as
superseded.

## Trigger conditions to revisit

Re-split if **any** of these hold in a future measurement:

1. **Exclusion-set hash becomes a merge-read bottleneck.** Today the hash
   set is small relative to the snapshot scan, so the "effectively free"
   claim holds. If profiling shows the hash lookup dominating
   `read_data` latency at production scale, a revived anti-join could
   shrink the set. (CHA-112 carried the original perf numbers; a future
   re-measurement should use the same methodology.)
2. **A downstream consumer needs physical-storage-level insert-vs-update
   distinction.** CDC, audit, and replication currently reconstruct the
   distinction by joining against a prior snapshot — the answer depends
   on the reader's base point anyway. If a consumer needs the
   distinction *at the storage layer* (e.g., a metrics sink that treats
   inserts and updates differently without reading historical state),
   re-split.
3. **Cold-tier scan plans benefit from skipping `insert_log` segments
   wholesale.** Today segment pruning is by `commit_micros`
   range, not by log table. If a future cold-tier layout (columnar
   zonemaps, bloom filters) makes per-log-table pruning cheaper than
   per-row `row_uuid` filtering, revisit.
4. **Strict-INSERT scanning becomes the dominant cost on high-write
   workloads.** CHA-121 landed strict-INSERT as a merge-on-read PK
   existence check inside the write transaction: the same planner +
   `merge_read` pipeline the query service uses, with `merge_read`'s
   all-hot fast path keeping the common OLTP case at one Postgres
   round trip. Note that a UNIQUE index on `upsert_log.row_uuid`
   would *not* work — every UPDATE appends another row with the same
   `row_uuid`, so the constraint would reject legal updates.
   Mitigations if the scan becomes the bottleneck: segment-level
   `row_uuid` statistics or bloom filters for per-segment pruning in
   the cold reader, a btree index on `row_uuid` in the hot
   `upsert_log` (per-row cost, no semantic change), or re-splitting
   into a physical `insert_log` that *does* carry `UNIQUE(row_uuid)`
   and lets the engine enforce the invariant.
5. **Dead-tombstone accumulation from post-fork create-then-delete
   sequences becomes a storage or audit-trail issue.** Under the
   unified `upsert_log`, branch-merge compaction cannot distinguish
   post-fork row creation from post-fork updates to pre-fork-inherited
   rows, because both land in `source.upsert_log`. As a consequence, if
   `source` creates a row post-fork and then tombstones it pre-merge,
   the tombstone still propagates to `target.delete_log` even though
   the row was never visible on `target`. Read correctness is
   unaffected (the tombstone excludes a `row_uuid` that isn't present
   in any upsert_log or snapshot), but the tombstones accumulate and
   surface in the audit trail. If either the storage cost or the
   audit-trail noise becomes material, a fix is to query
   `target`'s pre-merge state via `build_merge_resolved` (against
   `target`'s plan as of the merge base) and filter them out. The
   prior `insert_log` membership predicate was a cheaper approximation
   of the same filter, so a targeted re-split of just the alive-route
   classification is also an option.

## Related tickets

- [CHA-134](https://linear.app/chapala/issue/CHA-134) — this decision.
- [CHA-133](https://linear.app/chapala/issue/CHA-133) — superseded; the
  branch-merge classification it was going to rework no longer exists.
- [CHA-121](https://linear.app/chapala/issue/CHA-121) — strict-INSERT
  semantics via a merge-on-read PK collision scan; shares the query
  path's planner + `merge_read`, so the physical layout choice here
  bounds its cost.
- [CHA-118](https://linear.app/chapala/issue/CHA-118) — the prior
  split that this decision unwinds for data tables. Table-metadata
  auditable stores were unwound in a follow-up ([CHA-149](https://linear.app/chapala/issue/CHA-149)).
- [CHA-149](https://linear.app/chapala/issue/CHA-149) — follow-up that
  applied the same unification to the `table_metadata_*_log` tables
  (per-schema at the time; lifted to per-catalog subpartitioned
  `schema_uuid → branch_uuid` by [CHA-163](https://linear.app/chapala/issue/CHA-163)).
- [CHA-112](https://linear.app/chapala/issue/CHA-112) — merge-on-read
  algorithm. The exclusion-set in Query B is the load-bearing
  structure whose shape this decision simplifies.
