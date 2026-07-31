# System tables reference

System tables use the same primitive types (log, store) as user data. Some
logs are composed into auditable stores as noted in the
[README](../README.md#auditable-stores-composition-pattern). Tables are split
into three scopes based on cardinality and access patterns.

## Storage metadata tables (global)

These tables are global to a Penca deployment. A single deployment is tied
to a single organization, so write contention on global tables is expected to
be low.

**1. Catalog store** (store)

| Column | Type |
|--------|------|
| `catalog_uuid` | UUID (PK) |
| `catalog_name` | text |
| `catalog_owner` | text |
| `description` | text |

CHA-433: retention is schema-broadest — the catalog carries no retention policy
(set `retention_duration_seconds` / `snapshot_density_seconds` on schemas/tables).

Indices: `catalog_owner`

Per-catalog table names (schema store, branch store, begin tx log, tx log,
abort tx log, table metadata upsert log, table metadata delete log, plus
the five CHA-198 persist + snapshot metadata parents) are derived by
convention from the catalog UUID — e.g.,
`f"{catalog_uuid}_commit_tx_log"` for the parent and
`commit_tx_log_partition(catalog_uuid, branch_uuid)` for branch-direct
partitions — rather than stored in a mapping column. The mapping is 1:1
and deterministic, so an explicit column would add a lookup to every read
path for information already fully determined by the catalog UUID. If
independent renaming or table sharing is ever needed, migrating from
convention to a mapping column is straightforward (add column, backfill
with convention values, update read path).

**5. Object metadata store** (store)

| Column | Type |
|--------|------|
| `object_uuid` | UUID (PK) |
| `git_hash` | text |
| `created_at` | int64 (micros, auto-generated) |

Indices: `git_hash`

## Core tables (per-catalog)

These tables are scoped to a catalog (CHA-163). The catalog is the resource
isolation boundary stated in `README.md`; lifting core metadata to that level
gives multi-schema atomicity (a single transaction can touch tables in
multiple schemas of one catalog) while preserving tenant-axis isolation:
writes to one catalog never contend with another's. Per-catalog tables give
physical isolation of B-tree indices, independent vacuum schedules, and
clean `DROP CATALOG` semantics.

Schemas remain a logical grouping below the catalog (matching Postgres
semantics). Schema-scoped DROP is preserved by **subpartitioning** the
table_metadata logs (see ADR
[0008](decisions/0008-table-metadata-subpartitioning.md)): the parent
partitions by `schema_uuid` so `DROP SCHEMA` cascades through PG's
partition tree atomically; the schema-level intermediate further partitions
by `branch_uuid` for cheap branch-direct reads.

Anything that affects point-in-time views of actual data (table names,
data tables, etc.) is composed into an auditable store from primitive
logs. Branch history is deliberately not auditable — branches are a
store, not part of an auditable store — mirroring git's model where
branch pointers are mutable refs but the commit history on each branch
is immutable.

**6. Schema metadata** — `__penca_system__.schemas`

Per CHA-177, schema metadata is stored as a regular Penca Table
under the well-known schema `__penca_system__`. Each row is one
schema on one branch in this catalog. The physical PG tables follow
the standard `{prefix}_data_{upsert,delete}_log` naming used by every
user data table:

```
prefix = data_log_prefix(system_schemas_table_uuid(catalog), branch_uuid)
       = row_uuid_for_pk(system_schemas_table_uuid(catalog), [branch_uuid])
```

So each branch has its own pair of physical SQL tables for
`__penca_system__.schemas`. Visibility resolves through the same
`commit_tx_log` JOIN every other auditable store uses; schema CRUD
participates in the same MVCC scheme as data writes (CHA-164).

`schema_uuid` is a deterministic xxh3 hash of
`catalog_uuid:schema_name`. Two well-known schemas are seeded at
`CreateCatalog`, both tagged with the catalog's `genesis_tx_uuid`:

- `public` — default target for unqualified DML (CHA-163).
- `__penca_system__` — reserved namespace for Penca-internal
  metadata exposed as first-class tables. User DDL/DML against it is
  allowed but operators should treat it as a public read surface, not
  a scratchpad: name collisions with future system tables will break
  upgrades.

User-column shape. `schema_uuid` is a first-class PK column and the
auditable-store `row_uuid = row_uuid_for_pk(system_schemas_table_uuid(
catalog), [schema_uuid])` like every other Penca table (CHA-380); the
`version_uuid` / `row_uuid` / `tx_uuid` system columns are not listed:

| Column | Type |
|--------|------|
| `schema_uuid` | text (PK) |
| `schema_name` | text |
| `branch_uuid` | text |
| `description` | text |
| `retention_duration_seconds` | int64 (seconds, nullable) |
| `snapshot_density_seconds` | int64 (seconds, nullable) |

`DeleteSchema` writes a tombstone to the matching
`{prefix}_data_delete_log` — same shape as user data deletes.

**7. Branch store** (store)

Per-catalog. Seeded with a `main` branch (`branch_uuid` is a deterministic
xxh3 hash of `catalog_uuid:branch_name`). A branch spans every schema
in its catalog (git-like).

| Column | Type |
|--------|------|
| `branch_uuid` | UUID (PK) |
| `branch_name` | text |
| `fork_commit_seq_num` | bigint |

**8. Begin transaction log** (log)

Per-catalog, LIST-partitioned by `branch_uuid`. Rows can be cleaned up
greedily once committed. Expired transactions require cleaning up
associated data rows before deletion.

| Column | Type |
|--------|------|
| `tx_uuid` | UUID (PK) |
| `branch_uuid` | UUID (FK) |
| `began_at_micros` | int64 (micros) |
| `expires_at_micros` | int64 (micros) |
| `comment` | text |
| `author` | text |

Indices: `branch_uuid`, `author`

**8b. Abort transaction log** (log)

Per-catalog. Append-only ledger of explicit `AbortTx` calls, mirroring
the `begin_tx_log` shape (LIST-partitioned by `branch_uuid`,
`(tx_uuid, branch_uuid)` PK). `CommitTx` consults this table as a
precondition and fails with `FAILED_PRECONDITION` if a row exists;
`AbortTx` inserts here under `ON CONFLICT DO NOTHING`, so re-aborting
the same transaction is a no-op.

| Column | Type |
|--------|------|
| `tx_uuid` | UUID (PK part) |
| `branch_uuid` | UUID (PK part) |
| `aborted_at_micros` | int64 (micros, DB-generated default) |

Indices: implicit `(tx_uuid, branch_uuid)` PK + per-branch partition.

**9. Transaction log** (log) — *one log per catalog, shared by all auditable stores in the catalog*

Per-catalog, LIST-partitioned by `branch_uuid`. A single Penca tx lives
in exactly one partition (one branch) and can touch tables in multiple
schemas, which is the entire point of CHA-163.

| Column | Type |
|--------|------|
| `tx_uuid` | UUID (PK) |
| `branch_uuid` | UUID (FK) |
| `began_at_micros` | int64 (micros) |
| `commit_micros` | int64 (micros) |
| `comment` | text |
| `author` | text |

Indices: `branch_uuid`, `author`

**9b. Per-(tx, table) summary index** (log)

Per-catalog, LIST-partitioned by `branch_uuid`. Records which logical
tables each penca tx wrote to — one row per distinct `(tx_uuid,
table_uuid)` regardless of row count. Bulk inserts pay one summary
row, not per-row overhead. The PK alone enforces idempotent emission
across multiple `WriteData` calls within the same penca tx.

Lifted out of CHA-5 (merge conflict detection) once CHA-168 (branch-
coordinated persist) needed the same metadata; both consumers join this
against the branch's `commit_tx_log` to find which tables a tx affected.

| Column | Type |
|--------|------|
| `tx_uuid` | UUID (PK part) |
| `branch_uuid` | UUID (PK part) |
| `table_uuid` | UUID (PK part) |

Indices: `(tx_uuid, branch_uuid, table_uuid)` PK + per-branch
partition. PK leads on `tx_uuid` (matching the tx-log family) so
downstream `WHERE tx_uuid IN (...)` lookups hit the PK directly — no
secondary index needed.

**10. Table metadata** — `__penca_system__.tables`

Per CHA-177, table metadata is stored as a regular Penca Table
under `__penca_system__.tables`. Each row is one table on one
branch. The physical PG tables follow the standard
`{prefix}_data_{upsert,delete}_log` naming:

```
prefix = data_log_prefix(system_tables_table_uuid(catalog), branch_uuid)
```

So each branch has its own pair of physical SQL tables for
`__penca_system__.tables`. CHA-164 brought table metadata into the
same MVCC scheme as data: rows carry `tx_uuid` and visibility
resolves via JOIN against `commit_tx_log_partition(catalog, branch)` — the
same shape as user data. `CommitTx` and `AbortTx` toggle visibility
for metadata and data rows together, atomically. See
[ADR 0011](decisions/0011-transactional-metadata-stores.md) for the
detailed rationale and [ADR 0012](decisions/0012-metadata-as-first-class-tables.md)
for the substrate decision.

Inserts (from `CreateTable`) and updates (from `UpdateTable`) append
to the same upsert log; the create-vs-update distinction resolves at
read time. Every row carries all fields forward in full — one row is
a complete table definition at a point in time.

**Single identity (CHA-177 + CHA-203).** A table's identity is
`(catalog_uuid, schema_uuid, table_uuid)` plus the branch axis
(`branch_uuid`). The PG data tables on each branch derive
deterministically from `(table_uuid, branch_uuid)` via
`upsert_log_table` / `delete_log_table`. `table_uuid` is a first-class
PK column and the auditable-store `row_uuid = row_uuid_for_pk(
system_tables_table_uuid(catalog), [table_uuid])` like every other Penca
table (CHA-380); `(table_uuid, branch_uuid)` is the row's logical identity
in `__penca_system__.tables` *and* the pointer to its data tables. Concurrent `CreateTable` on the
same `(table_uuid, branch_uuid)` target the same PG tables (`CREATE
TABLE IF NOT EXISTS` makes the second a no-op); both metadata rows
land with different `version_uuid`s and auditable-store dedup picks
the latest.

User-column shape:

| Column | Type |
|--------|------|
| `table_uuid` | text |
| `table_name` | text |
| `schema_uuid` | text |
| `branch_uuid` | text |
| `arrow_schema` | bytes (Arrow IPC serialized schema) |
| `partition_keys` | text[] |
| `clustering_keys` | text[] |
| `primary_keys` | text[] |
| `description` | text |
| `retention_duration_seconds` | int64 (seconds, nullable) |
| `snapshot_density_seconds` | int64 (seconds, nullable) |

`DeleteTable` writes a tombstone to the matching
`{prefix}_data_delete_log` — same shape as user data deletes.

**10b. Index metadata** (`index_metadata`) — `__penca_system__.indexes`

The `index_metadata` store holds index definitions (CHA-455), stored as a
regular Penca Table under
`__penca_system__.indexes` — the same dogfooded auditable-store pattern
as `__penca_system__.tables`: per-branch `{prefix}_data_{upsert,delete}_log`
physicals, rows carrying `tx_uuid` with visibility resolved via the
`commit_tx_log_partition(catalog, branch)` JOIN (MVCC + time-travel + `CreateBranch`
fork inheritance for free). Written by `CreateIndex` and inline
`CreateTable.indexes`; `UpdateIndex` is rename-only (appends a new auditable
row), `DeleteIndex` writes a tombstone. `index_uuid` is a first-class PK
column and `row_uuid = row_uuid_for_pk(system_indexes_table_uuid(catalog),
[index_uuid])` like every other Penca table (CHA-380); `table_uuid` is the
distinct foreign key naming the owning table, and `index_name` is unique
only within that table. This is the *definition* store —
query planning never reads it (it reads the materialization table below); a
freshly-created index is not usable until the next snapshot materializes its
per-segment sidecars (ADR 0026 §5).

User-column shape:

| Column | Type |
|--------|------|
| `index_uuid` | text (PK; `row_uuid = row_uuid_for_pk(sys_indexes, [index_uuid])`) |
| `table_uuid` | text (FK: owning table) |
| `index_name` | text (unique within table) |
| `columns` | text[] (indexed columns, in order) |
| `index_type` | int32 (`IndexType`: 1 = `SCALAR_BTREE`) |

No `unique` flag (ADR 0026 §4) — cold read-accelerators built after the
write committed cannot enforce a write-time constraint.

**11. Object metadata store** (store)

Same schema as the global object metadata store (table 5), scoped
per-catalog.

**12. Table persist metadata** — `{catalog_uuid}_table_persist_metadata`

Per-catalog, LIST-partitioned by `branch_uuid` (CHA-198). One row per
`(table, persist event, log_kind)` triple. `log_kind` is
`CHECK`-restricted to `{upsert_log, delete_log}` — CHA-218 collapsed
the `commit_tx_log` virtual table (commit_tx_log is hot-only; cold rows pre-join
the tx metadata columns — `commit_micros, began_at_micros,
comment, author`, plus `write_seq_num` (CHA-431) and `commit_seq_num`
(CHA-430)). Gates visibility for every child
`table_persist_segment_metadata` row — a segment is visible to reads
only when its row + parent table_persist both have non-NULL
`commit_micros`.

CHA-220 reshape: `branch_persist_metadata` is gone; this table is the
persist recovery anchor. `persisted_at_micros` (formerly on the deleted
parent) moves onto this row.

| Column | Type |
|--------|------|
| `table_persist_uuid` | UUID (PK part) |
| `branch_uuid` | UUID (PK part, partition key) |
| `table_uuid` | UUID |
| `persisted_at_micros` | int64 (micros, the watermark this persist advances to) |
| `commit_seq_num` | int64 (nullable; CHA-443 persist seq watermark = `MAX(commit_seq_num)` over the committed rows persisted — the seq analog of `persisted_at_micros`; NULL on the aborts-only branch) |
| `log_kind` | text CHECK (`upsert_log`, `delete_log`) |
| `written_at_micros` | int64 (DB-generated on insert) |
| `commit_micros` | int64 (NULL until phase 2) |

Indices: implicit `(branch_uuid, table_persist_uuid)` PK + per-branch
partition.

**13. Table purge metadata** — `{catalog_uuid}_table_purge_metadata`

Per-catalog, LIST-partitioned by `branch_uuid` (CHA-198). One row per
`Purge(T)` invocation (CHA-220). CHA-233 (ADR 0019): `plan()`'s
hot/cold visibility cutoff is sourced from
`table_persist_metadata.persisted_at_micros`, not from this watermark.
`purged_at_micros` now feeds Purge's idempotence check (skip the hot
DELETE when nothing eligible past grace strictly advances past
`max_purged`) and CHA-221's commit_tx_log GC branch-min.

| Column | Type |
|--------|------|
| `table_purge_uuid` | UUID (PK part) |
| `branch_uuid` | UUID (PK part, partition key) |
| `table_uuid` | UUID |
| `purged_at_micros` | int64 (micros, the watermark this purge advances to) |
| `written_at_micros` | int64 (DB-generated on insert) |
| `commit_micros` | int64 (NULL until phase 2) |

Indices: implicit `(branch_uuid, table_purge_uuid)` PK + per-branch
partition + `(table_uuid, commit_micros DESC)` (latest-purge
lookup path).

**14. Table persist segment metadata** — `{catalog_uuid}_table_persist_segment_metadata`

Per-catalog, LIST-partitioned by `branch_uuid` (CHA-198). One row per
cold file. Segments JOIN their parent `table_persist_metadata` row for
the `log_kind` classification (CHA-218: only `upsert_log` and
`delete_log` exist post-CHA-218 — no commit_tx_log on cold).

| Column | Type |
|--------|------|
| `table_persist_segment_uuid` | UUID (PK part) |
| `table_persist_uuid` | UUID |
| `branch_uuid` | UUID (PK part, partition key) |
| `table_uuid` | UUID |
| `chunk_idx` | int32 (CHA-215 sibling-uniquifier; 0 for unchunked) |
| `min_tx_commit_micros` | int64 (micros) |
| `max_tx_commit_micros` | int64 (micros) |
| `min_commit_seq_num` | int64 (CHA-430, commit-order prune axis) |
| `max_commit_seq_num` | int64 (CHA-430, commit-order prune axis) |
| `object_uri` | text |
| `offset` | int64 (set at compact time) |
| `length` | int64 (set at compact time) |
| `row_count` | int64 |
| `format` | text (parquet, lance) |
| `content_hash` | UUID NOT NULL (CHA-545, segment-cache key with `format`) |
| `size_bytes` | int64 |
| `metadata` | JSONB |
| `statistics` | JSONB |
| `written_at_micros` | int64 (DB-generated on insert) |
| `commit_micros` | int64 (NULL until per-segment commit) |

Indices: implicit `(branch_uuid, table_persist_segment_uuid)` PK +
per-branch partition.

`content_hash` is the `xxh3_128` of the segment's typed in-memory Arrow
batch, computed once at write time and inherited verbatim by every
reference copy (carry-forward, CHA-539 fork copy). It keys the in-process
`SegmentCache`, so a fork and its parent share one decoded entry for a byte
range they both reference — which the row uuid cannot express, since a
reference copy mints a fresh uuid over bytes it did not rewrite. `NOT NULL`
with no default: every writer computes it, and a catalog predating the
column is recreated rather than migrated (there is no in-place migration
path for catalog metadata), so there is no legacy row to default. Not
indexed — reads carry the value through, they never look a row up by it.

Each level's `written_at_micros` / `commit_micros` pair
supports the three-level commit decoupling. Rows with
`commit_micros IS NULL` (at any level) are invisible to reads
because the parent gating filters them out. See
[algorithms.md](algorithms.md#persist-hot--cold) for the phase 1 / 2
algorithm.

**14b. Cold tx_log segment metadata** — `{catalog_uuid}_tx_log_persist_segment_metadata`

Per-catalog, **unpartitioned** (`branch_uuid` a column; the slim per-branch
commit map is low-volume, unlike the per-branch-partitioned data-segment index
above). One row per cold `tx_log` file, written two-phase by `persist_tx_log`
(CHA-507; ADR 0030). Rows with `committed_at_micros IS NULL` are uncommitted
(invisible to reads + the watermark). `W_txlog = MAX(max_commit_seq_num)` over
committed rows gates `PurgeTxLog`; reads seek the sorted `commit_seq_num` /
`commit_micros` columns.

| Column | Type |
|--------|------|
| `tx_log_segment_uuid` | UUID (PK part) |
| `branch_uuid` | UUID (PK part) |
| `min_commit_seq_num` | int64 |
| `max_commit_seq_num` | int64 |
| `min_commit_micros` | int64 (micros) |
| `max_commit_micros` | int64 (micros) |
| `object_uri` | text |
| `row_count` | int64 |
| `format` | text (parquet, lance) |
| `committed_at_micros` | int64 (NULL until per-segment commit) |

Deliberately carries **no** `content_hash`, unlike the three cold-artifact
tables around it (14, 16, 17): a cold `tx_log` file is read by
`read_tx_log_batches`, never through `SegmentCache`, and is never
reference-copied — so it has neither of the two properties the hash exists
to serve.

**15. Table snapshot metadata** — `{catalog_uuid}_table_snapshot_metadata`

Per-catalog, LIST-partitioned by `branch_uuid` (CHA-198). One row per
snapshot operation. Stores the table-level watermark that applies
consistently to all segments in this snapshot.

| Column | Type |
|--------|------|
| `table_snapshot_uuid` | UUID (PK part) |
| `branch_uuid` | UUID (PK part, partition key) |
| `table_uuid` | UUID |
| `snapshotted_at_micros` | int64 (micros) |
| `commit_seq_num` | int64 (CHA-443 snapshot seq watermark `W_snap` = `max(prev W_snap, MAX(included persist seg.max_commit_seq_num))`; `-1` genesis for an empty baseline so the seq-aware picker keeps it selectable) |
| `durable` | bool (NOT NULL, default false; CHA-432 permanent retention rung — set once at snapshot creation and sticky, iff no prior durable rung / density unset / at least `snapshot_density_seconds` past the last durable. The retention floor is the newest durable at/before the window start) |
| `partition_keys` | text[] (nullable; write-time layout keys, CHA-404) |
| `clustering_keys` | text[] (nullable; effective sort keys — clustering defaulting to PKs, CHA-404) |
| `written_at_micros` | int64 (micros, auto-generated) |
| `commit_micros` | int64 (micros, nullable) |

Indices: implicit `(branch_uuid, table_snapshot_uuid)` PK + per-branch
partition.

**16. Table snapshot segment metadata** — `{catalog_uuid}_table_snapshot_segment_metadata`

Per-catalog, LIST-partitioned by `branch_uuid` (CHA-198). Typically
one row per partition (oversized partitions split into multiple
chunked rows; the empty-merge placeholder is a zero-row segment with
no partition); multiple segments may share one packed file, addressed
by `(offset, length)` row ranges (CHA-404). Segment files are
immutable — never compacted (ADR 0024, CHA-407); per-segment
`statistics` carry the pruning bounds. Stores file location and
per-segment `commit_micros` for progressive availability
(individual segments become queryable before the full snapshot
completes).

| Column | Type |
|--------|------|
| `table_snapshot_segment_uuid` | UUID (PK part) |
| `table_snapshot_uuid` | UUID |
| `branch_uuid` | UUID (PK part, partition key) |
| `table_uuid` | UUID |
| `chunk_idx` | int32 (CHA-215 sibling-uniquifier; 0 for unchunked) |
| `object_uri` | text |
| `offset` | int64 NOT NULL (row offset of the segment's range within the file; whole-file range for a single-segment file) |
| `length` | int64 NOT NULL (row count of the range) |
| `size_bytes` | int64 |
| `format` | text (lance, parquet) |
| `content_hash` | UUID NOT NULL (CHA-545, segment-cache key with `format` — see table 14) |
| `metadata` | JSON (format-specific, e.g., row group size) |
| `statistics` | JSON (column stats: min/max for filterable columns) |
| `row_count` | int64 |
| `written_at_micros` | int64 (micros, auto-generated) |
| `commit_micros` | int64 (micros, nullable) |

Indices: implicit `(branch_uuid, table_snapshot_segment_uuid)` PK +
per-branch partition.

Statistics are stored as JSON (not binary) so that filtering can be pushed
down to the Postgres query engine.

**17. Cold-index materialization metadata** — `{catalog_uuid}_table_snapshot_index_metadata` (parent) + `{catalog_uuid}_table_snapshot_segment_index_metadata` (child)

The *materialization* half of the cold index (CHA-412 / ADR 0026 §5), split into a
snapshot parent/child pair mirroring `table_snapshot_metadata` →
`table_snapshot_segment_metadata`. Both per-catalog, LIST-partitioned by
`branch_uuid`. Query planning reads THESE tables, never
`__penca_system__.indexes` — it needs artifact URIs, which are physical
lifecycle outputs. `index_uuid IS NULL` ⇒ the strictly-internal `row_uuid`
identity index (never user-facing); non-NULL ⇒ a *declared* index — either a
built-in system-table name index (CHA-481 — deterministic
`naming::system_name_index_uuid`, auto-built on `__penca_system__.*` snapshots,
never a row in the `__penca_system__.indexes` user-DDL registry) or a user
secondary index (CHA-463). There is **no** `index_kind` column —
`index_uuid IS NULL` is the role discriminator.

**Parent** — one row per `(snapshot, index)`, a fileless header re-declared fresh
each snapshot and reaped when its snapshot retires:

| Column | Type |
|--------|------|
| `table_snapshot_index_uuid` | UUID (PK part) — deterministic xxh3 from `naming::table_snapshot_index_uuid(table_snapshot_uuid, index_uuid)` (discriminator `"row_uuid"` when `index_uuid` is NULL) |
| `branch_uuid` | UUID (PK part, partition key) |
| `table_snapshot_uuid` | UUID (the snapshot this index belongs to) |
| `index_uuid` | UUID (nullable — NULL ⇒ internal `row_uuid` index; a built-in system-table name index uses a deterministic `naming::system_name_index_uuid` value that is *not* a `__penca_system__.indexes` row (CHA-481); otherwise a logical, un-enforced reference to `__penca_system__.indexes`, ADR 0015) |
| `written_at_micros` | int64 (micros, auto-generated) |
| `commit_micros` | int64 (micros, nullable) |

Index: `(table_snapshot_uuid)` for the planner's "does snapshot S have index X?" probe.

**Child** — one row per `(segment, index)` sidecar (an index sidecar is itself a
cold file), referencing its parent via `table_snapshot_index_uuid`. Carries
forward by reference with its base segment and participates in the ref-counted GC
/ `segment_delete_set` grace sweep when retired or compacted (CHA-405 / ADR 0019):

| Column | Type |
|--------|------|
| `segment_index_uuid` | UUID (PK part) — deterministic xxh3 `row_uuid_for_pk(segment_uuid, ["row_uuid"])`; a fresh build and a carry-forward of the same segment produce the same id |
| `branch_uuid` | UUID (PK part, partition key) |
| `segment_uuid` | UUID (the base segment this sidecar accelerates) |
| `table_snapshot_index_uuid` | UUID (the parent index row — carries the index identity; the child does not duplicate `index_uuid`) |
| `object_uri` | text |
| `offset` | int64 |
| `length` | int64 |
| `format` | text (lance, parquet) |
| `content_hash` | UUID NOT NULL (CHA-545, segment-cache key with `format` — see table 14) |
| `size_bytes` | int64 |
| `statistics` | bytes (indexed-key min/max bounds; binary, decoded in-planner by the CHA-454 seek in the `SnapshotTableProvider`) |
| `written_at_micros` | int64 (micros, auto-generated) |
| `commit_micros` | int64 (micros, nullable) |

Indices: implicit `(branch_uuid, …_uuid)` PKs + per-branch partitions; child
`(segment_uuid)` + `(object_uri)`; parent `(table_snapshot_uuid)`.

`index_uuid IS NULL` (role: internal vs user secondary) and `index_type` (physical
layout, on `__penca_system__.indexes`) are orthogonal.

Sidecars carry `content_hash` for the same reason base segments do: they are
read out of object storage through the same `SegmentCache` and copied by
reference by both carry-forward and the CHA-539 fork copy. `segment_index_uuid`
is stable across a carry-forward *within* a branch, but a fork derives it from
the child's own `segment_uuid` and so mints a new id over unchanged bytes —
which is exactly the duplication the hash collapses.

## Data tables (per-branch, per-table)

Both user tables and the two system tables (`__penca_system__.schemas`,
`__penca_system__.tables`) store their data in the same shape: a pair of
physical SQL tables per `(table_uuid, branch_uuid)`, named using
`data_log_prefix(table_uuid, branch_uuid) =
row_uuid_for_pk(table_uuid, [branch_uuid])`. This eliminates cross-branch
schema conflicts — the same table name can have different schemas on
different branches without interference. Physical data tables are created
at `CreateTable` time (DDL/DML separation means no lazy creation needed).
For the two system tables, the physicals are created at `CreateBranch`
along with the materialization of inherited rows.

Together with the schema's transaction log (#8), each branch's data tables
compose into an auditable store.

**12. Data upsert log** (log) — *auditable store: user data upsert log*

Records inserts and updates to user rows uniformly. Each mutation appends a new
version row; the merge algorithm selects the latest committed version per
`row_uuid`. See [docs/decisions/0001-unified-upsert-log.md](decisions/0001-unified-upsert-log.md)
for the rationale for the unified shape.

| Column | Type |
|--------|------|
| `version_uuid` | UUID (PK) |
| `row_uuid` | UUID |
| `tx_uuid` | UUID (FK) |
| *...user-defined columns* | |

Indices: `row_uuid`, `tx_uuid`

`row_uuid` is a deterministic xxh3_128 hash of the table UUID and primary key
values, so the same logical row has a stable identity across branches and
merges. Every table must declare at least one primary key column at creation
time (`primary_keys` in the table metadata). If a table has no natural key,
use a synthetic UUID column as the primary key.

**13. Data delete log** (log) — *auditable store: user data delete log*

| Column | Type |
|--------|------|
| `row_uuid` | UUID |
| `tx_uuid` | UUID (FK) |

Indices: `tx_uuid`

Primary keys are immutable — changing them on an existing table is
forbidden because it would invalidate all `row_uuid` hashes in the
upsert and delete logs.
