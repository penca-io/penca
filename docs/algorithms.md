# Core algorithms

This document describes the step-by-step algorithms behind Penca's write
path, read path, and branch merge. For architecture and storage model
context, see [README.md](../README.md).

Code references point to the Rust crates (`penca-api` managers +
`penca-storage-*` clients + Postgres / object storage). The gRPC
transport is orthogonal to the algorithm; servicers in
`penca-server-grpc` are thin wrappers around the manager methods
documented here.

## Design principles

**No untracked orphaned files in object storage.** Every file in object
storage must be tracked by a metadata row. List operations on object
storage (S3, GCS) are expensive and should never be required for
correctness. When cleaning up after failures, always delete the file
before its metadata row. If file deletion fails, the metadata row must
be preserved — an uncommitted metadata row pointing to an existing file
is recoverable; a file with no metadata row is an orphan that can only
be found by listing the entire bucket.

**No foreign keys in metadata tables.** Cross-table references in the
Postgres metadata schema (parent persist rows ↔ child segment rows,
`tx_table_log` ↔ `commit_tx_log`, sys-table data ↔ `branch_store`, etc.) are
not enforced by `FOREIGN KEY` constraints. Relational integrity comes
from deterministic UUID derivation (`xxh3` of known inputs), staged
writes (parent inserted before child), and recovery sweeps that walk
the tree on the next operation. See ADR 0015 for the rationale and
the rules that apply when adding new metadata tables.

## Table DDL (non-transactional)

Table create, update, and delete are immediate admin operations — not
wrapped in begin_tx/commit_tx. This follows the industry-standard
DDL/DML separation used by Spanner, CockroachDB, MySQL, and Oracle.

**Implementation:** `AdminManager.create_table`, `update_table`,
`delete_table` (`lib/api/admin.py`)

### Three-UUID identity model

Every table has these UUIDs (post-CHA-177):

| UUID | Derivation | Purpose |
|------|-----------|---------|
| `table_uuid` | `xxh3(schema_uuid, table_name)` | Stable identity across branches; used to derive user-row identity |
| `row_uuid` (user-row) | `xxh3(table_uuid, pk_values)` | Per-row identity, branch-independent (load-bearing for merge matching) |
| `version_uuid` | `xxh3(row_uuid, tx_uuid)` (ADR 0013) | PK of every auditable-store row; structurally enforces "one version per (entity, tx)" |

Per-branch data tables (`{prefix}_data_upsert_log` /
`{prefix}_data_delete_log`, with `prefix = xxh3(table_uuid,
branch_uuid)`) are created at `create_table` time. Each branch gets
its own per-branch data tables, so branch-specific schema evolution
never conflicts. Storage-shape rationale: see
[decisions/0001-unified-upsert-log.md](decisions/0001-unified-upsert-log.md).
The CHA-177 switch to deterministic per-branch tables (replacing the
pre-CHA-177 per-create-tx carry-forward) is in
[decisions/0012-metadata-as-first-class-tables.md](decisions/0012-metadata-as-first-class-tables.md).
The CHA-203 recursive UUID chain for persist + snapshot derived rows
is in
[decisions/0016-canonical-uuid-construction-for-derived-rows.md](decisions/0016-canonical-uuid-construction-for-derived-rows.md).

### Non-transactional auditable store

Table metadata lives in per-catalog `table_metadata_upsert_log` and
`table_metadata_delete_log` tables, subpartitioned `schema_uuid →
branch_uuid` per ADR 0008. Unlike data mutations, these carry
`commit_micros` directly (no `tx_uuid`, no commit_tx_log join). Resolution
uses the same ranked-window algorithm as data reads, but filters on
`commit_micros` and `branch_uuid` columns directly.

## Write path

The write path spans three operations: begin a transaction, apply
mutations, and commit.

For single-mutation transactions, calling `WriteData` with `tx_uuid`
unset auto-commits — the server combines all three operations into a
single Postgres transaction, skipping the `begin_tx_log` and writing
directly to the `commit_tx_log` (same pattern as merge transactions). This
reduces three round trips to one and returns the new commit timestamp
inline on `WriteDataResponse.commit_micros`. Use the full
`BeginTx` → `WriteData` (with the open `tx_uuid`) → `CommitTx` flow
for multi-statement transactions or large payloads that exceed the
gRPC message size limit.

### BeginTx

**Implementation:** `WriteManager.begin_tx`
(`lib/api/write.py`)

1. Generate a `tx_uuid` (caller-supplied or random).
2. Compute `expires_at_micros = now() + timeout_seconds * 1_000_000`.
3. INSERT into the catalog's `begin_tx_log` branch partition:
   ```sql
   INSERT INTO {begin_tx_log_partition}
     (tx_uuid, branch_uuid, began_at_micros, expires_at_micros, comment, author)
   VALUES (%s, %s, now_micros(), now_micros() + timeout, %s, %s)
   ```
4. Return `BeginTxResponse { tx_uuid, began_at_micros, expires_at_micros }`.

The begin_tx_log tracks pending transactions. Rows are cleaned up after
commit; expired rows (past `expires_at_micros`) are garbage-collected.

### WriteData

**Implementation:** `WriteManager.write_data`
(`lib/api/write.py`)

The request includes a `branch_uuid` that identifies which branch's
per-branch data tables to write to.

For each `Change` in the request:

1. Look up the table's `primary_keys` from the table metadata auditable
   store (scoped to `schema_uuid` and `branch_uuid`).
2. Resolve the per-branch PG table names via the naming helpers:
   ```python
   upsert_log = upsert_log_table(table_uuid, branch_uuid)
   delete_log = delete_log_table(table_uuid, branch_uuid)
   ```
3. **Upserts**: every row write (new or overwrite) goes to the
   branch's data upsert log. The client does not pre-classify —
   `Change.upserts` is a single Arrow IPC payload.
   - Deserialize the Arrow IPC bytes into a `RecordBatch`.
   - For each row, compute a deterministic `row_uuid` from the
     cross-branch-stable `table_uuid` so the same row has stable
     identity regardless of branch:
     ```python
     row_uuid = xxh3_128_hexdigest(f"{table_uuid}:{pk1}:{pk2}:...")
     # Formatted as UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
     ```
   - INSERT into the branch's data upsert log:
     ```sql
     INSERT INTO {upsert_log}
       (version_uuid, row_uuid, tx_uuid, ...user_columns)
     VALUES (...)
     ```
   - The same primary key values always produce the same `row_uuid`,
     giving rows a stable identity across branches and merges. See
     [decisions/0001-unified-upsert-log.md](decisions/0001-unified-upsert-log.md)
     for why the write path no longer routes new vs existing rows to
     separate tables.
4. **Deletes**: INSERT into the branch's data delete log:
   ```sql
   INSERT INTO {delete_log} (row_uuid, tx_uuid) VALUES (%s, %s)
   ```

After all `Change` records are processed (and still inside the same
Postgres transaction), emit one row per distinct table touched into
the catalog's `tx_table_log` branch partition (CHA-181):

```sql
INSERT INTO {tx_table_log_partition} (tx_uuid, branch_uuid, table_uuid)
SELECT %s::uuid, %s::uuid, t FROM unnest(ARRAY[...]::uuid[]) AS t
ON CONFLICT (tx_uuid, branch_uuid, table_uuid) DO NOTHING
```

This is the per-(tx, table) summary index — one row per distinct
table regardless of row count. Bulk inserts pay one summary row, not
per-row overhead. `ON CONFLICT DO NOTHING` keeps emission idempotent
across multiple `WriteData` calls within the same penca tx.
Consumers (CHA-5 conflict detection, CHA-168 persist) join this against
the branch's `commit_tx_log` to find which tables a tx affected.

Mutations are not visible to readers until the transaction is committed
(the read path joins against `commit_tx_log`, which only contains committed
transactions).

### CommitTx

**Implementation:** `WriteManager._commit_tx_in_transaction`
(`lib/api/write.py`)

Runs inside a Postgres transaction:

1. Look up the begin record from `begin_tx_log` to get `branch_uuid` and
   metadata.
2. INSERT into the catalog's `commit_tx_log` branch partition, guarded by a
   `WHERE NOT EXISTS` subquery against the same branch's `abort_tx_log`
   partition:
   ```sql
   INSERT INTO {commit_tx_log_partition}
     (tx_uuid, branch_uuid, began_at_micros, comment, author)
   SELECT %s, %s, %s, %s, %s
   WHERE NOT EXISTS (
     SELECT 1 FROM {abort_tx_log_partition}
     WHERE tx_uuid = %s AND branch_uuid = %s
   )
   RETURNING commit_micros
   -- commit_micros uses the column DEFAULT (now_micros())
   ```
3. If `RETURNING` produces zero rows, an `AbortTx` already landed for
   this `(tx_uuid, branch_uuid)` — raise `FailedPreconditionError`
   (gRPC `FAILED_PRECONDITION`).
4. Otherwise return `CommitTxResponse { commit_micros }`.

### AbortTx

**Implementation:** `WriteManager._abort_tx_in_transaction`
(`lib/api/write.py`)

`AbortTx` makes rollback observable: after `AbortTx`, a subsequent
`CommitTx` on the same `tx_uuid` fails with `FAILED_PRECONDITION`
instead of silently materializing minutes later. The TTL on
`begin_tx_log` is the fallback for crashed clients that never reach
`AbortTx`.

Runs inside a Postgres transaction:

1. Look up the begin record from `begin_tx_log` to validate the
   `tx_uuid` and resolve `branch_uuid`. If absent, raise
   `NotFoundError`.
2. Single-statement guard + idempotent insert against the per-branch
   `abort_tx_log` partition:
   ```sql
   WITH already_committed AS (
     SELECT EXISTS (
       SELECT 1 FROM {commit_tx_log_partition}
       WHERE tx_uuid = %s AND branch_uuid = %s
     ) AS v
   ), ins AS (
     INSERT INTO {abort_tx_log_partition} (tx_uuid, branch_uuid)
     SELECT %s, %s FROM already_committed WHERE NOT v
     ON CONFLICT (tx_uuid, branch_uuid) DO NOTHING
   )
   SELECT v FROM already_committed
   ```
3. If `already_committed` is `TRUE`, raise `FailedPreconditionError`.
   Otherwise return `AbortTxResponse` (whether the row was newly
   inserted or already existed — re-abort is a no-op).

## Read path

The read path resolves a table, issues a symmetric per-tier merge query
against hot (Postgres) and cold (DataFusion over Arrow) storage, then
streams the pre-resolved snapshot with an exclusion filter. The result
is a stream of Arrow `RecordBatch`es containing `row_uuid` + user
columns, representing the current committed state of the table.

**Implementation:**

- Python: `QueryManager.read_data` (`lib/api/query.py`) → `merge_read` (`lib/merge/read.py`)
- Rust: `QueryManager::read_data` (`crates/penca-api/src/query/mod.rs`) → `stream_merged` (`crates/penca-merge/src/lib.rs`)

### Overview

Every read targets exactly one branch (defaults to main on the client;
the server requires an explicit `branch_uuid` or `branch_name`). The
`branch_uuid` flows through table resolution and plan generation so
the correct per-branch data tables are read.

```
read_data(request)
  │
  ├── resolve schema_uuid, branch_uuid, table_uuid
  ├── plan(catalog_uuid, table_uuid, branch_uuid) → hot + cold plan
  │     ├── HotStoragePlan   (per-branch upsert/delete log + commit_tx_log partition)
  │     └── ColdStoragePlan
  │           ├── SnapshotPlan (pre-resolved snapshot segments)
  │           └── LogPlan     (persisted upsert/delete + commit_tx_log segments)
  │
  ├── yield empty schema-header batch (user schema)
  │
  └── stream_merged(plan, driver, dl, user_schema, snapshot)
        │
        ├── Phase 1+2: one two-arm resolve per tier (CHA-368)
        │     ├── hot:  Postgres SQL (PgDialect)
        │     └── cold: same SQL run by DataFusion (DfDialect) over Arrow
        │           tables registered from cold log segments via FormatReader
        │     → each tier returns visible upserts (is_delete=false) UNION
        │       winning tombstones (is_delete=true)
        │     → union + dedup by row_uuid (max commit_micros)
        │     → exclusion set = full row_uuid set of the composed resolve
        │       (derived UNFILTERED, before the residual — CHA-142)
        │     → live delta = is_delete=false subset, after the DataFusion
        │       residual (the single user-filter engine, ADR 0023)
        │     → yield live delta batch
        │
        └── Phase 3: stream snapshot segments (one at a time)
              └── exclusion anti-join (in scan SQL, or per batch for ByPlan)
                  + the same DataFusion residual filter
              └── yield filtered batch
```

The schema-header batch is an empty `RecordBatch` with the table's user
schema, yielded before the merge stream begins. Clients can always
recover the schema from `Table::from_batches(...)` even when the table
is empty. `audit_data` emits the symmetric header with
`audit_output_schema(user_schema)`.

### Plan resolution

**Implementation:** `QueryManager.plan` (`crates/penca-api/src/query/meta_plan.rs`)

The plan tells the query engine where to find data:

- **Hot storage plan** (`HotStoragePlan`): Postgres table names for the
  per-branch upsert and delete logs plus the branch's `commit_tx_log`
  partition.
- **Cold storage plan** (`ColdStoragePlan`): two sub-plans.
  - `SnapshotPlan`: snapshot segments + `snapshotted_at_micros` (the
    `committed_at` of the latest tx in the snapshot baseline).
  - `LogPlan`: upsert/delete + commit_tx_log segments +
    `persisted_at_micros` (the `committed_at` of the latest tx persisted to
    cold).

The plan also carries a `committed_at` bounds pair (`min`, `max`) used
by the merge queries to clip committed transactions to the time range
relevant to each tier:

- Hot: `min = purged_at_micros` — the highest watermark that has been
  purged from hot for T. Anything `> purged_at_micros` is still in
  hot. Between `Persist(T)` and `Purge(T)` the rows exist in both tiers;
  hot's `min` keeps them visible from hot, and the merge layer's
  per-`row_uuid` latest-commit dedup collapses the temporary double
  presence into one visible row (per
  [ADR 0018](decisions/0018-purge-as-hot-cold-visibility-cutoff.md)).
- Cold: `min = snapshotted_at_micros` (the snapshot baseline already
  resolved everything before that time); `max = persisted_at_micros`
  (cold cannot have anything newer than what persist wrote).

A caller-supplied `as_of_micros` tightens `max` on both tiers for
point-in-time reads.

### Visibility (`ReadSnapshot`)

The merge SQL emits a visibility predicate over `commit_micros`
and `tx_uuid` driven by a `ReadSnapshot` value passed into `stream_merged`.
Three variants:

| Variant | Triggered by | Predicate |
| --- | --- | --- |
| `Latest` | (default) `as_of_micros` and `open_tx_uuid` both unset | no upper bound — read all committed |
| `AsOfMicros(ts)` | `ReadDataRequest.as_of_micros` set | `commit_micros <= ts` (point-in-time) |
| `OpenTx { began_at_micros, tx_uuid }` | `ReadDataRequest.open_tx_uuid` set; servicer resolves `(branch_uuid, began_at_micros)` from `begin_tx_log` | `(commit_micros < began_at_micros) OR (tx_uuid = open_tx_uuid)` |

`OpenTx` is read-your-own-writes for an open transaction (CHA-165). The
strict `<` excludes other txs that committed *after* this tx's BEGIN
(snapshot isolation, no non-repeatable reads inside a tx). The OR'd
`tx_uuid` clause picks up this tx's own uncommitted upserts/deletes
from the data logs (where `commit_micros IS NULL`). `as_of_micros`
and `open_tx_uuid` are mutually exclusive on `ReadDataRequest` —
mixing them is incoherent (RYOW into a different point-in-time view
where the table or schema may not exist yet).

Cold tier degenerates: by the CHA-103 invariant, only committed txs
are persisted to cold, so the OR'd `tx_uuid = open_tx_uuid` clause never
matches a cold row. Cold-tier visibility under `OpenTx` reduces to
`commit_micros < began_at_micros` plus the existing abort-set
subtraction. No cold-side code change.

The servicer resolves `open_tx_uuid` once per `ReadData` call via a
single `begin_tx_log` PK lookup; missing/wrong-branch returns
`NOT_FOUND` / `FAILED_PRECONDITION`. Branch must match the request's
branch — symmetric with the no-default-branch invariant.

Cold storage plans are populated from `table_persist_segment_metadata`
(queried per `hot_storage_table_name`) and
`table_snapshot_segment_metadata`.
Only segments with `commit_micros IS NOT NULL` are included —
this filters out incomplete persists from crashed or in-progress
operations (see [Persist](#persist) below).

### Symmetric per-tier merge

The merge algorithm is encoded twice, in SQL — once for hot, once for
cold — in matching builders:

- Hot: `penca_merge::sql::build_merge_resolved`. JOINs `upsert_log` /
  `delete_log` against the hot `commit_tx_log` partition to recover
  `commit_micros` and `commit_seq_num` for each row.
- Cold: `penca_merge::sql::build_cold_merge_resolved` (CHA-218). Cold
  rows already carry `commit_micros` inline (denormalized at persist time per
  [ADR 0017](decisions/0017-cold-data-segments-pre-joined-tx-metadata.md)),
  so there is no JOIN — cold reads are a pure scan.

Both pairs share the same "latest row per partition" mechanism,
delegated to `Dialect::latest_per_partition`:

- `PgDialect` → `SELECT DISTINCT ON (row_uuid) ... ORDER BY row_uuid,
  commit_seq_num DESC, write_seq_num DESC` (fastest on Postgres).
- `DfDialect` → `ROW_NUMBER() OVER (PARTITION BY row_uuid ORDER BY
  commit_seq_num DESC, write_seq_num DESC)` wrapped with `WHERE rn = 1`
  (DataFusion has no `DISTINCT ON`).

**Two-arm resolve (CHA-368).** Each tier's builder emits a *single*
query whose output is `row_uuid, <user_cols>, commit_micros, is_delete`
— a `UNION ALL` of two arms:

- **visible upserts** (`is_delete = false`): the latest committed upsert
  per `row_uuid` that no newer tombstone shadows (user columns valued);
- **winning tombstones** (`is_delete = true`): the `row_uuid`s whose
  latest committed write is a delete (user columns NULL — never emitted;
  the row_uuid only feeds the exclusion set).

The two arms are mutually exclusive and exhaustive per `row_uuid`, so each
touched `row_uuid` appears exactly once. "Latest" and "shadowed by a
tombstone" are decided by the composite commit-order key
`(commit_seq_num, write_seq_num)` (CHA-243 / CHA-429 / CHA-431), which also
handles delete-then-reinsert: a row deleted then reinserted survives
because its reinsert out-orders the delete. See `build_merge_resolved` /
`build_cold_merge_resolved` for the authoritative SQL — the composite
tiebreaker predicate is spelled out lexicographically for DataFusion
portability, and the hot builder JOINs `commit_tx_log` while the cold one
reads `commit_micros`/`commit_seq_num` inline.

Phases 1+2 run this resolve once per tier, concurrently. The two
`RecordBatch`es are unioned and deduped by `row_uuid` (max `commit_micros`
wins across the tier boundary — the tiers partition strictly on
`commit_micros`, so no inversion straddles the boundary). From the composed
batch:

- the **exclusion set** is the full `row_uuid` set — every touched row
  shadows a same-uuid snapshot row. It is derived here, from the
  **unfiltered** resolve, *before* the user filter (CHA-142), so a current
  version that fails the filter can never let a stale snapshot version
  resurface. (Every `row_uuid` ever written or tombstoned in the window
  participates; there is no anti-join against a separate `insert_log` under
  the unified `upsert_log` —
  [decisions/0001-unified-upsert-log.md](decisions/0001-unified-upsert-log.md).)
- the **live delta** is the `is_delete = false` subset, emitted after the
  DataFusion residual (the single user-filter engine — CHA-368 /
  [ADR 0023](decisions/0023-single-query-execution-engine.md)) trims it to
  the user predicate.

This retires the separate "Query B" exclusion probe that earlier ran a
`SELECT DISTINCT row_uuid` union per tier: the exclusion set now falls out
of the same scan that produces the delta.

### Snapshot stream

Phase 3 streams each `SnapshotPlan.segment` via `penca-dl`'s cached
per-segment reads (`FormatReader::read_segment`, projected to the user
schema).
Before the read loop, the user filter is parsed once via
`SessionContext::parse_sql_expr` against the user schema and fed to
`penca_dl::stats::prune_segments_by_stats` (DataFusion
`PruningPredicate`-based) to trim segments whose min/max stats can't
match the predicate (CHA-82). The unpruned set is then read with
bounded concurrency. Per `RecordBatch`, a vectorized `row_uuid`
membership check against the exclusion set drops shadowed rows.
Surviving rows are yielded directly — there is no final union or
materialization.

Snapshot pruning is **snapshot-tier only**. Per
[decisions/0022-no-persist-segment-pruning.md](decisions/0022-no-persist-segment-pruning.md),
persist segments carry stats (for the `TableProvider::statistics()`
CBO aggregate — see below) but are never pruned by user filter:
doing so would corrupt the exclusion-set query inside the same
`SessionContext` and let stale snapshot rows leak through.

Row-level filtering is **not** done by the format readers at all:
per [decisions/0023-single-query-execution-engine.md](decisions/0023-single-query-execution-engine.md),
the readers return every row in the slice and DataFusion applies the
predicate (the CHA-353 residual). Only the coarse segment-level
min/max pruning above runs ahead of the read.

The memory ceiling is one snapshot segment plus the two resolved
batches and the exclusion set. Since each segment is sized for a
single file read and the resolved batches are bounded by the volume
of committed mutations since the snapshot baseline, memory stays
bounded regardless of total table size.

### Cold-tier execution

Cold persist segments are registered as `TableProvider`s in a
DataFusion `SessionContext` and queried via
`ctx.sql(build_merge_resolved::<DfDialect>(...))`.

- Rust: `penca_dl::driver::DatafusionDlDriver` registers `upsert_log`,
  `delete_log`, and `commit_tx_log` datasets backed by a `FormatReader`
  (Parquet or Lance) with projection pushdown. Persist-side filter
  pushdown stays `TableProviderFilterPushDown::Unsupported` (ADR 0022).
  `PersistTableProvider::statistics()` does expose a
  `Precision::Inexact` table-level aggregate (row counts + per-column
  min/max + null_count via `penca_dl::stats::aggregate_table_statistics`)
  for DataFusion's CBO cardinality estimation in join planning.

Hot and cold use the same shared SQL via the `Dialect` abstraction.

### Audit data

**Implementation:** `QueryManager.audit_data` (`lib/api/query.py`,
`crates/penca-api/src/query.rs`)

Unlike `read_data` (which resolves to the latest committed version),
`audit_data` returns every committed row version and every tombstone,
joined with transaction metadata. It runs two direct Postgres JOINs
rather than the merge pipeline, one per output channel:

```sql
-- Upsert channel (AuditDataResponse.upserts)
SELECT u.row_uuid, <user_cols>, u.tx_uuid, t.began_at_micros,
       t.commit_micros, t.comment, t.author
FROM upsert_log u
INNER JOIN commit_tx_log t ON u.tx_uuid = t.tx_uuid
WHERE t.branch_uuid = $1 [AND optional timestamp filters]
ORDER BY t.commit_micros, u.row_uuid;

-- Delete channel (AuditDataResponse.deletes)
SELECT d.row_uuid, d.tx_uuid, t.began_at_micros,
       t.commit_micros, t.comment, t.author
FROM delete_log d
INNER JOIN commit_tx_log t ON d.tx_uuid = t.tx_uuid
WHERE t.branch_uuid = $1 [AND optional timestamp filters]
ORDER BY t.commit_micros, d.row_uuid;
```

Each channel is streamed via its own server-side cursor and yielded
as `RecordBatch` chunks. Empty schema-header batches on both sides
(shapes `audit_upsert_schema(user_schema)` and
`audit_delete_schema()`) are emitted first so clients can always
recover the schema — even when one or both sides produce no rows.
The Python client materializes both streams and returns
`tuple[upserts_table, deletes_table]`; this is a breaking change
from the previous single-table return (see
[decisions/0001-unified-upsert-log.md](decisions/0001-unified-upsert-log.md)).
Deletes in the audit trail are net-new: the prior audit shape
excluded tombstones entirely.

## Merge branch

**Implementation:**

- Python: `WriteManager.merge_branch` (`lib/api/write.py`)
- Rust: `WriteManager::merge_branch` (`crates/penca-api/src/write.rs`)

Merges compact the source branch's post-fork activity into a single
**merge transaction** on the target branch. The entire operation runs
inside one Postgres transaction, so either the whole merge commits
atomically or nothing does.

### Point-in-time coherence

The merge produces *exactly one* new transaction on target — the
`merge_tx` — with a single `commit_micros`. Source's
per-transaction history is not copied; source tx_uuids never appear in
target's `commit_tx_log`.

Consequences for `read_data` with `as_of_micros = t`:

- `t < merge_tx.commit_micros` → target reads see pre-merge
  target state only. No source intermediate states leak through.
- `t ≥ merge_tx.commit_micros` → target reads see the fully
  merged state.

This trades off source-history reconstructability from target (it's
lost) for PIT coherence on target. The merge appears as a single,
discrete commit rather than a backfill of source's timeline.

### Fast-forward only (for now)

Under the unified `upsert_log`, the classification invariant is
simpler than it used to be: every `row_uuid`'s final state on source
is either "alive" (latest upsert beats tombstone) or "dead"
(tombstone wins). Alive rows land in `target.upsert_log`; dead rows
land in `target.delete_log`. Same-row edits on both branches still
cannot be reconciled without conflict detection, so the compaction
runs fast-forward-only.

```sql
-- ensure_fast_forward guard
SELECT 1 FROM {schema_commit_tx_log} t
WHERE t.branch_uuid = target_branch_uuid
  AND t.commit_seq_num > source_branch.fork_commit_seq_num
LIMIT 1;
```

If the guard trips, the merge fails with `InvalidRequestError`.
`TODO(CHA-5)` tracks non-FF conflict detection (classifying
same-row insert/insert, update/update, insert/delete, etc.).

### Algorithm

**Step 1: Lock source commit_tx_log partition** — `LOCK TABLE ... IN EXCLUSIVE MODE`
on the source branch's commit_tx_log partition. Serializes against concurrent
commits on source; allows reads. Merge-vs-merge on the same source
also serializes here.

**Step 2: Fast-forward guard** — as above.

**Step 3: Create merge transaction on target.** Written straight to
`commit_tx_log` (no begin_tx_log, no pending state):

```sql
INSERT INTO {target_commit_tx_log_partition}
  (tx_uuid, branch_uuid, began_at_micros, comment, author)
VALUES ($merge_tx_uuid, $target_branch_uuid, now_micros(), ...);
-- commit_micros from column DEFAULT
```

**Step 4: Per-table compaction.** For each table in the schema, the
merge emits *two* `INSERT ... SELECT`s against the target's
per-branch logs. Both share a `WITH` block of CTEs that resolve
source's committed state (Postgres `WITH` is statement-scoped, so the
CTE bodies are inlined into each INSERT; the cost is small compared
to the log scans themselves).

Shared CTEs:

```sql
WITH source_committed_tx AS (
    SELECT tx_uuid, commit_micros FROM {source_commit_tx_log_partition}
    WHERE commit_micros > 0
),
source_upserts_joined AS (
    SELECT u.row_uuid, u.<user_cols>, c.commit_micros
    FROM {source_upsert_log} u
    JOIN source_committed_tx c USING (tx_uuid)
),
latest_upserts AS ( /* dialect latest_per_partition on row_uuid, commit_micros DESC */ ),
source_deletes AS (
    SELECT d.row_uuid, MAX(c.commit_micros) AS deleted_at
    FROM {source_delete_log} d JOIN source_committed_tx c USING (tx_uuid)
    GROUP BY d.row_uuid
)
```

Routing rule for each source-side row outcome:

| source-side outcome (`final_alive` = latest upsert beats tombstone; `final_dead` = tombstone wins) | Target log |
|---|---|
| `final_alive` | `upsert_log` |
| `final_dead`  | `delete_log` |

The two `INSERT`s:

```sql
-- 1. alive upserts → target.upsert_log
INSERT INTO {target_upsert_log} (version_uuid, row_uuid, tx_uuid, <user_cols>)
SELECT gen_random_uuid(), l.row_uuid, $merge_tx_uuid, l.<user_cols>
FROM latest_upserts l
LEFT JOIN source_deletes d USING (row_uuid)
WHERE d.deleted_at IS NULL OR l.commit_micros > d.deleted_at;

-- 2. winning tombstones → target.delete_log
INSERT INTO {target_delete_log} (row_uuid, tx_uuid)
SELECT d.row_uuid, $merge_tx_uuid
FROM source_deletes d
LEFT JOIN latest_upserts l USING (row_uuid)
WHERE l.commit_micros IS NULL OR d.deleted_at > l.commit_micros;
```

Both INSERTs carry the single `$merge_tx_uuid` and land at
`merge_tx.commit_micros`. `row_uuid` values are stable across
branches because they derive from `table_uuid` (not
physical), so the same primary key maps to the same `row_uuid` on
both source and target. Storage-shape rationale: see
[decisions/0001-unified-upsert-log.md](decisions/0001-unified-upsert-log.md).

### What happens to source's transaction history

Source `tx_uuid`s are not copied to target — the per-row rewrite in
`merge_table_data` stamps every target row with the single merge
`tx_uuid`. Source's `commit_tx_log` partition stays intact on the source
branch (which is still queryable).

Tx framing is internal post-CHA-222 (see
[decisions/0018-tx-is-internal-no-introspection-rpc.md](decisions/0018-tx-is-internal-no-introspection-rpc.md));
the user-visible artifact of a merge is the
`MergeBranchResponse.commit_micros` plus the per-row audit
metadata that `AuditData` surfaces on target (pre-joined onto every
row at persist time — `commit_micros`, `began_at_micros`,
`comment`, `author` — per
[ADR 0017](decisions/0017-cold-data-segments-pre-joined-tx-metadata.md)).
A merge-provenance lookup is therefore an `AuditData` call against
target windowed on the merge's `commit_micros`.

## Persist (hot → cold)

**Implementation:** `LifecycleManager::persist_locked`
(`crates/penca-api/src/lifecycle.rs`)

Persist moves committed data for a single table from Postgres (hot
storage) to object storage (cold storage). Post-CHA-220 the operation
is **per-table**: `persist(catalog, schema, branch, table)` advances
that table's `persisted_at_micros` watermark and writes T's hot rows to
cold. It does **not** delete hot rows — those move to
[Purge](#purge-hot-rows--watermark). Queries continue to serve T's
data from hot until Purge runs; the hot/cold visibility cutoff used
by `plan()` is `purged_at_micros`, not `persisted_at_micros`
(see [ADR 0018](decisions/0018-purge-as-hot-cold-visibility-cutoff.md)).

CHA-218 made `commit_tx_log` hot-only. Each cold upsert/delete segment row
carries the four denormalized tx metadata columns
(`commit_micros, began_at_micros, comment, author`) inline,
pre-joined at persist time from the hot `commit_tx_log` partition. Cold reads
are a pure scan — no JOIN — and there is no `log_kind = 'commit_tx_log'`
artifact on cold. See
[ADR 0017](decisions/0017-cold-data-segments-pre-joined-tx-metadata.md).

### Identifier resolution

`PersistRequest` carries a per-table identifier block: catalog + schema
+ branch + table (name-based, UUID-based, or mixed). An optional
`target_micros` lets callers cap the watermark (e.g. for tests);
production callers omit it and the server uses `now()`.

Lock key: `persist:{table_uuid}:{branch_uuid}`. Serializes
`Persist(T)` against `Persist(T)` only — cross-operation pairs
(`Persist↔Snapshot`, `Persist↔Purge`) take separate keys under ADR
0019 §"Lock scoping" and run lock-free. Persists on different
tables run in parallel.

### Two-level commit-decoupled algorithm

```
TABLE_PERSIST_METADATA            (1 row per (T, persist event, log_kind);
                                  log_kind ∈ {upsert_log, delete_log})
   ↳ TABLE_PERSIST_SEGMENT_METADATA  (N rows per table_persist)
```

Each level has its own `commit_micros` flag. A row is visible
to the read path only when its row + every ancestor are committed,
which lets the algorithm commit work incrementally without sacrificing
crash safety.

```
persist(catalog, schema, branch, table, target_micros=None)
  │
  ├── 1. Resolve catalog_uuid, schema_uuid, branch_uuid, table_uuid.
  │      effective_target = min(target_micros ?? now(),
  │                             oldest_open_began_at(branch) - 1).
  │      The open-tx clamp guarantees cold's max committed_at <
  │      every open tx's began_at by construction (open txs live on
  │      begin_tx_log[branch] — a cheap branch-scoped read).
  │
  ├── Phase 1 — durable writes (no Pg tx):
  │     ├── Hot read: SELECT u.*, t.{committed_at, began_at,
  │     │   comment, author} FROM upsert_log u JOIN commit_tx_log t
  │     │   ON u.tx_uuid = t.tx_uuid WHERE row_uuid bound for T
  │     │   AND committed_at <= effective_target (and the symmetric
  │     │   delete read). The four tx metadata columns are
  │     │   denormalized onto every projected row right here — the
  │     │   only place in the system where the JOIN runs against
  │     │   cold-bound data.
  │     ├── Project to cold layout: drop (version_uuid, tx_uuid),
  │     │   keep (row_uuid, <user_cols>, committed_at, began_at,
  │     │   comment, author). Same projection for deletes
  │     │   (without <user_cols>).
  │     ├── For each non-empty kind (upsert, delete):
  │     │     ├── INSERT table_persist_metadata (committed = NULL)
  │     │     ├── INSERT table_persist_segment_metadata (committed = NULL)
  │     │     ├── Write {hot_name}/{segment_uuid}.parquet
  │     │     └── UPDATE segment SET committed_at = now()
  │     └── On per-kind failure: per-kind cleanup; the other kind
  │         (and any prior committed persist event on T) is unaffected.
  │
  └── Phase 2 — parent flips (one Pg tx):
        └── UPDATE the (at most two) table_persist rows for this event
            SET committed_at = now().

Returns PersistResponse { persisted_at_micros = stamp } where
stamp = max(
    max(committed_at over committed rows being persisted),
    max(aborted_at over aborted txs whose hot rows Persist
        just cleaned)
).
Unset (None) when there's nothing committed AND no aborted hot
rows to clean past effective_target, OR when the strict-advance
gate trips (stamp <= last_persisted_at(T)).
```

**Stamping rule (post-CHA-227, post-CHA-221 v2.1 / ADR 0021):**
`persisted_at_micros` is `max(committed_at, aborted_at)` over the
rows Persist just handled — the broadened rule moves aborted hot
cleanup from "never happens" to "happens in Persist's existing
phase-2 tx" (see [ADR 0021](decisions/0021-persist-owns-aborted-hot-cleanup.md)).
Persist's open-tx invariant `persisted_at < every open tx's
began_at` survives transitively because every `committed_at <=
effective_target`, every `aborted_at <= effective_target`, and
`effective_target < oldest_open_began_at - 1` by Persist's existing
clamp.

**Aborted hot cleanup (v2.1):** Persist reads aborted tx_uuids
that wrote to `T` (join `abort_tx_log[B] ⋈ tx_table_log[B]` on
`tx_uuid`, filter `table_uuid = T` and `aborted_at_micros <=
effective_target_micros`), then DELETEs those tx_uuids' rows
from `upsert_log[T,B]` / `delete_log[T,B]` inside the phase-2 tx.
Idempotent — re-running matches no rows after the first cleanup.

**All-aborts case:** If there are aborted hot rows to clean but no
committed rows to persist past the clamp, Persist still writes a
single `table_persist_metadata` row (no segments) so
`persisted_at(T)` advances and downstream consumers (Purge,
`ListPersistedTables`, `PurgeTxLog`) see the new watermark.
`log_kind = upsert_log` is conventional in this case — the kind
only affects the deterministic `table_persist_uuid` hash; no
segments are written.

**Strict-advance no-op gate (v2.1):** Persist reads
`last_persisted_at(T)` and no-ops when the computed stamp
wouldn't strictly advance. Prevents redundant
`table_persist_metadata` rows on re-Persist with unchanged hot
state.

**Per-segment commits in Phase 1.** Each
`table_persist_segment_metadata` row flips `committed_at` immediately
after its file write succeeds, before the parent table_persist flips.
Visibility still gates on the parent commits, but the eager segment
flip means a recovery sweep doesn't have to re-derive "which files
are durable on disk" from scratch.

**What Phase 2 no longer does.** Pre-CHA-220 Phase 2 also deleted
hot upsert/delete log rows and the four hot tx-log-family rows
(`commit_tx_log`, `abort_tx_log`, `begin_tx_log`, `tx_table_log`) past
`effective_target`. Hot upsert/delete deletes now live in
[Purge](#purge-hot-rows--watermark); hot tx-log family GC is the
branch-scoped [PurgeTxLog](#purge-tx-log-branch-scoped) pass
([CHA-221](https://linear.app/chapala/issue/CHA-221)).

**Join-before-purge ordering (CHA-218).** Still load-bearing: Phase 1
copies every committed tx's metadata onto the cold segments it
writes, so when Purge later deletes the corresponding hot rows the
information is already preserved on cold.

### Cold-tolerant system-metadata reads

The `__penca_system__.{schemas,tables}` tables persist like any other
user table — create_schema / create_table / update_table commit_tx_log
entries that reference those rows can land in cold and reads must
tolerate the post-persist+purge state where the row's hot upsert/delete
entries are gone.

`QueryManager::resolve_table_metadata` and
`resolve_schema_metadata` (`crates/penca-storage-meta/src/lib.rs`)
route through `stream_merged` against the sys-table's
`data_log_prefix(sys_table_uuid, branch)` — the same hot+cold merge
machinery as user data reads. Cold visibility is read directly from
the `commit_micros` column on each cold row (CHA-218); no JOIN
against cold commit_tx_log is needed.

### Crash recovery

Selective recovery from a mid-persist crash is tracked separately as
**CHA-197**. The commit decoupling that lands here enables the sweep
— crashed persists leave a tree of uncommitted rows and the recovery
walk visits each level top-down, rolling forward segments whose files
landed and rolling back ones that didn't, then closing or leaving the
parent table_persist rows as appropriate. Until that ships, uncommitted
rows are correctly invisible to reads (parent gating) but cleanup
debt accumulates on the next persist. Phase-1 retries with identical
inputs replay to the same deterministic `table_persist_uuid` and slot
into the same row via `ON CONFLICT DO UPDATE` as a no-op write.

## Purge (hot rows + watermark)

**Implementation:** `LifecycleManager::purge_locked`
(`crates/penca-api/src/lifecycle.rs`)

Purge deletes T's hot upsert/delete log rows up to T's persist
watermark and advances `table_purge_metadata(T).purged_at_micros` —
the hot/cold visibility cutoff used by `plan()`. It is the visibility
flip event: between `Persist(T)` and `Purge(T)` the rows exist in both
tiers and queries serve from hot; after `Purge(T)` they serve from
cold.

```
purge(catalog, schema, branch, table)
  │
  ├── 1. Resolve catalog_uuid, schema_uuid, branch_uuid, table_uuid.
  │      Lock key: purge:{table_uuid}:{branch_uuid} (serializes
  │      Purge↔Purge on T only — Persist and Snapshot take their
  │      own keys; ADR 0019 §"Lock scoping").
  │
  ├── 2. Read persisted_at = latest_committed_table_persist_watermark(T)
  │      and last_purged = latest_committed_table_purge_watermark(T).
  │
  ├── 3. real_purge = persisted_at.is_some()
  │                   AND persisted_at > last_purged
  │
  ├── Phase 1 (one Pg row insert):
  │      purged_at_micros =
  │          if real_purge then persisted_at
  │          else max(now(), last_purged + 1)
  │      INSERT table_purge_metadata (committed_at = NULL).
  │
  ├── Phase 2 (one Pg tx):
  │      └── if real_purge:
  │            ├── DELETE FROM {upsert_log[T]} WHERE tx_uuid IN
  │            │     (SELECT tx_uuid FROM {commit_tx_log[branch]}
  │            │       WHERE commit_micros <= $purged_at)
  │            ├── DELETE FROM {delete_log[T]} (symmetric)
  │            └── (no-op branch skips both deletes)
  │      └── UPDATE table_purge_metadata SET committed_at = now().
  │
  └── On Phase 2 error: delete_uncommitted_table_purge cleanup
      (matches the persist phase-1 cleanup pattern).

Returns PurgeResponse { purged_at_micros }.
```

**Why the no-op fast-path still writes a row.** Every dirty table
needs a defined `purged_at_micros` for downstream branch-min
computations (CHA-221's GC pass, future scheduler bookkeeping). When
there is no committed persist state strictly newer than the last purge,
the watermark advances to `max(now, last_purged + 1)` — strictly
monotone within a branch — so the per-table watermark is always
defined.

**Idempotency.** Two purges on T see the same `persisted_at`. The
deterministic `table_purge_uuid` is keyed on `(catalog, branch,
table, purged_at)`; a replay with identical inputs slots into the
same row via `ON CONFLICT (branch_uuid, table_purge_uuid) DO UPDATE`
as a no-op write.

**Crash recovery.** Mid-purge crash leaves the uncommitted
`table_purge_metadata` row plus the hot rows. The next purge runs
the no-op fast-path or retries the same watermark; the
deterministic UUID slots into the same row and the cleanup
helper (`delete_uncommitted_table_purge`) sweeps any leftover row
from the crash.

## Purge commit_tx_log (branch-scoped)

**Implementation:** `LifecycleManager::purge_tx_log_locked`
(`crates/penca-api/src/lifecycle.rs`); pure helper
`compute_purge_tx_log_cutoffs`
(`crates/penca-storage-meta/src/watermarks.rs`); SQL helpers on
`LifecycleManager` (`crates/penca-storage-meta/src/lib.rs`).

`PurgeTxLog` is the branch-scoped GC for the four hot tx-log family
tables (`commit_tx_log`, `tx_table_log`, `abort_tx_log`, `begin_tx_log`).
Per-table Persist + Purge does not touch these — they're shared
across the branch, so the GC pass runs branch-scoped and reads each
table's stored `table_purge_metadata.purged_at_micros` to derive a
branch-wide cutoff.

```
purge_tx_log(catalog, branch)
  │
  ├── Lock: purge_tx_log:{branch_uuid} (branch-scoped; orthogonal
  │   to per-table persist:/snapshot:/purge: keys, ADR 0019).
  │
  ├── 1. cleanup_started_at_micros = LifecycleManager::now_micros(pool)
  │
  ├── 2. Read S = distinct tables in tx_table_log[B] whose writer tx
  │      is settled by the snapshot — i.e. inner-joined against
  │      (commit_tx_log[B] WHERE commit_micros <= cleanup_started_at)
  │      ∪ (abort_tx_log[B] WHERE aborted_at_micros <=
  │      cleanup_started_at) on tx_uuid. Then left-joined with each
  │      table's MAX(table_purge_metadata.purged_at_micros)
  │      WHERE commit_micros IS NOT NULL
  │      AND commit_micros <= cleanup_started_at_micros
  │      (the as-of filter — load-bearing per ADR 0021). LEFT JOIN,
  │      so absent purge rows surface as NULL → Option<i64>::None.
  │      One SQL round-trip.
  │
  ├── 3. compute_purge_tx_log_cutoffs(S):
  │        max_micros = if S.is_empty() then None
  │                     else Some(1 + min(purged_at over S,
  │                                       treating None as 0))
  │      Empty-S short-circuits: response = { purged_at_micros: None }.
  │      The cleanup_started_at_micros clamp is NOT a parameter to
  │      this helper under v2.1 — it's already baked into the
  │      as-of filter at step 2, so max_micros - 1 ≤
  │      cleanup_started_at_micros by construction.
  │
  └── 4. cutoff = max_micros - 1.
        Single composite SQL — WITH-CTE eligibility set + four
        deletes against that fixed set:

        WITH eligible AS (
          SELECT tx_uuid FROM commit_tx_log[B]
            WHERE commit_micros <= cutoff
          UNION ALL
          SELECT a.tx_uuid FROM abort_tx_log[B] a
            WHERE a.aborted_at_micros <= cutoff
               OR (
                 -- pure-begin+abort fast-path: tx has no writes
                 -- anywhere, so no hot-row / purged_at dependency.
                 -- Safety bound is just "abort is in our snapshot".
                 a.aborted_at_micros <= cleanup_started_at_micros
                 AND NOT EXISTS (
                   SELECT 1 FROM tx_table_log[B] t WHERE t.tx_uuid = a.tx_uuid
                 )
               )
        ),
        d_commit_tx_log     AS (DELETE FROM commit_tx_log[B]       WHERE tx_uuid IN eligible),
        d_tx_table   AS (DELETE FROM tx_table_log[B] WHERE tx_uuid IN eligible),
        d_abort      AS (DELETE FROM abort_tx_log[B] WHERE tx_uuid IN eligible)
        DELETE FROM begin_tx_log[B] WHERE tx_uuid IN eligible;

Returns PurgeTxLogResponse { purged_at_micros: Some(cutoff) }.
```

**Why the table set is `tx_table_log`, not "all branch tables".**
A table absent from `tx_table_log[B]` has no `commit_tx_log` dependency on
this branch — any hot rows it might have are from already-GC'd txs,
and those could only have been GC'd if their hot rows were already
purged. So empty / never-written tables drop out automatically; they
don't pin `max_micros` at 0 the way a naive "min over all branch
tables" reading would. Fully-settled-and-GC'd tables also drop out
once their `tx_table_log` entries clear, so the GC liveness
self-heals — no need for a separate "stale-table fast-advance"
mechanism (rejected in the ticket §Out-of-scope on liveness
grounds).

**Why the `tx_table_log` set is filtered through `commit_tx_log[B] ∪
abort_tx_log[B]` (v2.1).** An in-flight open tx that has called
`write_data` on table T inserts a `tx_table_log[B]` row at mutate
time, before any commit/abort decision. Without the settled-tx
filter, T would enter S with `purged_at = 0` (Persist couldn't have
advanced it — the open-tx clamp on `persisted_at` keeps it strictly
below `oldest_open_began_at`, ADR 0019), pinning `cutoff = MIN(purged_at
over S) = 0` and starving the GC of progress on every other table
in the branch until the open writer settles. Filtering S to tables
touched by *settled* txs (committed in `commit_tx_log` OR aborted in
`abort_tx_log`, both timestamps `<= cleanup_started_at`) lets
`cutoff` advance over in-flight writers on unrelated tables. The
filter shares its `cleanup_started_at` bound with the
`table_purge_metadata` as-of filter below so both halves see a
consistent snapshot.

**Why the watermark MUST be the stored `purged_at`, not derived from
`persisted_at`.** ADR 0019 §"Reading the watermark" — consumers read
`table_purge_metadata.purged_at_micros` directly. Substituting a
`MAX(persisted_at) WHERE now - committed_at > query_timeout`
derivation looks equivalent but reads `persisted_at`, which is the
exact conflation the rule forbids: it would let the GC advance past
hot rows the universal grace window is still protecting, breaking
the live-query safety chain.

**The as-of filter on `table_purge_metadata.commit_micros`
(v2.1).** Load-bearing — replaces v1's explicit
`cleanup_started_at_micros` clamp on the DELETE. The filter
restricts step 2's purged_at view to rows whose phase-2 commit
landed at or before `cleanup_started_at_micros`. Concurrent
`Purge(T)` cycles that commit *during* PurgeTxLog's SQL execution
are invisible to PurgeTxLog's view of `purged_at(T)`, so their
late watermark advance can't pull `MIN(purged_at over S)` past a
tx that committed after the cleanup pass started. The cutoff
threaded into the composite DELETE (`max_micros - 1`) is bounded
by `cleanup_started_at_micros - grace` by construction — no
separate clamp parameter needed on the DELETE itself. See
[ADR 0021](decisions/0021-persist-owns-aborted-hot-cleanup.md)
§"Long-cleanup-race" for the full safety chain.

**Open-tx safety via the eligibility set.** This implementation
deviates from the ticket's literal "step 5–8 with chained `NOT IN`"
shape, which would delete the `tx_table_log` (step 6) and
`begin_tx_log` (step 8) rows of any in-flight tx that committed
concurrently with the cleanup pass: an open tx with writes has a
`tx_table_log` entry but no `commit_tx_log` or `abort_tx_log` row at
statement start, so it satisfies step 6's `NOT IN (commit_tx_log ∪
abort_tx_log)`. The next CommitTx would fail to find its
`begin_tx_log` row.

The composite-SQL version instead computes an `eligible` CTE of
*settled-and-GC-eligible* txs upfront (tx_uuid IN commit_tx_log[B] OR
tx_uuid IN abort_tx_log[B], gated on cutoff), then deletes from
all four tables for that fixed set. Open txs are never in
`eligible` (they're not in `commit_tx_log` and not in `abort_tx_log` at
statement start) ⇒ their `begin_tx_log` / `tx_table_log` rows are
preserved unconditionally. PG's `WITH ... DELETE` snapshot
semantics guarantee the four sub-deletes all match against the
same pre-statement view, so the chained-NOT-IN ordering concern
disappears.

**Aborted-with-writes txs ARE GC'd under v2.1 (was a leak in
v1).** v1's eligibility CTE gated the aborted half on
`tx_uuid NOT IN tx_table_log`, which created a chicken-and-egg:
aborted-with-writes txs had `tx_table_log` entries that nothing
ever cleared, so their `abort_tx_log` row was never eligible.
v2.1 drops the `NOT IN tx_table_log` clause — safe because
Persist (ADR 0021) now cleans aborted hot rows and advances
`purged_at(T)` past the abort. By the time an aborted X enters
`eligible` here (`X.aborted_at <= cutoff <= purged_at(T_X)` for
every T_X X wrote to), X's hot rows in every T_X are gone. No
chicken-and-egg, no leak.

**Liveness.** `max_micros` advances at the pace of the slowest-
Purged table currently in `tx_table_log[B]`. A written-but-never-
Persisted table pins `max_micros = 1` (no DELETE matches
`committed_at < 1`) — correct: hot rows still exist for those txs,
so their `commit_tx_log` entries must be preserved. Under the scheduler
([CHA-154](https://linear.app/chapala/issue/CHA-154)), every table
with hot data gets Persisted+Purged on a periodic cadence, so the
GC advances naturally. "commit_tx_log accumulates forever" only happens
when a table is written-but-never-Persisted indefinitely — an
operator-detectable failure mode.

**Scheduler integration.** The
[lifecycle scheduler](services/lifecycle-scheduler.md) invokes
`PurgeTxLog` unconditionally at the end of each `tick_branch`,
after the per-table Persist+Snapshot and Purge loops. The empty-set
fast-path is the no-op gate; no scheduler-side watermark is needed
(unlike `last_modified_tick` / `last_purge_tick`, which guard the
listing round-trips).

## Compact (cold → cold)

**Implementation** (`crates/penca-api/src/lifecycle/`):

- Cycle: `LifecycleManager::compact_persist_segments` (`compact_op.rs`).
- Wave: `compact_one_scope` (`compact.rs`).
- Plan: `plan_wave` (`compact_plan.rs`).

Compact reorganizes cold persist segments — no tier change, no persist
event. N small segments merge into one larger file; each input
segment's metadata row is UPDATEd in place to point at a slice of the
merged file via `(object_uri, offset, length)` (CHA-168). Slice-aware
reads come from `ColdStorageClient::read_persist_segments`, which
honors per-input `offset` + `length` so already-compacted segments
slice cleanly.

**Persist segments only.** Snapshot segment files are immutable —
never compacted (ADR 0024, CHA-407). Snapshot output is born compacted
by the CHA-404 packed write, which addresses one row range per
partition inside each file via the same `(object_uri, offset, length)`
shape.

This section documents the per-scope active+sealed algorithm
introduced in CHA-202.

### Per-scope active+sealed model

A **scope** is the unit a wave operates on: `(branch_uuid,
table_uuid, log_kind)`, with `log_kind` ∈ {`upsert_log`, `delete_log`,
`commit_tx_log`} compacting independently.

`table_persist_segment_metadata` carries an `is_sealed BOOL NOT NULL`
column. Three semantic states:

- `is_sealed = false`, URI unique among the scope's unsealed rows —
  *uncompacted segment* (a fresh persist write).
- `is_sealed = false`, URI shared with ≥1 other unsealed row in the
  scope — *active merged file*. The shared rows are its slices.
- `is_sealed = true` — terminal. Either a previously-sealed merged
  file (no longer the active) or an uncompacted that was sealed in
  place (oversized standalone; see § plan_wave). Sealed rows never
  participate in another wave on this scope.

**Active+sealed invariant.** Among the unsealed rows on a scope, at
most one URI appears in ≥2 rows. That URI is the active merged
file; the rows are its slices. Every other unsealed URI appears in
exactly one row.

The invariant holds inductively across all four wave outcomes
`plan_wave` can produce:

| Outcome         | Prior active | Post-wave unsealed                                                                            |
|-----------------|--------------|-----------------------------------------------------------------------------------------------|
| Fresh extend    | none         | New active (URI shared by ≥2 folded rows) + remaining uncompacted singletons                  |
| Extend          | exists       | New active (URI shared by prior-active slices + folded singletons); prior URI gone            |
| Cascade-seal    | exists, unwritable | Prior active's rows sealed (still pointing at prior URI); new active under the next seed |
| Seal-only       | exists, no folds possible | Prior active's rows sealed; no new active; remaining uncompacted unchanged           |

In every case the post-wave unsealed set has at most one shared URI.

### `plan_wave` in step notation

`plan_wave(rows, max_segment_bytes, uri_of) -> Option<WavePlan>`
(`crates/penca-api/src/lifecycle/compact_plan.rs`):

```
 1. Group rows by URI. Let A be the URI appearing in >1 row, or ∅.
 2. active_indices ← indices of rows whose URI = A, in input order.
 3. uncompacted    ← (0..|rows|) \ active_indices, in input order.
 4. current        ← active_indices
    current_size   ← Σ size_bytes(rows[i]) for i ∈ active_indices
    seal_indices   ← ∅
    folded         ← 0
 5. For each idx in uncompacted, let s ← size_bytes(rows[idx]):
 6.     If current_size + s ≤ max: fold.
 7.     Else if folded ≥ 1 ∧ |current| ≥ 2: break.
 8.     Else if |current| ≥ 1: cascade-seal current; restart at idx.
 9.     Else: seal idx in place (oversized standalone).
10. If folded = 0 ∧ seal_indices = ∅: return None.
11. If |current| < 2 ∧ seal_indices = ∅: return None.
12. If |current| < 2: return WavePlan{∅, seal_indices}.
13. Return WavePlan{current, seal_indices}.
```

The loop terminates two ways:

- **Line-7 break** — `current` is a writable new active (≥2 inputs,
  ≥1 fold). Remaining uncompacted defers to the next wave.
- **End-of-uncompacted** — every candidate got a fold / cascade /
  standalone decision. `current` at end-of-walk is the proposed new
  active; it may be empty (only standalones happened), singleton
  (no folds, only cascade-seals), or ≥2 (a buildable new active).

**State-changing-progress invariant.** Every `Some(plan)` commits at
least one of: a new merged file is produced (`|input_indices| ≥ 2`),
or ≥1 row is sealed. `None` is returned only on a genuine no-op:
empty input, or a pure singleton unsealed set with no prior active.

The line-8 cascade-seal and line-9 standalone-seal arms are what
close the v1/v2 stall mode where a scope with an unwritable prior
active plus a single uncompacted (or with an oversized lone
uncompacted) would keep replanning and producing `None` forever
while the unsealed set never shrank.

### Plan inside the locking tx

`compact_one_scope` (`compact.rs`) executes in this strict order:

```
tx.begin
  ├── LifecycleManager::enumerate_unsealed_*_segments_for_scope(tx, …,
  │     for_update = true)            -- SELECT … WHERE is_sealed = FALSE FOR UPDATE
  ├── plan_wave(rows, max_segment_bytes, |r| r.object_uri)
  │     └── plan == None              → tx.commit; return None
  ├── seal-only (plan.input_indices empty):
  │     seal_table_*_segments_by_uuids(tx, …)
  │                                   → tx.commit; return None
  ├── LifecycleManager::insert_compact_segment(pool, …)
  │                                   -- auto-commit, separate session,
  │                                   -- commit_micros = NULL
  ├── ColdStorageClient::write_*_segment(writer, merged_uri, batch)
  ├── for each input: LifecycleManager::repoint_table_*_segment(tx, …)
  ├── if plan.seal_indices: seal_table_*_segments_by_uuids(tx, …)
  ├── LifecycleManager::commit_compact_segment(tx, …, merged_uri)
  │                                   -- NULL → tx-time micros
tx.commit
  └── for each old uri: ColdStorageClient::delete_segment(writer, …, best_effort)
```

`plan_wave` runs **inside** `tx`, after `SELECT FOR UPDATE` has
row-locked the scope's unsealed rows. Running it outside `tx` would
be a TOCTOU between enumerate and commit: another compact on the
same scope could seal or repoint a row in between, leaving this
wave operating against a stale view of the unsealed set.

**Concurrent compacts on the same scope.** Under PG READ COMMITTED
+ `SELECT FOR UPDATE`: T2's enumerate blocks on T1's row locks;
when T1 commits, T2 unblocks and the `WHERE is_sealed = FALSE`
predicate naturally drops every row T1 just sealed. T2 plans
against the post-T1 state — no extra coordination, no compensating
re-check needed.

**`compact_segment_metadata` lives on the auto-commit `pool`**, not
on `tx`, so the Phase-1 INSERT survives a `tx` rollback. The row's
`commit_micros` stays `NULL` until the in-tx
`commit_compact_segment` UPDATE flips it to tx-time micros — that
UPDATE lives or dies with the merge.

### Wave vs. cycle layering

**Today (CHA-202).** Three layers:

- **Cycle** = one RPC: `compact_persist_segments`. Lists unsealed
  scopes via `LifecycleManager::list_unsealed_persist_scopes_on_table`,
  then iterates one wave per scope serially.
- **Wave** = one call to `compact_one_scope`. One `plan_wave`, one
  merge tx, at most one new merged file produced, plus any seals.
- **Plan** = one `plan_wave` invocation.

So today, one cycle = one wave per scope. The scheduler triggers
cycles on a cadence; a deep backlog drains across multiple cycles.

**Optional future — per-scope drain loop.** Replace the
one-wave-per-scope step inside the cycle with a bounded loop:

```
compact_one_scope(scope):
    loop:
        plan = plan_wave(enumerate(scope))   // each iteration: fresh tx
        if plan is None: return              // stopping condition
        commit(plan)
```

**Stopping condition.** `plan_wave` returns `None` iff (a) the
input is empty, or (b) `|current| < 2 ∧ seal_indices = ∅` — i.e. a
pure sub-threshold singleton with no prior active. By the
state-changing-progress invariant every other state produces a
`Some(plan)`, so the loop strictly shrinks the unsealed set on each
iteration. Terminal state: at most one uncompacted segment left on
the scope (`|current| = 1 ∧ seal_indices = ∅`).

**Trade-offs.** Pros — one RPC fully drains a scope; the caller
doesn't have to schedule repeat cycles. Cons — unbounded wall-time
per RPC; lock churn on the same scope; cross-scope fairness suffers
if scope iteration stays serial. A bounded inner-while
(`max_waves_per_scope = N`) is the natural middle ground.

Not implemented in CHA-202 — flagged as a known future direction.

### Crash recovery

Orphan-tracking lives in **`compact_segment_metadata`** — per-catalog,
`LIST` partitioned by `branch_uuid`
(`crates/penca-db/src/dialect/pg.rs:425`):

```
compact_segment_metadata (
    object_uri           TEXT NOT NULL,
    branch_uuid          UUID NOT NULL,
    table_uuid           UUID NOT NULL,
    commit_micros  BIGINT,           -- NULL = in-flight or aborted
    PRIMARY KEY (branch_uuid, object_uri)
) PARTITION BY LIST (branch_uuid)
```

For `commit_tx_log` compacts, `table_uuid` is the per-catalog system
commit_tx_log table UUID (not a user table); for `upsert_log` /
`delete_log`, it's the user `table_uuid`.

`commit_micros` semantics:

- `NULL` — `insert_compact_segment` happened; the merge tx has not
  committed. Either it's still in flight, or it crashed/rolled back
  and both the row and the merged file are orphans awaiting the
  cleanup sweep.
- non-`NULL` — the merge tx committed; the merged file is
  referenced by `table_*_segment_metadata` rows on the branch.

**Crash between INSERT and `tx.commit`.** The merge tx aborts → no
`table_*_segment_metadata` row repoints; per-input files stay
referenced; the merged file is an orphan on cold storage; the
`compact_segment_metadata` row stays `NULL`. Compact retries on the
same scope are unaffected — the aborted tx released row locks and
left the unsealed set as it was. CHA-49 will sweep `NULL` rows on a
schedule.

**Branch deletion picks up in-flight orphans.**
`WriteManager::delete_branch`
(`crates/penca-api/src/write/mod.rs`) calls
`LifecycleManager::get_compact_segment_uris_for_branch` *in addition
to* the persist/snapshot segment enumerations, so the merged files
behind both committed AND `NULL` rows are queued onto
`segment_delete_set` in the same transaction that drops the
`compact_segment_metadata` rows. Without this branch a
crashed-mid-compact merged file would leak past branch delete: nothing
else names it, so no later enumeration would ever find it.

**Visibility is preserved end-to-end.** No row's
`commit_micros` is ever nulled. Before the merge `tx.commit`,
readers see the original `(uri, NULL offset, NULL length)` layout;
after, every repointed row points at its slice of the merged file.
There is no intermediate state where a committed row is invisible
to `plan()`.

**Related.** CHA-49 — committed-row orphan cleanup routine.
CHA-215 — persist chunking at `max_segment_bytes`. Now that CHA-215
has landed, no fresh persist write can exceed the cap, so the
standalone-seal arm (step 9) is no longer reachable from new writes.
The arm stays in place for pre-CHA-215 oversized rows that may still
live on disk in long-lived environments; once those are folded or
sealed away, it is dead code.

## Branch deletion

**Implementation:** `WriteManager::delete_branch`
(`crates/penca-api/src/write/mod.rs`)

Branch deletion is a **pure metadata operation**: it unlinks nothing itself.
Dropping the branch's segment rows is what makes a file unreferenced, and the
`segment_delete_set` refcount gate decides — past the universal grace window —
whether any other branch still names it.

That indirection is load-bearing (CHA-539). Since CHA-531 a carried row lives in
one branch's partition while its `object_uri` names the file another branch
wrote, so a teardown that unlinked whatever its own enumeration reached would
destroy a sibling's data in *either* direction across a fork edge: deleting the
child unlinks files its carried rows name (the parent's), and deleting the parent
unlinks files the child reads. Handing the URIs to the gate instead of deleting
them is what makes both safe, and it replaces the previous best-effort delete —
a queued row that fails to collect is simply retried by the next sweep.

**Phase 1: Collect URIs.** Catalog-wide over the branch's tables:
`table_persist_segment_metadata`, `table_snapshot_segment_metadata`, the
cold-index sidecars (`table_snapshot_segment_index_metadata` — their own files,
reachable only through an index header, so they need their own enumeration or
they leak past the partition CASCADE), and `compact_segment_metadata`. The last
covers crashed-mid-compact merged files, whose `NULL` rows no segment metadata
points at. Deduped: one physical file legitimately backs several rows, and the
delete set holds one row per file.

**Phase 2: Drop metadata and queue the files, in one transaction.**

1. DELETE the branch row from `branch_store`.
2. DROP the hot data tables per table on the branch.
3. `drop_branch_partitions` — DROP TABLE CASCADE on the per-branch leaves of the
   persist/snapshot metadata families, which removes the branch's segment rows
   and with them its references.
4. `insert_segment_delete_set_rows` for the Phase-1 set.

Steps 3 and 4 share the transaction deliberately: dropping the references and
queueing the files must be one atomic fact. A crash between them would leave
either an unreferenced file nothing will ever collect, or a queued file still
referenced with no clock to reconcile. The enqueue's `ON CONFLICT` refresh gives
a URI already queued by another branch's retirement the later grace clock.

## Snapshot (cold storage optimization)

**Implementation:** `LifecycleManager.snapshot`
(`lib/api/lifecycle.py`)

Snapshot creates a read-optimized, point-in-time materialization of a
table's cold storage data. After a snapshot, the read path only merges
log entries committed after the snapshot baseline against the snapshot
data — it no longer needs to process all log segments since genesis.

Snapshots also serve as time-travel checkpoints. Old snapshot segments
are never deleted by this operation — cleanup is handled by a separate
garbage collection process.

### Cold-only semantics

Snapshot operates exclusively on cold storage data. Hot storage (Postgres
tables with unpersisted mutations) is not read. This avoids contending
with live OLTP queries. For tables with regular persist intervals, the
snapshot covers the vast majority of data, so reads after a snapshot
typically only merge a small number of recent log entries from hot
storage.

### Two-table metadata model

Snapshot metadata is split across two tables:

- **`table_snapshot_metadata`** — one row per snapshot operation. Stores
  the table-level `snapshotted_at_micros` watermark that applies
  consistently to all segments in this snapshot. Has its own
  `commit_micros` to track whether the full operation completed.
- **`table_snapshot_segment_metadata`** — one row per partition
  (multiple rows can share one packed file via `(offset, length)` row
  ranges, CHA-404). Stores the file location, per-segment `statistics`
  (the pruning bounds), and per-segment `commit_micros` for
  progressive availability.

The parent table ensures a consistent watermark across all partitions
and is the snapshot's atomic commit boundary. Per-segment
`commit_micros` enables the read path to use individual segments
as soon as they're written, without waiting for the entire snapshot to
finish.

### Algorithm

```
snapshot(request)
  │
  ├── 1. Resolve schema_uuid, data_log_prefix_uuid, branch_uuid
  ├── 2. Read Arrow schema + partition_keys from table metadata
  ├── 3. Get read plan (existing snapshot + log segments)
  │
  ├── 4. Read cold storage only:
  │     ├── Existing snapshot segments (if any)
  │     ├── Cold upsert log segments
  │     └── Cold delete log segments
  │     └── Early exit if no cold data exists
  │
  ├── 5. Compute snapshotted_at_micros watermark
  │     └── PersistPlan.persisted_at_micros = MAX(commit_micros)
  │       over this table's cold upsert + delete segments
  │       (CHA-218: cold commit_tx_log is gone; each cold row already
  │       carries commit_micros inline)
  │
  ├── 6. Run merge algorithm (stream_all_cold_parts)
  │     └── Resolves deletes, deduplicates, produces clean output
  │
  ├── 7. Partition merged result by table's partition keys
  │     └── No partition keys → single partition containing all rows
  │
  ├── 8. Within transaction (SELECT FOR UPDATE on input segments):
  │     ├── INSERT INTO table_snapshot_metadata (committed_at = NULL)
  │     ├── For each partition:
  │     │   ├── INSERT INTO table_snapshot_segment_metadata (committed_at = NULL)
  │     │   ├── Write snapshot file to object storage
  │     │   └── UPDATE segment SET committed_at = now() (progressive availability)
  │     └── UPDATE table_snapshot_metadata SET committed_at = now()
  │
  └── 9. Return table_snapshot_uuid + segment_uuids
```

### Partition-aware segmentation

If the table defines `partition_keys`, the merged result is grouped by
distinct partition key values. Each partition becomes a separate
snapshot segment row whose per-slice `statistics` carry the
partition-column bounds. Tables without partition keys produce a single
segment (a NULL partition label).

The `max_segment_bytes` service-level config controls the maximum size
of a single segment file, enforced at write time: the CHA-404 packer
accumulates whole partitions into one file up to the cap, and an
oversized single partition splits into chunked sibling rows (CHA-215).
Snapshot segment files are immutable — never compacted or rewritten
(ADR 0024).

### Concurrency control

Snapshot acquires `SELECT ... FOR UPDATE` locks on the segment metadata
rows it reads (both snapshot and log segments) during the transaction.
This prevents concurrent compact from merging those same segments
mid-snapshot. Persist is safe to run concurrently — it creates new
metadata rows (no lock conflict), and `commit_micros` is
monotonically increasing per branch, so persisted data always has
timestamps after the snapshot watermark.

### The `snapshotted_at_micros` watermark

The `snapshotted_at_micros` value stored on the parent
`table_snapshot_metadata` row is always derived from the actual
`MAX(commit_micros)` in the cold commit_tx_log (filtered by branch and
capped by the request's `snapshotted_at_micros` if provided). It is not
simply copied from the request.

This is important for correctness: if the request specifies a timestamp
ahead of the actual latest committed transaction, using the request
value would cause the read plan to skip log segments between the real
watermark and the requested value. By storing the true watermark, the
plan correctly identifies all log segments that still need processing.

The request's `snapshotted_at_micros` serves as an upper bound
(`as_of_micros`) on which transactions the merge algorithm includes —
it caps the snapshot at a specific point in time for deterministic,
reproducible snapshots.

### Crash safety

Snapshot uses a two-phase protocol with per-segment progressive commit:

- **Phase 1** inserts the parent snapshot row with `committed_at = NULL`,
  then for each partition: inserts the segment row, writes the file,
  and sets the segment's `committed_at`. Each segment becomes
  independently usable as soon as its `committed_at` is set.
- **Phase 2** sets `committed_at` on the parent, marking the full
  snapshot as complete.
- **On failure**, cleanup deletes written files, then uncommitted
  segment and parent metadata rows, upholding the
  [no-orphans design principle](#design-principles). Carried-forward
  rows (below) are cleaned up **row-only** — their `object_uri` is a
  shared prior-snapshot file that must never be deleted on this
  snapshot's error path.

### Incremental snapshots: delta-partition carry-forward (CHA-406)

Snapshot segment files are immutable (ADR 0024). When a new snapshot
follows a prior one and only a few partitions changed, rewriting every
partition is wasted work. Carry-forward rewrites only the **touched**
partitions and references the prior file for the **untouched** ones —
a new `table_snapshot_segment_metadata` row under the new snapshot
pointing at the same `object_uri` + `(offset, length)`.

**Eligibility gate** — carry-forward engages iff *all* hold; any
failure falls back to the full rewrite above:

1. a prior committed snapshot exists with non-placeholder segments;
2. its recorded `partition_keys` / `clustering_keys` are present
   (`NULL` = a pre-CHA-404 parent row → ineligible) and equal the
   current layout keys (any key change forces a full rewrite — the
   ADR 0024 layout-key invariant);
3. the table is partitioned (`partition_keys` non-empty — an
   unpartitioned table is one always-touched partition);
4. the partition-key set ⊆ the primary-key set (makes delete
   attribution PK-derivable and partition moves structurally
   impossible — the v1 gate);
5. every prior segment row's partition label is derivable from its
   `statistics` blob (a label-exact writer leaves `min == max` for each
   partition key); any underivable segment logs a warning and falls
   back.

**Algorithm (eligible path):**

```
├── Resolve the delta only (persist rows in the window, NO snapshot
│   baseline): penca_merge::resolve_log_tiers → {resolved, exclusion_set}
├── touched = {partition labels with delta upserts}
│           ∪ {labels any windowed delete attributes to}
│     └── delete attribution: read the window's cold delete segments
│         (they carry PK columns; partition ⊆ PK) and collect distinct
│         partition labels. Over-inclusion is byte-correct.
├── Split prior segments by their stats-derived label:
│     ├── touched  → rewrite stream (original chunk_idx order preserved)
│     └── untouched → carried map (label → prior segment uuids)
├── Stream the touched prior subset (snapshot_segment_stream) with the
│   SAME exclusion_set, merge with the delta, pack into new files;
│   interleave the carried rows at their label position so the new
│   snapshot's ORDER BY chunk_idx stays label-sorted for the next cycle
├── Write files + commit; insert carried rows (NULL committed) then
│   bulk-commit them — same two-phase gate as written rows — BEFORE the
│   parent commit
└── Commit parent; retire older snapshots (CHA-405 refcount sweep makes
    retiring the now-shared prior snapshot safe)
```

**Empty-merge placeholder** (CHA-228) is written only when
`!wrote_any && carried.is_empty()` — a carried-only snapshot (deletes
or no-op cycle that touched nothing requiring a rewrite) commits its
watermark through the carried rows, so no placeholder is needed.

Carry-forward is partition-level: each new snapshot stays internally
recency-consistent (no read-path change). Segment-level carry-forward
and clustering-key pruning are deferred.

### Interaction with the read path

`QueryManager.plan()` selects the latest fully committed snapshot
(the `table_snapshot_metadata` row with the highest `commit_micros`)
and returns all its segments from `table_snapshot_segment_metadata`. Using
a single snapshot generation avoids mixing segments from different
partition schemes. The snapshot's `snapshotted_at_micros` is used as the
baseline — only log segments with `max_tx_commit_micros >
snapshotted_at_micros` are included. The merge algorithm then only
processes transactions committed after the snapshot baseline,
significantly reducing work for tables with many historical log segments.
