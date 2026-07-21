# ADR 0012 — Metadata logs are first-class Penca Tables

## Status

Accepted (CHA-164 / CHA-177). Per-branch physical via deterministic
`data_log_prefix` (CHA-177) supersedes the original
catalog-scoped + `physical_table_uuid` shape sketched in this ADR's
first draft.

## Context

CHA-164 made schema and table DDL transactional by introducing two new
auditable stores: `schema_*_log` and `table_metadata_*_log`. These were
second-class internal storage — partitioned tables with their own
naming convention, bespoke DDL, special handling everywhere they were
touched.

That created an asymmetry: data tables flow through the full lifecycle
(Persist / Snapshot / Compact / Purge), but metadata tables don't, even
though both are auditable stores resolved with the same CTE.
Specifically, the post-persist `commit_tx_log` purge had to be disabled in
`51aab73` because the data-persist watermark wasn't safe to advance past
— `commit_tx_log` now also gates `table_metadata_*_log` visibility, so
purging tx rows below the data watermark would orphan committed DDL
metadata. Without metadata logs flowing through persist, the watermark
can never include them, and the purge stays disabled.

## Decision

Metadata logs are **regular Penca Tables** stored in the standard
`{prefix}_data_upsert_log` / `{prefix}_data_delete_log` format,
exposed under the well-known schema `__penca_system__`:

- `__penca_system__.schemas` — replaces `schema_*_log`. Each row is a
  schema on one branch in this catalog.
- `__penca_system__.tables` — replaces `table_metadata_*_log`. Each
  row is a table on one branch.

**Per-branch by construction.** Like every user data table, the
per-branch PG tables are named via the two-arg helpers:

```python
upsert_log = naming.upsert_log_table(table_uuid, branch_uuid)
delete_log = naming.delete_log_table(table_uuid, branch_uuid)
```

The internal prefix (`row_uuid_for_pk(table_uuid, [branch_uuid])`) is
computed inside the helper — callers pass identity directly. So
branch X's `__penca_system__.tables` data lives at one PG table,
branch Y's at another. `CreateBranch` materializes parent-branch rows
under the new branch's `branch_uuid` exactly the way it materializes
data tables — read parent-branch rows, rewrite with the child
branch_uuid, append to child branch's `__penca_system__` data tables
under one shared `materialize_tx`.

**No carry-forward identifier column.** Per ADR 0011 §3 (rewritten
under CHA-177), the per-branch storage location is derived
deterministically from `(table_uuid, branch_uuid)`; no separate
pointer column is carried in metadata. See
[naming.rs](../../crates/penca-core/src/naming.rs) for the canonical
identity-model table.

`row_uuid` derivation in the system tables (CHA-380):

- `__penca_system__.schemas`: `schema_uuid` is a first-class PK column;
  `row_uuid = row_uuid_for_pk(system_schemas_table_uuid, [schema_uuid])`.
- `__penca_system__.tables`: `table_uuid` is a first-class PK column;
  `row_uuid = row_uuid_for_pk(system_tables_table_uuid, [table_uuid])`.
- `__penca_system__.indexes` (CHA-455): `index_uuid` is a first-class PK
  column; `row_uuid = row_uuid_for_pk(system_indexes_table_uuid, [index_uuid])`.

The system tables follow the universal auditable-store pattern (ADR 0013)
like every other Penca table — the entity's own uuid is a distinct schema
column, cross-branch stable, and `row_uuid` is the canonical hash of it.
CHA-380 superseded the original mechanism, which overloaded `row_uuid` as the
described entity's uuid directly (a "deliberate skip" of the hash level); that
overload accumulated special-casing across the metadata read/seek path, so it
was regularized to the universal model.

**Branch isolation is encoded in which PG table the row lives in,
not in the row content.** Branch B's `__penca_system__.tables` rows
live in `upsert_log_table(sys_tables_table_uuid, B_branch_uuid)`
(a different PG table than branch A's), so no cross-branch dedup
risk; the `row_uuid` doesn't need to repeat what the table-name layer
already encodes.

`CreateBranch` materializes parent rows under the child's per-branch
PG tables — same `row_uuid`s, new `tx_uuid` (the synthetic `fork_tx`).

Bootstrap order at `CreateCatalog`:

1. Create per-catalog control tables (`commit_tx_log` family, `branch_store`).
2. Insert the genesis tx row.
3. Create the four PG SQL tables for the two system tables on the
   main branch via the standard `create_data_tables(table_uuid,
   branch_uuid, …)` helper.
4. Insert the Schema rows for `__penca_system__` and `public`
   directly into the system schemas PG table (raw SQL, since the API
   hasn't bootstrapped yet — the API would need these rows to exist
   to function).
5. Insert the Table rows for `__penca_system__.schemas` and
   `__penca_system__.tables` directly into the system tables PG
   table on main.

After step 5, every subsequent `CreateSchema` / `CreateTable` / etc.
writes its metadata row to the system data tables via the standard
storage path (`metadata_client.insert_schema_row` /
`insert_table_metadata` both target
`upsert_log_table(sys_X_table_uuid, branch_uuid)`). The
**substrate** is unified — read, persist, snapshot, compact, and purge
all run against the system tables exactly the way they run against
user data tables.

A separate refactor (CHA-174) folded the admin RPC handlers into
`WriteService` / `QueryService` at the **service boundary** — the
`AdminService` proto is gone, the topology shrunk by one container,
and the 6 DDL handlers now live alongside `MutateData` in
`WriteManager`.

Note that CHA-174 deliberately did **not** route DDL writes through
`apply_changes`. Doing so would conflict with the `row_uuid IS
table_uuid` / `row_uuid IS schema_uuid` skip described above:
`apply_changes::insert_rows` computes `row_uuid_for_pk(table_uuid,
&pk_refs)`, which would derive a different UUID than the cross-branch
identifier the system tables deliberately reuse. The 6 handlers
continue calling `MetadataClient::insert_table_metadata` /
`insert_schema_row` / matching tombstone helpers directly, each within
its own Pg tx — same code shape as before, just hosted under
`WriteService`. CHA-181 added explicit `tx_table_log` emit calls at
each of those handlers (and at `merge_branch`'s bulk
INSERT-FROM-SELECT path) to keep the index complete despite the
bypass.

### Concurrent CreateTable

With deterministic `data_log_prefix(table, branch)`, concurrent
`CreateTable` calls for the same `(table_uuid, branch_uuid)` compute
the same prefix and target the same data tables. PG's
`CREATE TABLE IF NOT EXISTS` makes the second create a no-op. Both
`CreateTable` rows land in `__penca_system__.tables` with different
`version_uuid`s (different tx_uuids); auditable-store dedup picks the
latest as canonical. Aborted-tx rows in the shared upsert log are
filtered out by the standard `commit_tx_log` JOIN at read time and swept by
the same row-level orphan sweep user data writes use. **No orphan
physical tables to track.** Lifecycle is symmetric with data writes.

## Consequences

**Positive:**

- Metadata logs flow through Persist / Snapshot / Compact / Purge
  unchanged. The data-persist watermark naturally includes metadata,
  so the post-persist `commit_tx_log` purge can be re-enabled safely (CHA-177
  does this).
- One storage abstraction across the codebase: `{prefix}_data_*` for
  everything. No `schema_upsert_log_partition`,
  `table_metadata_upsert_log_partition`, etc. naming or partitioning
  helpers.
- `SELECT * FROM __penca_system__.{schemas,tables}` works via the
  standard read path — no special-case dispatch in `read_data`.
- New system tables can be added the same way (insert a Table row at
  `CreateCatalog`, create its physical SQL tables) without changing
  any pipeline code.
- Branch isolation is automatic via per-branch physicals; no cross-
  branch filter needed at the storage layer.
- Disaster recovery: lose `__penca_system__.tables` and you can
  recompute every table's data location from `(table_uuid, branch)` —
  no metadata-only state required.

**Negative:**

- Bootstrap has a recursive flavor (we insert metadata rows for the
  system tables into the system tables themselves) that requires raw
  SQL inserts at `CreateCatalog`. Documented; isolated to one place.

## Alternatives considered

- **Keep `schema_*_log` / `table_metadata_*_log` as second-class
  storage; add dispatch shims in persist / snapshot / compact / read.**
  The pragmatic but architecturally muddy answer. Each lifecycle
  function would carry a special case for system tables. The first
  shim landed in `dd0d19f` (read-path dispatch) and was reverted
  alongside this refactor — it was a tell that we'd kept a
  half-measure.

- **Catalog-scoped physical with `branch_uuid` as a user column**
  (the original CHA-177 ticket text). One PG table per system table
  per catalog, all branches' rows mixed together; `WHERE branch_uuid
  = X` to scope per branch. Considered briefly to avoid recursive
  bootstrap, but this makes `__penca_system__.{schemas,tables}` the
  only Penca Tables that aren't per-branch. The recursive bootstrap
  isn't actually a problem — at `CreateCatalog` we know all the
  inputs deterministically (genesis_tx_uuid is fixed per catalog,
  main_branch_uuid is fixed per catalog), so we can compute and
  insert the bootstrap rows in one shot. Per-branch physical wins on
  symmetry: every Penca Table — user data or system metadata — has
  the same per-branch deterministic data location.

- **Drop `physical_table_uuid` via UPDATE-migrates-data semantics**
  (one of the alternatives in this ADR's first draft). Considered
  when the design still had a `physical_table_uuid` column carried
  forward across UPDATEs. CHA-177's deterministic-by-(table, branch)
  prefix obviates the carry-forward problem entirely — there's no
  pointer column to migrate; the prefix is computed.
