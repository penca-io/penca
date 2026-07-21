# ADR 0020 — Non-deterministic namespace UUIDs + rename support

## Status

Proposed ([CHA-236](https://linear.app/chapala/issue/CHA-236)).

Builds on:

- [ADR 0011](0011-transactional-metadata-stores.md) — transactional
  metadata stores as the home for namespace rows.
- [ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md)
  — preserved unchanged for `row_uuid` and `version_uuid`.
- [ADR 0016](0016-canonical-uuid-construction-for-derived-rows.md) —
  preserved unchanged for derived rows (persist / snapshot / purge
  chain).
- [ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md) and
  [ADR 0019](0019-plan-time-pinning-and-universal-grace-window.md) —
  the per-table `lifecycle` machinery they describe is keyed on
  `(table_uuid, branch_uuid)`, both of which remain stable across
  rename under this ADR.

## Context

Pre-CHA-236, every namespace UUID was a deterministic hash of its
human-readable name and parent identity:

- `catalog_uuid = xxh3(catalog_name)`
- `schema_uuid  = xxh3(catalog_uuid, schema_name)`
- `branch_uuid  = xxh3(catalog_uuid, branch_name)`
- `table_uuid   = xxh3(schema_uuid, table_name)`

Name resolution was pure computation: `naming::get_*_uuid` derived a
UUID from a name with no roundtrip. The trade is that the name *is*
the identity input — renaming an object would change its UUID, which
would invalidate every client reference, every per-tier physical
address derived from `(table_uuid, branch_uuid)`, every persist /
snapshot / purge chain entry, and the per-row `row_uuid` hash basis.
The wire contract reflected this: `UpdateTableRequest` documented
`table_name` as "identifier-only … and immutable after creation."

Rename is a standard database feature. Every namespace object in PG,
MySQL, Snowflake, and BigQuery has a server-minted OID distinct from
its user-visible name; users rename freely; OIDs survive. The
deterministic-name-hash model in Penca was the artifact of a
"no roundtrip on resolve" optimization, not a deliberate identity
design — and the cost of that optimization (no rename, ever) is much
higher than the supposed savings.

The "free pull" of name → UUID roundtrips that the deterministic
hash bought collapses on inspection:

- Every DDL with non-trivial server-side validation fetches its
  parent-reference row anyway (does the catalog exist? the schema?
  the branch?). One PG SELECT per DDL, already on the wire.
  Returning the server-minted UUID from that same SELECT is the same
  roundtrip — zero incremental cost.
- Per-branch physical storage already derives from `(table_uuid,
  branch_uuid)` ([CHA-177](https://linear.app/chapala/issue/CHA-177)).
  Branching does not copy rows; the same `table_uuid` lives at
  distinct physical addresses on each branch. Rename on branch A is
  a per-branch metadata upsert that doesn't touch other branches.
- `__penca_system__.tables` is itself a Penca Table with
  per-branch merge-on-read, so the persisted `table_name` column is
  naturally MVCC and naturally per-branch.

Data-tier identifiers (`row_uuid`, `version_uuid`, persist / snapshot
/ purge chain) are a different argument. Cold storage has no PK
constraint or hash; Penca owns identity end-to-end; the perf
rationale for compact single-column hash-derivation is real; rename
is irrelevant at the row level. ADR 0013 / 0016 stand unchanged.

This ADR scopes only namespace identifiers (catalog / schema /
branch / table).

## Decision

### Seven points

1. **Server-minted random namespace UUIDs.** Namespace UUIDs
   (`catalog_uuid` / `schema_uuid` / `branch_uuid` / `table_uuid`)
   for user-created resources are minted with `Uuid::new_v4()` at
   `Create*` time and persisted on the namespace row. The
   `naming::get_{catalog,schema,branch,table}_uuid` helpers are
   deleted (commit 2 of CHA-236).
2. **Data-tier identifiers unchanged.** `row_uuid`, `version_uuid`,
   `genesis_tx_uuid`, and the persist / snapshot / purge chain stay
   deterministic per ADRs 0013 / 0016. Their inputs are now
   random namespace UUIDs instead of name-derived ones, but the
   chain shape, the parity goldens for chain-rooted helpers, and the
   per-tier physical addressing all preserve unchanged.
3. **Structural / user-namespace boundary.** Three deterministic
   anchors stay rooted on `catalog_uuid` so server-internal write
   paths can address per-catalog system rows without state:
   - `system_schema_uuid(catalog_uuid)` — `__penca_system__`
     schema.
   - `system_schemas_table_uuid(catalog_uuid)` —
     `__penca_system__.schemas`.
   - `system_tables_table_uuid(catalog_uuid)` —
     `__penca_system__.tables`.

   Same rule that already governs the tx-log family and the
   per-catalog persist / snapshot / purge metadata parents: anything
   the storage layer needs to address by well-known per-catalog
   identity is structural (deterministic UUID, non-renamable,
   non-deletable, non-`MutateData`-writable). Everything else
   (`public` schema, all user-created resources) is a user namespace
   object (random UUID, fully mutable). Every mutating handler calls
   `assert_not_system_{schema,table}` after identifier resolution
   and rejects targeting the three anchors with `INVALID_ARGUMENT`.
4. **`*_uuid` is the recommended-stability addressing form.** The
   identifier-resolution rules in `common.proto` are rewritten:
   - UUID identifies the resource by its server-minted random UUID,
     which does not change across rename.
   - Name is human convenience scoped to the current state, or to
     the `as_of_micros` snapshot on read RPCs that support it.

   Clients that persist references across sessions should persist
   UUIDs.
5. **Name uniqueness is server-enforced, not hash-collision-derived.**
   - `catalog_store.catalog_name` carries `UNIQUE`.
   - Per-catalog `branch_store.branch_name` carries `UNIQUE` (scope:
     within a catalog).
   - `__penca_system__.schemas` / `__penca_system__.tables` name
     uniqueness is enforced by a within-tx existence check on
     `Create*` and on `Update*` with `new_{schema,table}_name` set.
     Returns `ALREADY_EXISTS` on collision.

   Soft-delete is out of scope here;
   [CHA-239](https://linear.app/chapala/issue/CHA-239) will later
   migrate the two PG `UNIQUE` constraints to partial indices
   (`WHERE deleted_at_micros IS NULL`) once soft-delete lands.
6. **No process-wide `name → uuid` cache.** Resolving a name under
   rename requires the current row, and any cross-request cache
   carries staleness risk under concurrent rename. The SQL server's
   session-scoped pinning of `(catalog_uuid, …)` for the lifetime
   of a session remains valid because UUIDs survive rename; that's
   session state, not a cache.
7. **Identity-resolution roundtrip is the explicit price of mutable
   names.** Name-only paths cost one PG SELECT (catalog / branch in
   non-MVCC stores) or one Penca Table `merge_read`
   (schema / table in `__penca_system__.*`). This is the same
   roundtrip every DDL with server-side parent validation already
   performs (see Context). The price is itemized rather than hidden.

### Mechanism — `as_of_micros`-aware name resolution

A renamed schema or table found via time travel must resolve under
the same snapshot as the data read. Otherwise
`ReadData(table_name="foo", as_of_micros=150)` would return
`NOT_FOUND` for a table renamed `foo → bar` at T=200 even though it
existed as `foo` at T=150 — the snapshot-aware data read would be
defeated by a snapshot-blind name resolve.

Four read RPCs gain `optional int64 as_of_micros`:

- `GetSchemaRequest`
- `ListSchemasRequest`
- `GetTableRequest`
- `ListTablesRequest`

The field is mutually exclusive with `open_tx_uuid` (matches the
`ReadDataRequest` convention) — `INVALID_ARGUMENT` if both are set.
`AuditDataRequest` reuses its existing
`committed_at: TimestampFilter` — name resolution uses
`committed_at.max_micros` (if set, else Latest), so an audit query
against a renamed table finds it by its historical name within the
requested window.

Resolution order at the API layer (every read handler):

1. `catalog_uuid` ← `catalog_store` SELECT (non-MVCC table; current
   row regardless of snapshot — see Limitation).
2. `branch_uuid` ← `branch_store` SELECT (non-MVCC; same).
3. Derive `ReadSnapshot` from `(as_of_micros, open_tx_uuid)`;
   validate `open_tx_uuid` against `begin_tx_log` on the resolved
   branch.
4. `schema_uuid` ← `__penca_system__.schemas` `merge_read` with
   the snapshot.
5. `table_uuid` ← `__penca_system__.tables` `merge_read` with the
   snapshot.

DDL writes (`Create*` / `Update*` / `Delete*` / `MutateData`) do not
grow `as_of_micros`. They operate on "now"; resolution uses Latest
plus `open_tx_uuid` for read-your-own-writes.

### Limitation — catalog + branch rename are snapshot-blind

`catalog_store` and `branch_store` are non-MVCC PG tables (per
ADR 0011's transactional-metadata-store partition). Name resolution
against them uses the current row regardless of `as_of_micros`. The
practical effect: catalog and branch rename do not honor time
travel; a `GetTable(catalog_name="old", as_of_micros=...)` after a
catalog rename `old → new` will return `NOT_FOUND` even if the
referenced row existed under `old` at the snapshot.

Migrating `catalog_store` and `branch_store` to MVCC Penca Tables
would close this gap, but the migration is a separate, much larger
effort (touching catalog bootstrap, branch DDL, every catalog /
branch lookup path, and the metadata service boundary). It is
tracked as [CHA-240](https://linear.app/chapala/issue/CHA-240)
(design ticket; "Low priority, not urgent") and is out of scope
here.

In practice, schema and table renames are the high-frequency case;
catalog and branch renames are administrative operations whose
audit / time-travel needs are lower. Documenting the asymmetry up
front is preferred over a half-mechanism that pretends the gap
isn't there.

### `UpdateBranch` RPC

`WriteService` gains a new `UpdateBranch` RPC; today only
`new_branch_name` is mutable. Branches are catalog-scoped per
[CHA-184](https://linear.app/chapala/issue/CHA-184); the request
carries the catalog identifier alongside the branch identifier.
Like `CreateBranch` / `DeleteBranch`, `UpdateBranch` is not
tx-tracked — branch rename updates `branch_store` directly.

`UpdateCatalog` / `UpdateSchema` / `UpdateTable` each gain
`optional string new_{catalog,schema,table}_name`. When set, the
server runs a name-uniqueness check (using the appropriate scope
per point 5 above) and emits `ALREADY_EXISTS` on collision,
otherwise propagates the new name into the metadata upsert.

## Trade-offs and consequences

- **Code-level boundaries that moved.**
  - `naming::get_{catalog,schema,branch,table}_uuid` deleted; the
    three structural anchors reroot directly on `catalog_uuid` and
    the commit_tx_log / segment-metadata partition helpers reroot on
    `(catalog_uuid, branch_uuid, partition_tag)` (commit 2 of
    CHA-236, already landed at `5806ea5`).
  - `MetadataClient::get_catalog` / `get_schema` / `get_table` plus
    a new `MetadataClient::get_branch_by_name` become the resolver
    surface; `resolve_{catalog,schema,branch,table}_uuid` become
    async and take the read snapshot (commit 4).
  - Proto surface gains `new_{catalog,schema,table}_name` on the
    corresponding `Update*Request`, `as_of_micros` on the four read
    RPCs, `main_branch_uuid` on `CreateCatalogResponse`, and the new
    `UpdateBranch` RPC carrying `new_branch_name` (commit 3, this
    commit).
- **No migration.** Penca is pre-release; `just integration-test`
  nukes fixtures on startup; no production data exists. The
  `naming::get_*_uuid` deletion is a hard break, not a deprecation.
- **No `oid` column.** `*_uuid` fields are repurposed in place —
  only the derivation changes, not the shape. The protobuf wire
  fields, the metadata-store columns, and every existing per-tier
  derivation continue to take a UUID; whether that UUID was once
  hash-derived or freshly minted at `Create*` is invisible to
  consumers.
- **`row_uuid IS schema_uuid / table_uuid` convention preserved.**
  No new entity-id column. The random UUID minted at `Create*`
  becomes both the entity UUID and the auditable-store row identity
  for the corresponding `__penca_system__` row.
- **One PG SELECT per name-only DDL path.** This is the same
  parent-validation roundtrip every DDL already performs (see
  Context). Itemized as a cost; not new total work.
- **Snapshot-blind catalog / branch rename.** See Limitation;
  tracked in CHA-240.
- **Client guidance.** Clients that persist references across
  sessions should persist `*_uuid` (stable across rename). Clients
  that operate by name are scoped to "now" or to the explicit
  `as_of_micros` snapshot they pass.
- **Hash-derivation audit.** Open question 1 on the CHA-236 ticket
  ("are there callsites where the current code relies on
  hash-derivation as an implicit consistency check?") is treated
  as in-scope: any audit finding ships fixed inside this PR.
  Removing hash-derivation that turns out to be load-bearing is
  a correctness regression introduced by this change; it cannot
  ship as a follow-up.

## Out of scope

- Data-tier identifiers (`row_uuid`, `version_uuid`, persist /
  snapshot / purge chain). ADRs 0013 / 0016 unchanged.
- Migration / backfill. Pre-release; no data to migrate.
- A separate `oid` column. `*_uuid` repurposed in place.
- Snapshot-aware catalog / branch rename. Tracked as
  [CHA-240](https://linear.app/chapala/issue/CHA-240).
- Soft-delete + grace-window sweep for catalog / branch DDL races.
  Tracked as [CHA-239](https://linear.app/chapala/issue/CHA-239).
  This ADR adds full `UNIQUE` constraints on `catalog_name` /
  `branch_name`; CHA-239 will migrate them to partial indices
  (`WHERE deleted_at_micros IS NULL`) once soft-delete lands.
- Servicer-boundary catalog / schema / branch existence validation.
  Covered by [CHA-92](https://linear.app/chapala/issue/CHA-92).

## Relationship to other ADRs

- **[ADR 0011](0011-transactional-metadata-stores.md):** the
  partition between non-MVCC PG metadata stores (`catalog_store`,
  `branch_store`) and per-branch Penca Tables (`__penca_system__.
  schemas`, `__penca_system__.tables`) is preserved. This ADR's
  snapshot-aware name resolution honors the partition: only the
  Penca Table side honors `as_of_micros`; the PG side resolves
  against the current row (see Limitation).
- **[ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md):**
  `row_uuid` / `version_uuid` derivation unchanged. The inputs are
  now random namespace UUIDs instead of name-derived ones, but the
  hash shape and per-row identity stability across branches and
  merges are unchanged.
- **[ADR 0016](0016-canonical-uuid-construction-for-derived-rows.md):**
  the persist / snapshot / purge chain construction is preserved.
  Chain helpers consume `(catalog_uuid, branch_uuid, table_uuid)`
  inputs; the inputs are now random instead of hash-derived, but
  each helper's hash-input-space-disjoint property is preserved
  (see commit 2's reroot of `table_purge_uuid` and
  `segment_delete_uuid` to catalog-scoped tags).
- **[ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md) and
  [ADR 0019](0019-plan-time-pinning-and-universal-grace-window.md):**
  per-table lifecycle keyed on `(table_uuid, branch_uuid)`. Both
  identifiers remain stable across rename; rename is observably
  transparent to the lifecycle scheduler.
- **[CHA-177](https://linear.app/chapala/issue/CHA-177):** per-branch
  physical addressing `data_log_prefix(table_uuid, branch_uuid)`
  preserved. Rename touches the row's `table_name` column; the
  prefix is unchanged.

## Pre-1.0 migration

Drop-and-recreate per the
[CHA-203](https://linear.app/chapala/issue/CHA-203) precedent.
Covers the addition of `UNIQUE (catalog_name)` on `catalog_store`
and `UNIQUE (branch_name)` on per-catalog `branch_store`; no
metadata migration is needed for the identity-minting flip because
no pre-CHA-236 fixture data is preserved across the integration
suite's `just integration-test` setup.
