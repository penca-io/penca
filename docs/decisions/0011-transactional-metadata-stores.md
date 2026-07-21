# 0011: Transactional metadata stores

**Status:** Accepted
**Linear:** [CHA-164](https://linear.app/chapala/issue/CHA-164)

## Context

Before [CHA-164](https://linear.app/chapala/issue/CHA-164), Penca's
metadata write path was deliberately split from its data write path.

- **Data writes** (`MutateData`): rows in `data_upsert_log` /
  `data_delete_log` carry a `tx_uuid`. Visibility resolves at read time
  via JOIN against `commit_tx_log` — no `commit_micros` column on the
  data row, just the foreign key into the catalog's commit_tx_log. `BeginTx` /
  `CommitTx` toggle visibility for every row tagged with that tx_uuid.

- **Metadata writes** (`CreateTable` / `UpdateTable` / `DeleteTable` /
  `CreateSchema` / `UpdateSchema` / `DeleteSchema`): rows in
  `table_metadata_*_log` and `schema_store` were committed at INSERT
  time (`commit_micros DEFAULT epoch`). No `tx_uuid`, no commit_tx_log
  JOIN, no `BeginTx`/`CommitTx` involvement. DDL was non-transactional
  by design.

This split was correct when `commit_tx_log` was per-schema (pre-[CHA-163](https://linear.app/chapala/issue/CHA-163)):
the multi-schema atomic transactions that DDL would need didn't exist
anyway. Once CHA-163 lifted `commit_tx_log` to per-catalog and made
`BEGIN; INSERT s1.x; INSERT s2.y; COMMIT` work, the next natural
extension was DDL inside a transaction — `BEGIN; CREATE TABLE staging;
INSERT INTO staging …; ALTER staging RENAME TO prod; COMMIT;` as a
single atomic unit, with no half-states visible to concurrent readers.

CHA-164 brings metadata into the same MVCC scheme as data. There were
several axes of choice along the way; this ADR captures the ones that
were non-obvious.

## Decisions

### 1. Mirror the data-path commit_tx_log JOIN, not a CommitTx UPDATE

The CHA-164 ticket originally proposed two visibility mechanisms:

> 3. Read path joins metadata log against `commit_tx_log` for visibility
>    instead of reading `commit_micros` directly off the row.
> 4. Read-your-own-writes: `commit_micros <= :as_of OR
>    tx_uuid = :my_open_tx_uuid`.
> 5. `CommitTx` extends to set `commit_micros` on data **and**
>    metadata log rows for this `tx_uuid` — same INSERT-into-commit_tx_log
>    step, broader UPDATE.

(3) + (4) describe a commit_tx_log JOIN model. (5) describes a parallel UPDATE
that materializes the commit timestamp onto the row.

The data path implements neither (5) nor a `commit_micros`
column — `data_upsert_log` is `(version_uuid, row_uuid, tx_uuid,
…user…)` with no commit timestamp. Visibility is purely the commit_tx_log
JOIN. CommitTx writes one commit_tx_log row; that single insert flips
visibility for every row tagged with that tx_uuid.

**Decision: metadata follows the data-path shape exactly.**
`table_metadata_*_log` and `schema_*_log` carry `tx_uuid`, no
`commit_micros`. CommitTx and AbortTx are unchanged — they
already toggle visibility for the data path; the metadata path
inherits the toggle for free.

The cost-benefit:

- **For (5):** would let reads use the simpler `commit_micros <=
  :as_of` predicate without a commit_tx_log JOIN. Saves one JOIN per metadata
  read.
- **Against (5):** doubles the cost of CommitTx (now an UPDATE against
  every row tagged with `tx_uuid`, not just an INSERT). Diverges from
  the data-path shape, so `nontx_resolve_*` and `tx_resolve_*` would
  both stay around. Two visibility models is more code surface, more
  documentation burden, more chance of subtle drift.

The JOIN cost is negligible at admin-tier scale (few thousand rows
total per catalog). The CommitTx-stays-cheap property is meaningful at
data-tier scale. The decision was easy: mirror data.

### 2. Eager physical CREATE; deferred physical DROP

`CreateTable` needs to create the per-branch PG tables
(`{data_log_prefix_uuid}_data_upsert_log` and `_data_delete_log`)
before subsequent DML in the same transaction can write to them.
`DROP TABLE` is the inverse — the physical tables must survive
mid-transaction `SELECT * FROM dropped_table` reads in case the tx
rolls back.

**Decision:**
- `CreateTable` eagerly creates the physical tables. On rollback, the
  lifecycle sweeper drops the orphans.
- `DeleteTable` writes a delete-log tombstone but does **not**
  immediately drop the physical tables. The lifecycle sweep drops them
  only after the tx commits — until then, a rollback leaves them
  intact and fully addressable.

This matches how every transactional-DDL system handles physical
storage: PG, CockroachDB, Snowflake all eagerly create at CREATE,
defer drops, and rely on a background sweeper for orphan cleanup.

### 3. Deterministic per-branch `data_log_prefix` (replaces `physical_table_uuid`)

A pre-CHA-164 attempt at `physical_table_uuid = hash(branch_uuid,
table_uuid)` collided on PG's `relation already exists` when an
aborted-but-not-yet-swept tx left orphan physical tables behind. That
motivated `physical_table_uuid = hash(table_uuid, tx_uuid)` — per-
create-tx, so concurrent CreateTable calls minted different physicals.
The cost was that `physical_table_uuid` was no longer derivable from
`(table_uuid, branch_uuid)` alone; every read path had to look it up
via the metadata log, and `UpdateTable` had to carry the prior
physical forward on each new metadata row.

**CHA-177 supersedes that decision.** The data tables for table T on
branch B live at `{prefix}_data_upsert_log` / `_data_delete_log`
where `prefix = row_uuid_for_pk(table_uuid, [branch_uuid])` — the
same value as the table's `row_uuid` in `__penca_system__.tables`
on B. Properties:

- **Deterministic per branch.** No metadata lookup needed; callers
  derive the prefix in pure compute.
- **Concurrent CreateTable is a no-op for the loser.** Both txs
  compute the same prefix and target the same data tables;
  `CREATE TABLE IF NOT EXISTS` makes the second create a no-op.
  Aborted-tx rows in the shared upsert log are filtered out by the
  standard `commit_tx_log` JOIN at read time, and swept by the same row-
  level orphan sweep user data writes use. No orphan physical tables
  to track.
- **`UpdateTable` is a plain metadata write.** No physical to carry
  forward — the prefix is the same per `(table, branch)` regardless
  of how many metadata versions the table has had.
- **Disaster-recovery is restored.** Lose `__penca_system__.tables`
  and you can recompute the prefix for any `(table, branch)` you
  know about; no audit-trail walk needed.

The `physical_table_uuid` column on metadata rows is **dropped** as
part of CHA-177. `MetadataClient.lookup_physical_table_uuid` and
`naming::get_physical_table_uuid` are gone — replaced by
`naming::data_log_prefix` (deterministic compute).

`ALTER TABLE foo RENAME TO bar` remains **out of scope for
CHA-164/CHA-177** (returns `UNIMPLEMENTED` from the SQL surface).
With deterministic-by-(table_uuid, branch) prefixes, rename would
mean either stranding data under foo's prefix (breaking the
"prefix == row_uuid in `__penca_system__.tables`" property) or
copying the data (synchronous large-table rewrite). Either deserves
its own design discussion.

### 4. Branch-scoped schemas + tables via per-branch physical (CHA-177)

Pre-CHA-177: schemas lived in `schema_upsert_log` / `schema_delete_log`
LIST-partitioned by `branch_uuid`; tables lived in similarly-shaped
`table_metadata_*_log` subpartitioned by `(schema_uuid, branch_uuid)`.
Both were special-cased storage with bespoke partition management.

**Decision: schemas are branch-scoped in the underlying storage but
read-through-main effectively in the API**, until merge-on-read
inheritance lands in a follow-up (Stage 2c of the CHA-164 plan).

Concretely: `MetadataClient.get_schema` / `list_schemas` always read
from `schema_upsert_log_partition(catalog, main)`. `CreateSchema` from
the API writes to main as well (regardless of which branch the caller
is on). Non-main branches don't yet see their own schema CRUD; this
is a known limitation tracked for the Stage 2c follow-up.

The full design (visible schemas on branch X = X's own
`schema_*_log` UNION parent's `schema_*_log` at the fork commit
(`fork_commit_seq_num`) MINUS X's effective deletes) mirrors the data
merge-on-read pattern in
`penca_merge::merge_read`. It needs a new resolve helper that UNIONs
two branch resolves with prefix-renamed CTEs to avoid name collision;
the work is well-scoped but not in this PR.

### 5. `schema_store` is removed

The original ticket comment proposed dual-writing to both `schema_store`
and the new `schema_*_log` to keep readers backward-compatible.
This was useful during the staged migration (Stages 1–2 wrote to both,
reads switched to the new path mid-migration) but isn't needed in the
final shape. After Stage 3 cuts over `CreateSchema` to write only to
`schema_*_log`, `schema_store` has no readers and no writers.

**Decision: remove `schema_store` entirely** — DDL, naming function,
parity-test entries, all gone. The only reference left is in the
catalog `parent_retention` JOIN, which migrates to a subquery against
`schema_*_log` directly.

## Consequences

- `CommitTx` and `AbortTx` need zero changes for CHA-164. The
  visibility flip falls out of the existing commit_tx_log insertion.
- The lifecycle sweeper grows new responsibilities: clean orphan
  uncommitted metadata-log rows after `AbortTx` or expired
  `BeginTx`; drop their orphan physical tables; drop the deferred-DROP
  physical tables for committed `DeleteTable` rows. This is Stage 4
  of the CHA-164 plan and lands as a follow-up.
- `nontx_resolve_*` is gone. The codebase has one auditable-store
  resolve helper (`resolve_sql` / `resolve_cte_sql`), parameterized by
  `entity_column` (`row_uuid` for every auditable store, data and
  metadata alike; CHA-380 made the system tables' `row_uuid` a canonical
  `row_uuid_for_pk` hash rather than the described entity's uuid) and
  `open_tx_uuid` for RYOW.
- Schema reads from non-main branches will return main's schemas
  until Stage 2c lands merge-on-read inheritance — known regression
  for branch-scoped schema visibility, tracked.
- Concurrent `CreateTable` on the same `(table_uuid, branch_uuid)` is
  last-commit-wins with the loser's physical orphaned. Per
  [CHA-5](https://linear.app/chapala/issue/CHA-5), strong
  serializability is the bigger gap and is its own work.

## Related decisions

- [ADR 0008](0008-table-metadata-subpartitioning.md) — the
  `schema_uuid → branch_uuid` subpartitioning of `table_metadata_*`
  set up by CHA-163; CHA-164 inherits it unchanged.
- [ADR 0007](0007-session-entity.md) — the connection-local session
  cache that owns the open-tx state Flight SQL clients use to drive
  RYOW.
- [ADR 0012](0012-metadata-as-first-class-tables.md) — the
  metadata-as-first-class-tables shape that lets schema and table
  metadata reuse the data path's storage primitives.
- [ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md)
  — how the "one version per `(entity, tx)`" invariant is enforced:
  deterministic `version_uuid = xxh3(row_uuid, tx_uuid)` with the
  PRIMARY KEY doing the work.
