# 0015 — No foreign keys in Penca metadata schema

Status: Accepted
Ticket: [CHA-168](https://linear.app/chapala/issue/CHA-168)

## Context

Penca's Postgres metadata schema has many parent/child relationships
between tables: `tx_table_log` rows reference a `commit_tx_log` row by
`tx_uuid`, the system data tables (`__penca_system__.{schemas,tables}`)
reference `branch_store` rows by `branch_uuid`, `branch_store` rows
reference a `catalog_store` row, and so on. None of these are
expressed as Postgres `FOREIGN KEY` constraints.

Three ad-hoc FKs slipped in during earlier work and were carried into
CHA-168 (the first is gone post-CHA-220 — `branch_persist_metadata` no
longer exists, and `table_persist_metadata` no longer carries a
`branch_persist_uuid` column):

* ~~`table_persist_metadata.branch_persist_uuid → branch_persist_metadata`~~
  (CHA-220: column + parent table both dropped)
* `table_persist_segment_metadata.table_persist_uuid → table_persist_metadata`
* `table_snapshot_segment_metadata.table_snapshot_uuid → table_snapshot_metadata`

A reviewer flagged the inconsistency: either every relationship in
the schema gets an FK, or none of them do. Mixed enforcement is
worse than either pole — readers can't tell from the schema which
relationships are "really" enforced and which rely on application
invariants.

## Decision

**Penca metadata tables use no `FOREIGN KEY` constraints.**
Relational integrity is enforced exclusively by application-level
invariants:

1. **Deterministic UUID derivation.** Most cross-table references
   are derived via `xxh3` from server-known inputs (e.g.
   `commit_tx_log_partition_uuid = xxh3(catalog, branch)`,
   `data_log_prefix_uuid = xxh3(table_uuid, branch_uuid)`). The UUID
   is reproducible from context — there is nothing to "look up" and
   nothing to constrain.
2. **Staged writes.** Write paths order their inserts so a child
   row is never inserted before its parent: `TABLE_PERSIST_METADATA`
   inserted before any `TABLE_PERSIST_SEGMENT_METADATA`, etc. The
   application invariant takes the place of FK enforcement.
3. **Recovery sweeps.** Mid-write crashes leave parents and children
   with `commit_micros IS NULL`. Recovery (e.g. CHA-197 for
   the persist tree) walks parent → child via plain `SELECT` joins on
   the FK columns and either rolls forward or rolls back. The
   sweep doesn't need engine FK metadata to do its work.

The three pre-existing FKs were dropped on CHA-168. The columns
remain (`branch_persist_uuid`, `table_persist_uuid`, `table_snapshot_uuid`),
they're still indexed, and the joins still work — only the
`REFERENCES` clauses are gone.

## Why no FKs

* **Consistency with the rest of the schema.** Most parent/child
  relationships in the Penca metadata layer are already FK-free
  (`tx_table_log` ↔ `commit_tx_log`, `upsert_log/delete_log` ↔ `commit_tx_log`,
  every per-catalog/per-branch table ↔ its parent store). Adding
  three more FKs makes the schema's enforcement model harder to
  reason about, not easier.
* **Insert performance.** Every child insert pays a B-tree probe on
  the parent table to verify the FK target exists. Cheap
  individually, multiplied by N segments × M persists × per-catalog
  multipliers. With CHA-198 lifting the persist metadata to per-catalog,
  the absolute cost stays small but there's nothing to balance it
  against.
* **Migration rigidity.** Any schema migration that wants to clean
  up parent rows has to be FK-aware (cascade or temporarily disable
  the constraint). FK-free schemas migrate with plain `DELETE`s.
* **Application invariants are stronger anyway.** "Insert parent
  before child" + "recovery sweep cleans orphans" is a tighter
  contract than the FK provides — the FK fires last and only
  catches programmer error, not real-world failure modes.

## What this isn't saying

* It is **not** an argument against indexes. Every FK column we
  drop stays indexed (the recovery sweep needs the index for its
  parent-walk SELECTs).
* It is **not** an argument against CHECK constraints, NOT NULL,
  unique indexes, or `ON CONFLICT (...)` clauses. Constraints that
  catch malformed individual rows or write-path concurrency bugs
  stay.
* It is **not** an excuse to skip the staged-write convention. With
  no FK enforcement, the *application* must order its inserts
  parent-before-child and the *recovery sweep* must exist for any
  multi-step write. Both are required; the FK is the part we drop.

## Applies going forward

When adding a new pair of related metadata tables:

* Use the same column name on both sides (e.g. `data_log_prefix_uuid`
  appearing in both `table_persist_metadata` and `table_snapshot_metadata`).
* Add an index on the FK column in the child table.
* Document the parent/child relationship in the table's schema-reference
  comment.
* **Do not** write `REFERENCES <parent>(<column>)`.
* If a recovery sweep is needed for crashed writes against the new
  table, file the sweep as part of the feature work — that's the
  enforcement mechanism that replaces the FK.

If a future change discovers a hard need for engine-level FK
enforcement on a specific table, that change supersedes this ADR for
that table — and the rest of the schema gets re-evaluated for the
same treatment.
