# ADR 0016 — Canonical UUID construction for derived rows

## Status

Accepted (CHA-203).

## Context

Penca persists three families of UUIDs:

1. **Identity UUIDs** — `catalog_uuid`, `schema_uuid`, `branch_uuid`,
   `table_uuid`. Derived deterministically from human-supplied names
   (see [naming.rs](../../crates/penca-core/src/naming.rs) for the
   chain). These have been canonical-deterministic since Penca's
   first design.
2. **Auditable-store row UUIDs** — `version_uuid`,
   `row_uuid` (`version_uuid`, `row_uuid_for_pk`). Deterministic
   from `(row_uuid, tx_uuid)` and `(table_uuid, pk_values)`
   respectively. [ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md)
   captures the rationale: the PK alone enforces "at most one version
   per `(entity, tx)`".
3. **Derived-row UUIDs** — `branch_persist_uuid`, `table_persist_uuid`,
   `table_persist_segment_uuid`, `table_snapshot_uuid`,
   `table_snapshot_segment_uuid`. Pre-CHA-203 these were minted via
   `Uuid::new_v4()` at write time, with `ON CONFLICT DO NOTHING` on
   the segment-metadata inserts. The random-UUID shape forced two
   awkward properties:

   - Phase-1 retries minted **new** UUIDs each time. A retry after a
     partial failure inserted a fresh row with a fresh PK; the
     pre-existing row (from the first attempt) sat orphaned under
     uncommitted parents until the recovery sweep walked the tree.
   - Operators could not pre-compute the persisted UUIDs from the
     inputs they passed in (`persisted_at_micros`, `table_uuid`,
     `branch_uuid`, etc.). Every white-box assertion against the
     persisted state required reading the row first to learn what
     UUID the server picked.

Both properties weakened the "every UUID in Penca is derivable from
its inputs" symmetry that the identity and auditable-store families
already enjoyed.

## Decision

**Every persisted UUID in the persist + snapshot family derives
recursively from its parent + its own discriminator(s) via
[`naming::row_uuid_for_pk`].** The chain mirrors the parent → child
structure of the metadata tables 1:1:

```
branch_persist_uuid        = row_uuid_for_pk(catalog_uuid,
                                           [branch_uuid, persisted_at_micros])
table_persist_uuid         = row_uuid_for_pk(branch_persist_uuid,
                                           [table_uuid, log_kind])
table_persist_segment_uuid = row_uuid_for_pk(table_persist_uuid,
                                           [chunk_idx])
table_snapshot_uuid      = row_uuid_for_pk(catalog_uuid,
                                           [branch_uuid, table_uuid,
                                            snapshotted_at_micros])
table_snapshot_segment_uuid = row_uuid_for_pk(table_snapshot_uuid,
                                              [chunk_idx])
```

`chunk_idx` (CHA-215) is the only sibling-uniquifier on the two
segment levels. Persist and snapshot each chunk their `RecordBatch` at
write time so no emitted segment exceeds `max_segment_bytes`; the
sibling chunks under one parent are identified by their 0-indexed
emit order. The earlier shape — hashing `(min_commit_micros,
max_commit_micros)` for persist and `(min_partition_value,
max_partition_value)` for snapshot — collapsed to identical inputs
across chunks of a single-tx oversized persist, so it could not have
distinguished siblings once the chunker existed. Those columns stay
on the segment rows because `plan_wave` / merge / time-travel still
query them for tx-window and partition bounds; they're data, not
identity.

Inserts at every level use `ON CONFLICT (...) DO UPDATE`. A phase-1
retry with identical inputs computes the same UUID at every level and
collapses to a no-op write — the pre-existing row stays under
uncommitted parents, the retry sees the row already present, and the
post-condition is identical regardless of how many times the writer
retries.

The Rust + Python implementations both expose the chain via dedicated
helpers (`table_persist_uuid`, `table_persist_segment_uuid`,
`table_snapshot_uuid`, `table_snapshot_segment_uuid`). Cross-language
parity is locked by `tests/static/static_naming_parity_test.py` and the
matching `test_parity_*` cases in `crates/penca-core/src/naming.rs`.

## Application bindings

- **User-data rows (CHA-177 / ADR 0012).** `row_uuid_for_pk(table_uuid,
  [pk_values])` — already deterministic; this ADR doesn't change it.
- **Derived versions (ADR 0013).** `version_uuid(row_uuid,
  tx_uuid)` — already deterministic via the same hash family; this
  ADR doesn't change it.
- **Per-branch data table names (CHA-177).** `upsert_log_table` /
  `delete_log_table` take `(table_uuid, branch_uuid)` and hash the
  prefix internally; not part of this ADR's chain but follows the
  same `row_uuid_for_pk` mechanism.
- **Persist + snapshot family (this ADR).** Chain shown above.

## Rename discipline

Every new/reshaped helper above is a public symbol; mirror its
Python and Rust definitions when changing inputs or output. The
cross-language parity tests pin the goldens. The helper name encodes
its parent — `table_persist_uuid` chains off `branch_persist_uuid`,
not off `catalog_uuid` — so the call-graph reads top-down without
needing a separate cardinality table.

## Why deterministic over random

| Property | Random `new_v4()` (pre-CHA-203) | Deterministic chain (chosen) |
| --- | --- | --- |
| Phase-1 retry shape | Fresh row per attempt; orphans accumulate under uncommitted parents until sweep | `DO UPDATE` no-op; no orphans |
| Operator pre-knowledge | Must read the row to learn the persisted UUID | Computable from the inputs the writer used |
| Cross-language parity | n/a (UUIDs differ per call) | Locked by golden tests; Rust + Python agree |
| Storage symmetry | Persist family is the odd one out | Every UUID in Penca is hash-derived |
| Cost | One PK insert per row | One PK insert per row + a hash (~150 ns) |

The cost-per-write is comparable. The retry-shape and parity wins
are why we picked deterministic.

## Non-goals

- **Auditable-store invariant.** ADR 0013 specifies "at most one
  version per `(entity, tx)`" for upsert/delete logs, enforced via
  `version_uuid` + PK. This ADR does **not** extend that
  invariant to the persist + snapshot family: those tables do not
  carry `tx_uuid`, there is no `commit_tx_log` join at read time, and
  segments under the same parent do not atomically commit (CHA-168
  splits phase 1 / phase 2 across many auto-commits). The
  deterministic-UUID mechanism is reused as a hash-formula
  convention only.
- **Backfill.** Pre-release; no migration path for catalogs created
  before CHA-203 landed. Drop and recreate.
- **`row_uuid` column on segment tables.** The chain doesn't need
  one — the segment's `table_persist_segment_uuid` IS its identity in
  the parent table.
- **`LogKind` proto enum.** `log_kind` is a closed-set storage column
  (`upsert_log` / `delete_log` / `commit_tx_log`) restricted by a PG CHECK
  constraint. No wire-level enum.

## Related decisions

- [ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md)
  — auditable-store rows use the same hash family for a different
  reason (PK-enforced invariant). This ADR cross-references it for
  hash-mechanism consistency without claiming the same invariant.
- [ADR 0012](0012-metadata-as-first-class-tables.md) — establishes
  the `__penca_system__.{schemas,tables}` shape that uses
  `row_uuid_for_pk` for entity identity. This ADR extends the same
  mechanism to derived rows.
- [ADR 0015](0015-no-foreign-keys-in-penca-metadata.md) — Penca
  metadata uses no FK constraints; the deterministic-UUID chain is
  one of the three integrity mechanisms that takes their place.
