# 0008 — Subpartition `table_metadata_*` by `(schema_uuid → branch_uuid)`

- **Status:** Accepted
- **Date:** 2026-04-29
- **Ticket:** [CHA-163](https://linear.app/chapala/issue/CHA-163)

## Context

CHA-163 lifts Penca's core metadata tables (`branches`, `commit_tx_log`, `begin_tx_log`, `abort_tx_log`, `table_metadata_*`) from per-schema scope to per-catalog scope. The tx-log family (`commit_tx_log`, `begin_tx_log`, `abort_tx_log`) keeps its single-axis partition strategy: one PG `LIST` partition per branch_uuid. A multi-schema transaction has exactly one branch_uuid, so the commit_tx_log row lives in exactly one partition regardless of which schemas it touches — which is the entire point of the lift.

The two `table_metadata` logs are different. They have a real two-axis cardinality: `(schema, branch)` pairs. Every (schema, branch) combination accumulates rows as table CRUD happens in that branch. Picking a partition strategy here forces a tradeoff between which axis gets cheap `DROP` and which axis pays for it via `DELETE` + vacuum.

We considered five options:

1. **Single-axis by `schema_uuid`** (per the original CHA-163 sketch). Cheap `DROP SCHEMA` (one `DROP PARTITION`). `DROP BRANCH` becomes `DELETE FROM ... WHERE branch_uuid = ?` across every schema, then vacuum.
2. **Single-axis by `branch_uuid`** (today's per-schema layout, lifted to per-catalog). Cheap `DROP BRANCH`. `DROP SCHEMA` becomes `DELETE` across every branch, then vacuum.
3. **Flat composite `LIST (schema_uuid, branch_uuid)`**. PG supports multi-column `LIST` (PG 14+); each leaf partition holds one (schema, branch) tuple. Same N×M leaf count as option 4. `DROP SCHEMA` requires enumerating every `(schema_X, *)` leaf and dropping each individually — no parent-level cascade. `DROP BRANCH` analogous.
4. **Subpartition `schema_uuid → branch_uuid`** (chosen). Tree-shaped: parent `LIST (schema_uuid)` → schema partition `LIST (branch_uuid)` → branch sub-partition. `DROP SCHEMA` is a single `DROP TABLE {schema_partition}` that cascades through PG's partition tree atomically. `DROP BRANCH` is `DELETE` across each schema's sub-partition, then vacuum.
5. **Inverted subpartition `branch_uuid → schema_uuid`**. Cheap `DROP BRANCH`, expensive `DROP SCHEMA`. The mirror image of option 4.

## Decision

**Subpartition `table_metadata_upsert_log` and `table_metadata_delete_log` by `schema_uuid` at the parent level, then by `branch_uuid` at the schema-partition level.** Option 4.

Concretely, the DDL shape is:

```sql
CREATE TABLE {catalog}_table_metadata_upsert_log (...)
  PARTITION BY LIST (schema_uuid);

-- One per schema, created at CreateSchema:
CREATE TABLE {catalog,schema}_meta_upsert_schema_part
  PARTITION OF {catalog}_table_metadata_upsert_log
  FOR VALUES IN (:schema_uuid)
  PARTITION BY LIST (branch_uuid);

-- One per (schema, branch), created at CreateBranch (for every existing
-- schema) and at CreateSchema (for every existing branch):
CREATE TABLE {catalog,schema,branch}_meta_upsert_part
  PARTITION OF {catalog,schema}_meta_upsert_schema_part
  FOR VALUES IN (:branch_uuid);
```

The intermediate schema partition (`_meta_upsert_schema_part`) holds no data of its own — it exists purely as a cascade target for `DROP SCHEMA`.

Same shape mirrored for `table_metadata_delete_log`.

## Why subpartition over flat composite

Both options 3 and 4 have the same leaf count and the same partition-pruning behavior on a two-key predicate (`WHERE schema_uuid = X AND branch_uuid = Y`) — both prune to one leaf. The decisive difference is `DROP SCHEMA`:

- **Flat composite (option 3)**: `DROP SCHEMA s1` requires either (a) enumerating every `(s1, *)` leaf via `pg_partitioned_table` introspection and dropping each one in a loop, or (b) listing them explicitly in a single multi-table `DROP TABLE`. Either way, no PG-level cascade — the application code does the enumeration.
- **Subpartition (option 4)**: `DROP TABLE {s1_meta_upsert_schema_part}` cascades through PG's partition tree atomically. One DDL statement, atomic against concurrent DML on other schemas, no application-level enumeration.

The intermediate-partition layer adds N "routing-only" relations to PG's catalog, but no data and no behavior cost on the read path (planner sees through subpartitioning transparently). At Penca's scale (catalogs have O(10s of schemas), schemas have O(10s of branches)), the routing overhead is irrelevant.

## Why `schema_uuid → branch_uuid` over `branch_uuid → schema_uuid`

Both subpartition orderings give cheap `DROP` on one axis at the cost of `DELETE` + vacuum on the other. The catalog-as-environment model determines which axis is which:

- **Schemas are slow-changing tenant structure.** A catalog (e.g. `prod`) has stable schemas (`analytics`, `staging`, `core`). Schemas correspond to organizational units, datasets, or product surfaces. They live for a long time. When one is deleted, it's typically a tenant offboarding or major data restructure — rare, but the operation should be clean (no scattered DELETE, no vacuum lag).
- **Branches are short-lived feature workspaces.** Agentic flows fork branches, do work, merge or abandon, delete. Branch lifetime is on the order of minutes to days. Per-branch row counts in `table_metadata_*` are bounded by per-branch DDL churn, which is small (table creates and updates per branch, not per data row).

So:
- `DROP SCHEMA` (rare, heavy) needs to be cheap → subpartition such that schema is the parent.
- `DROP BRANCH` (frequent, light) is fine via `DELETE` because the per-branch row count is small.

Inverting to `branch_uuid → schema_uuid` (option 5) gives us the wrong tradeoff: cheap on the frequent-but-light operation, expensive on the rare-but-heavy one.

## Why partition on both axes at all

Single-axis by `schema_uuid` (option 1) is what the original CHA-163 sketch had. It gives cheap `DROP SCHEMA` but no further partition pruning — every read for a specific (schema, branch) scans the whole schema's partition. With two-level partitioning, branch-direct reads prune to a single sub-partition, which matters for the common per-(schema, branch) read pattern.

`DELETE FROM ... WHERE branch_uuid = ?` against a single-axis-by-schema partition (option 1's `DROP BRANCH` path) also locks the whole schema partition while it runs; with subpartitioning, it locks only one branch sub-partition per schema, which is more concurrency-friendly on a busy catalog.

## Consequences

- **`DROP SCHEMA` is one cascading `DROP TABLE` per `table_metadata_*` log** (plus the schema's row in `schema_store`). No `DELETE` + vacuum on shared partitions; other schemas' sub-partitions are untouched.
- **`DROP BRANCH` is `DELETE FROM {meta_upsert_part} WHERE TRUE` across each schema's sub-partition for that branch**, then vacuum. Per-branch row counts are small enough that the `DELETE` is fast and the vacuum lag is tolerable. (`DROP TABLE` of each leaf sub-partition is also possible but adds N drop-table statements per branch deletion.)
- **`CreateBranch` adds N sub-partitions to existing schemas** — one per existing schema in the catalog. Mirror: **`CreateSchema` adds M sub-partitions** — one per existing branch in the catalog. Both run inside `ensure_branch_partitions(catalog, branch)` / `ensure_schema_partitions(catalog, schema)` in the bootstrap path.
- **The partition-direct convention (`feedback_target_partitions_directly`) extends to the new leaf**: reads/writes to `table_metadata_*` target the leaf sub-partition by name (`table_metadata_upsert_log_partition(catalog, schema, branch)`), never the parent or the intermediate. The intermediate schema partition appears only in DDL.
- **PG version requirement is unchanged.** Subpartitioning has been supported since PG 10; multi-column `LIST` (the alternative) needs PG 14+, so subpartitioning is also more portable.

## Future considerations (would prompt revisiting)

These would force a re-think of the subpartition choice:

- **`DROP BRANCH` becoming a hot path with large per-branch row counts.** If `table_metadata_*` ever accumulates thousands of rows per branch (e.g., automated table creation per query, table metadata versions), the `DELETE` cost on `DROP BRANCH` becomes meaningful. At that point, dropping branch sub-partitions individually per schema is still O(N schemas) DDL statements — at some scale that's worse than single-axis-by-branch.
- **Catalogs with very many schemas (hundreds to thousands).** N×M sub-partitions where N is large stresses PG's partition tree (10k+ partitions per parent is when things start to slow down). At that scale, hash partitioning or a different layout wins. Not a near-term concern — the catalog-as-environment model expects O(10s) of schemas per catalog.
- **Cross-axis queries that scan all schemas for one branch** (e.g. "give me every table that exists on branch X"). With this subpartition order, that read scans every schema's sub-partition for that branch (PG can prune to "the branch sub-partition under each schema partition," but it visits N partitions). If this becomes a hot path, an inverted index or a separate per-branch view would be the answer, not flipping the subpartition order.

## Pointers

- **CHA-163** — the per-catalog metadata lift this ADR partition-strategies for.
- **`feedback_target_partitions_directly`** — the project-wide invariant that DML uses leaf-partition names directly. The new `table_metadata_upsert_log_partition(catalog, schema, branch)` and `table_metadata_delete_log_partition(catalog, schema, branch)` naming functions in `crates/penca-core/src/naming.rs` and the Python mirror return leaf names.
- **PG docs on partition cascades** — https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE.
