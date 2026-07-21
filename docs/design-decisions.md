# Design Decisions

This document records critical design tradeoffs and their rationale. The
README describes *what* Penca does; this document explains *why* it is built
the way it is.

Each entry states the decision, the alternatives considered, and the reason
the chosen approach won.

---

## Branch partitioning over independent tables

Per-catalog log tables (`commit_tx_log`, `begin_tx_log`, `abort_tx_log`,
`table_metadata_upsert_log`, `table_metadata_delete_log`) use Postgres LIST
partitioning by `branch_uuid` rather than fully independent per-branch tables
(the pattern used for data tables). The two `table_metadata_*` tables are
subpartitioned `schema_uuid → branch_uuid` per
[decisions/0008-table-metadata-subpartitioning.md](decisions/0008-table-metadata-subpartitioning.md).

**Why not independent tables?** Some per-catalog operations are not
branch-scoped. The internal `get_tx_status` lookup (used by `CommitTx` /
`AbortTx` to classify a `tx_uuid` as Open / Aborted / Expired / Committed)
joins `begin_tx_log`, `abort_tx_log`, and `commit_tx_log` for a single
`(catalog, branch, tx_uuid)` triple — and the merge-fast-forward and
sweep helpers walk every branch's `commit_tx_log` partition under a catalog.
With partitioning, these queries hit the parent table and Postgres scans
all partitions transparently. With independent tables, the caller would
need to enumerate every branch's table or restructure each internal
helper to require a branch — without any read path actually benefiting
from the schema change.

**Why not keep them unpartitioned?** Without partitioning, concurrent workloads
on different branches share the same heap and indexes, causing B-tree page
splits, shared vacuum pressure, and lock contention. Partitioning gives each
branch its own physical storage while preserving cross-branch queryability
through the parent table.

Per-table data tables (`{data_log_prefix_uuid}_data_upsert_log`,
`{data_log_prefix_uuid}_data_delete_log`) are already per-branch by design —
`data_log_prefix_uuid = xxh3(table_uuid, branch_uuid)` (CHA-177) — and do not
need partitioning. (The unified `upsert_log` layout — which also applies to
the per-catalog `table_metadata_upsert_log` — is recorded in
[decisions/0001-unified-upsert-log.md](decisions/0001-unified-upsert-log.md).)

---

## Partition-direct reads and writes

Hot-path operations (reads and writes where the branch is known) target
partition tables directly instead of going through the parent table.

**Why?** `CREATE TABLE ... PARTITION OF` and `DROP TABLE partition` both take
an `ACCESS EXCLUSIVE` lock on the parent table. This lock conflicts with every
other lock mode, including `ACCESS SHARE` acquired by `SELECT`. Even though the
lock is sub-millisecond, it means creating or deleting a branch momentarily
blocks all reads on the parent — including reads for unrelated branches that
would otherwise be partition-pruned.

By targeting partition tables directly when the branch is known, hot-path
operations never touch the parent table and are unaffected by DDL on other
branches. Only the internal cross-branch reads (e.g., the `tx_uuid`
disambiguation legs of `get_tx_status`) go through the parent table, and
these are infrequent.

---

## Eager branch materialization

`create_branch` eagerly creates the branch's per-catalog log partitions
(`commit_tx_log`, `begin_tx_log`, `abort_tx_log`) plus a `table_metadata_*` branch
sub-partition under every existing schema, copies table metadata records, and
creates empty physical data tables for all tables visible on the source branch.

**Why not lazy?** Lazy creation (materializing on first write) requires branch
hierarchy traversal to find the parent branch's metadata, as-of-timestamp
scoping to respect the branch's base transaction, and handling of race
conditions when concurrent writes trigger lazy creation simultaneously. This
complexity is hard to get right and produces subtle bugs — for example, a table
that exists on the source branch appearing to "not exist" on the new branch
until someone explicitly re-creates it.

Eager materialization avoids all of this. Branch creation is not a hot-path
operation and is expected to be moderately expensive (it creates real Postgres
tables). The tradeoff is acceptable: branch creation takes slightly longer, but
every subsequent read and write on the branch works correctly without fallback
logic.

---

## "Default to main" lives only in the client facade

## First-class domain objects in proto, not response wrapping everywhere

External-facing domain objects (`Catalog`, `Schema`, `Table`, `Branch`)
live in `common.proto` as standalone messages and are composed into response
wrappers (`GetCatalogResponse`, `ListTablesResponse`, etc.) in the service
protos. This makes them reusable across multiple RPCs and gives the client a
single type to depend on. (No `Tx` here: transactions are an internal
mechanism — see [decisions/0018-tx-is-internal-no-introspection-rpc.md](decisions/0018-tx-is-internal-no-introspection-rpc.md).)

**Why not apply this to internal protos?** The messages in
`storage_metadata.proto` (`PlanResponse`, `HotStoragePlan`,
`LogSegment`, `SnapshotSegment`, `GetTableMetadataResponse`) are internal operational
structures, not domain objects. Nothing outside the storage layer references
them, and no message shape is reused across multiple responses. Extracting
them into standalone types would be premature abstraction — the pattern is
only valuable when the same object appears in multiple contexts.

---

## Transactional branch creation and deletion

`create_branch` and `delete_branch` wrap all their work — branch_store
row, partition DDL, table materialization / data table drops — in a single
Postgres transaction. If any step fails, everything rolls back.

**Why is this safe?** Postgres supports transactional DDL (`CREATE TABLE`,
`DROP TABLE` roll back on abort). The DDL targets per-branch partition tables
and per-branch data tables, not shared resources, so the lock footprint is
limited to tables that don't exist yet (create) or are about to be removed
(delete).

**Why not leave them non-transactional?** Without a transaction, a failure
partway through `create_branch` leaves a partially-materialized branch
(branch_store row exists, some partitions created, some tables missing).
Similarly, a failed `delete_branch` could drop data tables but leave the
branch_store row, or vice versa. Both states require manual cleanup.

---

## "Default to main" lives only in the client facade

Proto branch fields (`branch_uuid`, `branch_name`) are optional as part of the
UUID-or-name identifier pattern. The client facade defaults `branch_name` to
`"main"` when neither field is provided. Internal code — `resolve_branch`,
managers, auditable stores — never defaults and requires a concrete branch.

**Why?** This eliminates ambiguity at every internal layer. In particular,
`branch_uuid=None` in the auditable store unambiguously means "I am targeting a
partition table directly, do not add a branch filter" rather than "I forgot to
pass a branch, please default to main." Keeping the default in one place (the
client facade) makes it easy to find and reason about.

---

## Python Flight SQL uses a context factory, not a shared `SessionContext`

The Python reference Flight SQL server (`DataFusionFlightSqlServer`) obtains a
`datafusion.SessionContext` via a caller-supplied factory invoked on every
Flight SQL RPC, rather than holding a single shared context. Semantically:
each concurrent request gets its own isolated planning context; mutations on
one request's context can't leak into another. Also enables dynamic catalog
and schema updates — the factory closes over penca's metadata client, so
freshly-registered tables become visible to new requests without restarting
the server. This matches the Rust server's intended behavior for
dynamically-evolving catalogs.

**Why not share one `SessionContext` across requests?** DataFusion's
`SessionContext` is not guaranteed safe for concurrent planning across
threads, and `FlightServerBase` dispatches RPCs on a native thread pool. A
shared context is a real thread-safety hazard.

**Why not clone the context per request (what Rust does)?** The Rust server
clones a `SessionState` (cheap, Arc-backed) for each call. The Python
`datafusion` package doesn't expose `SessionState` or any real clone
operation — `copy.copy` returns a Python alias pointing at the same
underlying Rust object (mutations leak), and `copy.deepcopy` can't pickle.
No clone primitive exists in the binding.

**Why a factory instead of rebuilding inline?** The factory is configured
once at server construction time (by penca's wiring code, not end users),
keeping the server-side interface clean. Each RPC calls it to get a fresh
context. Guidance for authors: factories should only register pre-built
catalog providers (which resolve tables lazily via `LifecycleManager`) — sub-
millisecond overhead. Avoid eager I/O inside the factory (`register_parquet`
reads footers, `register_listing_table` hits object storage,
`CREATE VIEW` DDL re-plans), because it runs on every RPC.

Tracked upstream: a real `clone_ctx()` in `datafusion-python` would let
Python match Rust's mechanics exactly; until then the factory is the correct
shape.
