# Architecture

How Penca is put together: the services, the two storage tiers, and the concepts the
API is built around. For the algorithms themselves (write path, read path, branch
merge, and their crash-safety invariants), see [algorithms.md](algorithms.md). For
building and running it, see [development.md](development.md).

## Architecture

Each microservice is a separate binary with its own config struct and
scaling profile. Per-service design docs live under
[`services/`](services/); architecture decisions in
[`decisions/`](decisions/).

| Service | Port | Purpose | Scaling profile |
|---|---|---|---|
| **query** | 50052 | Catalog / schema / table reads, branch / tx reads, `ReadData` + `AuditData` streaming reads | CPU-bound, stateless, horizontal |
| **write** | 50053 | Catalog / schema / table DDL, branching, transactions, data mutations | IO-bound, Postgres transactions |
| **lifecycle** | 50054 | Persist, snapshot, purge, compaction, tx-log GC, dirty-set discovery (`ListModifiedTables` / `ListPersistedTables`) | Mixed; CPU-spiky during snapshot |
| **lifecycle-scheduler** | n/a | Drives `Persist → Snapshot → Purge` on a periodic tick so the hot → cold pipeline advances without an operator. Pure gRPC client of query / lifecycle: no listen port | Single replica (v0, no leader election) |
| **penca-sql-server** | 50060 | Arrow Flight SQL endpoint: proxies query / write | CPU-bound (DataFusion planning), stateless, horizontal |

The query and lifecycle services read Postgres and object storage
directly. Read planning (deciding *what to read and where*) is an
in-process call inside the query service, not a service hop.

```
                                       ┌──────────────────────────┐
                                       │ SQL client (BI / ADBC /  │
                                       │  Flight SQL driver)      │
                                       └────────────┬─────────────┘
                                                    │  Flight SQL
                                                    ▼
 ┌──────────────────────────┐           ┌───────────────────────────┐
 │ Programmatic client      │           │ penca-sql-server          │
 │ (PencaClient, or any     │           │ (Flight SQL + DataFusion; │
 │  gRPC client built from  │           │  proxies query / write    │
 │  the proto files)        │           │  via gRPC)                │
 └────────────┬─────────────┘           └────────────┬──────────────┘
              │ gRPC (3 channels)                    │ gRPC (2 channels:
              │                                      │  query / write)
              ▼                                      ▼
 ┌────────────────────────────────────────────────────────────────────┐
 │  query            write            lifecycle                       │
 │  :50052           :50053           :50054                          │
 └────────────────────────────────────────────────────────────────────┘
                 ▲                                  │
                 │ gRPC (internal)                  ▼
 ┌───────────────┴───────────────┐  Postgres (hot tier + system metadata)
 │ lifecycle-scheduler           │     +  object storage (cold tier)
 │ (tick loop, no listen port;   │
 │  Persist → Snapshot → Purge)  │
 └───────────────────────────────┘
```

## Storage tiers

- **Hot (Postgres).** Recent unpersisted mutations. Low-latency reads
  and ACID writes. The query engine reads and writes Postgres directly
  via SQL.
- **Cold (object storage).** Any S3-compatible store (SeaweedFS in the
  shipped stack), or a local filesystem path; `OBJECT_STORAGE_PROVIDER`
  accepts `s3` and `local`. Holds the bulk of historical data as
  columnar files. Lance by default, Parquet supported, and the
  reader/writer trait in `penca-format` is where a third format would
  go. The query engine reads files directly.

Both tiers store the same auditable-store shape (upsert log + delete
log), so log segments in either tier may carry tombstones and
superseded versions. Reads resolve in two passes: a **per-tier
merge** runs the same SQL in hot and cold to pick the latest version
per row id and apply tombstones, then a **cross-tier merge** unions
the two with hot taking precedence over cold. See
[algorithms.md](algorithms.md#read-path).

The in-process read planner (`QueryManager::plan`, in `penca-api`) is the index that
knows where data lives across both tiers; it tells the query engine *what to read and
where*, computed in-process rather than over a service hop, and never touches the data
itself. Metadata reads live on the query layer alongside it, sharing its caches
([ADR 0028](decisions/0028-metadata-reads-on-query-layer.md)).

## Concepts

### Catalogs, branches, schemas, tables

Data is organized in a four-level hierarchy of **catalog → branch →
schema → table**:

- **Catalog.** Top-level organizational unit. Boundary for access
  control, billing, and resource isolation. Typically a deployment
  environment (dev / staging / prod). Per CHA-163, core metadata
  (branches, tx logs, table metadata) lives at this level.
- **Branch.** Versioning layer beneath catalog, modeled after git.
  A branch spans every schema in its catalog, so `BEGIN; INSERT
  s1.t; INSERT s2.t; COMMIT` is a single multi-schema atomic
  transaction. Every read and write targets exactly one branch;
  cross-branch reads are never valid. Defaults to `main`,
  auto-created at `CreateCatalog` time.
- **Schema.** Namespace beneath a branch. Pure Postgres-style
  namespace; cheap to create / drop, no per-schema heavyweight infra.
  `CreateCatalog` bootstraps two well-known schemas: `public` (the
  default target for unqualified DML, mirroring Postgres convention)
  and `__penca_system__` (reserved for Penca-internal metadata
  surfaced as first-class tables; see CHA-164/CHA-177).
- **Table.** Arrow-typed structured data. The unit the query engine
  reads from and writes to.

The primary value of branching is **read/write isolation**, giving
agents and researchers safe access to production data without copying
it or risking the live system. Branch concurrency is optimistic
(last-writer-wins at the row level). `MergeBranch` resolves the
source's current state via set-based SQL into the target's logs under
one merge transaction. See
[algorithms.md](algorithms.md#merge-branch).

Creating a fork is not purely bookkeeping: `create_branch` first drives a
`PersistBranch` on the **source**, flushing its committed-but-unpersisted rows to cold,
so that everything at or before the fork point is durable in the cold tier the child
reads through. Fork latency therefore tracks the parent's unpersisted backlog, not its
total size.

Forking is single-level today: a branch may only be forked from its catalog's `main`.
The read planner enumerates one immediate parent and does not recurse, so a fork off a
fork would silently drop grandparent rows; `create_branch` rejects it rather than
serve wrong data.

Deleting a branch immediately and permanently deletes all data on
that branch (table metadata, tx history, per-branch data tables)
atomically. No soft-delete, no undo.

### Identity

Namespace UUIDs (`catalog_uuid`, `schema_uuid`, `branch_uuid`, `table_uuid`) are
**server-minted at `Create*` time** and persisted on the namespace row
([ADR 0020](decisions/0020-non-deterministic-namespace-uuids.md)). They are random
rather than derived from the name, and that is exactly what makes them
**rename-stable**: names are mutable (`UpdateCatalog` / `UpdateSchema` / `UpdateTable`
each accept a `new_*_name`), while every physical address, lifecycle chain entry and
client reference keyed on a UUID survives the rename untouched.

Deterministic `xxh3_128` identity is still load-bearing, but scoped to the places the
storage layer must address without a lookup:

- **Structural anchors.** The `__penca_system__` schema and its two bootstrap tables,
  derived from `catalog_uuid` so server-internal write paths can reach them with no
  prior state.
- **Per-branch partition leaves.** The tx-log family and the per-table log tables.
- **Row identity.** User rows hash from `(table_uuid, pk_values)`, and derived rows in
  the persist + snapshot family chain off their parent via the recursive
  `row_uuid_for_pk` mechanism
  ([ADR 0013](decisions/0013-auditable-store-invariant-deterministic-version-uuid.md),
  [ADR 0016](decisions/0016-canonical-uuid-construction-for-derived-rows.md)).

Primary-key **values** stay immutable after insert: a row's identity hashes from them,
so changing one is a delete plus an insert, not an update.

API request messages accept human-readable names anywhere a UUID is expected. Since a
namespace UUID is no longer computable from its name, that resolution is a metadata
read rather than a pure hash; a round trip ADR 0020 accepted deliberately as the price
of rename support. Per-message comments in the `.proto` files document which identifier
combinations are sufficient for each RPC; when both a UUID and a name are supplied, the
UUID always wins.

### Tables: log vs store vs auditable store

Every table in Penca, system or user, is one of two primitives:

| Type | Mutations | Description |
|---|---|---|
| **Log** | Append only | Immutable once written. The substrate for auditable stores. |
| **Store** | Insert / update / delete | Mutable current-state. No history. |

User data tables and the system table-metadata table are **auditable
stores**; a composition of an upsert log + delete log + transaction
log that provides insert/update/delete semantics with full version
history and time-travel. Reads execute a symmetric per-tier
[merge-on-read](algorithms.md#read-path) that resolves the
latest committed upsert per row minus effective deletes. Storage
shape rationale: [ADR 0001](decisions/0001-unified-upsert-log.md),
[ADR 0008](decisions/0008-table-metadata-subpartitioning.md).

Only committed transactions are persisted from hot to cold storage;
transaction TTLs guarantee cold storage never contains uncommitted or
expired data.

### Retention

`RetentionConfig` has two fields:

- `retention_duration_seconds`: how far back history is kept. Absent = inherit from
  the schema; absent there too = retain indefinitely.
- `snapshot_density_seconds`: spacing between durable snapshot ladder rungs. Absent =
  inherit from the parent. When both are set it must be `<= retention_duration_seconds`,
  so the retention window always contains at least one rung.

Configured at **two** levels, schema and table. The effective policy resolves per-field
as `coalesce(table, schema)`. Retention is deliberately schema-broadest rather than
catalog-broadest: the read path needs the effective window at plan time, and keeping it
at schema level means it is already in the resolved scope with no extra catalog fetch,
while system tables under `__penca_system__` (which carry no retention) floor to nothing
without a special case. The catalog's `default_retention_config` field is reserved and
no longer read.

`retention_duration_seconds` is **immutable once set**. Tightening a window after forks
exist would pull the history floor out from under a descendant's audit reads, so the
write path rejects the change rather than allow it.

Two halves of retention exist, and only one is built:

- **The read floor is enforced.** `plan` computes a retention floor alongside the read
  plan, and a time-travel read whose `as_of` falls below it is rejected outright rather
  than served partial data.
- **Nothing prunes yet.** There is no prune-by-retention lifecycle op, so data below the
  floor is still physically present; it is simply unreachable. Snapshot reads the
  retention config, but only to decide which snapshots become durable ladder rungs, not
  to drop versions.

### Partitioning and clustering

- **Partition keys.** Columns used for query pruning. Must be
  string-representable (string / integer / date / timestamp / boolean)
  so the snapshot writer can group rows by a text partition label;
  per-segment column statistics carry the pruning bounds. Partition
  keys do **not** affect the physical file layout, partitioning is a
  metadata-level index (one snapshot-segment row per distinct
  partition value, with offset + length into the snapshot file).
- **Clustering keys.** Columns used to sort data within each
  partition. Improves scan efficiency for range queries and ordered
  access.

Both are specified at table creation and modifiable via `UpdateTable`
(modification on a non-empty table may trigger background
reorganization).

### Data lifecycle

Write → persist → compact → snapshot → purge. Writes land in Postgres
(hot) under a penca tx; persist moves committed data to
per-physical-table cold-storage segments under a two-phase, no-orphans
protocol; compact merges small segments; snapshot materializes a
read-optimized point-in-time view; purge reclaims hot rows once they clear the universal
grace window. The `lifecycle-scheduler` drives `persist → snapshot → purge`
autonomously on a periodic tick ([ADR 0019](decisions/0019-plan-time-pinning-and-universal-grace-window.md)).
Full algorithms with crash-safety invariants:
[algorithms.md](algorithms.md). System table shapes:
[schema-reference.md](schema-reference.md).
