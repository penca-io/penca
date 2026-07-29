# Architecture

How Penca is put together: the services, the two storage tiers, and the concepts the
API is built around. For the algorithms themselves — write path, read path, branch
merge, and their crash-safety invariants — see [algorithms.md](algorithms.md). For
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
| **lifecycle-scheduler** | — | Drives `Persist → Snapshot → Purge` on a periodic tick so the hot → cold pipeline advances without an operator. Pure gRPC client of query / lifecycle — no listen port | Single replica (v0, no leader election) |
| **penca-sql-server** | 50060 | Arrow Flight SQL endpoint — proxies query / write | CPU-bound (DataFusion planning), stateless, horizontal |

The query and lifecycle services read Postgres and object storage
directly. Read planning (deciding *what to read and where*) is an
in-process library call (`penca-storage-meta`), not a service hop.

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

- **Hot (Postgres)** — recent unpersisted mutations. Low-latency reads
  and ACID writes. The query engine reads and writes Postgres directly
  via SQL.
- **Cold (object storage)** — S3 / GCS / SeaweedFS / any S3-compatible
  store. Holds the bulk of historical data as columnar files (Lance
  default; Parquet supported, Vortex / Nimble pluggable). The query
  engine reads files directly.

Both tiers store the same auditable-store shape (upsert log + delete
log), so log segments in either tier may carry tombstones and
superseded versions. Reads resolve in two passes: a **per-tier
merge** runs the same SQL in hot and cold to pick the latest version
per row id and apply tombstones, then a **cross-tier merge** unions
the two with hot taking precedence over cold. See
[algorithms.md](algorithms.md#read-path).

The in-process read planner (`penca-storage-meta`, `MetadataClient::plan`)
is the index that knows where data lives across both tiers — it tells the
query engine *what to read and where*, computed in-process rather than over
a service hop, and never touches the data itself.

## Concepts

### Catalogs, branches, schemas, tables

Data is organized in a four-level hierarchy — **catalog → branch →
schema → table**:

- **Catalog** — top-level organizational unit. Boundary for access
  control, billing, and resource isolation. Typically a deployment
  environment (dev / staging / prod). Per CHA-163, core metadata
  (branches, tx logs, table metadata) lives at this level.
- **Branch** — versioning layer beneath catalog, modeled after git.
  A branch spans every schema in its catalog, so `BEGIN; INSERT
  s1.t; INSERT s2.t; COMMIT` is a single multi-schema atomic
  transaction. Every read and write targets exactly one branch;
  cross-branch reads are never valid. Defaults to `main`,
  auto-created at `CreateCatalog` time.
- **Schema** — namespace beneath a branch. Pure Postgres-style
  namespace; cheap to create / drop, no per-schema heavyweight infra.
  `CreateCatalog` bootstraps two well-known schemas: `public` (the
  default target for unqualified DML, mirroring Postgres convention)
  and `__penca_system__` (reserved for Penca-internal metadata
  surfaced as first-class tables — see CHA-164/CHA-177).
- **Table** — Arrow-typed structured data. The unit the query engine
  reads from and writes to.

The primary value of branching is **read/write isolation** — giving
agents and researchers safe access to production data without copying
it or risking the live system. Branch concurrency is optimistic
(last-writer-wins at the row level). `MergeBranch` resolves the
source's current state via set-based SQL into the target's logs under
one merge transaction. See
[algorithms.md](algorithms.md#merge-branch).

Forking is single-level today: a branch may only be forked from its catalog's `main`.
The read planner enumerates one immediate parent and does not recurse, so a fork off a
fork would silently drop grandparent rows — `create_branch` rejects it rather than
serve wrong data.

Deleting a branch immediately and permanently deletes all data on
that branch (table metadata, tx history, per-branch data tables)
atomically. No soft-delete, no undo.

### Identity

Every entity with an immutable key has a deterministic `xxh3_128`
UUID derived from that key — `catalog_uuid = xxh3(catalog_name)`,
`schema_uuid = xxh3(catalog_uuid:schema_name)`, and so on through
table and branch. User-row UUIDs derive from `(table_uuid, pk_values)`;
derived rows in the persist + snapshot family chain off their parent
UUID via the recursive `row_uuid_for_pk` mechanism (ADR 0016). Each
UUID transitively encodes its parent identity through its hash input.

This means name → UUID is a pure computation: no database lookups, no
caches, no staleness. The same entity on different branches has the
same `table_uuid`; deleting and recreating produces the same UUID
(the merge-on-read CTE handles re-insert-after-delete correctly via
time-aware deletes).

User-supplied keys — catalog / schema / table / branch names, primary
keys — are **immutable** after creation. Changing them would
invalidate UUID references throughout the system. Only `tx_uuid` uses
a random UUID (events with no immutable key).

API request messages accept human-readable names anywhere a UUID is
expected — the server resolves names to UUIDs via pure hash
computation. Per-message comments in the `.proto` files document
which identifier combinations are sufficient for each RPC; when both
a UUID and a name are supplied, the UUID always wins.

### Tables: log vs store vs auditable store

Every table in Penca — system or user — is one of two primitives:

| Type | Mutations | Description |
|---|---|---|
| **Log** | Append only | Immutable once written. The substrate for auditable stores. |
| **Store** | Insert / update / delete | Mutable current-state. No history. |

User data tables and the system table-metadata table are **auditable
stores** — a composition of an upsert log + delete log + transaction
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

- `retention_duration_seconds` — how far back history is kept. Absent = inherit from
  the schema; absent there too = retain indefinitely.
- `snapshot_density_seconds` — spacing between durable snapshot ladder rungs. Absent =
  inherit from the parent. When both are set it must be `<= retention_duration_seconds`,
  so the retention window always contains at least one rung.

Configured at **two** levels — schema and table. The effective policy resolves per-field
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
  floor is still physically present — it is simply unreachable. Snapshot reads the
  retention config, but only to decide which snapshots become durable ladder rungs, not
  to drop versions.

### Partitioning and clustering

- **Partition keys** — columns used for query pruning. Must be
  string-representable (string / integer / date / timestamp / boolean)
  so the snapshot writer can group rows by a text partition label;
  per-segment column statistics carry the pruning bounds. Partition
  keys do **not** affect the physical file layout — partitioning is a
  metadata-level index (one snapshot-segment row per distinct
  partition value, with offset + length into the snapshot file).
- **Clustering keys** — columns used to sort data within each
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
[algorithms.md](algorithms.md).
