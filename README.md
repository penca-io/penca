<p align="center">
  <img src="docs/penca-logo.svg" alt="Penca" width="200">
</p>

[![CI](https://github.com/penca-io/penca/actions/workflows/ci.yml/badge.svg)](https://github.com/penca-io/penca/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94+-orange)](rust-toolchain.toml)
[![Python](https://img.shields.io/badge/python-3.10+-blue)](pyproject.toml)

An open-source lakebase that unifies production (OLTP) and analytical
(OLAP) workloads on a single system. Self-hostable with minimal
infrastructure (Postgres + object storage).

Penca runs as three gRPC microservices (query, write, lifecycle)
fronting a shared Postgres + object storage stack,
plus a Flight SQL gateway (`penca-sql-server`) that translates SQL
into those gRPC calls and a lifecycle scheduler that drives the
hot → cold → snapshot → purge pipeline forward on its own. The whole
server is implemented in Rust (tonic + DataFusion). SQL clients (JDBC /
ODBC / ADBC) connect to the Flight SQL gateway; programmatic clients
connect to the gRPC services directly.

## Prerequisites

### To run

| Tool | Why | Install |
|---|---|---|
| [Docker](https://docs.docker.com/engine/install/) | Postgres + SeaweedFS + servicer containers for `just penca-up` | platform installer |
| [`uv`](https://docs.astral.sh/uv/) | Python toolchain for the client + demo (Python 3.10+) | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| [`just`](https://github.com/casey/just) | Recipe runner (`just penca-up`, `just integration-test`, …) | `uv tool install rust-just` |

### To develop

Install `just` (see [To run](#to-run)), then from the repo root run [`just bootstrap`](Justfile). Idempotent: re-running picks up where it left off (each step checks before installing), so it's safe after a binary upgrade or a config pull.

Go 1.26.3+ is the one prereq `bootstrap` does not auto-install — platform variance across `apt` / `brew` / tarball makes a single one-liner unreliable. Install from [go.dev/dl](https://go.dev/dl/) and re-run.

#### What `just bootstrap` installs

| Tool | Why |
|---|---|
| [Rust 1.94+](rust-toolchain.toml) | Build the server from source (Docker pulls a prebuilt image for the run path). `rust-toolchain.toml` pins the version. |
| [`rust-analyzer`](https://rust-analyzer.github.io/) | Powers the `LSP` tool and the `language-server` MCP server for Claude Code agents (`goToDefinition`, `findReferences`, `rename_symbol`). |
| [`mcp-language-server`](https://github.com/isaacphi/mcp-language-server) | MCP server registered in [`.mcp.json`](.mcp.json) that exposes rust-analyzer's `rename_symbol` and friends to agents. |
| [`kata`](https://github.com/kenn-io/kata) | Per-ticket task queue used by `/do-issue` (plan → red → drain). |
| [`roborev`](https://github.com/kenn-io/roborev) | Continuous post-commit reviewer; findings feed back into `kata` via the bridge in `scripts/roborev-kata-hook.sh`. |
| [`headroom`](https://headroom-docs.vercel.app/docs) | Opt-in context-compression proxy for the Claude Code loop, **off by default** (see the opt-in section below). |

Plus the wiring no contributor should have to remember: kata PATH symlink + daemon start + `penca` project binding, roborev daemon registration + `post-commit` hook, the per-repo memory symlink (ADR 0016), and pre-commit hooks for the `pre-commit` + `commit-msg` stages.

#### Shared issue-graph client (experimental, opt-in)

An experiment ([CHA-447](https://linear.app/chapala/issue/CHA-447)) toward letting `/do-issue` planning navigate the whole issue graph from a shared kata instance instead of paying a Linear round-trip per hop. **Off by default** — with the env below unset, the VM stays local-only and nothing changes.

- `PENCA_KATA_GRAPH_URL` — base URL of the shared kata daemon (the issue corpus).
- `PENCA_KATA_GRAPH_TOKEN` — this VM's identity token for it.
- `PENCA_KATA_GRAPH_ALLOW_INSECURE` — set only for a dev-over-http instance.

When `PENCA_KATA_GRAPH_URL` is set, `just bootstrap` (via `init-agent-tools`) probes the instance and reports reachability. Read the shared graph **only** through [`scripts/kata-issue-graph.sh`](scripts/kata-issue-graph.sh) (`show` / `list` / `search` / …) — a scoped, read-only wrapper. Scoping matters: kata's `KATA_SERVER` is process-global, so setting it globally (or dropping a repo-root `.kata.local.toml`) would also route the local `cha-NNN` task-queue drain to the shared daemon. The wrapper sets the remote env inline on exec only, keeping the **local task-queue daemon authoritative** and refusing any mutating subcommand.

This scaffold is inert until the shared instance exists and is populated — tracked in [CHA-450](https://linear.app/chapala/issue/CHA-450) (stand up the daemon), [CHA-451](https://linear.app/chapala/issue/CHA-451) (Linear → kata sync), and [CHA-449](https://linear.app/chapala/issue/CHA-449) (wire `/do-issue` Step 1 to consume it).

#### Headroom context-compression proxy (experimental, opt-in)

[Headroom](https://headroom-docs.vercel.app/docs) ([CHA-465](https://linear.app/chapala/issue/CHA-465)) is a local proxy that compresses what an agent reads — tool outputs, file reads, query results — *before* it reaches the model, trading some risk for fewer tokens. `just bootstrap` installs it (`uv tool install "headroom-ai[proxy]"`) so it's available, but **off by default**: nothing redirects Claude Code until you opt in.

To use it, launch the proxy and point Claude Code at it:

```bash
just headroom-proxy                                  # serves on :8787
ANTHROPIC_BASE_URL=http://localhost:8787 claude      # in another shell
```

Two caveats to validate before trusting it for real work (tracked in [CHA-465](https://linear.app/chapala/issue/CHA-465)):

- **Auth.** Headroom's docs don't specify whether the proxy forwards Claude Code's existing auth header or expects its own `ANTHROPIC_API_KEY`. Confirm your auth path (OAuth subscription vs. API key) survives the hop before relying on it.
- **Prompt caching.** A proxy that rewrites request bodies can invalidate Anthropic's prompt cache, which would *raise* cost and latency — the opposite of the goal. Check that cache hit-rate doesn't regress under a real session before defaulting it on.

## Quick start

Bring up the full stack and run the audit demo:

```bash
just penca-up                           # Postgres + SeaweedFS + 3 servicers + scheduler + Flight SQL gateway
set -a && source docker/.client.env      # PENCA_*_URL for the PencaClient
uv run python examples/audit_demo.py
```

`audit_demo.py` walks through Penca's auditable-store semantics on a
fresh `users(name PK, value)` table:

1. Three transactions on `main`: `insert(alice, bob)` →
   `upsert(alice=99, charlie)` → `delete(bob)`.
2. **`read_data`** — current state (alice=99, charlie=30; bob gone).
3. **`audit_data`** — full version history including the
   tombstone for bob; `audit_data(after=tx1)` shows only the post-tx1
   diff.
4. **`read_data(as_of=tx1)`** — time-travel back to alice=10, bob=20
   before the upsert + delete landed.

The same flow expressed as SQL through Flight SQL is the same wire
calls under the hood — DML translates to `WriteService.WriteData`,
SELECT goes through the merge-on-read planner. See
[Connecting](#connecting).

## Architecture

Each microservice is a separate binary with its own config struct and
scaling profile. Per-service design docs live under
[`docs/services/`](docs/services/); architecture decisions in
[`docs/decisions/`](docs/decisions/).

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
 │ Programmatic client      │           │ penca-sql-server         │
 │ (PencaClient, or any    │           │ (Flight SQL + DataFusion; │
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

### Storage tiers

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
[docs/algorithms.md](docs/algorithms.md#read-path).

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
[docs/algorithms.md](docs/algorithms.md#merge-branch).

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
[merge-on-read](docs/algorithms.md#read-path) that resolves the
latest committed upsert per row minus effective deletes. Storage
shape rationale: [ADR 0001](docs/decisions/0001-unified-upsert-log.md),
[ADR 0008](docs/decisions/0008-table-metadata-subpartitioning.md).

Only committed transactions are persisted from hot to cold storage;
transaction TTLs guarantee cold storage never contains uncommitted or
expired data.

### Retention

`RetentionConfig` has two independent fields — a row version is
eligible for removal during snapshot only when it exceeds *both*:

- `retain_max_versions` — max historical versions per row.
  `NULL` = keep all, `0` = current only, `N` = latest N.
- `retention_duration_us` — max age in microseconds.
  `NULL` = retain indefinitely.

Configured at three levels: catalog (required), schema (optional
override), table (optional override). The effective policy resolves
per-field as `coalesce(table, schema, catalog)`, so changing a
catalog default retroactively applies to every un-overridden table —
no backfill needed.

Retention is enforced at snapshot time, not at write time. All
versions stay available for time-travel until a snapshot runs.

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
read-optimized point-in-time view (applies tombstones, enforces
retention); purge reclaims hot rows once they clear the universal grace
window. The `lifecycle-scheduler` drives `persist → snapshot → purge`
autonomously on a periodic tick ([ADR 0019](docs/decisions/0019-plan-time-pinning-and-universal-grace-window.md)).
Full algorithms with crash-safety invariants:
[docs/algorithms.md](docs/algorithms.md).

## Connecting

Two entry points front the same three microservices:

- **Programmatic gRPC** — direct channels to `WriteService`,
  `QueryService`, `LifecycleService` on
  ports 50052–50054. Full surface: catalog / schema / table CRUD
  (mutations on Write, reads on Query), branching, transactions,
  data mutations, lifecycle ops, streaming reads (`ReadData`,
  `AuditData`). The shipped Python `PencaClient` connects here; any
  third-party client built from the `protos/` files works the same
  way. `List*` RPCs are paginated with opaque base64 page tokens
  (currently wrapping an offset, but the type is opaque so we can
  switch to keyset pagination without breaking clients).
- **Arrow Flight SQL** — port 50060, served by `penca-sql-server`.
  Reads (`SELECT`), DML (`INSERT` / `UPDATE` / `DELETE`), and
  transaction control (`BEGIN` / `COMMIT` / `ROLLBACK` via the Flight
  SQL action endpoints) for BI / ADBC / JDBC / ODBC clients. SQL DML
  translates to `WriteService.WriteData` under the hood; multi-table
  atomic writes still go through the gRPC `Insert` / `Update` /
  `Delete` primitives. See
  [docs/services/penca-sql-server.md](docs/services/penca-sql-server.md)
  for the session model, catalog pinning, and tx routing
  ([ADR 0007](docs/decisions/0007-session-entity.md),
  [ADR 0010](docs/decisions/0010-flight-sql-tx-pin-routing.md)).

The Python `PencaClient` wraps both surfaces:
`execute_query(sql)` / `execute_stream(sql)` / `execute_update(sql)`
for SQL; `read_data` / `audit_data` / `write_data` / branch + tx
methods for the gRPC surface.

## Running locally

`just penca-up` brings up the full stack — Postgres, SeaweedFS, the
3 servicer containers, the lifecycle scheduler, and the Flight SQL
gateway — via `docker/compose.yml`. A `bootstrap-init` one-shot service seeds the
global Penca tables + the default catalog before the servicers bind
their ports, and `just penca-up` writes `docker/.client.env` (the
`PENCA_*_URL`s the client needs) + `docker/.baseline.env` (direct-
Postgres URL for the integration suite's white-box assertions).
Requires Docker.

| Profile | Behavior |
|---|---|
| `test` (default) | Random host ports — parallel-worktree-safe |
| `dev` | Fixed ports 50052–50055 + 50060 |

Standalone deployments (your own Postgres + object store) bootstrap
the database by running the same image the cluster runs — no version
drift between operator's bootstrap and prod:

```bash
docker run --rm \
  -e DATABASE_URL="postgres://penca:penca@PROD_PG_HOST:5432/penca" \
  -e SQL_SERVER_DEFAULT_CATALOG=public \
  ghcr.io/penca-io/penca-rust-server:latest \
  penca-bootstrap
```

> The published `ghcr.io/penca-io/penca-rust-server:latest` image
> arrives with [CHA-187](https://linear.app/chapala/issue/CHA-187);
> until then, contributors building from source can use the `cargo
> run` path documented under [Development](#development).

## Repository structure

```
protos/                                 # Proto source definitions (.proto files)
├── buf.yaml
└── penca_proto/
    ├── external/v1/                    # Public APIs
    │   ├── common.proto                # Shared messages (Branch, Tx, Change, …)
    │   ├── lifecycle.proto             # LifecycleService — persist, snapshot, purge, compact, sweep, tx-log GC
    │   ├── query.proto                 # QueryService — catalog/schema/table reads, branch + tx reads, ReadData / AuditData
    │   └── write.proto                 # WriteService — catalog/schema/table DDL, branching, transactions, mutations
    │
    │   # The read-plan + segment shapes are native penca_core types
    │   # (no proto) since CHA-445 deleted StorageMetadataService.

crates/                                 # Rust workspace (production server)
├── penca-core/                        # Identity (xxh3 UUIDs), naming, error types, env-var loading
├── penca-proto/                       # tonic-build + protox bindings of the .proto files
├── penca-sql/                         # Tiny shared `Dialect` trait — peer dep of penca-db / penca-dl
├── penca-db/                          # Hot-tier `DbDriver`/`Dialect` + Postgres impl (`PgDriver`, `PgTransactionDriver`)
├── penca-dl/                          # Cold-tier `DlDriver`/`Dialect` + DataFusion impl (`DatafusionDlDriver`)
├── penca-format/                      # Columnar reader/writer trait + Parquet & Lance impls
├── penca-storage-hot/                 # Stateless `HotStorageClient` (Postgres upsert/delete logs)
├── penca-storage-meta/                # Stateless `MetadataClient` (~50 methods: catalog/schema/table/branch/tx CRUD, segments, snapshots, plan)
├── penca-storage-cold/                # Stateless `ColdStorageClient` (object-store list/get/put + format dispatch)
├── penca-merge/                       # Symmetric per-tier merge-on-read SQL builder (`penca_merge::sql`)
├── penca-datafusion/                  # `PencaCatalogProviderList` / `SchemaProvider` / `PencaTableProvider`; per-conn `ConnScope`
├── penca-api/                         # Orchestration: `WriteManager`, `QueryManager`, `LifecycleManager`
├── penca-observability/               # Shared `tracing` subscriber init (`init_tracing`) for every binary — RUST_LOG filter + opt-in span timing
├── penca-server-grpc/                 # tonic gRPC servicers + 3 service binaries + `penca-bootstrap`
├── penca-lifecycle-scheduler/         # Autonomous `Persist → Snapshot → Purge` tick loop (binary `penca-lifecycle-scheduler`) — pure gRPC client, no listen port
└── penca-sql-server/                  # Flight SQL gateway binary (port 50060) — DataFusion + arrow-flight, per-connection plan cache, DML translator

packages/                               # Python packages (workspace members)
├── penca-proto/                       # Generated Python protobuf + grpc stubs (consumed by the client and the test suite)
└── penca-client/
    ├── src/penca_client/
    │   ├── client.py                   # `PencaClient` — gRPC channels for the 3 services + ADBC Flight SQL for SQL DML/reads
    │   ├── config.py                   # Pydantic BaseSettings for client env (PENCA_*_URL, PENCA_SQL_URL)
    │   ├── status.py / types.py        # gRPC error mapping, typed catalog/schema/table response wrappers
    │   └── arrow.py / naming.py / _time.py / errors.py    # Small client-side helpers (Arrow IPC, deterministic UUIDs mirrored from penca-core, time conversion, typed errors)
    └── tests/
        └── unit/                       # Pure-Python tests for the client helpers (no infra)

tests/                                  # System-level tests of Penca end-to-end (the Python client is the test driver, not the subject)
├── integration/                        # Runs against the Rust servers via gRPC + Flight SQL — correctness oracle (PG driver for white-box assertions inlined in `integration_helpers.py`)
└── performance/                        # Throughput benchmarks against the Rust servers, with a direct-Postgres baseline

docker/                                 # Postgres + SeaweedFS + Rust servicer containers (compose.yml, Dockerfile.rust-server, env templates)
linear/                                 # Linear issue tracker integration (source of truth for labels/projects)
scripts/                                # Dev tooling (commit-msg validation, blank-line check, sync_linear, roadmap)
docs/                                   # Architecture docs, ADRs, style guide, performance numbers
Justfile                                # Development recipes (`just lint`, `just penca-up`, `just integration-test`, …)
```

## Development

Run `just` to list every recipe (Just installation is in
[Prerequisites](#prerequisites)):

| Recipe | Description |
|--------|-------------|
| `just install-tools` | Install dev-only tools not pinned in `Cargo.toml`/`pyproject.toml` (currently `samply` for profiling + `cargo-sweep` for build-tree GC). Run once after cloning. |
| `just compile-protos` | Regenerate Python + Rust protobuf bindings from all `.proto` files |
| `just lint` | Run ruff linter |
| `just format` / `just format-check` | Run / check ruff formatter + blank-line fixer |
| `just check` | Run Python lint + format check + unit tests + static checks, plus Rust clippy / fmt-check / test. Mirrors CI. |
| `just penca-up [profile]` | Start the full stack (the `bootstrap-init` compose service seeds global tables before servicers bind). `profile` = `test` (default, random ports) or `dev` (fixed ports). Requires Docker. |
| `just penca-down [profile]` | Stop servicers + infra and remove volumes. |
| `just integration-test [services]` | Start infra, run integration tests against the Rust services, tear down. Pass service names to scope: `just integration-test lifecycle query`. Requires Docker. |
| `just perf-test [paths]` | Start infra, run performance tests against the Rust services, tear down. `paths` scope the run to one or more dirs/files under `tests/performance/` (e.g. `grpc`, `grpc/oltp_test.py`); omit to run everything. Captures each run to `.perf/results.jsonl` and writes a static HTML report (`.perf/report-<run_id>.html`) comparing it to history; pass `--record` to also persist the run into the SQLite history. Sources `docker/.baseline.env` for the direct-Postgres baseline. Requires Docker. |
| `just perf-trends` | Per-series markdown summary (regression flags) + trend PNGs over the SQLite perf history (`.perf/perf.db`). |
| `just perf-dashboard [run_id]` | Launch the Streamlit dashboard over the SQLite perf history; pass a `run_id` to open the comparison view for that run. |
| `just tdd` | Start infra, run TDD tests from `tests/tdd/` (gitignored), tear down. Requires Docker. |
| `just sync-linear` | Sync to Linear (`--labels`, `--projects`, `--retag`). Requires `LINEAR_API_KEY`. |
| `just roadmap` | Print open Linear issues, optionally filtered (`--project`, `--priority`, `--label`, `--query`). Requires `LINEAR_API_KEY`. |

Coding conventions, TDD workflow, and architectural rationale:
[docs/style-guide.md](docs/style-guide.md),
[docs/development-methodology-guide.md](docs/development-methodology-guide.md),
[docs/design-decisions.md](docs/design-decisions.md).

Contributors building from source can run `penca-bootstrap` directly
against a local Postgres without Docker:

```bash
DATABASE_URL=postgres://penca:penca@localhost:5432/penca \
SQL_SERVER_DEFAULT_CATALOG=public \
    cargo run -p penca-server-grpc --bin penca-bootstrap
```

This is the from-source path; the documented operator path is the
`docker run` snippet under [Running locally](#running-locally).

### Profiling

Penca uses [`samply`](https://github.com/mstange/samply) for CPU
profiling — both local benchmarks and attaching to running services.
Install once with `just install-tools`.

Profile a benchmark:

```bash
samply record cargo bench --bench <bench-name>
```

Profile a running service — find the PID with `docker top <container>`
or `ps`, then attach:

```bash
samply record -p <PID>
```

`samply` opens [Firefox Profiler](https://profiler.firefox.com) in
your browser with a local HTTP server as the data source, so profile
data never leaves your machine. Firefox Profiler's call-tree, marker,
and async-await visualizations are the canonical view for `samply`
output.

#### Profiling the perf suite (`just perf-test --profile`)

`just perf-test --profile [paths...]` runs the performance suite as
usual (JSONL capture + HTML report; add `--record` to persist to SQLite)
while samply also records a CPU profile of each containerized servicer
under load, attaching to the
container's host PID (`samply record -p`). It is an opt-in flag — like
`--trace` — so a plain `just perf-test` is never
slowed. It profiles `query`, `write`, `lifecycle`,
and `penca-sql-server`; path args narrow the *workload* the
same way they do without the flag (e.g.
`just perf-test --profile performance_query_test.py`).

Profiles are written to the gitignored `.perf/` dir as
`.perf/profile-<svc>.json`. Open one with:

```bash
samply load .perf/profile-<svc>.json
```

Prerequisites:

- **Passwordless `sudo`.** The containerized servicers run as root, so
  samply attaches as root (`CAP_PERFMON`): unprivileged
  `perf_event_open` against another user's process is denied at *every*
  `kernel.perf_event_paranoid` level, and root bypasses the paranoid
  check, so no sysctl tuning is needed. `--profile` preflights `sudo -n`
  and refuses to run without it.
- **A profiling build.** `--profile` builds the servicer image with the
  `[profile.profiling]` Cargo profile (full DWARF + frame pointers) by
  exporting `CARGO_PROFILE=profiling` to the compose build; samply can
  then symbolicate down to source lines and inlined frames. Normal
  `just penca-up` / `just perf-test` runs stay on the lean `release`
  image.

### Configuration

All values are required (no defaults) — knobs come from env vars
injected by `docker/compose.yml`. Server-side configs live in
[`crates/penca-server-grpc/src/config.rs`](crates/penca-server-grpc/src/config.rs)
(per-microservice),
[`crates/penca-sql-server/src/config.rs`](crates/penca-sql-server/src/config.rs)
(Flight SQL gateway), and
[`crates/penca-lifecycle-scheduler/src/config.rs`](crates/penca-lifecycle-scheduler/src/config.rs)
(scheduler).

| Env var | Used by | Purpose |
|---|---|---|
| `DATABASE_URL`, `PG_POOL_MIN`, `PG_POOL_MAX` | all 4 + sql-server | Postgres connection |
| `BIND_ADDR` | all 4 + sql-server | gRPC / Flight SQL server bind (scheduler has no listen port) |
| `RUST_LOG` | every binary | `tracing` `EnvFilter` directive; unset = ERROR-only (fails loud, no in-code default) |
| `PENCA_SPAN_TIMING` | query, sql-server | Opt-in span busy/idle timing (`FmtSpan::CLOSE`); empty = off |
| `OBJECT_STORAGE_PROVIDER`, `OBJECT_STORAGE_BUCKET`, `OBJECT_STORAGE_FORMAT`, `OBJECT_STORAGE_*` | query, write, lifecycle | Cold storage backend (`s3` / `local`; Lance or Parquet) |
| `QUERY_DEFAULT_PAGE_SIZE`, `QUERY_DEFAULT_STREAM_BATCH_SIZE` | query | Pagination (catalog/schema/table reads + branch/tx reads) + streaming batch size |
| `QUERY_SEGMENT_READ_CONCURRENCY` | query | Max in-flight cold-segment reads during `stream_merged` (memory-safety cap) |
| `QUERY_SNAPSHOT_PRUNE_MIN_SEGMENTS` | query | Skip snapshot-segment pruning below this planned-segment count (CHA-353; `0` always prunes) |
| `QUERY_INDEX_SEEK_MAX_PROBE_TUPLES` | query | Probe-tuple cartesian cap for covering-index selection (CHA-485; over-cap skips the index, `0` disables selection) |
| `QUERY_SNAPSHOT_SEGMENT_CACHE_BUDGET_BYTES` | query | Byte budget for the in-process snapshot-segment cache (CHA-252) |
| `QUERY_SNAPSHOT_LIST_CACHE_TTL_SECONDS` | query | TTL for the snapshot-list cache (CHA-441); MUST be `<= min(snapshot interval, QUERY_TIMEOUT_SECONDS)` so a stale list never outlives the retired snapshot files it names |
| `QUERY_SNAPSHOT_LIST_CACHE_MAX_ENTRIES` | query | Max `(catalog, branch, table)` snapshot lists held in the CHA-441 cache (`0` disables) |
| `QUERY_TIMEOUT_SECONDS` | query, lifecycle, scheduler | Hard cap on `read_data`/`audit_data` runtime = universal destructive-op grace window; all three MUST agree ([ADR 0019](docs/decisions/0019-plan-time-pinning-and-universal-grace-window.md)) |
| `WRITE_DEFAULT_TX_TIMEOUT_SECONDS`, `WRITE_MAX_TX_TIMEOUT_SECONDS` | write | Tx TTL bounds |
| `LIFECYCLE_DEFAULT_MAX_SEGMENT_BYTES` | lifecycle | Compaction ceiling |
| `LIFECYCLE_SEGMENT_READ_CONCURRENCY` | lifecycle | Max in-flight cold-segment reads during snapshot's merge_read (memory-safety cap) |
| `HOT_PURGE_GRACE_SECONDS` | lifecycle | Hot-purge grace window; the expired-begin ledger GC waits `max(SCHEDULER_TICK_INTERVAL_SECONDS, this)` before dropping a timed-out tx's ledger (CHA-444 / [ADR 0027](docs/decisions/0027-decoupled-purge-seq-cutoff-and-split-grace.md)) |
| `QUERY_SERVICE_ADDR`, `WRITE_SERVICE_ADDR` | sql-server | Upstream gRPC addresses (Query for catalog/table metadata reads, Write for DML) |
| `SQL_SERVER_FLIGHT_STATEMENT_CACHE_CAPACITY` | sql-server | Per-connection Flight SQL logical-plan cache size (CHA-355; `0` disables) |
| `SQL_SERVER_DEFAULT_CATALOG`, `SQL_SERVER_DEFAULT_SCHEMA`, `SQL_SERVER_DEFAULT_BRANCH` | sql-server | Per-session pinned catalog + unqualified-DML defaults |
| `QUERY_SERVICE_ADDR`, `LIFECYCLE_SERVICE_ADDR` | scheduler | Upstream gRPC addresses for the autonomous tick loop |
| `SCHEDULER_TICK_INTERVAL_SECONDS` | scheduler, lifecycle | Purge sweep cadence (negative = boot then idle forever); lifecycle reads it too, to floor the expired-begin ledger-GC grace (CHA-444 / [ADR 0027](docs/decisions/0027-decoupled-purge-seq-cutoff-and-split-grace.md)) — both MUST agree |
| `SCHEDULER_LIST_PAGE_SIZE` | scheduler | List-tables page size |

The Python `PencaClient` reads the channel URLs:
`PENCA_QUERY_URL`, `PENCA_WRITE_URL`,
`PENCA_LIFECYCLE_URL`,
`PENCA_SQL_URL`. `just penca-up` writes these to
`docker/.client.env` (and `PENCA_DB_*` to `docker/.baseline.env` for
white-box test access + the perf baseline); `just integration-test`
and `just perf-test` source both files automatically.

## Further reading

- [docs/algorithms.md](docs/algorithms.md) — write path, read path,
  branch merge with crash-safety invariants
- [docs/services/](docs/services/) — per-service design docs (RPCs,
  dependencies, failure modes)
- [docs/decisions/](docs/decisions/) — accepted ADRs
- [docs/schema-reference.md](docs/schema-reference.md) — system table
  schemas (global storage metadata, per-catalog, per-table)
- [docs/performance.md](docs/performance.md) — benchmarks across hot,
  cold-snapshotted, and mixed states
- Open work: [Linear roadmap](https://linear.app/chapala). `just
  roadmap` prints a summary; `just roadmap --query "search terms"`
  searches.

## Status

The Rust port ([CHA-103](https://linear.app/chapala/issue/CHA-103))
is the production implementation. The Python gRPC + Flight SQL
servers were decommissioned in
[CHA-186](https://linear.app/chapala/issue/CHA-186); `packages/penca-client/`
now ships only the Python client. The system-level test harness lives at
the top level under [`tests/integration/`](tests/integration/), which
talks pure gRPC + Flight SQL to the Rust services and stays as the
correctness oracle.
