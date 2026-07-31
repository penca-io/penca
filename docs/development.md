# Development

Everything you need to build, run, test, and profile Penca locally. For what Penca
*is*, start at the [README](../README.md); for how the system is put together, see
[architecture.md](architecture.md).

## Prerequisites

### To run

| Tool | Why | Install |
|---|---|---|
| [Docker](https://docs.docker.com/engine/install/) | Postgres + SeaweedFS + servicer containers for `just penca-up` | platform installer |
| [`uv`](https://docs.astral.sh/uv/) | Python toolchain for the client + demo (Python 3.10+) | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| [`just`](https://github.com/casey/just) | Recipe runner (`just penca-up`, `just integration-test`, …) | `uv tool install rust-just` |

### To develop

Install `just` (see [To run](#to-run)), then from the repo root run [`just bootstrap`](../Justfile). Idempotent: re-running picks up where it left off (each step checks before installing), so it's safe after a binary upgrade or a config pull.

Go 1.26.3+ is the one prereq `bootstrap` does not auto-install; platform variance across `apt` / `brew` / tarball makes a single one-liner unreliable. Install from [go.dev/dl](https://go.dev/dl/) and re-run.

#### What `just bootstrap` installs

| Tool | Why |
|---|---|
| [Rust 1.94+](../rust-toolchain.toml) | Build the server. `just penca-up` pulls the published image, so you only need this when you change Rust code and run `just penca-up --build=1` (which `just integration-test`, `just perf-test` and `just tdd` do for you). `rust-toolchain.toml` pins the version. |
| [`rust-analyzer`](https://rust-analyzer.github.io/) | Powers the `LSP` tool and the `language-server` MCP server for Claude Code agents (`goToDefinition`, `findReferences`, `rename_symbol`). |
| [`mcp-language-server`](https://github.com/isaacphi/mcp-language-server) | MCP server registered in [`.mcp.json`](../.mcp.json) that exposes rust-analyzer's `rename_symbol` and friends to agents. |
| [`kata`](https://github.com/kenn-io/kata) | Per-ticket task queue used by `/do-issue` (plan → red → drain). |
| [`roborev`](https://github.com/kenn-io/roborev) | Continuous post-commit reviewer; findings feed back into `kata` via the bridge in `scripts/roborev-kata-hook.sh`. |
| [`headroom`](https://headroom-docs.vercel.app/docs) | Opt-in context-compression proxy for the Claude Code loop, **off by default** (see the opt-in section below). |

Plus the wiring no contributor should have to remember: kata PATH symlink + daemon start + `penca` project binding, roborev daemon registration + `post-commit` hook, the per-repo memory symlink (ADR 0016), and pre-commit hooks for the `pre-commit` + `commit-msg` stages.

#### Shared issue-graph client (experimental, opt-in)

An experiment ([CHA-447](https://linear.app/chapala/issue/CHA-447)) toward letting `/do-issue` planning navigate the whole issue graph from a shared kata instance instead of paying a Linear round-trip per hop. **Off by default**; with the env below unset, the VM stays local-only and nothing changes.

- `PENCA_KATA_GRAPH_URL`: base URL of the shared kata daemon (the issue corpus).
- `PENCA_KATA_GRAPH_TOKEN`: this VM's identity token for it.
- `PENCA_KATA_GRAPH_ALLOW_INSECURE`: set only for a dev-over-http instance.

When `PENCA_KATA_GRAPH_URL` is set, `just bootstrap` (via `init-agent-tools`) probes the instance and reports reachability. Read the shared graph **only** through [`scripts/kata-issue-graph.sh`](../scripts/kata-issue-graph.sh) (`show` / `list` / `search` / …), a scoped, read-only wrapper. Scoping matters: kata's `KATA_SERVER` is process-global, so setting it globally (or dropping a repo-root `.kata.local.toml`) would also route the local `cha-NNN` task-queue drain to the shared daemon. The wrapper sets the remote env inline on exec only, keeping the **local task-queue daemon authoritative** and refusing any mutating subcommand.

This scaffold is inert until the shared instance exists and is populated; tracked in [CHA-450](https://linear.app/chapala/issue/CHA-450) (stand up the daemon), [CHA-451](https://linear.app/chapala/issue/CHA-451) (Linear → kata sync), and [CHA-449](https://linear.app/chapala/issue/CHA-449) (wire `/do-issue` Step 1 to consume it).

#### Headroom context-compression proxy (experimental, opt-in)

[Headroom](https://headroom-docs.vercel.app/docs) ([CHA-465](https://linear.app/chapala/issue/CHA-465)) is a local proxy that compresses what an agent reads (tool outputs, file reads, query results) *before* it reaches the model, trading some risk for fewer tokens. `just bootstrap` installs it (`uv tool install "headroom-ai[proxy]"`) so it's available, but **off by default**: nothing redirects Claude Code until you opt in.

To use it, launch the proxy and point Claude Code at it:

```bash
just headroom-proxy                                  # serves on :8787
ANTHROPIC_BASE_URL=http://localhost:8787 claude      # in another shell
```

Two caveats to validate before trusting it for real work (tracked in [CHA-465](https://linear.app/chapala/issue/CHA-465)):

- **Auth.** Headroom's docs don't specify whether the proxy forwards Claude Code's existing auth header or expects its own `ANTHROPIC_API_KEY`. Confirm your auth path (OAuth subscription vs. API key) survives the hop before relying on it.
- **Prompt caching.** A proxy that rewrites request bodies can invalidate Anthropic's prompt cache, which would *raise* cost and latency; the opposite of the goal. Check that cache hit-rate doesn't regress under a real session before defaulting it on.

## Running locally

`just penca-up` brings up the full stack (Postgres, SeaweedFS, the
3 servicer containers, the lifecycle scheduler, and the Flight SQL
gateway) via [`docker/compose.yml`](../docker/compose.yml). A `bootstrap-init` one-shot service seeds the
global Penca tables + the default catalog before the servicers bind
their ports, and `just penca-up` writes `docker/.client.env` (the
`PENCA_*_URL`s the client needs) + `docker/.baseline.env` (direct-
Postgres URL for the integration suite's white-box assertions).
Requires Docker.

| Profile | Behavior |
|---|---|
| `dev` (default) | Fixed ports 50052–50054 + 50060, lifecycle scheduler running |
| `test` | Random host ports: parallel-worktree-safe; scheduler idle so it can't race a suite's manual lifecycle calls |
| `s3` | Cold tier is a real S3 bucket instead of the in-stack SeaweedFS; Postgres still local. See [Backing the cold tier with a real S3 bucket](#backing-the-cold-tier-with-a-real-s3-bucket) |

To keep your data across restarts, give it a directory; both Postgres and the object
store write there, and it survives `just penca-down`:

```bash
just penca-up --db ~/.penca/data
```

The directory must live **outside** the repo, which `penca-up` enforces rather than
warns about. The repo is the Docker build context (`context: ..`), and Postgres
creates its datadir mode 0700 owned by a container uid — so an in-repo datadir makes
the next `--build` fail outright reading the context, not merely run slowly.

### Backing the cold tier with a real S3 bucket

The `s3` profile points the three storage-touching servicers (query, write,
lifecycle) at a bucket you own. Postgres stays local, so this is the "real cold
tier, disposable hot tier" configuration — useful for seeing what the lifecycle
scheduler actually writes, and for sizing what a deployment costs to store.

The bucket must already exist; nothing in the stack creates it.

```bash
export PENCA_S3_BUCKET=my-penca-bucket
export PENCA_S3_REGION=us-west-1

# Only needed where there is no instance role — see below.
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)

just penca-up --profile s3 --db ~/.penca/data
```

`PENCA_S3_BUCKET` has no default on purpose. A silent fall-through to the dev
bucket name would point a real deployment at the wrong store, and reads against
the wrong bucket surface as an empty table rather than an error — so
[`docker/s3.env`](../docker/s3.env) fails the run instead.

Credentials are read from the **shell** environment, not `~/.aws/credentials`: a
`aws configure` profile on disk is not visible inside a container. Leaving them
unset is a valid configuration rather than a broken one — empty keys make
`object_store` fall through to the standard AWS credential chain, which is what
you want under an EC2/EKS instance role.

For an S3-compatible store that is not AWS (MinIO, R2, Ceph), set
`OBJECT_STORAGE_ENDPOINT` to its URL, and `OBJECT_STORAGE_SCHEME=http` if that
endpoint is plaintext. Empty is what selects AWS proper: it makes
`ObjectStorageConfig` skip `with_endpoint` so the endpoint is derived from the
region.

Two things behave differently under this profile:

- **No SeaweedFS container.** The `seaweedfs` compose profile is off, so nothing
  starts an S3 gateway with nothing to serve. `penca-up` prints the bucket in
  place of the gateway port.
- **`penca-down` does not touch the bucket.** It removes containers and Docker
  volumes; your objects and their storage cost outlive the stack. Clean up with
  `aws s3 rm --recursive` when you are done with a scratch bucket.

Standalone deployments (your own Postgres + object store) bootstrap
the database by running the same image the cluster runs; no version
drift between operator's bootstrap and prod:

```bash
docker run --rm \
  -e DATABASE_URL="postgres://penca:penca@PROD_PG_HOST:5432/penca" \
  -e SQL_SERVER_DEFAULT_CATALOG=public \
  ghcr.io/penca-io/penca-rust-server:latest \
  penca-bootstrap
```

`:latest` is the newest `v*` release. CI publishes each release, and every
`main` merge, as a manifest list covering `linux/amd64` and `linux/arm64`,
each built on a native runner — so `docker run` and Compose resolve your
architecture without being told. Pin a specific release with `:vX.Y.Z`, or
track unreleased `main` with `:main`. Merges that change the image also
get a `:<short-sha>` tag to pin; docs-only merges publish nothing.

## Repository structure

```
protos/                                 # Proto source definitions (.proto files)
├── buf.yaml
└── penca_proto/
    ├── external/v1/                    # Public APIs
    │   ├── common.proto                # Shared messages (Branch, Tx, Change, …)
    │   ├── lifecycle.proto             # LifecycleService: persist, snapshot, purge, compact, sweep, tx-log GC
    │   ├── query.proto                 # QueryService: catalog/schema/table reads, branch + tx reads, ReadData / AuditData
    │   └── write.proto                 # WriteService: catalog/schema/table DDL, branching, transactions, mutations
    │
    │   # The read-plan + segment shapes are native penca_core types
    │   # (no proto) since CHA-445 deleted StorageMetadataService.

crates/                                 # Rust workspace (production server)
├── penca-core/                        # Identity (xxh3 UUIDs), naming, error types, env-var loading
├── penca-proto/                       # tonic-build + protox bindings of the .proto files
├── penca-sql/                         # Tiny shared `Dialect` trait: peer dep of penca-db / penca-dl
├── penca-db/                          # Hot-tier `DbDriver`/`Dialect` + Postgres impl (`PgDriver`, `PgTransactionDriver`)
├── penca-dl/                          # Cold-tier `DlDriver`/`Dialect` + DataFusion impl (`DatafusionDlDriver`)
├── penca-format/                      # Columnar reader/writer trait + Parquet & Lance impls
├── penca-storage-hot/                 # Stateless `HotStorageClient` (Postgres upsert/delete logs)
├── penca-storage-meta/                # Metadata storage layer: `LifecycleManager` plus the segment/snapshot/tx-log row shapes (read surface rehomed onto `QueryManager`, ADR 0028)
├── penca-storage-cold/                # Stateless `ColdStorageClient` (object-store list/get/put + format dispatch)
├── penca-merge/                       # Symmetric per-tier merge-on-read SQL builder (`penca_merge::sql`)
├── penca-datafusion/                  # `PencaCatalogProviderList` / `SchemaProvider` / `PencaTableProvider`; per-conn `ConnScope`
├── penca-api/                         # Orchestration: `WriteManager`, `QueryManager`, `LifecycleManager`
├── penca-observability/               # Shared `tracing` subscriber init (`init_tracing`) for every binary: RUST_LOG filter + opt-in span timing
├── penca-server-grpc/                 # tonic gRPC servicers + 3 service binaries + `penca-bootstrap`
├── penca-lifecycle-scheduler/         # Autonomous `Persist → Snapshot → Purge` tick loop (binary `penca-lifecycle-scheduler`): pure gRPC client, no listen port
└── penca-sql-server/                  # Flight SQL gateway binary (port 50060): DataFusion + arrow-flight, per-connection plan cache, DML translator

packages/                               # Python packages (workspace members)
├── penca-proto/                       # Generated Python protobuf + grpc stubs (consumed by the client and the test suite)
└── penca-client/
    ├── src/penca_client/
    │   ├── client.py                   # `PencaClient`: gRPC channels for the 3 services + ADBC Flight SQL for SQL DML/reads
    │   ├── config.py                   # Pydantic BaseSettings for client env (PENCA_*_URL, PENCA_SQL_URL)
    │   ├── status.py / types.py        # gRPC error mapping, typed catalog/schema/table response wrappers
    │   └── arrow.py / naming.py / _time.py / errors.py    # Small client-side helpers (Arrow IPC, deterministic UUIDs mirrored from penca-core, time conversion, typed errors)
    └── tests/
        └── unit/                       # Pure-Python tests for the client helpers (no infra)

tests/                                  # System-level tests of Penca end-to-end (the Python client is the test driver, not the subject)
├── integration/                        # Runs against the Rust servers via gRPC + Flight SQL: correctness oracle (PG driver for white-box assertions inlined in `integration_helpers.py`)
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
| `just penca-up [--profile P] [--db DIR] [--build=1]` | Start the full stack (the `bootstrap-init` compose service seeds global tables before servicers bind). `--profile` = `dev` (default: fixed ports, lifecycle scheduler running) or `test` (random ports so parallel worktrees don't collide, scheduler idle so it can't race the suites' manual lifecycle calls). `--db DIR` persists Postgres and the object store under a host directory, so the stack survives `penca-down`. `--build=1` compiles the image from your working tree instead of pulling the published one — the value is required, `just` has no valueless flags. Requires Docker. |
| `just penca-down [profile]` | Stop servicers + infra and remove volumes. |
| `just integration-test [services]` | Start infra, run integration tests against the Rust services, tear down. Pass service names to scope: `just integration-test lifecycle query`. Requires Docker. |
| `just perf-test [paths]` | Start infra, run performance tests against the Rust services, tear down. `paths` scope the run to one or more dirs/files under `tests/performance/` (e.g. `grpc`, `grpc/oltp_test.py`); omit to run everything. Captures each run to `.perf/results.jsonl` and writes a static HTML report (`.perf/report-<run_id>.html`) comparing it to history; pass `--record` to also persist the run into the SQLite history. Sources `docker/.baseline.env` for the direct-Postgres baseline. Requires Docker. |
| `just perf-trends` | Per-series markdown summary (regression flags) + trend PNGs over the SQLite perf history (`.perf/perf.db`). |
| `just perf-dashboard [run_id]` | Launch the Streamlit dashboard over the SQLite perf history; pass a `run_id` to open the comparison view for that run. |
| `just tdd` | Start infra, run TDD tests from `tests/tdd/` (gitignored), tear down. Requires Docker. |
| `just sync-linear` | Sync to Linear (`--labels`, `--projects`, `--retag`). Requires `LINEAR_API_KEY`. |
| `just roadmap` | Print open Linear issues, optionally filtered (`--project`, `--priority`, `--label`, `--query`). Requires `LINEAR_API_KEY`. |

Coding conventions, TDD workflow, and architectural rationale:
[style-guide.md](style-guide.md),
[development-methodology-guide.md](development-methodology-guide.md),
[design-decisions.md](design-decisions.md).

Contributors building from source can run `penca-bootstrap` directly
against a local Postgres without Docker:

```bash
DATABASE_URL=postgres://penca:penca@localhost:5432/penca \
SQL_SERVER_DEFAULT_CATALOG=public \
    cargo run -p penca-server-grpc --bin penca-bootstrap
```

This is the from-source path; the documented operator path is the
`docker run` snippet under [Running locally](#running-locally).

### Testing

The system-level test harness lives at the top level under
[`tests/integration/`](../tests/integration/). It talks pure gRPC + Flight SQL to the
Rust services; the Python client is the test *driver*, not the subject, and is the
project's correctness oracle. White-box assertions reach Postgres directly through the
helpers inlined in `integration_helpers.py`.

- `just check` is the pre-push gate: Python lint + format check + unit tests + static
  checks, plus Rust clippy / fmt-check / test. It mirrors CI, and it does **not** run
  the integration suite.
- `just integration-test [services]` starts infra, runs the suite against the Rust
  services, and tears down. Scope it while iterating (`just integration-test lifecycle
  query`); run it unscoped on a fresh stack before opening a PR.
- `just tdd` runs whatever is in the gitignored `tests/tdd/` directory against a live
  stack. It is the inner-loop tool; nothing there is a deliverable.
- `just perf-test [paths]` runs the throughput benchmarks with a direct-Postgres
  baseline. See [performance.md](performance.md) for what the numbers mean.

### Profiling

Penca uses [`samply`](https://github.com/mstange/samply) for CPU
profiling; both local benchmarks and attaching to running services.
Install once with `just install-tools`.

Profile a benchmark:

```bash
samply record cargo bench --bench <bench-name>
```

Profile a running service, find the PID with `docker top <container>`
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
container's host PID (`samply record -p`). It is an opt-in flag, like
`--trace`, so a plain `just perf-test` is never
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

All values are required (no defaults), knobs come from env vars
injected by `docker/compose.yml`. Server-side configs live in
[`crates/penca-server-grpc/src/config.rs`](../crates/penca-server-grpc/src/config.rs)
(per-microservice),
[`crates/penca-sql-server/src/config.rs`](../crates/penca-sql-server/src/config.rs)
(Flight SQL gateway), and
[`crates/penca-lifecycle-scheduler/src/config.rs`](../crates/penca-lifecycle-scheduler/src/config.rs)
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
| `QUERY_TIMEOUT_SECONDS` | query, lifecycle, scheduler | Hard cap on `read_data`/`audit_data` runtime = universal destructive-op grace window; all three MUST agree ([ADR 0019](decisions/0019-plan-time-pinning-and-universal-grace-window.md)) |
| `WRITE_DEFAULT_TX_TIMEOUT_SECONDS`, `WRITE_MAX_TX_TIMEOUT_SECONDS` | write | Tx TTL bounds |
| `WRITE_SNAPSHOT_SEGMENT_CACHE_BUDGET_BYTES`, `WRITE_SNAPSHOT_LIST_CACHE_TTL_SECONDS`, `WRITE_SNAPSHOT_LIST_CACHE_MAX_ENTRIES` | write | Write-side mirrors of the query snapshot caches; same contracts as the `QUERY_*` rows above |
| `LIFECYCLE_DEFAULT_MAX_SEGMENT_BYTES` | lifecycle | Compaction ceiling |
| `LIFECYCLE_SEGMENT_READ_CONCURRENCY` | lifecycle | Max in-flight cold-segment reads during snapshot's merge_read (memory-safety cap) |
| `HOT_PURGE_GRACE_SECONDS` | lifecycle | Hot-purge grace window; the expired-begin ledger GC waits `max(SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS, SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS, this)` before dropping a timed-out tx's ledger (CHA-444 / [ADR 0027](decisions/0027-decoupled-purge-seq-cutoff-and-split-grace.md)). The max over both cadences is a conservative bound: Purge rides the snapshot loop today, but the loop-to-op assignment is not an invariant |
| `QUERY_SERVICE_ADDR`, `WRITE_SERVICE_ADDR` | sql-server | Upstream gRPC addresses (Query for catalog/table metadata reads, Write for DML) |
| `SQL_SERVER_FLIGHT_STATEMENT_CACHE_CAPACITY` | sql-server | Per-connection Flight SQL logical-plan cache size (CHA-355; `0` disables) |
| `SQL_SERVER_DEFAULT_CATALOG`, `SQL_SERVER_DEFAULT_SCHEMA`, `SQL_SERVER_DEFAULT_BRANCH` | sql-server | Per-session pinned catalog + unqualified-DML defaults |
| `LIFECYCLE_SERVICE_ADDR` | write, scheduler | Upstream lifecycle address: the write service calls `PersistBranch` at fork time; the scheduler drives the tick loop |
| `QUERY_SERVICE_ADDR` | sql-server, scheduler | Upstream query address for the autonomous tick loop |
| `SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS` | scheduler, lifecycle | Persist sweep cadence (**non-positive** = that loop boots then idles forever); lifecycle reads it too, to floor the expired-begin ledger-GC grace (CHA-444 / [ADR 0027](decisions/0027-decoupled-purge-seq-cutoff-and-split-grace.md)): both services MUST agree |
| `SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS` | scheduler, lifecycle | Snapshot + Purge + tx-log GC sweep cadence (**non-positive** = that loop boots then idles forever); same ledger-GC floor contract, both services MUST agree |
| `SCHEDULER_LIST_PAGE_SIZE` | scheduler | List-tables page size |

The Python `PencaClient` reads the channel URLs:
`PENCA_QUERY_URL`, `PENCA_WRITE_URL`,
`PENCA_LIFECYCLE_URL`,
`PENCA_SQL_URL`. `just penca-up` writes these to
`docker/.client.env` (and `PENCA_DB_*` to `docker/.baseline.env` for
white-box test access + the perf baseline); `just integration-test`
and `just perf-test` source both files automatically.
