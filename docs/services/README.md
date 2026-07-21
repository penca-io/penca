# Penca services

Penca is deployed as four independent gRPC microservices, a lifecycle
scheduler, and an optional Flight SQL gateway. Each is backed by a
single-purpose binary with its own config struct, lifecycle, and
scaling profile. Per-service design docs:

| Service | Port | Design doc |
|---|---|---|
| query | 50052 | [query.md](query.md) |
| write | 50053 | [write.md](write.md) |
| lifecycle | 50054 | [lifecycle.md](lifecycle.md) |
| lifecycle-scheduler | — | [lifecycle-scheduler.md](lifecycle-scheduler.md) |
| penca-sql-server | 50060 | [penca-sql-server.md](penca-sql-server.md) |

WriteService owns every Penca mutation (catalog/schema/table DDL,
branching, transactions, data writes); QueryService owns every read
(catalog/schema/table reads, branches, transactions, ReadData,
AuditData). The pre-CHA-174 AdminService is folded into both — see
[ADR 0014](../decisions/0014-fold-admin-into-write-query.md).

Each doc covers: purpose, RPCs, dependencies, config env vars, streaming
semantics (where applicable), error taxonomy, and failure modes.

## Topology and the perf boundary

Services are **always colocated** — same host in dev (single
`docker compose` stack), same Kubernetes node / availability zone in
production (behind one service mesh). Cross-service gRPC hops are
treated as effectively free. The perf boundary worth optimizing is the
boundary between any service and its external dependencies: Postgres
and object storage.

Concrete consequences for design discussions in this directory:

- **Internal RPC fan-out is fine.** It's OK for penca-sql-server to
  issue multiple internal RPCs (query → PK metadata + collision check,
  write → append+commit) for a single SQL `INSERT`. It's OK for
  lifecycle to fan out query RPCs during compaction.
- **Postgres round trips count.** Combine multi-statement work into
  data-modifying CTEs, cache version-keyed metadata in callers, prefer
  one large query over several small ones.
- **Object-storage round trips count more.** Object storage may not be
  colocated (S3 / R2 / GCS — separate VPC, separate region). Snapshot
  fan-out, cold-tier scan plans, and lifecycle persist are all sized
  against this — minimize reads, batch writes.
- **Don't collapse services to remove hops.** If a boundary feels
  chatty, the boundary may be in the wrong place — but in-process
  composition is not the right fix.

Full rationale and revisit triggers: [ADR 0005](../decisions/0005-colocated-microservices-perf-boundary.md).

## Shared conventions

- **Server is Rust.** The 4 microservice binaries live in
  `crates/penca-server-grpc/src/bin/`; the lifecycle scheduler in
  `crates/penca-lifecycle-scheduler/`; the Flight SQL gateway in
  `crates/penca-sql-server/`. Tonic + tokio.
- **No defaults.** Every setting is required from environment variables.
  Defaults live in `docker/compose.yml`.
- **Typed errors.** Managers raise typed `ApiError` variants; servicers
  translate via `crates/penca-server-grpc/src/status.rs` to
  `tonic::Code::NOT_FOUND` / `INVALID_ARGUMENT` / `INTERNAL`.
- **Arrow IPC for streaming.** Streaming RPCs carry `RecordBatch` bytes
  in the `record_batch_ipc` field; clients deserialize with
  `arrow::ipc::reader::StreamReader` or `pyarrow.ipc.open_stream`.
