# write

**Port**: 50053
**Proto**: `penca_proto/external/v1/write.proto` → `WriteService`
**Crate**: `crates/penca-server-grpc/src/write.rs` (binary: `penca_write`); manager logic in `crates/penca-api/src/write.rs`

## Purpose

Owns every Penca mutation: catalog/schema/table DDL, branch lifecycle,
transaction begin/commit, and data mutations (upsert / delete). DDL
and data writes share the same `commit_tx_log`-backed MVCC envelope (CHA-164),
so DDL participates in transactions alongside `WriteData`.

## RPCs

### Catalog DDL

Catalog mutations are not tx-tracked: they bootstrap per-catalog
metadata storage (the per-catalog `commit_tx_log`, `__penca_system__.schemas`,
`__penca_system__.tables`) which the rest of the tx machinery depends
on. No `tx_uuid` / `author` / `comment` plumbing.

| RPC | Notes |
|---|---|
| `CreateCatalog` | Mints a random `catalog_uuid` + `main_branch_uuid` (CHA-236), bootstraps the per-catalog metadata tables, returns both UUIDs. `catalog_store.UNIQUE(catalog_name)` rejects duplicate names with `ALREADY_EXISTS`. |
| `UpdateCatalog` | Owner / description / retention update; `new_catalog_name` renames the catalog (CHA-236) — `catalog_uuid` stays put. |
| `DeleteCatalog` | Cascades through all schemas / tables on `main`. Drops per-catalog physicals. Irreversible. |

### Schema DDL

Schemas are branch-scoped (CHA-177); writes go through
`__penca_system__.schemas` partitioned per branch and gated on
`commit_tx_log` for visibility (CHA-164).

| RPC | `tx_uuid` | Behaviour |
|---|---|---|
| `CreateSchema` | unset | Auto-commit (requires `author` / `comment`). |
| `CreateSchema` | set | Append to open tx (`author` / `comment` must be unset). |
| `UpdateSchema` | (same mode-switch) | Description / retention update; `new_schema_name` renames the schema on this branch (CHA-236). `schema_uuid` stays put. |
| `DeleteSchema` | (same mode-switch) | Cascades through all tables on the branch (soft tombstones). |

### Table DDL

Tables are branch-scoped (CHA-177); rows in
`__penca_system__.tables` carry `tx_uuid` and resolve visibility via
JOIN against the catalog's `commit_tx_log`.

| RPC | `tx_uuid` | Behaviour |
|---|---|---|
| `CreateTable` | unset | Auto-commit (requires `author` / `comment`). |
| `CreateTable` | set | Append to open tx. Combines with `WriteData` on the same `tx_uuid` for "create table + insert rows" in one tx. |
| `UpdateTable` | (same mode-switch) | Schema evolution + metadata update; `new_table_name` renames the table on this branch (CHA-236). `table_uuid` stays put — persisted segments and audit history keep resolving. |
| `DeleteTable` | (same mode-switch) | Soft-delete via tombstone on `__penca_system__.tables`; lifecycle sweep drops physicals after commit. |

### Branching + transaction lifecycle

| RPC | Notes |
|---|---|
| `CreateBranch` | Mints a random `branch_uuid` (CHA-236). Per-catalog `branch_store.UNIQUE(branch_name)` rejects duplicate names with `ALREADY_EXISTS`. Branches are catalog-scoped (CHA-163), spanning every schema in the catalog. |
| `UpdateBranch` | `new_branch_name` renames the branch (CHA-236). `branch_uuid` stays put — descendant branch references and persisted cold segments keep resolving. |
| `DeleteBranch` | Atomic full-branch delete (metadata + every per-branch data table on the branch) |
| `MergeBranch` | Copy source branch's resolved state into target as a merge tx (`EXCLUSIVE` source lock) |
| `BeginTx` | Allocates a random `tx_uuid`; sets TTL via `WRITE_DEFAULT_TX_TIMEOUT_SECONDS` / `WRITE_MAX_TX_TIMEOUT_SECONDS`. Tx is catalog-scoped: a single tx can mutate multiple schemas atomically. |
| `CommitTx` | Writes `commit_micros` on the commit_tx_log row, making upserts/deletes/DDL visible. Fails `FAILED_PRECONDITION` if the tx was aborted. |
| `AbortTx`  | Inserts into the catalog's `abort_tx_log`. Idempotent — re-aborting the same `tx_uuid` is a no-op. Fails `FAILED_PRECONDITION` if the tx has already committed. |

`AbortTx` makes rollback observable; the `BeginTx` TTL is the fallback
for crashed clients that never reach `AbortTx`.

### Data mutation

| RPC | `tx_uuid` | Behaviour |
|---|---|---|
| `WriteData` | unset | **Auto-commit**: server opens a tx, applies changes, commits — one round-trip, returns the new `Tx`. `author` / `comment` are tx metadata. |
| `WriteData` | set    | **Append**: appends `Change` payloads to the upsert / delete logs against the supplied open tx. Caller still calls `CommitTx` to finalize. `author` / `comment` must be unset. |

`WriteData` is the entire data-mutation surface. `tx_uuid`'s
presence mode-switches between auto-commit and append. Both
programmatic clients (Python `WriteManager`, Rust `penca-api`) and
penca-sql-server's SQL DML translator land here. The service is
intentionally thin: it does not parse SQL or run the merge-on-read
pipeline. Each call is a pure append (`upsert_log` / `delete_log`)
inside one Postgres transaction.

`Change.upserts` carries Arrow IPC bytes in the table's user-column
shape. The WriteService looks up the table's primary keys (one
`MetadataClient::get_table` call per `Change`), derives `row_uuid`
deterministically via `naming::row_uuid_for_pk(table_uuid, pk_values)`,
and mints a fresh `version_uuid` per row before INSERT. `Change.deletes`
carries pre-derived `row_uuid` strings — the only identity the caller
ships on the wire. The semantics on the wire are LWW; strict-INSERT
validation and SQL WHERE resolution live in penca-sql-server, not here.
See [ADR 0006](../decisions/0006-sql-dml-out-of-write-microservice.md)
for the rationale (why DML orchestration moved out of the write
microservice and into the gateway, why row identity is server-derived).

## Dependencies

- **Postgres** — all writes are SQL `INSERT`s into the upsert/delete
  and tx logs, wrapped in Postgres transactions.

No dependency on object storage (cold-tier writes happen in `lifecycle`)
or other microservices.

## Config

| Env var | Purpose |
|---|---|
| `DATABASE_URL`, `PG_POOL_MIN`, `PG_POOL_MAX` | Postgres pool |
| `BIND_ADDR` | gRPC server bind |
| `WRITE_DEFAULT_TX_TIMEOUT_SECONDS` | Default TTL on `BeginTx` when the client does not specify one |
| `WRITE_MAX_TX_TIMEOUT_SECONDS` | Hard cap — request TTLs above this are clamped |
| `WRITE_SNAPSHOT_SEGMENT_CACHE_BUDGET_BYTES` | Byte budget for the write path's in-process snapshot-segment cache (CHA-472) — shared with the query service so a hot autocommit point-write reuses decoded segments |
| `WRITE_SNAPSHOT_LIST_CACHE_TTL_SECONDS` | TTL for the write path's snapshot-list cache (CHA-472). MUST be `<= min(snapshot interval, QUERY_TIMEOUT_SECONDS)` — a stale list must never outlive the retired snapshot files it names (the GC grace bound) |
| `WRITE_SNAPSHOT_LIST_CACHE_MAX_ENTRIES` | Max `(catalog, branch, table)` snapshot lists held in the write path's cache (`0` disables) |
| `LIFECYCLE_SERVICE_ADDR` | Address of the lifecycle service the write path calls to flush the source branch hot→cold at `CreateBranch` (PersistBranch, CHA-273 rework). The persist loop runs in the lifecycle pod, not the write pod |

## Streaming

None. Every RPC is unary. Large batches flow inside the request
message body — Arrow IPC bytes on `Change.upserts`, repeated
`row_uuid`s on `Change.deletes`. Streaming write ingest is not in
scope for CHA-123.

## Error taxonomy

| Manager raises | Servicer returns | Examples |
|---|---|---|
| `NotFoundError` | `NOT_FOUND` | `CommitTx(unknown_tx)`, `WriteData` targeting a missing table |
| `InvalidRequestError` | `INVALID_ARGUMENT` | Expired tx, TTL above the cap, schema mismatch between batch and target table |
| `FailedPreconditionError` | `FAILED_PRECONDITION` | `CommitTx` on an aborted tx; `AbortTx` on an already-committed tx |
| `ApiError` (base) | `INTERNAL` | Postgres deadlock, constraint violation |

## Failure modes

- **Postgres unavailable.** All mutations fail fast with `UNAVAILABLE`.
  Clients should retry on a backoff; idempotency comes from the fact
  that mutations key on `(tx_uuid, row_uuid)` — a retry of the same batch
  under the same tx is a no-op.
- **Tx TTL expiry mid-batch.** Server rejects further `WriteData` calls
  on the expired tx (`INVALID_ARGUMENT`). Client starts a new tx and
  retries.
- **MergeBranch blocks production writes.** `MergeBranch` holds
  `SELECT ... FOR UPDATE` on the source branch's `commit_tx_log` partition for
  the entire merge. Long merges block concurrent commits on the source.
  [CHA-14](https://linear.app/chapala/issue/CHA-14) /
  [CHA-16](https://linear.app/chapala/issue/CHA-16) track the two-phase
  pointer-swap design that will make this non-blocking.
- **Branch delete races.** `DeleteBranch` is atomic and irreversible;
  there is no soft-delete. In-flight reads holding a branch UUID will
  return `NOT_FOUND` on their next Postgres round-trip.
