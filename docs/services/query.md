# query

**Port**: 50052
**Proto**: `penca_proto/external/v1/query.proto` → `QueryService`
**Crate**: `crates/penca-server-grpc/src/query.rs` (binary: `penca_query`); manager logic in `crates/penca-api/src/query.rs`

## Purpose

Read path for user data and metadata. Executes merge-on-read across
hot (Postgres) and cold (object storage) tiers and streams Arrow
`RecordBatch` bytes back to clients. Also serves catalog / schema /
table / branch / transaction reads (read-only counterparts to
`WriteService`'s mutations).

The `Query` RPC (SQL string → DataFusion → Arrow) was removed in
CHA-123. SQL is served over Arrow Flight SQL on the Rust backend —
tracked under [CHA-124](https://linear.app/chapala/issue/CHA-124).

## RPCs

### Catalog / schema / table metadata (read-only)

| RPC | Kind | Notes |
|---|---|---|
| `GetCatalog` | Unary | Catalog by uuid or name |
| `ListCatalogs` | Unary, paginated | All catalogs visible to the caller; optional owner filter |
| `GetSchema` | Unary | Schema by uuid or name; branch-scoped (CHA-177); supports `open_tx_uuid` for RYOW |
| `ListSchemas` | Unary, paginated | Schemas in a catalog on a branch |
| `GetTable` | Unary | Table metadata; effective retention coalesces table → schema → catalog (per ADR 0011) |
| `ListTables` | Unary, paginated | Tables in a schema on a branch |

### Branches (read-only)

| RPC | Kind | Notes |
|---|---|---|
| `GetBranch` | Unary | Branch metadata by name or UUID |
| `ListBranches` | Unary, paginated | All branches on a schema |

Transactions are an internal mechanism with no introspection RPC (ADR
0018). The load-bearing scalar `commit_micros` is returned inline
on `CommitTx` / `WriteData` / `MergeBranch`; per-row audit metadata
(commit/begin timestamps, comment, author) flows through `AuditData`.

### Data

| RPC | Kind | Notes |
|---|---|---|
| `ReadData` | Server-streaming | Merge-on-read of current-state data; streams Arrow IPC batches |
| `AuditData` | Server-streaming | Full version history — upsert rows on `.upserts`, tombstones on `.deletes`, both joined with tx log |

## Dependencies

- **Postgres** — hot-tier reads (upsert/delete/tx logs).
- **Object storage** — cold-tier reads (Parquet / Lance segment files).
- **read planner** — `QueryManager::plan` (the `penca-storage-meta`
  library, called in-process — not a service hop since CHA-445 deleted
  StorageMetadataService) resolves which hot tables + cold segments
  participate in a given read. The plan drives per-tier execution.

## Config

| Env var | Purpose |
|---|---|
| `DATABASE_URL`, `PG_POOL_MIN`, `PG_POOL_MAX` | Postgres pool |
| `BIND_ADDR` | gRPC server bind |
| `QUERY_DEFAULT_PAGE_SIZE` | Default page size on paginated RPCs |
| `QUERY_DEFAULT_STREAM_BATCH_SIZE` | `RecordBatch` row count for streaming |
| `QUERY_SEGMENT_READ_CONCURRENCY` | Max in-flight cold-segment reads during `stream_merged` (memory-safety cap) |
| `QUERY_SNAPSHOT_PRUNE_MIN_SEGMENTS` | Prune snapshot segments only when the planned count exceeds this (CHA-353; `1` skips the single-segment case, `0` always prunes) |
| `QUERY_INDEX_SEEK_MAX_PROBE_TUPLES` | Cap on the probe-tuple cartesian product per covering index (CHA-485). Over the cap the index is skipped — full scan + residual filter, never a truncated probe set; `0` disables user-index selection |
| `QUERY_SNAPSHOT_SEGMENT_CACHE_BUDGET_BYTES` | Byte budget for the in-process snapshot-segment cache (CHA-252) |
| `QUERY_SNAPSHOT_LIST_CACHE_TTL_SECONDS` | TTL for the snapshot-list cache (CHA-441). MUST be `<= min(snapshot interval, QUERY_TIMEOUT_SECONDS)` — a stale list must never outlive the retired snapshot files it names (the GC grace bound) |
| `QUERY_SNAPSHOT_LIST_CACHE_MAX_ENTRIES` | Max `(catalog, branch, table)` snapshot lists held in the CHA-441 cache (`0` disables) |
| `QUERY_TIMEOUT_SECONDS` | Hard cap on `read_data` / `audit_data` runtime; equals the universal grace window the lifecycle service + scheduler read from the same var (ADR 0019) |
| `OBJECT_STORAGE_PROVIDER` | `s3` or `local` |
| `OBJECT_STORAGE_BUCKET`, `OBJECT_STORAGE_FORMAT` (`parquet`/`lance`) | Cold storage layout |
| `OBJECT_STORAGE_ACCESS_KEY`, `OBJECT_STORAGE_SECRET_KEY`, `OBJECT_STORAGE_REGION`, `OBJECT_STORAGE_ENDPOINT`, `OBJECT_STORAGE_SCHEME` | S3 creds (S3 provider only) |
| `OBJECT_STORAGE_PATH` | Local FS path (local provider only) |

## Streaming

Both `ReadData` and `AuditData` are true server-streams — batches flow
as they're produced, not after full materialization.

- **`AuditData`** streams two producers back-to-back: upsert rows via
  `HotStorageClient.audit_upserts_stream` (server-side named cursor
  joining `upsert_log` and `commit_tx_log`) and delete tombstones via
  `HotStorageClient.audit_deletes_stream` (same cursor pattern over
  `delete_log`). Each `AuditDataResponse` message carries either
  `bytes upserts` or `bytes deletes`; the client collects both streams
  into separate `pa.Table`s and returns them as
  `tuple[upserts_table, deletes_table]`. Memory flat in result-set size
  per stream. Empty schema-header batches are emitted on both channels
  (shapes: `audit_upsert_schema(user_schema)` and `audit_delete_schema()`)
  so callers can always recover the schema even when one or both sides
  are empty. The delete stream is a behavior addition under CHA-134 —
  tombstones were previously invisible to `audit_data`. See
  [decisions/0001-unified-upsert-log.md](../decisions/0001-unified-upsert-log.md).
- **`ReadData`** runs one two-arm resolve per tier (CHA-368: visible
  upserts + winning tombstones, `is_delete`-flagged) once against hot and
  once against cold, unions the results in-memory, derives the exclusion
  set from the composed resolve's full `row_uuid` set, applies the user
  filter as a DataFusion residual, then streams each snapshot segment
  through the exclusion anti-join + the same residual. Memory ceiling =
  one resolved set + the exclusion `HashSet<row_uuid>` + one snapshot
  segment. Emits an empty schema-header batch with the user schema first
  for the same reason as `AuditData`. See
  [algorithms.md](../algorithms.md#read-path) for the full pipeline.

## Session reuse (cold reads)

Every cold-tier read builds a DataFusion `SessionContext` — the persist-log
session (`build_persist_session`), the snapshot-scan session
(`build_snapshot_session`), and the snapshot-pruning predicate plan. A warm
`SessionContext::new()` is ~128 µs/call in release (the default registry is
process-global singletons — the ~1.4 ms figure is a one-time cold/debug cost),
several per query, so the service builds one **cold-session template** at startup
(`penca_dl::build_cold_session_template`) and injects it (`Arc<SessionState>`)
into every `DatafusionDlDriver`. Each per-unit context is a ~71 µs clone
(`derive_cold_session`) with a fresh, isolated catalog — so
concurrent reads never collide on the fixed cold table names (`l`, `exclusion`,
`upsert_log`, `delete_log`). See
[Layered session scope and caching](../development-methodology-guide.md#layered-session-scope-and-caching)
for the mechanism and the `Arc<dyn CatalogProviderList>` trap it avoids. CHA-421.

## Error taxonomy

| Manager raises | Servicer returns | Examples |
|---|---|---|
| `NotFoundError` | `NOT_FOUND` | `GetBranch("missing")`, `ReadData(unknown_table)` |
| `InvalidRequestError` | `INVALID_ARGUMENT` | Malformed identifier combo, invalid `as_of_micros` |
| `ApiError` (base) | `INTERNAL` | Merge failure, cold-storage read error |

Streaming RPCs surface errors mid-stream via gRPC trailers; clients
should handle a partial stream followed by a non-OK status.

## Failure modes

- **Postgres unavailable.** All reads fail with `UNAVAILABLE`. No local
  state to recover.
- **Cold-storage unavailable.** Reads for tables with cold segments fail
  mid-stream; hot-only reads still succeed.
- **Concurrent persist during read.** Plan resolution and batch reads are
  not in a shared tx — a persist may delete hot rows between plan and read.
  The same rows will appear in cold storage; the merge tolerates this
  (a row in both tiers is deduped by `row_uuid` + `commit_micros`).
- **Large merge OOM.** Snapshot buffer + delete set must fit in RAM.
  CHA-127 removes this ceiling.
