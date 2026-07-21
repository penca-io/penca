# lifecycle

**Port**: 50054
**Proto**: `penca_proto/external/v1/lifecycle.proto` → `LifecycleService`
**Crate**: `crates/penca-server-grpc/src/lifecycle.rs` (binary: `penca_lifecycle`); manager logic in `crates/penca-api/src/lifecycle.rs`

## Purpose

Moves committed data from hot to cold storage (persist), re-packs cold
persist-segment files (compact), and produces read-optimized
point-in-time materializations (snapshot). Snapshot segment files are
immutable — never compacted (ADR 0024). Mostly IO-bound, CPU-spiky during snapshot
(delete resolution + version coalesce).

## RPCs (6)

| RPC | Kind | Notes |
|---|---|---|
| `Persist` | Unary | Per-table: write T's committed hot upsert/delete log rows to cold; ALSO delete T's aborted hot rows past the open-tx clamp (CHA-221 v2.1 / ADR 0021). Stamps `table_persist_metadata.persisted_at_micros = max(max(committed_at over persisted rows), max(aborted_at over aborted hot rows cleaned))`. Does NOT delete *committed* hot rows — queries keep serving from hot until `Purge`. |
| `Purge` | Unary | Per-table: delete T's hot upsert/delete log rows up to `persisted_at_micros(T)`. Stamps `table_purge_metadata.purged_at_micros`. CHA-233 (ADR 0019): the hot/cold visibility cutoff `plan()` uses is sourced from `persisted_at_micros`, not from this watermark; `purged_at_micros` only feeds Purge's idempotence check and `PurgeTxLog`'s branch-min. |
| `PurgeTxLog` | Unary | Per-branch: GC the four hot tx-log family tables (`commit_tx_log` / `tx_table_log` / `abort_tx_log` / `begin_tx_log`) up to a branch-wide `max_micros = 1 + MIN(purged_at over tables in tx_table_log[B])`. Empty-set fast-path leaves `purged_at_micros` unset. See [algorithms.md#purge-tx-log-branch-scoped](../algorithms.md#purge-tx-log-branch-scoped). |
| `CompactPersistSegments` | Unary | Per-table: merge small upsert/delete log segment files into fewer, larger files (semantics preserved) |
| `Snapshot` | Unary | Per-table: build a read-optimized snapshot — apply deletes, coalesce versions per `RetentionConfig`, emit tombstone-free segments |
| `SweepSegments` | Unary | Per-branch: physically delete cold segment files queued for removal by past compact waves; gated on the universal grace window (ADR 0019) |

`Persist`, `Snapshot`, and `Purge` each take their own per-operation
per-table advisory lock — `persist:{table_uuid}:{branch_uuid}`,
`snapshot:{table_uuid}:{branch_uuid}`, `purge:{table_uuid}:{branch_uuid}`
respectively. Same-operation pairs on T serialize; cross-operation
pairs (`Persist↔Snapshot`, `Persist↔Purge`, `Snapshot↔Purge`) are
lock-free because pillars 1 (plan-time threading) and 3 (grace
window) make them safe without serialization. `Persist(T1)` and
`Persist(T2)` run in parallel on different keys. `PurgeTxLog` takes
a branch-scoped key `purge_tx_log:{branch_uuid}` — at most one pass
per branch at a time, orthogonal to the per-table keys. See
[ADR 0019](../decisions/0019-plan-time-pinning-and-universal-grace-window.md)
for the lock-scoping decision, the current
`Persist(T) → Snapshot(T) → Purge(T)` chain semantics, and the
grace-bounded Purge that backs it (supersedes
[ADR 0018](../decisions/0018-purge-as-hot-cold-visibility-cutoff.md)
on the cutoff source and the shared advisory key).

See [algorithms.md#persist-hot--cold](../algorithms.md#persist-hot--cold),
[purge](../algorithms.md#purge-hot-rows--watermark),
[purge commit_tx_log](../algorithms.md#purge-tx-log-branch-scoped),
[compact](../algorithms.md#compact), and
[snapshot](../algorithms.md#snapshot) for the step-by-step algorithms.

## Dependencies

- **Postgres** — hot-tier reads and metadata writes (segment metadata
  rows in `*_persist_segments`, `*_snapshot_segments`, `*_snapshots`).
- **Object storage** — cold-tier reads (compact, snapshot) and writes
  (persist, compact, snapshot).

No dependency on other microservices — lifecycle operates on segment
files directly. The query service observes lifecycle's changes through
its in-process read planner (`QueryManager::plan`), not through
lifecycle RPCs.

## Config

| Env var | Purpose |
|---|---|
| `DATABASE_URL`, `PG_POOL_MIN`, `PG_POOL_MAX` | Postgres pool |
| `BIND_ADDR` | gRPC server bind |
| `LIFECYCLE_DEFAULT_MAX_SEGMENT_BYTES` | Ceiling on compacted segment size; also the persist size target |
| `LIFECYCLE_SEGMENT_READ_CONCURRENCY` | Max in-flight cold-segment reads during snapshot's `stream_all_cold_parts` (memory-safety cap) |
| `QUERY_TIMEOUT_SECONDS` | Destructive-side grace window for Purge of hot rows + compaction GC; MUST equal the query service's `QUERY_TIMEOUT_SECONDS` (ADR 0019) |
| `OBJECT_STORAGE_PROVIDER`, `OBJECT_STORAGE_BUCKET`, `OBJECT_STORAGE_FORMAT`, `OBJECT_STORAGE_*` | Cold storage |

## Streaming

None. All RPCs are unary. Progress visibility for long-running ops is
currently status-code-only; a streaming progress API may land in a
future ticket if operators need it.

## Error taxonomy

| Manager raises | Servicer returns | Examples |
|---|---|---|
| `NotFoundError` | `NOT_FOUND` | `Persist(unknown_physical_table)`, `Snapshot(missing branch)` |
| `InvalidRequestError` | `INVALID_ARGUMENT` | Invalid `RetentionConfig`, incompatible segment set for compaction |
| `ApiError` (base) | `INTERNAL` | Object-storage write failure, partial-persist recovery failure |

## Failure modes

- **Partial persist.** Phase 1 writes segment metadata with
  `commit_micros = NULL` before the file lands; Phase 2 flips
  the parent commit timestamps in a single Postgres tx. A crash
  between phases leaves an `IS NULL` metadata row pointing at a real
  file — invisible to reads, detectable via `written_at_micros`, and
  safely cleaned up by a repersist. The two-phase design guarantees
  data exists in at least one tier at every instant. The hot delete
  is no longer in Persist's Phase 2 — it moves to `Purge`, so between
  Persist and Purge the rows exist in *both* tiers and queries serve
  from hot (see [ADR 0018](../decisions/0018-purge-as-hot-cold-visibility-cutoff.md)).
- **Partial purge.** Same two-phase shape: Phase 1 inserts a
  `table_purge_metadata` row with `commit_micros = NULL`,
  Phase 2 deletes the hot rows and flips the commit timestamp in a
  single Postgres tx. A crash mid-purge leaves the uncommitted
  metadata row plus the hot rows; the next purge runs the no-op
  fast-path or retries the same watermark and the deterministic
  `table_purge_uuid` slots into the same row via `DO UPDATE`.
- **Snapshot OOM.** Resolving large delete sets + version coalesce is
  memory-intensive. A snapshot OOM kills the lifecycle container only
  — reads and writes keep serving. The in-flight snapshot is marked
  failed and can be retried; partial cold-tier artifacts (if any) are
  cleaned up via the `written_at_micros` GC path.
- **Cold-storage write failure mid-persist.** Segment file write fails →
  Phase 1 metadata row is left with `commit_micros = NULL` and
  no file. GC sweeps these; the next persist re-attempts.
- **No untracked orphans.** Every cold-storage file is tracked by a
  metadata row. On failure paths, files are always deleted *before*
  their metadata rows so an orphan never arises.
