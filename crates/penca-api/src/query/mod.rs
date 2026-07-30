//! Query operations for reading data and metadata.
//!
//! [`QueryManager`] provides read-only access to branches, transactions,
//! and table data. The `read_data` method orchestrates the full merge-on-read
//! path via `penca_merge::stream_merged`.
//!
//! The metadata reads live on `QueryManager` (ADR 0028) so its caches serve
//! the query AND write read paths through one eligibility gate: `meta_plan`
//! holds the read-plan assembly, `meta_resolve` the system-table resolves +
//! metadata getters.

mod cold_read;
mod index_select;
mod meta_plan;
mod meta_resolve;

use crate::scope::ResolvedScope;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::ipc::convert::try_schema_from_ipc_buffer;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use penca_core::{Format, PersistSegment, Plan, naming};
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::pg::PgDriver;
use penca_db::driver::{DbDriver, SqlValue};
use penca_dl::cache::SegmentCache;
use penca_dl::driver::{DatafusionDlDriver, DlDriver};
use penca_dl::list_cache::SnapshotListCache;
use penca_dl::{SessionContext, SessionState};
use penca_format::reader::FormatReader;
use penca_merge::sql::build_merge_resolved;
use penca_merge::{IndexSeek, ReadSnapshot};
use penca_proto::external::v1::create_branch_request::ForkPoint;
use penca_proto::external::v1::{
    AuditDataRequest, GetBranchRequest, GetBranchResponse, GetCatalogRequest, GetCatalogResponse,
    GetIndexRequest, GetIndexResponse, GetSchemaRequest, GetSchemaResponse, GetTableRequest,
    GetTableResponse, ListBranchesRequest, ListBranchesResponse, ListCatalogsRequest,
    ListCatalogsResponse, ListIndexesRequest, ListIndexesResponse, ListSchemasRequest,
    ListSchemasResponse, ListTablesRequest, ListTablesResponse, Projection, ReadDataRequest, Table,
    Watermark,
};
use penca_sql::Dialect;
use penca_storage_cold::ColdStorageClient;
use penca_storage_hot::{
    AuditRowFilter, HotStorageClient, audit_delete_schema, audit_upsert_schema,
    execute_query_as_batch, stream_query_as_batches,
};
use penca_storage_meta::LifecycleManager;
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::error::ApiError;
use crate::pagination::{pagination_from_request, take_page_and_next_token, timestamp_bounds};
use crate::resolve::{parse_resolved_uuid, resolve_branch, resolve_catalog};

/// Pinned, boxed stream of `RecordBatch` results used by `read_data` and `audit_*`.
pub type BatchStream<'a> = Pin<Box<dyn Stream<Item = Result<RecordBatch, ApiError>> + Send + 'a>>;

/// Planning result for a single `audit_data` RPC: identifier
/// resolution, the table's Arrow schema, both cold persist-segment
/// lists, and the per-tier combined `commit_micros` windows
/// (already folded with the ADR 0018 hot/cold cutoff so the audit
/// methods don't recompute it).
///
/// Built once per request (`plan_audit`) and threaded into both
/// `audit_upserts` and `audit_deletes` to avoid a duplicate
/// `table_persist_segment_metadata` round-trip per side.
pub struct AuditPlan {
    catalog_uuid: uuid::Uuid,
    branch_uuid: uuid::Uuid,
    table_uuid: uuid::Uuid,
    user_schema: SchemaRef,
    /// The table's declared primary keys, needed to construct the widened
    /// cold delete schema for `cold_delete_audit_batches`.
    primary_keys: Vec<String>,
    /// Combined cold-side committed_at window as half-open `[from, to)`
    /// micros scalars. Both `None` pre-Purge: the cold tier contributes
    /// nothing and both segment lists below are empty. Post-Purge,
    /// `cold_to = min(user_to, hot_min)` — the user bound capped by ADR
    /// 0018's hot/cold cutoff. The cold helpers apply these as an
    /// inclusive-from / exclusive-to filter on each batch's
    /// `commit_micros`.
    cold_from: Option<i64>,
    cold_to: Option<i64>,
    /// Combined hot-side committed_at window as half-open `[from, to)`
    /// micros scalars: `from = max(user_from, hot_min)`, `to = user_to`.
    /// The hot SQL builder applies these as `committed_at >= from AND <
    /// to`, where the `from` arm structurally excludes any row that's
    /// already in cold post-Purge.
    hot_from: Option<i64>,
    hot_to: Option<i64>,
    /// Half-open `commit_seq_num` window for a seq-axis `committed`
    /// audit (both `None` for the micros axis). Applied as a per-row drop
    /// on the cold scan and a `t.commit_seq_num` predicate on the hot stream;
    /// the committed_at tier fence above still partitions hot vs cold.
    /// All committed-window args here are split `[from, to)` scalars (not
    /// `IntegerRange`) so the micros and seq axes are passed uniformly;
    /// `IntegerRange` stays at the proto boundary.
    seq_from: Option<i64>,
    seq_to: Option<i64>,
    cold_upsert_segments: Vec<PersistSegment>,
    cold_delete_segments: Vec<PersistSegment>,
    /// When set, `audit_data` reattaches per-tx `author`/`comment` by joining
    /// the cold `tx_log`; when unset those columns are omitted.
    include_tx_metadata: bool,
    /// The branch's committed cold `tx_log` segments (the commit map), read
    /// once here and joined by the cold audit builders on `commit_seq_num`
    /// when `include_tx_metadata` is set. Empty when the flag is off.
    tx_log_segments: Vec<PersistSegment>,
    /// The parent branch's persist segments, for a forked branch's
    /// audit — so `audit_data` surfaces the inherited history. Empty for a
    /// non-forked branch. Streamed after the branch's own cold segments,
    /// capped per-row at `base_seq_to` so the child never audits the parent's
    /// post-fork rows.
    base_cold_upsert_segments: Vec<PersistSegment>,
    base_cold_delete_segments: Vec<PersistSegment>,
    /// The parent branch's committed cold `tx_log` segments, joined onto the
    /// inherited base rows to reattach their `author`/`comment` when
    /// `include_tx_metadata` is set — the parent's pre-fork commits live in
    /// the parent's tx_log, not the child's. Empty for a non-forked branch or
    /// when the flag is off.
    base_tx_log_segments: Vec<PersistSegment>,
    /// Exclusive per-row `commit_seq_num` upper bound for the base (parent)
    /// segments = `min(seq_to, fork_seed + 1)`. `None` for a non-forked branch.
    base_seq_to: Option<i64>,
    /// ids point-lookup restriction, decoded once here so both
    /// stream halves (upserts + deletes) share one derivation.
    row_uuids: Option<Vec<uuid::Uuid>>,
    /// ADR 0019 §"Four-part mechanism" item 4 — wall-clock deadline
    /// captured at `plan_audit` entry (`T_q + query_timeout`). Threaded
    /// into both `audit_upserts` and `audit_deletes` so the two stream
    /// halves share one bound from the original RPC start, not refreshed
    /// per call.
    deadline: tokio::time::Instant,
}

/// Penca read operations (branches, transactions, data).
///
/// Holds service-level config (e.g. pagination defaults, batch sizes).
/// Database state is accessed via the driver parameter on each method.
#[derive(Clone)]
pub struct QueryManager {
    pub default_page_size: i64,
    pub default_stream_batch_size: u32,
    /// Max in-flight segment reads during stream_merged's snapshot phase.
    /// Memory-safety knob — each read materializes a whole segment.
    pub segment_read_concurrency: usize,
    /// Skip snapshot segment pruning unless the planned segment count exceeds
    /// this (from `QUERY_SNAPSHOT_PRUNE_MIN_SEGMENTS`).
    pub snapshot_prune_min_segments: usize,
    /// Cap on the cartesian product of per-column IN-list bindings
    /// when the planner selects a covering user index (from
    /// `QUERY_INDEX_SEEK_MAX_PROBE_TUPLES`). Over the cap the index is
    /// skipped — a correctness-preserving optimization cutoff (the read
    /// falls back to full scan + residual filter, never a truncated probe
    /// set). `0` disables user-index selection entirely.
    pub index_seek_max_probe_tuples: usize,
    /// Hard cap on `read_data` / `audit_data` execute-time duration,
    /// in micros. Derived from `QUERY_TIMEOUT_SECONDS` at config load
    /// (see ADR 0019). Stored in micros so SQL math against
    /// `commit_micros` / `persisted_at_micros` stays in the same
    /// unit as every other timestamp in the system.
    pub query_timeout_micros: i64,
    /// Process-lifetime cache of decoded snapshot segments, shared (behind
    /// `Arc`) into every per-query `DatafusionDlDriver`. Budget is
    /// env-configured by the hosting service.
    pub snapshot_cache: Arc<SegmentCache>,
    /// Process-lifetime cache of snapshot segment *lists* — the immutable
    /// `(segments, W_snap)` baseline keyed `(catalog, branch, table, W_snap)`,
    /// so a warm read skips the per-read Postgres snapshot-list round-trip.
    /// TTL + entry cap are env-configured, TTL `<=` the GC grace.
    pub snapshot_list_cache: Arc<SnapshotListCache>,
    /// Process-wide cold-session template: the default function registry +
    /// analyzer/optimizer rules, built once at service startup and injected
    /// into every per-query `DatafusionDlDriver` so cold reads clone it
    /// (~71 µs) instead of paying the warm `SessionContext::new()` cost
    /// (~128 µs/call in release). Held opaquely, built by the binary via
    /// `penca_dl::build_cold_session_template`.
    pub session_template: Arc<SessionState>,
}

impl QueryManager {
    /// Build a metadata-reader `QueryManager` for the write and lifecycle
    /// services. The caller supplies the caches: the write service shares the
    /// query path's, so its hot point-write resolve hits the snapshot-list
    /// cache, while the lifecycle service passes disabled ones (it always
    /// reads fresh). The page-size / concurrency / timeout knobs are inert for
    /// this role — the resolves/getters hardcode their own `stream_merged`
    /// concurrency, never paginate, and never wrap a read in the query-timeout
    /// deadline.
    pub fn for_metadata_reads(
        session_template: Arc<SessionState>,
        snapshot_cache: Arc<SegmentCache>,
        snapshot_list_cache: Arc<SnapshotListCache>,
    ) -> Self {
        Self {
            default_page_size: 1000,
            default_stream_batch_size: 1000,
            segment_read_concurrency: 2,
            snapshot_prune_min_segments: 1,
            index_seek_max_probe_tuples: 1024,
            query_timeout_micros: 0,
            snapshot_cache,
            snapshot_list_cache,
            session_template,
        }
    }
}

/// ADR 0019 §"Four-part mechanism" item 4 — wrap a `BatchStream` so
/// every `next()` is racing a single deadline. The first poll past
/// `deadline` yields `ApiError::QueryTimeout` and the stream ends;
/// the gRPC layer maps it to `RESOURCE_EXHAUSTED`.
///
/// One shared deadline (not refreshed per poll) is what makes the
/// grace argument hold: the cap is `T_q + query_timeout`, where `T_q`
/// is the RPC start.
fn with_query_timeout<'a>(
    deadline: tokio::time::Instant,
    inner: BatchStream<'a>,
) -> BatchStream<'a> {
    Box::pin(async_stream::try_stream! {
        let mut inner = inner;
        loop {
            // `timeout_at(deadline, ...)` uses an absolute `Instant`, so
            // every poll races against the same wall-clock cap captured
            // at the RPC start — the bound is `T_q + query_timeout`,
            // not refreshed per poll.
            match tokio::time::timeout_at(deadline, inner.next()).await {
                Ok(Some(Ok(batch))) => yield batch,
                Ok(Some(Err(e))) => Err(e)?,
                Ok(None) => break,
                Err(_) => Err(ApiError::QueryTimeout(
                    "retry with a fresh plan".to_string(),
                ))?,
            }
        }
    })
}

/// Race a one-shot plan-phase future against the same `T_q + cap`
/// deadline that [`with_query_timeout`] wraps the stream phase with —
/// see ADR 0019 §"Four-part mechanism" item 4. Without this, a plan-
/// phase metadata round-trip that pins on a PG lock past the cap
/// would only surface `RESOURCE_EXHAUSTED` on the first stream poll,
/// after the caller already waited the full hang.
async fn await_within_query_timeout<F, T, E>(
    deadline: tokio::time::Instant,
    fut: F,
) -> Result<T, ApiError>
where
    F: Future<Output = Result<T, E>>,
    E: Into<ApiError>,
{
    match tokio::time::timeout_at(deadline, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err(ApiError::QueryTimeout(
            "retry with a fresh plan".to_string(),
        )),
    }
}

/// Derive the user-visible `SchemaRef` for `read_data` from
/// the table's full schema and the request's optional projection.
/// Three states:
///   - `None` (field unset): all user columns.
///   - `Some(p)` with `p.columns` empty: 0-column projection — yields
///     zero-width batches whose `num_rows` still reflects the matched
///     row count, so `SELECT COUNT(*)`-shaped queries get cardinality
///     without materializing user data.
///   - `Some(p)` with `p.columns` non-empty: project to those named
///     columns in declaration order; unknown names → `InvalidRequest`.
fn apply_projection(
    full_schema: SchemaRef,
    projection: Option<&Projection>,
) -> Result<SchemaRef, ApiError> {
    match projection {
        None => Ok(full_schema),
        Some(p) if p.columns.is_empty() => Ok(Arc::new(ArrowSchema::empty())),
        Some(p) => {
            let mut indices: Vec<usize> = Vec::with_capacity(p.columns.len());
            let mut missing: Vec<&str> = Vec::new();
            for name in &p.columns {
                match full_schema.index_of(name) {
                    Ok(i) => indices.push(i),
                    Err(_) => missing.push(name),
                }
            }
            if !missing.is_empty() {
                return Err(ApiError::InvalidRequest(format!(
                    "Unknown column(s): {}",
                    missing.join(", ")
                )));
            }
            Ok(Arc::new(
                full_schema.project(&indices).map_err(ApiError::Arrow)?,
            ))
        }
    }
}

/// Derive the read's visibility snapshot from `open_tx_uuid` (RYOW) /
/// `as_of_micros` (explicit point-in-time) / `as_of_seq` (explicit seq travel)
/// / neither (pinned to the per-branch seq frontier). The request axes are
/// mutually exclusive — see `ReadDataRequest` proto comments. When
/// `open_tx_uuid` is supplied, validates the format, looks up the tx on the
/// request's branch leaf partitions (so a tx on a different branch surfaces as
/// `NotFound`), and rejects expired / aborted / committed states with
/// `FailedPrecondition`.
// The snapshot derivation inputs are irreducible.
#[allow(clippy::too_many_arguments)]
async fn resolve_query_snapshot<D>(
    driver: &D,
    deadline: tokio::time::Instant,
    catalog_uuid: &uuid::Uuid,
    branch_uuid: &uuid::Uuid,
    request_open_tx_uuid: Option<&str>,
    request_commit_micros: Option<i64>,
    request_commit_seq_num: Option<i64>,
    default_frontier: Option<i64>,
) -> Result<ReadSnapshot, ApiError>
where
    D: DbDriver<Row = PgRow>,
{
    // The two `as_of` arms are mutually exclusive by the proto oneof;
    // either is mutually exclusive with open_tx (RYOW only at the tx's own
    // begin frontier).
    if (request_commit_micros.is_some() || request_commit_seq_num.is_some())
        && request_open_tx_uuid.is_some()
    {
        return Err(ApiError::InvalidRequest(
            "exactly one of as_of / open_tx_uuid may be set".to_string(),
        ));
    }
    if let Some(open_tx_uuid) = request_open_tx_uuid {
        // Validate format up front (also prevents SQL injection when
        // the tx_uuid is interpolated into the synthetic UNION row
        // literal in the merge SQL builder). Once parsed we keep the
        // typed `uuid::Uuid` and pass it through; downstream code never
        // re-parses or re-stringifies.
        let tx_uuid = uuid::Uuid::parse_str(open_tx_uuid).map_err(|_| {
            ApiError::InvalidRequest(format!("malformed open_tx_uuid: {open_tx_uuid}"))
        })?;

        // Target the leaf partitions for the request's branch.
        // get_tx_status's begin_tx_log lookup against the leaf
        // therefore returns `None` for any tx that lives on a
        // different branch — no separate branch-mismatch check needed.
        let begin_partition = naming::begin_tx_log_partition(catalog_uuid, branch_uuid);
        let abort_partition = naming::abort_tx_log_partition(catalog_uuid, branch_uuid);
        let tx_partition = naming::commit_tx_log_partition(catalog_uuid, branch_uuid);
        let hot = HotStorageClient;
        let status = await_within_query_timeout(
            deadline,
            hot.get_tx_status(
                driver,
                &begin_partition,
                &abort_partition,
                &tx_partition,
                &tx_uuid,
                /*for_update=*/ false,
            ),
        )
        .await?
        .ok_or_else(|| {
            ApiError::NotFound(format!(
                "open_tx_uuid not found in begin_tx_log on branch {branch_uuid} \
                 (tx may be on a different branch or was never begun): \
                 {open_tx_uuid}"
            ))
        })?;

        match status {
            penca_storage_hot::TxStatus::Open {
                began_at_seq_num, ..
            } => Ok(ReadSnapshot::OpenTx {
                began_at_seq_num,
                tx_uuid,
            }),
            penca_storage_hot::TxStatus::Expired { expired_at_micros } => {
                Err(ApiError::FailedPrecondition(format!(
                    "open_tx_uuid expired at {expired_at_micros} (lifecycle sweep will \
                     move it to abort_tx_log): {open_tx_uuid}"
                )))
            }
            penca_storage_hot::TxStatus::Aborted {
                aborted_at_micros, ..
            } => Err(ApiError::FailedPrecondition(format!(
                "open_tx_uuid was aborted at {aborted_at_micros}: {open_tx_uuid}"
            ))),
            penca_storage_hot::TxStatus::Committed { commit_micros } => {
                Err(ApiError::FailedPrecondition(format!(
                    "open_tx_uuid was already committed at {commit_micros}; \
                 re-issue the read without open_tx_uuid (or pass as_of_micros to \
                 view post-commit state): {open_tx_uuid}"
                )))
            }
        }
    } else if let Some(seq) = request_commit_seq_num {
        // Explicit seq-axis pin — exact, no resolution.
        Ok(ReadSnapshot::AsOfSeq(seq))
    } else if let Some(ts) = request_commit_micros {
        Ok(ReadSnapshot::AsOfMicros(ts))
    } else {
        // A read with neither an open tx nor an explicit as_of pins a bounded
        // snapshot at the per-branch seq frontier (counter - 1) rather than
        // pg_now, so "read latest" composes with the seq tier-fence and
        // resolves names on the same axis as data. `read_data` threads the
        // frontier it captured for identifier resolution as
        // `default_frontier` so the whole RPC shares ONE pin; a single-shot
        // caller passes None and self-captures here. Still never an unbounded
        // read — the frontier is a bounded upper.
        let frontier = match default_frontier {
            Some(seq) => seq,
            None => {
                QueryManager::branch_seq_frontier(
                    driver,
                    &catalog_uuid.to_string(),
                    &branch_uuid.to_string(),
                )
                .await?
            }
        };
        Ok(ReadSnapshot::LatestSeq(frontier))
    }
}

impl QueryManager {
    /// Wall-clock deadline for the current RPC: `now + query_timeout_micros`.
    /// Captured once at the manager entry point and threaded into the
    /// returned `BatchStream` (and, for `audit_data`, into the `AuditPlan`).
    fn deadline_now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_micros(self.query_timeout_micros as u64)
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(catalog_uuid = tracing::field::Empty),
    )]
    pub async fn get_catalog(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &GetCatalogRequest,
    ) -> Result<GetCatalogResponse, ApiError> {
        let catalog = LifecycleManager::get_catalog(
            driver,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;

        match catalog {
            Some(c) => {
                tracing::Span::current().record("catalog_uuid", c.catalog_uuid.as_str());
                Ok(GetCatalogResponse { catalog: Some(c) })
            }
            None => Err(ApiError::NotFound("catalog not found".to_string())),
        }
    }

    /// A branch's max committed `commit_seq_num` (the inclusive seq frontier).
    /// The SQL server's `GetFlightInfo` captures this once to pin an
    /// auto-commit statement's reads on the seq axis. Thin wrapper over the
    /// same [`QueryManager::branch_seq_frontier`] the `read_data` default path
    /// uses, so the SQL pin and the gRPC default share one max-committed source.
    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn get_max_commit_seq_num(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<i64, ApiError> {
        Ok(QueryManager::branch_seq_frontier(driver, catalog_uuid, branch_uuid).await?)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn list_catalogs(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &ListCatalogsRequest,
    ) -> Result<ListCatalogsResponse, ApiError> {
        let (page_size, offset) =
            pagination_from_request(request.pagination.as_ref(), self.default_page_size);

        // Treat the empty-string proto default as "no filter".
        let owner = request.owner.as_deref().filter(|s| !s.is_empty());
        let catalogs =
            LifecycleManager::list_catalogs_paginated(driver, owner, page_size + 1, offset).await?;

        let (page, next_page_token) = take_page_and_next_token(catalogs, page_size, offset);

        Ok(ListCatalogsResponse {
            catalogs: page,
            next_page_token,
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn get_branch(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &GetBranchRequest,
    ) -> Result<GetBranchResponse, ApiError> {
        let catalog = resolve_catalog(
            driver,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        // resolve_branch returns the whole Branch, so it is reused as the
        // response instead of re-reading branch_store.
        let branch = resolve_branch(
            driver,
            &catalog_uuid,
            request.branch_uuid.as_deref(),
            request.branch_name.as_deref(),
        )
        .await?;
        let branch_uuid = parse_resolved_uuid(&branch.branch_uuid, "branch_uuid")?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));

        Ok(GetBranchResponse {
            branch: Some(branch),
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(catalog_uuid = tracing::field::Empty),
    )]
    pub async fn list_branches(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &ListBranchesRequest,
    ) -> Result<ListBranchesResponse, ApiError> {
        let catalog = resolve_catalog(
            driver,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;

        tracing::Span::current().record("catalog_uuid", tracing::field::display(&catalog_uuid));

        let catalog_str = catalog_uuid.to_string();
        let (page_size, offset) =
            pagination_from_request(request.pagination.as_ref(), self.default_page_size);

        let rows =
            LifecycleManager::list_branches_paginated(driver, &catalog_str, page_size + 1, offset)
                .await?;

        let (branches, next_page_token) = take_page_and_next_token(rows, page_size, offset);

        Ok(ListBranchesResponse {
            branches,
            next_page_token,
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn get_schema<L: DlDriver + ?Sized>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl_driver: &L,
        request: &GetSchemaRequest,
    ) -> Result<GetSchemaResponse, ApiError> {
        let scope = ResolvedScope::resolve_schema(self, driver, dl_driver, request, None).await?;
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&scope.catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&scope.branch_uuid));
        let schema_uuid = scope.schema_uuid.ok_or_else(|| {
            ApiError::InvalidRequest("must provide schema_uuid or schema_name".into())
        })?;
        span.record("schema_uuid", tracing::field::display(&schema_uuid));

        // resolve_schema populates schema_row whenever a schema
        // identifier is present (required above), so reuse the carried row
        // directly instead of re-resolving it by uuid.
        let schema = scope.schema_row.ok_or_else(|| {
            ApiError::Internal("resolve_schema did not populate schema_row".into())
        })?;
        Ok(GetSchemaResponse {
            schema: Some(schema),
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn list_schemas<L: DlDriver + ?Sized>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl_driver: &L,
        request: &ListSchemasRequest,
    ) -> Result<ListSchemasResponse, ApiError> {
        let (page_size, offset) =
            pagination_from_request(request.pagination.as_ref(), self.default_page_size);
        let scope = ResolvedScope::resolve_schema(self, driver, dl_driver, request, None).await?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&scope.catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&scope.branch_uuid));

        let catalog_uuid_str = scope.catalog_uuid.to_string();
        let branch_str = scope.branch_uuid.to_string();

        let schemas = self
            .list_schemas_paginated(
                driver,
                dl_driver,
                &catalog_uuid_str,
                page_size + 1,
                offset,
                Some(&branch_str),
                &scope.snapshot,
            )
            .await?;

        let (page, next_page_token) = take_page_and_next_token(schemas, page_size, offset);

        Ok(ListSchemasResponse {
            schemas: page,
            next_page_token,
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn get_table<L: DlDriver + ?Sized>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl_driver: &L,
        request: &GetTableRequest,
    ) -> Result<GetTableResponse, ApiError> {
        let scope = ResolvedScope::resolve_table(self, driver, dl_driver, request, None).await?;
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&scope.catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&scope.branch_uuid));
        let catalog_str = scope.catalog_uuid.to_string();
        let schema_uuid = scope.schema_uuid.ok_or_else(|| {
            ApiError::Internal("resolve_table did not populate schema_uuid".into())
        })?;
        span.record("schema_uuid", tracing::field::display(&schema_uuid));
        let schema_str = schema_uuid.to_string();

        // resolve_table always carries the resolved
        // `__penca_system__.tables` row now (by-uuid reads it catalog-wide,
        // by-name schema-scoped), so reuse it instead of re-fetching by uuid.
        let mut table = scope
            .table_row
            .ok_or_else(|| ApiError::Internal("resolve_table did not populate table_row".into()))?;
        span.record("table_uuid", table.table_uuid.as_str());
        crate::retention::apply_effective_retention(
            self,
            driver,
            dl_driver,
            &catalog_str,
            &schema_str,
            &mut table,
        )
        .await?;
        Ok(GetTableResponse { table: Some(table) })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn list_tables<L: DlDriver + ?Sized>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl_driver: &L,
        request: &ListTablesRequest,
    ) -> Result<ListTablesResponse, ApiError> {
        let scope = ResolvedScope::resolve_schema(self, driver, dl_driver, request, None).await?;
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&scope.catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&scope.branch_uuid));
        let schema_uuid = scope.schema_uuid.ok_or_else(|| {
            ApiError::InvalidRequest("must provide schema_uuid or schema_name".into())
        })?;
        span.record("schema_uuid", tracing::field::display(&schema_uuid));
        let branch_str = scope.branch_uuid.to_string();
        let catalog_uuid_str = scope.catalog_uuid.to_string();
        let schema_uuid_str = schema_uuid.to_string();
        let mut tables = self
            .meta_list_tables(
                driver,
                dl_driver,
                &catalog_uuid_str,
                &schema_uuid_str,
                Some(&branch_str),
                &scope.snapshot,
            )
            .await?;

        // Hoist parent fetches above the loop — every table in the
        // response shares the same `(catalog, schema)` parents, so two
        // reads suffice instead of 2N.
        let schema_rc = crate::retention::fetch_parent_retention(
            self,
            driver,
            dl_driver,
            &catalog_uuid_str,
            &schema_uuid_str,
        )
        .await?;
        for table in &mut tables {
            let effective =
                crate::retention::coalesce_retention(&table.retention_config, &schema_rc);
            table.retention_config = Some(effective);
        }

        let (page_size, offset) =
            pagination_from_request(request.pagination.as_ref(), self.default_page_size);
        let start = offset as usize;
        // In-memory pagination: skip to the requested offset, then
        // over-fetch by one so `take_page_and_next_token` can detect a
        // next page without re-checking the total.
        let after_skip: Vec<Table> = tables
            .into_iter()
            .skip(start)
            .take((page_size + 1) as usize)
            .collect();
        let (page, next_page_token) = take_page_and_next_token(after_skip, page_size, offset);

        Ok(ListTablesResponse {
            tables: page,
            next_page_token,
        })
    }

    /// Get one index definition by uuid or `(table, name)`, time-travelled
    /// by the request's `open_tx_uuid` / `as_of_micros` pin. NOT_FOUND when
    /// no index resolves at that snapshot.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn get_index<L: DlDriver + ?Sized>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl_driver: &L,
        request: &GetIndexRequest,
    ) -> Result<GetIndexResponse, ApiError> {
        let scope = ResolvedScope::resolve_table(self, driver, dl_driver, request, None).await?;
        let catalog_str = scope.catalog_uuid.to_string();
        let branch_str = scope.branch_uuid.to_string();
        let table = scope
            .table_row
            .ok_or_else(|| ApiError::Internal("resolve_table did not populate table_row".into()))?;
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&scope.catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&scope.branch_uuid));
        span.record("table_uuid", table.table_uuid.as_str());
        let index = self
            .meta_get_index(
                driver,
                dl_driver,
                &catalog_str,
                &table.table_uuid,
                request.index_uuid.as_deref(),
                request.index_name.as_deref(),
                Some(&branch_str),
                &scope.snapshot,
            )
            .await?
            .ok_or_else(|| ApiError::NotFound("index not found".to_string()))?;
        Ok(GetIndexResponse { index: Some(index) })
    }

    /// List every index defined on a table at the request's snapshot.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn list_indexes<L: DlDriver + ?Sized>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl_driver: &L,
        request: &ListIndexesRequest,
    ) -> Result<ListIndexesResponse, ApiError> {
        let scope = ResolvedScope::resolve_table(self, driver, dl_driver, request, None).await?;
        let catalog_str = scope.catalog_uuid.to_string();
        let branch_str = scope.branch_uuid.to_string();
        let table = scope
            .table_row
            .ok_or_else(|| ApiError::Internal("resolve_table did not populate table_row".into()))?;
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&scope.catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&scope.branch_uuid));
        span.record("table_uuid", table.table_uuid.as_str());
        let indexes = self
            .meta_list_indexes(
                driver,
                dl_driver,
                &catalog_str,
                &table.table_uuid,
                Some(&branch_str),
                &scope.snapshot,
            )
            .await?;
        // Index counts per table are small; the response lists all of
        // them (no pagination — see ListIndexesRequest).
        Ok(ListIndexesResponse { indexes })
    }

    /// Execute the full merge-on-read path for a single table.
    ///
    /// Resolves identifiers → fetches arrow schema → calls
    /// `QueryManager::plan` → delegates to `penca_merge::stream_merged`.
    /// Returns a stream of `RecordBatch`.
    ///
    /// Uses `async_stream` internally so the `Plan` is owned
    /// by the stream generator, avoiding lifetime issues with
    /// `stream_merged`'s borrow of `&plan`.
    ///
    /// `readers` is an `Arc<HashMap<..>>` because the cold-tier driver
    /// ([`DatafusionDlDriver`]) needs a `'static` handle on the map
    /// to hand into DataFusion's owned table providers.
    ///
    /// **Span lifetime caveat:** `#[instrument]` here brackets the
    /// plan-phase future only — the span ends when the boxed stream is
    /// returned to the caller, not when the stream is exhausted. The
    /// stream-phase timing is the gRPC layer's request span's job.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
            ids_rows = tracing::field::Empty,
        ),
    )]
    pub async fn read_data<'a, D, R>(
        &'a self,
        driver: &'a D,
        readers: Arc<HashMap<i32, R>>,
        request: &ReadDataRequest,
    ) -> Result<BatchStream<'a>, ApiError>
    where
        D: DbDriver<Row = PgRow>,
        R: FormatReader + 'static,
    {
        // ADR 0019 §"Four-part mechanism" item 4 — capture T_q + cap
        // before any await so the deadline binds the full RPC, including
        // the metadata round-trips below and the stream phase.
        let deadline = self.deadline_now();
        // Metadata reads on `__penca_system__.tables` go through
        // stream_merged (hot+cold), so they need a DlDriver; the same driver
        // flows into the user-data stream_merged below, so share one instance.
        let dl = DatafusionDlDriver::new(
            readers.clone(),
            self.snapshot_cache.clone(),
            self.session_template.clone(),
        );
        // The default path (no as_of, no open_tx) and the seq arm both pin the
        // commit_seq_num axis. The merge probes run as independent statements
        // and must share ONE bounded pin, captured once below — the seq
        // frontier for a default read, the request's explicit axis otherwise.
        let (request_commit_micros, request_commit_seq_num) =
            crate::scope::read_data_as_of_axes(&request.as_of);
        // Resolve the identifier-stage snapshot from the request's as_of /
        // open_tx so a renamed table/schema is findable at its historical name
        // on the SAME axis as the data. On the default path resolve_table
        // self-captures the per-branch seq frontier — hence the `None` — and
        // that same frontier is reused for the data read below so the whole
        // RPC shares one pin. The micros/seq/open_tx arms ignore it.
        let scope = ResolvedScope::resolve_table(self, driver, &dl, request, None).await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        // The seq frontier resolve_table pinned for a default read — threaded
        // into the data-read snapshot so identifiers + data share it. `None`
        // on any explicit-as_of / open_tx read (those arms self-derive).
        let default_frontier = if request_commit_micros.is_none()
            && request_commit_seq_num.is_none()
            && request.open_tx_uuid.is_none()
        {
            match &scope.snapshot {
                ReadSnapshot::AsOfSeq(frontier) | ReadSnapshot::LatestSeq(frontier) => {
                    Some(*frontier)
                }
                _ => None,
            }
        } else {
            None
        };
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        let schema_uuid = scope.schema_uuid.ok_or_else(|| {
            ApiError::Internal("resolve_table did not populate schema_uuid".into())
        })?;
        span.record("schema_uuid", tracing::field::display(&schema_uuid));
        // resolve_table already carries the resolved `__penca_system__.tables`
        // row, so its table_uuid + Arrow schema are reused instead of refetched.
        let table = scope
            .table_row
            .ok_or_else(|| ApiError::Internal("resolve_table did not populate table_row".into()))?;
        // Validate the stored row_uuid here (clean Internal on the
        // server-corruption-only malformed case) rather than letting the raw
        // string reach `plan()`'s internal parse — matches plan_audit.
        let table_uuid = parse_resolved_uuid(&table.table_uuid, "table_uuid")?;
        let table_uuid_str = table_uuid.to_string();
        span.record("table_uuid", table_uuid_str.as_str());
        let branch_uuid_str = branch_uuid.to_string();
        let catalog_uuid_str = catalog_uuid.to_string();

        // A by-NAME resolve (the SQL server's hot path) already carries the
        // schema in `scope.schema_row`, so this stays zero-roundtrip; a
        // by-UUID resolve doesn't populate it and pays one metadata read, off
        // the SQL hot path. The write-time monotonicity guard keeps a
        // time-travel read's historical policy >= current, so the folded floor
        // never wrongly rejects a valid read.
        let retention_duration_seconds = crate::retention::effective_retention_duration(
            self,
            driver,
            &dl,
            &catalog_uuid_str,
            &schema_uuid.to_string(),
            scope.schema_row.as_ref(),
            &table.retention_config,
        )
        .await?;

        if table.arrow_schema.is_empty() {
            return Err(ApiError::NotFound(format!(
                "table arrow schema not found: {table_uuid_str}"
            )));
        }
        let arrow_schema_bytes = table.arrow_schema;

        let arrow_schema =
            try_schema_from_ipc_buffer(&arrow_schema_bytes).map_err(ApiError::Arrow)?;
        let full_schema: SchemaRef = Arc::new(arrow_schema);

        // Validation runs against the FULL (unprojected) schema + declared
        // primary_keys, so a column projection never changes what a valid
        // ids batch looks like.
        let row_uuids = crate::pk_batch::optional_row_uuids(
            &request.ids,
            &table_uuid,
            &full_schema,
            &table.primary_keys,
        )?;
        // Count only — PK values are PII-gated out of spans, same as `filter`.
        // 0 = unrestricted; the kernel rejects 0-row batches, so 0 is
        // unambiguous.
        span.record("ids_rows", row_uuids.as_ref().map_or(0, Vec::len) as u64);

        // Apply columns pushdown here (once, up-front) so every tier's
        // SQL builder and the cold-tier segment readers all see the
        // projected schema and only materialize the requested columns.
        // `stream_merged` derives its SELECT list from `user_schema.fields()`.
        // Clone first: `full_schema` is retained and handed to `stream_merged` so
        // the snapshot-segment cache decodes the whole (unprojected) segment
        // once and serves every projection from it.
        let schema_ref = apply_projection(full_schema.clone(), request.projection.as_ref())?;

        let snapshot = resolve_query_snapshot(
            driver,
            deadline,
            &catalog_uuid,
            &branch_uuid,
            request.open_tx_uuid.as_deref(),
            request_commit_micros,
            request_commit_seq_num,
            default_frontier,
        )
        .await?;

        // Inclusive `as_of_micros` upper bound the planner uses for
        // cold-segment selection. `ReadSnapshot::plan_as_of_micros`
        // encodes the snapshot-variant translation (OpenTx's strict
        // `< began_at` is mapped to inclusive `began_at - 1`); see
        // its doc-comment in `penca_merge` for the full rule.
        let plan_as_of_micros = snapshot.plan_as_of_micros();
        // On a seq-axis read this lets the planner skip whole cold segments
        // past the seq cutoff (`min_commit_seq_num > N`); `None` for the
        // micros / OpenTx axes, where committed_at selection stands.
        let plan_commit_seq_upper = snapshot.plan_commit_seq_upper();
        // Decode + validate the structured `indexes` seek BEFORE plan(), and
        // only when the caller sent one, so most reads skip the defined-index
        // resolve. An `indexes` naming columns that are not a DEFINED index is
        // rejected here — fail-fast at the boundary.
        let index_seek = if request.indexes.is_empty() {
            None
        } else {
            let defined = self
                .meta_list_indexes(
                    driver,
                    &dl,
                    &catalog_uuid_str,
                    &table_uuid_str,
                    Some(&branch_uuid_str),
                    &snapshot,
                )
                .await?;
            Some(index_select::decode_index_seek(
                &request.indexes,
                &full_schema,
                &defined,
            )?)
        };
        let (index_bindings, index_residual) = match index_seek {
            Some(seek) => (Some(seek.bindings), Some(seek.residual)),
            None => (None, None),
        };
        // The exact-cover signal is the ORIGINAL request filter being empty —
        // the SQL producer pushes no residual when the structured seek fully
        // covers (the SQL-server FilterExec is the Inexact net). MUST be
        // captured BEFORE the index residual folds into the effective filter.
        let exact_selection = request.filter.is_none();
        // The effective merge filter ANDs the request filter with the index
        // seek's residual, so a defined-but-unmaterialized index (no seek entry)
        // is still correctly restricted in the merge fallback; the bypass path
        // ignores this filter (the seek is the exact selection).
        let filter = combine_filters(request.filter.clone(), index_residual.as_deref());
        let snapshot_cache = self.snapshot_cache.clone();
        // The snapshot-list cache is keyed on the resolved snapshot's W_snap,
        // so it is safe for every read; a disabled cache
        // (`SnapshotListCache::disabled`) is the per-service opt-out, not a
        // per-snapshot gate. The Arc is cloned for the `'a` stream.
        let snapshot_list_cache = self.snapshot_list_cache.clone();
        let session_template = self.session_template.clone();
        let stream_batch_size = self.default_stream_batch_size as usize;
        let segment_read_concurrency = self.segment_read_concurrency;
        let snapshot_prune_min_segments = self.snapshot_prune_min_segments;
        let index_seek_max_probe_tuples = self.index_seek_max_probe_tuples;
        let inner: BatchStream<'a> = Box::pin(async_stream::try_stream! {
            // Plan-time atomicity comes from explicit threading inside
            // `QueryManager::plan` — `hot_min` bounds both the snapshot picker
            // and the persist segment fetch, so concurrent Persist+Purge
            // commits between reads can't shift this plan's hot/cold cutoff.
            // No surrounding REPEATABLE READ tx required.
            let (plan, retention_floor) = self.plan(
                driver,
                &catalog_uuid_str,
                &table_uuid_str,
                &branch_uuid_str,
                plan_as_of_micros,
                plan_commit_seq_upper,
                retention_duration_seconds,
                Some(snapshot_list_cache.as_ref()),
            )
            .await?;

            // Below-floor reads are an ERROR, never a clamp: a clamped answer
            // is data at a different instant than the caller asked for.
            // Surfaces as the stream's first item, like other plan errors.
            if let Some(floor) = retention_floor
                && meta_plan::retention_floor_below(
                    floor,
                    plan_as_of_micros,
                    plan_commit_seq_upper,
                )
            {
                Err(ApiError::FailedPrecondition(format!(
                    "as_of precedes retention horizon (floor: commit_seq_num={}, \
                     snapshotted_at_micros={})",
                    floor.commit_seq_num, floor.snapshotted_at_micros
                )))?;
            }

            // Emit an empty schema-header batch first so clients can
            // always recover the user schema via `Table::from_batches`,
            // even when the table is empty and `stream_merged` yields
            // nothing.
            yield RecordBatch::new_empty(schema_ref.clone());

            // Three-way dispatch on plan shape. All-hot: no cold tier at all —
            // skip the merge pipeline and stream the resolved hot SQL
            // directly, eliminating the per-segment exclusion-set machinery
            // and the row_uuid/dedup pass. All-cold: no hot tier — compose
            // only the cold arms via `stream_all_cold`, no hot probes in the
            // flow. Mixed: the full `stream_merged` pipeline. `is_all_hot` is
            // checked FIRST so a truly empty plan (no hot, no cold) stays on
            // `stream_all_hot`'s empty-stream arm.
            //
            // The tier_shape event is the observability seam the
            // snapshot-only / merged acceptance tests scrape.
            tracing::debug!(tier_shape = tier_shape(&plan), "read_data tier dispatch");

            if is_all_hot(&plan) {
                let mut stream = stream_all_hot(
                    driver,
                    &plan,
                    &schema_ref,
                    &snapshot,
                    filter.as_deref(),
                    row_uuids.as_deref(),
                    stream_batch_size,
                    &session_template,
                );
                while let Some(next) = stream.next().await {
                    yield next?;
                }
            } else {
                // Arc-clones only, and deliberately never constructed on the
                // all-hot path above.
                let dl = DatafusionDlDriver::new(
                    readers.clone(),
                    snapshot_cache.clone(),
                    session_template.clone(),
                );
                let mut seeks = IndexSeek::identity_seeks(row_uuids.as_deref());
                // The covering user-index seek is driven by the WIRE `indexes`
                // tuples (decoded + validated pre-plan), matched against the
                // snapshot's MATERIALIZED indexes. Materialized → a seek entry
                // (rides the bypass when it is the single covering seek, else
                // the merge as a scan accelerator the residual re-filters);
                // defined-but-unmaterialized → no entry, served by the
                // residual `filter`. Identity MUST stay FIRST: it is the only
                // entry that restricts the exclusion set.
                if let Some(bindings) = index_bindings.as_ref()
                    && let Some(snapshot_plan) =
                        plan.cold_storage.as_ref().and_then(|cold| cold.snapshot.as_ref())
                    && !snapshot_plan.indexes.is_empty()
                {
                    let user_entries = index_select::select_from_bindings(
                        bindings,
                        snapshot_plan,
                        index_seek_max_probe_tuples,
                    );
                    if !user_entries.is_empty() {
                        // Scraped by
                        // tests/integration/integration_user_index_seek_test.py
                        // — the field names are a test contract.
                        tracing::debug!(
                            index_seek = true,
                            index_seek_entries = user_entries.len(),
                            "user index seek selected"
                        );
                        seeks.get_or_insert_with(Vec::new).extend(user_entries);
                    }
                } else if index_bindings.is_none()
                    && let Some(fragment) = filter.as_deref()
                    && !fragment.is_empty()
                    && let Some(snapshot_plan) =
                        plan.cold_storage.as_ref().and_then(|cold| cold.snapshot.as_ref())
                    && !snapshot_plan.indexes.is_empty()
                {
                    // Filter re-parse path: a gRPC caller sent a `filter` but
                    // no structured `indexes` (e.g. an ids-restricted read
                    // with a covering filter — the SQL path always sends wire
                    // tuples). `select_index_seeks` emits its own index_seek
                    // marker.
                    let user_entries = index_select::select_index_seeks(
                        &dl,
                        fragment,
                        snapshot_plan,
                        &schema_ref,
                        index_seek_max_probe_tuples,
                    )
                    .await;
                    if !user_entries.is_empty() {
                        seeks.get_or_insert_with(Vec::new).extend(user_entries);
                    }
                }
                let mut stream = cold_read::stream_cold_read(
                    driver,
                    &dl,
                    &plan,
                    &schema_ref,
                    &full_schema,
                    &snapshot,
                    filter.as_deref(),
                    seeks,
                    exact_selection,
                    segment_read_concurrency,
                    snapshot_prune_min_segments,
                );
                while let Some(item) = stream.next().await {
                    yield item?;
                }
            }
        });
        Ok(with_query_timeout(deadline, inner))
    }

    /// Audit upsert stream for a table.
    ///
    /// Resolves identifiers → fetches arrow schema → joins upsert_log →
    /// commit_tx_log on hot, then appends a projected scan of the cold persist
    /// upsert segments. Together this produces every committed version
    /// of every row, annotated with transaction metadata, across both
    /// tiers.
    ///
    /// Cold rows carry the four tx metadata columns inline, so
    /// the cold tail is a pure scan — no JOIN. The audit horizon is the
    /// underlying cold persist segments (not the snapshot baseline), so
    /// `QueryManager::read_persist_segments_for_window` is used to fetch every
    /// committed segment regardless of snapshot state.
    ///
    /// **Span lifetime caveat:** `#[instrument]` here brackets the
    /// plan-phase future only — see `read_data`'s doc-comment for the
    /// stream-phase ownership note.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = %plan.catalog_uuid,
            branch_uuid = %plan.branch_uuid,
            table_uuid = %plan.table_uuid,
        ),
    )]
    pub async fn audit_upserts<'a, D, R>(
        &'a self,
        driver: &'a D,
        readers: Arc<HashMap<i32, R>>,
        plan: &AuditPlan,
    ) -> Result<BatchStream<'a>, ApiError>
    where
        D: DbDriver<Row = PgRow> + 'a,
        R: FormatReader + 'static,
    {
        let upsert_table = naming::upsert_log_table(&plan.table_uuid, &plan.branch_uuid);
        let commit_part = naming::commit_tx_log_partition(&plan.catalog_uuid, &plan.branch_uuid);
        let schema_ref = plan.user_schema.clone();
        let hot_from = plan.hot_from;
        let to_micros = plan.hot_to;
        let seq_from = plan.seq_from;
        let seq_to = plan.seq_to;
        let batch_size = self.default_stream_batch_size as usize;

        let include_tx_metadata = plan.include_tx_metadata;
        let audit_schema = audit_upsert_schema(&schema_ref, include_tx_metadata);
        // Cold audit joins MUST run on a session derived from the driver's
        // template (shared function registry + optimizer rules), never a fresh
        // SessionContext::new().
        let cold_session = penca_dl::derive_cold_session(&self.session_template);
        let cold_batches = cold_upsert_audit_batches(
            &cold_session,
            readers.as_ref(),
            &plan.cold_upsert_segments,
            &plan.tx_log_segments,
            &schema_ref,
            &audit_schema,
            include_tx_metadata,
            plan.cold_from,
            plan.cold_to,
            seq_from,
            seq_to,
            plan.row_uuids.as_deref(),
        )
        .await?;
        // The parent branch's inherited upsert history, capped per-row at the
        // fork seq (`base_seq_to`).
        let base_cold_batches = if plan.base_cold_upsert_segments.is_empty() {
            Vec::new()
        } else {
            // Its OWN session, not `cold_session`. `cold_audit_batches`
            // registers its MemTable under the fixed name `d`, so two non-empty
            // arms on one context collide with "The table d already exists" — a
            // hard error, not wrong rows. Unreachable while a fork's own cold arm
            // is always empty; CHA-539's fork copy makes both arms non-empty on
            // every fork. Gating the base arm off for at-or-above-fork reads does
            // NOT cover this: a below-fork as-of read keeps both arms by design.
            let base_cold_session = penca_dl::derive_cold_session(&self.session_template);
            cold_upsert_audit_batches(
                &base_cold_session,
                readers.as_ref(),
                &plan.base_cold_upsert_segments,
                &plan.base_tx_log_segments,
                &schema_ref,
                &audit_schema,
                plan.include_tx_metadata,
                plan.cold_from,
                plan.cold_to,
                seq_from,
                plan.base_seq_to,
                plan.row_uuids.as_deref(),
            )
            .await?
        };
        let deadline = plan.deadline;
        let row_uuids = plan.row_uuids.clone();

        let inner: BatchStream<'a> = Box::pin(async_stream::try_stream! {
            // Emit an empty schema-header batch first so clients can
            // always recover the full audit schema via
            // `Table::from_batches`, even when the stream is empty.
            yield RecordBatch::new_empty(audit_schema.clone());

            // Cold then hot. The post-purge persist watermark guarantees only
            // the *boundary* invariant `max(cold.committed_at) <
            // min(hot.committed_at)`; neither tier is internally sorted by
            // `commit_micros`, so `audit_data` does not promise a sorted stream.
            for batch in cold_batches {
                yield batch;
            }
            // Inherited parent history follows the branch's own cold.
            for batch in base_cold_batches {
                yield batch;
            }

            let hot = HotStorageClient;
            let mut stream = hot.audit_upserts_stream(
                driver,
                &upsert_table,
                &commit_part,
                &schema_ref,
                batch_size,
                AuditRowFilter {
                    from_micros: hot_from,
                    to_micros,
                    from_seq: seq_from,
                    to_seq: seq_to,
                    row_uuids: row_uuids.as_deref(),
                    include_tx_metadata,
                },
            );

            loop {
                let next = std::future::poll_fn(|cx| {
                    Pin::new(&mut stream).poll_next(cx)
                }).await;
                match next {
                    Some(Ok(batch)) => yield batch,
                    Some(Err(e)) => Err(ApiError::HotStorage(e))?,
                    None => break,
                }
            }
        });
        Ok(with_query_timeout(deadline, inner))
    }

    /// Audit delete (tombstone) stream for a table.
    ///
    /// Joins delete_log → commit_tx_log on hot, then appends a projected scan
    /// of the cold persist delete segments. Emits `row_uuid` plus tx
    /// columns; user data columns are absent.
    ///
    /// Cold delete rows carry the four tx metadata columns inline, so the
    /// cold tail is a pure scan over the per-segment audit horizon — no
    /// snapshot filter, see `read_persist_segments_for_window`.
    ///
    /// **Span lifetime caveat:** plan-phase only — see `read_data`'s
    /// doc-comment.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = %plan.catalog_uuid,
            branch_uuid = %plan.branch_uuid,
            table_uuid = %plan.table_uuid,
        ),
    )]
    pub async fn audit_deletes<'a, D, R>(
        &'a self,
        driver: &'a D,
        readers: Arc<HashMap<i32, R>>,
        plan: &AuditPlan,
    ) -> Result<BatchStream<'a>, ApiError>
    where
        D: DbDriver<Row = PgRow> + 'a,
        R: FormatReader + 'static,
    {
        let delete_table = naming::delete_log_table(&plan.table_uuid, &plan.branch_uuid);
        let commit_part = naming::commit_tx_log_partition(&plan.catalog_uuid, &plan.branch_uuid);
        let hot_from = plan.hot_from;
        let to_micros = plan.hot_to;
        let seq_from = plan.seq_from;
        let seq_to = plan.seq_to;
        let batch_size = self.default_stream_batch_size as usize;

        let include_tx_metadata = plan.include_tx_metadata;
        // Derive the cold audit session from the driver's template.
        let cold_session = penca_dl::derive_cold_session(&self.session_template);
        let cold_batches = cold_delete_audit_batches(
            &cold_session,
            readers.as_ref(),
            &plan.cold_delete_segments,
            &plan.tx_log_segments,
            &plan.user_schema,
            &plan.primary_keys,
            include_tx_metadata,
            plan.cold_from,
            plan.cold_to,
            seq_from,
            seq_to,
            plan.row_uuids.as_deref(),
        )
        .await?;
        // The parent branch's inherited delete history, capped per-row at the
        // fork seq (`base_seq_to`).
        let base_cold_batches = if plan.base_cold_delete_segments.is_empty() {
            Vec::new()
        } else {
            // Its own session — same fixed-name collision as the upsert side.
            let base_cold_session = penca_dl::derive_cold_session(&self.session_template);
            cold_delete_audit_batches(
                &base_cold_session,
                readers.as_ref(),
                &plan.base_cold_delete_segments,
                &plan.base_tx_log_segments,
                &plan.user_schema,
                &plan.primary_keys,
                plan.include_tx_metadata,
                plan.cold_from,
                plan.cold_to,
                seq_from,
                plan.base_seq_to,
                plan.row_uuids.as_deref(),
            )
            .await?
        };
        let deadline = plan.deadline;

        let user_schema = plan.user_schema.clone();
        let primary_keys = plan.primary_keys.clone();
        let row_uuids = plan.row_uuids.clone();
        let inner: BatchStream<'a> = Box::pin(async_stream::try_stream! {
            yield RecordBatch::new_empty(
                audit_delete_schema(&user_schema, &primary_keys, include_tx_metadata)
                    .map_err(ApiError::HotStorage)?,
            );

            // Cold then hot. See audit_upserts for the ordering
            // contract: the stream is not promised to be sorted.
            for batch in cold_batches {
                yield batch;
            }
            // Inherited parent delete history follows the branch's own.
            for batch in base_cold_batches {
                yield batch;
            }

            let hot = HotStorageClient;
            let mut stream = hot
                .audit_deletes_stream(
                    driver,
                    &delete_table,
                    &commit_part,
                    &user_schema,
                    &primary_keys,
                    batch_size,
                    AuditRowFilter {
                        from_micros: hot_from,
                        to_micros,
                        from_seq: seq_from,
                        to_seq: seq_to,
                        row_uuids: row_uuids.as_deref(),
                        include_tx_metadata,
                    },
                )
                .map_err(ApiError::HotStorage)?;

            loop {
                let next = std::future::poll_fn(|cx| {
                    Pin::new(&mut stream).poll_next(cx)
                }).await;
                match next {
                    Some(Ok(batch)) => yield batch,
                    Some(Err(e)) => Err(ApiError::HotStorage(e))?,
                    None => break,
                }
            }
        });
        Ok(with_query_timeout(deadline, inner))
    }

    /// Plan a single `audit_data` RPC: resolve identifiers, fetch the
    /// table's Arrow schema, and fetch every cold upsert + delete
    /// persist segment in the requested window.
    ///
    /// The hot/cold cutoff lookup (`hot_min_commit_micros`) is
    /// threaded into the cold segment fetch's upper bound
    /// (`cold_to = min(user_to, hot_min)`), so a concurrent
    /// Persist+Purge cycle committing between the two round-trips
    /// can't drop rows: segments past `hot_min` are excluded by the
    /// bound, regardless of whether the post-Purge state has yet
    /// shifted. Hot reads at execute time key off the same `hot_min`
    /// (`audit_data` has no merge dedup, so the partition must be
    /// strict).
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
            ids_rows = tracing::field::Empty,
        ),
    )]
    pub async fn plan_audit<R>(
        &self,
        pool: &PgDriver,
        readers: Arc<HashMap<i32, R>>,
        request: &AuditDataRequest,
    ) -> Result<AuditPlan, ApiError>
    where
        R: FormatReader + 'static,
    {
        // ADR 0019 §"Four-part mechanism" item 4 — capture `T_q + cap`
        // for the audit RPC. Threaded into both audit_upserts and
        // audit_deletes (via AuditPlan.deadline) so the two stream
        // halves share one bound from the original plan call.
        let deadline = self.deadline_now();
        // The __penca_system__.tables metadata read goes through
        // stream_merged, so it needs a DlDriver for cold-segment access.
        let dl = DatafusionDlDriver::new(
            readers,
            self.snapshot_cache.clone(),
            self.session_template.clone(),
        );
        // AuditData reuses `committed_at.max` as the `as_of_micros` snapshot
        // for name resolution (via its `RequestIdents` impl), so a renamed
        // table resolves at its historical name across the audit window. With
        // no upper bound it falls back to a self-captured `pg_now` — never an
        // unbounded read.
        let scope = ResolvedScope::resolve_table(self, pool, &dl, request, None).await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        // resolve_table already read this table's `__penca_system__.tables`
        // row at `scope.snapshot`, carrying arrow_schema + primary_keys — so
        // reuse it rather than issuing a second identical stream_merged.
        let table = scope
            .table_row
            .ok_or_else(|| ApiError::Internal("resolve_table did not populate table_row".into()))?;
        let table_uuid = parse_resolved_uuid(&table.table_uuid, "table_uuid")?;
        span.record("table_uuid", tracing::field::display(&table_uuid));
        let branch_uuid_str = branch_uuid.to_string();

        let catalog_uuid_str = catalog_uuid.to_string();
        let table_uuid_str = table_uuid.to_string();
        if table.arrow_schema.is_empty() {
            return Err(ApiError::NotFound(format!(
                "table arrow schema not found: {table_uuid}"
            )));
        }
        let primary_keys = table.primary_keys.clone();
        let arrow_schema =
            try_schema_from_ipc_buffer(&table.arrow_schema).map_err(ApiError::Arrow)?;
        let user_schema: SchemaRef = Arc::new(arrow_schema);

        // Decoded once here so both stream halves share one derivation.
        let row_uuids = crate::pk_batch::optional_row_uuids(
            &request.ids,
            &table_uuid,
            &user_schema,
            &primary_keys,
        )?;
        // Count only — PK values are PII-gated; 0 = unrestricted.
        span.record("ids_rows", row_uuids.as_ref().map_or(0, Vec::len) as u64);
        // The micros arm bounds cold-segment SELECTION plus the committed_at
        // per-row/hot filters. The seq arm is applied as a per-row drop on
        // cold plus a hot SQL predicate, with cold-segment selection falling
        // back to the committed_at tier fence (the micros window is None for a
        // seq audit, so cold = everything below hot_min). A seq segment-skip
        // on min/max_commit_seq_num would be an optimization, not correctness.
        let (committed_micros_window, committed_seq_window) =
            crate::scope::audit_committed_axes(&request.committed);
        let (mut user_from, user_to) = timestamp_bounds(committed_micros_window);
        let (mut seq_from, seq_to) = match committed_seq_window {
            Some(r) => (r.min, r.max),
            None => (None, None),
        };
        // The cold segment fetch below is bounded by the same
        // `hot_min = max(persisted_at) + 1` cutoff the hot read keys off at
        // execute time (ADR 0019). Between Persist's commit and Purge's
        // grace-bounded hot delete the same rows live physically in both
        // tiers, and `audit_data` has no merge dedup, so the partition must be
        // strict. Pinning `cold_to = min(user_to, hot_min)` to THIS read's
        // `hot_min` is what stops a concurrent Persist between the two
        // round-trips from shifting the cold segment set — no surrounding
        // REPEATABLE READ tx required.
        //
        // Pre-Persist (hot_min == 0) the cold tier contributes nothing at all,
        // so the segment fetch is short-circuited away entirely.
        //
        // The retention floor rides the same hot_min round trip.
        let schema_uuid = scope.schema_uuid.ok_or_else(|| {
            ApiError::Internal("resolve_table did not populate schema_uuid".into())
        })?;
        let retention_duration_seconds = crate::retention::effective_retention_duration(
            self,
            pool,
            &dl,
            &catalog_uuid_str,
            &schema_uuid.to_string(),
            scope.schema_row.as_ref(),
            &table.retention_config,
        )
        .await?;
        let (hot_min, retention_floor) = await_within_query_timeout(
            deadline,
            LifecycleManager::hot_min_commit_micros(
                pool,
                &catalog_uuid_str,
                &branch_uuid_str,
                &table_uuid_str,
                retention_duration_seconds,
            ),
        )
        .await?;
        // Enforce the floor on the present audit axis, strict `<`. An explicit
        // lower bound below the floor is REJECTED; an unset one means "all
        // retained history" and clamps up to the floor. A fully-unbounded audit
        // defaults to the micros axis.
        if let Some(floor) = retention_floor {
            if committed_seq_window.is_some() {
                match seq_from {
                    Some(from) if from < floor.commit_seq_num => {
                        return Err(ApiError::FailedPrecondition(format!(
                            "audit from precedes retention horizon \
                             (floor commit_seq_num={})",
                            floor.commit_seq_num
                        )));
                    }
                    None => seq_from = Some(floor.commit_seq_num),
                    _ => {}
                }
            } else {
                match user_from {
                    Some(from) if from < floor.snapshotted_at_micros => {
                        return Err(ApiError::FailedPrecondition(format!(
                            "audit from precedes retention horizon \
                             (floor commit_micros={})",
                            floor.snapshotted_at_micros
                        )));
                    }
                    None => user_from = Some(floor.snapshotted_at_micros),
                    _ => {}
                }
            }
        }
        let (cold_upsert_segments, cold_delete_segments) = if hot_min == 0 {
            (Vec::new(), Vec::new())
        } else {
            let cold_to = match user_to {
                Some(t) => Some(t.min(hot_min)),
                None => Some(hot_min),
            };
            await_within_query_timeout(
                deadline,
                self.read_persist_segments_for_window(
                    pool,
                    &catalog_uuid_str,
                    &branch_uuid_str,
                    &table_uuid_str,
                    user_from,
                    cold_to,
                    // Audit selects segments on the committed_at window; the
                    // seq-axis `committed` cursor prunes per row, not per
                    // segment, so no segment seq skip here.
                    None,
                ),
            )
            .await?
        };

        // Cold window `[cold_from, cold_to)` — both `None` pre-Purge.
        let (cold_from, cold_to) = if hot_min == 0 {
            (None, None)
        } else {
            (
                user_from,
                Some(match user_to {
                    Some(t) => t.min(hot_min),
                    None => hot_min,
                }),
            )
        };
        // Hot window `[hot_from, hot_to)`.
        let hot_from = match user_from {
            Some(f) => Some(f.max(hot_min)),
            None if hot_min > 0 => Some(hot_min),
            None => None,
        };
        let hot_to = user_to;

        // The cold audit builders reattach author/comment via a
        // commit_seq_num join against these.
        let include_tx_metadata = request.include_tx_metadata;
        let tx_log_segments = if include_tx_metadata && hot_min != 0 {
            tx_log_persist_segments(
                pool,
                &catalog_uuid_str,
                &branch_uuid_str,
                cold_from,
                cold_to,
                seq_from,
                seq_to,
            )
            .await?
        } else {
            Vec::new()
        };

        // For a forked branch, enumerate the parent's persist segments so
        // `audit_data` surfaces the inherited history. Capped at the fork seq
        // at the segment level here and per-row via `base_seq_to` below, so the
        // child never audits the parent's post-fork rows. Fetched
        // unconditionally, independent of the child's `hot_min == 0` skip — the
        // parent's history lives entirely in its cold tier. Its tx_log is read
        // too so the inherited rows reattach author/comment from the parent's
        // own commit map, not the child's.
        let (
            base_cold_upsert_segments,
            base_cold_delete_segments,
            base_tx_log_segments,
            base_seq_to,
        ) = match self
            .read_branch_lineage(pool, &catalog_uuid_str, &branch_uuid_str)
            .await?
        {
            // CHA-539: the child's OWN persist rows now carry the parent's CDC
            // down to the inherited snapshot's watermark, so enumerating the base
            // arm over that same range would double-report every change row. Cap
            // the base arm at the inherited watermark; below it the parent is
            // still the only source. `None` (parent had no eligible snapshot at
            // the fork) means the child owns the whole inherited history, so the
            // arm is skipped entirely.
            Some((parent_branch_uuid, fork_commit_seq_num, _fork_commit_micros)) => {
                // The child's own arm already returns the inherited CDC down to
                // the adopted baseline's watermark, so the base arm must stop
                // there or every inherited change row is emitted twice —
                // `audit_data` concatenates the two arms and does not dedup, and
                // both clamp to the same ceiling, so the duplicates are exact.
                //
                // `None` means the child inherited from genesis (no eligible
                // parent snapshot at the fork), so it owns the whole inherited
                // history and the arm is skipped entirely.
                let inherited_watermark = self
                    .inherited_baseline_watermark(
                        pool,
                        &catalog_uuid_str,
                        &branch_uuid_str,
                        &table_uuid_str,
                        fork_commit_seq_num,
                    )
                    .await?;
                let base_cold_to = match inherited_watermark {
                    Some(watermark) => {
                        let cap = watermark.saturating_add(1);
                        Some(cold_to.map_or(cap, |to| to.min(cap)))
                    }
                    // Inherited from genesis: the child's own arm covers
                    // everything, so give the base arm an empty window.
                    None => Some(i64::MIN),
                };
                let (base_upserts, base_deletes) = self
                    .read_persist_segments_for_window(
                        pool,
                        &catalog_uuid_str,
                        &parent_branch_uuid,
                        &table_uuid_str,
                        cold_from,
                        base_cold_to,
                        Some(fork_commit_seq_num),
                    )
                    .await?;
                // Exclusive per-row cap: parent rows must be <= fork_seed,
                // tightened by any audit seq-window upper.
                let base_seq_to = Some(base_audit_seq_cap(seq_to, fork_commit_seq_num));
                // Only fetch the parent tx_log when it will actually be joined:
                // the audit builders short-circuit on empty base segments, so a
                // parent with no persisted history in the window needs no read.
                let base_tx_log = if include_tx_metadata
                    && !(base_upserts.is_empty() && base_deletes.is_empty())
                {
                    tx_log_persist_segments(
                        pool,
                        &catalog_uuid_str,
                        &parent_branch_uuid,
                        cold_from,
                        cold_to,
                        seq_from,
                        base_seq_to,
                    )
                    .await?
                } else {
                    Vec::new()
                };
                (base_upserts, base_deletes, base_tx_log, base_seq_to)
            }
            None => (Vec::new(), Vec::new(), Vec::new(), None),
        };

        Ok(AuditPlan {
            catalog_uuid,
            branch_uuid,
            table_uuid,
            user_schema,
            primary_keys,
            cold_from,
            cold_to,
            hot_from,
            hot_to,
            seq_from,
            seq_to,
            cold_upsert_segments,
            cold_delete_segments,
            include_tx_metadata,
            tx_log_segments,
            base_cold_upsert_segments,
            base_cold_delete_segments,
            base_tx_log_segments,
            base_seq_to,
            row_uuids,
            deadline,
        })
    }

    /// Execute a SQL query via DataFusion.
    ///
    /// Not yet implemented — DataFusion integration is deferred.
    pub async fn query(&self) -> Result<(), ApiError> {
        unimplemented!("DataFusion SQL query support is not yet implemented")
    }

    /// Resolve a commit-order fork position — the `ForkPoint` oneof — to a full
    /// [`Watermark`], hot `commit_tx_log` first and the durable cold `tx_log`
    /// second. A `commit_seq_num` is an EXACT lookup (gapless); a `commit_micros`
    /// is AS-OF (the latest commit at or before T, since micros is non-gapless);
    /// an unset position resolves the branch head. `Ok(None)` when the position
    /// names no committed tx on the branch in either tier.
    ///
    /// The hot `commit_tx_log` row may be GC'd by PurgeTxLog while the
    /// position is still durably committed in cold, so a hot miss is not
    /// authoritative — hence the waterfall. This is a read, so it (and its cold
    /// half) live on `QueryManager`; the write path reaches it through its
    /// `query_manager` handle.
    pub async fn resolve_committed_tx<R: FormatReader>(
        &self,
        pool: &PgDriver,
        readers: &HashMap<i32, R>,
        catalog_uuid: &uuid::Uuid,
        branch_uuid: &uuid::Uuid,
        fork_point: Option<&ForkPoint>,
    ) -> Result<Option<Watermark>, ApiError> {
        if let Some(watermark) = self
            .resolve_committed_tx_from_hot(pool, catalog_uuid, branch_uuid, fork_point)
            .await?
        {
            return Ok(Some(watermark));
        }
        // Hot miss: the row may have been purged — fall back to the cold tx_log.
        self.resolve_committed_tx_from_cold(pool, readers, catalog_uuid, branch_uuid, fork_point)
            .await
    }

    /// Hot half of [`Self::resolve_committed_tx`]: resolve a fork position from
    /// the branch's hot `commit_tx_log` partition. `Ok(None)` when no committed
    /// tx matches in hot — which may just mean the row was purged, so the caller
    /// falls back to cold rather than treating the miss as authoritative.
    async fn resolve_committed_tx_from_hot(
        &self,
        pool: &PgDriver,
        catalog_uuid: &uuid::Uuid,
        branch_uuid: &uuid::Uuid,
        fork_point: Option<&ForkPoint>,
    ) -> Result<Option<Watermark>, ApiError> {
        let tx_part = naming::commit_tx_log_partition(catalog_uuid, branch_uuid);
        let tx_q = PgDialect::quote_identifier(&tx_part);
        let (sql, params): (String, Vec<SqlValue>) = match fork_point {
            Some(ForkPoint::CommitSeqNum(seq)) => (
                format!(
                    "SELECT commit_seq_num, commit_micros FROM {tx_q} \
                     WHERE commit_seq_num = $1 LIMIT 1"
                ),
                vec![SqlValue::Int64(*seq)],
            ),
            // commit_seq_num DESC so a micros tie resolves to the latest position.
            Some(ForkPoint::CommitMicros(micros)) => (
                format!(
                    "SELECT commit_seq_num, commit_micros FROM {tx_q} \
                     WHERE commit_micros <= $1 ORDER BY commit_seq_num DESC LIMIT 1"
                ),
                vec![SqlValue::Int64(*micros)],
            ),
            None => (
                format!(
                    "SELECT commit_seq_num, commit_micros FROM {tx_q} \
                     ORDER BY commit_seq_num DESC LIMIT 1"
                ),
                Vec::new(),
            ),
        };

        if let Some(row) = pool.fetch_optional(&sql, &params).await? {
            return Ok(Some(Watermark {
                commit_seq_num: row
                    .try_get::<i64, _>("commit_seq_num")
                    .map_err(|e| ApiError::Metadata(e.into()))?,
                commit_micros: row
                    .try_get::<i64, _>("commit_micros")
                    .map_err(|e| ApiError::Metadata(e.into()))?,
            }));
        }
        Ok(None)
    }

    /// Cold half of [`Self::resolve_committed_tx`]: resolve a fork position from
    /// the durable cold `tx_log` when its hot `commit_tx_log` row has been
    /// purged. Enumerates the branch's committed cold tx_log segments, picks the
    /// single segment that can hold the answer, reads its sorted
    /// `(commit_seq_num, commit_micros, ...)` rows, and seeks: exact match for a
    /// `commit_seq_num` (gapless), latest `commit_micros <= T` for a
    /// `commit_micros`, and the max seq for the head case. `Ok(None)` when the
    /// position names no committed tx in cold.
    async fn resolve_committed_tx_from_cold<R: FormatReader>(
        &self,
        pool: &PgDriver,
        readers: &HashMap<i32, R>,
        catalog_uuid: &uuid::Uuid,
        branch_uuid: &uuid::Uuid,
        fork_point: Option<&ForkPoint>,
    ) -> Result<Option<Watermark>, ApiError> {
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let segments = penca_storage_meta::LifecycleManager::read_committed_tx_log_segments(
            pool,
            &catalog_str,
            &branch_str,
        )
        .await?;

        // Exactly one segment can hold the answer, so read one — never the whole
        // history. Segments are non-overlapping and ascending by
        // min_commit_seq_num (== micros order, co-monotonic).
        let candidate: Option<&penca_storage_meta::TxLogSegment> = match fork_point {
            // seq: the unique covering segment.
            Some(ForkPoint::CommitSeqNum(k)) => segments
                .iter()
                .find(|s| s.min_commit_seq_num <= *k && *k <= s.max_commit_seq_num),
            // as-of micros: the newest segment whose earliest commit is <= T
            // holds the latest commit <= T (any newer segment is wholly > T).
            Some(ForkPoint::CommitMicros(t)) => {
                segments.iter().rev().find(|s| s.min_commit_micros <= *t)
            }
            // head: the newest segment.
            None => segments.last(),
        };
        let Some(segment) = candidate else {
            return Ok(None);
        };

        let format = segment.format.parse::<Format>().map_err(|_| {
            ApiError::Internal(format!(
                "cold tx_log segment has invalid format: {}",
                segment.format
            ))
        })?;
        let persist_segment = PersistSegment {
            segment_uuid: segment.tx_log_segment_uuid.clone(),
            uri: segment.object_uri.clone(),
            format,
            row_count: segment.row_count,
            ..PersistSegment::default()
        };
        let batches: Vec<RecordBatch> = ColdStorageClient::read_persist_segments(
            readers,
            std::slice::from_ref(&persist_segment),
            &penca_storage_meta::tx_log_arrow_schema(),
            None,
        )
        .try_collect()
        .await
        .map_err(ApiError::ColdStorage)?;
        seek_committed_tx_in_batches(&batches, fork_point)
    }
}

/// Scan cold tx_log batches for a fork position: the matching row with the
/// greatest `commit_seq_num` — exact for a seq, latest `commit_micros <= T` for
/// as-of micros, max seq for the head case. Pure over the batches (unit-tested),
/// so [`QueryManager::resolve_committed_tx_from_cold`] can bound the read to the
/// single segment that can hold the answer.
fn seek_committed_tx_in_batches(
    batches: &[RecordBatch],
    fork_point: Option<&ForkPoint>,
) -> Result<Option<Watermark>, ApiError> {
    let mut best: Option<Watermark> = None;
    for batch in batches {
        let seqs: &Int64Array = batch
            .column_by_name("commit_seq_num")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| ApiError::Internal("cold tx_log missing commit_seq_num".into()))?;
        let micros: &Int64Array = batch
            .column_by_name("commit_micros")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| ApiError::Internal("cold tx_log missing commit_micros".into()))?;
        for i in 0..batch.num_rows() {
            let (seq, micro) = (seqs.value(i), micros.value(i));
            let matches = match fork_point {
                Some(ForkPoint::CommitSeqNum(k)) => seq == *k,
                Some(ForkPoint::CommitMicros(t)) => micro <= *t,
                None => true,
            };
            if matches && best.as_ref().is_none_or(|b| seq > b.commit_seq_num) {
                best = Some(Watermark {
                    commit_seq_num: seq,
                    commit_micros: micro,
                });
            }
        }
    }
    Ok(best)
}

/// True when the plan has no cold tier work to do — both the cold log and
/// the snapshot are absent. The merge-on-read pipeline reduces to a single
/// hot-tier query in that case.
fn is_all_hot(plan: &Plan) -> bool {
    // A forked branch's base cold source is folded only in the merge
    // pipeline (stream_merged / stream_all_cold), so a plan carrying one is
    // never all-hot — routing it to stream_all_hot would silently drop the
    // inherited parent data.
    if plan.base_cold_storage.is_some() {
        return false;
    }
    match &plan.cold_storage {
        None => true,
        Some(c) => c.persist.is_none() && c.snapshot.is_none(),
    }
}

/// True when the plan has no hot tier — the read composes only the cold
/// arms via `stream_all_cold`. Checked after `is_all_hot`, so
/// a truly empty plan never reaches this predicate's dispatch arm.
///
/// Staged: `QueryManager::plan` currently emits a hot plan for every
/// read, so this returns false on all planner-produced plans today —
/// see the dispatch-site staging note in `read_data`.
fn is_all_cold(plan: &Plan) -> bool {
    plan.hot_storage.is_none()
}

/// True when the read is served entirely from the immutable snapshot
/// baseline — no hot tier (the existence gate dropped it) AND a cold snapshot
/// with no persist band past it (`Pu <= W_snap`, the minimum-latency fast
/// path). Distinguished from a *cold-only* read (snapshot + a persist band) so
/// [`tier_shape`] can label the dispatched arm.
fn is_snapshot_only(plan: &Plan) -> bool {
    plan.hot_storage.is_none()
        && plan
            .cold_storage
            .as_ref()
            .is_some_and(|cold| cold.snapshot.is_some() && cold.persist.is_none())
}

/// A read may be served by the DataFusion-free snapshot seek when its plan is
/// snapshot-only and its selection is *exact* (`exact_selection`: the seek IS
/// the complete answer — no residual filter to re-apply, ADR 0023/0029). The
/// gate is deliberately **axis-independent**: it admits `LatestSeq`,
/// `AsOfSeq`, `AsOfMicros`, AND `OpenTx` alike.
///
/// Soundness is carried entirely by `is_snapshot_only`, so no axis check is
/// needed:
/// - A snapshot-only plan has NO hot tier. The loose existence gate
///   (`phase_one_fence_and_existence`: a table-scoped `EXISTS(upsert) OR
///   EXISTS(delete)` with no as_of/fence predicate) reports `hot_present = true`
///   whenever *either* hot log holds any row, so `hot_present = false` means both
///   are empty — provably no hot overlay for ANY reader. That includes an open
///   tx's own uncommitted RMW writes (those ARE log rows, so the bare EXISTS
///   subsumes them): a tx that wrote the key keeps `hot_present = true` → not
///   snapshot-only → this gate is false → the seek does not fire, which is
///   what keeps open-tx RYOW correct.
/// - The planner already resolves the snapshot bounded by the read's axis
///   frontier (`hot_min_and_snapshot_pick`: `commit_seq <= began_at_seq_num - 1`
///   for `OpenTx`, `<= as_of` for time-travel), so every row in that snapshot is
///   visible under the read's predicate and the seek over it is exact on every
///   axis — it reads the as_of-appropriate snapshot, not "the latest."
///
/// The single source of truth for the two seek-bypass call sites — `read_data`'s
/// `stream_cold_read` (identity, streaming, drops `row_uuid`) and metadata's
/// `read_system_table` (identity or name, collected, keeps `row_uuid`) — so a
/// change to *when* the bypass is eligible lives in one place. The caller
/// separately confirms it has a resolvable seek entry to run.
fn is_direct_seek_eligible(plan: &Plan, exact_selection: bool) -> bool {
    is_snapshot_only(plan) && exact_selection
}

/// AND-compose an optional base filter with an optional extra fragment, each
/// parenthesized so precedence is preserved. The effective merge filter is the
/// request filter ANDed with a structured `indexes` seek's residual.
fn combine_filters(base: Option<String>, extra: Option<&str>) -> Option<String> {
    match (base, extra) {
        (Some(b), Some(e)) => Some(format!("({b}) AND ({e})")),
        (Some(b), None) => Some(b),
        (None, Some(e)) => Some(e.to_string()),
        (None, None) => None,
    }
}

/// The dispatched tier shape, for the `tier_shape` observability
/// event. `is_all_hot` is checked first (a truly-empty plan rides the all-hot
/// empty-stream arm), matching the dispatch order in `read_data`.
fn tier_shape(plan: &Plan) -> &'static str {
    if is_all_hot(plan) {
        "all_hot"
    } else if is_all_cold(plan) {
        if is_snapshot_only(plan) {
            "snapshot_only"
        } else {
            "cold_only"
        }
    } else {
        "merged"
    }
}

/// All-hot fast path: build the merge-resolved SQL once, project to the
/// user schema, and stream. Bypasses the per-segment dedup and exclusion
/// machinery that `stream_merged` runs unconditionally.
///
/// When `plan.hot_storage` is also `None` (truly empty plan), yields no
/// batches.
// `build_merge_resolved` does not splice the user `WHERE` — DataFusion is the
// single filter engine — and emits a two-arm resolve (visible upserts
// `is_delete = false` UNION winning tombstones `is_delete = true`), so this fn
// drops the tombstone arm in PG (`WHERE NOT m.is_delete`) and applies the user
// predicate as a DataFusion residual per batch, through the same
// `full_plan_predicate` the snapshot tier evaluates inside its scan. Hence the
// cold-session `template` param, used to derive one residual `SessionContext`
// per filtered read. The arg set is irreducible.
#[allow(clippy::too_many_arguments)]
fn stream_all_hot<'a, D>(
    driver: &'a D,
    plan: &Plan,
    schema_ref: &'a SchemaRef,
    snapshot: &ReadSnapshot,
    filter: Option<&str>,
    row_uuids: Option<&[uuid::Uuid]>,
    batch_size: usize,
    session_template: &Arc<SessionState>,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, ApiError>> + Send + 'a>>
where
    D: DbDriver<Row = PgRow>,
{
    let Some(hot_plan) = plan.hot_storage.as_ref() else {
        return Box::pin(futures_util::stream::empty());
    };

    let user_cols: Vec<&str> = schema_ref
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let hot_max = hot_plan.committed_at.as_ref().and_then(|f| f.max_micros);
    // The hot↔cold tier fence is the seq lower `commit_seq_num > W_persist` on
    // `commit_seq.min_seq`. `None` pre-Persist, where hot owns every row.
    let tier_seq_lower = hot_plan.commit_seq.and_then(|c| c.min_seq);
    let snapshot = snapshot.tighten_for_hot(hot_max);

    // Unfiltered resolve — the user predicate is applied by DataFusion
    // (residual below), never spliced here. The resolve carries an `is_delete`
    // flag per row (visible upsert vs winning tombstone).
    let inner_sql = build_merge_resolved::<PgDialect>(
        &hot_plan.upsert_table_name,
        &hot_plan.delete_table_name,
        &hot_plan.commit_tx_log_table_name,
        &user_cols,
        tier_seq_lower,
        &snapshot,
        row_uuids,
    );

    let has_filter = filter.is_some_and(|f| !f.is_empty());

    // 0-col projection (DataFusion planning `SELECT COUNT(*)` is the canonical
    // trigger) — the caller consumes only `num_rows`. Push the aggregation into
    // Postgres, but ONLY when there's no user filter: the predicate is a
    // DataFusion residual that needs the filter's columns, and a 0-col
    // projection has none, so PG cannot count *filtered* rows. Under
    // DataFusion's Inexact filter pushdown a COUNT with a
    // `WHERE` always projects the filter's columns, so a 0-col read never
    // carries a filter — the `!has_filter` guard makes that structural, routing
    // any stray filtered 0-col read to the streaming residual path below (which
    // counts the residual-passing rows). `WHERE NOT m.is_delete` drops the
    // tombstone arm the resolve now emits so deleted rows aren't counted. The
    // downstream `CountAll` aggregate sums `num_rows` across batches, so a single
    // 0-col batch carrying `num_rows = count` (via
    // `RecordBatchOptions::with_row_count`) collapses O(N) IPC payload to one
    // round-trip.
    if user_cols.is_empty() && !has_filter {
        let sql = format!("SELECT COUNT(*) AS n FROM ({inner_sql}) m WHERE NOT m.is_delete");
        let probe_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![Field::new(
            "n",
            DataType::Int64,
            false,
        )]));
        let out_schema = schema_ref.clone();
        return Box::pin(async_stream::try_stream! {
            let batch = execute_query_as_batch(driver, &sql, &[], &probe_schema).await?;
            // PG `COUNT(*)` is a BIGINT, decoded as Int64 by `rows_to_batch`.
            // Always exactly one row; `value(0)` is safe. The Int64
            // downcast is structurally guaranteed by `probe_schema`
            // above, but surface the invariant via a typed error
            // rather than panicking.
            let count_i64 = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    arrow::error::ArrowError::ComputeError(
                        "COUNT(*) probe column did not decode as Int64".into(),
                    )
                })?
                .value(0);
            let count: usize = count_i64.try_into().map_err(|_| {
                arrow::error::ArrowError::ComputeError(format!(
                    "COUNT(*) returned negative value: {count_i64}"
                ))
            })?;
            yield RecordBatch::try_new_with_options(
                out_schema,
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(count)),
            )?;
        });
    }

    // Invariant guard: reaching here with no user columns
    // means `has_filter` (the no-filter 0-col case took the COUNT fast path
    // above). Under DataFusion's Inexact filter pushdown a filtered read always
    // projects the filter's columns, so a 0-col projection never carries a
    // filter — but nothing downstream enforces that. Fail loudly with a typed
    // INTERNAL error rather than building `SELECT  FROM ...` (invalid SQL) or a
    // residual with no columns to bind against.
    if user_cols.is_empty() {
        return Box::pin(futures_util::stream::once(async {
            Err(ApiError::Internal(
                "all-hot read has a residual filter but a 0-column projection: \
                 the filter's columns were not projected (CHA-368 invariant)"
                    .to_string(),
            ))
        }));
    }

    // No user filter: project to user columns, drop the tombstone arm in PG, and
    // the stream is the answer as-is. The internal row_uuid / commit_micros /
    // is_delete that build_merge_resolved exposes are not emitted.
    if !has_filter {
        let projection = user_cols
            .iter()
            .map(|c| PgDialect::quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {projection} FROM ({inner_sql}) m WHERE NOT m.is_delete");
        let base = stream_query_as_batches(driver, sql, vec![], schema_ref.clone(), batch_size);
        return Box::pin(base.map(|item| item.map_err(ApiError::from)));
    }

    // Filtered read: apply the user predicate as a DataFusion residual —
    // the same `full_plan_predicate` the snapshot tier and the merged path use, so
    // every tier filters through one engine. The residual may reference `row_uuid`
    // (identity / RYOW / system-table reads filter by it), so — like the merged
    // path, whose residual runs on the full resolved schema — the batch the
    // predicate binds against MUST carry row_uuid. Project `row_uuid` + user cols
    // out of PG (still dropping the tombstone arm), run the residual, then project
    // row_uuid back out before yielding, since the caller's output schema is user
    // cols only.
    let residual_schema = penca_merge::snapshot_read_schema(schema_ref); // row_uuid + user cols
    let projection = std::iter::once("row_uuid".to_string())
        .chain(user_cols.iter().map(|c| PgDialect::quote_identifier(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {projection} FROM ({inner_sql}) m WHERE NOT m.is_delete");
    let base = stream_query_as_batches(driver, sql, vec![], residual_schema.clone(), batch_size);

    // The predicate is compiled ONCE per read (planning registers a throwaway
    // table `l` on the session; per-batch would re-register `l` and error on the
    // second batch), then evaluated per batch. Session derived once from the
    // shared cold-session template; filter owned for the stream's life.
    // row_uuid is column 0; the user cols follow — project them back out after the
    // residual so the yielded schema matches the caller's user-cols projection.
    let session = penca_dl::derive_cold_session(session_template);
    let filter_owned = filter.map(str::to_string);
    let user_col_indices: Vec<usize> = (1..=user_cols.len()).collect();
    Box::pin(async_stream::try_stream! {
        let residual = penca_merge::ResidualFilter::compile(
            &session,
            filter_owned.as_deref(),
            &residual_schema,
        )
        .await?;
        let mut base = base;
        let mut rows_in: i64 = 0;
        let mut rows_out: i64 = 0;
        while let Some(item) = base.next().await {
            let batch = item?;
            rows_in += batch.num_rows() as i64;
            let filtered = residual.apply(batch)?;
            rows_out += filtered.num_rows() as i64;
            // Drop row_uuid → the user-cols output schema the caller expects.
            yield filtered.project(&user_col_indices)?;
        }
        // Residual selectivity: rows_in = PG's full projected delta, rows_out =
        // survivors. Counts only — the filter fragment stays PII-gated. A
        // stream cancelled mid-flight emits no event.
        tracing::debug!(rows_in, rows_out, "stream_all_hot residual applied");
    })
}

/// Read the cold upsert persist segments (which do NOT carry author/comment
/// inline) and hand them to [`penca_merge::cold_audit_batches`], which filters
/// on the committed_at / commit_seq_num windows + the `ids` restriction,
/// reattaches `author`/`comment` from the cold tx_log via a `commit_seq_num`
/// join when `include_tx_metadata`, and projects to the audit schema.
#[allow(clippy::too_many_arguments)]
async fn cold_upsert_audit_batches<R: FormatReader + 'static>(
    ctx: &SessionContext,
    readers: &HashMap<i32, R>,
    segments: &[PersistSegment],
    tx_log_segments: &[PersistSegment],
    user_schema: &SchemaRef,
    audit_schema: &SchemaRef,
    include_tx_metadata: bool,
    committed_from: Option<i64>,
    committed_to: Option<i64>,
    seq_from: Option<i64>,
    seq_to: Option<i64>,
    row_uuids: Option<&[uuid::Uuid]>,
) -> Result<Vec<RecordBatch>, ApiError> {
    if segments.is_empty() {
        return Ok(Vec::new());
    }
    let cold_upsert_schema = penca_merge::cold_upsert_schema(user_schema);
    let data_batches: Vec<RecordBatch> = ColdStorageClient::read_persist_segments_bounded(
        readers,
        segments,
        &cold_upsert_schema,
        None,
    )
    .try_collect()
    .await
    .map_err(ApiError::ColdStorage)?;
    let (tx_log_batches, tx_log_schema) =
        read_tx_log_batches(readers, tx_log_segments, include_tx_metadata).await?;
    penca_merge::cold_audit_batches(
        ctx,
        data_batches,
        cold_upsert_schema,
        tx_log_batches,
        tx_log_schema,
        audit_schema.clone(),
        include_tx_metadata,
        committed_from,
        committed_to,
        seq_from,
        seq_to,
        row_uuids,
    )
    .await
    .map_err(ApiError::Merge)
}

/// Read the branch's committed cold tx_log segments into batches for the audit
/// join. A no-op (empty) read when the caller didn't request tx metadata.
async fn read_tx_log_batches<R: FormatReader + 'static>(
    readers: &HashMap<i32, R>,
    tx_log_segments: &[PersistSegment],
    include_tx_metadata: bool,
) -> Result<(Vec<RecordBatch>, SchemaRef), ApiError> {
    let schema = penca_storage_meta::tx_log_arrow_schema();
    if !include_tx_metadata || tx_log_segments.is_empty() {
        return Ok((Vec::new(), schema));
    }
    let batches: Vec<RecordBatch> =
        ColdStorageClient::read_persist_segments(readers, tx_log_segments, &schema, None)
            .try_collect()
            .await
            .map_err(ApiError::ColdStorage)?;
    Ok((batches, schema))
}

/// Half-open overlap test for pruning cold tx_log segments to an audit
/// window before reading files — a segment `[min, max]` overlaps `[from, to)`
/// iff `max >= from` and `min < to` on each set axis; an unset bound is no
/// constraint. Pure, so the prune's boundary behavior is unit-tested.
fn tx_log_segment_in_window(
    segment: &penca_storage_meta::TxLogSegment,
    committed_from: Option<i64>,
    committed_to: Option<i64>,
    seq_from: Option<i64>,
    seq_to: Option<i64>,
) -> bool {
    let micros_overlaps = committed_from.is_none_or(|f| segment.max_commit_micros >= f)
        && committed_to.is_none_or(|t| segment.min_commit_micros < t);
    let seq_overlaps = seq_from.is_none_or(|f| segment.max_commit_seq_num >= f)
        && seq_to.is_none_or(|t| segment.min_commit_seq_num < t);
    micros_overlaps && seq_overlaps
}

/// Read the branch's committed cold tx_log segment metadata and shape
/// it as `PersistSegment`s the audit builders can read + join on
/// `commit_seq_num`.
async fn tx_log_persist_segments(
    driver: &PgDriver,
    catalog_uuid_str: &str,
    branch_uuid_str: &str,
    committed_from: Option<i64>,
    committed_to: Option<i64>,
    seq_from: Option<i64>,
    seq_to: Option<i64>,
) -> Result<Vec<PersistSegment>, ApiError> {
    let segments = penca_storage_meta::LifecycleManager::read_committed_tx_log_segments(
        driver,
        catalog_uuid_str,
        branch_uuid_str,
    )
    .await?;
    let mut out = Vec::with_capacity(segments.len());
    for segment in &segments {
        // Prune on the audit window before reading files, so a windowed audit
        // doesn't materialize the whole branch commit history.
        if !tx_log_segment_in_window(segment, committed_from, committed_to, seq_from, seq_to) {
            continue;
        }
        let format = segment.format.parse::<Format>().map_err(|_| {
            ApiError::Internal(format!(
                "cold tx_log segment has invalid format: {}",
                segment.format
            ))
        })?;
        out.push(PersistSegment {
            segment_uuid: segment.tx_log_segment_uuid.clone(),
            uri: segment.object_uri.clone(),
            format,
            row_count: segment.row_count,
            ..PersistSegment::default()
        });
    }
    Ok(out)
}

/// Mirror of [`cold_upsert_audit_batches`] for the delete log. Cold delete row
/// shape is `(row_uuid, <pk_cols>, write_seq_num, commit_micros,
/// began_at_micros, commit_seq_num)` — no author/comment.
/// [`penca_merge::cold_audit_batches`] filters, joins the
/// cold tx_log for author/comment when requested, and projects to the audit
/// schema.
#[allow(clippy::too_many_arguments)]
async fn cold_delete_audit_batches<R: FormatReader + 'static>(
    ctx: &SessionContext,
    readers: &HashMap<i32, R>,
    segments: &[PersistSegment],
    tx_log_segments: &[PersistSegment],
    user_schema: &SchemaRef,
    primary_keys: &[String],
    include_tx_metadata: bool,
    committed_from: Option<i64>,
    committed_to: Option<i64>,
    seq_from: Option<i64>,
    seq_to: Option<i64>,
    row_uuids: Option<&[uuid::Uuid]>,
) -> Result<Vec<RecordBatch>, ApiError> {
    if segments.is_empty() {
        return Ok(Vec::new());
    }
    let audit_schema = audit_delete_schema(user_schema, primary_keys, include_tx_metadata)?;
    let cold_delete_schema = penca_merge::cold_delete_schema(user_schema, primary_keys)?;
    let data_batches: Vec<RecordBatch> = ColdStorageClient::read_persist_segments_bounded(
        readers,
        segments,
        &cold_delete_schema,
        None,
    )
    .try_collect()
    .await
    .map_err(ApiError::ColdStorage)?;
    let (tx_log_batches, tx_log_schema) =
        read_tx_log_batches(readers, tx_log_segments, include_tx_metadata).await?;
    penca_merge::cold_audit_batches(
        ctx,
        data_batches,
        cold_delete_schema,
        tx_log_batches,
        tx_log_schema,
        audit_schema,
        include_tx_metadata,
        committed_from,
        committed_to,
        seq_from,
        seq_to,
        row_uuids,
    )
    .await
    .map_err(ApiError::Merge)
}

/// Exclusive per-row `commit_seq_num` upper bound for a forked
/// branch's parent (base) audit segments. Parent rows must be `<= fork_seed`
/// (exclusive bound `fork_seed + 1`), tightened by any audit seq-window upper
/// so the base never exceeds the audit request's own window.
fn base_audit_seq_cap(audit_seq_to: Option<i64>, fork_commit_seq_num: i64) -> i64 {
    let fork_cap = fork_commit_seq_num.saturating_add(1);
    audit_seq_to.map_or(fork_cap, |upper| fork_cap.min(upper))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_log_seg(
        min_seq: i64,
        max_seq: i64,
        min_micros: i64,
        max_micros: i64,
    ) -> penca_storage_meta::TxLogSegment {
        penca_storage_meta::TxLogSegment {
            tx_log_segment_uuid: String::new(),
            object_uri: String::new(),
            format: "parquet".to_string(),
            row_count: 0,
            min_commit_seq_num: min_seq,
            max_commit_seq_num: max_seq,
            min_commit_micros: min_micros,
            max_commit_micros: max_micros,
        }
    }

    #[test]
    fn tx_log_segment_in_window_boundaries() {
        // Segment covers seq [10, 20], micros [1000, 2000].
        let seg = tx_log_seg(10, 20, 1000, 2000);

        // Unset window: always overlaps.
        assert!(tx_log_segment_in_window(&seg, None, None, None, None));

        // Seq window [from, to): touching `from` (max == from) overlaps;
        // ending exactly at `to` (min == to) does NOT (half-open upper).
        assert!(tx_log_segment_in_window(&seg, None, None, Some(20), None));
        assert!(!tx_log_segment_in_window(&seg, None, None, Some(21), None));
        assert!(tx_log_segment_in_window(&seg, None, None, None, Some(11)));
        assert!(!tx_log_segment_in_window(&seg, None, None, None, Some(10)));

        // Micros window: same half-open semantics.
        assert!(tx_log_segment_in_window(&seg, Some(2000), None, None, None));
        assert!(!tx_log_segment_in_window(
            &seg,
            Some(2001),
            None,
            None,
            None
        ));
        assert!(tx_log_segment_in_window(&seg, None, Some(1001), None, None));
        assert!(!tx_log_segment_in_window(
            &seg,
            None,
            Some(1000),
            None,
            None
        ));

        // Both axes must overlap (AND): a seq-in / micros-out window drops it.
        assert!(!tx_log_segment_in_window(
            &seg,
            Some(2001),
            None,
            Some(15),
            None
        ));
    }

    /// The cold fork-point seek — exact seq, as-of micros (latest
    /// `<= T` by max seq), out-of-range → None, and head (max seq).
    #[test]
    fn seek_committed_tx_in_batches_covers_seq_micros_and_head() {
        use arrow::array::StringArray;
        // Commits: (seq 2, micros 200), (seq 5, 500), (seq 7, 700).
        let batch = RecordBatch::try_new(
            penca_storage_meta::tx_log_arrow_schema(),
            vec![
                Arc::new(Int64Array::from(vec![2_i64, 5, 7])),
                Arc::new(Int64Array::from(vec![200_i64, 500, 700])),
                Arc::new(StringArray::from(vec!["a", "a", "a"])),
                Arc::new(StringArray::from(vec!["c", "c", "c"])),
            ],
        )
        .unwrap();
        let batches = [batch];
        let seek = |fp: Option<ForkPoint>| {
            seek_committed_tx_in_batches(&batches, fp.as_ref())
                .unwrap()
                .map(|w| (w.commit_seq_num, w.commit_micros))
        };

        assert_eq!(seek(Some(ForkPoint::CommitSeqNum(5))), Some((5, 500)));
        assert_eq!(seek(Some(ForkPoint::CommitSeqNum(6))), None);
        // As-of T between seq-5 and seq-7 resolves to the latest committed <= T.
        assert_eq!(seek(Some(ForkPoint::CommitMicros(600))), Some((5, 500)));
        // As-of before the earliest commit resolves to nothing.
        assert_eq!(seek(Some(ForkPoint::CommitMicros(100))), None);
        // Head takes the greatest seq.
        assert_eq!(seek(None), Some((7, 700)));
    }
    use penca_db::driver::SqlValue;

    #[test]
    fn base_audit_seq_cap_bounds_parent_at_fork() {
        // No audit upper: exclusive cap = fork_seed + 1 (parent rows <= fork).
        assert_eq!(base_audit_seq_cap(None, 5), 6);
        // Audit upper tighter than the fork: the audit window wins.
        assert_eq!(base_audit_seq_cap(Some(3), 5), 3);
        // Audit upper looser than the fork: the fork cap wins.
        assert_eq!(base_audit_seq_cap(Some(10), 5), 6);
        // Exact fork boundary: a row at commit_seq_num == fork_seed survives
        // (cap is exclusive at fork_seed + 1); fork_seed + 1 is dropped.
        assert_eq!(base_audit_seq_cap(Some(6), 5), 6);
    }

    #[test]
    fn combine_filters_composes_all_four_shapes() {
        // The effective merge filter ANDs the request filter with the
        // index-seek residual, each parenthesized so precedence is preserved.
        assert_eq!(combine_filters(None, None), None);
        assert_eq!(
            combine_filters(Some("a = 1".into()), None),
            Some("a = 1".into())
        );
        assert_eq!(combine_filters(None, Some("b = 2")), Some("b = 2".into()));
        assert_eq!(
            combine_filters(Some("a = 1".into()), Some("b = 2")),
            Some("(a = 1) AND (b = 2)".into())
        );
    }

    // No-op driver: the no-tx / no-as_of and explicit-as_of resolution
    // paths never touch the database, so every method can return empty.
    struct NoopDriver;

    impl DbDriver for NoopDriver {
        type Row = PgRow;

        async fn execute(&self, _query: &str) -> Result<Vec<PgRow>, sqlx::Error> {
            Ok(vec![])
        }
        async fn execute_no_result(&self, _query: &str) -> Result<(), sqlx::Error> {
            Ok(())
        }
        async fn execute_many(&self, _queries: &[String]) -> Result<(), sqlx::Error> {
            Ok(())
        }
        async fn execute_params(
            &self,
            _query: &str,
            _params: &[SqlValue],
        ) -> Result<Vec<PgRow>, sqlx::Error> {
            Ok(vec![])
        }
        async fn execute_no_result_params(
            &self,
            _query: &str,
            _params: &[SqlValue],
        ) -> Result<(), sqlx::Error> {
            Ok(())
        }
        async fn fetch_optional(
            &self,
            _query: &str,
            _params: &[SqlValue],
        ) -> Result<Option<PgRow>, sqlx::Error> {
            Ok(None)
        }
        async fn close(&self) {}
        fn fetch_stream<'a>(
            &'a self,
            _query: &'a str,
            _params: &'a [SqlValue],
        ) -> Pin<Box<dyn Stream<Item = Result<PgRow, sqlx::Error>> + Send + 'a>> {
            Box::pin(futures_util::stream::empty())
        }
    }

    fn test_deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(60)
    }

    // A read with neither `open_tx_uuid` nor an explicit `as_of` pins the
    // "read latest" snapshot on the SEQ axis, not committed_at micros, so it
    // composes with the seq tier-fence and resolves names on the same axis as
    // data.
    #[tokio::test]
    async fn resolve_query_snapshot_no_tx_no_as_of_pins_snapshot() {
        let catalog = uuid::Uuid::nil();
        let branch = uuid::Uuid::nil();
        let snapshot = resolve_query_snapshot(
            &NoopDriver,
            test_deadline(),
            &catalog,
            &branch,
            /*open_tx_uuid=*/ None,
            /*commit_micros=*/ None,
            /*commit_seq_num=*/ None,
            /*default_frontier=*/ Some(2_468_135),
        )
        .await
        .expect("resolution must not error on the no-tx / no-as_of path");

        // The default "read latest" path pins the SEQ frontier as `LatestSeq`
        // — the cache-eligible marker, not committed_at micros — and a threaded
        // default_frontier pins exactly that value.
        assert_eq!(snapshot, ReadSnapshot::LatestSeq(2_468_135));
    }

    // An explicit `as_of_micros` maps to `AsOfMicros(ts)` verbatim.
    #[tokio::test]
    async fn resolve_query_snapshot_explicit_as_of_unchanged() {
        let catalog = uuid::Uuid::nil();
        let branch = uuid::Uuid::nil();
        let snapshot = resolve_query_snapshot(
            &NoopDriver,
            test_deadline(),
            &catalog,
            &branch,
            /*open_tx_uuid=*/ None,
            /*commit_micros=*/ Some(1_234_567),
            /*commit_seq_num=*/ None,
            /*default_frontier=*/ None,
        )
        .await
        .expect("explicit as_of must resolve");

        assert_eq!(snapshot, ReadSnapshot::AsOfMicros(1_234_567));
    }

    // An explicit `commit_seq_num` maps to `AsOfSeq(n)` verbatim —
    // exact, no resolution, no DB round-trip.
    #[tokio::test]
    async fn resolve_query_snapshot_explicit_commit_seq_num_maps_to_as_of_seq() {
        let catalog = uuid::Uuid::nil();
        let branch = uuid::Uuid::nil();
        let snapshot = resolve_query_snapshot(
            &NoopDriver,
            test_deadline(),
            &catalog,
            &branch,
            /*open_tx_uuid=*/ None,
            /*commit_micros=*/ None,
            /*commit_seq_num=*/ Some(42),
            /*default_frontier=*/ None,
        )
        .await
        .expect("explicit commit_seq_num must resolve");

        assert_eq!(snapshot, ReadSnapshot::AsOfSeq(42));
    }

    /// Pins the three-way dispatch contract over the four plan shapes,
    /// including the precedence property the dispatch comment leans on:
    /// a truly empty plan is all-hot (checked first), never all-cold.
    #[test]
    fn dispatch_predicates_cover_all_plan_shapes() {
        use penca_core::{ColdStoragePlan, HotStoragePlan, PersistPlan, SnapshotPlan};

        let hot = || Some(HotStoragePlan::default());
        let cold = || {
            Some(ColdStoragePlan {
                snapshot: Some(SnapshotPlan::default()),
                persist: Some(PersistPlan::default()),
            })
        };

        // Empty plan: BOTH predicates hold (hot_storage is None), and
        // only read_data checking is_all_hot first keeps it on
        // stream_all_hot's empty-stream arm — the precedence is the
        // contract, not the predicates alone.
        let empty = Plan {
            hot_storage: None,
            cold_storage: None,
            base_cold_storage: None,
        };
        assert!(is_all_hot(&empty));
        assert!(is_all_cold(&empty));

        // Cold storage present but with no work is still all-hot.
        let cold_shell = Plan {
            hot_storage: hot(),
            cold_storage: Some(ColdStoragePlan {
                snapshot: None,
                persist: None,
            }),
            base_cold_storage: None,
        };
        assert!(is_all_hot(&cold_shell));
        assert!(!is_all_cold(&cold_shell));

        // Hot only.
        let all_hot = Plan {
            hot_storage: hot(),
            cold_storage: None,
            base_cold_storage: None,
        };
        assert!(is_all_hot(&all_hot));
        assert!(!is_all_cold(&all_hot));

        // Cold only: the stream_all_cold arm.
        let all_cold = Plan {
            hot_storage: None,
            cold_storage: cold(),
            base_cold_storage: None,
        };
        assert!(!is_all_hot(&all_cold));
        assert!(is_all_cold(&all_cold));

        // Mixed: the stream_merged arm.
        let mixed = Plan {
            hot_storage: hot(),
            cold_storage: cold(),
            base_cold_storage: None,
        };
        assert!(!is_all_hot(&mixed));
        assert!(!is_all_cold(&mixed));

        // A forked branch's base cold source is folded only in the
        // merge pipeline, so a plan carrying one is never all-hot even with no
        // own cold tier — otherwise a fresh forked read would take the
        // fold-free stream_all_hot path and read empty.
        let base = Some(penca_core::BaseColdStorage::default());
        let forked_hot = Plan {
            hot_storage: hot(),
            cold_storage: None,
            base_cold_storage: base.clone(),
        };
        assert!(
            !is_all_hot(&forked_hot),
            "base source forces off the all-hot path"
        );
        // No own hot tier + a base source routes to the all-cold arm (which
        // also folds the base).
        let forked_cold = Plan {
            hot_storage: None,
            cold_storage: None,
            base_cold_storage: base,
        };
        assert!(!is_all_hot(&forked_cold));
        assert!(is_all_cold(&forked_cold));
    }

    /// The direct point-read arm gates on `is_snapshot_only`, true only
    /// for a cold snapshot leg with NO persist band and NO hot tier. A persist
    /// band (a write after the snapshot) or any hot tier structurally excludes
    /// the arm — pinning the gate's exclusions.
    #[test]
    fn is_snapshot_only_requires_snapshot_without_persist_or_hot() {
        use penca_core::{ColdStoragePlan, HotStoragePlan, PersistPlan, SnapshotPlan};

        // Snapshot-only: cold snapshot leg, no persist band, no hot tier.
        let snapshot_only = Plan {
            hot_storage: None,
            cold_storage: Some(ColdStoragePlan {
                snapshot: Some(SnapshotPlan::default()),
                persist: None,
            }),
            base_cold_storage: None,
        };
        assert!(is_snapshot_only(&snapshot_only));
        // Mutually exclusive with is_all_hot — the arm lives in the cold branch.
        assert!(!is_all_hot(&snapshot_only));

        // Persist band present (a write after the snapshot) → not snapshot-only.
        let with_persist_band = Plan {
            hot_storage: None,
            cold_storage: Some(ColdStoragePlan {
                snapshot: Some(SnapshotPlan::default()),
                persist: Some(PersistPlan::default()),
            }),
            base_cold_storage: None,
        };
        assert!(!is_snapshot_only(&with_persist_band));

        // Cold persist only (no snapshot) → not snapshot-only.
        let persist_only = Plan {
            hot_storage: None,
            cold_storage: Some(ColdStoragePlan {
                snapshot: None,
                persist: Some(PersistPlan::default()),
            }),
            base_cold_storage: None,
        };
        assert!(!is_snapshot_only(&persist_only));

        // Snapshot present but a hot tier too → not snapshot-only.
        let snapshot_plus_hot = Plan {
            hot_storage: Some(HotStoragePlan::default()),
            cold_storage: Some(ColdStoragePlan {
                snapshot: Some(SnapshotPlan::default()),
                persist: None,
            }),
            base_cold_storage: None,
        };
        assert!(!is_snapshot_only(&snapshot_plus_hot));
    }
}
