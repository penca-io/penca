// Vendored from datafusion-flight-sql-server v0.4.16.
//
// Changes from upstream:
// - Removed substrait support (not needed for Penca).
// - Removed FlightSqlServiceConfig (schema_with_metadata) — not needed.
// - Replaced `log::info!` with `tracing::info!`.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{
    FlightSqlService as ArrowFlightSqlService, PeekableFlightDataStream,
};
use arrow_flight::sql::{
    self, ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandGetXdbcTypeInfo, CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
    CommandStatementQuery, CommandStatementSubstraitPlan, CommandStatementUpdate,
    DoPutPreparedStatementResult, ProstMessageExt as _, SqlInfo, SqlNullOrdering,
    SqlSupportedCaseSensitivity, SqlSupportedTransaction, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, Ticket,
};
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::arrow::datatypes::Schema;
use datafusion::datasource::TableType;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::{SQLOptions, SessionContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use penca_datafusion::PinnedAsOfSeqGuard;
use penca_db::driver::pg::PgDriver;
use penca_proto::external::v1::GetMaxCommitSeqNumRequest;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use prost::Message;
use prost::bytes::Bytes;
use std::net::SocketAddr;
use tracing_futures::Instrument as _;

use tonic::service::Routes;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

use super::pin::pin_as_of_seq;
use super::state::{CommandTicket, QueryHandle};
use crate::session::{
    ConnSession, ConnSessionFactory, SessionSnapshot, conn_session_from_request,
    snapshot_from_request,
};
use crate::tx;

type Result<T, E = Status> = std::result::Result<T, E>;

/// The Arrow Flight `DoGet` response body: a boxed stream of `FlightData`.
/// Aliased once so the `FlightService::DoGetStream` associated type and the
/// shared [`record_batch_response`] helper name the same (otherwise
/// `clippy::type_complexity`-tripping) type.
type DoGetStreamBody = Pin<Box<dyn Stream<Item = Result<FlightData>> + Send + 'static>>;

/// Flight SQL service backed by DataFusion.
///
/// One instance lives globally for the server's lifetime, carrying the
/// shared channels + the per-conn factory. Per-TCP-connection state
/// (catalog pin, branch pin, open tx, `Arc<SessionContext>`) lives on
/// [`ConnSession`] instances minted by `factory.mint(...)` and threaded
/// into request extensions by [`super::server::PerConnService`] — see ADR 0007.
pub struct FlightSqlService {
    sql_options: Option<SQLOptions>,
    /// gRPC channels to the surrounding microservices. The DML translator
    /// in `do_put_statement_update` derives `row_uuid` / `version_uuid`
    /// client-side and ships INSERT/UPDATE/DELETE through `WriteData`,
    /// using `query_channel` for table metadata (PKs, arrow schema) and
    /// for the strict-INSERT collision check. See ADR 0006.
    query_channel: Channel,
    write_channel: Channel,
    /// Postgres pool used solely for orchestrator-level advisory locks
    /// (per-(branch, table) serialisation of strict-INSERTs). No data
    /// reads or writes flow through this pool — those go through gRPC.
    pool: PgDriver,
    /// Per-TCP-connection session factory (CHA-255). The
    /// [`super::server::PerConnMakeService`] in [`Self::serve`] mints a fresh
    /// [`ConnSession`] from this factory on every accepted TCP
    /// connection. Per-request handlers read the resulting
    /// `Arc<ConnSession>` from request extensions (no global cache,
    /// no UUID indirection).
    factory: Arc<ConnSessionFactory>,
}

impl FlightSqlService {
    /// Creates a new `FlightSqlService`.
    ///
    /// The factory constructed in `main.rs` owns the deployment-level
    /// defaults (catalog / branch / schema) and the template
    /// `SessionState`; this service borrows the factory at serve time.
    pub fn new(
        query_channel: Channel,
        write_channel: Channel,
        pool: PgDriver,
        factory: Arc<ConnSessionFactory>,
    ) -> Self {
        Self {
            sql_options: None,
            query_channel,
            write_channel,
            pool,
            factory,
        }
    }

    /// Set SQL verification options (e.g. restrict DDL/DML).
    pub fn with_sql_options(self, sql_options: SQLOptions) -> Self {
        Self {
            sql_options: Some(sql_options),
            ..self
        }
    }

    /// Serve on the specified address with per-TCP-connection session
    /// scoping (CHA-255). Each accepted TCP conn gets a fresh
    /// [`ConnSession`] minted on its first request. Shuts down
    /// gracefully on SIGINT / SIGTERM via [`super::server::default_shutdown_signal`].
    pub async fn serve(self, addr: String) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let addr: SocketAddr = addr.parse()?;
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let listener = listener.into_std()?;
        self.serve_with_shutdown(listener, super::server::default_shutdown_signal())
            .await
    }

    /// Serve using an existing TCP listener with the default
    /// SIGINT / SIGTERM shutdown signal.
    pub async fn serve_with_listener(
        self,
        listener: std::net::TcpListener,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.serve_with_shutdown(listener, super::server::default_shutdown_signal())
            .await
    }

    /// Serve using an existing TCP listener and a user-supplied shutdown
    /// signal. The signal future is awaited concurrently with each
    /// `accept()`; once it resolves the accept loop exits and the
    /// driver drains in-flight connections.
    ///
    /// Used by both the default deployment path (where the signal is
    /// SIGINT/SIGTERM) and tests / programmatic callers (which can
    /// pass a `tokio::sync::oneshot::Receiver<()>` mapped through
    /// `.map(|_| ())`).
    ///
    /// Drives `hyper::server::conn::http2::Builder::serve_connection`
    /// directly so we can install a per-TCP-connection
    /// [`super::server::PerConnService`] (CHA-255) — tonic 0.14's `Server::serve`
    /// wraps the layered service in `BoxCloneService` that clones per
    /// HTTP/2 stream rather than per accepted TCP conn, so a
    /// `tower::Layer` can't be the per-conn boundary. We pick
    /// `http2::Builder` over `hyper-util`'s `auto::Builder` because
    /// gRPC is HTTP/2-only; the auto-builder's protocol-sniff is
    /// dead work for our use case.
    pub async fn serve_with_shutdown<F>(
        self,
        listener: std::net::TcpListener,
        shutdown: F,
    ) -> std::result::Result<(), Box<dyn std::error::Error>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tracing::info!(addr = %listener.local_addr()?, "flight sql server listening");
        let factory = self.factory.clone();
        let routes = Routes::new(FlightServiceServer::new(self));
        let make = super::server::PerConnMakeService::new(routes, factory);
        let listener = tokio::net::TcpListener::from_std(listener)?;
        super::server::drive_with_hyper_util(listener, make, shutdown).await
    }

    async fn new_context<T>(
        &self,
        request: Request<T>,
    ) -> Result<(Request<T>, FlightSqlSessionContext)> {
        let (metadata, extensions, msg) = request.into_parts();
        let inspect_request = Request::from_parts(metadata, extensions, ());

        let (conn, snapshot) = self.request_session_context(&inspect_request).await?;
        // Per ADR 0010, every conn caches one `Arc<SessionContext>` for
        // its lifetime. We just borrow it here — no per-request
        // SessionState clone, no per-request catalog tree rebuild.
        // CHA-345: BEGIN/COMMIT/ROLLBACK flip the `Arc`-shared
        // `ConnScope.open_tx_cell` (kept in sync with the conn's
        // authoritative `open_tx_uuid` mutex); the provider tree —
        // `PencaTableProvider::scan` plus the schema/catalog metadata
        // reads — resolves the open tx from that cell.
        let ctx = conn.ctx();
        let statement_cache = conn.statement_cache();

        let (metadata, extensions, _) = inspect_request.into_parts();
        Ok((
            Request::from_parts(metadata, extensions, msg),
            FlightSqlSessionContext {
                inner: ctx,
                sql_options: self.sql_options,
                snapshot,
                conn,
                statement_cache,
            },
        ))
    }
}

impl FlightSqlService {
    /// CHA-374 / CHA-460: mint and install the auto-commit read-snapshot pin for
    /// a `GetFlightInfo` leg. Returns the pinned `as_of_seq` (a `commit_seq_num`
    /// frontier) to stamp on the outgoing ticket and the RAII guard to hold
    /// across the plan build (cleared on drop). Skips the `GetMaxCommitSeqNum`
    /// hop entirely when a tx is open — the open tx carries the snapshot via
    /// `open_tx_uuid`. Shared by the statement and prepared-statement
    /// entry-points so both drivers pin identically.
    async fn install_autocommit_pin(
        &self,
        conn: &ConnSession,
        open_tx: Option<&str>,
    ) -> Result<(Option<i64>, Option<PinnedAsOfSeqGuard>)> {
        let seq_frontier = if open_tx.is_none() {
            let resp = QueryServiceClient::new(self.query_channel.clone())
                .get_max_commit_seq_num(GetMaxCommitSeqNumRequest {
                    catalog_uuid: Some(conn.catalog_uuid.clone()),
                    branch_uuid: Some(conn.branch_uuid.clone()),
                })
                .await
                .map_err(|status| {
                    Status::internal(format!(
                        "GetMaxCommitSeqNum (pin frontier) failed: {status}"
                    ))
                })?;
            Some(resp.into_inner().max_commit_seq_num)
        } else {
            None
        };
        let as_of = seq_frontier.and_then(|n| pin_as_of_seq(open_tx, n));
        let guard = as_of.map(|a| conn.install_pinned_as_of_seq(a));
        Ok((as_of, guard))
    }

    /// Standard per-request entry-point setup.
    ///
    /// Every Flight SQL request entry point — `new_context` (queries /
    /// DML / prepared-statement creation), `do_action_begin_transaction`,
    /// `do_action_end_transaction`, and the `SetSessionOptions` /
    /// `GetSessionOptions` arms of `do_action_fallback` — needs to:
    ///
    /// 1. Pull the [`Arc<ConnSession>`] populated by [`super::server::PerConnService`].
    /// 2. Take a fresh [`SessionSnapshot`] at the request boundary so
    ///    a long DML on stream A doesn't observe a sibling stream B's
    ///    mid-flight `SET search_path` / `BEGIN`.
    /// 3. Reject mid-session `x-penca-branch` / `x-penca-catalog`
    ///    header drift against the conn's pinned values (CHA-119
    ///    / CHA-253). Catalog *existence* is verified fail-fast at
    ///    conn-mint by [`ConnSessionFactory::mint`]; this step catches
    ///    the case where a client sends a *changed* header value on
    ///    a later HTTP/2 stream on the same TCP conn — the mint code
    ///    only reads headers on the first request, so the rejection
    ///    has to land here. Mid-session catalog setters via
    ///    `SetSessionOptions(catalog: …)` or `SET catalog` are
    ///    rejected separately by [`crate::set::plan_catalog`].
    async fn request_session_context<T>(
        &self,
        request: &Request<T>,
    ) -> Result<(Arc<ConnSession>, SessionSnapshot), Status> {
        let conn = require_conn_session(request)?;
        let snapshot = snapshot_from_request(request).ok_or_else(|| {
            Status::internal("SessionSnapshot missing — PerConnService not installed?")
        })?;
        super::headers::validate_branch_header(request, &snapshot)?;
        super::headers::validate_catalog_header(request, &snapshot)?;
        Ok((conn, snapshot))
    }
}

static GET_TABLE_TYPES_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

/// Server-capability batch returned by `CommandGetSqlInfo`. Built once at
/// process start; the per-request handlers filter it via the standard
/// `CommandGetSqlInfo::into_builder(&SQL_INFO_DATA)` path. JDBC drivers
/// (Dremio's `flight-sql-jdbc-driver`, used by every JetBrains DB tool +
/// DBeaver) load this batch on connect and read individual keys to
/// populate `DatabaseMetaData`.
static SQL_INFO_DATA: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut b = SqlInfoDataBuilder::new();
    b.append(SqlInfo::FlightSqlServerName, "penca");
    b.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    b.append(SqlInfo::FlightSqlServerArrowVersion, arrow::ARROW_VERSION);
    b.append(SqlInfo::FlightSqlServerReadOnly, false);
    b.append(SqlInfo::FlightSqlServerSql, true);
    b.append(SqlInfo::FlightSqlServerSubstrait, false);
    b.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::Transaction as i32,
    );
    b.append(SqlInfo::SqlDdlCatalog, false);
    b.append(SqlInfo::SqlDdlSchema, false);
    b.append(SqlInfo::SqlDdlTable, false);
    b.append(
        SqlInfo::SqlIdentifierCase,
        SqlSupportedCaseSensitivity::SqlCaseSensitivityCaseInsensitive as i32,
    );
    b.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
    // arrow-flight 57's `SqlSupportedCaseSensitivity` has no
    // "preserves case, matches case-sensitively" variant — our actual
    // quoted-ident behavior (DataFusion default, byte-equal lookup
    // against storage) maps to none of the available enum values.
    // Postgres' Flight SQL proxies hit the same gap. Reporting
    // `Unknown` is honest at the spec's resolution; the Dremio JDBC
    // driver tolerates it and falls back to conservative defaults
    // from `DatabaseMetaData`.
    b.append(
        SqlInfo::SqlQuotedIdentifierCase,
        SqlSupportedCaseSensitivity::SqlCaseSensitivityUnknown as i32,
    );
    b.append(
        SqlInfo::SqlNullOrdering,
        SqlNullOrdering::SqlNullsSortedAtEnd as i32,
    );
    b.append(
        SqlInfo::SqlKeywords,
        datafusion::sql::sqlparser::keywords::ALL_KEYWORDS,
    );
    b.append(SqlInfo::SqlTransactionsSupported, true);
    // `0` per the FlightSql.proto convention = "no limit". The ticket
    // listed this as the single key `SQL_MAX_IDENTIFIER_LENGTH`, but
    // arrow-flight 57's binding breaks it out per identifier kind —
    // each `getMax*NameLength()` method on `DatabaseMetaData` reads a
    // different key. Penca imposes no length cap on any identifier
    // (storage routes by uuid; the gRPC `create_*` APIs accept
    // arbitrary-length byte strings), so all four populate as 0.
    b.append(SqlInfo::SqlMaxColumnNameLength, 0i64);
    b.append(SqlInfo::SqlDbSchemaNameLength, 0i64);
    b.append(SqlInfo::SqlMaxCatalogNameLength, 0i64);
    b.append(SqlInfo::SqlMaxTableNameLength, 0i64);
    b.build().expect("SqlInfoDataBuilder values are well-typed")
});

struct FlightSqlSessionContext {
    /// `Arc<SessionContext>` borrowed from the per-conn cache (ADR
    /// 0010). Cloned/borrowed across requests on the same conn;
    /// concurrency safety provided by DataFusion's internal
    /// `Arc<RwLock<SessionState>>`.
    pub(super) inner: Arc<SessionContext>,
    sql_options: Option<SQLOptions>,
    /// Per-request session snapshot taken in middleware. Threaded down
    /// to `dml::execute` and `set::handle_set` for one consistent view
    /// across the whole DML pipeline.
    snapshot: SessionSnapshot,
    /// The conn the request landed on. Populated by [`new_context`] so
    /// transaction-control entry points (`BEGIN` / `COMMIT` /
    /// `ROLLBACK`) can call `conn.set_open_tx` / `take_open_tx`
    /// directly without a second `Extensions::get::<Arc<ConnSession>>()`
    /// lookup against the request.
    conn: Arc<ConnSession>,
    /// The conn's logical-plan cache (CHA-355). `get_flight_info_statement`
    /// registers the planned statement here and stamps the returned
    /// `statement_uuid` on the ticket; `do_get_fallback` looks it up to reuse
    /// the plan.
    statement_cache: Arc<super::statement_cache::StatementCache>,
}

impl FlightSqlSessionContext {
    async fn sql_to_logical_plan(&self, sql: &str) -> DataFusionResult<LogicalPlan> {
        // CHA-367: collapse the repeated CatalogProvider::schema /
        // SchemaProvider::table gRPCs DataFusion makes within this one
        // `create_logical_plan` to one each. The guard clears the memo when it
        // drops at the end of this call, so the next statement re-resolves
        // live (RYOW / mid-tx DDL). Covers the DoGet cache-miss re-plan path.
        let _memo_guard = self.conn.install_plan_resolution_memo();
        let plan = self.inner.state().create_logical_plan(sql).await?;
        let verifier = self.sql_options.unwrap_or_default();
        verifier.verify_plan(&plan)?;
        Ok(plan)
    }

    async fn execute_logical_plan(
        &self,
        plan: LogicalPlan,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.inner
            .execute_logical_plan(plan)
            .await?
            .execute_stream()
            .await
    }
}

/// Encode an executed `SendableRecordBatchStream` as the Arrow Flight
/// `DoGetStream` response. Shared by every `do_get_fallback` arm
/// (CHA-355 plan-cache hit, plan-cache miss / re-plan, and the
/// prepared-statement path) so the IPC-encode tail lives in one place.
///
/// `advertised_schema` is the schema `get_flight_info` returned to the client
/// (the logical plan's, via `codec::get_schema_for_plan`). CHA-402: when the
/// executed stream's physical schema is more nullable than what was advertised
/// (DataFusion's scalar-subquery decorrelation over-marks a non-null `COUNT`
/// nullable), each batch is relabeled back to the advertised nullability so
/// `DoGet` matches `get_flight_info` — ADBC rejects any divergence. When the two
/// schemas already agree (the hot path), the stream is encoded untouched.
fn record_batch_response(
    stream: SendableRecordBatchStream,
    advertised_schema: SchemaRef,
) -> Result<Response<DoGetStreamBody>> {
    let stream_schema = stream.schema();
    let flight_data_stream =
        match super::codec::reconcile_stream_to_advertised(&stream_schema, &advertised_schema) {
            None => {
                // Hot path: schemas match — encode the stream as-is, no per-batch work.
                let arrow_stream = stream.map(|i| {
                    let batch = i.map_err(|e| FlightError::ExternalError(e.into()))?;
                    Ok(batch)
                });
                encode_flight_stream(stream_schema, arrow_stream)
            }
            Some(target_schema) => {
                // Nullability diverged — relabel each batch to the advertised schema
                // (zero-copy; `try_new` validates a now-non-null column has no nulls).
                // CHA-402: observe when the advertise/stream divergence actually fires
                // (the matching-schema `None` hot path above stays silent).
                tracing::debug!(
                    target: "penca_sql::schema_reconcile",
                    result_fields = target_schema.fields().len(),
                    "reconciled DoGet stream to advertised nullability"
                );
                let batch_schema = Arc::clone(&target_schema);
                let arrow_stream = stream.map(move |i| {
                    let batch = i.map_err(|e| FlightError::ExternalError(e.into()))?;
                    RecordBatch::try_new(Arc::clone(&batch_schema), batch.columns().to_vec())
                        .map_err(|e| FlightError::ExternalError(e.into()))
                });
                encode_flight_stream(target_schema, arrow_stream)
            }
        };
    Ok(Response::new(instrument_flight_encode(flight_data_stream)))
}

/// Encode a `RecordBatch` stream as a boxed `FlightData` response stream —
/// the one place the `FlightDataEncoderBuilder` chain lives for both
/// [`record_batch_response`] arms (schema-match and nullability-reconcile).
fn encode_flight_stream<S>(schema: SchemaRef, stream: S) -> DoGetStreamBody
where
    S: Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static,
{
    FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(stream)
        .map_err(super::error::flight_error_to_status)
        .boxed()
}

/// CHA-417: run a `DoGet` response stream inside one `flight_encode` debug
/// span — the encode tail of every `do_get_fallback` arm (statement,
/// prepared, cache-miss re-plan), since [`record_batch_response`] is the
/// single helper they all return through. Cumulative `items`/`data_bytes`
/// are recorded at end-of-stream (no per-item spans); `data_bytes` is the
/// Arrow payload size (header + body + app_metadata), not the full
/// protobuf `FlightData` wire size. With `PENCA_SPAN_TIMING` set, the
/// span-close `time.busy` is the Flight encode bucket's share of the
/// response — note `busy` also includes polling the upstream
/// plan-execution stream, so read it against the child spans' own
/// timings. An errored or client-cancelled stream still closes the span
/// (with timing) but leaves the count fields unrecorded — counts are
/// stamped only on clean end-of-stream, so a timed close with no counts
/// reads as "aborted", not "zero rows".
fn instrument_flight_encode(stream: DoGetStreamBody) -> DoGetStreamBody {
    let span = tracing::debug_span!(
        "flight_encode",
        items = tracing::field::Empty,
        data_bytes = tracing::field::Empty,
    );
    // Zero cost when off (CHA-417): unlike the ipc_encode/
    // stream_query_as_batches sites, this counting layer is NEW stream
    // plumbing rather than a pre-existing block — so when the debug span
    // is disabled by the level filter, skip the wrapper entirely.
    if span.is_disabled() {
        return stream;
    }

    Box::pin(
        async_stream::stream! {
            let mut items: i64 = 0;
            let mut data_bytes: i64 = 0;
            let mut inner = std::pin::pin!(stream);
            while let Some(item) = inner.next().await {
                if let Ok(flight_data) = &item {
                    items += 1;
                    data_bytes += flight_data.data_header.len() as i64
                        + flight_data.data_body.len() as i64
                        + flight_data.app_metadata.len() as i64;
                }
                yield item;
            }
            tracing::Span::current().record("items", items);
            tracing::Span::current().record("data_bytes", data_bytes);
        }
        .instrument(span),
    )
}

/// Resolve a cached `GetFlightInfo` statement by its `statement_uuid` key,
/// emitting the load-bearing `penca_sql::statement_cache` hit/miss event
/// exactly once. `None` means the caller must re-plan (a miss is always safe).
/// Both `do_get_fallback` arms — statement and prepared — route the
/// cache-outcome decision through here so the event's name and fields live in a
/// single place; each arm owns only its own re-plan fallback.
///
/// A miss carries a `reason` so cache effectiveness is observable:
/// `unstamped` = the ticket carried no `statement_uuid` (old client, or a path
/// that registered no plan); `evicted` = a key was stamped but is absent from
/// this connection's cache — the umbrella for genuine FIFO eviction
/// (capacity too small), a cross-connection ticket replay, and a disabled
/// (`capacity == 0`) cache. Only `evicted` is the "raise the capacity" signal.
fn resolve_statement_cache(
    statement_cache: &super::statement_cache::StatementCache,
    statement_uuid: Option<&str>,
) -> Option<super::statement_cache::StatementCacheEntry> {
    let key = match statement_uuid {
        None => {
            tracing::info!(target: "penca_sql::statement_cache", outcome = "miss", reason = "unstamped");
            return None;
        }
        Some(key) => key,
    };
    match statement_cache.get(key) {
        Some(entry) => {
            tracing::info!(target: "penca_sql::statement_cache", outcome = "hit");
            Some(entry)
        }
        None => {
            tracing::info!(target: "penca_sql::statement_cache", outcome = "miss", reason = "evicted");
            None
        }
    }
}

/// Normalize a wire-level `transaction_id` (raw bytes from
/// `CommandStatementUpdate`) into the `Option<String>` shape the DML
/// path expects. Empty bytes mean "no explicit tx" and surface as
/// `None`; non-UTF-8 surfaces as `INVALID_ARGUMENT`.
fn normalize_wire_transaction_id(
    transaction_id: Option<&prost::bytes::Bytes>,
) -> Result<Option<String>, Status> {
    match transaction_id {
        None => Ok(None),
        Some(bytes) if bytes.is_empty() => Ok(None),
        Some(bytes) => Ok(Some(
            std::str::from_utf8(bytes)
                .map_err(|_| Status::invalid_argument("transaction_id must be UTF-8"))?
                .to_string(),
        )),
    }
}

#[tonic::async_trait]
impl ArrowFlightSqlService for FlightSqlService {
    type FlightService = FlightSqlService;

    #[tracing::instrument(skip_all, err)]
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse>> + Send>>>> {
        Err(Status::unimplemented("handshake is not supported"))
    }

    #[tracing::instrument(skip_all, fields(command_kind = tracing::field::Empty), err)]
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        let (request, ctx) = self.new_context(request).await?;
        let ticket = CommandTicket::try_decode(request.into_inner().ticket)
            .map_err(super::error::flight_error_to_status)?;
        // CHA-374: re-pin the auto-commit snapshot the GetFlightInfo leg minted
        // and stamped on this ticket, for the whole execution. Held across both
        // the cache-hit `execute_logical_plan` and the cache-miss re-plan
        // (`sql_to_logical_plan` re-plan), so physical-planning scans
        // and any re-plan metadata reads resolve at the one pinned snapshot via
        // the `ConnScope` cell. `None` when the ticket carries no pin (in-tx) —
        // the open tx then carries consistency and no guard is installed.
        let _as_of_guard = ticket
            .as_of_seq
            .map(|as_of| ctx.conn.install_pinned_as_of_seq(as_of));
        tracing::Span::current().record(
            "command_kind",
            match &ticket.command {
                sql::Command::CommandStatementQuery(_) => "statement",
                sql::Command::CommandPreparedStatementQuery(_) => "prepared",
                _ => "other",
            },
        );

        match ticket.command {
            sql::Command::CommandStatementQuery(CommandStatementQuery { query, .. }) => {
                // CHA-355: reuse the plan `get_flight_info_statement` cached
                // under `ticket.statement_uuid` instead of re-planning. A miss
                // (absent statement_uuid / evicted / disabled cache / server restart)
                // falls back to re-planning via `sql_to_logical_plan` — always safe, so
                // there is no correctness dependence on a hit.
                // CHA-402: reconcile the DoGet stream to the schema
                // get_flight_info advertised (the logical plan's, via
                // get_schema_for_plan). Derive it from the same plan we execute,
                // before `execute_logical_plan` consumes it.
                let (advertised_schema, stream) = match resolve_statement_cache(
                    &ctx.statement_cache,
                    ticket.statement_uuid.as_deref(),
                ) {
                    Some(entry) => {
                        let advertised_schema = super::codec::get_schema_for_plan(&entry.plan);
                        let stream = ctx
                            .execute_logical_plan(entry.plan)
                            .await
                            .map_err(super::error::df_error_to_status)?;
                        (advertised_schema, stream)
                    }
                    None => {
                        let plan = ctx
                            .sql_to_logical_plan(&query)
                            .await
                            .map_err(super::error::df_error_to_status)?;
                        let advertised_schema = super::codec::get_schema_for_plan(&plan);
                        let stream = ctx
                            .execute_logical_plan(plan)
                            .await
                            .map_err(super::error::df_error_to_status)?;
                        (advertised_schema, stream)
                    }
                };
                record_batch_response(stream, advertised_schema)
            }
            sql::Command::CommandPreparedStatementQuery(CommandPreparedStatementQuery {
                prepared_statement_handle,
            }) => {
                // CHA-355: ADBC (and any prepared-statement client) reaches
                // DoGet here, not the `CommandStatementQuery` arm. Reuse the
                // unparameterized plan `get_flight_info_prepared_statement`
                // cached under `ticket.statement_uuid`; a miss re-plans from the
                // handle's SQL. Parameters bind per-execute on both paths, so
                // the cached plan stays unparameterized and is reusable across
                // executions with different bindings.
                let handle = QueryHandle::try_decode(prepared_statement_handle)?;
                let mut plan = match resolve_statement_cache(
                    &ctx.statement_cache,
                    ticket.statement_uuid.as_deref(),
                ) {
                    Some(entry) => entry.plan,
                    None => ctx
                        .sql_to_logical_plan(handle.query())
                        .await
                        .map_err(super::error::df_error_to_status)?,
                };
                // CHA-402: advertise == the unparameterized plan's logical schema
                // (what get_flight_info_prepared_statement returned). Binding
                // parameters into a filter does not change the projection schema,
                // so capture it before `with_param_values`.
                let advertised_schema = super::codec::get_schema_for_plan(&plan);
                if let Some(param_values) = super::codec::decode_param_values(handle.parameters())
                    .map_err(super::error::arrow_error_to_status)?
                {
                    plan = plan
                        .with_param_values(param_values)
                        .map_err(super::error::df_error_to_status)?;
                }
                let stream = ctx
                    .execute_logical_plan(plan)
                    .await
                    .map_err(super::error::df_error_to_status)?;
                record_batch_response(stream, advertised_schema)
            }
            _ => Err(Status::internal(format!(
                "statement handle not found: {:?}",
                ticket.command
            ))),
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(query_len = query.query.len(), as_of = tracing::field::Empty),
        err
    )]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let (request, ctx) = self.new_context(request).await?;
        let sql = &query.query;

        // CHA-374 / CHA-460: mint + install the auto-commit seq pin before
        // planning so planning's metadata reads and the DoGet scans share one
        // seq snapshot. No-op (and no GetMaxCommitSeqNum hop) when a tx is open.
        let (as_of, _as_of_guard) = self
            .install_autocommit_pin(&ctx.conn, ctx.snapshot.open_tx_uuid.as_deref())
            .await?;
        if let Some(as_of) = as_of {
            tracing::Span::current().record("as_of", as_of);
        }

        // CHA-367: memoize per-build schema/table resolution for this plan.
        let _memo_guard = ctx.conn.install_plan_resolution_memo();
        let (rewritten_sql, plan) = crate::gateway::plan_for_get_flight_info(
            &ctx.inner,
            &ctx.snapshot,
            ctx.sql_options,
            sql,
        )
        .await?;
        // SET-dispatch swaps the wire SQL for SET_PLACEHOLDER_SQL so the
        // DoGet leg returns an empty result rather than re-applying;
        // non-SET arms return `None` so the SELECT hot path stays
        // alloc-free.
        let query = match rewritten_sql {
            Some(rewritten) => CommandStatementQuery {
                query: rewritten,
                ..query
            },
            None => query,
        };

        let flight_descriptor = request.into_inner();
        let dataset_schema = super::codec::get_schema_for_plan(&plan);
        // CHA-355: register the plan we just built and stamp its statement_uuid
        // on the ticket so `do_get_fallback` reuses it instead of re-planning. The
        // schema above is still derived from `plan` directly. For the SET arm
        // `plan` is the placeholder plan and `query` is `SET_PLACEHOLDER_SQL`,
        // so a DoGet hit runs the placeholder (empty result) and a miss
        // re-runs `SET_PLACEHOLDER_SQL` — both empty, SET semantics preserved.
        let statement_uuid = ctx.statement_cache.insert(plan);
        let mut ticket = CommandTicket::new(sql::Command::CommandStatementQuery(query))
            .with_statement_uuid(statement_uuid);
        if let Some(as_of) = as_of {
            ticket = ticket.with_as_of(as_of);
        }
        let ticket = ticket
            .try_encode()
            .map_err(super::error::flight_error_to_status)?;
        let endpoint = FlightEndpoint::new().with_ticket(Ticket { ticket });
        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor)
            .try_with_schema(dataset_schema.as_ref())
            .map_err(super::error::arrow_error_to_status)?;
        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        Err(Status::unimplemented("substrait plans are not supported"))
    }

    #[tracing::instrument(
        skip_all,
        fields(query_len = tracing::field::Empty, as_of = tracing::field::Empty),
        err
    )]
    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let (request, ctx) = self.new_context(request).await?;
        let handle = QueryHandle::try_decode(cmd.prepared_statement_handle.clone())
            .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?;
        tracing::Span::current().record("query_len", handle.query().len());

        // CHA-374 / CHA-460: mint + install the auto-commit seq pin before
        // planning (held across the cache-miss re-plan below). No-op + no
        // GetMaxCommitSeqNum hop in-tx. ADBC reaches the pin here, the same
        // shared helper JDBC uses at the statement leg.
        let (as_of, _as_of_guard) = self
            .install_autocommit_pin(&ctx.conn, ctx.snapshot.open_tx_uuid.as_deref())
            .await?;
        if let Some(as_of) = as_of {
            tracing::Span::current().record("as_of", as_of);
        }

        // CHA-367: reuse the plan `do_action_create_prepared_statement` already
        // built and cached for this statement instead of re-planning — that is
        // the redundant second pass (PREPARE then GetFlightInfo) which re-issued
        // get_schema/get_table for the same statement at the same snapshot. A
        // miss (no stamped uuid, eviction, or a pre-CHA-367 handle) falls back
        // to planning from the handle's SQL under a per-build resolution memo.
        // Reuse the PREPARE-cached plan AND its uuid when present, so the DoGet
        // ticket points at the *same* cache entry — one StatementCache slot per
        // prepared query, preserving CHA-355's one-insert-per-query sizing. A
        // miss (no stamped uuid, eviction, or a pre-CHA-367 handle) re-plans
        // under the per-build resolution memo and inserts a fresh entry.
        let (plan, statement_uuid) = match handle.statement_uuid().and_then(|uuid| {
            ctx.statement_cache
                .get(uuid)
                .map(|entry| (entry.plan, uuid.to_string()))
        }) {
            Some((plan, uuid)) => {
                tracing::debug!(target: "penca_sql::plan_reuse", pass = "get_flight_info_prepared", outcome = "hit");
                (plan, uuid)
            }
            None => {
                tracing::debug!(target: "penca_sql::plan_reuse", pass = "get_flight_info_prepared", outcome = "miss");
                // In the standard flow `do_action_create_prepared_statement`
                // already applied the SET and stashed the placeholder on the
                // handle, so the SET arm of the gateway plan helper rarely
                // fires here — it's defensive cover for client-crafted handles.
                // The rewritten SQL is discarded because the inbound `cmd` is
                // re-used as the response ticket as-is. The memo guard collapses
                // repeated schema/table resolution within this fallback build.
                let _memo_guard = ctx.conn.install_plan_resolution_memo();
                let (_rewritten_sql, plan) = crate::gateway::plan_for_get_flight_info(
                    &ctx.inner,
                    &ctx.snapshot,
                    ctx.sql_options,
                    handle.query(),
                )
                .await?;
                // CHA-355: register the re-planned plan so the DoGet leg reuses
                // it instead of re-planning a third time.
                let uuid = ctx.statement_cache.insert(plan.clone());
                (plan, uuid)
            }
        };

        let flight_descriptor = request.into_inner();
        let dataset_schema = super::codec::get_schema_for_plan(&plan);
        let mut ticket = CommandTicket::new(sql::Command::CommandPreparedStatementQuery(cmd))
            .with_statement_uuid(statement_uuid);
        if let Some(as_of) = as_of {
            ticket = ticket.with_as_of(as_of);
        }
        let ticket = ticket
            .try_encode()
            .map_err(super::error::flight_error_to_status)?;
        let endpoint = FlightEndpoint::new().with_ticket(Ticket { ticket });
        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor)
            .try_with_schema(dataset_schema.as_ref())
            .map_err(super::error::arrow_error_to_status)?;
        Ok(Response::new(flight_info))
    }

    #[tracing::instrument(skip_all, err)]
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let (request, _ctx) = self.new_context(request).await?;
        let flight_descriptor = request.into_inner();
        let ticket_bytes: Bytes = query.as_any().encode_to_vec().into();
        let schema = query.into_builder().schema();
        let flight_info = flight_info_with_self_ticket(&schema, ticket_bytes, flight_descriptor)?;
        Ok(Response::new(flight_info))
    }

    #[tracing::instrument(skip_all, err)]
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let (request, _ctx) = self.new_context(request).await?;
        let flight_descriptor = request.into_inner();
        let ticket_bytes: Bytes = query.as_any().encode_to_vec().into();
        let schema = query.into_builder().schema();
        let flight_info = flight_info_with_self_ticket(&schema, ticket_bytes, flight_descriptor)?;
        Ok(Response::new(flight_info))
    }

    #[tracing::instrument(skip_all, err)]
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let (request, _ctx) = self.new_context(request).await?;
        let flight_descriptor = request.into_inner();
        let ticket_bytes: Bytes = query.as_any().encode_to_vec().into();
        let schema = query.into_builder().schema();
        let flight_info = flight_info_with_self_ticket(&schema, ticket_bytes, flight_descriptor)?;
        Ok(Response::new(flight_info))
    }

    #[tracing::instrument(skip_all, err)]
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let (request, _ctx) = self.new_context(request).await?;
        let flight_descriptor = request.into_inner();
        let ticket_bytes: Bytes = query.as_any().encode_to_vec().into();
        let flight_info =
            flight_info_with_self_ticket(&GET_TABLE_TYPES_SCHEMA, ticket_bytes, flight_descriptor)?;
        Ok(Response::new(flight_info))
    }

    #[tracing::instrument(skip_all, err)]
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        self.request_session_context(&request).await?;
        let flight_descriptor = request.into_inner();
        let ticket_bytes: Bytes = query.as_any().encode_to_vec().into();
        let schema = query.into_builder(&SQL_INFO_DATA).schema();
        let flight_info = flight_info_with_self_ticket(&schema, ticket_bytes, flight_descriptor)?;
        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        Err(Status::unimplemented("get_flight_info_primary_keys"))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        Err(Status::unimplemented("get_flight_info_exported_keys"))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        Err(Status::unimplemented("get_flight_info_imported_keys"))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        Err(Status::unimplemented("get_flight_info_cross_reference"))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        Err(Status::unimplemented("get_flight_info_xdbc_type_info"))
    }

    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        Err(Status::unimplemented("do_get_statement"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        Err(Status::unimplemented("do_get_prepared_statement"))
    }

    #[tracing::instrument(skip_all, err)]
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        let (_request, ctx) = self.new_context(request).await?;
        let catalog_names = ctx.inner.catalog_names();
        let mut builder = query.into_builder();
        for catalog_name in &catalog_names {
            builder.append(catalog_name);
        }
        let schema = builder.schema();
        let batch = builder.build();
        Ok(Response::new(single_batch_stream(schema, batch)))
    }

    #[tracing::instrument(skip_all, err)]
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        let (_request, ctx) = self.new_context(request).await?;
        let catalog_name = query.catalog.clone();
        let mut builder = query.into_builder();
        if let Some(catalog_name) = &catalog_name
            && let Some(catalog) = ctx.inner.catalog(catalog_name)
        {
            for schema_name in &catalog.schema_names() {
                builder.append(catalog_name, schema_name);
            }
        }
        let schema = builder.schema();
        let batch = builder.build();
        Ok(Response::new(single_batch_stream(schema, batch)))
    }

    #[tracing::instrument(skip_all, err)]
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        let (_request, ctx) = self.new_context(request).await?;
        let catalog_name = query.catalog.clone();
        let mut builder = query.into_builder();
        if let Some(catalog_name) = &catalog_name
            && let Some(catalog) = ctx.inner.catalog(catalog_name)
        {
            for schema_name in &catalog.schema_names() {
                if let Some(schema) = catalog.schema(schema_name) {
                    for table_name in &schema.table_names() {
                        if let Some(table) = schema
                            .table(table_name)
                            .await
                            .map_err(super::error::df_error_to_status)?
                        {
                            builder
                                .append(
                                    catalog_name,
                                    schema_name,
                                    table_name,
                                    table.table_type().to_string(),
                                    &table.schema(),
                                )
                                .map_err(super::error::flight_error_to_status)?;
                        }
                    }
                }
            }
        }
        let schema = builder.schema();
        let batch = builder.build();
        Ok(Response::new(single_batch_stream(schema, batch)))
    }

    #[tracing::instrument(skip_all, err)]
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        let (_, _) = self.new_context(request).await?;
        let table_types: ArrayRef = Arc::new(StringArray::from(
            vec![TableType::Base, TableType::View, TableType::Temporary]
                .into_iter()
                .map(|tt| tt.to_string())
                .collect::<Vec<String>>(),
        ));
        let batch = RecordBatch::try_from_iter(vec![("table_type", table_types)]).unwrap();
        Ok(Response::new(single_batch_stream(
            GET_TABLE_TYPES_SCHEMA.clone(),
            Ok(batch),
        )))
    }

    #[tracing::instrument(skip_all, err)]
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        self.request_session_context(&request).await?;
        let builder = query.into_builder(&SQL_INFO_DATA);
        let schema = builder.schema();
        let batch = builder.build();
        Ok(Response::new(single_batch_stream(schema, batch)))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        Err(Status::unimplemented("do_get_primary_keys"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        Err(Status::unimplemented("do_get_exported_keys"))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        Err(Status::unimplemented("do_get_imported_keys"))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        Err(Status::unimplemented("do_get_cross_reference"))
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        Err(Status::unimplemented("do_get_xdbc_type_info"))
    }

    #[tracing::instrument(skip_all, fields(query_len = ticket.query.len()), err)]
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let (_request, ctx) = self.new_context(request).await?;
        let transaction_id = normalize_wire_transaction_id(ticket.transaction_id.as_ref())?;
        // CHA-259: parse + classify + dispatch through one gateway.
        // SET → `set::handle_set`; BEGIN/COMMIT/ROLLBACK → `tx::*`;
        // DML → `dml::execute`; SELECT → invalid_argument;
        // CREATE SCHEMA/CREATE TABLE (auto-commit CHA-172 or
        // transactional CHA-345) → `ddl::execute`; other DDL
        // → "use gRPC WriteService" wording. The same gateway runs in
        // the JDBC-side prepared-statement entry-points below so
        // routing is identical across drivers.
        let gctx = crate::gateway::UpdateCtx {
            session_ctx: &ctx.inner,
            write_channel: &self.write_channel,
            query_channel: &self.query_channel,
            pool: &self.pool,
            snapshot: &ctx.snapshot,
            conn: &ctx.conn,
        };
        crate::gateway::execute_update(&gctx, &ticket.query, transaction_id.as_deref(), None).await
    }

    #[tracing::instrument(skip_all, err)]
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        let (request, _) = self.new_context(request).await?;
        let mut handle = QueryHandle::try_decode(query.prepared_statement_handle)?;
        let mut decoder = FlightDataDecoder::new(
            request
                .into_inner()
                .map_err(super::error::status_to_flight_error),
        );
        let schema = super::codec::decode_schema(&mut decoder).await?;
        let mut parameters = Vec::new();
        let mut encoder = StreamWriter::try_new(&mut parameters, &schema)
            .map_err(super::error::arrow_error_to_status)?;
        let mut total_rows = 0;
        while let Some(msg) = decoder.try_next().await? {
            match msg.payload {
                DecodedPayload::None => {}
                DecodedPayload::Schema(_) => {
                    return Err(Status::invalid_argument(
                        "parameter flight data must contain a single schema",
                    ));
                }
                DecodedPayload::RecordBatch(record_batch) => {
                    total_rows += record_batch.num_rows();
                    encoder
                        .write(&record_batch)
                        .map_err(super::error::arrow_error_to_status)?;
                }
            }
        }
        if total_rows > 1 {
            return Err(Status::invalid_argument(
                "parameters should contain a single row",
            ));
        }
        handle.set_parameters(Some(parameters.into()));
        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(Bytes::from(handle)),
        })
    }

    #[tracing::instrument(
        skip_all,
        fields(
            sql_len = tracing::field::Empty,
            has_params = tracing::field::Empty,
        ),
        err,
    )]
    async fn do_put_prepared_statement_update(
        &self,
        handle: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let (request, ctx) = self.new_context(request).await?;
        // CHA-259: route through the same gateway as
        // `do_put_statement_update`. JDBC's `Statement.execute` walks
        // `ActionCreatePreparedStatement` → `DoPutPreparedStatementUpdate`
        // for every non-query SQL (DDL, DML, BEGIN/COMMIT/ROLLBACK,
        // SET); the prepared-statement handle round-trip carries the
        // SQL string the user typed back to us, and from there the
        // dispatch is identical to the ADBC path.
        //
        // `CommandPreparedStatementUpdate` carries only
        // `prepared_statement_handle` — no wire-level transaction_id
        // field — so the gateway picks up any open tx from the
        // session snapshot via `tx::resolve_tx_uuid_for_dml`.
        let qh = QueryHandle::try_decode(handle.prepared_statement_handle)?;
        tracing::Span::current().record("sql_len", qh.query().len());
        // CHA-333: decode bound parameters from the request's
        // FlightData stream. Per the Apache flight-sql-jdbc-driver,
        // `PreparedStatement.executeUpdate()` packs the parameter
        // VectorSchemaRoot into the DoPut body (see
        // `FlightSqlClient$PreparedStatement.executeUpdate` bytecode);
        // params do NOT travel via the handle for the update path
        // (that's the query path's stash, via
        // `do_put_prepared_statement_query`). A bare
        // `Statement.execute(...)` walks the same wire surface but
        // sends an empty VectorSchemaRoot — `decode_params_from_stream`
        // returns `None` for that shape so existing non-parameterized
        // callers (TestFlightSqlUnsupportedInTxDdlRejectionDriverParity,
        // TestFlightSqlCreateTableAutoCommitEndToEnd, etc.) see
        // unchanged behavior.
        let params = super::codec::decode_params_from_stream(request.into_inner()).await?;
        tracing::Span::current().record("has_params", params.is_some());
        let gctx = crate::gateway::UpdateCtx {
            session_ctx: &ctx.inner,
            write_channel: &self.write_channel,
            query_channel: &self.query_channel,
            pool: &self.pool,
            snapshot: &ctx.snapshot,
            conn: &ctx.conn,
        };
        crate::gateway::execute_update(&gctx, qh.query(), None, params).await
    }

    async fn do_put_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("substrait plans are not supported"))
    }

    #[tracing::instrument(skip_all, fields(query_len = query.query.len()), err)]
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let (_, ctx) = self.new_context(request).await?;
        let sql = query.query;

        // SET applies eagerly at prepare time and the returned
        // `rewritten_sql` is `Some(SET_PLACEHOLDER_SQL)` — stashing the
        // placeholder on the handle means the subsequent
        // `get_flight_info_prepared_statement` + DoGet legs return an
        // empty result instead of re-applying (DataGrip path; see the
        // 2026-05-06 ticket comment). Non-SET paths return `None`; we
        // stash the original SQL on the handle in that case.
        //
        // CHA-259: routing through `gateway::plan_for_create_prepared_statement`
        // short-circuits unsupported DDL (transactional DDL → ADR
        // 0010; non-CREATE-SCHEMA/non-CREATE-TABLE auto-commit DDL →
        // "use gRPC WriteService") before DataFusion's
        // `statement_to_plan` ever runs — that's what closes the JDBC
        // residual gap from CHA-257 where `CREATE TABLE` previously
        // bailed inside `register_table`. Auto-commit `CREATE SCHEMA`
        // / `CREATE TABLE` prepare with an empty dataset_schema so
        // the driver routes to `DoPutPreparedStatementUpdate` and
        // the execute leg dispatches via `gateway::execute_update` →
        // `crate::ddl::execute_*` (CHA-172). The prep helper (vs the
        // read-only `plan_for_get_flight_info`) is permissive about
        // DML / DDL / BEGIN / COMMIT / ROLLBACK because the JDBC
        // driver walks `ActionCreatePreparedStatement` for *every*
        // kind of SQL and decides later whether to call
        // `DoPutPreparedStatementUpdate` or `GetFlightInfo` + `DoGet`.
        // CHA-367: memoize per-build schema/table resolution for the prepare
        // pass (the ADBC path plans here, then again in GetFlightInfo).
        let _memo_guard = ctx.conn.install_plan_resolution_memo();
        let prepared = crate::gateway::plan_for_create_prepared_statement(
            &ctx.inner,
            &ctx.snapshot,
            ctx.sql_options,
            &sql,
        )
        .await?;
        let handle_sql = prepared.rewritten_sql.unwrap_or(sql);

        // dataset_plan and parameter_plan are the same plan for SELECT
        // and SET (no placeholder asymmetry). For DML / TX they differ:
        // dataset_plan is empty (steers driver to update path via the
        // empty `dataset_schema` heuristic) while parameter_plan
        // carries the planned VALUES / SELECT source so the prepare-
        // time `parameter_schema` reflects the user's `?` placeholders.
        // See `gateway::plan_for_create_prepared_statement` (CHA-333).
        let dataset_schema = super::codec::get_schema_for_plan(&prepared.dataset_plan);
        let parameter_schema = super::codec::parameter_schema_for_plan(&prepared.parameter_plan)
            .map_err(|e| e.as_ref().clone())?;
        let dataset_schema = super::codec::encode_schema(dataset_schema.as_ref())
            .map_err(super::error::arrow_error_to_status)?;
        let parameter_schema = super::codec::encode_schema(parameter_schema.as_ref())
            .map_err(super::error::arrow_error_to_status)?;

        // CHA-367: for the Select / Set arms — the only ones that reach
        // GetFlightInfo + DoGet — `dataset_plan` is exactly the plan
        // GetFlightInfo would rebuild. Cache it now and stamp its uuid on the
        // handle so `get_flight_info_prepared_statement` reuses it instead of
        // re-resolving the same identifiers. DML / DDL / tx arms route to DoPut
        // and carry an empty steering `dataset_plan`, so they are not cached.
        let mut handle = QueryHandle::new(handle_sql, None);
        if prepared.dataset_plan_reusable_at_getflightinfo {
            let statement_uuid = ctx.statement_cache.insert(prepared.dataset_plan);
            handle = handle.with_statement_uuid(statement_uuid);
        }
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: Bytes::from(handle),
            dataset_schema,
            parameter_schema,
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        // NOP — stateless
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented("substrait plans are not supported"))
    }

    #[tracing::instrument(skip_all, err)]
    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        let (conn, snapshot) = self.request_session_context(&request).await?;
        let (_catalog_uuid, tx_uuid) =
            tx::handle_begin(&conn, &snapshot, &self.write_channel).await?;
        // Return the real `tx_uuid` so structured Flight SQL clients can
        // (a) thread it through `do_put_statement_update` directly and
        // (b) tell two open transactions apart on the same connection
        // — though re-BEGIN is already rejected by `handle_begin`, so
        // the second case is only a sanity check.
        Ok(ActionBeginTransactionResult {
            transaction_id: Bytes::from(tx_uuid.into_bytes()),
        })
    }

    #[tracing::instrument(
        skip_all,
        fields(action = tracing::field::Empty),
        err,
    )]
    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        use arrow_flight::sql::EndTransaction;

        let (conn, snapshot) = self.request_session_context(&request).await?;
        let action = EndTransaction::try_from(query.action)
            .map_err(|_| Status::invalid_argument("invalid EndTransaction.action"))?;
        match action {
            EndTransaction::Commit => {
                tracing::Span::current().record("action", "commit");
                tx::handle_commit(&conn, &snapshot, &self.write_channel).await
            }
            EndTransaction::Rollback => {
                tracing::Span::current().record("action", "rollback");
                tx::handle_rollback(&conn, &snapshot, &self.write_channel).await
            }
            EndTransaction::Unspecified => Err(Status::invalid_argument(
                "EndTransaction.action must be COMMIT or ROLLBACK",
            )),
        }
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented("do_action_begin_savepoint"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("do_action_end_savepoint"))
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        Err(Status::unimplemented("do_action_cancel_query"))
    }

    /// Intercept the Flight SQL `SetSessionOptions` /
    /// `GetSessionOptions` action types.
    ///
    /// arrow-flight 57.3's dispatch ladder doesn't know about these
    /// types (they post-date the crate version we're pinned to), so
    /// every `SetSessionOptions` / `GetSessionOptions` action arrives
    /// here. We route them through the unified
    /// [`request_session_context`][Self::request_session_context]
    /// helper — symmetric with the other entry points — and the
    /// per-action handlers in [`super::session_options`]; anything
    /// else falls through to the upstream "invalid action" message
    /// the rest of the dispatch surface produces.
    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let action_type = request.get_ref().r#type.clone();
        match action_type.as_str() {
            super::session_options::SET_SESSION_OPTIONS_ACTION_TYPE => {
                let (conn, snapshot) = self.request_session_context(&request).await?;
                let ctx = conn.ctx();
                let body = super::session_options::handle_set_session_options(
                    &snapshot,
                    &ctx,
                    &request.get_ref().body,
                )?;
                let output = futures::stream::iter(vec![Ok(arrow_flight::Result { body })]);
                Ok(Response::new(Box::pin(output)))
            }
            super::session_options::GET_SESSION_OPTIONS_ACTION_TYPE => {
                let (conn, snapshot) = self.request_session_context(&request).await?;
                let ctx = conn.ctx();
                let body = super::session_options::handle_get_session_options(
                    &snapshot,
                    &ctx,
                    &request.get_ref().body,
                )?;
                let output = futures::stream::iter(vec![Ok(arrow_flight::Result { body })]);
                Ok(Response::new(Box::pin(output)))
            }
            other => Err(Status::invalid_argument(format!(
                "do_action: The defined request is invalid: {other:?}"
            ))),
        }
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Encode a single `RecordBatch` (or its Arrow build error) into the
/// flight data stream shape `do_get_*` arms return. Shared by the five
/// metadata `do_get_*` arms (`catalogs`, `schemas`, `tables`,
/// `table_types`, `sql_info`) whose only difference is how they build
/// the batch + schema; the encoder pipeline is identical.
fn single_batch_stream(
    schema: SchemaRef,
    batch: std::result::Result<RecordBatch, FlightError>,
) -> Pin<Box<dyn Stream<Item = Result<FlightData>> + Send>> {
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::once(async move { batch }))
        .map_err(Status::from);
    Box::pin(stream)
}

/// Assemble a [`FlightInfo`] from a Flight-encoded ticket payload, a
/// schema, and a descriptor. Shared by the five metadata
/// `get_flight_info_*` arms (`catalogs`, `schemas`, `tables`,
/// `table_types`, `sql_info`) whose only difference is the schema
/// source — they all return a self-ticket pointing at their own
/// `CommandGet*` payload so the matching `do_get_*` handler can decode
/// it back.
fn flight_info_with_self_ticket(
    schema: &Schema,
    ticket_bytes: Bytes,
    descriptor: FlightDescriptor,
) -> Result<FlightInfo, Status> {
    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: ticket_bytes,
    });
    let flight_info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(super::error::arrow_error_to_status)?
        .with_endpoint(endpoint)
        .with_descriptor(descriptor);
    Ok(flight_info)
}

/// Pull the per-request [`Arc<ConnSession>`] populated by
/// [`super::server::PerConnService`]. Returns `INTERNAL` if missing — the
/// per-conn service is wired in `serve()` / `serve_with_listener()`, so
/// absence means a wiring bug, not a client error.
fn require_conn_session<T>(request: &Request<T>) -> Result<Arc<ConnSession>, Status> {
    conn_session_from_request(request)
        .ok_or_else(|| Status::internal("ConnSession missing — PerConnService not installed?"))
}
