//! Per-TCP-connection session state for penca-sql-server.
//!
//! Per [ADR 0007](../../docs/decisions/0007-session-entity.md), a Penca SQL
//! session is owned by one TCP connection. When the TCP conn closes
//! (cleanly or via network drop), the session is gone — no cross-conn
//! cookie reuse, no idle-eviction sweeper, no `SessionCache` DashMap
//! (all removed in CHA-255). This matches how PostgreSQL scopes
//! databases to a TCP connection: `psql` to a database, run statements,
//! close the connection, state is gone.
//!
//! ## Per-connection state
//!
//! [`ConnSession`] holds everything the conn carries for its lifetime:
//!
//! - `catalog_uuid` / `catalog_name` — frozen at mint from the
//!   `x-penca-catalog` header (CHA-253), falling back to
//!   `SQL_SERVER_DEFAULT_CATALOG`. Routing identity is by `catalog_uuid`;
//!   `catalog_name` is the connection's self-described label, used for
//!   `SET catalog` / `SetSessionOptions(catalog: …)` name comparisons
//!   and for the `validate_catalog_header` per-request check.
//! - `branch_uuid` / `branch_name` — frozen at mint from the
//!   `x-penca-branch` header (CHA-119) and resolved to `branch_uuid`
//!   via [`LifecycleManager::get_branch_by_name`] (CHA-255 — rename-stable
//!   routing). `branch_uuid` flows into every wire payload that
//!   addresses a branch; `branch_name` is the user-facing handle used
//!   for header validation and the `SET branch` rejection text.
//! - `catalog_list: Vec<(String, String)>` — `(name, uuid)` snapshot of
//!   all catalogs in the deployment, taken once at mint via
//!   `QueryServiceClient::list_catalogs`. Powers
//!   [`crate::session::ConnSession::ctx`]'s
//!   `PencaCatalogProviderList::from_snapshot`. New catalogs created
//!   mid-session are not visible to this conn; reconnect to refresh.
//! - `ctx: Arc<SessionContext>` — per-conn DataFusion context with the
//!   per-conn `PencaCatalogProviderList` registered. Concurrency safety
//!   is provided by DataFusion's internal `Arc<RwLock<SessionState>>`.
//! - `open_tx_uuid: Mutex<Option<String>>` — `Some(tx_uuid)` while the
//!   conn has an open Penca transaction (between `BEGIN` and
//!   `COMMIT`/`ROLLBACK`). Lives behind a `tokio::sync::Mutex` because
//!   multiple HTTP/2 streams on the same TCP conn can race against this
//!   field; ADBC drivers serialise statement execution per connection,
//!   so the mutex is rarely contended but cheap insurance.
//! - `default_schema_name` is **not** a field on `ConnSession` — it
//!   lives on `SessionConfig.options.catalog.default_schema` inside
//!   `ctx`. The schema write path (`crate::set::write_default_schema`)
//!   is unchanged from CHA-119.
//!
//! [`ConnSessionFactory`] holds the deployment-level defaults (catalog,
//! branch, schema, channels, template `SessionState`) and the entry
//! point [`ConnSessionFactory::mint`] that runs the once-per-conn
//! initialisation: resolve the catalog header, resolve the branch
//! header via `get_branch_by_name`, enumerate the catalog list, build
//! the per-conn `Arc<SessionContext>`, and return an
//! `Arc<ConnSession>`.
//!
//! [`ConnSessionInit`] is the per-conn lazy holder used by
//! [`crate::flight_sql::service::PerConnService`]. It wraps an
//! `Arc<OnceCell<Arc<ConnSession>>>` so all HTTP/2 streams on the same
//! TCP conn observe the same initialisation result; `Clone` is
//! Arc-share, never fresh-mint.

use std::sync::{Arc, RwLock};

use crate::flight_sql::statement_cache::StatementCache;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::execution::session_state::SessionStateBuilder;
use penca_datafusion::ConnScope;
use penca_datafusion::catalog_list::PencaCatalogProviderList;
use penca_datafusion::{PinnedAsOfSeqGuard, PlanResolutionMemoCell, PlanResolutionMemoGuard};

/// The `Arc`-shared mutable cells `mint` threads from the owning
/// [`ConnSession`] into the per-conn provider tree's [`ConnScope`]: the
/// open-transaction projection (CHA-345) read during planning/scan, and the
/// per-plan-build resolution memo (CHA-367). Bundled so [`ConnSessionFactory::build_ctx`]
/// takes one parameter for both instead of growing an argument per cell.
struct SharedConnCells {
    open_tx_cell: Arc<RwLock<Option<String>>>,
    as_of_seq_cell: Arc<RwLock<Option<i64>>>,
    resolution_memo_cell: PlanResolutionMemoCell,
}
use penca_db::driver::pg::PgDriver;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use penca_proto::external::v1::write_service_client::WriteServiceClient;
use penca_proto::external::v1::{AbortTxRequest, ListCatalogsRequest, PaginationRequest};
use penca_storage_meta::LifecycleManager;
use tokio::sync::{Mutex, OnceCell};
use tonic::transport::Channel;
use tonic::{Request, Status};

/// gRPC metadata header clients use to choose the connection's branch at
/// handshake time (CHA-119). Branch is Penca-specific (no JDBC/ADBC
/// analog). Pinned at conn-mint, immutable for the connection's lifetime,
/// re-validated against [`ConnSession::branch_name`] on every request via
/// [`crate::flight_sql::headers::validate_branch_header`].
///
/// Absent header → server falls back to `SQL_SERVER_DEFAULT_BRANCH`.
pub const BRANCH_HEADER_NAME: &str = "x-penca-branch";

/// gRPC metadata header clients use to choose the connection's catalog
/// at handshake time (CHA-253). Catalog *does* have a JDBC analog
/// (`Connection.setCatalog`) but Penca models a connection as bound
/// to one catalog (Postgres-shaped), so the binding is established at
/// handshake — same shape as [`BRANCH_HEADER_NAME`] — rather than via
/// the post-handshake `SetSessionOptions(catalog: …)` surface.
/// `setCatalog`-as-no-op semantics are preserved by [`crate::set`]: a
/// post-handshake setter to the pinned value is a no-op, to any other
/// value rejects with `FAILED_PRECONDITION` ("catalog is fixed at
/// handshake; reconnect to switch").
///
/// Absent header → server falls back to `SQL_SERVER_DEFAULT_CATALOG`.
pub const CATALOG_HEADER_NAME: &str = "x-penca-catalog";

/// Per-TCP-connection session state. One instance per accepted TCP
/// conn, shared across every HTTP/2 stream on that conn via the
/// `Arc<ConnSession>` stashed in request extensions by
/// [`crate::flight_sql::service::PerConnService`].
///
/// Identity is by uuid (`catalog_uuid`, `branch_uuid`): routing and
/// cross-catalog gates use uuid; name fields are the connection's
/// self-described labels for `SET catalog` / `SET branch` name
/// comparisons and the `validate_*_header` checks.
pub struct ConnSession {
    pub catalog_uuid: String,
    pub catalog_name: String,
    pub branch_uuid: String,
    pub branch_name: String,
    /// Per-conn DataFusion `SessionContext`, built once at mint by the
    /// factory. Borrowed by per-request handlers (`ctx.sql(...)`). The
    /// catalog tree registered on it shares the `open_tx_cell` below, so
    /// no per-request `SessionContext` mutation is needed on
    /// BEGIN/COMMIT/ROLLBACK (CHA-345 — the cell flip is the sole
    /// per-request transaction signal to the provider tree). See ADR 0010.
    pub ctx: Arc<SessionContext>,
    /// `Some(tx_uuid)` while the connection has an open Penca
    /// transaction — the **authoritative** store (the `open_tx_cell`
    /// below is its read-side projection). Behind a `tokio::sync::Mutex`
    /// because multiple HTTP/2 streams on the same TCP conn share the
    /// same `Arc<ConnSession>`; ADBC drivers serialise statement
    /// execution per connection in practice, so contention is rare.
    ///
    /// **Private and accessed only via [`Self::set_open_tx`] /
    /// [`Self::take_open_tx`] / [`Self::snapshot`] / `Drop`.** The
    /// two-place write under this `Mutex` (this field + the `open_tx_cell`
    /// shared with the provider tree) is load-bearing for RYOW + tx-aware
    /// metadata correctness (CHA-345). A caller grabbing
    /// `conn.open_tx_uuid.lock().await` and mutating inside the guard
    /// would bypass the cell write — the encapsulation is the structural
    /// guarantee CHA-255 introduces.
    open_tx_uuid: Mutex<Option<String>>,
    /// CHA-345: the read-side projection of `open_tx_uuid`, `Arc`-shared
    /// with the per-conn provider tree via [`ConnScope::open_tx_cell`].
    /// `PencaSchemaProvider` / `PencaTableProvider` read it during
    /// planning (they can't take the conn's `Mutex`); `set_open_tx` /
    /// `take_open_tx` flip it in the same critical section that flips the
    /// `Mutex` above, so the two never drift. See ADR 0010's CHA-345
    /// addendum.
    open_tx_cell: Arc<RwLock<Option<String>>>,
    /// CHA-374 / CHA-460: per-statement pinned auto-commit snapshot cell (a
    /// `commit_seq_num` frontier), `Arc`-shared with the provider tree via
    /// [`ConnScope::as_of_seq_cell`]. Installed for one statement's
    /// GetFlightInfo plan build / DoGet execute via
    /// [`Self::install_pinned_as_of_seq`] and cleared on guard drop, so a prior
    /// auto-commit statement's pin never leaks into a later one.
    as_of_seq_cell: Arc<RwLock<Option<i64>>>,
    /// Cloned from the factory at mint and used by [`Drop`] to spawn
    /// the `AbortTx` call when the conn closes mid-tx.
    write_channel: Channel,
    /// Per-connection logical-plan cache (CHA-355). `GetFlightInfo` stashes the
    /// statement-query plan here under a server-minted handle; `DoGet` reuses
    /// it instead of re-planning. Shared (by `Arc`) across the conn's HTTP/2
    /// streams; the cache's own `Mutex` serialises concurrent access.
    statement_cache: Arc<StatementCache>,
    /// CHA-367: per-plan-build metadata-resolution memo cell, `Arc`-shared with
    /// the provider tree via [`ConnScope::resolution_memo_cell`]. Installed for
    /// the duration of one plan build via [`Self::install_plan_resolution_memo`]
    /// so repeated `CatalogProvider::schema` / `SchemaProvider::table` calls
    /// within one `create_logical_plan` collapse to one gRPC each. Cleared
    /// between builds (RYOW / mid-tx DDL visibility preserved). The read-side
    /// projection lives on the provider tree, like `open_tx_cell`.
    resolution_memo_cell: PlanResolutionMemoCell,
}

impl std::fmt::Debug for ConnSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnSession")
            .field("catalog_name", &self.catalog_name)
            .field("catalog_uuid", &self.catalog_uuid)
            .field("branch_name", &self.branch_name)
            .field("branch_uuid", &self.branch_uuid)
            .finish_non_exhaustive()
    }
}

impl ConnSession {
    /// Snapshot the per-conn state at a request boundary. Multiple
    /// HTTP/2 streams on the same TCP conn can race against shared
    /// mutable state (`SET search_path`, `BEGIN`); the snapshot pattern
    /// lets each entry-point freeze a view at the start of its request
    /// so a long DML on stream A doesn't observe a sibling stream B's
    /// mid-flight mutation.
    pub async fn snapshot(&self) -> SessionSnapshot {
        let open_tx_uuid = self.open_tx_uuid.lock().await.clone();
        SessionSnapshot {
            catalog_uuid: self.catalog_uuid.clone(),
            catalog_name: self.catalog_name.clone(),
            branch_uuid: self.branch_uuid.clone(),
            branch_name: self.branch_name.clone(),
            open_tx_uuid,
        }
    }

    /// Record an open Penca transaction. Writes both the authoritative
    /// per-conn `open_tx_uuid` field and the `ConnScope` cell read by the
    /// provider tree (`PencaTableProvider::scan` for RYOW data reads,
    /// `PencaSchemaProvider::{table,table_names,table_exist}` for
    /// tx-aware metadata reads — CHA-345). The two-place write under one
    /// lock is load-bearing: skipping the cell write would make a table
    /// created mid-tx invisible to the same tx's reads.
    ///
    /// Returns `Err(Status)` if the conn already has an open tx —
    /// nested transactions are not supported.
    pub async fn set_open_tx(&self, tx_uuid: String) -> Result<(), Status> {
        let mut guard = self.open_tx_uuid.lock().await;
        if guard.is_some() {
            return Err(Status::failed_precondition(
                "session already has an open transaction; nested transactions are not supported",
            ));
        }
        *guard = Some(tx_uuid.clone());
        *self.open_tx_cell.write().unwrap() = Some(tx_uuid);
        Ok(())
    }

    /// Atomically clear the conn's open transaction and return its
    /// `(catalog_uuid, tx_uuid)` together. Returns `None` if no tx is
    /// open (bare COMMIT/ROLLBACK case). Clears the `ConnScope` cell in
    /// the same critical section so the provider tree stops pinning the
    /// tx (CHA-345).
    pub async fn take_open_tx(&self) -> Option<(String, String)> {
        let mut guard = self.open_tx_uuid.lock().await;
        let tx_uuid = guard.take()?;
        *self.open_tx_cell.write().unwrap() = None;
        Some((self.catalog_uuid.clone(), tx_uuid))
    }

    /// Borrow the per-conn `Arc<SessionContext>` for query planning /
    /// execution.
    pub fn ctx(&self) -> Arc<SessionContext> {
        self.ctx.clone()
    }

    /// Borrow the per-conn logical-plan cache (CHA-355). The Flight SQL
    /// `GetFlightInfo` / `DoGet` handlers register and reuse statement-query
    /// plans through it. `pub(crate)` to match `StatementCache`'s visibility — the
    /// cache is an internal Flight SQL detail, not part of the crate's surface.
    pub(crate) fn statement_cache(&self) -> Arc<StatementCache> {
        self.statement_cache.clone()
    }

    /// Install a fresh per-plan-build resolution memo for the duration of one
    /// `create_logical_plan` (CHA-367), returning an RAII guard that clears it
    /// on drop. Bind the guard for the whole plan build; while it is alive the
    /// provider tree memoizes `get_schema`/`get_table` resolutions on the
    /// shared [`ConnScope`] cell, collapsing the repeated lookups DataFusion
    /// makes within one build to one gRPC each. Clearing on drop is what keeps
    /// a resolution from leaking into a later statement (RYOW / mid-tx DDL —
    /// CHA-345). Every Flight SQL planning entry point wraps its build in this
    /// guard; a path that forgets to simply resolves live (no memo installed).
    #[must_use = "the memo is cleared when the guard drops; hold it across the plan build"]
    pub(crate) fn install_plan_resolution_memo(&self) -> PlanResolutionMemoGuard {
        PlanResolutionMemoGuard::install(self.resolution_memo_cell.clone())
    }

    /// Pin the auto-commit read snapshot — a `commit_seq_num` frontier (CHA-374 /
    /// CHA-460) — for the duration of one statement's GetFlightInfo plan build
    /// or DoGet execute. Cleared on guard drop so a prior auto-commit
    /// statement's pin never leaks into a later one. Caller skips this entirely
    /// when a tx is open (the open tx carries the snapshot; the cell stays
    /// `None`).
    #[must_use = "the pin is cleared when the guard drops; hold it across the statement"]
    pub(crate) fn install_pinned_as_of_seq(&self, as_of_seq: i64) -> PinnedAsOfSeqGuard {
        PinnedAsOfSeqGuard::install(self.as_of_seq_cell.clone(), as_of_seq)
    }
}

impl Drop for ConnSession {
    /// On TCP conn close, abort any in-flight Penca transaction.
    ///
    /// `try_lock` is the right shape here because we hold the only
    /// outstanding `Arc<ConnSession>` — every HTTP/2 stream on this
    /// conn has dropped its clone by the time the inner Arc reaches
    /// refcount 0. Best-effort fire-and-forget: if the tokio runtime
    /// is shutting down or the WriteService channel is broken, we
    /// rely on the WriteService TTL backstop.
    fn drop(&mut self) {
        let Some(tx_uuid) = self.open_tx_uuid.try_lock().ok().and_then(|g| g.clone()) else {
            return;
        };
        let catalog_uuid = self.catalog_uuid.clone();
        let branch_uuid = self.branch_uuid.clone();
        let channel = self.write_channel.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let mut client = WriteServiceClient::new(channel);
                    let resp = client
                        .abort_tx(AbortTxRequest {
                            catalog_uuid: Some(catalog_uuid),
                            branch_uuid: Some(branch_uuid),
                            branch_name: None,
                            tx_uuid: tx_uuid.clone(),
                            ..Default::default()
                        })
                        .await;
                    if let Err(e) = resp {
                        tracing::warn!(
                            tx_uuid = %tx_uuid,
                            error = %e,
                            "ConnSession::Drop: AbortTx failed; relying on WriteService TTL"
                        );
                    }
                });
            }
            Err(_) => {
                tracing::warn!(
                    tx_uuid = %tx_uuid,
                    "ConnSession::Drop: no tokio runtime; relying on WriteService TTL"
                );
            }
        }
    }
}

/// Per-request snapshot of a [`ConnSession`]'s state. Built once at the
/// request boundary by the entry-point handler so downstream consumers
/// (DataFusion catalog list, `tx::validate_session_catalog`,
/// `tx::resolve_tx_uuid_for_dml`, `dml::execute`) read from a frozen
/// view rather than re-locking the shared mutable state.
#[derive(Debug, Clone)]
pub struct SessionSnapshot {
    pub catalog_uuid: String,
    pub catalog_name: String,
    pub branch_uuid: String,
    pub branch_name: String,
    pub open_tx_uuid: Option<String>,
}

impl SessionSnapshot {
    /// Test-only constructor.
    #[cfg(test)]
    pub(crate) fn for_test(
        catalog_name: impl Into<String>,
        catalog_uuid: impl Into<String>,
        branch_uuid: impl Into<String>,
        branch_name: impl Into<String>,
        open_tx_uuid: Option<String>,
    ) -> Self {
        Self {
            catalog_name: catalog_name.into(),
            catalog_uuid: catalog_uuid.into(),
            branch_uuid: branch_uuid.into(),
            branch_name: branch_name.into(),
            open_tx_uuid,
        }
    }
}

/// Per-request value of the `x-penca-branch` header (if any).
/// Consumed once at conn-mint; on every subsequent request
/// [`crate::flight_sql::headers::validate_branch_header`] rejects
/// values that disagree with the conn's pinned branch name.
#[derive(Debug, Clone)]
pub struct BranchHeader(pub Option<String>);

/// Per-request value of the `x-penca-catalog` header (if any).
/// Consumed once at conn-mint; on every subsequent request
/// [`crate::flight_sql::headers::validate_catalog_header`] rejects
/// values that disagree with the conn's pinned catalog name.
#[derive(Debug, Clone)]
pub struct CatalogHeader(pub Option<String>);

/// Read the [`Arc<ConnSession>`] populated by [`crate::flight_sql::service::PerConnService`]
/// from a tonic request's extensions. Returns `None` only if the
/// per-conn service didn't run (a wiring bug).
pub fn conn_session_from_request<T>(req: &Request<T>) -> Option<Arc<ConnSession>> {
    req.extensions().get::<Arc<ConnSession>>().cloned()
}

/// Read the [`SessionSnapshot`] populated by [`crate::flight_sql::service::PerConnService`]
/// from a tonic request's extensions.
pub fn snapshot_from_request<T>(req: &Request<T>) -> Option<SessionSnapshot> {
    req.extensions().get::<SessionSnapshot>().cloned()
}

/// Read the [`BranchHeader`] populated by [`crate::flight_sql::service::PerConnService`]
/// from a tonic request's extensions.
pub fn branch_header_from_request<T>(req: &Request<T>) -> Option<BranchHeader> {
    req.extensions().get::<BranchHeader>().cloned()
}

/// Read the [`CatalogHeader`] populated by [`crate::flight_sql::service::PerConnService`]
/// from a tonic request's extensions.
pub fn catalog_header_from_request<T>(req: &Request<T>) -> Option<CatalogHeader> {
    req.extensions().get::<CatalogHeader>().cloned()
}

/// Deployment-level defaults + shared channels used to mint a fresh
/// [`ConnSession`] for each accepted TCP connection. Built once in
/// `main.rs`, cloned into every per-conn `PerConnService`.
/// Deployment-level defaults — **names only**. Per-catalog identifiers
/// (`catalog_uuid` / `branch_uuid`) are *not* cached here:
///
/// - `catalog_uuid` is server-minted per CHA-236 and stable across
///   renames, but caching it at server startup would be wrong if the
///   default catalog itself is renamed / re-created out-of-band. We
///   pay one `LifecycleManager::get_catalog` per conn at mint to
///   resolve afresh.
/// - `branch_uuid` is **catalog-scoped** (CHA-163): the `main` branch
///   in catalog `public` has a different uuid than the `main` branch
///   in catalog `sql_cat_foo`. Caching one uuid at startup would be
///   actively wrong for any conn pinning a non-default catalog via
///   `x-penca-catalog`; the resulting `BeginTx(catalog=foo,
///   branch_uuid=main-in-public)` would hash to a partition that
///   doesn't exist in `foo`. Always re-resolved via
///   `LifecycleManager::get_branch_by_name(pool, conn_catalog_uuid,
///   name)` at mint.
///
/// `main.rs` runs both resolutions once at startup as a **fail-fast
/// bootstrap check** ("does the deployment have the default catalog +
/// `main` branch?") and discards the uuids.
pub struct ConnSessionFactory {
    default_catalog_name: String,
    default_branch_name: String,
    default_schema_name: String,
    query_channel: Channel,
    write_channel: Channel,
    pool: PgDriver,
    template: SessionState,
    /// Capacity for each connection's [`StatementCache`] (CHA-355), sourced once
    /// from `SQL_SERVER_FLIGHT_STATEMENT_CACHE_CAPACITY` at server start.
    flight_statement_cache_capacity: usize,
}

impl ConnSessionFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        default_catalog_name: String,
        default_branch_name: String,
        default_schema_name: String,
        query_channel: Channel,
        write_channel: Channel,
        pool: PgDriver,
        template: SessionState,
        flight_statement_cache_capacity: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            default_catalog_name,
            default_branch_name,
            default_schema_name,
            query_channel,
            write_channel,
            pool,
            template,
            flight_statement_cache_capacity,
        })
    }

    /// Initialise a fresh [`ConnSession`] for an accepted TCP
    /// connection. Resolves the `x-penca-catalog` / `x-penca-branch`
    /// headers to their uuids (fail-fast on miss), enumerates the
    /// catalog list once via `list_catalogs`, builds the per-conn
    /// `Arc<SessionContext>`, and returns the wrapped `Arc<ConnSession>`.
    ///
    /// Catalog/branch fail-fast at mint means the client sees the
    /// error on the first request — no half-baked session lingers
    /// (CHA-253).
    #[tracing::instrument(
        skip_all,
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            catalog_count = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn mint(
        &self,
        catalog_override: Option<String>,
        branch_override: Option<String>,
    ) -> Result<Arc<ConnSession>, Status> {
        let (catalog_name, catalog_uuid) = self.resolve_catalog(catalog_override).await?;
        tracing::Span::current().record("catalog_uuid", catalog_uuid.as_str());
        let (branch_name, branch_uuid) =
            self.resolve_branch(branch_override, &catalog_uuid).await?;
        tracing::Span::current().record("branch_uuid", branch_uuid.as_str());
        let catalog_list = self.fetch_catalog_list().await?;
        tracing::Span::current().record("catalog_count", catalog_list.len());
        // CHA-345: the open-tx cell is created here and shared, by
        // `Arc::clone`, between the `ConnSession` (authoritative flips on
        // BEGIN/COMMIT/ROLLBACK) and the per-conn provider tree built
        // inside `build_ctx` (read-side, via `ConnScope`).
        let open_tx_cell = Arc::new(RwLock::new(None));
        // CHA-367: created here and `Arc`-shared, like `open_tx_cell`, between
        // the `ConnSession` (which installs the per-build memo guard) and the
        // provider tree built inside `build_ctx` (which reads/populates it).
        let resolution_memo_cell = Arc::new(RwLock::new(None));
        // CHA-374 / CHA-460: per-statement pinned-as_of_seq cell, Arc-shared
        // like the others between the ConnSession (installs the pin guard) and
        // the provider tree (reads it via ConnScope::pinned_as_of_seq).
        let as_of_seq_cell = Arc::new(RwLock::new(None));
        let ctx = self.build_ctx(
            &catalog_name,
            &catalog_uuid,
            &branch_uuid,
            &self.default_schema_name,
            catalog_list,
            SharedConnCells {
                open_tx_cell: open_tx_cell.clone(),
                as_of_seq_cell: as_of_seq_cell.clone(),
                resolution_memo_cell: resolution_memo_cell.clone(),
            },
        );
        Ok(Arc::new(ConnSession {
            catalog_uuid,
            catalog_name,
            branch_uuid,
            branch_name,
            ctx,
            open_tx_uuid: Mutex::new(None),
            open_tx_cell,
            as_of_seq_cell,
            write_channel: self.write_channel.clone(),
            statement_cache: Arc::new(StatementCache::new(self.flight_statement_cache_capacity)),
            resolution_memo_cell,
        }))
    }

    async fn resolve_catalog(
        &self,
        catalog_override: Option<String>,
    ) -> Result<(String, String), Status> {
        // Always re-resolve via `get_catalog` against the live
        // `catalog_store` — names are durable but uuids are
        // server-minted per CHA-236 and the deployment may have been
        // re-bootstrapped or renamed since startup.
        let name = catalog_override.unwrap_or_else(|| self.default_catalog_name.clone());
        match LifecycleManager::get_catalog(&self.pool, None, Some(&name)).await {
            Ok(Some(catalog)) => Ok((name, catalog.catalog_uuid)),
            Ok(None) => Err(missing_catalog_status(&name)),
            Err(err) => Err(Status::internal(format!(
                "LifecycleManager::get_catalog failed while resolving \
                 connection-pinned catalog `{name}` at mint: {err}"
            ))),
        }
    }

    async fn resolve_branch(
        &self,
        branch_override: Option<String>,
        catalog_uuid: &str,
    ) -> Result<(String, String), Status> {
        // Always resolve via `get_branch_by_name` against the conn's
        // pinned `catalog_uuid`. Branches are catalog-scoped (CHA-163),
        // so `(catalog_uuid="public", branch_name="main")` and
        // `(catalog_uuid="sql_cat_X", branch_name="main")` resolve to
        // distinct `branch_uuid`s. See the `ConnSessionFactory` struct
        // doc for why this isn't pre-cached at startup.
        let name = branch_override.unwrap_or_else(|| self.default_branch_name.clone());
        match LifecycleManager::get_branch_by_name(&self.pool, catalog_uuid, &name).await {
            Ok(Some(branch)) => Ok((name, branch.branch_uuid)),
            Ok(None) => Err(missing_branch_status(&name)),
            Err(err) => Err(Status::internal(format!(
                "LifecycleManager::get_branch_by_name failed while resolving \
                 connection-pinned branch `{name}` at mint: {err}"
            ))),
        }
    }

    /// One batched `list_catalogs` paginated to exhaustion. The
    /// resulting `(name, uuid)` vec is frozen for the conn's lifetime
    /// inside [`PencaCatalogProviderList`].
    async fn fetch_catalog_list(&self) -> Result<Vec<(String, String)>, Status> {
        let mut client = QueryServiceClient::new(self.query_channel.clone());
        let mut all = Vec::new();
        let mut page_token = String::new();
        loop {
            let resp = client
                .list_catalogs(ListCatalogsRequest {
                    pagination: Some(PaginationRequest {
                        page_size: 1000,
                        page_token: page_token.clone(),
                    }),
                    ..Default::default()
                })
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "list_catalogs failed while building per-conn snapshot at mint: {e}"
                    ))
                })?
                .into_inner();
            for catalog in resp.catalogs {
                all.push((catalog.catalog_name, catalog.catalog_uuid));
            }
            match resp.next_page_token {
                Some(token) if !token.is_empty() => {
                    page_token = token;
                }
                _ => break,
            }
        }
        Ok(all)
    }

    /// Build the per-conn `Arc<SessionContext>`. Clones the server-startup
    /// template once and composes a fresh per-conn
    /// [`PencaCatalogProviderList`] (frozen `(name, uuid)` snapshot +
    /// the conn's pinned `catalog_uuid` + `branch_uuid` + the shared
    /// `open_tx_cell`) over it. The conn's catalog lives on the provider
    /// tree's `ConnScope`; there is no scan-time cross-catalog check
    /// (CHA-346).
    ///
    /// CHA-345: `open_tx_cell` is `Arc`-shared with the owning
    /// `ConnSession`, which flips it on BEGIN/COMMIT/ROLLBACK; the
    /// provider tree reads it during planning.
    fn build_ctx(
        &self,
        catalog_name: &str,
        catalog_uuid: &str,
        branch_uuid: &str,
        default_schema: &str,
        catalog_list: Vec<(String, String)>,
        cells: SharedConnCells,
    ) -> Arc<SessionContext> {
        let provider_list = Arc::new(PencaCatalogProviderList::from_snapshot(
            ConnScope {
                query_channel: self.query_channel.clone(),
                catalog_uuid: catalog_uuid.to_string(),
                catalog_name: catalog_name.to_string(),
                branch_uuid: branch_uuid.to_string(),
                open_tx_cell: cells.open_tx_cell,
                as_of_seq_cell: cells.as_of_seq_cell,
                resolution_memo_cell: cells.resolution_memo_cell,
            },
            catalog_list,
        ));
        let mut state = SessionStateBuilder::new_from_existing(self.template.clone())
            .with_catalog_list(provider_list)
            .build();
        let options = state.config_mut().options_mut();
        options.catalog.default_catalog = catalog_name.to_string();
        options.catalog.default_schema = default_schema.to_string();
        Arc::new(SessionContext::new_with_state(state))
    }
}

/// Per-conn lazy holder for [`ConnSession`]. Constructed fresh in
/// [`crate::flight_sql::service::PerConnMakeService::call`] (once per
/// accepted TCP connection), then Arc-shared across every HTTP/2
/// stream on that conn via clones of [`crate::flight_sql::service::PerConnService`].
///
/// `Clone` is **Arc-share, never fresh-mint** — flipping this to
/// fresh-mint would fork sessions per HTTP/2 stream and break every
/// per-conn invariant (frozen catalog, single open_tx, drop-on-close).
/// The `Clone` impl is hand-rolled with this invariant called out so a
/// future change can't accidentally `#[derive]` over it.
pub struct ConnSessionInit {
    factory: Arc<ConnSessionFactory>,
    /// Shared across every `PerConnService` clone on the same TCP conn.
    /// The first request through wins the `get_or_try_init` race; every
    /// subsequent request reads the same `Arc<ConnSession>`.
    cell: Arc<OnceCell<Arc<ConnSession>>>,
}

impl Clone for ConnSessionInit {
    fn clone(&self) -> Self {
        // Arc-share — every per-stream clone of `PerConnService` MUST
        // observe the same `OnceCell` so the first-request mint wins
        // and subsequent streams read the same `Arc<ConnSession>`.
        Self {
            factory: self.factory.clone(),
            cell: self.cell.clone(),
        }
    }
}

impl ConnSessionInit {
    pub fn new(factory: Arc<ConnSessionFactory>) -> Self {
        Self {
            factory,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// Lazily mint the conn's [`ConnSession`] on first call; return the
    /// existing `Arc<ConnSession>` on every subsequent call. The
    /// `catalog_override` / `branch_override` headers are read from the
    /// **first** request that triggers the mint; later requests' header
    /// values are ignored at this level (the per-request
    /// `validate_*_header` calls in
    /// [`crate::flight_sql::service`] catch drift).
    ///
    /// Takes `Option<&str>` rather than `Option<String>` so the
    /// per-request hot path (every call after the first) doesn't pay
    /// two `String` clones that get dropped inside `get_or_try_init`'s
    /// closure-not-invoked branch. Mint allocates inside the closure
    /// (one-time cost).
    pub async fn init_or_get(
        &self,
        catalog_override: Option<&str>,
        branch_override: Option<&str>,
    ) -> Result<Arc<ConnSession>, Status> {
        self.cell
            .get_or_try_init(|| async {
                self.factory
                    .mint(
                        catalog_override.map(str::to_string),
                        branch_override.map(str::to_string),
                    )
                    .await
            })
            .await
            .cloned()
    }
}

fn missing_catalog_status(catalog_name: &str) -> Status {
    Status::failed_precondition(format!(
        "this connection is pinned to catalog `{catalog_name}`, but no catalog by that \
         name exists in the catalog store. Either create it via \
         `WriteService::CreateCatalog`, or reconnect with a different \
         `x-penca-catalog` gRPC metadata header value (catalog is fixed at \
         handshake; reconnect to switch — CHA-253)."
    ))
}

fn missing_branch_status(branch_name: &str) -> Status {
    Status::failed_precondition(format!(
        "this connection is pinned to branch `{branch_name}`, but no branch by that \
         name exists in this catalog. Either create it via \
         `WriteService::CreateBranch`, or reconnect with a different \
         `x-penca-branch` gRPC metadata header value (branch is fixed at \
         handshake; reconnect to switch — CHA-255)."
    ))
}
