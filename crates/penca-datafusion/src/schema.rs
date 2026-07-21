//! `SchemaProvider` backed by Penca's admin gRPC service.

use std::any::Any;
use std::sync::Arc;

use arrow::ipc::convert::try_schema_from_ipc_buffer;
use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use penca_proto::external::v1::{GetTableRequest, ListTablesRequest, PaginationRequest};

use crate::conn_scope::ConnScope;
use crate::plan_resolution_memo::{ResolvedIndex, ResolvedTable};
use crate::table::PencaTableProvider;

/// DataFusion `SchemaProvider` that resolves tables via `QueryServiceClient`.
///
/// Table operations are not cached **across** requests: tables change with
/// DDL / branch operations, and stale table metadata would cause query
/// failures rather than just delayed discovery (CHA-255 deleted the old TTL
/// cache for this reason). CHA-367 adds a memo scoped to a single plan build
/// only — `table` / `table_exist` reuse one `get_table` gRPC per identifier
/// within one `create_logical_plan`, and the memo is cleared between builds
/// (see [`crate::plan_resolution_memo`]), so cross-request / mid-tx DDL
/// visibility is unchanged. `table_names` (the `SHOW TABLES` listing) is still
/// resolved live every call. Revisit in CHA-120 if table listing becomes a
/// bottleneck.
///
/// Carries its [`ConnScope`]. `catalog_uuid` is threaded down to
/// [`PencaTableProvider`], whose provider tree is built entirely with
/// the conn's catalog — cross-catalog identifiers are gated upstream by
/// the catalog-list short-circuit in [`crate::catalog_list`] (and, on
/// the DML path, `validate_session_catalog_name`), so they never reach
/// scan. There is no scan-time cross-catalog check (CHA-346).
///
/// `branch_uuid` is the conn's branch in the conn's catalog; wire
/// payloads route by uuid per CHA-255 (rename-stable across
/// out-of-band `UpdateBranch`).
///
/// CHA-345: the scope's `open_tx_cell` is read by `table` / `table_names`
/// / `table_exist` so metadata reads see tables created mid-transaction.
/// This is what relaxes ADR 0010's "Flight SQL clients only see committed
/// metadata" restriction — the `SchemaProvider::table` trait has no
/// `&Session`, but the cell is reachable from `&self.scope`.
#[derive(Debug)]
pub(crate) struct PencaSchemaProvider {
    scope: ConnScope,
    schema_name: String,
}

impl PencaSchemaProvider {
    pub(crate) fn new(scope: ConnScope, schema_name: String) -> Self {
        Self { scope, schema_name }
    }
}

#[async_trait]
impl SchemaProvider for PencaSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // Not cached — see struct-level doc comment.
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %self.scope.catalog_uuid,
            schema = %self.schema_name,
            branch = %self.scope.branch_uuid,
            open_tx = ?self.scope.open_tx_uuid(),
        ),
    )]
    fn table_names(&self) -> Vec<String> {
        let channel = self.scope.query_channel.clone();
        let catalog_name = self.scope.catalog_name.clone();
        let schema_name = self.schema_name.clone();
        let branch_uuid = self.scope.branch_uuid.clone();
        // CHA-345: tx-aware listing — tables created mid-tx appear in
        // SHOW TABLES via the ConnScope cell. CHA-374: also send the pinned
        // auto-commit as_of so this resolves at the statement's snapshot.
        let (open_tx_uuid, as_of_seq) = self.scope.read_snapshot_fields();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                // Paginate to exhaustion — table metadata lists are bounded
                // (typically dozens, not millions). If this ever becomes a
                // bottleneck, the symptom will be slow SHOW TABLES responses.
                crate::pagination::paginate_to_exhaustion("list_tables", |page_token| {
                    let channel = channel.clone();
                    let catalog_name = catalog_name.clone();
                    let schema_name = schema_name.clone();
                    let branch_uuid = branch_uuid.clone();
                    let open_tx_uuid = open_tx_uuid.clone();
                    async move {
                        let mut client = QueryServiceClient::new(channel);
                        let resp = client
                            .list_tables(ListTablesRequest {
                                catalog_name: Some(catalog_name),
                                schema_name: Some(schema_name),
                                // CHA-255: route by branch_uuid (rename-stable).
                                branch_uuid: Some(branch_uuid),
                                branch_name: None,
                                catalog_uuid: None,
                                schema_uuid: None,
                                open_tx_uuid,
                                // CHA-460: the SQL pin is a commit_seq_num frontier;
                                // identifier resolution pins on the seq axis to
                                // match the data read. Micros stays unset.
                                as_of_micros: None,
                                as_of_seq,
                                pagination: Some(PaginationRequest {
                                    page_size: 1000,
                                    page_token,
                                }),
                            })
                            .await?;
                        let inner = resp.into_inner();
                        let items: Vec<String> =
                            inner.tables.into_iter().map(|t| t.table_name).collect();
                        Ok::<_, tonic::Status>((items, inner.next_page_token))
                    }
                })
                .await
            })
        })
    }

    // CHA-367: per-plan-build memoized — see `resolve_table_live` and the
    // `plan_resolution_memo` module. Within one plan build, repeated `table()`
    // calls for the same name reuse the first resolution (one `get_table`
    // gRPC); the memo is cleared between builds, so a table created mid-tx
    // still re-resolves on the next statement (CHA-345).
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %self.scope.catalog_uuid,
            schema = %self.schema_name,
            table = %name,
            branch = %self.scope.branch_uuid,
            open_tx = ?self.scope.open_tx_uuid(),
        ),
    )]
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let resolved_table = self.resolved_table_memoized(name).await?;
        Ok(resolved_table.map(|resolved| {
            Arc::new(PencaTableProvider::new(
                self.scope.clone(),
                self.schema_name.clone(),
                name.to_string(),
                resolved.arrow_schema,
                resolved.primary_keys,
                resolved.indexes,
            )) as Arc<dyn TableProvider>
        }))
    }

    // CHA-286: a single GetTable answers existence directly (the previous body
    // re-paginated ListTables through table_names just to String::contains).
    // CHA-367: routes through the same `resolve_table_live` + per-plan-build
    // memo as `table()`, so existence and provider-build for one identifier
    // share a single `get_table` gRPC within a plan build. Flattens to `bool`
    // because `SchemaProvider::table_exist` is infallible: a non-NotFound error
    // is logged and folded to `false` (the transient error is not memoized).
    //
    // A table exists iff the resolution produced a schema (`Some`); a miss
    // (`Status::not_found`, or a `get_table` that returns no payload) is `None`
    // and `false`, agreeing with `table()`'s `Ok(None)`. The two derive their
    // answer from the same memoized `Option<ResolvedTable>`, so they cannot
    // diverge.
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %self.scope.catalog_uuid,
            schema = %self.schema_name,
            table = %name,
            branch = %self.scope.branch_uuid,
            open_tx = ?self.scope.open_tx_uuid(),
        ),
    )]
    fn table_exist(&self, name: &str) -> bool {
        // Sync fast path on a memo hit: a read-only re-check that skips the
        // block_in_place core hand-off (and its multi-thread-runtime
        // requirement). The miss/put logic stays solely in
        // `resolved_table_memoized`, so this cannot diverge from `table()`.
        if let Some(memoized) = self.scope.memo_get_table(&self.schema_name, name) {
            return memoized.is_some();
        }
        // CHA-345: the memoized resolver threads the conn's open `tx_uuid`,
        // so a table created mid-tx exists for this connection.
        // `block_in_place` bridges the async resolver to this sync trait
        // method.
        let resolved = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.resolved_table_memoized(name))
        });
        match resolved {
            Ok(resolved) => resolved.is_some(),
            Err(e) => {
                // Mirrors table_names's log-and-fold error handling to preserve
                // the infallible bool contract; a transient error is not cached.
                tracing::error!(error = %e, "get_table failed");
                false
            }
        }
    }
}

impl PencaSchemaProvider {
    /// Memoized table resolution for one plan build (CHA-367): consult the
    /// `ConnScope` memo, fall back to one live [`Self::resolve_table_live`]
    /// gRPC, and record the result (hit or confirmed miss). The shared
    /// kernel of `table()` and `table_exist()`, so the
    /// memoize-around-resolution sequence lives at one site and the two
    /// trait methods cannot diverge.
    async fn resolved_table_memoized(&self, name: &str) -> Result<Option<ResolvedTable>> {
        match self.scope.memo_get_table(&self.schema_name, name) {
            Some(memoized) => Ok(memoized),
            None => {
                let resolved = self.resolve_table_live(name).await?;
                self.scope.memo_put_table(
                    self.schema_name.clone(),
                    name.to_string(),
                    resolved.clone(),
                );
                Ok(resolved)
            }
        }
    }

    /// Resolve `name` against the query service with a live `get_table` gRPC,
    /// returning the table's resolved metadata — Arrow schema + declared
    /// primary keys — (`Ok(Some)`), a confirmed miss (`Ok(None)`), or a
    /// lookup error (`Err`). Shared by `table()` and `table_exist()` so one
    /// identifier costs one gRPC per plan build (the caller memoizes the
    /// result on the `ConnScope`).
    ///
    /// The `SchemaProvider::table` contract distinguishes "doesn't exist"
    /// (`Ok(None)`) from "lookup failed" (`Err`). QueryService signals the
    /// former with `Status::not_found` (see
    /// `penca-api::resolve::resolve_table`); CHA-257 — folding both into
    /// `External` mislabels every CREATE/SELECT against a fresh name as the
    /// wrapped `code: NotFound` JDBC users surface. A `get_table` that returns
    /// `Ok` with no `table` payload (not produced by today's server — it only
    /// returns `Ok` when `table: Some(_)`) is likewise treated as a miss
    /// (`Ok(None)`), so `table()` and `table_exist()` cannot disagree.
    async fn resolve_table_live(&self, name: &str) -> Result<Option<ResolvedTable>> {
        let mut client = QueryServiceClient::new(self.scope.query_channel.clone());
        // CHA-374: pin the auto-commit snapshot (mutually exclusive with the
        // open tx) so name resolution matches the data read's snapshot.
        let (open_tx_uuid, as_of_seq) = self.scope.read_snapshot_fields();
        let resp = match client
            .get_table(GetTableRequest {
                catalog_name: Some(self.scope.catalog_name.clone()),
                schema_name: Some(self.schema_name.clone()),
                table_name: Some(name.to_string()),
                // CHA-255: route by branch_uuid (rename-stable).
                branch_uuid: Some(self.scope.branch_uuid.clone()),
                branch_name: None,
                catalog_uuid: None,
                schema_uuid: None,
                table_uuid: None,
                // CHA-345: tx-aware metadata read — a table created mid-tx
                // resolves here via the ConnScope cell.
                open_tx_uuid,
                // CHA-460: identifier resolution pins the seq axis (matches the
                // data read); micros unset on the SQL path.
                as_of_micros: None,
                as_of_seq,
            })
            .await
        {
            Ok(r) => r,
            Err(s) if s.code() == tonic::Code::NotFound => return Ok(None),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        };

        match resp.into_inner().table {
            Some(table) => {
                let arrow_schema =
                    try_schema_from_ipc_buffer(&table.arrow_schema).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                Ok(Some(ResolvedTable {
                    arrow_schema: Arc::new(arrow_schema),
                    primary_keys: table.primary_keys.into(),
                    // CHA-492: thread the defined index set through so scan can
                    // pack a structured `indexes` seek for a covering predicate.
                    indexes: table
                        .indexes
                        .into_iter()
                        .map(|ix| ResolvedIndex {
                            index_uuid: ix.index_uuid,
                            index_name: ix.index_name,
                            key_columns: ix.columns.into(),
                        })
                        .collect(),
                }))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    //! CHA-257 — `PencaSchemaProvider::table` must translate a server-side
    //! `Status::not_found` from `QueryService::get_table` into `Ok(None)`
    //! (DataFusion's `SchemaProvider::table` contract for "doesn't exist"),
    //! while every other `Status` keeps propagating as `External`.
    //!
    //! The stub `QueryService` below is an in-process tonic server bound to an
    //! ephemeral loopback port — sufficient to drive `get_table` from a real
    //! `QueryServiceClient<Channel>` without standing up the full server stack.
    use super::*;

    use crate::plan_resolution_memo::PlanResolutionMemoGuard;
    use crate::test_support::{StubQuery, spawn_stub};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use datafusion::error::DataFusionError;
    use penca_proto::external::v1::{GetTableResponse, Table};
    use tonic::Status;
    use tonic::transport::Channel;

    fn provider_for(channel: Channel) -> PencaSchemaProvider {
        PencaSchemaProvider::new(
            ConnScope {
                query_channel: channel,
                catalog_uuid: "cat-uuid".into(),
                catalog_name: "public".into(),
                branch_uuid: "branch-uuid".into(),
                open_tx_cell: std::sync::Arc::new(std::sync::RwLock::new(None)),
                as_of_seq_cell: std::sync::Arc::new(std::sync::RwLock::new(None)),
                resolution_memo_cell: std::sync::Arc::new(std::sync::RwLock::new(None)),
            },
            "public".into(),
        )
    }

    #[tokio::test]
    async fn not_found_from_query_service_resolves_to_ok_none() {
        let channel = spawn_stub(StubQuery {
            get_table: Some(Err(Status::not_found("table not found: users"))),
            ..Default::default()
        })
        .await;
        let provider = provider_for(channel);

        let result = provider
            .table("users")
            .await
            .expect("NotFound from QueryService must not propagate as DataFusionError");
        assert!(
            result.is_none(),
            "NotFound must map to Ok(None) per SchemaProvider::table contract"
        );
    }

    #[tokio::test]
    async fn non_not_found_status_propagates_as_external() {
        let channel = spawn_stub(StubQuery {
            get_table: Some(Err(Status::internal("boom"))),
            ..Default::default()
        })
        .await;
        let provider = provider_for(channel);

        let err = provider
            .table("users")
            .await
            .expect_err("non-NotFound Status must propagate");
        let DataFusionError::External(inner) = err else {
            panic!("expected DataFusionError::External, got {err:?}");
        };
        let status = inner
            .downcast_ref::<Status>()
            .expect("External must wrap the original tonic::Status unchanged");
        assert_eq!(
            status.code(),
            tonic::Code::Internal,
            "wrapped Status must preserve its code"
        );
    }

    // CHA-286 — `table_exist` routes through a single `GetTable` RPC; CHA-367
    // shares that resolution with `table()` via the per-build memo. These tests
    // stub only `get_table`.
    //
    // `flavor = "multi_thread"` is required: `table_exist` is sync and uses
    // `block_in_place` to bridge to the async client, which panics on the
    // default current-thread runtime.

    #[tokio::test(flavor = "multi_thread")]
    async fn table_exist_not_found_from_get_table_returns_false() {
        let channel = spawn_stub(StubQuery {
            get_table: Some(Err(Status::not_found("table not found: users"))),
            ..Default::default()
        })
        .await;
        let provider = provider_for(channel);

        assert!(
            !provider.table_exist("users"),
            "NotFound from get_table must resolve to false"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn table_exist_ok_response_without_payload_returns_false() {
        // CHA-367: existence is "the resolution produced a schema". A
        // `get_table` that returns `Ok` with no `table` payload is a miss
        // (`Ok(None)`), so `table_exist` is `false` — agreeing with `table()`,
        // which has no schema to build a provider. (Today's server never sends
        // this shape — it returns `Status::not_found` for a miss — so this pins
        // the unreachable defensive arm to the same answer as a real miss.)
        let channel = spawn_stub(StubQuery {
            get_table: Some(Ok(GetTableResponse { table: None })),
            ..Default::default()
        })
        .await;
        let provider = provider_for(channel);

        assert!(
            !provider.table_exist("users"),
            "an Ok with no table payload must resolve to false (a miss), matching table()"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn table_exist_non_not_found_status_returns_false() {
        // Mirrors `table_names`'s log-and-break-with-empty-vec error
        // handling on the bool side: non-NotFound errors are logged and
        // folded to `false` to preserve the infallible `bool` contract.
        let channel = spawn_stub(StubQuery {
            get_table: Some(Err(Status::internal("boom"))),
            ..Default::default()
        })
        .await;
        let provider = provider_for(channel);

        assert!(
            !provider.table_exist("users"),
            "non-NotFound error from get_table must resolve to false"
        );
    }

    #[tokio::test]
    async fn table_exist_memo_hit_answers_without_bridging() {
        // Deliberately a current-thread runtime (default #[tokio::test]
        // flavor): `block_in_place` PANICS here, so a green run proves the
        // memoized fast path answers before the bridge — and that the hit
        // agrees with `table()`'s resolution of the same identifier.
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let mut arrow_schema_ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut arrow_schema_ipc, &schema).unwrap();
            writer.finish().unwrap();
        }
        let channel = spawn_stub(StubQuery {
            get_table: Some(Ok(GetTableResponse {
                table: Some(Table {
                    table_uuid: "11111111-2222-3333-4444-555555555555".into(),
                    table_name: "users".into(),
                    arrow_schema: arrow_schema_ipc,
                    primary_keys: vec!["a".to_string()],
                    ..Default::default()
                }),
            })),
            ..Default::default()
        })
        .await;
        let provider = provider_for(channel);
        let _guard = PlanResolutionMemoGuard::install(provider.scope.resolution_memo_cell.clone());

        // Warm the memo through the resolution path (one live gRPC).
        let resolved = provider.table("users").await.unwrap();
        assert!(resolved.is_some(), "stubbed table must resolve");

        assert!(
            provider.table_exist("users"),
            "a memo hit must answer table_exist without the block_in_place bridge"
        );
    }
}
