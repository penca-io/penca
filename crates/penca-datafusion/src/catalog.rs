//! `CatalogProvider` backed by Penca's QueryService.
//!
//! Constructed only for the conn's pinned catalog — see
//! [`crate::catalog_list::PencaCatalogProviderList::catalog`] for the
//! cross-catalog short-circuit. Schema lookups (`schema_names`,
//! `schema`) hit gRPC live on every call (CHA-255). The previous TTL
//! cache (`MetadataCaches`) was deleted along with the cookie-keyed
//! session model — schemas need to stay live so that `CREATE SCHEMA
//! foo; SELECT * FROM foo.t` in the same session resolves. The
//! catalog half of the old cache is replaced by the per-conn
//! `(name, uuid)` snapshot in
//! [`crate::catalog_list::PencaCatalogProviderList`].
//!
//! CHA-345: `schema` / `schema_names` thread the conn's open `tx_uuid`
//! (from the `ConnScope` cell) into their wire reads, so a schema
//! created earlier in the same transaction resolves — the catalog-level
//! analogue of the table-level tx-awareness in [`crate::schema`].
//!
//! `branch_uuid` is the conn's branch in the conn's catalog,
//! rename-stable per CHA-255: every wire payload (`list_schemas`,
//! `get_schema`, `list_tables`, `get_table`, `read_data`) routes by
//! uuid so an out-of-band `UpdateBranch` doesn't break in-flight
//! queries.

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use penca_proto::external::v1::{GetSchemaRequest, ListSchemasRequest, PaginationRequest};

use crate::conn_scope::ConnScope;
use crate::schema::PencaSchemaProvider;

#[derive(Debug)]
pub(crate) struct PencaCatalogProvider {
    scope: ConnScope,
}

impl PencaCatalogProvider {
    pub(crate) fn new(scope: ConnScope) -> Self {
        Self { scope }
    }
}

impl CatalogProvider for PencaCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %self.scope.catalog_uuid,
            branch = %self.scope.branch_uuid,
            open_tx = ?self.scope.open_tx_uuid(),
        ),
    )]
    fn schema_names(&self) -> Vec<String> {
        let channel = self.scope.query_channel.clone();
        let catalog_uuid = self.scope.catalog_uuid.clone();
        let branch_uuid = self.scope.branch_uuid.clone();
        // CHA-345: tx-aware listing — schemas created mid-tx appear in
        // SHOW SCHEMAS via the ConnScope cell. CHA-374: also send the pinned
        // auto-commit as_of so this resolves at the statement's snapshot.
        let (open_tx_uuid, as_of_seq) = self.scope.read_snapshot_fields();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                // Paginate to exhaustion — schema metadata lists are bounded
                // (typically single digits). If this ever becomes a bottleneck,
                // the symptom will be slow SHOW SCHEMAS responses.
                crate::pagination::paginate_to_exhaustion("list_schemas", |page_token| {
                    let channel = channel.clone();
                    let catalog_uuid = catalog_uuid.clone();
                    let branch_uuid = branch_uuid.clone();
                    let open_tx_uuid = open_tx_uuid.clone();
                    async move {
                        let mut client = QueryServiceClient::new(channel);
                        let resp = client
                            .list_schemas(ListSchemasRequest {
                                catalog_uuid: Some(catalog_uuid),
                                catalog_name: None,
                                pagination: Some(PaginationRequest {
                                    page_size: 1000,
                                    page_token,
                                }),
                                open_tx_uuid,
                                // CHA-460: identifier resolution pins the seq
                                // axis to match the data read; micros unset.
                                as_of_micros: None,
                                as_of_seq,
                                // CHA-255: route by branch_uuid (rename-stable).
                                branch_uuid: Some(branch_uuid),
                                branch_name: None,
                            })
                            .await?;
                        let inner = resp.into_inner();
                        let items: Vec<String> =
                            inner.schemas.into_iter().map(|s| s.schema_name).collect();
                        Ok::<_, tonic::Status>((items, inner.next_page_token))
                    }
                })
                .await
            })
        })
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %self.scope.catalog_uuid,
            branch = %self.scope.branch_uuid,
            schema = %name,
            open_tx = ?self.scope.open_tx_uuid(),
        ),
    )]
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // CHA-367: reuse a resolution from earlier in this plan build if one is
        // memoized; otherwise resolve live and record it. The memo is cleared
        // between builds (see `plan_resolution_memo`), so a schema created
        // mid-tx still re-resolves on the next statement — CHA-345 tx-aware
        // visibility is preserved by `resolve_schema_live`'s live gRPC.
        let canonical = match self.scope.memo_get_schema(name) {
            Some(memoized) => memoized,
            None => match self.resolve_schema_live(name) {
                Ok(resolved) => {
                    self.scope
                        .memo_put_schema(name.to_string(), resolved.clone());
                    resolved
                }
                Err(e) => {
                    // Transient lookup error: fold to "not found" for this call
                    // (the `CatalogProvider::schema` trait is infallible) but do
                    // NOT memoize it, so a later `schema()` in the same build
                    // retries. Mirrors the table path, which propagates / folds
                    // errors without caching them.
                    tracing::error!(error = %e, "get_schema failed");
                    None
                }
            },
        };
        canonical.map(|schema_name| {
            Arc::new(PencaSchemaProvider::new(self.scope.clone(), schema_name))
                as Arc<dyn SchemaProvider>
        })
    }
}

impl PencaCatalogProvider {
    /// Resolve `name` against the query service with a live `get_schema` gRPC.
    /// `Ok(Some(name))` = found; `Ok(None)` = a confirmed miss (the server
    /// signalled `not_found`, or returned no schema) — safe to memoize;
    /// `Err(status)` = a transient lookup error the caller must NOT memoize.
    ///
    /// Threads the conn's open `tx_uuid` so a schema created earlier in the
    /// same transaction resolves (CHA-345); without it, planning of an in-tx
    /// `INSERT`/`SELECT` against `schema.table` would fail with "schema not
    /// found" before the table-level read is ever reached.
    fn resolve_schema_live(&self, name: &str) -> Result<Option<String>, tonic::Status> {
        let channel = self.scope.query_channel.clone();
        let catalog_uuid = self.scope.catalog_uuid.clone();
        let schema_name = name.to_string();
        let branch_uuid = self.scope.branch_uuid.clone();
        // CHA-374: pin the auto-commit snapshot (mutually exclusive with the
        // open tx) so schema resolution matches the data read's snapshot.
        let (open_tx_uuid, as_of_seq) = self.scope.read_snapshot_fields();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut client = QueryServiceClient::new(channel);
                match client
                    .get_schema(GetSchemaRequest {
                        catalog_uuid: Some(catalog_uuid),
                        catalog_name: None,
                        schema_uuid: None,
                        schema_name: Some(schema_name),
                        open_tx_uuid,
                        // CHA-460: identifier resolution pins the seq axis to
                        // match the data read; micros unset.
                        as_of_micros: None,
                        as_of_seq,
                        branch_uuid: Some(branch_uuid),
                        branch_name: None,
                    })
                    .await
                {
                    // A present schema, or `schema: None` (a confirmed miss in
                    // the no-payload shape) — both safe to memoize.
                    Ok(resp) => Ok(resp.into_inner().schema.map(|s| s.schema_name)),
                    // `not_found` is a confirmed miss — memoizable.
                    Err(s) if s.code() == tonic::Code::NotFound => Ok(None),
                    // Any other status is transient — surface it so the caller
                    // skips the memo write.
                    Err(e) => Err(e),
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    //! CHA-367 — `PencaCatalogProvider::schema` must not memoize a *transient*
    //! `get_schema` lookup error as a confirmed miss. A non-`NotFound` `Status`
    //! folds to `None` for the (infallible) call but leaves the per-build memo
    //! empty, so a later `schema()` in the same build retries; a `not_found` is
    //! a confirmed miss and IS memoized. Mirrors the table-path coverage in
    //! [`crate::schema`] (`not_found_from_query_service_resolves_to_ok_none` et al).
    use super::*;
    use std::sync::RwLock;

    use crate::plan_resolution_memo::{self, PlanResolutionMemoGuard};
    use crate::test_support::{StubQuery, spawn_stub};
    use penca_proto::external::v1::{GetSchemaResponse, Schema};
    use tonic::Status;
    use tonic::transport::Channel;

    /// A `PencaCatalogProvider` over `channel` with a fresh memo cell + an
    /// installed build guard. Returns the provider and the cell so the test can
    /// assert what was (not) memoized. The guard is returned too so the caller
    /// keeps it alive for the duration of the test.
    fn provider_with_memo(
        channel: Channel,
    ) -> (
        PencaCatalogProvider,
        plan_resolution_memo::PlanResolutionMemoCell,
        PlanResolutionMemoGuard,
    ) {
        let cell: plan_resolution_memo::PlanResolutionMemoCell = Arc::new(RwLock::new(None));
        let guard = PlanResolutionMemoGuard::install(cell.clone());
        let provider = PencaCatalogProvider::new(ConnScope {
            query_channel: channel,
            catalog_uuid: "cat-uuid".into(),
            catalog_name: "public".into(),
            branch_uuid: "branch-uuid".into(),
            open_tx_cell: Arc::new(RwLock::new(None)),
            as_of_seq_cell: Arc::new(RwLock::new(None)),
            resolution_memo_cell: cell.clone(),
        });
        (provider, cell, guard)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transient_get_schema_error_is_not_memoized() {
        let channel = spawn_stub(StubQuery {
            get_schema: Some(Err(Status::internal("boom"))),
            ..Default::default()
        })
        .await;
        let (provider, cell, _guard) = provider_with_memo(channel);

        // The infallible trait method folds the transient error to `None`...
        assert!(provider.schema("public").is_none());
        // ...but must NOT poison the memo, so a later call in the same build
        // re-issues the gRPC rather than reading a cached confirmed-miss.
        assert_eq!(
            plan_resolution_memo::memo_get_schema(&cell, "public"),
            None,
            "a transient get_schema error must not be memoized as a confirmed miss"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn not_found_get_schema_is_memoized_as_miss() {
        let channel = spawn_stub(StubQuery {
            get_schema: Some(Err(Status::not_found("schema not found"))),
            ..Default::default()
        })
        .await;
        let (provider, cell, _guard) = provider_with_memo(channel);

        assert!(provider.schema("public").is_none());
        // A confirmed not-found IS memoizable so a repeat lookup in the build
        // doesn't re-issue the gRPC to re-learn "not found".
        assert_eq!(
            plan_resolution_memo::memo_get_schema(&cell, "public"),
            Some(None),
            "not_found must be memoized as a confirmed miss"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn found_get_schema_is_memoized() {
        let channel = spawn_stub(StubQuery {
            get_schema: Some(Ok(GetSchemaResponse {
                schema: Some(Schema {
                    schema_name: "public".into(),
                    ..Schema::default()
                }),
            })),
            ..Default::default()
        })
        .await;
        let (provider, cell, _guard) = provider_with_memo(channel);

        assert!(provider.schema("public").is_some());
        assert_eq!(
            plan_resolution_memo::memo_get_schema(&cell, "public"),
            Some(Some("public".to_string())),
            "a resolved schema must be memoized by canonical name"
        );
    }
}
