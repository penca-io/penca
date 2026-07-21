//! `CatalogProviderList` backed by a per-TCP-connection frozen snapshot.
//!
//! The list of catalogs visible to a connection is taken once at
//! session-mint time via a single batched `QueryServiceClient::list_catalogs`
//! call, then frozen for the connection's lifetime (CHA-255).
//! `catalog_names()` returns the full snapshot (so `SHOW CATALOGS` /
//! ADBC `GetCatalogs` works) but `catalog(name)` returns `Some` only
//! for the conn's pinned catalog — cross-catalog access isn't
//! supported, and a connection only ever exercises catalog operations
//! against the catalog it's pinned to.
//!
//! Per [ADR 0010](../../../../docs/decisions/0010-flight-sql-tx-pin-routing.md),
//! the connection-scoped catalog pin lives on the per-conn provider
//! tree's [`ConnScope`] (`catalog_uuid`), built once at session-mint
//! time. Cross-catalog access is gated by the catalog-list short-circuit
//! below (SELECT path) and `validate_session_catalog_name` (DML path);
//! there is no scan-time cross-catalog check (CHA-346). The open
//! `tx_uuid` (CHA-345) flows through the `Arc`-shared
//! `ConnScope.open_tx_cell` and is read across the provider
//! tree — `PencaCatalogProvider::{schema,schema_names}`,
//! `PencaSchemaProvider::{table,table_names,table_exist}`, and
//! `PencaTableProvider::scan` — so a schema/table created earlier in
//! the same transaction resolves. The catalog-list-level short-circuit here means a SQL
//! statement like `SELECT * FROM other_catalog.public.t` never
//! reaches the wire — DataFusion's planner emits a
//! `table '<other.catalog.t>' not found` error directly. The DML path
//! gets the more actionable "cross-catalog" wording from
//! [`penca_sql_server::tx::validate_session_catalog_name`] before
//! ever traversing the catalog tree.

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, CatalogProviderList};

use crate::catalog::PencaCatalogProvider;
use crate::conn_scope::ConnScope;

pub struct PencaCatalogProviderList {
    scope: ConnScope,
    /// `(catalog_name, catalog_uuid)` pairs captured by
    /// `ConnSessionFactory::mint` at first request and frozen for the
    /// conn's lifetime. Powers `catalog_names()` (the full list); only
    /// the conn's own catalog is reachable via `catalog(name)`.
    snapshot: Vec<(String, String)>,
}

impl std::fmt::Debug for PencaCatalogProviderList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PencaCatalogProviderList")
            .field("scope", &self.scope)
            .field("snapshot_len", &self.snapshot.len())
            .finish_non_exhaustive()
    }
}

impl PencaCatalogProviderList {
    /// Construct from a `(name, uuid)` snapshot already enumerated by the
    /// caller (typically `ConnSessionFactory::mint` via one
    /// `QueryServiceClient::list_catalogs` call). Treats the snapshot as
    /// authoritative for the connection's lifetime.
    pub fn from_snapshot(scope: ConnScope, snapshot: Vec<(String, String)>) -> Self {
        Self { scope, snapshot }
    }
}

impl CatalogProviderList for PencaCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        // Penca catalogs are managed via `WriteService::CreateCatalog`
        // over Flight RPC, not through DataFusion's `register_catalog`.
        // Return `None` to honor the trait contract — the return value
        // is the *previous* registration at `name`, and Penca has
        // never stored anything here, so `None` is the truthful answer.
        // (The prior `Some(catalog)` falsely echoed the just-passed
        // argument as if it were a prior registration.)
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        self.snapshot
            .iter()
            .map(|(name, _uuid)| name.clone())
            .collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        // Cross-catalog access isn't supported (CHA-169 / ADR 0010 /
        // CHA-255): each `(catalog, branch)` combination has its own
        // physical partition tables, and a conn is bound to exactly
        // one catalog. Short-circuit at the catalog-list level so a
        // cross-catalog SQL statement never reaches the wire —
        // DataFusion emits `table '<fqn>' not found` directly. The
        // DML path gets a more actionable wording from
        // `tx::validate_session_catalog_name` before traversing the
        // catalog tree.
        if name != self.scope.catalog_name {
            return None;
        }
        Some(Arc::new(PencaCatalogProvider::new(self.scope.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::catalog::MemoryCatalogProvider;
    use tonic::transport::Channel;

    #[tokio::test]
    async fn register_catalog_returns_none_and_does_not_store() {
        let list = PencaCatalogProviderList::from_snapshot(
            ConnScope {
                query_channel: Channel::from_static("http://localhost:0").connect_lazy(),
                catalog_uuid: "pinned-uuid".into(),
                catalog_name: "pinned".into(),
                branch_uuid: "main-uuid".into(),
                open_tx_cell: std::sync::Arc::new(std::sync::RwLock::new(None)),
                as_of_seq_cell: std::sync::Arc::new(std::sync::RwLock::new(None)),
                resolution_memo_cell: std::sync::Arc::new(std::sync::RwLock::new(None)),
            },
            vec![("pinned".into(), "pinned-uuid".into())],
        );

        let registered =
            list.register_catalog("other".into(), Arc::new(MemoryCatalogProvider::new()));
        assert!(
            registered.is_none(),
            "register_catalog must return None — Penca never had a previous registration at this name",
        );

        assert_eq!(
            list.catalog_names(),
            vec!["pinned".to_string()],
            "register_catalog must not append to the frozen snapshot",
        );

        assert!(
            list.catalog("other").is_none(),
            "conn-pin short-circuit must still hold — only the pinned catalog name resolves",
        );
    }
}
