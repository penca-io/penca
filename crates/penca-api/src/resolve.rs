//! Identifier resolution helpers for flexible request handling.
//!
//! Namespace UUIDs are server-minted random values, so name → uuid resolution
//! is a database lookup, not a hash:
//!
//! - catalog / branch ← `catalog_store` / `branch_store` SELECT (non-MVCC
//!   PG tables; the name lookup is snapshot-blind by design — ADR 0020).
//! - schema / table ← `__penca_system__.{schemas,tables}` `stream_merged`
//!   under the caller's `ReadSnapshot`, so a name resolves under the same
//!   time-travel window as the subsequent data read (a table renamed
//!   `foo → bar` at T=200 still resolves from `table_name="foo"` at
//!   `as_of_micros=150`).
//!
//! **Table resolution is asymmetric by identifier:**
//! [`QueryManager::resolve_table_by_uuid`] is **catalog-wide** — it matches
//! `__penca_system__.tables` on `row_uuid` alone (no schema scoping), so a
//! table resolves by `table_uuid` regardless of which schema holds it and
//! no schema identifier is required. [`QueryManager::resolve_table_by_name`] stays
//! schema-scoped (the name lookup needs the schema parent).
//!
//! **Precedence rule (loud-on-purpose):** when both `X_uuid` and `X_name`
//! are supplied, `X_uuid` wins — the name is ignored, with no consistency
//! check between them.

use penca_core::naming::MAIN_BRANCH_NAME;
use penca_db::driver::DbDriver;
use penca_dl::driver::DlDriver;
use penca_merge::ReadSnapshot;
use penca_proto::external::v1::{Branch, Catalog, Schema, Table};
use penca_storage_meta::LifecycleManager;

use crate::query::QueryManager;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::error::ApiError;

/// Resolve a [`Catalog`] by `catalog_uuid`. NOT_FOUND if absent (a
/// `catalog_store` read, so the uuid path resolves existence, not just
/// format).
pub async fn resolve_catalog_by_uuid(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &str,
) -> Result<Catalog, ApiError> {
    parse_uuid(catalog_uuid)?;
    LifecycleManager::get_catalog(driver, Some(catalog_uuid), None)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("catalog not found: {catalog_uuid}")))
}

/// Resolve a [`Catalog`] by `catalog_name`. NOT_FOUND if absent.
pub async fn resolve_catalog_by_name(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_name: &str,
) -> Result<Catalog, ApiError> {
    LifecycleManager::get_catalog(driver, None, Some(catalog_name))
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("catalog not found: {catalog_name}")))
}

/// Dispatch to [`resolve_catalog_by_uuid`] / [`resolve_catalog_by_name`] —
/// uuid wins when both are supplied.
pub async fn resolve_catalog(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: Option<&str>,
    catalog_name: Option<&str>,
) -> Result<Catalog, ApiError> {
    if let Some(s) = catalog_uuid {
        resolve_catalog_by_uuid(driver, s).await
    } else if let Some(name) = catalog_name {
        resolve_catalog_by_name(driver, name).await
    } else {
        Err(ApiError::InvalidRequest(
            "must provide catalog_uuid or catalog_name".into(),
        ))
    }
}

/// Resolve a [`Branch`] by `branch_uuid` within a catalog. NOT_FOUND if
/// absent (a per-catalog `branch_store` read).
pub async fn resolve_branch_by_uuid(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &Uuid,
    branch_uuid: &str,
) -> Result<Branch, ApiError> {
    parse_uuid(branch_uuid)?;
    let catalog_str = catalog_uuid.to_string();
    LifecycleManager::get_branch_row(driver, &catalog_str, branch_uuid)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("branch not found: {branch_uuid}")))
}

/// Resolve a [`Branch`] by `branch_name` within a catalog. NOT_FOUND if
/// absent.
pub async fn resolve_branch_by_name(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &Uuid,
    branch_name: &str,
) -> Result<Branch, ApiError> {
    let catalog_str = catalog_uuid.to_string();
    LifecycleManager::get_branch_by_name(driver, &catalog_str, branch_name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("branch not found: {branch_name}")))
}

/// Resolve the catalog's `main` branch UUID.
pub async fn resolve_main_branch_uuid(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &Uuid,
) -> Result<Uuid, ApiError> {
    let main_branch = resolve_branch_by_name(driver, catalog_uuid, MAIN_BRANCH_NAME).await?;
    parse_resolved_uuid(&main_branch.branch_uuid, "branch_uuid")
}

/// Dispatch to [`resolve_branch_by_uuid`] / [`resolve_branch_by_name`] —
/// uuid wins when both are supplied. Errors if neither is provided (clients
/// default to "main" themselves if they want the default branch).
pub async fn resolve_branch(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &Uuid,
    branch_uuid: Option<&str>,
    branch_name: Option<&str>,
) -> Result<Branch, ApiError> {
    if let Some(s) = branch_uuid {
        resolve_branch_by_uuid(driver, catalog_uuid, s).await
    } else if let Some(name) = branch_name {
        resolve_branch_by_name(driver, catalog_uuid, name).await
    } else {
        Err(ApiError::InvalidRequest(
            "must provide branch_uuid or branch_name".into(),
        ))
    }
}

impl QueryManager {
    /// Resolve a [`Schema`] by `schema_uuid`. The lookup is per-catalog, so a
    /// `(catalog_uuid=A, schema_uuid=X_in_B)` tuple surfaces NOT_FOUND.
    pub async fn resolve_schema_by_uuid<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &Uuid,
        schema_uuid: &str,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Schema, ApiError>
    where
        L: DlDriver + ?Sized,
    {
        parse_uuid(schema_uuid)?;
        let catalog_str = catalog_uuid.to_string();
        self.meta_get_schema(
            driver,
            dl,
            &catalog_str,
            Some(schema_uuid),
            None,
            branch_uuid,
            snapshot,
        )
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("schema not found: {schema_uuid}")))
    }

    /// Resolve a [`Schema`] by `schema_name`. Name resolution reads
    /// `__penca_system__.schemas` under the caller's snapshot so a renamed
    /// schema resolves at its historical name when `snapshot` is `AsOfMicros`.
    pub async fn resolve_schema_by_name<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &Uuid,
        schema_name: &str,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Schema, ApiError>
    where
        L: DlDriver + ?Sized,
    {
        let catalog_str = catalog_uuid.to_string();
        self.meta_get_schema(
            driver,
            dl,
            &catalog_str,
            None,
            Some(schema_name),
            branch_uuid,
            snapshot,
        )
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("schema not found: {schema_name}")))
    }

    /// Dispatch to [`Self::resolve_schema_by_uuid`] / [`Self::resolve_schema_by_name`] —
    /// uuid wins when both are supplied.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_schema<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &Uuid,
        schema_uuid: Option<&str>,
        schema_name: Option<&str>,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Schema, ApiError>
    where
        L: DlDriver + ?Sized,
    {
        if let Some(s) = schema_uuid {
            self.resolve_schema_by_uuid(driver, dl, catalog_uuid, s, branch_uuid, snapshot)
                .await
        } else if let Some(name) = schema_name {
            self.resolve_schema_by_name(driver, dl, catalog_uuid, name, branch_uuid, snapshot)
                .await
        } else {
            Err(ApiError::InvalidRequest(
                "must provide schema_uuid or schema_name".into(),
            ))
        }
    }

    /// Resolve a [`Table`] by `table_uuid`, **catalog-wide** (schema-agnostic):
    /// the lookup spans every schema on the branch, so a table whose real schema
    /// differs from any convenient schema the caller might pass still resolves —
    /// including `__penca_system__` bootstrap-table rows. NOT_FOUND if absent.
    pub async fn resolve_table_by_uuid<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &Uuid,
        table_uuid: &str,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Table, ApiError>
    where
        L: DlDriver + ?Sized,
    {
        parse_uuid(table_uuid)?;
        let catalog_str = catalog_uuid.to_string();
        self.get_table_by_uuid(driver, dl, &catalog_str, table_uuid, branch_uuid, snapshot)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("table not found: {table_uuid}")))
    }

    /// Resolve a [`Table`] by `table_name` within a schema (schema-scoped). Name
    /// resolution reads `__penca_system__.tables` under the caller's snapshot so
    /// a renamed table resolves at its historical name when `snapshot` is
    /// `AsOfMicros`.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_table_by_name<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &Uuid,
        schema_uuid: &Uuid,
        table_name: &str,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Table, ApiError>
    where
        L: DlDriver + ?Sized,
    {
        let catalog_str = catalog_uuid.to_string();
        let schema_str = schema_uuid.to_string();
        self.meta_get_table(
            driver,
            dl,
            &catalog_str,
            &schema_str,
            None,
            Some(table_name),
            branch_uuid,
            snapshot,
        )
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("table not found: {table_name}")))
    }

    /// Dispatch to [`Self::resolve_table_by_uuid`] / [`Self::resolve_table_by_name`] — uuid
    /// wins (catalog-wide, schema ignored); the name path requires `schema_uuid`
    /// as the parent.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_table<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &Uuid,
        schema_uuid: Option<&Uuid>,
        table_uuid: Option<&str>,
        table_name: Option<&str>,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Table, ApiError>
    where
        L: DlDriver + ?Sized,
    {
        if let Some(uuid_str) = table_uuid {
            self.resolve_table_by_uuid(driver, dl, catalog_uuid, uuid_str, branch_uuid, snapshot)
                .await
        } else if let Some(name) = table_name {
            let schema = schema_uuid.ok_or_else(|| {
                ApiError::InvalidRequest("schema_uuid required when resolving table by name".into())
            })?;
            self.resolve_table_by_name(
                driver,
                dl,
                catalog_uuid,
                schema,
                name,
                branch_uuid,
                snapshot,
            )
            .await
        } else {
            Err(ApiError::InvalidRequest(
                "must provide table_uuid, or schema_uuid + table_name".into(),
            ))
        }
    }
}

fn parse_uuid(s: &str) -> Result<Uuid, ApiError> {
    s.parse::<Uuid>()
        .map_err(|e| ApiError::InvalidRequest(format!("invalid UUID '{s}': {e}")))
}

/// Parse a UUID string that the resolver layer itself produced — a validated
/// request id echoed back on a resolved object, or a stored row column. A
/// failure here is server-side corruption, not bad client input, so it
/// surfaces as `Internal` rather than `InvalidRequest`.
pub fn parse_resolved_uuid(s: &str, what: &str) -> Result<Uuid, ApiError> {
    s.parse::<Uuid>()
        .map_err(|e| ApiError::Internal(format!("resolved {what} '{s}' is not a valid uuid: {e}")))
}
