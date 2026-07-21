//! `ListModifiedTables` / `ListPersistedTables` — scheduler dirty-set
//! discovery, rehomed onto `LifecycleService` (CHA-445).
//!
//! "What's dirty / what's persisted" is lifecycle enumeration, and the
//! scheduler already holds a `LifecycleServiceClient`. The SQL lives in
//! [`LifecycleManager`]; this is the thin orchestration wrapper (UUID
//! validation + pagination) that was formerly the `StorageMetadataManager`
//! methods of the deleted storage-metadata service.

use penca_db::driver::DbDriver;
use penca_proto::external::v1::{
    ListModifiedTablesRequest, ListModifiedTablesResponse, ListPersistedTablesRequest,
    ListPersistedTablesResponse,
};
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;
use crate::pagination::{pagination_from_request, take_page_and_next_token, timestamp_bounds};

/// Server-side default/cap for a single page on the two list-tables RPCs.
/// The scheduler is the only caller and pages explicitly; this just keeps
/// a page small enough to cap response size if a debug client omits
/// `page_size`.
const LIST_TABLES_DEFAULT_PAGE_SIZE: i64 = 100;

/// Parse the `(catalog_uuid, branch_uuid)` the list RPCs scope to, failing
/// fast at the boundary on a malformed UUID. `LifecycleManager` re-parses
/// internally to keep its reads pure-typed; this surfaces a uniform
/// `InvalidRequest` shape.
fn parse_catalog_branch(catalog: &str, branch: &str) -> Result<(Uuid, Uuid), ApiError> {
    let catalog_uuid = Uuid::parse_str(catalog)
        .map_err(|_| ApiError::InvalidRequest(format!("malformed catalog_uuid: {catalog}")))?;
    let branch_uuid = Uuid::parse_str(branch)
        .map_err(|_| ApiError::InvalidRequest(format!("malformed branch_uuid: {branch}")))?;
    Ok((catalog_uuid, branch_uuid))
}

impl LifecycleManager {
    /// Enumerate distinct `table_uuid`s touched by committed transactions
    /// on `(catalog_uuid, branch_uuid)` within the half-open `modified_at`
    /// window. Aborted-tx writes are structurally excluded by the `commit_tx_log`
    /// join. Drives the lifecycle scheduler's per-tick dirty-table sweep.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(catalog = %request.catalog_uuid, branch = %request.branch_uuid),
    )]
    pub async fn list_modified_tables(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &ListModifiedTablesRequest,
    ) -> Result<ListModifiedTablesResponse, ApiError> {
        let (catalog_uuid, branch_uuid) =
            parse_catalog_branch(&request.catalog_uuid, &request.branch_uuid)?;
        let (page_size, offset) =
            pagination_from_request(request.pagination.as_ref(), LIST_TABLES_DEFAULT_PAGE_SIZE);
        let (min_micros, max_micros) = timestamp_bounds(request.modified_at.as_ref());

        // Fetch `page_size + 1` to detect a next page without a count query.
        let rows = penca_storage_meta::LifecycleManager::list_modified_table_uuids_paginated(
            driver,
            &catalog_uuid,
            &branch_uuid,
            min_micros,
            max_micros,
            page_size + 1,
            offset,
        )
        .await?;

        let (page_uuids, next_page_token) = take_page_and_next_token(rows, page_size, offset);
        let table_uuids: Vec<String> = page_uuids.into_iter().map(|u| u.to_string()).collect();
        Ok(ListModifiedTablesResponse {
            table_uuids,
            next_page_token,
        })
    }

    /// Enumerate distinct `table_uuid`s with a committed
    /// `table_persist_metadata` row whose `commit_micros` falls in
    /// the half-open `persisted_at` window (the `commit_micros IS NOT
    /// NULL` filter excludes uncommitted persists). Drives the scheduler's
    /// per-tick Purge enumeration.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(catalog = %request.catalog_uuid, branch = %request.branch_uuid),
    )]
    pub async fn list_persisted_tables(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &ListPersistedTablesRequest,
    ) -> Result<ListPersistedTablesResponse, ApiError> {
        let (catalog_uuid, branch_uuid) =
            parse_catalog_branch(&request.catalog_uuid, &request.branch_uuid)?;
        let (page_size, offset) =
            pagination_from_request(request.pagination.as_ref(), LIST_TABLES_DEFAULT_PAGE_SIZE);
        let (min_micros, max_micros) = timestamp_bounds(request.persisted_at.as_ref());

        let rows = penca_storage_meta::LifecycleManager::list_persisted_table_uuids_paginated(
            driver,
            &catalog_uuid,
            &branch_uuid,
            min_micros,
            max_micros,
            page_size + 1,
            offset,
        )
        .await?;

        let (page_uuids, next_page_token) = take_page_and_next_token(rows, page_size, offset);
        let table_uuids: Vec<String> = page_uuids.into_iter().map(|u| u.to_string()).collect();
        Ok(ListPersistedTablesResponse {
            table_uuids,
            next_page_token,
        })
    }
}
