//! Catalog / branch / table-listing pagination helpers.
//!
//! Each function drains a paginated list RPC and returns the whole
//! collection. All four delegate to [`crate::paginate::paginate_all`]
//! and supply the per-page request shape via an `AsyncFnMut` closure.

use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use penca_proto::external::v1::{
    Branch, Catalog, IntegerRange, ListBranchesRequest, ListCatalogsRequest,
    ListModifiedTablesRequest, ListPersistedTablesRequest,
};
use tonic::transport::Channel;

pub(crate) async fn list_all_catalogs(
    query: &mut QueryServiceClient<Channel>,
    page_size: i32,
) -> Result<Vec<Catalog>, tonic::Status> {
    crate::paginate::paginate_all(page_size, async |pagination| {
        let resp = query
            .list_catalogs(ListCatalogsRequest {
                pagination: Some(pagination),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok((resp.catalogs, resp.next_page_token))
    })
    .await
}

pub(crate) async fn list_all_branches(
    query: &mut QueryServiceClient<Channel>,
    page_size: i32,
    catalog_uuid: &str,
) -> Result<Vec<Branch>, tonic::Status> {
    crate::paginate::paginate_all(page_size, async |pagination| {
        let resp = query
            .list_branches(ListBranchesRequest {
                catalog_uuid: Some(catalog_uuid.to_string()),
                pagination: Some(pagination),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok((resp.branches, resp.next_page_token))
    })
    .await
}

pub(crate) async fn paginate_modified_tables(
    lifecycle: &mut LifecycleServiceClient<Channel>,
    page_size: i32,
    catalog_uuid: &str,
    branch_uuid: &str,
    min_micros: i64,
    max_micros: i64,
) -> Result<Vec<String>, tonic::Status> {
    crate::paginate::paginate_all(page_size, async |pagination| {
        let resp = lifecycle
            .list_modified_tables(ListModifiedTablesRequest {
                catalog_uuid: catalog_uuid.to_string(),
                branch_uuid: branch_uuid.to_string(),
                modified_at: Some(IntegerRange {
                    min: Some(min_micros),
                    max: Some(max_micros),
                }),
                pagination: Some(pagination),
            })
            .await?
            .into_inner();
        Ok((resp.table_uuids, resp.next_page_token))
    })
    .await
}

pub(crate) async fn paginate_persisted_tables(
    lifecycle: &mut LifecycleServiceClient<Channel>,
    page_size: i32,
    catalog_uuid: &str,
    branch_uuid: &str,
    min_micros: i64,
    max_micros: i64,
) -> Result<Vec<String>, tonic::Status> {
    crate::paginate::paginate_all(page_size, async |pagination| {
        let resp = lifecycle
            .list_persisted_tables(ListPersistedTablesRequest {
                catalog_uuid: catalog_uuid.to_string(),
                branch_uuid: branch_uuid.to_string(),
                persisted_at: Some(IntegerRange {
                    min: Some(min_micros),
                    max: Some(max_micros),
                }),
                pagination: Some(pagination),
            })
            .await?
            .into_inner();
        Ok((resp.table_uuids, resp.next_page_token))
    })
    .await
}
