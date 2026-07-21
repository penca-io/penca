//! QueryService gRPC server implementation.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use penca_api::QueryManager;
use penca_db::driver::pg::PgDriver;
use penca_dl::driver::DlDriver;
use penca_format::reader::FormatReader;
use penca_proto::external::v1::query_service_server::QueryService;
use penca_proto::external::v1::{
    AuditDataRequest, AuditDataResponse, GetBranchRequest, GetBranchResponse, GetCatalogRequest,
    GetCatalogResponse, GetIndexRequest, GetIndexResponse, GetMaxCommitSeqNumRequest,
    GetMaxCommitSeqNumResponse, GetSchemaRequest, GetSchemaResponse, GetTableRequest,
    GetTableResponse, ListBranchesRequest, ListBranchesResponse, ListCatalogsRequest,
    ListCatalogsResponse, ListIndexesRequest, ListIndexesResponse, ListSchemasRequest,
    ListSchemasResponse, ListTablesRequest, ListTablesResponse, ReadDataRequest, ReadDataResponse,
};
use tonic::{Request, Response, Status};

use crate::ipc::ipc_response_stream;
use crate::status::api_error_to_status;

pub struct QueryServiceImpl<L: DlDriver + ?Sized, R: FormatReader> {
    pub pool: PgDriver,
    pub dl_driver: Arc<L>,
    pub readers: Arc<HashMap<i32, R>>,
    pub manager: QueryManager,
}

#[tonic::async_trait]
impl<L, R> QueryService for QueryServiceImpl<L, R>
where
    L: DlDriver + ?Sized + Send + Sync + 'static,
    R: FormatReader + 'static,
{
    #[tracing::instrument(
        skip_all,
        fields(catalog = ?request.get_ref().catalog_uuid),
    )]
    async fn get_catalog(
        &self,
        request: Request<GetCatalogRequest>,
    ) -> Result<Response<GetCatalogResponse>, Status> {
        crate::validation::validate_get_catalog(request.get_ref())?;
        let resp = self
            .manager
            .get_catalog(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    // No discriminating fields: per CHA-315 review, `ListCatalogsRequest.owner`
    // can carry an email address per the proto comment ("e.g., email or
    // service account"), so it falls under the same PII gate as
    // `database_url` and `ReadDataRequest.filter` — kept out of spans
    // until a hashing/correlation convention exists.
    #[tracing::instrument(skip_all)]
    async fn list_catalogs(
        &self,
        request: Request<ListCatalogsRequest>,
    ) -> Result<Response<ListCatalogsResponse>, Status> {
        let resp = self
            .manager
            .list_catalogs(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            schema = ?request.get_ref().schema_uuid,
            branch = ?request.get_ref().branch_uuid,
        ),
    )]
    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        crate::validation::validate_get_schema(request.get_ref())?;
        let resp = self
            .manager
            .get_schema(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
        ),
    )]
    async fn list_schemas(
        &self,
        request: Request<ListSchemasRequest>,
    ) -> Result<Response<ListSchemasResponse>, Status> {
        crate::validation::validate_list_schemas(request.get_ref())?;
        let resp = self
            .manager
            .list_schemas(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            schema = ?request.get_ref().schema_uuid,
            branch = ?request.get_ref().branch_uuid,
            table = ?request.get_ref().table_uuid,
        ),
    )]
    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        crate::validation::validate_get_table(request.get_ref())?;
        let resp = self
            .manager
            .get_table(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            schema = ?request.get_ref().schema_uuid,
            branch = ?request.get_ref().branch_uuid,
        ),
    )]
    async fn list_tables(
        &self,
        request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        crate::validation::validate_list_tables(request.get_ref())?;
        let resp = self
            .manager
            .list_tables(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    async fn get_index(
        &self,
        request: Request<GetIndexRequest>,
    ) -> Result<Response<GetIndexResponse>, Status> {
        crate::validation::validate_get_index(request.get_ref())?;
        let resp = self
            .manager
            .get_index(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    async fn list_indexes(
        &self,
        request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        crate::validation::validate_list_indexes(request.get_ref())?;
        let resp = self
            .manager
            .list_indexes(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            schema = ?request.get_ref().schema_uuid,
            branch = ?request.get_ref().branch_uuid,
        ),
    )]
    async fn get_branch(
        &self,
        request: Request<GetBranchRequest>,
    ) -> Result<Response<GetBranchResponse>, Status> {
        crate::validation::validate_get_branch(request.get_ref())?;
        let resp = self
            .manager
            .get_branch(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            schema = ?request.get_ref().schema_uuid,
        ),
    )]
    async fn list_branches(
        &self,
        request: Request<ListBranchesRequest>,
    ) -> Result<Response<ListBranchesResponse>, Status> {
        crate::validation::validate_list_branches(request.get_ref())?;
        let resp = self
            .manager
            .list_branches(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    type ReadDataStream =
        Pin<Box<dyn Stream<Item = Result<ReadDataResponse, Status>> + Send + 'static>>;

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            schema = ?request.get_ref().schema_uuid,
            branch = ?request.get_ref().branch_uuid,
            table = ?request.get_ref().table_uuid,
        ),
    )]
    async fn read_data(
        &self,
        request: Request<ReadDataRequest>,
    ) -> Result<Response<Self::ReadDataStream>, Status> {
        crate::validation::validate_read_data(request.get_ref())?;
        let req = request.into_inner();
        let pool = self.pool.clone();
        let readers = self.readers.clone();
        let manager = self.manager.clone();

        // pool and readers are moved into the async_stream generator to
        // satisfy 'static. readers is passed as an Arc clone so the cold
        // resolver can hold a 'static handle for DataFusion.
        let stream = async_stream::try_stream! {
            let batch_stream = manager.read_data(&pool, readers, &req)
                .await
                .map_err(api_error_to_status)?;
            for await item in ipc_response_stream(batch_stream, |data| ReadDataResponse { data }) {
                yield item?;
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    type AuditDataStream =
        Pin<Box<dyn Stream<Item = Result<AuditDataResponse, Status>> + Send + 'static>>;

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            schema = ?request.get_ref().schema_uuid,
            branch = ?request.get_ref().branch_uuid,
            table = ?request.get_ref().table_uuid,
        ),
    )]
    async fn audit_data(
        &self,
        request: Request<AuditDataRequest>,
    ) -> Result<Response<Self::AuditDataStream>, Status> {
        crate::validation::validate_audit_data(request.get_ref())?;
        let req = request.into_inner();
        let pool = self.pool.clone();
        let manager = self.manager.clone();
        let readers = self.readers.clone();

        let stream = async_stream::try_stream! {
            // Plan once — single metadata round-trip shared by both
            // audit_upserts and audit_deletes.
            let plan = manager.plan_audit(&pool, readers.clone(), &req)
                .await
                .map_err(api_error_to_status)?;

            // Upsert side: emit each batch as an AuditDataResponse with
            // only the `upserts` field populated. Then the delete side,
            // populating `deletes` only. Clients tolerate either field
            // being empty per the proto contract.
            let upserts = manager.audit_upserts(&pool, readers.clone(), &plan)
                .await
                .map_err(api_error_to_status)?;
            for await item in ipc_response_stream(upserts, |bytes| AuditDataResponse {
                upserts: bytes,
                deletes: Vec::new(),
            }) {
                yield item?;
            }

            let deletes = manager.audit_deletes(&pool, readers, &plan)
                .await
                .map_err(api_error_to_status)?;
            for await item in ipc_response_stream(deletes, |bytes| AuditDataResponse {
                upserts: Vec::new(),
                deletes: bytes,
            }) {
                yield item?;
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
        ),
    )]
    async fn get_max_commit_seq_num(
        &self,
        request: Request<GetMaxCommitSeqNumRequest>,
    ) -> Result<Response<GetMaxCommitSeqNumResponse>, Status> {
        crate::validation::validate_get_max_commit_seq_num(request.get_ref())?;
        let req = request.into_inner();
        let catalog_uuid = req
            .catalog_uuid
            .as_deref()
            .ok_or_else(|| Status::invalid_argument("catalog_uuid is required"))?;
        let branch_uuid = req
            .branch_uuid
            .as_deref()
            .ok_or_else(|| Status::invalid_argument("branch_uuid is required"))?;
        let max_commit_seq_num = self
            .manager
            .get_max_commit_seq_num(&self.pool, catalog_uuid, branch_uuid)
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(GetMaxCommitSeqNumResponse {
            max_commit_seq_num,
        }))
    }
}
