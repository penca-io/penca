//! WriteService gRPC server implementation.

use std::collections::HashMap;
use std::sync::Arc;

use penca_api::WriteManager;
use penca_db::driver::pg::PgDriver;
use penca_dl::driver::DlDriver;
use penca_format::reader::FormatReader;
use penca_format::writer::FormatWriter;
use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use penca_proto::external::v1::write_service_server::WriteService;
use penca_proto::external::v1::{
    AbortTxRequest, AbortTxResponse, BeginTxRequest, BeginTxResponse, BranchOpRequest,
    CommitTxRequest, CommitTxResponse, CreateBranchRequest, CreateBranchResponse,
    CreateCatalogRequest, CreateCatalogResponse, CreateIndexRequest, CreateIndexResponse,
    CreateSchemaRequest, CreateSchemaResponse, CreateTableRequest, CreateTableResponse,
    DeleteBranchRequest, DeleteBranchResponse, DeleteCatalogRequest, DeleteCatalogResponse,
    DeleteIndexRequest, DeleteIndexResponse, DeleteSchemaRequest, DeleteSchemaResponse,
    DeleteTableRequest, DeleteTableResponse, MergeBranchRequest, MergeBranchResponse,
    UpdateBranchRequest, UpdateBranchResponse, UpdateCatalogRequest, UpdateCatalogResponse,
    UpdateIndexRequest, UpdateIndexResponse, UpdateSchemaRequest, UpdateSchemaResponse,
    UpdateTableRequest, UpdateTableResponse, WriteDataRequest, WriteDataResponse,
};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::status::api_error_to_status;

pub struct WriteServiceImpl<L: DlDriver + ?Sized, W: FormatWriter, R: FormatReader> {
    pub pool: PgDriver,
    pub dl_driver: Arc<L>,
    pub writer: Arc<W>,
    /// CHA-507: cold-tier readers so `create_branch` can resolve a fork
    /// position from the durable cold tx_log when its hot commit_tx_log row
    /// has been purged. Shares the same `Arc` handed to the metadata driver.
    pub readers: Arc<HashMap<i32, R>>,
    pub manager: WriteManager,
    /// CHA-273 rework: the lifecycle service the `create_branch` handler calls to
    /// flush the source branch hot→cold (PersistBranch) before recording the
    /// fork. Cloned per call — a tonic client clone shares the channel.
    pub lifecycle_client: LifecycleServiceClient<Channel>,
    /// Server-configured ceiling on `BeginTx.timeout_seconds` (CHA-92).
    pub max_tx_timeout_seconds: i64,
}

#[tonic::async_trait]
impl<L, W, R> WriteService for WriteServiceImpl<L, W, R>
where
    L: DlDriver + ?Sized + Send + Sync + 'static,
    W: FormatWriter + 'static,
    R: FormatReader + 'static,
{
    #[tracing::instrument(
        skip_all,
        fields(catalog_name = %request.get_ref().catalog_name),
    )]
    async fn create_catalog(
        &self,
        request: Request<CreateCatalogRequest>,
    ) -> Result<Response<CreateCatalogResponse>, Status> {
        crate::validation::validate_create_catalog(request.get_ref())?;
        let resp = self
            .manager
            .create_catalog(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            new_name = ?request.get_ref().new_catalog_name,
        ),
    )]
    async fn update_catalog(
        &self,
        request: Request<UpdateCatalogRequest>,
    ) -> Result<Response<UpdateCatalogResponse>, Status> {
        crate::validation::validate_update_catalog(request.get_ref())?;
        let resp = self
            .manager
            .update_catalog(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(catalog = ?request.get_ref().catalog_uuid),
    )]
    async fn delete_catalog(
        &self,
        request: Request<DeleteCatalogRequest>,
    ) -> Result<Response<DeleteCatalogResponse>, Status> {
        crate::validation::validate_delete_catalog(request.get_ref())?;
        let resp = self
            .manager
            .delete_catalog(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch_name = %request.get_ref().branch_name,
            fork_point = ?request.get_ref().fork_point,
            source_branch = ?request.get_ref().source_branch_uuid,
            requested_branch_uuid = ?request.get_ref().branch_uuid,
        ),
    )]
    async fn create_branch(
        &self,
        request: Request<CreateBranchRequest>,
    ) -> Result<Response<CreateBranchResponse>, Status> {
        crate::validation::validate_create_branch(request.get_ref())?;
        let req = request.get_ref();
        // CHA-515: reject non-main forks before any work touches the source.
        // PersistBranch below mutates the source (hot→cold flush), so this
        // guard must run first for the rejection to be a true no-op.
        self.manager
            .ensure_fork_source_is_main(&self.pool, req)
            .await
            .map_err(api_error_to_status)?;
        // Resolve the fork position (the request's fork_point → Watermark) in
        // the write pod — a position that names no committed tx is a hard
        // INVALID_ARGUMENT — then synchronously flush the SOURCE branch hot→cold
        // up to that position via PersistBranch (the persist loop runs in the
        // lifecycle pod), then record the fork.
        //
        // PersistBranch is continue-on-error per table for the scheduler's sake,
        // and signals a partial flush by withholding the watermark. CreateBranch
        // needs all-or-nothing: the child reads the parent's COLD tier, so a fork
        // recorded over a partial flush would silently serve a child that is
        // missing the unflushed tables' rows. An absent watermark must fail the
        // fork.
        let fork = self
            .manager
            .resolve_fork_watermark(&self.pool, self.readers.as_ref(), req)
            .await
            .map_err(api_error_to_status)?;
        self.lifecycle_client
            .clone()
            .persist_branch(BranchOpRequest {
                catalog_uuid: req.catalog_uuid.clone(),
                catalog_name: req.catalog_name.clone(),
                branch_uuid: req.source_branch_uuid.clone(),
                branch_name: req.source_branch_name.clone(),
                target: Some(fork),
            })
            .await?
            .into_inner()
            .watermark
            .ok_or_else(|| {
                // Echo whichever identification the caller supplied — CreateBranch
                // accepts either UUIDs or names, and a placeholder would be
                // useless in exactly the name-based case.
                let branch = req
                    .source_branch_uuid
                    .as_deref()
                    .or(req.source_branch_name.as_deref())
                    .unwrap_or("<unidentified>");
                let catalog = req
                    .catalog_uuid
                    .as_deref()
                    .or(req.catalog_name.as_deref())
                    .unwrap_or("<unidentified>");
                Status::internal(format!(
                    "CreateBranch aborted: source branch {branch} in catalog \
                     {catalog} could not be fully flushed hot→cold. The lifecycle \
                     service logged one warn per failing table, keyed by the \
                     resolved catalog and branch UUIDs."
                ))
            })?;
        let resp = self
            .manager
            .create_branch(&self.pool, self.dl_driver.as_ref(), req, &fork)
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
    async fn delete_branch(
        &self,
        request: Request<DeleteBranchRequest>,
    ) -> Result<Response<DeleteBranchResponse>, Status> {
        crate::validation::validate_delete_branch(request.get_ref())?;
        let resp = self
            .manager
            .delete_branch(
                &self.pool,
                self.dl_driver.as_ref(),
                &*self.writer,
                request.get_ref(),
            )
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
            new_name = ?request.get_ref().new_branch_name,
        ),
    )]
    async fn update_branch(
        &self,
        request: Request<UpdateBranchRequest>,
    ) -> Result<Response<UpdateBranchResponse>, Status> {
        crate::validation::validate_update_branch(request.get_ref())?;
        let resp = self
            .manager
            .update_branch(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            source_branch = ?request.get_ref().source_branch_uuid,
            target_branch = ?request.get_ref().target_branch_uuid,
        ),
    )]
    async fn merge_branch(
        &self,
        request: Request<MergeBranchRequest>,
    ) -> Result<Response<MergeBranchResponse>, Status> {
        crate::validation::validate_merge_branch(request.get_ref())?;
        let resp = self
            .manager
            .merge_branch(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            schema_name = %request.get_ref().schema_name,
        ),
    )]
    async fn create_schema(
        &self,
        request: Request<CreateSchemaRequest>,
    ) -> Result<Response<CreateSchemaResponse>, Status> {
        crate::validation::validate_create_schema(request.get_ref())?;
        let resp = self
            .manager
            .create_schema(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            schema = ?request.get_ref().schema_uuid,
            new_name = ?request.get_ref().new_schema_name,
        ),
    )]
    async fn update_schema(
        &self,
        request: Request<UpdateSchemaRequest>,
    ) -> Result<Response<UpdateSchemaResponse>, Status> {
        crate::validation::validate_update_schema(request.get_ref())?;
        let resp = self
            .manager
            .update_schema(&self.pool, self.dl_driver.as_ref(), request.get_ref())
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
    async fn delete_schema(
        &self,
        request: Request<DeleteSchemaRequest>,
    ) -> Result<Response<DeleteSchemaResponse>, Status> {
        crate::validation::validate_delete_schema(request.get_ref())?;
        let resp = self
            .manager
            .delete_schema(&self.pool, self.dl_driver.as_ref(), request.get_ref())
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
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        crate::validation::validate_create_table(request.get_ref())?;
        let resp = self
            .manager
            .create_table(&self.pool, self.dl_driver.as_ref(), request.get_ref())
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
            new_name = ?request.get_ref().new_table_name,
        ),
    )]
    async fn update_table(
        &self,
        request: Request<UpdateTableRequest>,
    ) -> Result<Response<UpdateTableResponse>, Status> {
        crate::validation::validate_update_table(request.get_ref())?;
        let resp = self
            .manager
            .update_table(&self.pool, self.dl_driver.as_ref(), request.get_ref())
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
    async fn delete_table(
        &self,
        request: Request<DeleteTableRequest>,
    ) -> Result<Response<DeleteTableResponse>, Status> {
        crate::validation::validate_delete_table(request.get_ref())?;
        let resp = self
            .manager
            .delete_table(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        crate::validation::validate_create_index(request.get_ref())?;
        let resp = self
            .manager
            .create_index(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    async fn update_index(
        &self,
        request: Request<UpdateIndexRequest>,
    ) -> Result<Response<UpdateIndexResponse>, Status> {
        crate::validation::validate_update_index(request.get_ref())?;
        let resp = self
            .manager
            .update_index(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    async fn delete_index(
        &self,
        request: Request<DeleteIndexRequest>,
    ) -> Result<Response<DeleteIndexResponse>, Status> {
        crate::validation::validate_delete_index(request.get_ref())?;
        let resp = self
            .manager
            .delete_index(&self.pool, self.dl_driver.as_ref(), request.get_ref())
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
    async fn begin_tx(
        &self,
        request: Request<BeginTxRequest>,
    ) -> Result<Response<BeginTxResponse>, Status> {
        crate::validation::validate_begin_tx(request.get_ref(), self.max_tx_timeout_seconds)?;
        let resp = self
            .manager
            .begin_tx(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
            tx = %request.get_ref().tx_uuid,
        ),
    )]
    async fn commit_tx(
        &self,
        request: Request<CommitTxRequest>,
    ) -> Result<Response<CommitTxResponse>, Status> {
        crate::validation::validate_commit_tx(request.get_ref())?;
        let resp = self
            .manager
            .commit_tx(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
            tx = %request.get_ref().tx_uuid,
        ),
    )]
    async fn abort_tx(
        &self,
        request: Request<AbortTxRequest>,
    ) -> Result<Response<AbortTxResponse>, Status> {
        crate::validation::validate_abort_tx(request.get_ref())?;
        let resp = self
            .manager
            .abort_tx(&self.pool, request.get_ref())
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
            tx = ?request.get_ref().tx_uuid,
        ),
    )]
    async fn write_data(
        &self,
        request: Request<WriteDataRequest>,
    ) -> Result<Response<WriteDataResponse>, Status> {
        crate::validation::validate_write_data(request.get_ref())?;
        let resp = self
            .manager
            .write_data(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }
}
