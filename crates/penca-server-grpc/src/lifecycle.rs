//! LifecycleService gRPC server implementation.

use std::collections::HashMap;
use std::sync::Arc;

use penca_api::LifecycleManager;
use penca_db::driver::pg::PgDriver;
use penca_dl::driver::DlDriver;
use penca_format::reader::FormatReader;
use penca_format::writer::FormatWriter;
use penca_proto::external::v1::lifecycle_service_server::LifecycleService;
use penca_proto::external::v1::{
    BranchOpRequest, BranchOpResponse, CompactPersistSegmentsRequest,
    CompactPersistSegmentsResponse, ListModifiedTablesRequest, ListModifiedTablesResponse,
    ListPersistedTablesRequest, ListPersistedTablesResponse, PersistRequest, PersistResponse,
    PurgeRequest, PurgeResponse, PurgeTxLogRequest, PurgeTxLogResponse, SnapshotRequest,
    SnapshotResponse, SweepSegmentsRequest, SweepSegmentsResponse,
};
use penca_storage_hot::HotStorageClient;
use tonic::{Request, Response, Status};

use crate::status::api_error_to_status;

pub struct LifecycleServiceImpl<R: FormatReader, L: DlDriver + ?Sized, W: FormatWriter> {
    pub pool: PgDriver,
    pub hot: HotStorageClient,
    pub readers: Arc<HashMap<i32, R>>,
    pub dl_driver: Arc<L>,
    pub writer: Arc<W>,
    pub manager: LifecycleManager,
}

#[tonic::async_trait]
impl<R, L, W> LifecycleService for LifecycleServiceImpl<R, L, W>
where
    R: FormatReader + 'static,
    L: DlDriver + ?Sized + Send + Sync + 'static,
    W: FormatWriter + 'static,
{
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
            table = ?request.get_ref().table_uuid,
            target_micros = ?request.get_ref().target_micros,
        ),
    )]
    async fn persist(
        &self,
        request: Request<PersistRequest>,
    ) -> Result<Response<PersistResponse>, Status> {
        crate::validation::validate_persist(request.get_ref())?;
        let resp = self
            .manager
            .persist(
                &self.pool,
                &self.hot,
                self.dl_driver.as_ref(),
                self.writer.as_ref(),
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
            table = ?request.get_ref().table_uuid,
        ),
    )]
    async fn purge(
        &self,
        request: Request<PurgeRequest>,
    ) -> Result<Response<PurgeResponse>, Status> {
        crate::validation::validate_purge(request.get_ref())?;
        let resp = self
            .manager
            .purge(&self.pool, self.dl_driver.as_ref(), request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
            table = ?request.get_ref().table_uuid,
        ),
    )]
    async fn compact_persist_segments(
        &self,
        request: Request<CompactPersistSegmentsRequest>,
    ) -> Result<Response<CompactPersistSegmentsResponse>, Status> {
        crate::validation::validate_compact_persist_segments(request.get_ref())?;
        let resp = self
            .manager
            .compact_persist_segments(
                &self.pool,
                self.dl_driver.as_ref(),
                self.readers.as_ref(),
                self.writer.as_ref(),
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
            table = ?request.get_ref().table_uuid,
            snapshotted_at_micros = ?request.get_ref().snapshotted_at_micros,
        ),
    )]
    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        crate::validation::validate_snapshot(request.get_ref())?;
        let resp = self
            .manager
            .snapshot(
                &self.pool,
                self.readers.as_ref(),
                self.dl_driver.as_ref(),
                self.writer.as_ref(),
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
        ),
    )]
    async fn sweep_segments(
        &self,
        request: Request<SweepSegmentsRequest>,
    ) -> Result<Response<SweepSegmentsResponse>, Status> {
        crate::validation::validate_sweep_segments(request.get_ref())?;
        let resp = self
            .manager
            .sweep_segments(&self.pool, self.writer.as_ref(), request.get_ref())
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
    async fn purge_tx_log(
        &self,
        request: Request<PurgeTxLogRequest>,
    ) -> Result<Response<PurgeTxLogResponse>, Status> {
        crate::validation::validate_purge_tx_log(request.get_ref())?;
        let resp = self
            .manager
            .purge_tx_log(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %request.get_ref().catalog_uuid,
            branch = %request.get_ref().branch_uuid,
        ),
    )]
    async fn list_modified_tables(
        &self,
        request: Request<ListModifiedTablesRequest>,
    ) -> Result<Response<ListModifiedTablesResponse>, Status> {
        crate::validation::validate_list_modified_tables(request.get_ref())?;
        let resp = self
            .manager
            .list_modified_tables(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
            target = ?request.get_ref().target,
        ),
    )]
    async fn persist_branch(
        &self,
        request: Request<BranchOpRequest>,
    ) -> Result<Response<BranchOpResponse>, Status> {
        crate::validation::validate_branch_op(request.get_ref())?;
        let watermark = self
            .manager
            .persist_branch(
                &self.pool,
                &self.hot,
                self.dl_driver.as_ref(),
                self.writer.as_ref(),
                request.get_ref(),
            )
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(BranchOpResponse { watermark }))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
            target = ?request.get_ref().target,
        ),
    )]
    async fn snapshot_branch(
        &self,
        request: Request<BranchOpRequest>,
    ) -> Result<Response<BranchOpResponse>, Status> {
        crate::validation::validate_branch_op(request.get_ref())?;
        let watermark = self
            .manager
            .snapshot_branch(
                &self.pool,
                self.readers.as_ref(),
                self.dl_driver.as_ref(),
                self.writer.as_ref(),
                request.get_ref(),
            )
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(BranchOpResponse { watermark }))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = ?request.get_ref().catalog_uuid,
            branch = ?request.get_ref().branch_uuid,
            target = ?request.get_ref().target,
        ),
    )]
    async fn persist_and_snapshot_branch(
        &self,
        request: Request<BranchOpRequest>,
    ) -> Result<Response<BranchOpResponse>, Status> {
        crate::validation::validate_branch_op(request.get_ref())?;
        let watermark = self
            .manager
            .persist_and_snapshot_branch(
                &self.pool,
                &self.hot,
                self.readers.as_ref(),
                self.dl_driver.as_ref(),
                self.writer.as_ref(),
                request.get_ref(),
            )
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(BranchOpResponse { watermark }))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %request.get_ref().catalog_uuid,
            branch = %request.get_ref().branch_uuid,
        ),
    )]
    async fn list_persisted_tables(
        &self,
        request: Request<ListPersistedTablesRequest>,
    ) -> Result<Response<ListPersistedTablesResponse>, Status> {
        crate::validation::validate_list_persisted_tables(request.get_ref())?;
        let resp = self
            .manager
            .list_persisted_tables(&self.pool, request.get_ref())
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(resp))
    }
}
