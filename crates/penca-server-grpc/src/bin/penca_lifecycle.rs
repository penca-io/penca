// CHA-507: persist_locked (generic over L, W) now calls the generic-over-W
// persist_tx_log to flush the cold tx_log before making segments visible. The
// extra layer of nested generic async futures pushes trait-solver query depth
// past the default 128 when this binary monomorphizes the full lifecycle call
// graph. Bump the limit for this crate root.
#![recursion_limit = "256"]

use std::sync::Arc;

use penca_api::{LifecycleManager, QueryManager};
use penca_db::driver::pg::PgDriver;
use penca_dl::build_cold_session_template;
use penca_dl::cache::SegmentCache;
use penca_dl::driver::DatafusionDlDriver;
use penca_dl::list_cache::SnapshotListCache;
use penca_proto::external::v1::lifecycle_service_server::LifecycleServiceServer;
use penca_server_grpc::config::{LifecycleServiceConfig, ObjectStorageConfig};
use penca_server_grpc::lifecycle::LifecycleServiceImpl;
use penca_storage_hot::HotStorageClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    penca_observability::init_tracing();

    let config = LifecycleServiceConfig::from_env();
    let storage_config = ObjectStorageConfig::from_env();
    let bind_addr = config.bind_addr.parse()?;
    let pool =
        PgDriver::connect(&config.database_url, config.pg_pool_min, config.pg_pool_max).await?;
    let readers = Arc::new(storage_config.build_readers().await);
    let writer = Arc::new(storage_config.build_writer().await);
    // CHA-421: cold-session template for the metadata driver below.
    let session_template = Arc::new(build_cold_session_template());
    // Lifecycle never serves cached snapshot reads — disabled cache (CHA-252).
    let dl_driver = Arc::new(DatafusionDlDriver::new(
        readers.clone(),
        Arc::new(SegmentCache::disabled()),
        session_template.clone(),
    ));
    let hot = HotStorageClient;
    let manager = LifecycleManager {
        base_uri: storage_config.base_uri(),
        storage_format: storage_config.format,
        max_segment_bytes: config.default_max_segment_bytes,
        segment_read_concurrency: config.segment_read_concurrency.get() as usize,
        query_timeout_micros: config.query_timeout_seconds * 1_000_000,
        hot_purge_grace_micros: config.hot_purge_grace_seconds * 1_000_000,
        // A disabled scheduler (negative tick) contributes no sweep floor.
        purge_sweep_interval_micros: config.scheduler_tick_interval_seconds.max(0) * 1_000_000,
        // CHA-472: the rehomed by-branch metadata reads are QueryManager methods
        // now; lifecycle reaches them through a metadata-reader handle (disabled
        // caches — lifecycle always reads fresh).
        query_manager: QueryManager::for_metadata_reads(
            session_template,
            Arc::new(SegmentCache::disabled()),
            Arc::new(SnapshotListCache::disabled()),
        ),
    };

    let service = LifecycleServiceImpl {
        pool,
        hot,
        readers,
        dl_driver,
        writer,
        manager,
    };

    tracing::info!(bind_addr = %bind_addr, "penca-lifecycle listening");
    // TODO(CHA-136): Disabling encode/decode size limits as a stop-gap
    // so wide-schema responses don't trip gRPC's default 4 MiB cap.
    // Real fix is to chunk batches by `default_stream_batch_size`
    // before yielding; restore the default once that lands.
    tonic::transport::Server::builder()
        .layer(penca_server_grpc::server::trace_layer())
        .add_service(
            LifecycleServiceServer::new(service)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
        .serve(bind_addr)
        .await?;

    Ok(())
}
