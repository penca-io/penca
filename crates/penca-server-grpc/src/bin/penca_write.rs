use std::sync::Arc;
use std::time::Duration;

use penca_api::{QueryManager, WriteManager};
use penca_db::driver::pg::PgDriver;
use penca_dl::build_cold_session_template;
use penca_dl::cache::SegmentCache;
use penca_dl::driver::DatafusionDlDriver;
use penca_dl::list_cache::SnapshotListCache;
use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use penca_proto::external::v1::write_service_server::WriteServiceServer;
use penca_server_grpc::config::{ObjectStorageConfig, WriteServiceConfig};
use penca_server_grpc::write::WriteServiceImpl;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    penca_observability::init_tracing();

    let config = WriteServiceConfig::from_env();
    let storage_config = ObjectStorageConfig::from_env();
    let bind_addr = config.bind_addr.parse()?;
    let pool =
        PgDriver::connect(&config.database_url, config.pg_pool_min, config.pg_pool_max).await?;
    let readers = Arc::new(storage_config.build_readers().await);
    let writer = Arc::new(storage_config.build_writer().await);
    // CHA-421: cold-session template for the metadata driver below.
    let session_template = Arc::new(build_cold_session_template());
    // CHA-472 (IMPL-3): the write path now serves cached metadata reads. One
    // process-lifetime snapshot-segment cache, shared into the metadata driver
    // below AND the write manager's QueryManager, plus a snapshot-list cache so
    // a hot autocommit point-write resolves its target identifier from cache
    // (the same caches the query service runs — ADR 0028). Soundness: the list
    // cache holds only the immutable cold snapshot baseline (CHA-492: keyed on
    // the resolved snapshot's `W_snap`, so an entry is valid for exactly the
    // snapshot version it addresses); the hot change-log is always read fresh,
    // so an open tx's own writes are never served stale from the list cache.
    let snapshot_cache = Arc::new(SegmentCache::new(
        config.snapshot_segment_cache_budget_bytes as u64,
    ));
    let snapshot_list_cache = Arc::new(SnapshotListCache::new(
        Duration::from_secs(config.snapshot_list_cache_ttl_seconds as u64),
        config.snapshot_list_cache_max_entries as u64,
    ));
    let dl_driver = Arc::new(DatafusionDlDriver::new(
        readers.clone(),
        snapshot_cache.clone(),
        session_template.clone(),
    ));
    let manager = WriteManager {
        default_tx_timeout_seconds: config.default_tx_timeout_seconds,
        query_manager: QueryManager::for_metadata_reads(
            session_template,
            snapshot_cache,
            snapshot_list_cache,
        ),
    };

    // CHA-273 rework: the source-branch hot→cold flush at CreateBranch runs in
    // the lifecycle pod now — the write path calls PersistBranch over gRPC rather
    // than persisting inline. Mirror the scheduler's channel build.
    let lifecycle_channel = Channel::from_shared(config.lifecycle_addr.clone())?
        .connect()
        .await?;
    let lifecycle_client = LifecycleServiceClient::new(lifecycle_channel);

    let service = WriteServiceImpl {
        pool,
        dl_driver,
        writer,
        readers,
        manager,
        lifecycle_client,
        max_tx_timeout_seconds: config.max_tx_timeout_seconds,
    };

    tracing::info!(bind_addr = %bind_addr, "penca-write listening");
    // TODO(CHA-136): Disabling encode/decode size limits as a stop-gap
    // so wide-schema responses don't trip gRPC's default 4 MiB cap.
    // Real fix is to chunk batches by `default_stream_batch_size`
    // before yielding; restore the default once that lands.
    tonic::transport::Server::builder()
        .layer(penca_server_grpc::server::trace_layer())
        .add_service(
            WriteServiceServer::new(service)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
        .serve(bind_addr)
        .await?;

    Ok(())
}
