use std::sync::Arc;
use std::time::Duration;

use penca_api::QueryManager;
use penca_db::driver::pg::PgDriver;
use penca_dl::build_cold_session_template;
use penca_dl::cache::SegmentCache;
use penca_dl::driver::DatafusionDlDriver;
use penca_dl::list_cache::SnapshotListCache;
use penca_proto::external::v1::query_service_server::QueryServiceServer;
use penca_server_grpc::config::{ObjectStorageConfig, QueryServiceConfig};
use penca_server_grpc::query::QueryServiceImpl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    penca_observability::init_tracing();

    let config = QueryServiceConfig::from_env();
    let storage_config = ObjectStorageConfig::from_env();
    let bind_addr = config.bind_addr.parse()?;
    let pool =
        PgDriver::connect(&config.database_url, config.pg_pool_min, config.pg_pool_max).await?;
    let readers = Arc::new(storage_config.build_readers().await);
    // CHA-252: one process-lifetime snapshot-segment cache, shared into both
    // the metadata driver below and every per-query driver via the manager.
    let snapshot_cache = Arc::new(SegmentCache::new(
        config.snapshot_segment_cache_budget_bytes as u64,
    ));
    // CHA-441: one process-lifetime snapshot-list cache. Deployment sets the
    // TTL <= min(snapshot interval, QUERY_TIMEOUT_SECONDS grace) so a stale
    // list never outlives the retired snapshot files it names.
    let snapshot_list_cache = Arc::new(SnapshotListCache::new(
        Duration::from_secs(config.snapshot_list_cache_ttl_seconds as u64),
        config.snapshot_list_cache_max_entries as u64,
    ));
    // CHA-421: build the expensive cold-session template once at startup and
    // inject it into the metadata driver below + every per-query driver.
    let session_template = Arc::new(build_cold_session_template());
    // CHA-168: read paths (get_table, list_tables, read_data) call into
    // QueryManager::resolve_table_metadata which routes through
    // stream_merged for hot+cold-tolerant reads of __penca_system__.tables.
    let dl_driver = Arc::new(DatafusionDlDriver::new(
        readers.clone(),
        snapshot_cache.clone(),
        session_template.clone(),
    ));
    let manager = QueryManager {
        default_page_size: config.default_page_size,
        default_stream_batch_size: config.default_stream_batch_size,
        segment_read_concurrency: config.segment_read_concurrency.get() as usize,
        snapshot_prune_min_segments: config.snapshot_prune_min_segments as usize,
        index_seek_max_probe_tuples: config.index_seek_max_probe_tuples as usize,
        query_timeout_micros: config.query_timeout_seconds * 1_000_000,
        snapshot_cache,
        snapshot_list_cache,
        session_template,
    };

    let service = QueryServiceImpl {
        pool,
        dl_driver,
        readers,
        manager,
    };

    tracing::info!(bind_addr = %bind_addr, "penca-query listening");
    // TODO(CHA-136): Disabling encode/decode size limits as a stop-gap
    // so wide-schema ReadData responses don't trip gRPC's default 4 MiB
    // cap. Real fix is to chunk batches by `default_stream_batch_size`
    // before yielding; restore the default once that lands.
    tonic::transport::Server::builder()
        .layer(penca_server_grpc::server::trace_layer())
        .add_service(
            QueryServiceServer::new(service)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
        .serve(bind_addr)
        .await?;

    Ok(())
}
