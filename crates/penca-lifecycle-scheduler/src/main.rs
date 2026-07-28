use penca_lifecycle_scheduler::{Scheduler, SchedulerConfig};
use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    penca_observability::init_tracing();
    let config = SchedulerConfig::from_env();

    let query_channel = Channel::from_shared(config.query_addr.clone())?
        .connect()
        .await?;
    let lifecycle_channel = Channel::from_shared(config.lifecycle_addr.clone())?
        .connect()
        .await?;

    let query = QueryServiceClient::new(query_channel);
    let lifecycle = LifecycleServiceClient::new(lifecycle_channel);

    // TODO(CHA-513): the single tick loop still drives Persist + Snapshot +
    // Purge together, so it runs at the persist (shorter) cadence — the sweep
    // that must not fall behind. The snapshot cadence is parsed and reported but
    // unused until the two-loop split consumes it.
    let mut scheduler = Scheduler::new(
        query,
        lifecycle,
        config.persist_tick_interval(),
        config.list_page_size(),
        config.grace_window_micros(),
    );

    tracing::info!(
        persist_tick_interval_seconds = config.persist_tick_interval_seconds,
        snapshot_tick_interval_seconds = config.snapshot_tick_interval_seconds,
        grace_window_seconds = config.query_timeout_seconds,
        list_page_size = config.list_page_size,
        "penca-lifecycle-scheduler starting"
    );
    scheduler.run().await
}
