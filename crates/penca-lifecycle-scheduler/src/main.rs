use penca_lifecycle_scheduler::{PersistLoop, Scheduler, SchedulerConfig, SnapshotLoop};
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

    // Each loop gets its own client pair; tonic clients clone cheaply over one
    // Channel, which is what lets the two run without shared mutable state.
    let scheduler = Scheduler::new(
        PersistLoop::new(
            query.clone(),
            lifecycle.clone(),
            config.persist_tick_interval(),
            config.list_page_size(),
        ),
        SnapshotLoop::new(
            query,
            lifecycle,
            config.snapshot_tick_interval(),
            config.list_page_size(),
            config.grace_window_micros(),
        ),
    );

    // Purge and tx-log GC ride the snapshot loop, so disabling it while persist
    // keeps running leaves hot rows with no reclaimer — and the lifecycle
    // service's ledger-GC floor still credits the persist cadence, so it may drop
    // bookkeeping for rows nothing will clear.
    if config.snapshot_tick_interval().is_none() && config.persist_tick_interval().is_some() {
        tracing::warn!(
            persist_tick_interval_seconds = config.persist_tick_interval_seconds,
            snapshot_tick_interval_seconds = config.snapshot_tick_interval_seconds,
            "snapshot loop disabled while persist runs: no Purge, no tx-log GC, \
             and hot will grow unbounded"
        );
    }

    tracing::info!(
        persist_tick_interval_seconds = config.persist_tick_interval_seconds,
        snapshot_tick_interval_seconds = config.snapshot_tick_interval_seconds,
        grace_window_seconds = config.query_timeout_seconds,
        list_page_size = config.list_page_size,
        "penca-lifecycle-scheduler starting"
    );
    scheduler.run().await
}
