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

    let mut scheduler = Scheduler::new(
        query,
        lifecycle,
        config.tick_interval(),
        config.list_page_size(),
        config.grace_window_micros(),
    );

    tracing::info!(
        tick_interval_seconds = config.tick_interval_seconds,
        grace_window_seconds = config.query_timeout_seconds,
        list_page_size = config.list_page_size,
        "penca-lifecycle-scheduler starting"
    );
    scheduler.run().await
}
