//! The persist loop — hot→cold memory relief on the SHORT cadence.
//!
//! ```text
//! every SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS:
//!   for each catalog (ListCatalogs):
//!     for each branch in catalog (ListBranches):
//!       PersistBranch(catalog, branch)
//! ```
//!
//! Stateless by construction: `PersistBranch` resolves its own dirty set
//! server-side, so unlike [`crate::snapshot_loop`] there is no enumeration
//! window to tile and no watermark to carry.

use std::time::Duration;

use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use penca_proto::external::v1::{Branch, BranchOpRequest, Catalog};
use tonic::transport::Channel;

use crate::{SchedulerError, discovery};

/// Persists every branch's modified tables on its own cadence, independent of
/// [`crate::snapshot_loop::SnapshotLoop`].
pub struct PersistLoop {
    query: QueryServiceClient<Channel>,
    lifecycle: LifecycleServiceClient<Channel>,
    /// `None` disables this loop alone — the snapshot loop keeps running.
    tick_interval: Option<Duration>,
    list_page_size: i32,
}

impl PersistLoop {
    pub fn new(
        query: QueryServiceClient<Channel>,
        lifecycle: LifecycleServiceClient<Channel>,
        tick_interval: Option<Duration>,
        list_page_size: i32,
    ) -> Self {
        Self {
            query,
            lifecycle,
            tick_interval,
            list_page_size,
        }
    }

    /// Run forever: `tick` + `sleep(tick_interval)`. Sleeping between ticks
    /// (rather than `tokio::time::interval`) keeps the documented "time between
    /// the END of one tick and the START of the next" contract — a fixed-rate
    /// timer would queue catch-up ticks after a slow sweep.
    pub async fn run(mut self) {
        let Some(interval) = self.tick_interval else {
            tracing::warn!(
                env_var = "SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS",
                "persist loop disabled: configured cadence is non-positive, \
                 no Persist will fire"
            );
            std::future::pending::<()>().await;
            return;
        };
        loop {
            let _ = self.tick().await;
            tokio::time::sleep(interval).await;
        }
    }

    /// One persist sweep over every `(catalog, branch)`.
    #[tracing::instrument(name = "persist_tick", skip_all, err)]
    async fn tick(&mut self) -> Result<(), SchedulerError> {
        let catalogs = discovery::list_all_catalogs(&mut self.query, self.list_page_size).await?;
        for catalog in catalogs {
            let branches = discovery::list_all_branches(
                &mut self.query,
                self.list_page_size,
                &catalog.catalog_uuid,
            )
            .await?;
            for branch in branches {
                self.persist_branch(&catalog, &branch).await;
            }
        }
        Ok(())
    }

    /// `PersistBranch` is continue-on-error per table: a poison table is logged
    /// and skipped, so it cannot starve the rest of the branch. That matters
    /// because the dirty set is enumerated `ORDER BY MAX(modified_at_micros)
    /// ASC` — a table whose Persist keeps failing never advances its timestamp
    /// and would otherwise sort first, and abort the sweep, on every tick.
    ///
    /// It signals partial completion by withholding the watermark rather than
    /// returning an error, so both arms are logged here: a transport error means
    /// the whole call failed, an absent watermark means some table did.
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %catalog.catalog_uuid,
            branch = %branch.branch_uuid,
        ),
    )]
    async fn persist_branch(&mut self, catalog: &Catalog, branch: &Branch) {
        match self
            .lifecycle
            .persist_branch(BranchOpRequest {
                catalog_uuid: Some(catalog.catalog_uuid.clone()),
                branch_uuid: Some(branch.branch_uuid.clone()),
                ..Default::default()
            })
            .await
        {
            Err(e) => tracing::warn!(
                error = %e,
                "PersistBranch failed; will retry next tick"
            ),
            Ok(resp) => {
                if resp.get_ref().watermark.is_none() {
                    tracing::warn!(
                        "PersistBranch incomplete: at least one table failed; \
                         see the lifecycle service log for which"
                    );
                }
            }
        }
    }
}
