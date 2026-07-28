//! The snapshot loop — compaction, Purge and tx-log GC on the LONG cadence.
//!
//! ```text
//! every SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS:
//!   for each catalog (ListCatalogs):
//!     for each branch in catalog (ListBranches):
//!       now = system clock micros
//!
//!       SnapshotBranch(catalog, branch)          // enumerates the PERSISTED set
//!
//!       modified = paginate ListModifiedTables([last_modified_tick, now))
//!       for T in modified: Purge(T)
//!       last_modified_tick[catalog, branch] = now
//!
//!       purge_upper = now - QUERY_TIMEOUT_SECONDS_micros
//!       if purge_upper > last_purge_tick:
//!         persisted = paginate ListPersistedTables([last_purge_tick, purge_upper))
//!         for T in persisted: Purge(T)
//!         last_purge_tick[catalog, branch] = purge_upper
//!
//!       PurgeTxLog(catalog, branch)
//! ```
//!
//! ## Why Purge rides this loop, not the persist loop
//!
//! Purge's committed axis targets `Pu = W_snap`, read from the latest committed
//! *snapshot* watermark behind a strict-advance gate. It therefore cannot
//! advance unless a Snapshot has run: on a fast persist tick it would compute no
//! advance and early-return, costing an RPC and buying nothing.
//!
//! The trade-off, accepted: Purge's other two axes — expired-begin cleanup and
//! abort cleanup — have no dependence on `W_snap` and now reclaim at the
//! snapshot cadence rather than the persist one. Both reclaim invisible garbage
//! (aborted rows and timed-out open txs serve no reads), and ADR 0027 §5 already
//! gives expired-begin ledger GC a wall-clock grace.

use std::collections::HashMap;
use std::time::Duration;

use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use penca_proto::external::v1::{Branch, BranchOpRequest, Catalog, PurgeTxLogRequest};
use tonic::transport::Channel;

use crate::{SchedulerError, discovery, ops, system_now_micros};

/// One in-memory watermark pair per `(catalog_uuid, branch_uuid)`.
///
/// Both belong to this loop: the persist loop enumerates nothing client-side,
/// so it needs no window and carries no state.
#[derive(Default, Clone, Copy)]
struct BranchWatermarks {
    /// Upper bound of the most recent `ListModifiedTables` sweep on this
    /// branch. The next sweep's lower bound is this value, so the half-open
    /// `[last, now)` windows tile with no gap.
    last_modified_tick: i64,
    /// Upper bound of the most recent `ListPersistedTables` sweep on this
    /// branch. The next sweep's lower bound is this value.
    last_purge_tick: i64,
}

/// Snapshots, purges and GCs each branch's tx-log family on its own cadence,
/// independent of [`crate::persist_loop::PersistLoop`].
pub struct SnapshotLoop {
    query: QueryServiceClient<Channel>,
    lifecycle: LifecycleServiceClient<Channel>,
    /// `None` disables this loop alone — the persist loop keeps running.
    tick_interval: Option<Duration>,
    list_page_size: i32,
    /// Universal grace window in micros; matches the lifecycle service's
    /// `query_timeout_micros`. Bounds the Purge enumeration's upper edge at
    /// `now - grace_window`.
    grace_window_micros: i64,
    /// Never persisted — restart resets to zero, making the first post-restart
    /// tick a full sweep. Safe because every lifecycle op is idempotent.
    watermarks: HashMap<(String, String), BranchWatermarks>,
}

impl SnapshotLoop {
    pub fn new(
        query: QueryServiceClient<Channel>,
        lifecycle: LifecycleServiceClient<Channel>,
        tick_interval: Option<Duration>,
        list_page_size: i32,
        grace_window_micros: i64,
    ) -> Self {
        Self {
            query,
            lifecycle,
            tick_interval,
            list_page_size,
            grace_window_micros,
            watermarks: HashMap::new(),
        }
    }

    /// Run forever: `tick` + `sleep(tick_interval)`. See
    /// [`crate::persist_loop::PersistLoop::run`] for why this sleeps rather
    /// than using a fixed-rate timer.
    pub async fn run(mut self) {
        let Some(interval) = self.tick_interval else {
            tracing::warn!(
                env_var = "SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS",
                "snapshot loop disabled: configured cadence is non-positive, \
                 no Snapshot, Purge or PurgeTxLog will fire"
            );
            std::future::pending::<()>().await;
            return;
        };
        loop {
            let _ = self.tick().await;
            tokio::time::sleep(interval).await;
        }
    }

    /// One snapshot/purge sweep over every `(catalog, branch)`.
    #[tracing::instrument(skip_all, err)]
    pub async fn tick(&mut self) -> Result<(), SchedulerError> {
        let catalogs = discovery::list_all_catalogs(&mut self.query, self.list_page_size).await?;
        for catalog in catalogs {
            let branches = discovery::list_all_branches(
                &mut self.query,
                self.list_page_size,
                &catalog.catalog_uuid,
            )
            .await?;
            for branch in branches {
                if let Err(e) = self.tick_branch(&catalog, &branch).await {
                    tracing::warn!(
                        catalog = %catalog.catalog_uuid,
                        branch = %branch.branch_uuid,
                        error = %e,
                        "tick_branch failed; will retry next tick"
                    );
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %catalog.catalog_uuid,
            branch = %branch.branch_uuid,
        ),
    )]
    async fn tick_branch(
        &mut self,
        catalog: &Catalog,
        branch: &Branch,
    ) -> Result<(), SchedulerError> {
        let key = (catalog.catalog_uuid.clone(), branch.branch_uuid.clone());
        let mut wm = self.watermarks.get(&key).copied().unwrap_or_default();
        let now = system_now_micros();

        self.snapshot_branch(catalog, branch).await;
        self.purge_modified(catalog, branch, &mut wm, now).await?;
        self.purge_aged_persisted(catalog, branch, &mut wm, now)
            .await?;
        self.sweep_tx_log(catalog, branch).await;

        self.watermarks.insert(key, wm);
        Ok(())
    }

    /// `SnapshotBranch` enumerates the PERSISTED set server-side (CHA-509), not
    /// the hot-modified set, so a table persisted-then-purged still gets
    /// re-snapshotted.
    ///
    /// Continue-on-error per table like `PersistBranch`, and for the same
    /// starvation reason — the persisted set is enumerated
    /// `ORDER BY MAX(commit_micros) ASC`. Both failure arms are logged, and
    /// either way this pass continues so Purge still runs and the watermarks
    /// still advance.
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %catalog.catalog_uuid,
            branch = %branch.branch_uuid,
        ),
    )]
    async fn snapshot_branch(&mut self, catalog: &Catalog, branch: &Branch) {
        match self
            .lifecycle
            .snapshot_branch(BranchOpRequest {
                catalog_uuid: Some(catalog.catalog_uuid.clone()),
                branch_uuid: Some(branch.branch_uuid.clone()),
                ..Default::default()
            })
            .await
        {
            Err(e) => tracing::warn!(
                catalog = %catalog.catalog_uuid,
                branch = %branch.branch_uuid,
                error = %e,
                "SnapshotBranch failed"
            ),
            Ok(resp) => {
                if resp.get_ref().watermark.is_none() {
                    tracing::warn!(
                        catalog = %catalog.catalog_uuid,
                        branch = %branch.branch_uuid,
                        "SnapshotBranch incomplete: at least one table failed; \
                         see the lifecycle service log for which"
                    );
                }
            }
        }
    }

    /// Purge every table modified in this window (CHA-444 / ADR 0027).
    ///
    /// `list_modified_tables` unions committed AND aborted writers, so this
    /// clears committed hot rows (advancing `Pu` to the just-committed
    /// `W_snap`), aborted hot rows, and expired-begin garbage — including for
    /// aborts-only tables that never reach `list_persisted`. Corrected Purge
    /// needs no grace (MVCC over the early-materialized hot read), so it is safe
    /// here. Stays a client-side per-table loop because Purge is a per-table RPC.
    ///
    /// TODO(CHA-502): move this loop server-side behind a `PurgeBranch` RPC,
    /// symmetric with the branch ops above.
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %catalog.catalog_uuid,
            branch = %branch.branch_uuid,
            window_start = wm.last_modified_tick,
            window_end = now,
        ),
    )]
    async fn purge_modified(
        &mut self,
        catalog: &Catalog,
        branch: &Branch,
        wm: &mut BranchWatermarks,
        now: i64,
    ) -> Result<(), SchedulerError> {
        let modified = discovery::paginate_modified_tables(
            &mut self.lifecycle,
            self.list_page_size,
            &catalog.catalog_uuid,
            &branch.branch_uuid,
            wm.last_modified_tick,
            now,
        )
        .await?;
        tracing::debug!(tables_modified = modified.len(), "purge_modified complete");
        for table_uuid in &modified {
            ops::purge_one(
                &mut self.lifecycle,
                &catalog.catalog_uuid,
                &branch.branch_uuid,
                table_uuid,
            )
            .await;
        }
        wm.last_modified_tick = now;
        Ok(())
    }

    /// Purge tables whose persist has aged past the grace window. A table both
    /// modified and persisted within one tick is purged by
    /// [`Self::purge_modified`] first and again here — the second `purge_one`
    /// re-reads `Pu = W_snap` (plus the `Pa = 0` no-op guard) and early-returns
    /// with no new row, so the overlap is an intentional, idempotent defensive
    /// re-run covering persisted-but-not-modified tables, not a missed dedupe.
    ///
    /// TODO(CHA-502): this needs a windowed `PurgeBranch` variant — its
    /// enumeration is `list_persisted_tables` over the grace window, distinct
    /// from the modified-pass one.
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %catalog.catalog_uuid,
            branch = %branch.branch_uuid,
            window_start = wm.last_purge_tick,
            window_end = tracing::field::Empty,
        ),
    )]
    async fn purge_aged_persisted(
        &mut self,
        catalog: &Catalog,
        branch: &Branch,
        wm: &mut BranchWatermarks,
        now: i64,
    ) -> Result<(), SchedulerError> {
        let purge_upper = now - self.grace_window_micros;
        tracing::Span::current().record("window_end", purge_upper);
        if purge_upper > wm.last_purge_tick {
            let persisted = discovery::paginate_persisted_tables(
                &mut self.lifecycle,
                self.list_page_size,
                &catalog.catalog_uuid,
                &branch.branch_uuid,
                wm.last_purge_tick,
                purge_upper,
            )
            .await?;
            tracing::debug!(
                tables_purged = persisted.len(),
                "purge_aged_persisted complete"
            );
            for table_uuid in &persisted {
                ops::purge_one(
                    &mut self.lifecycle,
                    &catalog.catalog_uuid,
                    &branch.branch_uuid,
                    table_uuid,
                )
                .await;
            }
            wm.last_purge_tick = purge_upper;
        }
        Ok(())
    }

    /// Branch-scoped tx-log family GC (CHA-221). Unconditional per tick — the
    /// RPC's own empty-set fast-path is the no-op gate, so no scheduler-side
    /// watermark is needed (unlike the two above, which guard listing
    /// round-trips). Errors logged and swallowed.
    async fn sweep_tx_log(&mut self, catalog: &Catalog, branch: &Branch) {
        if let Err(e) = self
            .lifecycle
            .purge_tx_log(PurgeTxLogRequest {
                catalog_uuid: Some(catalog.catalog_uuid.clone()),
                branch_uuid: Some(branch.branch_uuid.clone()),
                ..Default::default()
            })
            .await
        {
            tracing::warn!(
                catalog = %catalog.catalog_uuid,
                branch = %branch.branch_uuid,
                error = %e,
                "PurgeTxLog failed"
            );
        }
    }
}
