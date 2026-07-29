//! PurgeTxLog: branch-wide GC for the four hot tx-log family tables.

use penca_db::driver::pg::PgDriver;
use penca_proto::external::v1::{PurgeTxLogRequest, PurgeTxLogResponse};
use penca_storage_meta::watermarks::compute_purge_tx_log_cutoffs;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;
use crate::resolve::{parse_resolved_uuid, resolve_branch, resolve_catalog};

impl LifecycleManager {
    /// GC the four hot tx-log family tables (`commit_tx_log` / `tx_table_log`
    /// / `abort_tx_log` / `begin_tx_log`) for a branch, off the per-table
    /// purge seq watermarks (ADR 0027).
    ///
    /// The cutoffs are branch-MINs over the settled tables: `MIN(Pu)` on the
    /// committed axis, `MIN(Pa)` on the aborted axis, and a `None` on either
    /// axis (some table unpurged there) blocks that axis entirely. The DELETE
    /// still runs — its expired-begin and pure-begin+abort branches apply even
    /// with both seq cutoffs `None` — and PG's `WITH ... DELETE` snapshot
    /// semantics make the four sub-DELETEs agree on one pre-statement view.
    ///
    /// Branch-scoped advisory lock `purge_tx_log:{branch_uuid}` serializes
    /// concurrent `PurgeTxLog` passes (orthogonal to the per-table Persist /
    /// Snapshot / Purge keys, ADR 0019 §"Lock scoping").
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn purge_tx_log(
        &self,
        pool: &PgDriver,
        request: &PurgeTxLogRequest,
    ) -> Result<PurgeTxLogResponse, ApiError> {
        let catalog = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.branch_uuid.as_deref(),
            request.branch_name.as_deref(),
        )
        .await?;
        let branch_uuid = parse_resolved_uuid(&branch.branch_uuid, "branch_uuid")?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));

        let lock_key = format!("purge_tx_log:{branch_uuid}");
        pool.advisory_lock(&lock_key, async || {
            self.purge_tx_log_locked(pool, catalog_uuid, branch_uuid)
                .await
        })
        .await
    }

    async fn purge_tx_log_locked(
        &self,
        pool: &PgDriver,
        catalog_uuid: Uuid,
        branch_uuid: Uuid,
    ) -> Result<PurgeTxLogResponse, ApiError> {
        // PG's clock, the same source `persist_locked` / `purge_locked` use.
        // Threaded into the as-of filter below so Purges committing during this
        // orchestration are invisible here (ADR 0021 §"Long-cleanup-race").
        let cleanup_started_at_micros =
            penca_storage_meta::LifecycleManager::now_micros(pool).await?;

        let rows = penca_storage_meta::LifecycleManager::tx_table_log_purge_watermarks_for_branch(
            pool,
            &catalog_uuid,
            &branch_uuid,
            cleanup_started_at_micros,
        )
        .await?;

        let cutoffs = compute_purge_tx_log_cutoffs(&rows);

        // Never GC a `commit_tx_log` row whose `commit_seq_num` is not yet
        // durable in the cold tx_log, so clamp the committed cutoff to the
        // tx_log persist watermark `W_txlog`. `persist_tx_log` runs first (and
        // fail-fast) in the branch persist ops, so normally `W_txlog >=` the
        // data-persist watermark and this clamp is non-binding; it binds only
        // when data was persisted past what the tx_log covers, holding those
        // hot rows until their cold copy exists. A branch that never ran
        // `persist_tx_log` (`W_txlog` `None`) is unconstrained.
        let w_txlog = penca_storage_meta::LifecycleManager::tx_log_persist_watermark(
            pool,
            &catalog_uuid.to_string(),
            &branch_uuid.to_string(),
        )
        .await?;
        let committed_cutoff = match (cutoffs.pu_cutoff, w_txlog) {
            (Some(pu), Some(w)) => Some(pu.min(w)),
            (pu, _) => pu,
        };

        // Expired-begin ledger-GC grace (ADR 0027 §5). `begin_tx_log` is the
        // ONLY handle Purge has to enumerate a timed-out tx's (invisible) hot
        // rows (`purge::delete_expired_begin_hot_rows`), so the ledger must not
        // drop until Purge has re-swept the tx's tables — hence the `max`,
        // which floors the grace at the sweep cadence so ≥1 Purge pass has run.
        let expired_begin_grace_micros = self
            .purge_sweep_interval_micros
            .max(self.hot_purge_grace_micros);

        // In-flight open txs are never in `eligible`, so their `begin_tx_log` /
        // `tx_table_log` state survives. See `delete_purge_tx_log_eligible`
        // for the full safety chain.
        penca_storage_meta::LifecycleManager::delete_purge_tx_log_eligible(
            pool,
            &catalog_uuid,
            &branch_uuid,
            committed_cutoff,
            cutoffs.pa_cutoff,
            cleanup_started_at_micros,
            expired_begin_grace_micros,
        )
        .await?;

        // The GC spans two seq axes, so there is no single watermark worth
        // reporting — hence the empty response.
        Ok(PurgeTxLogResponse {})
    }
}
