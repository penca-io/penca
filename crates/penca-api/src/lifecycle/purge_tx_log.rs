//! PurgeTxLog: branch-wide GC for the four hot tx-log family tables
//! (CHA-221).
//!
//! [`LifecycleManager::purge_tx_log`] computes a per-branch eligible
//! cutoff via the as-of-snapshotted per-table purge watermark and
//! executes one composite `WITH eligible AS (...)` DELETE across
//! `commit_tx_log`, `tx_table_log`, `abort_tx_log`, and `begin_tx_log`.

use penca_db::driver::pg::PgDriver;
use penca_proto::external::v1::{PurgeTxLogRequest, PurgeTxLogResponse};
use penca_storage_meta::watermarks::compute_purge_tx_log_cutoffs;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;
use crate::resolve::{parse_resolved_uuid, resolve_branch, resolve_catalog};

impl LifecycleManager {
    /// GC the four hot tx-log family tables (`commit_tx_log` / `tx_table_log`
    /// / `abort_tx_log` / `begin_tx_log`) for a branch (CHA-221;
    /// CHA-444 / ADR 0027 re-axised it onto the purge seq watermarks).
    ///
    /// Algorithm:
    ///
    /// 1. Capture `cleanup_started_at_micros` from PG's wallclock.
    /// 2. Read `S` = distinct tables in `tx_table_log[B]` whose writer tx is
    ///    *settled* (committed or aborted `<= cleanup_started_at`), left-joined
    ///    with each table's seq watermarks `MAX(last_purged_commit_seq_num)` (Pu)
    ///    and `MAX(last_purged_aborted_seq_num)` (Pa) *as of*
    ///    `cleanup_started_at`. One SQL round-trip. See §Long-cleanup-race.
    /// 3. Compute the branch-min seq cutoffs `MIN(Pu over S)` / `MIN(Pa over S)`
    ///    via `compute_purge_tx_log_cutoffs` (`None` blocks an axis when any
    ///    table is unpurged on it).
    /// 4. Single composite `WITH eligible AS (...)` DELETE over four disjoint
    ///    branches (committed `commit_seq_num <= Pu`, aborted-with-writes
    ///    `aborted_at_seq_num < Pa`, pure-begin+abort, expired-begin wall-clock
    ///    grace). Runs unconditionally — the expired/pure branches apply even
    ///    when both seq cutoffs are `None`. PG's `WITH ... DELETE` snapshot
    ///    semantics make the four sub-DELETEs match one pre-statement view.
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
        // Step 1: snapshot the pass's start time from PG (single
        // monotone clock — same source `persist_locked` / `purge_locked`
        // use). Threaded into the as-of filter on
        // `tx_table_log_purge_watermarks_for_branch` so concurrent
        // Purges committing during this orchestration are invisible
        // to PurgeTxLog (ADR 0021 §"Long-cleanup-race").
        let cleanup_started_at_micros =
            penca_storage_meta::LifecycleManager::now_micros(pool).await?;

        // Steps 2-3: one SQL round-trip read of S ⋈ purged_at(T)
        // AS OF cleanup_started_at_micros.
        let rows = penca_storage_meta::LifecycleManager::tx_table_log_purge_watermarks_for_branch(
            pool,
            &catalog_uuid,
            &branch_uuid,
            cleanup_started_at_micros,
        )
        .await?;

        // Step 4: compute the branch-min seq cutoffs (Pu / Pa) from the
        // per-table watermarks.
        let cutoffs = compute_purge_tx_log_cutoffs(&rows);

        // CHA-507: never GC a `commit_tx_log` row whose `commit_seq_num` is not
        // yet durable in the cold tx_log. Clamp the committed cutoff to the
        // tx_log persist watermark `W_txlog = MAX(max_commit_seq_num)` over
        // committed cold tx_log segments. `persist_tx_log` runs first (and
        // fail-fast) in the branch persist ops, so `W_txlog >=` the data-persist
        // watermark there and this clamp is non-binding; it binds only when data
        // was persisted past what the tx_log covers, holding those hot rows
        // until their cold copy exists. A branch that never ran `persist_tx_log`
        // (`W_txlog` `None`) is unconstrained — pre-CHA-507 behavior.
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

        // Expired-begin ledger-GC grace (ADR 0027 §5): the hot-purge grace
        // window, floored at one Purge sweep interval. The expired-begin branch
        // drops a timed-out tx's `begin_tx_log` / `tx_table_log` — and
        // `begin_tx_log` is the ONLY handle Purge has to enumerate that tx's
        // (invisible) hot rows (`purge::delete_expired_begin_hot_rows`). So the
        // ledger must not drop until Purge has re-swept the tx's tables: the
        // `max` floors the grace at the sweep cadence so ≥1 Purge pass has run.
        // CHA-444 (ADR 0027) replaces the old `query_timeout` grace — that was
        // the query-service cap, with no relation to the Purge sweep cadence.
        let expired_begin_grace_micros = self
            .purge_sweep_interval_micros
            .max(self.hot_purge_grace_micros);

        // Step 5: single composite DELETE over four disjoint eligibility
        // branches — committed (`commit_seq_num <= Pu`), aborted-with-writes
        // (`aborted_at_seq_num < Pa`), pure-begin+abort, and expired-begin
        // (wall-clock grace). Runs unconditionally: the expired-begin /
        // pure-begin+abort branches apply even when both seq cutoffs are
        // `None`. In-flight open txs are never in `eligible`, so their
        // `begin_tx_log` / `tx_table_log` state is preserved. PG's
        // `WITH ... DELETE` snapshot semantics give the atomicity. See
        // `delete_purge_tx_log_eligible` for the safety chain.
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

        // CHA-444: the GC is fire-and-forget across two seq axes — no single
        // micros watermark to report (PurgeTxLogResponse is empty).
        Ok(PurgeTxLogResponse {})
    }
}
