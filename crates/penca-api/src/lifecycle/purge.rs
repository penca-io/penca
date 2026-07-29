//! Purge: clear-from-hot after Persist (ADR 0027).
//!
//! `purge_locked` does three things, each on its own axis:
//! - **committed cleanup** — advance the read fence `Pu` to `W_snap` and
//!   delete committed hot rows with `commit_seq_num <= Pu` (atomic with the
//!   watermark commit). `Pu` is the hot↔cold fence `plan()` reads.
//! - **abort cleanup** — delete aborted hot rows (`aborted_at_seq_num < F`,
//!   the abort-counter frontier) and advance the abort watermark `Pa = F`.
//! - **expired-begin cleanup** — delete the hot rows of expired-but-never-
//!   committed/aborted txs. Invisible garbage, no watermark (their ledger GC
//!   is wall-clock; see ADR 0027 §5).

use penca_core::naming::{
    abort_tx_log_partition, begin_tx_log_partition, commit_tx_log_partition, delete_log_table,
    table_purge_uuid, upsert_log_table,
};
use penca_db::dialect::Dialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::pg::PgDriver;
use penca_db::driver::{DbDriver, SqlValue};
use penca_dl::driver::DlDriver;
use penca_proto::external::v1::{PurgeRequest, PurgeResponse};
use penca_storage_meta::watermarks::compute_purge_watermark;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;

impl LifecycleManager {
    /// Clear T's hot rows and advance its purge watermarks.
    ///
    /// The response carries the committed watermark `Pu`, unset when `Pu` did
    /// not advance — branch-min consumers read an absent row as "this table
    /// has not contributed a watermark yet".
    ///
    /// Locked per-table via `purge:{table_uuid}:{branch_uuid}` — serializes
    /// `Purge(T)` against `Purge(T)` only (race-losers no-op via the
    /// deterministic `purge_uuid` + the strict-advance early-out). Cross-
    /// operation pairs (`Persist↔Purge`, `Snapshot↔Purge`) are lock-free —
    /// the committed-fence MVCC argument (ADR 0027 §Correctness) and the
    /// invisible-garbage abort/expired deletes are both lock-free.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn purge<L>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &PurgeRequest,
    ) -> Result<PurgeResponse, ApiError>
    where
        L: DlDriver + ?Sized,
    {
        // See `persist`'s `step1_now` doc-comment for the rationale.
        let step1_now = penca_storage_meta::LifecycleManager::now_micros(pool).await?;
        let snapshot = penca_merge::ReadSnapshot::AsOfMicros(step1_now);
        let (catalog_uuid, branch_uuid, table_uuid) = self
            .resolve_catalog_branch_and_table(
                pool,
                dl_driver,
                request.catalog_uuid.as_deref(),
                request.catalog_name.as_deref(),
                request.schema_uuid.as_deref(),
                request.schema_name.as_deref(),
                request.branch_uuid.as_deref(),
                request.branch_name.as_deref(),
                request.table_uuid.as_deref(),
                request.table_name.as_deref(),
                &snapshot,
            )
            .await?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("table_uuid", tracing::field::display(&table_uuid));

        let lock_key = format!("purge:{table_uuid}:{branch_uuid}");
        pool.advisory_lock(&lock_key, async || {
            self.purge_locked(pool, catalog_uuid, branch_uuid, table_uuid, step1_now)
                .await
        })
        .await
    }

    async fn purge_locked(
        &self,
        pool: &PgDriver,
        catalog_uuid: Uuid,
        branch_uuid: Uuid,
        table_uuid: Uuid,
        step1_now: i64,
    ) -> Result<PurgeResponse, ApiError> {
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let table_str = table_uuid.to_string();

        Self::delete_expired_begin_hot_rows(
            pool,
            &catalog_uuid,
            &branch_uuid,
            &table_uuid,
            step1_now,
        )
        .await?;

        // Committed cleanup target: Pu = W_snap, strict-advance-gated against
        // the last committed Pu.
        let wsnap =
            penca_storage_meta::LifecycleManager::latest_committed_table_snapshot_seq_watermark(
                pool,
                &catalog_str,
                &branch_str,
                &table_str,
            )
            .await?;
        let last_pu =
            penca_storage_meta::LifecycleManager::latest_committed_table_purge_seq_watermark(
                pool,
                &catalog_str,
                &branch_str,
                &table_str,
            )
            .await?;
        let pu_advance = compute_purge_watermark(wsnap, last_pu);

        // Abort cleanup target: Pa = F, the abort-counter frontier, likewise
        // strict-advance-gated.
        let frontier = penca_storage_meta::LifecycleManager::read_abort_seq_frontier(
            pool,
            &catalog_uuid,
            &branch_uuid,
        )
        .await?;
        let last_pa = penca_storage_meta::LifecycleManager::latest_committed_table_purge_aborted_seq_watermark(
            pool,
            &catalog_str,
            &branch_str,
            &table_str,
        )
        .await?;
        // Treat "no prior abort purge" as Pa=0 so a no-abort branch (frontier
        // == 0) does not spuriously advance Pa from None to 0 and write a bogus
        // purge row. The first real abort allocates aborted_at_seq_num=0 and
        // bumps the frontier to 1, so a genuine advance is still F > 0.
        let pa_advance = compute_purge_watermark(frontier, last_pa.or(Some(0)));

        // Safe to bail: the expired-begin cleanup above already ran.
        if pu_advance.is_none() && pa_advance.is_none() {
            return Ok(PurgeResponse {
                purged_at_micros: last_pu,
            });
        }

        // Seed the two-phase row identity on the (Pu, Pa) pair (NULL → -1).
        let purge_uuid = table_purge_uuid(
            &catalog_uuid,
            &branch_uuid,
            &table_uuid,
            pu_advance.unwrap_or(-1),
            pa_advance.unwrap_or(-1),
        );
        let purge_uuid_str = purge_uuid.to_string();

        // Phase 1: insert uncommitted (NULL committed_at).
        penca_storage_meta::LifecycleManager::insert_table_purge(
            pool,
            &catalog_str,
            &purge_uuid_str,
            &branch_str,
            &table_str,
            pu_advance,
            pa_advance,
        )
        .await?;

        // Phase 2: hot deletes (committed <= Pu, aborts < Pa) + commit the
        // watermark row, one tx, so the fence advance and the clear-from-hot
        // are atomic.
        let phase2: Result<(), ApiError> = async {
            let tx = pool
                .begin()
                .await
                .map_err(|e| ApiError::Metadata(e.into()))?;
            let upsert_table = upsert_log_table(&table_uuid, &branch_uuid);
            let delete_table = delete_log_table(&table_uuid, &branch_uuid);
            let commit_tx_log_table = commit_tx_log_partition(&catalog_uuid, &branch_uuid);
            let abort_table = abort_tx_log_partition(&catalog_uuid, &branch_uuid);

            if let Some(pu) = pu_advance {
                let subquery = format!(
                    "SELECT tx_uuid FROM {commit_tx_log} WHERE commit_seq_num <= $1",
                    commit_tx_log = PgDialect::quote_identifier(&commit_tx_log_table),
                );
                Self::delete_hot_log_rows_for_tx_subquery(
                    &tx,
                    &upsert_table,
                    &delete_table,
                    &subquery,
                    &[SqlValue::Int64(pu)],
                )
                .await?;
            }
            if let Some(pa) = pa_advance {
                let subquery = format!(
                    "SELECT tx_uuid FROM {abort} WHERE aborted_at_seq_num < $1",
                    abort = PgDialect::quote_identifier(&abort_table),
                );
                Self::delete_hot_log_rows_for_tx_subquery(
                    &tx,
                    &upsert_table,
                    &delete_table,
                    &subquery,
                    &[SqlValue::Int64(pa)],
                )
                .await?;
            }
            penca_storage_meta::LifecycleManager::commit_table_purge(
                &tx,
                &catalog_str,
                &branch_str,
                &purge_uuid_str,
            )
            .await?;
            tx.commit()
                .await
                .map_err(|e| ApiError::Metadata(e.into()))?;
            Ok(())
        }
        .await;

        if let Err(err) = phase2 {
            let _ = penca_storage_meta::LifecycleManager::delete_uncommitted_table_purge(
                pool,
                &catalog_str,
                &branch_str,
                &purge_uuid_str,
            )
            .await;
            return Err(err);
        }

        // The reported watermark is the committed fence Pu — what `plan()` reads.
        Ok(PurgeResponse {
            purged_at_micros: pu_advance.or(last_pu),
        })
    }

    /// Delete the hot upsert/delete rows of **expired-begin** txs — timed-out
    /// open txs that never committed or explicitly aborted (a `begin_tx_log`
    /// row, expired, with no `commit_tx_log` / `abort_tx_log` row). Their rows are
    /// invisible to reads (no committed `commit_tx_log` row to join), so this needs
    /// no watermark and no grace; it runs every pass to reclaim the garbage.
    /// Ledger GC (`begin_tx_log` / `tx_table_log`) is the separate wall-clock
    /// expiry grace in `PurgeTxLog` (ADR 0027 §5).
    async fn delete_expired_begin_hot_rows(
        pool: &PgDriver,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
        now_micros: i64,
    ) -> Result<(), ApiError> {
        let upsert_table = upsert_log_table(table_uuid, branch_uuid);
        let delete_table = delete_log_table(table_uuid, branch_uuid);
        let begin_table = begin_tx_log_partition(catalog_uuid, branch_uuid);
        let commit_tx_log_table = commit_tx_log_partition(catalog_uuid, branch_uuid);
        let abort_table = abort_tx_log_partition(catalog_uuid, branch_uuid);

        let subquery = format!(
            "SELECT b.tx_uuid FROM {begin} b \
             WHERE b.expires_at_micros < $1 \
               AND NOT EXISTS (SELECT 1 FROM {commit_tx_log} t WHERE t.tx_uuid = b.tx_uuid) \
               AND NOT EXISTS (SELECT 1 FROM {abort} a WHERE a.tx_uuid = b.tx_uuid)",
            begin = PgDialect::quote_identifier(&begin_table),
            commit_tx_log = PgDialect::quote_identifier(&commit_tx_log_table),
            abort = PgDialect::quote_identifier(&abort_table),
        );
        Self::delete_hot_log_rows_for_tx_subquery(
            pool,
            &upsert_table,
            &delete_table,
            &subquery,
            &[SqlValue::Int64(now_micros)],
        )
        .await
    }

    /// DELETE the hot upsert/delete rows of the txs selected by
    /// `tx_uuid_subquery` — the shared `[upsert_log, delete_log]` fan-out
    /// behind every Purge hot clear (committed `commit_seq_num <= Pu`, aborted
    /// `aborted_at_seq_num < Pa`, and expired-begin garbage). The caller owns
    /// *which* txs are doomed (the subquery and its `$1` bind); this owns only
    /// wiping both hot log tables. `executor` is the phase-2 `tx` for the
    /// committed/abort clears and the `pool` for the lock-free expired-begin
    /// sweep — both impl [`DbDriver`].
    async fn delete_hot_log_rows_for_tx_subquery(
        executor: &impl DbDriver,
        upsert_table: &str,
        delete_table: &str,
        tx_uuid_subquery: &str,
        params: &[SqlValue],
    ) -> Result<(), ApiError> {
        for log_table in [upsert_table, delete_table] {
            let sql = format!(
                "DELETE FROM {tbl} WHERE tx_uuid IN ({subq})",
                tbl = PgDialect::quote_identifier(log_table),
                subq = tx_uuid_subquery,
            );
            executor
                .execute_no_result_params(&sql, params)
                .await
                .map_err(|e| ApiError::Metadata(e.into()))?;
        }
        Ok(())
    }
}
