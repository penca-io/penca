//! `persist_tx_log` — flush the slim per-branch commit map to cold, so a fork
//! position and the audit tx-metadata survive hot `commit_tx_log` GC.
//!
//! MUST run **first** in `persist_branch`, before any data-table persist: a
//! cold data segment drops author/comment and leans on the cold tx_log join
//! for them, so the tx_log covering `<= target` must be durable before any
//! data segment referencing those seqs can become visible.
//!
//! Incremental + idempotent: the segment uuid is deterministic on
//! `max_commit_seq_num`, so a re-run of the same range upserts the same row.
//! Two-phase durable write (insert uncommitted → write file → commit) leaves a
//! crashed run's segment invisible to both reads and the watermark, and the
//! next run redoes the range.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use penca_core::naming::{
    commit_tx_log_partition, tx_log_persist_segment_uri, tx_log_persist_segment_uuid,
};
use penca_db::dialect::Dialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::pg::PgDriver;
use penca_db::driver::{DbDriver, SqlValue};
use penca_format::writer::FormatWriter;
use penca_storage_cold::ColdStorageClient;
use sqlx::Row;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;

impl LifecycleManager {
    /// Flush hot `commit_tx_log` up to `target_commit_seq_num` into a cold
    /// `tx_log` segment. Serialized per branch by the `persist_tx_log:{branch}`
    /// advisory lock; a no-op when nothing new is committed past `W_txlog`.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = %catalog_uuid,
            branch_uuid = %branch_uuid,
            target_commit_seq_num = target_commit_seq_num,
        ),
    )]
    pub async fn persist_tx_log<W: FormatWriter>(
        &self,
        pool: &PgDriver,
        writer: &W,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        target_commit_seq_num: i64,
    ) -> Result<(), ApiError> {
        // `W_txlog` only ever grows, so an unlocked read that already covers
        // `target` is authoritative — a concurrent flush can only push it
        // higher. That lets per-table Persist call `persist_tx_log`
        // unconditionally and no-op here without contending on the lock. The
        // locked path re-checks the watermark, so this is a pure optimization,
        // not the correctness boundary.
        let w_txlog = penca_storage_meta::LifecycleManager::tx_log_persist_watermark(
            pool,
            &catalog_uuid.to_string(),
            &branch_uuid.to_string(),
        )
        .await?
        .unwrap_or(-1);
        if target_commit_seq_num <= w_txlog {
            return Ok(());
        }

        let lock_key = format!("persist_tx_log:{branch_uuid}");
        pool.advisory_lock(&lock_key, async || {
            self.persist_tx_log_locked(
                pool,
                writer,
                catalog_uuid,
                branch_uuid,
                target_commit_seq_num,
            )
            .await
        })
        .await
    }

    async fn persist_tx_log_locked<W: FormatWriter>(
        &self,
        pool: &PgDriver,
        writer: &W,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        target_commit_seq_num: i64,
    ) -> Result<(), ApiError> {
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();

        // Reclaim any uncommitted tx_log segment (+ its orphaned cold file) left
        // by a crash between a prior file write and its commit. The tx_log URI
        // kind is outside the persist/snapshot orphan sweeps, so this is its
        // only reclamation path. Delete the file FIRST, then drop the row, so a
        // failed file delete leaves the reclamation record for the next sweep
        // rather than orphaning the file with no DB record.
        for (seg_uuid, uri) in
            penca_storage_meta::LifecycleManager::list_uncommitted_tx_log_segments(
                pool,
                &catalog_str,
                &branch_str,
            )
            .await?
        {
            if ColdStorageClient::delete_segment(writer, &uri, true)
                .await
                .is_ok()
            {
                let _ =
                    penca_storage_meta::LifecycleManager::delete_uncommitted_tx_log_persist_segment(
                        pool,
                        &catalog_str,
                        &branch_str,
                        &seg_uuid,
                    )
                    .await;
            }
        }

        // Lower bound: the highest seq already durable in cold. Only committed
        // segments count, so a crashed prior run does not advance it. `-1` ⇒
        // nothing flushed yet (commit_seq_num starts at 0).
        let w_txlog = penca_storage_meta::LifecycleManager::tx_log_persist_watermark(
            pool,
            &catalog_str,
            &branch_str,
        )
        .await?
        .unwrap_or(-1);
        if target_commit_seq_num <= w_txlog {
            return Ok(());
        }

        let tx_q = PgDialect::quote_identifier(&commit_tx_log_partition(catalog_uuid, branch_uuid));
        let sql = format!(
            "SELECT commit_seq_num, commit_micros, author, comment FROM {tx_q} \
             WHERE commit_seq_num > $1 AND commit_seq_num <= $2 \
             ORDER BY commit_seq_num"
        );
        let rows = pool
            .execute_params(
                &sql,
                &[
                    SqlValue::Int64(w_txlog),
                    SqlValue::Int64(target_commit_seq_num),
                ],
            )
            .await?;
        if rows.is_empty() {
            return Ok(());
        }

        // The non-Option gets are safe: commit_tx_log's author/comment are NOT NULL.
        let mut seqs = Vec::with_capacity(rows.len());
        let mut micros = Vec::with_capacity(rows.len());
        let mut authors = Vec::with_capacity(rows.len());
        let mut comments = Vec::with_capacity(rows.len());
        for r in &rows {
            seqs.push(r.get::<i64, _>("commit_seq_num"));
            micros.push(r.get::<i64, _>("commit_micros"));
            authors.push(r.get::<String, _>("author"));
            comments.push(r.get::<String, _>("comment"));
        }
        let min_seq = seqs[0];
        let max_seq = seqs[seqs.len() - 1];
        let min_micros = *micros.iter().min().expect("rows non-empty");
        let max_micros = *micros.iter().max().expect("rows non-empty");

        let batch = RecordBatch::try_new(
            penca_storage_meta::tx_log_arrow_schema(),
            vec![
                Arc::new(Int64Array::from(seqs)),
                Arc::new(Int64Array::from(micros)),
                Arc::new(StringArray::from(authors)),
                Arc::new(StringArray::from(comments)),
            ],
        )
        .map_err(|e| ApiError::Internal(format!("build tx_log batch: {e}")))?;

        let seg_uuid = tx_log_persist_segment_uuid(catalog_uuid, branch_uuid, max_seq);
        let seg_uuid_str = seg_uuid.to_string();
        let uri = tx_log_persist_segment_uri(
            &self.base_uri,
            catalog_uuid,
            branch_uuid,
            &seg_uuid,
            self.storage_format.extension(),
        );

        penca_storage_meta::LifecycleManager::insert_tx_log_persist_segment(
            pool,
            &catalog_str,
            &branch_str,
            &seg_uuid_str,
            min_seq,
            max_seq,
            min_micros,
            max_micros,
            &uri,
            rows.len() as i64,
            self.storage_format.extension(),
        )
        .await?;

        if let Err(e) = ColdStorageClient::write_tx_log_persist_segment(writer, &uri, &batch).await
        {
            let _ = ColdStorageClient::delete_segment(writer, &uri, true).await;
            let _ =
                penca_storage_meta::LifecycleManager::delete_uncommitted_tx_log_persist_segment(
                    pool,
                    &catalog_str,
                    &branch_str,
                    &seg_uuid_str,
                )
                .await;
            return Err(ApiError::ColdStorage(e));
        }

        penca_storage_meta::LifecycleManager::commit_tx_log_persist_segment(
            pool,
            &catalog_str,
            &branch_str,
            &seg_uuid_str,
        )
        .await?;
        Ok(())
    }
}
