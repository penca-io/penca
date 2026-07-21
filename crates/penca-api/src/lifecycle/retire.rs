//! Retire: standalone all-but-latest snapshot retirement
//! (CHA-405 / ADR 0024 §4; decoupled from the Snapshot op in CHA-468).
//!
//! [`LifecycleManager::retire_snapshots`] is the standalone retirement op: it
//! takes the `snapshot:{table}:{branch}` advisory lock and runs
//! [`LifecycleManager::retire_snapshots_except_latest`], which deletes every
//! committed snapshot's metadata rows except the latest's and enqueues the
//! retired files in `segment_delete_set`; the ref-counted sweep
//! (`sweep_segments`) performs the physical deletes.
//!
//! CHA-468 pulled this out of the Snapshot commit path and left it **disabled
//! by default** — nothing calls `retire_snapshots` yet, so a Snapshot only
//! materialises a baseline and snapshots accumulate. That stops a newer
//! snapshot from stranding an open (RYOW) tx's baseline and forcing a slow
//! cold-persist-log reconstruction. Re-enabling retirement safely — the
//! `PruneSnapshotSegments` RPC + scheduler step + the open-tx-safe
//! two-baseline retention (keep latest + latest ≤ the earliest open tx,
//! bounded by the tx timeout) — is TODO(CHA-55).

use std::collections::BTreeSet;

use penca_db::driver::pg::PgDriver;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;

impl LifecycleManager {
    /// Standalone snapshot-retirement op (CHA-468): acquire the
    /// `snapshot:{table}:{branch}` advisory lock and retire every committed
    /// snapshot of `(table, branch)` except the latest.
    ///
    /// Decoupled from the Snapshot commit path so retirement can be scheduled,
    /// tuned, and **disabled independently** of materialisation. It is
    /// **disabled by default**: nothing wires this op yet, so snapshots
    /// accumulate and open (RYOW) txs keep a usable baseline. The lock is the
    /// same key the Snapshot op takes, preserving the CHA-405 serialization
    /// invariant — snapshot-file reference counts change only serialized with
    /// snapshot creation.
    ///
    /// TODO(CHA-55): the `PruneSnapshotSegments` RPC + scheduler step + the
    /// open-tx-safe two-baseline retention policy (keep latest + latest ≤ the
    /// earliest open tx, bounded by the tx timeout) that re-enables retirement
    /// without regressing open-tx read latency.
    pub async fn retire_snapshots(
        &self,
        pool: &PgDriver,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
    ) -> Result<(), ApiError> {
        let lock_key = super::snapshot_lock_key(table_uuid, branch_uuid);
        pool.advisory_lock(&lock_key, async || {
            self.retire_snapshots_except_latest(pool, catalog_uuid, branch_uuid, table_uuid)
                .await
        })
        .await
    }

    /// Retire every committed snapshot of `(table, branch)` except the latest
    /// (CHA-405 / ADR 0024 §4). The locked body of
    /// [`LifecycleManager::retire_snapshots`].
    ///
    /// Snapshots are a read-optimization cache: only the latest
    /// committed snapshot serves reads, so retirement keeps only the
    /// latest and drops its predecessors. An `as_of` older than the latest
    /// snapshot watermark falls back to the raw persist log — never
    /// GC'd here (Purge is hot-tier only), so correctness is preserved
    /// and only old-`as_of` read perf degrades until persist-log
    /// retention (CHA-425) bounds history with a loud horizon.
    ///
    /// One tx: enqueue each retired file's `object_uri` in
    /// `segment_delete_set`, delete the retired segment rows, delete
    /// the now-segmentless parents. Atomicity mirrors compaction's
    /// merge-tx enqueue (CHA-233): there is no state where rows are
    /// gone but files were never enqueued. Retirement never decides
    /// file deletability — that is the sweep's refcount gate; under
    /// carry-forward (CHA-406) a retired file still referenced by a
    /// younger snapshot stays queued until the reference holder's own
    /// retirement re-enqueues it, restarting the grace clock at the
    /// last reference drop (the deterministic `segment_delete_uuid`
    /// collapses both enqueues onto one row, which is what lets the
    /// `ON CONFLICT` refresh fire).
    ///
    /// Runs inside the caller's `snapshot:{table}:{branch}` advisory
    /// lock, so a table's snapshot-file reference counts only change
    /// serialized with snapshot creation. Errors propagate: the
    /// committed snapshot is durable, and retirement is idempotent —
    /// it re-runs on the next retirement pass.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = %catalog_uuid,
            branch_uuid = %branch_uuid,
            table_uuid = %table_uuid,
        ),
    )]
    pub(super) async fn retire_snapshots_except_latest(
        &self,
        pool: &PgDriver,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
    ) -> Result<(), ApiError> {
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let table_str = table_uuid.to_string();

        let retired = penca_storage_meta::LifecycleManager::get_retired_snapshot_segments(
            pool,
            &catalog_str,
            &branch_str,
            &table_str,
        )
        .await?;
        if retired.is_empty() {
            // Steady state for a table's first snapshot (and every
            // re-run after a successful retirement) — no tx opened.
            // The parent sweep still runs so a crash-orphaned,
            // segmentless parent (the snapshot_op phase-1a hazard) is
            // reaped on the next cycle even when nothing retires.
            penca_storage_meta::LifecycleManager::delete_orphaned_snapshot_metadata(
                pool,
                &catalog_str,
                &branch_str,
                &table_str,
            )
            .await?;
            penca_storage_meta::LifecycleManager::delete_orphaned_table_snapshot_index_rows(
                pool,
                &catalog_str,
                &branch_str,
            )
            .await?;
            return Ok(());
        }

        let segment_uuid_strs: Vec<String> = retired
            .iter()
            .map(|(seg_uuid, _)| seg_uuid.clone())
            .collect();
        let distinct_uris: Vec<String> = retired
            .iter()
            .map(|(_, uri)| uri.clone())
            .collect::<BTreeSet<String>>()
            .into_iter()
            .collect();

        let tx = pool
            .begin()
            .await
            .map_err(|e| ApiError::Metadata(e.into()))?;
        penca_storage_meta::LifecycleManager::insert_segment_delete_set_rows(
            &tx,
            &catalog_str,
            &branch_str,
            &table_str,
            &distinct_uris,
        )
        .await?;

        // CHA-455: the retired base segments' cold-index sidecars are
        // themselves cold files — enqueue their URIs in the same
        // segment_delete_set sweep, then drop the sidecar metadata rows so
        // they don't outlive the base segments they reference. A carried
        // sidecar copies the prior file's URI by reference, so the
        // ref-counted sweep gate (CHA-405) — extended to refcount
        // segment_index_metadata too — pins the file until the last
        // sidecar row referencing it retires. No-op until CHA-412 emits
        // sidecars.
        let sidecars = penca_storage_meta::LifecycleManager::list_segment_index_metadata(
            &tx,
            &catalog_str,
            &branch_str,
            &segment_uuid_strs,
        )
        .await?;
        if !sidecars.is_empty() {
            let sidecar_uris: Vec<String> = sidecars
                .iter()
                .map(|s| s.object_uri.clone())
                .collect::<BTreeSet<String>>()
                .into_iter()
                .collect();
            penca_storage_meta::LifecycleManager::insert_segment_delete_set_rows(
                &tx,
                &catalog_str,
                &branch_str,
                &table_str,
                &sidecar_uris,
            )
            .await?;
        }
        // Unconditional (no-op on empty): deletes committed AND any stray
        // uncommitted sidecar rows so none outlive their base segments —
        // the committed-only `sidecars` list above would otherwise leave
        // uncommitted orphans behind.
        penca_storage_meta::LifecycleManager::delete_segment_index_metadata_for_segments(
            &tx,
            &catalog_str,
            &branch_str,
            &segment_uuid_strs,
        )
        .await?;

        penca_storage_meta::LifecycleManager::delete_snapshot_segments_by_uuids(
            &tx,
            &catalog_str,
            &branch_str,
            &segment_uuid_strs,
        )
        .await?;
        penca_storage_meta::LifecycleManager::delete_orphaned_snapshot_metadata(
            &tx,
            &catalog_str,
            &branch_str,
            &table_str,
        )
        .await?;
        // CHA-412: reap the parent index rows of any snapshot just retired
        // above — the parent analog of dropping a retired segment's sidecars.
        penca_storage_meta::LifecycleManager::delete_orphaned_table_snapshot_index_rows(
            &tx,
            &catalog_str,
            &branch_str,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|e| ApiError::Metadata(e.into()))?;

        tracing::debug!(
            retired_segment_rows = retired.len(),
            enqueued_uris = distinct_uris.len(),
            "retired snapshots beyond latest"
        );
        Ok(())
    }
}
