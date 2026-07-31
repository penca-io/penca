//! Retire: standalone all-but-latest snapshot retirement (ADR 0024 §4).
//! Metadata rows go here; the ref-counted `sweep_segments` performs the
//! physical file deletes.
//!
//! **Disabled by default** — nothing calls `retire_snapshots`, so a Snapshot
//! only materialises a baseline and snapshots accumulate. Deliberate: a newer
//! snapshot would otherwise strand an open (RYOW) tx's baseline and force a
//! slow cold-persist-log reconstruction.

use std::collections::BTreeSet;

use penca_db::driver::pg::PgDriver;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;

impl LifecycleManager {
    /// Acquire the `snapshot:{table}:{branch}` advisory lock and retire every
    /// committed snapshot of `(table, branch)` except the latest.
    ///
    /// Separate from the Snapshot commit path so retirement can be scheduled,
    /// tuned, and disabled independently of materialisation. The lock is the
    /// same key the Snapshot op takes, preserving the serialization invariant:
    /// snapshot-file reference counts change only serialized with snapshot
    /// creation.
    ///
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

    /// The locked body of [`LifecycleManager::retire_snapshots`] (ADR 0024 §4).
    ///
    /// Snapshots are a read-optimization cache: only the latest committed one
    /// serves reads. An `as_of` older than the latest snapshot watermark falls
    /// back to the raw persist log — never GC'd here (Purge is hot-tier only) —
    /// so dropping predecessors costs old-`as_of` read perf, never correctness.
    ///
    /// One tx: enqueue each retired file's `object_uri` in `segment_delete_set`,
    /// delete the retired segment rows, delete the now-segmentless parents.
    /// There must be no state where rows are gone but files were never
    /// enqueued. Retirement never decides file deletability — that is the
    /// sweep's refcount gate; under carry-forward a retired file still
    /// referenced by a younger snapshot stays queued until the reference
    /// holder's own retirement re-enqueues it, restarting the grace clock at
    /// the last reference drop (the set is keyed on `object_uri` alone, so both
    /// enqueues land on one row and the `ON CONFLICT` refresh fires).
    ///
    /// Errors propagate: the committed snapshot is durable and retirement is
    /// idempotent, so it re-runs on the next pass.
    ///
    /// TODO(CHA-55): the `PruneSnapshotSegments` RPC + scheduler step + the
    /// open-tx-safe two-baseline retention policy (keep latest + latest ≤ the
    /// earliest open tx, bounded by the tx timeout) that re-enables retirement
    /// without regressing open-tx read latency.
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
            // The parent sweep still runs with nothing to retire, so a
            // crash-orphaned segmentless parent (the snapshot_op phase-1a
            // hazard) is reaped on the next cycle.
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
        // Cold-index sidecars are cold files too: enqueue their URIs in the
        // same sweep so they don't outlive the base segments they reference. A
        // carried sidecar copies the prior file's URI by reference, so the
        // sweep's refcount gate — which counts `segment_index_metadata` as
        // well — pins the file until the last sidecar row referencing it
        // retires.
        let sidecars = penca_storage_meta::LifecycleManager::list_segment_index_metadata(
            &tx,
            &catalog_str,
            &branch_str,
            &segment_uuid_strs,
        )
        .await?;
        let sidecar_uris: Vec<String> = sidecars
            .iter()
            .map(|s| s.object_uri.clone())
            .collect::<BTreeSet<String>>()
            .into_iter()
            .collect();
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
        penca_storage_meta::LifecycleManager::delete_orphaned_table_snapshot_index_rows(
            &tx,
            &catalog_str,
            &branch_str,
        )
        .await?;

        // Delete-set LAST, per the ordering invariant on
        // `insert_segment_delete_set_rows`. Since CHA-546 every statement above
        // names this branch's partitions, so the tx holds no segment-metadata
        // parent lock and the invariant costs this path nothing. Position within
        // the tx is free for ADR 0019 item 3 — it requires the rows to commit
        // atomically with the retirement, not to precede it.
        penca_storage_meta::LifecycleManager::insert_segment_delete_set_rows(
            &tx,
            &catalog_str,
            &distinct_uris,
        )
        .await?;
        if !sidecar_uris.is_empty() {
            penca_storage_meta::LifecycleManager::insert_segment_delete_set_rows(
                &tx,
                &catalog_str,
                &sidecar_uris,
            )
            .await?;
        }

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
