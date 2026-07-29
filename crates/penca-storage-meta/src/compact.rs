//! Compact two-phase commit (`compact_segment_metadata`), seal flips
//! on `table_persist_segment_metadata`, and the
//! `segment_delete_set` GC tombstone helpers (insert + eligibility
//! scan + delete-by-PK) that drive `sweep_segments`. The cold-file
//! reconciliation helper `get_compact_segment_uris_for_branch`
//! consumed by `DeleteBranch` also lives here.

use penca_core::naming;
use penca_db::driver::{DbDriver, SqlValue, format_sql_uuid_array};
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::helpers::{epoch, parse_uuid, qi};
use crate::{LifecycleManager, Result};

impl LifecycleManager {
    // Concurrency safety for compaction itself comes from `SELECT FOR
    // UPDATE` on the input `table_persist_segment_metadata` rows, not
    // from anything this table does. The rows here are an audit-trail
    // for in-flight merged files so a future orphan-cleanup routine
    // can find files left behind by crashed compacts.

    /// Insert an in-flight `compact_segment_metadata` row before
    /// writing a merged file. Auto-commit (no PG tx) so the row
    /// survives a later tx rollback — the whole point of this table
    /// is to leave a trail of "I wrote a file here" that outlives the
    /// transactional segment-repoint. `commit_micros` is NULL;
    /// the compact tx flips it via `commit_compact_segment` once the
    /// segment-row UPDATEs succeed.
    ///
    /// 1 SQL query.
    pub async fn insert_compact_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        object_uri: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::compact_segment_metadata_table(&catalog);
        let sql = format!(
            "INSERT INTO {table} \
                (object_uri, branch_uuid, table_uuid, commit_micros) \
             VALUES ($1, $2, $3, NULL)",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Text(object_uri.to_string()),
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Mark a `compact_segment_metadata` row committed (Phase-2 of the
    /// three-phase compact). Designed for in-tx use — the
    /// caller batches this alongside `repoint_table_persist_segment`
    /// inside the same Phase-2 transaction so the URI swap on
    /// `table_persist_segment_metadata` and the compact-row commit are
    /// atomic. `branch_uuid` is the partition key so PG prunes to one
    /// leaf rather than scanning every branch's partition.
    ///
    /// 1 SQL query.
    pub async fn commit_compact_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        object_uri: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::compact_segment_metadata_table(&catalog);
        let sql = format!(
            "UPDATE {table} SET commit_micros = {epoch} \
             WHERE branch_uuid = $1 AND object_uri = $2",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::Text(object_uri.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Flip every `table_persist_segment_metadata` row in `segment_uuids`
    /// to `is_sealed = true`. Used on the seal-and-start-new boundary
    /// of the active+sealed compact algorithm: the prior
    /// active's segment UUIDs (already in hand inside the merge tx)
    /// transition out of the unsealed set so they never participate
    /// in a future compact wave.
    ///
    /// PK lookup via `(branch_uuid, table_persist_segment_uuid)` so the
    /// UPDATE is partition-pruned and index-served — matches the shape
    /// of [`Self::delete_table_persist_segments_by_uuids`]. Designed for
    /// in-tx use; caller invokes inside the compact merge tx so the
    /// seal and the new active's URI rewrites commit atomically.
    ///
    /// 1 SQL query.
    pub async fn seal_table_persist_segments_by_uuids(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        segment_uuids: &[&str],
    ) -> Result<()> {
        if segment_uuids.is_empty() {
            return Ok(());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_segment_metadata_table(&catalog);
        let arr = format_sql_uuid_array(segment_uuids);
        let sql = format!(
            "UPDATE {table} SET is_sealed = TRUE \
             WHERE branch_uuid = $1 \
               AND table_persist_segment_uuid = ANY({arr})",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(())
    }

    /// Enumerate every `compact_segment_metadata.object_uri` on a
    /// branch — both committed rows (merged files still referenced by
    /// `table_*_segment_metadata` rows on the branch) and NULL rows
    /// (crashed-mid-compact orphans). Used by `delete_branch` so cold
    /// files written by an in-flight compact don't leak when the
    /// branch is dropped: the partition CASCADE removes the rows;
    /// these URIs let the file deletes line up.
    ///
    /// 1 SQL query.
    pub async fn get_compact_segment_uris_for_branch(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<Vec<String>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::compact_segment_metadata_table(&catalog);
        let sql = format!(
            "SELECT object_uri FROM {table} WHERE branch_uuid = $1",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(rows
            .iter()
            .map(|r| r.get::<String, _>("object_uri"))
            .collect())
    }

    // ADR 0019 §"Four-part mechanism" item 3. Compact's merge tx
    // enqueues one row per replaced old URI inside its own tx — the
    // INSERT here is atomic with the URI swap on
    // `table_*_segment_metadata`. A separate sweep
    // (`sweep_segments`) reads rows whose
    // `written_at_micros + query_timeout < now`, deletes the cold
    // file, then deletes the row.

    /// Insert one `segment_delete_set` row per `object_uri` — cold
    /// files a compact merge tx is replacing or a snapshot retirement
    /// tx is releasing. `written_at_micros` is stamped via `DEFAULT
    /// {epoch}` so the grace clock matches the PG server time used by
    /// every other lifecycle commit. One `unnest` statement regardless
    /// of batch size: retirement enqueues every file of a retired
    /// snapshot on the Snapshot RPC's critical path under the
    /// per-table advisory lock, so per-row round-trips don't scale.
    ///
    /// `object_uri` is the whole key, so re-enqueues of one URI
    /// collapse onto one row and the conflict arm REFRESHES
    /// `written_at_micros`: the grace clock restarts at the LAST
    /// enqueue. Under carry-forward (ADR 0024 §4) that last enqueue is
    /// the retirement that dropped the file's final reference —
    /// without the refresh, a shared file already queued by an earlier
    /// retirement would be sweep-eligible the instant its refcount
    /// hits zero, inside the query-timeout window of plans pinned to
    /// the just-retired snapshot. Because the key is catalog-wide
    /// (CHA-531), that refresh spans fork edges too: a child
    /// retiring a URI it carried from its parent extends the same row
    /// the parent's own retirement wrote, so the grace window a
    /// concurrent reader on either branch relies on is the maximum
    /// across all of them, computed at enqueue rather than
    /// reconstructed by the sweep. Compact-retry re-enqueues only
    /// extend grace (harmless).
    ///
    /// Designed for in-tx use — caller passes `&tx` from inside the
    /// compact merge tx so the enqueue commits atomically with the
    /// URI swap on `table_persist_segment_metadata` /
    /// `table_snapshot_segment_metadata` (the retirement tx does the
    /// same with its segment-row deletes).
    ///
    /// 1 SQL query.
    pub async fn insert_segment_delete_set_rows(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        object_uris: &[String],
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::segment_delete_set_table(&catalog);
        let sql = format!(
            "INSERT INTO {table} (object_uri) \
             SELECT unnest($1::text[]) \
             ON CONFLICT (object_uri) \
             DO UPDATE SET written_at_micros = {epoch}",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::TextArray(object_uris.to_vec())])
            .await?;
        Ok(())
    }

    /// Read every `segment_delete_set` row in the catalog past the grace
    /// window — `now_micros - written_at_micros > query_timeout`, the
    /// cold-segment GC grace (ADR 0019's `query_timeout` bound) — whose file
    /// is at snapshot reference count zero (ADR 0024 §4). Returns the
    /// `object_uri`s; the caller deletes the cold file then calls
    /// [`Self::delete_segment_delete_set_row`] per URI.
    ///
    /// The `NOT EXISTS` arm is the refcount gate: under carry-forward
    /// one physical file is referenced by N
    /// `table_snapshot_segment_metadata` rows sharing one
    /// `object_uri`, and a queued URI must not be physically deleted
    /// while any row still references it. Uncommitted rows count too —
    /// necessarily: an in-flight snapshot's carried refs can outlive
    /// its source snapshot's retirement, and a slow snapshot op can
    /// exceed the grace window, so committed-only gating could delete
    /// a file an about-to-commit snapshot references. The flip side is
    /// a live-lock on crash-orphaned uncommitted rows (phase-1 inserts
    /// are auto-commit; failed-snapshot cleanup is best-effort
    /// in-process): until a snapshot-orphan reaper exists
    /// (TODO(CHA-435)), a hard crash mid-snapshot permanently pins
    /// every shared URI its orphan rows reference.
    ///
    /// CHA-531 widened which snapshots can produce such an orphan —
    /// carry-forward means a child's in-flight snapshot references the
    /// parent's files — but not the shape of the pin: the delete set
    /// holds one row per file, so an orphan blocks exactly the files
    /// it references and nothing else. A still-referenced row stays
    /// queued; the retirement that drops the last reference
    /// re-enqueues the URI and refreshes its grace clock (see
    /// [`Self::insert_segment_delete_set_rows`]). Persist-compaction
    /// URIs never appear in the snapshot segment table, so the gate is
    /// a structural no-op for them.
    ///
    /// Both the candidate scan and the two refcount probes are
    /// **catalog-wide**: carry-forward crosses fork edges, so a child
    /// branch's snapshot can reference a file the parent wrote, and a
    /// branch-scoped probe would not see it. `idx_..._sds_age` serves
    /// the eligibility scan; the per-leaf `object_uri` indexes serve
    /// the refcount probes (base segments via `idx_..._tssm_uri`,
    /// cold-index sidecars via `idx_..._tssim_uri`, CHA-455).
    ///
    /// Cost note: a catalog-wide correlated `NOT EXISTS` plans as one
    /// index probe per branch leaf, so each candidate row costs
    /// O(branches) probes rather than one. Combined with the note
    /// above — refcount-pinned rows sit in the expired range and are
    /// re-scanned by every sweep — a standing blocked set costs
    /// O(blocked_rows x branches) per sweep. The triage signal is
    /// `sweep_segments`' `eligible`/`deleted` pair (penca-api's
    /// `lifecycle::sweep`), which should be read against that
    /// baseline.
    ///
    /// 1 SQL query.
    pub async fn eligible_segment_delete_set_rows(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        now_micros: i64,
        query_timeout_micros: i64,
    ) -> Result<Vec<String>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::segment_delete_set_table(&catalog);
        let seg_table = naming::table_snapshot_segment_metadata_table(&catalog);
        // Cold-index sidecars are their own files queued in the
        // same delete set, and a carried sidecar copies the prior file's
        // `object_uri` by reference. So the refcount gate must pin a
        // queued URI while ANY base segment OR sidecar row still
        // references it — otherwise an older segment's retirement would
        // make a file eligible while a younger carried sidecar still
        // points at it. One NOT EXISTS arm per referencing table.
        let seg_index_table = naming::table_snapshot_segment_index_metadata_table(&catalog);
        // CHA-531: neither arm filters on `branch_uuid`. Carry-forward
        // crosses fork edges, so a child's snapshot rows reference a
        // file the parent wrote while living in the CHILD's partition;
        // a branch-scoped probe cannot see them and the sweep would
        // delete a file the child still reads.
        //
        // There is no cross-branch grace arm to go with them: the
        // delete set is keyed on `object_uri` alone, so a URI has one
        // row and one clock, and the enqueue's `ON CONFLICT` refresh
        // already advanced it to the last retirement on any branch.
        //
        // `written_at_micros < $1` keeps the column on the LHS so
        // `idx_..._sds_age` is reliably used for the range scan. The
        // `$1 - written_at_micros > $2` form is semantically
        // equivalent but not consistently sargable across PG planner
        // versions.
        let sql = format!(
            "SELECT object_uri FROM {table} sds \
             WHERE sds.written_at_micros < $1 \
               AND NOT EXISTS (\
                 SELECT 1 FROM {seg_table} seg \
                 WHERE seg.object_uri = sds.object_uri\
               ) \
               AND NOT EXISTS (\
                 SELECT 1 FROM {seg_index_table} six \
                 WHERE six.object_uri = sds.object_uri\
               )",
            table = qi(&table),
            seg_table = qi(&seg_table),
            seg_index_table = qi(&seg_index_table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::Int64(now_micros - query_timeout_micros)])
            .await?;
        Ok(rows
            .iter()
            .map(|r| r.get::<String, _>("object_uri"))
            .collect())
    }

    /// Delete one `segment_delete_set` row by its `object_uri` PK.
    /// Called by the sweep only after the cold-file delete succeeds —
    /// a transient cold-storage failure leaves the row in place for
    /// the next sweep to retry.
    ///
    /// 1 SQL query.
    pub async fn delete_segment_delete_set_row(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        object_uri: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::segment_delete_set_table(&catalog);
        let sql = format!(
            "DELETE FROM {table} WHERE object_uri = $1",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::Text(object_uri.to_string())])
            .await?;
        Ok(())
    }
}
