//! Snapshot metadata + snapshot segment two-phase commit and the
//! retention-prune helpers consumed by the snapshot lifecycle and
//! DeleteBranch cleanup.

use penca_core::naming;
use penca_db::dialect::DbDialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::{DbDriver, SqlValue, format_sql_uuid_array};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::helpers::{epoch, parse_uuid, qi};
use crate::{CarriedSegmentSpec, LifecycleManager, MetadataError, Result};

impl LifecycleManager {
    /// Insert a snapshot metadata row (phase 1).
    ///
    /// 1 SQL query.
    pub async fn insert_snapshot_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        table_snapshot_uuid: &str,
        snapshotted_at_micros: i64,
        partition_keys: &[String],
        clustering_keys: &[String],
        // The snapshot seq watermark W_snap; -1 for an empty/genesis baseline.
        commit_seq_num: i64,
        // Whether this snapshot is a durable retention rung. Decided once by
        // the caller (`decide_durable`) at creation and sticky thereafter.
        durable: bool,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_metadata_table(&catalog);
        // `table_snapshot_uuid` is deterministic from
        // `(catalog, branch, table, snapshotted_at)`, so retries collapse via
        // `DO UPDATE`.
        //
        // `partition_keys` / `clustering_keys` are parent-level because a key
        // change between snapshots forces a full rewrite (ADR 0024), so one key
        // set covers every segment in the snapshot.
        //
        // `durable` is deliberately omitted from the `DO UPDATE` set — it is
        // decided once at creation and must stay sticky so the retention floor
        // is monotonic. A crash-retry re-inserts the identical row but never
        // flips an already-recorded flag.
        let sql = format!(
            "INSERT INTO {table} \
             (table_snapshot_uuid, branch_uuid, table_uuid, \
              snapshotted_at_micros, partition_keys, clustering_keys, commit_seq_num, durable) \
             VALUES ($1, $2, $3, $4, $5::text[], $6::text[], $7, $8) \
             ON CONFLICT (branch_uuid, table_snapshot_uuid) DO UPDATE \
                SET snapshotted_at_micros = EXCLUDED.snapshotted_at_micros, \
                    partition_keys = EXCLUDED.partition_keys, \
                    clustering_keys = EXCLUDED.clustering_keys, \
                    commit_seq_num = EXCLUDED.commit_seq_num",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(table_snapshot_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                    SqlValue::Int64(snapshotted_at_micros),
                    SqlValue::TextArray(partition_keys.to_vec()),
                    SqlValue::TextArray(clustering_keys.to_vec()),
                    SqlValue::Int64(commit_seq_num),
                    SqlValue::Bool(durable),
                ],
            )
            .await?;
        Ok(())
    }

    /// The `snapshotted_at_micros` of the most recent **committed**
    /// durable snapshot for `(branch, table)`, or `None` when none is durable
    /// yet. The sticky durable-assignment decision reads this once at snapshot
    /// creation to gate the next rung by the density cadence.
    ///
    /// 1 SQL query.
    pub async fn last_durable_snapshot_at(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
    ) -> Result<Option<i64>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_metadata_table(&catalog);
        let sql = format!(
            "SELECT MAX(snapshotted_at_micros) AS last_durable FROM {table} \
             WHERE branch_uuid = $1 AND table_uuid = $2 \
               AND durable AND commit_micros IS NOT NULL",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                ],
            )
            .await?;
        Ok(rows.first().and_then(|row| {
            let last_durable: Option<i64> = row.get("last_durable");
            last_durable
        }))
    }

    /// Insert a snapshot segment with NULL `commit_micros`.
    ///
    /// `offset`/`length` are the segment's row range within its file — multiple
    /// segments, one per partition, can share one `object_uri`. A
    /// single-segment file is the whole-file range `(0, row_count)`, never NULL.
    ///
    /// 1 SQL query.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_snapshot_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        table_snapshot_segment_uuid: &str,
        table_snapshot_uuid: &str,
        chunk_idx: u32,
        object_uri: &str,
        offset: i64,
        length: i64,
        row_count: i64,
        format_text: &str,
        statistics: &[u8],
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_metadata_table(&catalog);
        let sql = format!(
            "INSERT INTO {table} \
             (table_snapshot_segment_uuid, table_snapshot_uuid, branch_uuid, table_uuid, \
              chunk_idx, object_uri, \"offset\", length, row_count, format, statistics) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (branch_uuid, table_snapshot_segment_uuid) DO UPDATE \
                SET object_uri = EXCLUDED.object_uri, \
                    \"offset\" = EXCLUDED.\"offset\", \
                    length = EXCLUDED.length, \
                    row_count = EXCLUDED.row_count, \
                    format = EXCLUDED.format, \
                    statistics = EXCLUDED.statistics",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(table_snapshot_segment_uuid)?,
                    SqlValue::uuid_str(table_snapshot_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                    SqlValue::Int64(chunk_idx as i64),
                    SqlValue::Text(object_uri.to_string()),
                    SqlValue::Int64(offset),
                    SqlValue::Int64(length),
                    SqlValue::Int64(row_count),
                    SqlValue::Text(format_text.to_string()),
                    SqlValue::Bytes(statistics.to_vec()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Update the `size_bytes` of a snapshot segment after file write.
    ///
    /// 1 SQL query.
    pub async fn update_snapshot_segment_size(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_segment_uuid: &str,
        size_bytes: i64,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_metadata_table(&catalog);
        let sql = format!(
            "UPDATE {table} SET size_bytes = $1 \
             WHERE branch_uuid = $2 AND table_snapshot_segment_uuid = $3",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Int64(size_bytes),
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_snapshot_segment_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Mark a snapshot segment as committed (phase 2).
    ///
    /// 1 SQL query.
    pub async fn commit_snapshot_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_segment_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_metadata_table(&catalog);
        let sql = format!(
            "UPDATE {table} SET commit_micros = {epoch} \
             WHERE branch_uuid = $1 AND table_snapshot_segment_uuid = $2",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_snapshot_segment_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Delete a snapshot segment only if uncommitted (crash cleanup).
    ///
    /// 1 SQL query.
    pub async fn delete_uncommitted_snapshot_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_segment_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_metadata_table(&catalog);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 AND table_snapshot_segment_uuid = $2 \
               AND commit_micros IS NULL",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_snapshot_segment_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Delete a snapshot metadata row only if uncommitted (crash cleanup).
    ///
    /// 1 SQL query.
    pub async fn delete_uncommitted_snapshot_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_metadata_table(&catalog);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 AND table_snapshot_uuid = $2 \
               AND commit_micros IS NULL",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_snapshot_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Mark a snapshot metadata row as committed (phase 2).
    ///
    /// 1 SQL query.
    pub async fn commit_snapshot_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_metadata_table(&catalog);
        let sql = format!(
            "UPDATE {table} SET commit_micros = {epoch} \
             WHERE branch_uuid = $1 AND table_snapshot_uuid = $2",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_snapshot_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Get `(table_snapshot_segment_uuid, object_uri)` for all snapshots of a
    /// table.
    ///
    /// 1 SQL query.
    /// Every committed-or-not snapshot segment on a branch, across all tables.
    ///
    /// Branch teardown's enumeration. Table-agnostic on purpose: the segment
    /// parent is LIST-partitioned on `branch_uuid`, so scoping by branch alone
    /// is both complete and cheaper than the per-table loop — and, unlike that
    /// loop, it needs no table list, so teardown does not have to resolve one
    /// (a cold-capable read) while holding the parents' `ACCESS EXCLUSIVE`.
    pub async fn get_snapshot_segments_for_branch(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<Vec<(String, String)>> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let seg_name = naming::table_snapshot_segment_metadata_partition(&catalog, &branch);
        let sql = format!(
            "SELECT table_snapshot_segment_uuid, object_uri \
             FROM {seg_table} WHERE branch_uuid = $1",
            seg_table = qi(&seg_name),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(rows
            .iter()
            .map(|r| {
                let uuid: Uuid = r.get("table_snapshot_segment_uuid");
                let uri: String = r.get("object_uri");
                (uuid.to_string(), uri)
            })
            .collect())
    }

    pub async fn get_snapshot_segments_for_table(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
    ) -> Result<Vec<(String, String)>> {
        let catalog = parse_uuid(catalog_uuid);
        let seg_name = naming::table_snapshot_segment_metadata_table(&catalog);
        let snap_name = naming::table_snapshot_metadata_table(&catalog);
        let sql = format!(
            "SELECT seg.table_snapshot_segment_uuid, seg.object_uri \
             FROM {seg_table} seg \
             INNER JOIN {snap_table} snap \
               ON seg.table_snapshot_uuid = snap.table_snapshot_uuid \
              AND seg.branch_uuid = snap.branch_uuid \
             WHERE snap.branch_uuid = $1 \
               AND seg.branch_uuid = $1 \
               AND snap.table_uuid = $2",
            seg_table = qi(&seg_name),
            snap_table = qi(&snap_name),
        );
        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                ],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| {
                let uuid: Uuid = r.get("table_snapshot_segment_uuid");
                let uri: String = r.get("object_uri");
                (uuid.to_string(), uri)
            })
            .collect())
    }

    /// Get `(table_snapshot_segment_uuid, object_uri)` for every
    /// COMMITTED snapshot of a table except the single latest (by
    /// `snapshotted_at_micros`).
    ///
    /// The retirement input (ADR 0024 §4): snapshots are a read-optimization
    /// cache, so when a new snapshot commits every older one is retired and
    /// only the latest serves reads. Bounded time-travel history is persist-log
    /// retention's job. Uncommitted parents are excluded on both sides: an
    /// in-flight snapshot is neither retire-able nor the retirement pivot.
    ///
    /// A deliberate sibling of [`Self::get_snapshot_segments_for_table`] (the
    /// all-snapshots enumeration `DeleteBranch` uses) — a ranked filter, not a
    /// boolean knob on the full enumeration.
    ///
    /// 1 SQL query.
    pub async fn get_retired_snapshot_segments(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
    ) -> Result<Vec<(String, String)>> {
        let catalog = parse_uuid(catalog_uuid);
        let seg_name = naming::table_snapshot_segment_metadata_table(&catalog);
        let snap_name = naming::table_snapshot_metadata_table(&catalog);
        let sql = format!(
            "SELECT seg.table_snapshot_segment_uuid, seg.object_uri \
             FROM {seg_table} seg \
             INNER JOIN {snap_table} snap \
               ON seg.table_snapshot_uuid = snap.table_snapshot_uuid \
              AND seg.branch_uuid = snap.branch_uuid \
             WHERE snap.table_uuid = $1 \
               AND seg.branch_uuid = $2 AND snap.branch_uuid = $2 \
               AND snap.commit_micros IS NOT NULL \
               AND snap.table_snapshot_uuid <> (\
                 SELECT table_snapshot_uuid FROM {snap_table} \
                 WHERE table_uuid = $1 \
                   AND branch_uuid = $2 \
                   AND commit_micros IS NOT NULL \
                 ORDER BY snapshotted_at_micros DESC LIMIT 1\
               )",
            seg_table = qi(&seg_name),
            snap_table = qi(&snap_name),
        );
        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::uuid_str(table_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                ],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| {
                let segment_uuid: Uuid = row.get("table_snapshot_segment_uuid");
                let object_uri: String = row.get("object_uri");
                (segment_uuid.to_string(), object_uri)
            })
            .collect())
    }

    /// Delete snapshot segments by UUID list.
    ///
    /// Used by retirement (ADR 0024 §4). Branch-deletion uses DROP PARTITION
    /// CASCADE instead.
    ///
    /// 1 SQL query.
    pub async fn delete_snapshot_segments_by_uuids(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_segment_uuids: &[String],
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_metadata_table(&catalog);
        let uuid_refs: Vec<&str> = table_snapshot_segment_uuids
            .iter()
            .map(String::as_str)
            .collect();
        let arr = format_sql_uuid_array(&uuid_refs);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 \
               AND table_snapshot_segment_uuid = ANY({arr})",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(())
    }

    /// Carry forward untouched prior-snapshot segments by reference
    /// (ADR 0024 §3): one new `table_snapshot_segment_uuid` per
    /// spec under the NEW snapshot, copying the prior row's storage
    /// columns (`object_uri`, `offset`, `length`, `row_count`,
    /// `size_bytes`, `format`, `metadata`, `statistics`) server-side —
    /// the file is never re-read. Inserts with NULL `commit_micros`
    /// so carried rows ride the same two-phase visibility gate as written
    /// rows.
    ///
    /// The prior rows are read from `source_branch_uuid` and the new
    /// rows are written under `branch_uuid`. The two differ on a fork's
    /// first snapshot, where the child references segments the parent
    /// wrote; they are the same branch for an ordinary same-branch
    /// carry-forward. Splitting them is what keeps a carried row in the
    /// referencing branch's own partition — the `object_uri` still names
    /// the branch that WROTE the file, so ownership is the column, not
    /// the path.
    ///
    /// One `UNNEST … JOIN` query: the three parallel spec arrays (new
    /// uuid, chunk_idx, prior uuid) join the segment table on the prior
    /// uuid + `source_branch_uuid`. Deterministic new uuids
    /// (`table_snapshot_segment_uuid(new_snap, chunk_idx)`) make the
    /// `ON CONFLICT DO UPDATE` crash-retry-idempotent, mirroring
    /// [`Self::insert_snapshot_segment`]. Inline `ARRAY[…]` literals (not
    /// binds) match the established uuid-array idiom
    /// ([`format_sql_uuid_array`]) — there is no array bind in
    /// [`SqlValue`].
    ///
    /// Why carried rows bypass `DurableSegmentWriter`: its
    /// `cleanup_on_err` deletes a group's FILE before its rows, but a
    /// carried row's `object_uri` is a SHARED prior-snapshot file that
    /// must never be deleted on this snapshot's error path. Carried rows
    /// are file-less for cleanup purposes — the caller cleans them up via
    /// [`Self::delete_uncommitted_snapshot_segments_by_uuids`] (row
    /// delete only).
    ///
    /// Invariant the caller owns: carried `chunk_idx` values must be
    /// disjoint from the written segments' `chunk_idx` in the same new
    /// snapshot (the packer's shared counter guarantees this). A
    /// collision would silently overwrite via the `ON CONFLICT DO
    /// UPDATE` rather than erroring.
    ///
    /// 1 SQL query (no-op on empty `specs`).
    pub async fn insert_carried_snapshot_segments(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        source_branch_uuid: &str,
        table_snapshot_uuid: &str,
        specs: &[CarriedSegmentSpec],
    ) -> Result<()> {
        if specs.is_empty() {
            return Ok(());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_metadata_table(&catalog);

        let new_uuids: Vec<&str> = specs.iter().map(|s| s.new_seg_uuid_str.as_str()).collect();
        let prior_uuids: Vec<&str> = specs
            .iter()
            .map(|s| s.prior_seg_uuid_str.as_str())
            .collect();
        let new_arr = format_sql_uuid_array(&new_uuids);
        let prior_arr = format_sql_uuid_array(&prior_uuids);
        let idx_inner: Vec<String> = specs.iter().map(|s| s.chunk_idx.to_string()).collect();
        let idx_arr = format!("ARRAY[{}]::bigint[]", idx_inner.join(","));

        let sql = format!(
            "INSERT INTO {table} \
             (table_snapshot_segment_uuid, table_snapshot_uuid, branch_uuid, \
              table_uuid, chunk_idx, object_uri, \"offset\", length, \
              row_count, size_bytes, format, metadata, statistics) \
             SELECT new.uuid, $1, $2, old.table_uuid, new.idx, \
                    old.object_uri, old.\"offset\", old.length, old.row_count, \
                    old.size_bytes, old.format, old.metadata, old.statistics \
             FROM UNNEST({new_arr}, {idx_arr}, {prior_arr}) \
                  AS new(uuid, idx, old_uuid) \
             JOIN {table} old \
               ON old.table_snapshot_segment_uuid = new.old_uuid \
              AND old.branch_uuid = $3 \
             ON CONFLICT (branch_uuid, table_snapshot_segment_uuid) DO UPDATE \
                SET table_snapshot_uuid = EXCLUDED.table_snapshot_uuid, \
                    chunk_idx = EXCLUDED.chunk_idx, \
                    object_uri = EXCLUDED.object_uri, \
                    \"offset\" = EXCLUDED.\"offset\", \
                    length = EXCLUDED.length, \
                    row_count = EXCLUDED.row_count, \
                    size_bytes = EXCLUDED.size_bytes, \
                    format = EXCLUDED.format, \
                    metadata = EXCLUDED.metadata, \
                    statistics = EXCLUDED.statistics \
             RETURNING table_snapshot_segment_uuid",
            table = qi(&table),
        );
        // RETURNING + a row-count check turns a non-joining prior uuid
        // (a stale/wrong spec, or a prior row retired between the read
        // and this insert) from silent data loss — a snapshot missing a
        // partition — into a loud error the caller rolls back and
        // full-rewrites. `execute_no_result_params` would discard the
        // count.
        let inserted = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::uuid_str(table_snapshot_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(source_branch_uuid)?,
                ],
            )
            .await?;
        if inserted.len() != specs.len() {
            return Err(MetadataError::Db(sqlx::Error::Protocol(format!(
                "carry-forward insert affected {} rows, expected {} — a prior \
                 segment uuid failed to join (retired or stale spec)",
                inserted.len(),
                specs.len()
            ))));
        }
        Ok(())
    }

    /// Commit a batch of snapshot segments by UUID (phase 2, bulk form of
    /// [`Self::commit_snapshot_segment`]). Commits carried rows alongside the
    /// written ones.
    ///
    /// 1 SQL query (no-op on empty input).
    pub async fn commit_snapshot_segments_by_uuids(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_segment_uuids: &[String],
    ) -> Result<()> {
        if table_snapshot_segment_uuids.is_empty() {
            return Ok(());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_metadata_table(&catalog);
        let uuid_refs: Vec<&str> = table_snapshot_segment_uuids
            .iter()
            .map(String::as_str)
            .collect();
        let arr = format_sql_uuid_array(&uuid_refs);
        let sql = format!(
            "UPDATE {table} SET commit_micros = {epoch} \
             WHERE branch_uuid = $1 \
               AND table_snapshot_segment_uuid = ANY({arr})",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(())
    }

    /// Delete a batch of snapshot segments only if still uncommitted
    /// (bulk crash cleanup, mirroring
    /// [`Self::delete_uncommitted_snapshot_segment`]). The
    /// `commit_micros IS NULL` guard makes this safe to call on
    /// the carried + written rows of an aborted snapshot without
    /// touching a concurrently-committed row. Carried rows are file-less
    /// for cleanup (their `object_uri` is a shared prior file), so this
    /// row-only delete is their complete cleanup path.
    ///
    /// 1 SQL query (no-op on empty input).
    pub async fn delete_uncommitted_snapshot_segments_by_uuids(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_segment_uuids: &[String],
    ) -> Result<()> {
        if table_snapshot_segment_uuids.is_empty() {
            return Ok(());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_metadata_table(&catalog);
        let uuid_refs: Vec<&str> = table_snapshot_segment_uuids
            .iter()
            .map(String::as_str)
            .collect();
        let arr = format_sql_uuid_array(&uuid_refs);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 \
               AND table_snapshot_segment_uuid = ANY({arr}) \
               AND commit_micros IS NULL",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(())
    }

    /// Delete snapshot metadata rows that have no remaining segments.
    ///
    /// Used by retirement (ADR 0024 §4) after segment delete to sweep parent
    /// rows orphaned by the segment cleanup.
    ///
    /// 1 SQL query.
    pub async fn delete_orphaned_snapshot_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let snap_name = naming::table_snapshot_metadata_table(&catalog);
        let seg_name = naming::table_snapshot_segment_metadata_table(&catalog);
        // NOT EXISTS, not NOT IN: the segment table's
        // `table_snapshot_uuid` is nullable, and one NULL in a NOT IN
        // subquery NULLs the whole predicate — silently turning this
        // DELETE into a branch-wide no-op. NOT EXISTS is NULL-safe and
        // matches the sweep refcount gate's idiom.
        let sql = format!(
            "DELETE FROM {snap_table} snap \
             WHERE snap.branch_uuid = $1 \
               AND snap.table_uuid = $2 \
               AND NOT EXISTS (\
                 SELECT 1 FROM {seg_table} seg \
                 WHERE seg.branch_uuid = $1 \
                   AND seg.table_snapshot_uuid = snap.table_snapshot_uuid\
               )",
            snap_table = qi(&snap_name),
            seg_table = qi(&seg_name),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Retention floor — the newest `durable` snapshot at/before the
    /// retention window start (`now_micros − retention_duration_seconds ×
    /// 1_000_000`). Returns the floor row's `(commit_seq_num,
    /// snapshotted_at_micros)` so consumers compare on the axis their
    /// `as_of`/`from` arrives on with no micros↔seq mapping (ADR 0025 §3/§5).
    ///
    /// `None` when retention is disabled (`retention_duration_seconds` unset —
    /// no query issued) or when no durable snapshot precedes the window (table
    /// younger than the window); downstream retention ops then no-op.
    ///
    /// The persist prune and snapshot retirement call this directly; the
    /// read path folds the same predicate onto the plan-time `hot_min` round
    /// trip. 0 SQL queries when disabled, else 1.
    pub async fn retention_floor(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        retention_duration_seconds: Option<i64>,
        now_micros: i64,
    ) -> Result<Option<(i64, i64)>> {
        let Some(duration_seconds) = retention_duration_seconds else {
            // Retention disabled ⇒ null floor ⇒ downstream ops keep everything.
            return Ok(None);
        };
        let window_start = now_micros - duration_seconds * 1_000_000;
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_metadata_table(&catalog);
        let sql = retention_floor_select(&qi(&table), "$1", "$2", "$3");
        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                    SqlValue::Int64(window_start),
                ],
            )
            .await?;
        Ok(rows.first().map(|row| {
            let commit_seq_num: i64 = row.get("commit_seq_num");
            let snapshotted_at_micros: i64 = row.get("snapshotted_at_micros");
            (commit_seq_num, snapshotted_at_micros)
        }))
    }
}

/// SQL for the retention-floor pick: the newest `durable` committed snapshot
/// with `snapshotted_at_micros <= {window_start_expr}`, projecting
/// `(commit_seq_num, snapshotted_at_micros)`.
///
/// Shared by [`LifecycleManager::retention_floor`] (window start bound as a
/// parameter) and the plan-time fold onto the `hot_min` round trip (window
/// start computed from the DB clock), so the durable/committed predicate lives
/// in exactly one place. `snapshot_table_sql` is the already-quoted snapshot
/// metadata table; `branch_param`/`table_param` are the `$N` placeholders;
/// `window_start_expr` is the SQL for the window start (a `$N` bind for the
/// helper, a `microsecond_epoch() - duration` expression for the fold).
pub fn retention_floor_select(
    snapshot_table_sql: &str,
    branch_param: &str,
    table_param: &str,
    window_start_expr: &str,
) -> String {
    format!(
        "SELECT commit_seq_num, snapshotted_at_micros FROM {snapshot_table_sql} \
         WHERE branch_uuid = {branch_param} AND table_uuid = {table_param} \
           AND durable AND commit_micros IS NOT NULL \
           AND snapshotted_at_micros <= {window_start_expr} \
         ORDER BY snapshotted_at_micros DESC, commit_seq_num DESC \
         LIMIT 1"
    )
}

/// The retention floor: the newest `durable` snapshot at/before the
/// window start. Both coordinates are carried so each consumer compares on the
/// axis its `as_of`/`from` arrives on — a named pair (not a positional
/// `(i64, i64)`) so the seq and micros axes can't be transposed at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionFloor {
    pub commit_seq_num: i64,
    pub snapshotted_at_micros: i64,
}

/// SQL for the retention window start on the plan-time fold: the DB clock now
/// (`microsecond_epoch()`) minus `retention_duration_seconds`, with the duration
/// bound at `duration_param` (e.g. `"$3"`). Centralized so the read fold
/// (`meta_plan`) and the audit fold (`persist`) share one expression.
pub fn retention_window_start_expr(duration_param: &str) -> String {
    format!(
        "({} - {} * 1000000)",
        PgDialect::microsecond_epoch(),
        duration_param
    )
}
