//! Cold-index *materialization* metadata helpers (ADR 0026 §5),
//! split into a snapshot parent/child pair mirroring `table_snapshot_metadata`
//! → `table_snapshot_segment_metadata`:
//!
//! * `table_snapshot_index_metadata` (PARENT) — one row per `(snapshot,
//!   index)`, a fileless header re-declared fresh each snapshot under the
//!   snapshot's two-phase commit and retired with it.
//! * `table_snapshot_segment_index_metadata` (CHILD) — one row per `(segment,
//!   index)` sidecar; a cold file with its own `(written_at, committed_at)`
//!   two-phase commit, carried forward with its base segment and GC'd via
//!   `segment_delete_set` when the base segment retires.
//!
//! The role discriminator `index_uuid IS NULL` ⇒ the strictly-internal
//! `row_uuid` identity index; non-NULL ⇒ a *declared* index — either a built-in
//! system-table name index (a deterministic [`naming::system_name_index_uuid`]
//! that is itself never a `__penca_system__.indexes` row) or a user secondary
//! index. The built-in name index is deliberately non-NULL so the `row_uuid`
//! read plan's `index_uuid IS NULL` filter excludes it. It lives on the PARENT
//! only; the child references the parent via `table_snapshot_index_uuid`.
//!
//! Every id here is the system's xxh3-in-Rust identity
//! ([`naming::row_uuid_for_pk`] / [`naming::table_snapshot_index_uuid`]) —
//! there are no SQL-side hashes. A carried child reuses the new snapshot's
//! parent id (passed by the caller, the same value the build inserted) plus its
//! own deterministic sidecar id, so build and carry agree and nothing can drift
//! cross-language.

use penca_core::naming;
use penca_db::driver::{DbDriver, SqlType, SqlValue, format_sql_uuid_array};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::helpers::{epoch, parse_uuid, qi};
use crate::{LifecycleManager, Result, SegmentIndexMetadata, TableSnapshotIndexMetadata};

impl LifecycleManager {
    /// Insert a parent index row with NULL `commit_micros` (phase 1) —
    /// the snapshot "has" this index. `index_uuid` is `None` for the internal
    /// `row_uuid` index. `key_columns` is the USER index's declared
    /// key columns, denormalized onto the snapshot-scoped header for planner
    /// covering-index selection (planning reads only snapshot-index metadata,
    /// ADR 0026 §5); `None` for the internal identity index and the built-in
    /// system name indexes. `table_snapshot_index_uuid` is the deterministic id
    /// from [`naming::table_snapshot_index_uuid`] (xxh3 via `row_uuid_for_pk`),
    /// so a phase-1 retry collapses via `ON CONFLICT DO UPDATE`. Mirrors
    /// [`Self::insert_snapshot_metadata`].
    ///
    /// 1 SQL query.
    pub async fn insert_table_snapshot_index(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_index_uuid: &str,
        table_snapshot_uuid: &str,
        index_uuid: Option<&str>,
        key_columns: Option<&[String]>,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_index_metadata_table(&catalog);
        let index_uuid_val = match index_uuid {
            Some(u) => SqlValue::uuid_str(u)?,
            None => SqlValue::Null(SqlType::Uuid),
        };
        let key_columns_val = match key_columns {
            Some(cols) => SqlValue::TextArray(cols.to_vec()),
            None => SqlValue::Null(SqlType::TextArray),
        };
        let sql = format!(
            "INSERT INTO {table} \
             (table_snapshot_index_uuid, branch_uuid, table_snapshot_uuid, index_uuid, \
              key_columns) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (branch_uuid, table_snapshot_index_uuid) DO UPDATE \
                SET table_snapshot_uuid = EXCLUDED.table_snapshot_uuid, \
                    index_uuid = EXCLUDED.index_uuid, \
                    key_columns = EXCLUDED.key_columns",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(table_snapshot_index_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_snapshot_uuid)?,
                    index_uuid_val,
                    key_columns_val,
                ],
            )
            .await?;
        Ok(())
    }

    /// Commit (phase 2) every uncommitted parent index row for a snapshot —
    /// committed in the SAME phase-2 as the snapshot's segments so an index is
    /// never visible out of step with the snapshot it describes.
    ///
    /// 1 SQL query.
    pub async fn commit_table_snapshot_index_for_snapshot(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_index_metadata_table(&catalog);
        let sql = format!(
            "UPDATE {table} SET commit_micros = {epoch} \
             WHERE branch_uuid = $1 AND table_snapshot_uuid = $2 \
               AND commit_micros IS NULL",
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

    /// Delete still-uncommitted parent index rows for a snapshot (crash cleanup
    /// for an aborted snapshot, mirroring
    /// [`Self::delete_uncommitted_snapshot_metadata`]).
    ///
    /// 1 SQL query.
    pub async fn delete_uncommitted_table_snapshot_index_for_snapshot(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_index_metadata_table(&catalog);
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

    /// Reap parent index rows whose snapshot no longer exists (its
    /// `table_snapshot_metadata` row was retired). Run after
    /// [`Self::delete_orphaned_snapshot_metadata`] so a just-retired snapshot's
    /// index headers are reaped in the same cycle — the parent analog of how
    /// retired segments drop their child sidecars. Branch-wide + NULL-safe
    /// `NOT EXISTS`, so it is idempotent and also reaps a crash-orphaned parent.
    ///
    /// 1 SQL query.
    pub async fn delete_orphaned_table_snapshot_index_rows(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let index_table = naming::table_snapshot_index_metadata_table(&catalog);
        let snap_table = naming::table_snapshot_metadata_table(&catalog);
        let sql = format!(
            "DELETE FROM {index_table} tsi \
             WHERE tsi.branch_uuid = $1 \
               AND NOT EXISTS (\
                 SELECT 1 FROM {snap_table} snap \
                 WHERE snap.branch_uuid = $1 \
                   AND snap.table_snapshot_uuid = tsi.table_snapshot_uuid\
               )",
            index_table = qi(&index_table),
            snap_table = qi(&snap_table),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(())
    }

    /// List committed parent index rows for a snapshot — the planning-read API
    /// answering "does snapshot S have index X?". The internal index is the row
    /// with `index_uuid` NULL.
    ///
    /// 1 SQL query.
    pub async fn list_table_snapshot_index(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_snapshot_uuid: &str,
    ) -> Result<Vec<TableSnapshotIndexMetadata>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_index_metadata_table(&catalog);
        let sql = format!(
            "SELECT table_snapshot_index_uuid, index_uuid \
             FROM {table} \
             WHERE branch_uuid = $1 AND table_snapshot_uuid = $2 \
               AND commit_micros IS NOT NULL",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_snapshot_uuid)?,
                ],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| {
                let index_uuid: Option<Uuid> = r.get("index_uuid");
                TableSnapshotIndexMetadata {
                    table_snapshot_index_uuid: r
                        .get::<Uuid, _>("table_snapshot_index_uuid")
                        .to_string(),
                    index_uuid: index_uuid.map(|u| u.to_string()),
                }
            })
            .collect())
    }

    /// Insert a child sidecar row with NULL `commit_micros` (phase 1).
    /// `table_snapshot_index_uuid` references the parent, which carries the
    /// index identity. Mirrors [`Self::insert_snapshot_segment`].
    ///
    /// 1 SQL query.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_segment_index_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        segment_index_uuid: &str,
        segment_uuid: &str,
        table_snapshot_index_uuid: &str,
        object_uri: &str,
        offset: i64,
        length: i64,
        format_text: &str,
        size_bytes: i64,
        statistics: &[u8],
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_index_metadata_table(&catalog);
        let sql = format!(
            "INSERT INTO {table} \
             (segment_index_uuid, branch_uuid, segment_uuid, table_snapshot_index_uuid, \
              object_uri, \"offset\", length, format, size_bytes, statistics) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (branch_uuid, segment_index_uuid) DO UPDATE \
                SET segment_uuid = EXCLUDED.segment_uuid, \
                    table_snapshot_index_uuid = EXCLUDED.table_snapshot_index_uuid, \
                    object_uri = EXCLUDED.object_uri, \
                    \"offset\" = EXCLUDED.\"offset\", \
                    length = EXCLUDED.length, \
                    format = EXCLUDED.format, \
                    size_bytes = EXCLUDED.size_bytes, \
                    statistics = EXCLUDED.statistics",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(segment_index_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(segment_uuid)?,
                    SqlValue::uuid_str(table_snapshot_index_uuid)?,
                    SqlValue::Text(object_uri.to_string()),
                    SqlValue::Int64(offset),
                    SqlValue::Int64(length),
                    SqlValue::Text(format_text.to_string()),
                    SqlValue::Int64(size_bytes),
                    SqlValue::Bytes(statistics.to_vec()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Commit (phase 2) every uncommitted child sidecar attached to the given
    /// base segments. Keyed by `segment_uuid` so the lifecycle commits carried +
    /// freshly-built sidecars in the SAME phase-2 batch as their base segments.
    ///
    /// 1 SQL query (no-op on empty input).
    pub async fn commit_segment_index_metadata_for_segments(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        segment_uuids: &[String],
    ) -> Result<()> {
        if segment_uuids.is_empty() {
            return Ok(());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_index_metadata_table(&catalog);
        let uuid_refs: Vec<&str> = segment_uuids.iter().map(String::as_str).collect();
        let arr = format_sql_uuid_array(&uuid_refs);
        let sql = format!(
            "UPDATE {table} SET commit_micros = {epoch} \
             WHERE branch_uuid = $1 AND segment_uuid = ANY({arr}) \
               AND commit_micros IS NULL",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(())
    }

    /// Delete still-uncommitted child sidecars on the given base segments (crash
    /// cleanup for an aborted snapshot, mirroring
    /// [`Self::delete_uncommitted_snapshot_segments_by_uuids`]). Carried sidecars
    /// reference a shared prior file, so this row-only delete is their complete
    /// cleanup path.
    ///
    /// 1 SQL query (no-op on empty input).
    pub async fn delete_uncommitted_segment_index_for_segments(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        segment_uuids: &[String],
    ) -> Result<()> {
        if segment_uuids.is_empty() {
            return Ok(());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_index_metadata_table(&catalog);
        let uuid_refs: Vec<&str> = segment_uuids.iter().map(String::as_str).collect();
        let arr = format_sql_uuid_array(&uuid_refs);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 AND segment_uuid = ANY({arr}) \
               AND commit_micros IS NULL",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(())
    }

    /// Delete every child sidecar row (committed or not) attached to the given
    /// base segments. Called from snapshot retirement after the sidecars' files
    /// have been enqueued in `segment_delete_set`, so the metadata rows don't
    /// outlive the base segments they reference.
    ///
    /// 1 SQL query (no-op on empty input).
    pub async fn delete_segment_index_metadata_for_segments(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        segment_uuids: &[String],
    ) -> Result<()> {
        if segment_uuids.is_empty() {
            return Ok(());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_index_metadata_table(&catalog);
        let uuid_refs: Vec<&str> = segment_uuids.iter().map(String::as_str).collect();
        let arr = format_sql_uuid_array(&uuid_refs);
        let sql = format!(
            "DELETE FROM {table} WHERE branch_uuid = $1 AND segment_uuid = ANY({arr})",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(())
    }

    /// List committed child sidecar rows for a set of base segments. The
    /// planning-read API (group by `segment_uuid`, probe the matching index);
    /// the lifecycle also uses it to collect a retiring segment's sidecar
    /// `object_uri`s before enqueuing them in `segment_delete_set`.
    ///
    /// 1 SQL query (no-op on empty input).
    pub async fn list_segment_index_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        segment_uuids: &[String],
    ) -> Result<Vec<SegmentIndexMetadata>> {
        if segment_uuids.is_empty() {
            return Ok(Vec::new());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_index_metadata_table(&catalog);
        let uuid_refs: Vec<&str> = segment_uuids.iter().map(String::as_str).collect();
        let arr = format_sql_uuid_array(&uuid_refs);
        let sql = format!(
            "SELECT segment_index_uuid, segment_uuid, table_snapshot_index_uuid, \
                    object_uri, \"offset\", length, format, size_bytes, statistics \
             FROM {table} \
             WHERE branch_uuid = $1 AND segment_uuid = ANY({arr}) \
               AND commit_micros IS NOT NULL",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(rows
            .iter()
            .map(|r| SegmentIndexMetadata {
                segment_index_uuid: r.get::<Uuid, _>("segment_index_uuid").to_string(),
                segment_uuid: r.get::<Uuid, _>("segment_uuid").to_string(),
                table_snapshot_index_uuid: r
                    .get::<Uuid, _>("table_snapshot_index_uuid")
                    .to_string(),
                object_uri: r.get("object_uri"),
                offset: r.get("offset"),
                length: r.get("length"),
                format: r.get("format"),
                size_bytes: r.get::<Option<i64>, _>("size_bytes").unwrap_or(0),
                statistics: r
                    .get::<Option<Vec<u8>>, _>("statistics")
                    .unwrap_or_default(),
            })
            .collect())
    }

    /// Every sidecar `object_uri` for a set of base segments — committed AND
    /// uncommitted — for branch teardown's delete-set enqueue.
    ///
    /// Deliberately unfiltered, unlike [`Self::list_segment_index_metadata`].
    /// Teardown's `DROP TABLE … CASCADE` removes a branch's sidecar rows
    /// regardless of their commit state, so a sidecar whose phase-2 stamp had
    /// not landed loses its row; under enqueue-only teardown, a URI that never
    /// reached `segment_delete_set` is never collected by anything. The two
    /// sibling enumerations teardown pairs this with
    /// ([`Self::get_table_persist_segments_for_tables`],
    /// [`Self::get_snapshot_segments_for_table`]) carry no commit predicate for
    /// the same reason.
    ///
    /// This is the read-side counterpart of the asymmetry `retire` already
    /// handles on the write side, where a committed-only list is paired with an
    /// unconditional `delete_segment_index_metadata_for_segments`.
    ///
    /// 1 SQL query (no-op on empty input).
    pub async fn list_all_segment_index_uris(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        segment_uuids: &[String],
    ) -> Result<Vec<String>> {
        if segment_uuids.is_empty() {
            return Ok(Vec::new());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_index_metadata_table(&catalog);
        let uuid_refs: Vec<&str> = segment_uuids.iter().map(String::as_str).collect();
        let arr = format_sql_uuid_array(&uuid_refs);
        let sql = format!(
            "SELECT object_uri FROM {table} \
             WHERE branch_uuid = $1 AND segment_uuid = ANY({arr})",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(rows.iter().map(|r| r.get("object_uri")).collect())
    }

    /// Carry forward the internal `row_uuid` sidecar of each carried base
    /// segment (CHA-406): for each `(new_seg ← prior_seg)` pair, copy the prior
    /// segment's committed sidecar to a new row under the new segment, NULL-
    /// committed (committed in the same phase-2 as the base segment via
    /// [`Self::commit_segment_index_metadata_for_segments`]). The file is never
    /// re-read — `object_uri`/`offset`/`length` are copied by reference. Mirrors
    /// [`Self::insert_carried_snapshot_segments`].
    ///
    /// Every id is the system's xxh3-in-Rust identity ([`naming::row_uuid_for_pk`]),
    /// never a SQL-side hash:
    /// - the new sidecar's `segment_index_uuid` is `row_uuid_for_pk(new_seg,
    ///   [index_slug])` — **identical to what a fresh build of that segment
    ///   produces** for the same index, so build and carry agree on a segment's
    ///   sidecar id and a crash-retry collapses via `ON CONFLICT`;
    /// - the prior sidecar is located by its own `row_uuid_for_pk(prior_seg,
    ///   [index_slug])`;
    /// - the carried row's `table_snapshot_index_uuid` is the caller's
    ///   `new_parent_index_uuid` (the new snapshot's re-declared parent for this
    ///   index — the same value the build inserted), so there is no parent JOIN
    ///   and no cross-language hash contract.
    ///
    /// `index_slug` is the per-index sidecar-id discriminator: the internal
    /// `row_uuid` index passes `"row_uuid"`; a user secondary index (CHA-483)
    /// passes its `index_uuid` string. Callers loop once per index/parent. The
    /// INNER JOIN carries a row only where the prior snapshot actually committed
    /// that index's sidecar for the segment — a newly-active index's
    /// not-yet-covered segments produce no carried row and are built fresh by
    /// the lifecycle (materialize-on-next-snapshot).
    ///
    /// The prior sidecars are read from `source_branch_uuid` and the new rows
    /// are written under `branch_uuid`, mirroring
    /// [`Self::insert_carried_snapshot_segments`]. They differ on a fork's
    /// first snapshot and are the same branch otherwise.
    ///
    /// 1 SQL query (no-op on empty input). `pairs`: `(new_seg, prior_seg)`.
    pub async fn insert_carried_segment_indexes(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        source_branch_uuid: &str,
        new_parent_index_uuid: &str,
        index_slug: &str,
        pairs: &[(String, String)],
    ) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_snapshot_segment_index_metadata_table(&catalog);
        // Resolve every id in Rust (xxh3 via row_uuid_for_pk) — never md5 in SQL.
        // The new sidecar id matches a fresh build of new_seg for this index; the
        // prior sidecar is found by its own id.
        let new_segs: Vec<String> = pairs.iter().map(|(n, _)| n.clone()).collect();
        let new_sidecars: Vec<String> = pairs
            .iter()
            .map(|(n, _)| naming::row_uuid_for_pk(&parse_uuid(n), &[index_slug]).to_string())
            .collect();
        let prior_sidecars: Vec<String> = pairs
            .iter()
            .map(|(_, p)| naming::row_uuid_for_pk(&parse_uuid(p), &[index_slug]).to_string())
            .collect();
        let new_seg_refs: Vec<&str> = new_segs.iter().map(String::as_str).collect();
        let new_sidecar_refs: Vec<&str> = new_sidecars.iter().map(String::as_str).collect();
        let prior_sidecar_refs: Vec<&str> = prior_sidecars.iter().map(String::as_str).collect();
        let new_seg_arr = format_sql_uuid_array(&new_seg_refs);
        let new_sidecar_arr = format_sql_uuid_array(&new_sidecar_refs);
        let prior_sidecar_arr = format_sql_uuid_array(&prior_sidecar_refs);
        let sql = format!(
            "INSERT INTO {table} \
             (segment_index_uuid, branch_uuid, segment_uuid, table_snapshot_index_uuid, \
              object_uri, \"offset\", length, format, size_bytes, statistics) \
             SELECT n.new_sidecar, $1, n.new_seg, $2, \
                    old.object_uri, old.\"offset\", old.length, old.format, \
                    old.size_bytes, old.statistics \
             FROM UNNEST({new_seg_arr}, {new_sidecar_arr}, {prior_sidecar_arr}) \
                    AS n(new_seg, new_sidecar, prior_sidecar) \
             JOIN {table} old \
               ON old.branch_uuid = $3 \
              AND old.segment_index_uuid = n.prior_sidecar \
              AND old.commit_micros IS NOT NULL \
             ON CONFLICT (branch_uuid, segment_index_uuid) DO UPDATE \
                SET segment_uuid = EXCLUDED.segment_uuid, \
                    table_snapshot_index_uuid = EXCLUDED.table_snapshot_index_uuid, \
                    object_uri = EXCLUDED.object_uri, \
                    \"offset\" = EXCLUDED.\"offset\", \
                    length = EXCLUDED.length, \
                    format = EXCLUDED.format, \
                    size_bytes = EXCLUDED.size_bytes, \
                    statistics = EXCLUDED.statistics",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(new_parent_index_uuid)?,
                    SqlValue::uuid_str(source_branch_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }
}
