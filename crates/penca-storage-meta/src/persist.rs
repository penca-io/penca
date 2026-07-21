//! `table_persist_metadata` + `table_persist_segment_metadata`
//! two-phase commit helpers, the persist watermark queries that feed
//! `lifecycle.rs::persist_locked` / `plan()`, and the unsealed-segment
//! scope/enumeration helpers consumed by `compact_persist_segments`
//! and `DeleteBranch`. Compact 2PC, seal flips, `segment_delete_set`
//! GC, and snapshot-segment seal live in `compact.rs`. Purge 2PC,
//! purge watermarks, and ADR-0021 aborted-hot-row cleanup live in
//! `purge.rs`.

use penca_core::log_kind::ParseLogKindError;
use penca_core::{LogKind, naming};
use penca_db::driver::{DbDriver, SqlValue, format_sql_text_array, format_sql_uuid_array};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::helpers::{epoch, parse_uuid, qi};
use crate::{LifecycleManager, MetadataError, Result, RetentionFloor};

impl LifecycleManager {
    /// Insert a `table_persist_metadata` row with NULL `commit_micros`
    /// (phase 1 of two-phase commit).
    ///
    /// CHA-203: `persisted_at_micros` and `log_kind` are part of the
    /// deterministic `table_persist_uuid` derivation (one row per
    /// `(catalog, branch, table, persisted_at, log_kind)`); `log_kind`
    /// is CHECK-restricted to [`LogKind::as_str`] values. Phase-1
    /// retries with identical inputs replay to the same PK and slot
    /// in via `DO UPDATE` (no-op write — `EXCLUDED.*` equals the
    /// existing column values when the UUID matches).
    ///
    /// 1 SQL query.
    pub async fn insert_table_persist(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        table_persist_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        persisted_at_micros: i64,
        log_kind: LogKind,
        // CHA-443 (IMPL-1): the persist seq watermark = MAX(commit_seq_num) over the
        // committed rows this persist moved cold — the seq analog of
        // persisted_at_micros. `None` on the aborts-only branch (no committed
        // rows persisted) → SQL NULL, so IMPL-4's MAX(commit_seq_num) ignores it.
        commit_seq_num: Option<i64>,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_metadata_table(&catalog);
        let sql = format!(
            "INSERT INTO {table} \
             (table_persist_uuid, branch_uuid, table_uuid, \
              persisted_at_micros, log_kind, commit_seq_num) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (branch_uuid, table_persist_uuid) DO UPDATE \
                SET persisted_at_micros = EXCLUDED.persisted_at_micros, \
                    log_kind = EXCLUDED.log_kind, \
                    commit_seq_num = EXCLUDED.commit_seq_num",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(table_persist_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                    SqlValue::Int64(persisted_at_micros),
                    SqlValue::Text(log_kind.as_str().to_string()),
                    SqlValue::from_opt_i64(commit_seq_num),
                ],
            )
            .await?;
        Ok(())
    }

    /// Mark a `table_persist_metadata` row committed (phase 2).
    ///
    /// 1 SQL query.
    pub async fn commit_table_persist(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_persist_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_metadata_table(&catalog);
        let sql = format!(
            "UPDATE {table} SET commit_micros = {epoch} \
             WHERE branch_uuid = $1 AND table_persist_uuid = $2",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_persist_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Delete a `table_persist_metadata` row only if uncommitted (crash cleanup).
    ///
    /// 1 SQL query.
    pub async fn delete_uncommitted_table_persist(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_persist_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_metadata_table(&catalog);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 AND table_persist_uuid = $2 \
               AND commit_micros IS NULL",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_persist_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Largest `persisted_at_micros` across committed `table_persist_metadata`
    /// rows for `(branch, table)`. CHA-233 (ADR 0019): feeds two consumers:
    /// `purge_locked`'s hot-row delete watermark, and — via
    /// `hot_min_commit_micros` — `plan()`'s hot↔cold visibility cutoff.
    ///
    /// `Ok(None)` when no committed persist has happened yet for this table.
    ///
    /// 1 SQL query (partition-pruned by `branch_uuid`; served by the
    /// `(table_uuid, log_kind)` index in `pg.rs` via the table-uuid leg).
    pub async fn latest_committed_table_persist_watermark(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
    ) -> Result<Option<i64>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_metadata_table(&catalog);
        let sql = format!(
            "SELECT MAX(persisted_at_micros) AS watermark FROM {table} \
             WHERE branch_uuid = $1 \
               AND table_uuid = $2 \
               AND commit_micros IS NOT NULL",
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
        Ok(rows
            .first()
            .and_then(|r| r.try_get::<Option<i64>, _>("watermark").ok().flatten()))
    }

    /// Largest `commit_seq_num` (`W_persist` / the persist watermark `P`) across
    /// committed `table_persist_metadata` rows for `(branch, table)` — the
    /// seq-axis sibling of [`Self::latest_committed_table_persist_watermark`].
    ///
    /// CHA-444 (ADR 0027) moved the hot↔cold read fence off `W_persist` onto
    /// the purge watermark `Pu`, so this has **no callers today**. Retained for
    /// CHA-466: its memory-shedding trigger slides `Pu` up toward
    /// `P − hot_grace`, and this reader is `P`. (Pre-CHA-444 it was the fence:
    /// cold served `commit_seq_num <= W_persist`, hot `> W_persist`; the seq
    /// partition is exact, so it needs no `+ 1` clamp.)
    ///
    /// `commit_seq_num` is a nullable column (CHA-428 backfilled it; aborts-only
    /// persist rows leave it NULL), so `MAX` skips the NULLs and `Ok(None)`
    /// still means "no committed persist with a seq yet" for this table.
    ///
    /// 1 SQL query (partition-pruned by `branch_uuid`; same index leg as the
    /// micros watermark).
    // CHA-353: trace span isolates this PG round-trip for the read-plan
    // busy/idle decomposition once CHA-466 wires it back in (no caller today).
    // Dormant under the default `penca=debug`; enable with
    // `…,penca_storage_meta=trace` + `PENCA_SPAN_TIMING=1` to time it.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn latest_committed_table_persist_seq_watermark(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
    ) -> Result<Option<i64>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_metadata_table(&catalog);
        let sql = format!(
            "SELECT MAX(commit_seq_num) AS watermark FROM {table} \
             WHERE branch_uuid = $1 \
               AND table_uuid = $2 \
               AND commit_micros IS NOT NULL",
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
        Ok(rows
            .first()
            .and_then(|r| r.try_get::<Option<i64>, _>("watermark").ok().flatten()))
    }

    /// Hot-side `commit_micros >= X` filter value implementing
    /// the ADR 0019 hot↔cold visibility cutoff: `max_persisted + 1`,
    /// or `0` if no persist has been committed for this `(branch,
    /// table)` yet. Cold serves rows with `committed_at <
    /// returned_value` (`<= max_persisted`); hot serves rows with
    /// `committed_at >= returned_value` (`> max_persisted`).
    ///
    /// `plan()` consults this for the hot lower bound; `audit_data`
    /// uses it as a strict tier partition. Between Persist and Purge
    /// the same rows physically live in both tiers, but the plan
    /// filter `>= max_persisted + 1` structurally excludes the
    /// pre-cutoff hot rows from the hot side. The universal grace
    /// window on Purge (ADR 0019) ensures those rows are still
    /// physically present in hot when a concurrent
    /// pre-cutoff-pinned plan executes.
    /// The audit hot/cold cutoff (`MAX(persisted_at_micros) + 1`, or 0
    /// pre-Persist). CHA-433: when `retention_duration_seconds` is `Some`, the
    /// retention floor is folded onto the SAME round trip and returned as a
    /// [`RetentionFloor`] — `plan_audit` enforces the `from < floor` check +
    /// unset-from clamp on it. `None` keeps the single delegated watermark read
    /// (floor `None`).
    // CHA-353: trace span isolates this PG round-trip in a read-plan
    // decomposition (busy vs idle). Dormant under the default
    // `penca=debug`; enable with `…,penca_storage_meta=trace` +
    // `PENCA_SPAN_TIMING=1` to time it.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn hot_min_commit_micros(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        retention_duration_seconds: Option<i64>,
    ) -> Result<(i64, Option<RetentionFloor>)> {
        let Some(duration_seconds) = retention_duration_seconds else {
            let hot_min = Self::latest_committed_table_persist_watermark(
                driver,
                catalog_uuid,
                branch_uuid,
                table_uuid,
            )
            .await?
            .map(|p| p + 1)
            .unwrap_or(0);
            return Ok((hot_min, None));
        };
        // Retention enabled: fold the floor onto the watermark read as its OWN
        // combined query (window start from the DB clock). Deliberately NOT
        // `latest_committed_table_persist_watermark` — lifecycle ops call that
        // without a floor, so overloading it would ripple. Reuses the shared
        // `retention_floor_select` predicate.
        let catalog = parse_uuid(catalog_uuid);
        let persist_name = naming::table_persist_metadata_table(&catalog);
        let snap_name = naming::table_snapshot_metadata_table(&catalog);
        let window_start = crate::retention_window_start_expr("$3");
        let floor_select =
            crate::retention_floor_select(&qi(&snap_name), "$1", "$2", &window_start);
        let sql = format!(
            "SELECT base.persist_wm, f.commit_seq_num AS floor_seq, \
                    f.snapshotted_at_micros AS floor_micros \
             FROM ( \
                 SELECT MAX(persisted_at_micros) AS persist_wm FROM {persist} \
                 WHERE branch_uuid = $1 AND table_uuid = $2 AND commit_micros IS NOT NULL \
             ) base \
             LEFT JOIN LATERAL ({floor_select}) f ON TRUE",
            persist = qi(&persist_name),
        );
        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                    SqlValue::Int64(duration_seconds),
                ],
            )
            .await?;
        let row = rows.first();
        let hot_min = row
            .and_then(|r| r.try_get::<Option<i64>, _>("persist_wm").ok().flatten())
            .map_or(0, |p| p + 1);
        let floor = row.and_then(|r| {
            let commit_seq_num = r.try_get::<Option<i64>, _>("floor_seq").ok().flatten()?;
            let snapshotted_at_micros =
                r.try_get::<Option<i64>, _>("floor_micros").ok().flatten()?;
            Some(RetentionFloor {
                commit_seq_num,
                snapshotted_at_micros,
            })
        });
        Ok((hot_min, floor))
    }

    /// Insert a table-persist segment with `commit_micros` as NULL
    /// (phase 1 of two-phase commit).
    ///
    /// CHA-203: `log_kind` lives only on the parent
    /// (`table_persist_metadata`) — segments JOIN up to classify. Phase-1
    /// retries with identical inputs replay to the same deterministic
    /// `table_persist_segment_uuid` and slot in via `DO UPDATE` (refreshes
    /// the storage-side columns; `object_uri` may move under compact).
    ///
    /// 1 SQL query.
    pub async fn insert_table_persist_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        table_persist_segment_uuid: &str,
        table_persist_uuid: &str,
        chunk_idx: u32,
        min_tx_commit_micros: i64,
        max_tx_commit_micros: i64,
        min_commit_seq_num: i64,
        max_commit_seq_num: i64,
        object_uri: &str,
        row_count: i64,
        format_text: &str,
        statistics: &[u8],
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_segment_metadata_table(&catalog);
        // CHA-430: min/max_commit_seq_num are stamped alongside the
        // committed_at bounds and, like them, are NOT refreshed by the
        // DO UPDATE — compact re-points storage location only and
        // preserves the original commit-order bounds.
        let sql = format!(
            "INSERT INTO {table} \
             (table_persist_segment_uuid, table_persist_uuid, branch_uuid, table_uuid, \
              chunk_idx, min_tx_commit_micros, max_tx_commit_micros, \
              min_commit_seq_num, max_commit_seq_num, \
              object_uri, row_count, format, statistics) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
             ON CONFLICT (branch_uuid, table_persist_segment_uuid) DO UPDATE \
                SET object_uri = EXCLUDED.object_uri, \
                    row_count = EXCLUDED.row_count, \
                    format = EXCLUDED.format, \
                    statistics = EXCLUDED.statistics",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(table_persist_segment_uuid)?,
                    SqlValue::uuid_str(table_persist_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_uuid)?,
                    SqlValue::Int64(chunk_idx as i64),
                    SqlValue::Int64(min_tx_commit_micros),
                    SqlValue::Int64(max_tx_commit_micros),
                    SqlValue::Int64(min_commit_seq_num),
                    SqlValue::Int64(max_commit_seq_num),
                    SqlValue::Text(object_uri.to_string()),
                    SqlValue::Int64(row_count),
                    SqlValue::Text(format_text.to_string()),
                    SqlValue::Bytes(statistics.to_vec()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Re-point an existing `table_persist_segment_metadata` row at a
    /// merged cold file via `(object_uri, offset, length)` slicing.
    /// Used by `compact_persist_segments` to consolidate N small files
    /// into one without churning metadata rows: the row keeps its
    /// `table_persist_segment_uuid`, `table_persist_uuid`,
    /// `(min, max)_tx_commit_micros`, **and**
    /// `commit_micros` (the row stays committed and visible to
    /// reads throughout — only the storage location changes).
    ///
    /// Mirrors snapshot compact's planned shape and reuses the
    /// already-existing `offset` / `length` columns on
    /// `table_persist_segment_metadata`. Reads honor the slice via
    /// `FormatReader::read_segment`.
    ///
    /// **Visibility:** the UPDATE deliberately does *not* touch
    /// `commit_micros`. The compact caller writes the merged
    /// file before calling this (write → UPDATE order), then batches
    /// every per-row UPDATE inside one transaction. Tx commit is the
    /// single visibility boundary: readers see either the pre-compact
    /// layout (every row at its original URI) or the post-compact
    /// layout (every row sliced into the merged file). No invisible
    /// intermediate state and no per-row visibility hole.
    ///
    /// `seal_now`: when `true`, also set `is_sealed = true` atomically
    /// with the URI rewrite. Used on the seal-and-start-new boundary
    /// of the active+sealed compact algorithm — the prior active's
    /// rows transition out of the unsealed set in the same UPDATE
    /// that points them at the new merged file. When `false`, the
    /// row stays unsealed (the normal extend-active path).
    ///
    /// 1 SQL query (per row — caller batches inside one tx).
    pub async fn repoint_table_persist_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_persist_segment_uuid: &str,
        object_uri: &str,
        offset: i64,
        length: i64,
        size_bytes: i64,
        format_text: &str,
        statistics: &[u8],
        seal_now: bool,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_segment_metadata_table(&catalog);
        let seal_clause = if seal_now { ", is_sealed = TRUE" } else { "" };
        let sql = format!(
            "UPDATE {table} SET \
                object_uri = $1, \
                \"offset\" = $2, \
                length = $3, \
                size_bytes = $4, \
                format = $5, \
                statistics = $6{seal_clause} \
             WHERE branch_uuid = $7 \
               AND table_persist_segment_uuid = $8",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Text(object_uri.to_string()),
                    SqlValue::Int64(offset),
                    SqlValue::Int64(length),
                    SqlValue::Int64(size_bytes),
                    SqlValue::Text(format_text.to_string()),
                    SqlValue::Bytes(statistics.to_vec()),
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_persist_segment_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Update the `size_bytes` of a table-persist segment after file write.
    ///
    /// 1 SQL query.
    pub async fn update_table_persist_segment_size(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_persist_segment_uuid: &str,
        size_bytes: i64,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_segment_metadata_table(&catalog);
        let sql = format!(
            "UPDATE {table} SET size_bytes = $1 \
             WHERE branch_uuid = $2 AND table_persist_segment_uuid = $3",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Int64(size_bytes),
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_persist_segment_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Mark a table-persist segment as committed (phase 2 of two-phase commit).
    ///
    /// 1 SQL query.
    pub async fn commit_table_persist_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_persist_segment_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_segment_metadata_table(&catalog);
        let sql = format!(
            "UPDATE {table} SET commit_micros = {epoch} \
             WHERE branch_uuid = $1 AND table_persist_segment_uuid = $2",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_persist_segment_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Delete a table-persist segment only if uncommitted (crash cleanup).
    ///
    /// 1 SQL query.
    pub async fn delete_uncommitted_table_persist_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_persist_segment_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_segment_metadata_table(&catalog);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 AND table_persist_segment_uuid = $2 \
               AND commit_micros IS NULL",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(table_persist_segment_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Return every distinct `(table_uuid, log_kind)` scope on a
    /// branch that has at least one `is_sealed = false` row in
    /// `table_persist_segment_metadata`. Used by
    /// `compact_persist_segments` to enumerate the scopes that are
    /// candidates for a compact wave on this branch.
    ///
    /// `min_persisted_at_micros` / `max_persisted_at_micros` filter on
    /// `seg.commit_micros` when provided (the per-segment persist
    /// watermark — `persisted_at` in the public proto). A scope shows up
    /// only if it has at least one row in the filter window.
    ///
    /// 1 SQL query (partition-pruned by `branch_uuid` and served by
    /// the partial `is_sealed = false` index per CHA-202).
    pub async fn list_unsealed_persist_scopes_on_branch(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        min_persisted_at_micros: Option<i64>,
        max_persisted_at_micros: Option<i64>,
    ) -> Result<Vec<(Uuid, LogKind)>> {
        let catalog = parse_uuid(catalog_uuid);
        let seg_table = naming::table_persist_segment_metadata_table(&catalog);
        let tfm_table = naming::table_persist_metadata_table(&catalog);
        let mut sql = format!(
            "SELECT DISTINCT seg.table_uuid, tfm.log_kind \
             FROM {seg} seg \
             INNER JOIN {tfm} tfm \
               ON seg.table_persist_uuid = tfm.table_persist_uuid \
              AND seg.branch_uuid = tfm.branch_uuid \
             WHERE seg.branch_uuid = $1 \
               AND seg.is_sealed = FALSE \
               AND seg.commit_micros IS NOT NULL",
            seg = qi(&seg_table),
            tfm = qi(&tfm_table),
        );
        let mut params: Vec<SqlValue> = vec![SqlValue::uuid_str(branch_uuid)?];
        if let Some(min) = min_persisted_at_micros {
            params.push(SqlValue::Int64(min));
            sql.push_str(&format!(" AND seg.commit_micros >= ${}", params.len()));
        }
        if let Some(max) = max_persisted_at_micros {
            params.push(SqlValue::Int64(max));
            sql.push_str(&format!(" AND seg.commit_micros < ${}", params.len()));
        }
        sql.push_str(" ORDER BY seg.table_uuid, tfm.log_kind");
        let rows = driver.execute_params(&sql, &params).await?;
        let mut out: Vec<(Uuid, LogKind)> = Vec::with_capacity(rows.len());
        for r in &rows {
            let t: Uuid = r.get("table_uuid");
            let k_text: String = r.get("log_kind");
            let k: LogKind = k_text.parse().map_err(|e: ParseLogKindError| {
                MetadataError::Db(sqlx::Error::Protocol(format!(
                    "unknown log_kind '{}' from table_persist_metadata",
                    e.0
                )))
            })?;
            out.push((t, k));
        }
        Ok(out)
    }

    /// Table-scoped variant of
    /// [`Self::list_unsealed_persist_scopes_on_branch`]. Returns the
    /// distinct `log_kind`s with at least one `is_sealed = false`
    /// `table_persist_segment_metadata` row for `(branch, table)`
    /// in the filter window. Result is at most 2 (`upsert_log` +
    /// `delete_log`).
    ///
    /// CHA-220: under CHA-154's per-table scheduler, the branch-wide
    /// helper would scan every scope on the branch for every table
    /// compact — O(N²) on `branch.table_count`. This variant pushes
    /// the `table_uuid` filter into the SQL so partition pruning
    /// plus the `(table_uuid, log_kind)` filter index resolve the
    /// query without enumerating other tables' scopes.
    ///
    /// 1 SQL query.
    pub async fn list_unsealed_persist_scopes_on_table(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        min_persisted_at_micros: Option<i64>,
        max_persisted_at_micros: Option<i64>,
    ) -> Result<Vec<LogKind>> {
        let catalog = parse_uuid(catalog_uuid);
        let seg_table = naming::table_persist_segment_metadata_table(&catalog);
        let tfm_table = naming::table_persist_metadata_table(&catalog);
        let mut sql = format!(
            "SELECT DISTINCT tfm.log_kind \
             FROM {seg} seg \
             INNER JOIN {tfm} tfm \
               ON seg.table_persist_uuid = tfm.table_persist_uuid \
              AND seg.branch_uuid = tfm.branch_uuid \
             WHERE seg.branch_uuid = $1 \
               AND seg.table_uuid = $2 \
               AND seg.is_sealed = FALSE \
               AND seg.commit_micros IS NOT NULL",
            seg = qi(&seg_table),
            tfm = qi(&tfm_table),
        );
        let mut params: Vec<SqlValue> = vec![
            SqlValue::uuid_str(branch_uuid)?,
            SqlValue::uuid_str(table_uuid)?,
        ];
        if let Some(min) = min_persisted_at_micros {
            params.push(SqlValue::Int64(min));
            sql.push_str(&format!(" AND seg.commit_micros >= ${}", params.len()));
        }
        if let Some(max) = max_persisted_at_micros {
            params.push(SqlValue::Int64(max));
            sql.push_str(&format!(" AND seg.commit_micros < ${}", params.len()));
        }
        sql.push_str(" ORDER BY tfm.log_kind");
        let rows = driver.execute_params(&sql, &params).await?;
        let mut out: Vec<LogKind> = Vec::with_capacity(rows.len());
        for r in &rows {
            let k_text: String = r.get("log_kind");
            let k: LogKind = k_text.parse().map_err(|e: ParseLogKindError| {
                MetadataError::Db(sqlx::Error::Protocol(format!(
                    "unknown log_kind '{}' from table_persist_metadata",
                    e.0
                )))
            })?;
            out.push(k);
        }
        Ok(out)
    }

    /// Enumerate every `is_sealed = false` `table_persist_segment_metadata`
    /// row on a single `(branch, table_uuid, log_kind)` scope. JOINed
    /// to `table_persist_metadata` so the caller can filter on
    /// `log_kind` directly.
    ///
    /// `for_update`: when `true`, the rows are returned under row
    /// locks (`SELECT ... FOR UPDATE OF seg`). The CHA-202 compact
    /// algorithm plans inside the merge tx — the unlocked variant is
    /// for tests + diagnostics only.
    ///
    /// `min_persisted_at_micros` / `max_persisted_at_micros` filter on
    /// `seg.commit_micros` (per-segment persist watermark) when
    /// provided.
    ///
    /// 1 SQL query.
    #[allow(clippy::too_many_arguments)]
    pub async fn enumerate_unsealed_persist_segments_for_scope(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        log_kind: LogKind,
        min_persisted_at_micros: Option<i64>,
        max_persisted_at_micros: Option<i64>,
        for_update: bool,
    ) -> Result<Vec<PgRow>> {
        let catalog = parse_uuid(catalog_uuid);
        let seg_table = naming::table_persist_segment_metadata_table(&catalog);
        let tfm_table = naming::table_persist_metadata_table(&catalog);
        let mut sql = format!(
            "SELECT seg.table_persist_segment_uuid, seg.table_persist_uuid, seg.object_uri, \
                    seg.\"offset\", seg.length, seg.format, seg.row_count, seg.size_bytes, \
                    seg.table_uuid, seg.min_tx_commit_micros, seg.max_tx_commit_micros, \
                    seg.is_sealed, tfm.log_kind \
             FROM {seg} seg \
             INNER JOIN {tfm} tfm \
               ON seg.table_persist_uuid = tfm.table_persist_uuid \
              AND seg.branch_uuid = tfm.branch_uuid \
             WHERE seg.branch_uuid = $1 \
               AND seg.table_uuid = $2 \
               AND tfm.log_kind = $3 \
               AND seg.is_sealed = FALSE \
               AND seg.commit_micros IS NOT NULL",
            seg = qi(&seg_table),
            tfm = qi(&tfm_table),
        );
        let mut params: Vec<SqlValue> = vec![
            SqlValue::uuid_str(branch_uuid)?,
            SqlValue::uuid_str(table_uuid)?,
            SqlValue::Text(log_kind.as_str().to_string()),
        ];
        if let Some(min) = min_persisted_at_micros {
            params.push(SqlValue::Int64(min));
            sql.push_str(&format!(" AND seg.commit_micros >= ${}", params.len()));
        }
        if let Some(max) = max_persisted_at_micros {
            params.push(SqlValue::Int64(max));
            sql.push_str(&format!(" AND seg.commit_micros < ${}", params.len()));
        }
        // ORDER BY min_tx_commit_micros for determinism + slice
        // -layout locality: rows pointing at the current active merged
        // file inherited the lowest tx watermarks (they were the
        // inputs to the prior compact wave) and so sort to the head;
        // subsequent uncompacted segments arrived from later persists
        // and follow. `plan_wave` identifies the active by URI count,
        // not by run-detection on this order, so the consecutive-active
        // -rows property is incidental — keep it for slice-layout
        // locality and deterministic planning, not as a correctness
        // invariant.
        sql.push_str(" ORDER BY seg.min_tx_commit_micros, seg.chunk_idx");
        if for_update {
            sql.push_str(" FOR UPDATE OF seg");
        }
        let rows = driver.execute_params(&sql, &params).await?;
        Ok(rows)
    }

    /// Return `(table_persist_segment_uuid, object_uri)` for every segment
    /// belonging to any of the given tables on a branch.
    ///
    /// CHA-203: keyed on `(branch_uuid, table_uuid IN (...))` on the
    /// segment table directly. Replaces the pre-CHA-203
    /// `get_table_persist_segments_by_hot_names` which keyed on the
    /// dropped `hot_storage_table_name` column. Used by DeleteBranch
    /// to enumerate every cold file the branch owns.
    ///
    /// 1 SQL query.
    pub async fn get_table_persist_segments_for_tables(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuids: &[&str],
    ) -> Result<Vec<(String, String)>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_segment_metadata_table(&catalog);
        if table_uuids.is_empty() {
            return Ok(Vec::new());
        }
        let arr = format_sql_text_array(table_uuids);
        let sql = format!(
            "SELECT table_persist_segment_uuid, object_uri \
             FROM {table} \
             WHERE branch_uuid = $1 \
               AND table_uuid::text = ANY({arr})",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(rows
            .iter()
            .map(|r| {
                let uuid: Uuid = r.get("table_persist_segment_uuid");
                let uri: String = r.get("object_uri");
                (uuid.to_string(), uri)
            })
            .collect())
    }

    /// Delete table-persist segments by UUID list.
    ///
    /// Used by retention-driven prune (CHA-49). Branch-deletion uses
    /// DROP PARTITION CASCADE instead.
    ///
    /// 1 SQL query.
    pub async fn delete_table_persist_segments_by_uuids(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_persist_segment_uuids: &[String],
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::table_persist_segment_metadata_table(&catalog);
        let segment_uuid_refs: Vec<&str> = table_persist_segment_uuids
            .iter()
            .map(String::as_str)
            .collect();
        let arr = format_sql_uuid_array(&segment_uuid_refs);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 \
               AND table_persist_segment_uuid = ANY({arr})",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(())
    }
}
