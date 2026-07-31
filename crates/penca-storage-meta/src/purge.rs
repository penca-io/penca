//! `table_purge_metadata` two-phase commit helpers, the purge /
//! purge-eligible watermark queries that feed
//! `lifecycle.rs::purge_locked`, and the ADR-0021 aborted-hot-row
//! cleanup helpers.

use penca_core::naming;
use penca_db::driver::{DbDriver, SqlType, SqlValue};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::helpers::{epoch, parse_uuid, qi};
use crate::{LifecycleManager, Result};

impl LifecycleManager {
    /// Insert a `table_purge_metadata` row with NULL `commit_micros`
    /// (phase 1 of two-phase commit).
    ///
    /// `table_purge_uuid` is derived deterministically from
    /// `(catalog, branch, table, purged_at)` (see
    /// [`naming::table_purge_uuid`]); phase-1 retries with
    /// identical inputs replay to the same PK and slot in via
    /// `DO UPDATE` (no-op write).
    ///
    /// 1 SQL query.
    /// A purge wave records the seq watermark(s) it
    /// advanced — `Pu` (`last_purged_commit_seq_num`, committed read fence) and/or
    /// `Pa` (`last_purged_aborted_seq_num`, abort cleanup frontier). NULL for
    /// an axis this wave did not advance.
    pub async fn insert_table_purge(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        table_purge_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        last_purged_commit_seq_num: Option<i64>,
        last_purged_aborted_seq_num: Option<i64>,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let table = naming::table_purge_metadata_partition(&catalog, &branch);
        let sql = format!(
            "INSERT INTO {table} \
             (table_purge_uuid, branch_uuid, table_uuid, \
              last_purged_commit_seq_num, last_purged_aborted_seq_num) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (branch_uuid, table_purge_uuid) DO UPDATE \
                SET last_purged_commit_seq_num = EXCLUDED.last_purged_commit_seq_num, \
                    last_purged_aborted_seq_num = EXCLUDED.last_purged_aborted_seq_num",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(table_purge_uuid)?,
                    SqlValue::Uuid(branch),
                    SqlValue::uuid_str(table_uuid)?,
                    last_purged_commit_seq_num
                        .map_or(SqlValue::Null(SqlType::Int64), SqlValue::Int64),
                    last_purged_aborted_seq_num
                        .map_or(SqlValue::Null(SqlType::Int64), SqlValue::Int64),
                ],
            )
            .await?;
        Ok(())
    }

    /// Mark a `table_purge_metadata` row committed (phase 2).
    ///
    /// 1 SQL query.
    pub async fn commit_table_purge(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_purge_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let table = naming::table_purge_metadata_partition(&catalog, &branch);
        let sql = format!(
            "UPDATE {table} SET commit_micros = {epoch} \
             WHERE branch_uuid = $1 AND table_purge_uuid = $2",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Uuid(branch),
                    SqlValue::uuid_str(table_purge_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Delete a `table_purge_metadata` row only if uncommitted (crash cleanup).
    ///
    /// 1 SQL query.
    pub async fn delete_uncommitted_table_purge(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_purge_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let table = naming::table_purge_metadata_partition(&catalog, &branch);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 AND table_purge_uuid = $2 \
               AND commit_micros IS NULL",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Uuid(branch),
                    SqlValue::uuid_str(table_purge_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// `Pu` — the committed hot↔cold read fence (ADR 0027):
    /// `MAX(last_purged_commit_seq_num)` over committed `table_purge_metadata`
    /// rows for `(branch, table)`. `plan()`'s fence reads this; `purge_locked`
    /// reads it for the strict-advance no-op check. `Ok(None)` when no
    /// committed purge has advanced `Pu` yet.
    ///
    /// 1 SQL query (partition-pruned; served by the partial `(table_uuid,
    /// last_purged_commit_seq_num DESC) WHERE commit_micros IS NOT NULL`
    /// index in `pg.rs`).
    pub async fn latest_committed_table_purge_seq_watermark(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
    ) -> Result<Option<i64>> {
        Self::max_committed_purge_column(
            driver,
            catalog_uuid,
            branch_uuid,
            table_uuid,
            "last_purged_commit_seq_num",
        )
        .await
    }

    /// `Pa` — the abort cleanup frontier (ADR 0027):
    /// `MAX(last_purged_aborted_seq_num)` over committed `table_purge_metadata`
    /// rows for `(branch, table)`. Feeds `purge_locked`'s strict-advance check
    /// and commit_tx_log GC's abort branch-min. `Ok(None)` when no committed purge
    /// has advanced `Pa` yet.
    pub async fn latest_committed_table_purge_aborted_seq_watermark(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
    ) -> Result<Option<i64>> {
        Self::max_committed_purge_column(
            driver,
            catalog_uuid,
            branch_uuid,
            table_uuid,
            "last_purged_aborted_seq_num",
        )
        .await
    }

    /// Shared `MAX(<col>)` over committed `table_purge_metadata` rows for
    /// `(branch, table)`. `col` is a fixed internal identifier (never user
    /// input).
    async fn max_committed_purge_column(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        col: &str,
    ) -> Result<Option<i64>> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let table = naming::table_purge_metadata_partition(&catalog, &branch);
        let sql = format!(
            "SELECT MAX({col}) AS watermark FROM {table} \
             WHERE branch_uuid = $1 \
               AND table_uuid = $2 \
               AND commit_micros IS NOT NULL",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(
                &sql,
                &[SqlValue::Uuid(branch), SqlValue::uuid_str(table_uuid)?],
            )
            .await?;
        Ok(rows
            .first()
            .and_then(|r| r.try_get::<Option<i64>, _>("watermark").ok().flatten()))
    }

    /// Read the branch's abort-order counter frontier — the next
    /// `aborted_at_seq_num` to allocate. Purge samples
    /// this at the start of a pass as the abort cleanup bound `F`: all aborts
    /// with `aborted_at_seq_num < F` are already allocated/visible, so cleaning
    /// `< F` and stamping `Pa = F` is exact. `Ok(None)` only if the counter
    /// row is missing (branch not initialized).
    pub async fn read_abort_seq_frontier(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
    ) -> Result<Option<i64>> {
        let part = naming::abort_seq_num_partition(catalog_uuid, branch_uuid);
        let sql = format!("SELECT seq_num FROM {part}", part = qi(&part));
        let rows = driver.execute_params(&sql, &[]).await?;
        Ok(rows.first().map(|r| r.get::<i64, _>("seq_num")))
    }

    /// `W_snap` — the snapshot seq watermark for `(branch, table)`:
    /// `MAX(commit_seq_num)` over committed `table_snapshot_metadata` rows. The
    /// happy-path purge target `Pu = W_snap` (ADR 0027): those rows
    /// are already in the durable, read-served snapshot baseline, so dropping
    /// them from hot is free. `Ok(None)` when no snapshot has committed yet.
    pub async fn latest_committed_table_snapshot_seq_watermark(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
    ) -> Result<Option<i64>> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let table = naming::table_snapshot_metadata_partition(&catalog, &branch);
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
                &[SqlValue::Uuid(branch), SqlValue::uuid_str(table_uuid)?],
            )
            .await?;
        Ok(rows
            .first()
            .and_then(|r| r.try_get::<Option<i64>, _>("watermark").ok().flatten()))
    }
}
