//! Branch-merge SQL for compacting source-branch activity onto a target branch.

use arrow::datatypes::SchemaRef;
use penca_db::dialect::pg::PgDialect;
use penca_db::dialect::{
    Dialect, build_composite_merge_resolution, leading_comma_if_nonempty, qualify_user_cols,
};
use penca_db::driver::{DbDriver, SqlValue};
use sqlx::postgres::PgRow;

use crate::{HotStorageClient, HotStorageError};

impl HotStorageClient {
    /// Compact a single table's source-branch activity into one merge_tx
    /// on the target branch.
    ///
    /// Given a fast-forward merge (target untouched since source's fork),
    /// the surviving per-row state on source flows into target as:
    ///
    /// | source-side outcome | target log   |
    /// |---------------------|--------------|
    /// | `final_alive`       | upsert_log   |
    /// | `final_dead`        | delete_log   |
    ///
    /// All rows written carry `merge_tx_uuid`, so target's commit_tx_log gets
    /// only the merge_tx (created by the caller). Source tx identities
    /// are deliberately discarded — downstream PIT reads on target at
    /// `t < merge_tx.committed_at` see pre-merge state, `t ≥
    /// merge_tx.committed_at` see the compacted merged state.
    ///
    /// **Must be called within a transaction** so both INSERTs
    /// commit atomically.
    ///
    /// **Precondition:** caller has verified the merge is fast-forward
    /// (target has no commits past source's fork point). Non-FF conflict
    /// detection is TODO(CHA-5).
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            source_upsert_table = %source_upsert_table,
            source_delete_table = %source_delete_table,
            target_upsert_table = %target_upsert_table,
            target_delete_table = %target_delete_table,
            source_tx_table = %source_tx_table,
            merge_tx_uuid = %merge_tx_uuid,
        ),
    )]
    pub async fn merge_table_data(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        source_upsert_table: &str,
        source_delete_table: &str,
        target_upsert_table: &str,
        target_delete_table: &str,
        source_tx_table: &str,
        merge_tx_uuid: &str,
        user_schema: &SchemaRef,
    ) -> Result<(), HotStorageError> {
        let su = PgDialect::quote_identifier(source_upsert_table);
        let sd = PgDialect::quote_identifier(source_delete_table);
        let tu = PgDialect::quote_identifier(target_upsert_table);
        let td = PgDialect::quote_identifier(target_delete_table);
        let stx = PgDialect::quote_identifier(source_tx_table);

        let user_cols: Vec<&str> = user_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let user_cols_list = user_cols
            .iter()
            .map(|c| PgDialect::quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        let user_cols_u = qualify_user_cols::<PgDialect>("u", &user_cols);
        let user_cols_l = qualify_user_cols::<PgDialect>("l", &user_cols);
        let lead = leading_comma_if_nonempty(&user_cols);

        // Source upsert/delete logs JOIN to `source_committed_tx` for the
        // commit timestamp; `write_seq_num` is per-row.
        let upsert_source = format!(
            "(SELECT u.row_uuid{lead}{user_cols_u}, \
                    c.commit_micros, u.write_seq_num \
             FROM {su} u JOIN source_committed_tx c USING (tx_uuid)) _u"
        );
        let delete_source = format!(
            "(SELECT d.row_uuid, c.commit_micros, d.write_seq_num \
             FROM {sd} d JOIN source_committed_tx c USING (tx_uuid)) _d"
        );

        // Shared with the read path: one source of truth for the composite
        // tiebreaker semantic, keeping branch-merge in lockstep with
        // `penca_merge::sql::build_merge_resolved`.
        let composite = build_composite_merge_resolution::<PgDialect>(
            &upsert_source,
            &delete_source,
            &user_cols,
            // Branch-merge discards source tx identities (no per-tx
            // seq), so it orders latest-wins on committed_at, not commit_seq_num.
            "commit_micros",
        );

        // The shared CTE prefix is re-emitted into each INSERT because
        // Postgres `WITH` is statement-scoped. `source_committed_tx` needs no
        // "is-committed" sentinel — commit_tx_log is committed-only by
        // construction (aborts go to `abort_tx_log`), so the JOIN against
        // {stx} is the filter.
        let shared_ctes = format!(
            "WITH source_committed_tx AS (\
                 SELECT tx_uuid, commit_micros FROM {stx}\
             ), {latest}, {deletes}",
            latest = composite.latest_cte,
            deletes = composite.deletes_cte,
        );

        // 1. surviving alive upserts → target.upsert_log
        let upsert_sql = format!(
            "{shared_ctes} \
             INSERT INTO {tu} (version_uuid, row_uuid, tx_uuid{lead}{user_cols_list}) \
             SELECT gen_random_uuid(), l.row_uuid, $1{lead}{user_cols_l} \
             FROM latest l \
             LEFT JOIN deletes d ON l.row_uuid = d.row_uuid \
             WHERE {upsert_visible}",
            upsert_visible = composite.upsert_visible_predicate,
        );
        driver
            .execute_no_result_params(&upsert_sql, &[SqlValue::uuid_str(merge_tx_uuid)?])
            .await?;

        // 2. surviving tombstones → target.delete_log
        // (same `gen_random_uuid()` exception as the upsert side
        // above — bulk INSERT-FROM-SELECT during merge, not
        // caller-visible). Mirror predicate: delete wins ONLY on strict
        // greater so ties resolve to upsert-wins (uniform with the
        // upsert side above).
        let delete_sql = format!(
            "{shared_ctes} \
             INSERT INTO {td} (version_uuid, row_uuid, tx_uuid) \
             SELECT gen_random_uuid(), d.row_uuid, $1 \
             FROM deletes d \
             LEFT JOIN latest l ON d.row_uuid = l.row_uuid \
             WHERE {delete_visible}",
            delete_visible = composite.delete_visible_predicate,
        );
        driver
            .execute_no_result_params(&delete_sql, &[SqlValue::uuid_str(merge_tx_uuid)?])
            .await?;

        Ok(())
    }
}
