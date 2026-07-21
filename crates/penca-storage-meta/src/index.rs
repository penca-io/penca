//! Index-metadata reads and writes against `__penca_system__.indexes`
//! (CHA-455) — the user-facing auditable *definition* store for cold
//! indexes, the third dogfooded system Penca Table alongside
//! `__penca_system__.{schemas,tables}`. Query planning never reads this;
//! it reads the per-segment `segment_index_metadata` materialization
//! (ADR 0026 §5).
//!
//! Mirrors `table.rs`: `row_uuid` IS `index_uuid`, the branch is implicit
//! in the per-branch upsert/delete-log partition, each write carries
//! `tx_uuid` (no `commit_micros`) and visibility resolves via the
//! `commit_tx_log_partition(catalog, branch)` JOIN inside `stream_merged`.

use penca_core::naming;
use penca_db::driver::{DbDriver, SqlValue};
use penca_db::resolve;
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::helpers::{parse_uuid, qi};
use crate::{LifecycleManager, Result};

impl LifecycleManager {
    /// Insert an index-metadata row into the `__penca_system__.indexes`
    /// upsert log (mirror of [`Self::insert_table_metadata`]). Used by
    /// `CreateIndex`, inline `CreateTable.indexes`, and the rename path of
    /// `UpdateIndex` (a new auditable version). `columns` is bound as a
    /// `text[]` so the SQL string stays plan-cache-stable.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_index_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        index_uuid: &str,
        table_uuid: &str,
        branch_uuid: &str,
        tx_uuid: &str,
        index_name: &str,
        columns: &[String],
        index_type: i32,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let sys_indexes_table_uuid = naming::system_indexes_table_uuid(&catalog);
        let table = naming::upsert_log_table(&sys_indexes_table_uuid, &branch);

        // CHA-380: index_uuid is the row's own PK column; derive row_uuid
        // canonically (`row_uuid_for_pk`). table_uuid stays a distinct foreign
        // key (the owning table), not the row's own identity.
        let row_uuid = naming::row_uuid_for_pk(
            &sys_indexes_table_uuid,
            &[parse_uuid(index_uuid).to_string().as_str()],
        );
        let tx_uuid_parsed = parse_uuid(tx_uuid);
        let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);
        let sql = format!(
            "INSERT INTO {table} \
             (version_uuid, row_uuid, tx_uuid, \
              index_uuid, table_uuid, index_name, columns, index_type) \
             VALUES ($1, $2, $3, $4, $5, $6, $7::text[], $8) \
             ON CONFLICT (version_uuid) DO UPDATE SET \
              table_uuid = EXCLUDED.table_uuid, \
              index_name = EXCLUDED.index_name, \
              columns = EXCLUDED.columns, \
              index_type = EXCLUDED.index_type",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Uuid(version_uuid),
                    SqlValue::Uuid(row_uuid),
                    SqlValue::Uuid(tx_uuid_parsed),
                    SqlValue::Uuid(parse_uuid(index_uuid)),
                    SqlValue::Uuid(parse_uuid(table_uuid)),
                    SqlValue::Text(index_name.to_string()),
                    SqlValue::TextArray(columns.to_vec()),
                    SqlValue::Int32(index_type),
                ],
            )
            .await?;
        Ok(())
    }

    /// Insert a tombstone into the `__penca_system__.indexes` delete log
    /// iff the index is visible at `request_tx_uuid` (mirror of
    /// [`Self::delete_table_metadata_if_visible`]). Returns `true` when a
    /// row was visible (the tombstone insert ran or no-op'd), `false` when
    /// the index doesn't exist — callers map `false` to `NotFound`.
    pub async fn delete_index_metadata_if_visible(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        index_uuid: &str,
        tx_uuid: &str,
        request_tx_uuid: Option<&str>,
    ) -> Result<bool> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let sys_indexes_table_uuid = naming::system_indexes_table_uuid(&catalog);
        let upsert_part = naming::upsert_log_table(&sys_indexes_table_uuid, &branch);
        let delete_part = naming::delete_log_table(&sys_indexes_table_uuid, &branch);
        let commit_tx_log_part = naming::commit_tx_log_partition(&catalog, &branch);

        // CHA-380: derive row_uuid canonically; the resolve filter
        // (`u.row_uuid = $1`) matches the derived row_uuid now stored, and
        // index_uuid is the widened delete-log PK column (CHA-185).
        let row_uuid = naming::row_uuid_for_pk(
            &sys_indexes_table_uuid,
            &[parse_uuid(index_uuid).to_string().as_str()],
        );
        let tx_uuid_parsed = parse_uuid(tx_uuid);
        let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);

        let cte_defs = resolve::resolve_cte_sql(&resolve::ResolveSpec {
            upsert_table_name: &upsert_part,
            delete_table_name: &delete_part,
            commit_tx_log_table_name: &commit_tx_log_part,
            user_columns: &[],
            entity_column: "row_uuid",
            row_filter: Some("u.row_uuid = $1"),
            since_micros: None,
            until_micros: None,
            branch_uuid: Some(branch_uuid),
            open_tx_uuid: request_tx_uuid,
        });

        let sql = format!(
            "WITH {cte_defs}, \
             existing AS (SELECT row_uuid FROM latest_upserts LIMIT 1), \
             inserted AS ( \
                 INSERT INTO {tomb} (version_uuid, row_uuid, tx_uuid, index_uuid) \
                 SELECT $2, $1, $3, $4 \
                 WHERE EXISTS (SELECT 1 FROM existing) \
                 ON CONFLICT (version_uuid) DO NOTHING \
             ) \
             SELECT EXISTS (SELECT 1 FROM existing) AS index_exists",
            tomb = qi(&delete_part),
        );

        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::Uuid(row_uuid),
                    SqlValue::Uuid(version_uuid),
                    SqlValue::Uuid(tx_uuid_parsed),
                    SqlValue::Uuid(parse_uuid(index_uuid)),
                ],
            )
            .await?;
        Ok(rows[0].get("index_exists"))
    }

    /// Materialize index-metadata rows onto a branch's upsert log with an
    /// explicit `tx_uuid` (CreateBranch fork copy; mirror of
    /// [`Self::materialize_table_metadata`]). `ON CONFLICT DO NOTHING`.
    #[allow(clippy::too_many_arguments)]
    pub async fn materialize_index_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        index_uuid: &str,
        table_uuid: &str,
        tx_uuid: &str,
        index_name: &str,
        columns: &[String],
        index_type: i32,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let sys_indexes_table_uuid = naming::system_indexes_table_uuid(&catalog);
        let table = naming::upsert_log_table(&sys_indexes_table_uuid, &branch);

        // CHA-380: derive row_uuid canonically so CreateBranch materialize
        // yields the same row_uuid as the parent's `insert_index_metadata`.
        let row_uuid = naming::row_uuid_for_pk(
            &sys_indexes_table_uuid,
            &[parse_uuid(index_uuid).to_string().as_str()],
        );
        let tx_uuid_parsed = parse_uuid(tx_uuid);
        let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);
        let sql = format!(
            "INSERT INTO {table} \
             (version_uuid, row_uuid, tx_uuid, \
              index_uuid, table_uuid, index_name, columns, index_type) \
             VALUES ($1, $2, $3, $4, $5, $6, $7::text[], $8) \
             ON CONFLICT (version_uuid) DO NOTHING",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Uuid(version_uuid),
                    SqlValue::Uuid(row_uuid),
                    SqlValue::Uuid(tx_uuid_parsed),
                    SqlValue::Uuid(parse_uuid(index_uuid)),
                    SqlValue::Uuid(parse_uuid(table_uuid)),
                    SqlValue::Text(index_name.to_string()),
                    SqlValue::TextArray(columns.to_vec()),
                    SqlValue::Int32(index_type),
                ],
            )
            .await?;
        Ok(())
    }
}
