//! Schema CRUD on `__penca_system__.schemas` per-branch, plus the
//! cold-tolerant `resolve_schema_metadata` reader that routes through
//! `stream_merged`.

use penca_core::naming;
use penca_db::driver::{DbDriver, SqlValue};
use sqlx::postgres::PgRow;

use crate::helpers::{parse_uuid, qi};
use crate::{LifecycleManager, Result};

impl LifecycleManager {
    /// Insert a new schema into the catalog's schema_store.
    ///
    /// 1 SQL query.
    /// Insert a schema row into `__penca_system__.schemas` on a branch.
    ///
    /// Writes through the standard data-table upsert log
    /// `{prefix}_data_upsert_log` where
    /// `prefix = data_log_prefix(sys_schemas_table_uuid, branch_uuid)`.
    /// Same SQL pattern as user data writes — `ON CONFLICT
    /// (version_uuid) DO UPDATE` makes within-tx UPSERT explicit.
    ///
    /// TODO(CHA-174): replace with a `write_data` call against
    /// `__penca_system__.schemas` once admin folds into Write.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_schema_row(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        schema_uuid: &str,
        tx_uuid: &str,
        schema_name: &str,
        description: &str,
        retention_duration_seconds: Option<i64>,
        snapshot_density_seconds: Option<i64>,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let sys_schemas_table_uuid = naming::system_schemas_table_uuid(&catalog);
        let table = naming::upsert_log_table(&sys_schemas_table_uuid, &branch);

        let row_uuid = naming::row_uuid_for_pk(
            &sys_schemas_table_uuid,
            &[parse_uuid(schema_uuid).to_string().as_str()],
        );
        let tx_uuid_parsed = parse_uuid(tx_uuid);
        let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);
        let sql = format!(
            "INSERT INTO {table} \
             (version_uuid, row_uuid, tx_uuid, \
              schema_uuid, schema_name, description, \
              retention_duration_seconds, snapshot_density_seconds) \
             VALUES ($1, $2, $3, $4, $5, \
              $6, $7, $8) \
             ON CONFLICT (version_uuid) DO UPDATE SET \
              schema_name = EXCLUDED.schema_name, \
              description = EXCLUDED.description, \
              retention_duration_seconds = EXCLUDED.retention_duration_seconds, \
              snapshot_density_seconds = EXCLUDED.snapshot_density_seconds",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Uuid(version_uuid),
                    SqlValue::Uuid(row_uuid),
                    SqlValue::Uuid(tx_uuid_parsed),
                    SqlValue::Uuid(parse_uuid(schema_uuid)),
                    SqlValue::Text(schema_name.to_string()),
                    SqlValue::Text(description.to_string()),
                    SqlValue::from_opt_i64(retention_duration_seconds),
                    SqlValue::from_opt_i64(snapshot_density_seconds),
                ],
            )
            .await?;
        Ok(())
    }

    /// Insert a schema tombstone into `__penca_system__.schemas` delete log.
    ///
    /// TODO(CHA-174): replace with `write_data` delete.
    pub async fn insert_schema_delete_row(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        schema_uuid: &str,
        tx_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let sys_schemas_table_uuid = naming::system_schemas_table_uuid(&catalog);
        let table = naming::delete_log_table(&sys_schemas_table_uuid, &branch);

        // `schema_uuid` is the widened delete-log PK column, populated to
        // match the upsert row.
        let row_uuid = naming::row_uuid_for_pk(
            &sys_schemas_table_uuid,
            &[parse_uuid(schema_uuid).to_string().as_str()],
        );
        let tx_uuid_parsed = parse_uuid(tx_uuid);
        let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);
        let sql = format!(
            "INSERT INTO {table} \
             (version_uuid, row_uuid, tx_uuid, schema_uuid) \
             VALUES ($1, $2, $3, $4) \
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
                    SqlValue::Uuid(parse_uuid(schema_uuid)),
                ],
            )
            .await?;
        Ok(())
    }
}
