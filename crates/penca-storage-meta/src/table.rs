//! Table-metadata reads and writes against `__penca_system__.tables`,
//! plus the `penca_api::QueryManager::resolve_read_snapshot` dispatcher that
//! threads `(open_tx_uuid, as_of_micros, as_of_seq)` into the
//! [`ReadSnapshot`] every read path consumes — defaulting to the
//! per-branch seq frontier when none is set.

use penca_core::naming;
use penca_db::driver::{DbDriver, SqlValue};
use penca_db::resolve;
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::helpers::{parse_uuid, qi};
use crate::{LifecycleManager, Result};

impl LifecycleManager {
    /// Insert a table metadata entry into the upsert_log partition.
    ///
    /// Used by both create_table and update_table; the create-vs-update
    /// distinction is resolved at read time. Rows carry `tx_uuid` (no
    /// `commit_micros`); visibility resolves via a JOIN against
    /// `commit_tx_log_partition(catalog, branch)`.
    ///
    /// 1 SQL query.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_table_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        table_uuid: &str,
        schema_uuid: &str,
        branch_uuid: &str,
        tx_uuid: &str,
        table_name: &str,
        arrow_schema: &[u8],
        partition_keys: &[String],
        clustering_keys: &[String],
        primary_keys: &[String],
        description: &str,
        retention_duration_seconds: Option<i64>,
        snapshot_density_seconds: Option<i64>,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let sys_tables_table_uuid = naming::system_tables_table_uuid(&catalog);
        let table = naming::upsert_log_table(&sys_tables_table_uuid, &branch);

        // `schema_uuid` stays a distinct foreign key (the row's schema
        // parent), NOT the row's own identity — `table_uuid` is the PK.
        let row_uuid = naming::row_uuid_for_pk(
            &sys_tables_table_uuid,
            &[parse_uuid(table_uuid).to_string().as_str()],
        );
        let tx_uuid_parsed = parse_uuid(tx_uuid);
        let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);
        // partition_keys / clustering_keys / primary_keys are PG `text[]`
        // (arrow `list<utf8>` → `text[]` per `arrow_type_to_sql`). Bind
        // via `SqlValue::TextArray` so the SQL string is stable across
        // calls (plan-cache friendly) and user-supplied keys flow
        // through as parameters, never interpolated.
        let sql = format!(
            "INSERT INTO {table} \
             (version_uuid, row_uuid, tx_uuid, \
              table_uuid, table_name, schema_uuid, \
              arrow_schema, partition_keys, clustering_keys, primary_keys, \
              description, retention_duration_seconds, snapshot_density_seconds) \
             VALUES ($1, $2, $3, $4, $5, \
              $6, $7, $8::text[], $9::text[], $10::text[], \
              $11, $12, $13) \
             ON CONFLICT (version_uuid) DO UPDATE SET \
              table_name = EXCLUDED.table_name, \
              arrow_schema = EXCLUDED.arrow_schema, \
              partition_keys = EXCLUDED.partition_keys, \
              clustering_keys = EXCLUDED.clustering_keys, \
              primary_keys = EXCLUDED.primary_keys, \
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
                    SqlValue::Uuid(parse_uuid(table_uuid)),
                    SqlValue::Text(table_name.to_string()),
                    SqlValue::Uuid(parse_uuid(schema_uuid)),
                    SqlValue::Bytes(arrow_schema.to_vec()),
                    SqlValue::TextArray(partition_keys.to_vec()),
                    SqlValue::TextArray(clustering_keys.to_vec()),
                    SqlValue::TextArray(primary_keys.to_vec()),
                    SqlValue::Text(description.to_string()),
                    SqlValue::from_opt_i64(retention_duration_seconds),
                    SqlValue::from_opt_i64(snapshot_density_seconds),
                ],
            )
            .await?;
        Ok(())
    }

    /// Insert a tombstone into `__penca_system__.tables` delete log.
    ///
    /// Soft-delete only — the table's physical data tables stay
    /// addressable for in-flight reads (rollback recovers them) and
    /// are eventually swept by lifecycle once committed. No
    /// synchronous DROP. Mirrors how user data table deletes work.
    ///
    /// TODO(CHA-174): replace with `write_data` delete.
    pub async fn delete_table_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        tx_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let sys_tables_table_uuid = naming::system_tables_table_uuid(&catalog);
        let table = naming::delete_log_table(&sys_tables_table_uuid, &branch);

        // `table_uuid` is the widened delete-log PK column, populated to match
        // the upsert row.
        let row_uuid = naming::row_uuid_for_pk(
            &sys_tables_table_uuid,
            &[parse_uuid(table_uuid).to_string().as_str()],
        );
        let tx_uuid_parsed = parse_uuid(tx_uuid);
        let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);
        let sql = format!(
            "INSERT INTO {table} \
             (version_uuid, row_uuid, tx_uuid, table_uuid) \
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
                    SqlValue::Uuid(parse_uuid(table_uuid)),
                ],
            )
            .await?;
        Ok(())
    }

    /// Visibility-checked variant of [`Self::delete_table_metadata`].
    /// Inserts the tombstone iff the table is visible at
    /// `request_tx_uuid` (RYOW honoured for tables created in the
    /// caller's open tx). Returns `true` when a row was visible (and
    /// the tombstone insert ran or was a no-op due to ON CONFLICT),
    /// `false` when the table doesn't exist — callers map `false` to
    /// `NotFound` without an extra `get_table` round-trip.
    ///
    /// 1 SQL query.
    pub async fn delete_table_metadata_if_visible(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        tx_uuid: &str,
        request_tx_uuid: Option<&str>,
    ) -> Result<bool> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let sys_tables_table_uuid = naming::system_tables_table_uuid(&catalog);
        let upsert_part = naming::upsert_log_table(&sys_tables_table_uuid, &branch);
        let delete_part = naming::delete_log_table(&sys_tables_table_uuid, &branch);
        let commit_tx_log_part = naming::commit_tx_log_partition(&catalog, &branch);

        // `table_uuid` is the widened delete-log PK column, populated to match
        // the upsert row.
        let row_uuid = naming::row_uuid_for_pk(
            &sys_tables_table_uuid,
            &[parse_uuid(table_uuid).to_string().as_str()],
        );
        let tx_uuid_parsed = parse_uuid(tx_uuid);
        let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);

        // Resolve CTEs filtered to this row_uuid; the row_filter
        // becomes the `u.row_uuid = $1` clause inside
        // `upserts_ranked`. `latest_upserts` then yields zero or one
        // row, which gates the tombstone INSERT and is reported back.
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

        // Postgres executes data-modifying CTEs unconditionally
        // regardless of whether the outer query references them, so the
        // tombstone INSERT always runs even though the final SELECT only
        // reads `existing`. No RETURNING needed.
        let sql = format!(
            "WITH {cte_defs}, \
             existing AS (SELECT row_uuid FROM latest_upserts LIMIT 1), \
             inserted AS ( \
                 INSERT INTO {tomb} (version_uuid, row_uuid, tx_uuid, table_uuid) \
                 SELECT $2, $1, $3, $4 \
                 WHERE EXISTS (SELECT 1 FROM existing) \
                 ON CONFLICT (version_uuid) DO NOTHING \
             ) \
             SELECT EXISTS (SELECT 1 FROM existing) AS table_exists",
            tomb = qi(&delete_part),
        );

        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::Uuid(row_uuid),
                    SqlValue::Uuid(version_uuid),
                    SqlValue::Uuid(tx_uuid_parsed),
                    SqlValue::Uuid(parse_uuid(table_uuid)),
                ],
            )
            .await?;
        Ok(rows[0].get("table_exists"))
    }

    /// Materialize table metadata into a branch's upsert log with an
    /// explicit `tx_uuid`. Used by `CreateBranch` to copy parent-branch
    /// tables onto the new branch.
    ///
    /// TODO(CHA-174): replace with `write_data` against
    /// `__penca_system__.tables` carrying N changes.
    #[allow(clippy::too_many_arguments)]
    pub async fn materialize_table_metadata(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        schema_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        tx_uuid: &str,
        table_name: &str,
        arrow_schema: &[u8],
        partition_keys: &[String],
        clustering_keys: &[String],
        primary_keys: &[String],
        description: &str,
        retention_duration_seconds: Option<i64>,
        snapshot_density_seconds: Option<i64>,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let sys_tables_table_uuid = naming::system_tables_table_uuid(&catalog);
        let table = naming::upsert_log_table(&sys_tables_table_uuid, &branch);

        // Derive row_uuid canonically so CreateBranch materialize yields the
        // same row_uuid as the parent's `insert_table_metadata`.
        let row_uuid = naming::row_uuid_for_pk(
            &sys_tables_table_uuid,
            &[parse_uuid(table_uuid).to_string().as_str()],
        );
        let tx_uuid_parsed = parse_uuid(tx_uuid);
        let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);
        // Same plan-cache rationale as `insert_table_metadata`.
        let sql = format!(
            "INSERT INTO {table} \
             (version_uuid, row_uuid, tx_uuid, \
              table_uuid, table_name, schema_uuid, \
              arrow_schema, partition_keys, clustering_keys, primary_keys, \
              description, retention_duration_seconds, snapshot_density_seconds) \
             VALUES ($1, $2, $3, $4, $5, \
              $6, $7, $8::text[], $9::text[], $10::text[], \
              $11, $12, $13) \
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
                    SqlValue::Uuid(parse_uuid(table_uuid)),
                    SqlValue::Text(table_name.to_string()),
                    SqlValue::Uuid(parse_uuid(schema_uuid)),
                    SqlValue::Bytes(arrow_schema.to_vec()),
                    SqlValue::TextArray(partition_keys.to_vec()),
                    SqlValue::TextArray(clustering_keys.to_vec()),
                    SqlValue::TextArray(primary_keys.to_vec()),
                    SqlValue::Text(description.to_string()),
                    SqlValue::from_opt_i64(retention_duration_seconds),
                    SqlValue::from_opt_i64(snapshot_density_seconds),
                ],
            )
            .await?;
        Ok(())
    }
}
