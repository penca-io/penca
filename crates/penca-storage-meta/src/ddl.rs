//! DDL-delegation surface — pass-throughs to `PgDialect` for catalog /
//! branch / data-table create / drop / evolve plus `bootstrap_system_rows`
//! (the catalog-bootstrap seed that populates `__penca_system__.{schemas,tables}`
//! and the genesis `tx_table_log` row).

use arrow::datatypes::Schema as ArrowSchema;
use penca_core::naming;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::DbDriver;
use sqlx::postgres::PgRow;

use crate::helpers::parse_uuid;
use crate::{LifecycleManager, MetadataError, Result};

impl LifecycleManager {
    /// Create global resource tables (catalog_store, schema_store,
    /// branch/table persist metadata, snapshot tables).
    pub async fn bootstrap(driver: &impl DbDriver<Row = PgRow>) -> Result<()> {
        PgDialect::bootstrap(driver).await?;
        Ok(())
    }

    /// Create per-catalog tables (branch store, tx logs, metadata logs)
    /// and bootstrap the main branch.
    ///
    /// `main_branch_uuid` and `public_schema_uuid` are random-minted by
    /// the caller; the system schema + its two tables remain
    /// deterministic per-catalog anchors. Inserts the four system rows
    /// (`public` + `__penca_system__` schemas, `__penca_system__.schemas`
    /// + `__penca_system__.tables` tables) so the catalog is discoverable
    ///   via `list_schemas` / `list_tables` and persist against system
    ///   tables can find their arrow schemas.
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %catalog_uuid,
            main_branch = %main_branch_uuid,
            public_schema = %public_schema_uuid,
        ),
    )]
    pub async fn create_catalog_tables(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        main_branch_uuid: &str,
        public_schema_uuid: &str,
    ) -> Result<()> {
        let uuid = parse_uuid(catalog_uuid);
        let main_branch = parse_uuid(main_branch_uuid);
        PgDialect::create_catalog_tables(driver, &uuid, &main_branch).await?;
        Self::bootstrap_system_rows(driver, catalog_uuid, main_branch_uuid, public_schema_uuid)
            .await?;
        Ok(())
    }

    /// Seed the four bootstrap rows under `__penca_system__.{schemas,tables}`
    /// on main, tagged with the catalog's genesis tx. Idempotent via the
    /// auditable-store `ON CONFLICT (version_uuid)` upsert.
    async fn bootstrap_system_rows(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        main_branch_uuid: &str,
        public_schema_uuid: &str,
    ) -> Result<()> {
        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

        let catalog = parse_uuid(catalog_uuid);
        let genesis_tx = naming::genesis_tx_uuid(&catalog).to_string();
        let system_schema_uuid = naming::system_schema_uuid(&catalog).to_string();

        // `__penca_system__` is a deterministic anchor; `public` is
        // random-minted by the caller.
        for (schema_uuid, schema_name, description) in [
            (
                system_schema_uuid.clone(),
                naming::SYSTEM_SCHEMA_NAME,
                "auto-created system schema (CHA-164)",
            ),
            (
                public_schema_uuid.to_string(),
                naming::PUBLIC_SCHEMA_NAME,
                "auto-created default schema (CHA-163)",
            ),
        ] {
            Self::insert_schema_row(
                driver,
                catalog_uuid,
                main_branch_uuid,
                &schema_uuid,
                &genesis_tx,
                schema_name,
                description,
                None,
                None,
            )
            .await?;
        }

        // Table rows: __penca_system__.{schemas,tables}. Each row's
        // `arrow_schema` field describes the table the row is *about*.
        let serialize = |schema: ArrowSchema| -> Result<Vec<u8>> {
            let mut writer =
                StreamWriter::try_new_with_options(Vec::new(), &schema, IpcWriteOptions::default())
                    .map_err(|e| MetadataError::Db(sqlx::Error::Protocol(e.to_string())))?;
            writer
                .finish()
                .map_err(|e| MetadataError::Db(sqlx::Error::Protocol(e.to_string())))?;
            writer
                .into_inner()
                .map_err(|e| MetadataError::Db(sqlx::Error::Protocol(e.to_string())))
        };

        let sys_schemas_arrow = serialize(PgDialect::system_schemas_arrow_schema())?;
        let sys_tables_arrow = serialize(PgDialect::system_tables_arrow_schema())?;
        let sys_indexes_arrow = serialize(PgDialect::system_indexes_arrow_schema())?;
        // `seed_membership` co-locates intent per table: the genesis tx
        // wrote rows to `schemas` (the schema rows) and `tables` (these
        // three self-describing rows), so those go into the tx_table_log
        // membership. It wrote NO rows into `__penca_system__.indexes`
        // itself (no indexes exist at genesis), so that table is seeded
        // (its self-describing row + data tables) but excluded from
        // membership — an empty table with a tx_table_log entry but no
        // persist watermark would pin PurgeTxLog's `min(purged_at)` at 0
        // forever. CreateIndex adds the membership on a real write.
        let mut system_table_uuids: Vec<String> = Vec::with_capacity(2);
        // Seed each self-describing row with its declared PK
        // (`schema_uuid` / `table_uuid` / `index_uuid`) so the `primary_keys`
        // column accurately describes the table — persist of
        // `__penca_system__.indexes` reads its PK off this row.
        for (table_uuid, table_name, arrow_bytes, primary_keys, seed_membership) in [
            (
                naming::system_schemas_table_uuid(&catalog).to_string(),
                naming::SYSTEM_SCHEMAS_TABLE_NAME,
                &sys_schemas_arrow,
                PgDialect::system_schemas_primary_keys(),
                true,
            ),
            (
                naming::system_tables_table_uuid(&catalog).to_string(),
                naming::SYSTEM_TABLES_TABLE_NAME,
                &sys_tables_arrow,
                PgDialect::system_tables_primary_keys(),
                true,
            ),
            (
                naming::system_indexes_table_uuid(&catalog).to_string(),
                naming::SYSTEM_INDEXES_TABLE_NAME,
                &sys_indexes_arrow,
                PgDialect::system_indexes_primary_keys(),
                false,
            ),
        ] {
            let description =
                format!("auto-created system table (CHA-177): __penca_system__.{table_name}");
            Self::insert_table_metadata(
                driver,
                catalog_uuid,
                &table_uuid,
                &system_schema_uuid,
                main_branch_uuid,
                &genesis_tx,
                table_name,
                arrow_bytes,
                &[],
                &[],
                &primary_keys,
                &description,
                None,
                None,
            )
            .await?;
            if seed_membership {
                system_table_uuids.push(table_uuid);
            }
        }

        // Seed tx_table_log with genesis-tx → system-table
        // membership so consumers can resolve the system tables via the
        // (tx_uuid, table_uuid) index just like user tables.
        let tx_table_part = naming::tx_table_log_partition(&catalog, &parse_uuid(main_branch_uuid));
        penca_storage_hot::HotStorageClient
            .insert_tx_table_log(
                driver,
                &tx_table_part,
                main_branch_uuid,
                &genesis_tx,
                &system_table_uuids,
            )
            .await
            .map_err(|e| match e {
                penca_storage_hot::HotStorageError::Sqlx(s) => MetadataError::Db(s),
                other => MetadataError::Db(sqlx::Error::Protocol(other.to_string())),
            })?;

        Ok(())
    }

    /// Drop all per-catalog tables (CASCADE drops sub-partitions).
    ///
    /// `main_branch_uuid` is required so main's system-table physicals
    /// can be located (it is random-minted and threaded by
    /// the caller).
    pub async fn drop_catalog_tables(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        main_branch_uuid: &str,
    ) -> Result<()> {
        let uuid = parse_uuid(catalog_uuid);
        let main_branch = parse_uuid(main_branch_uuid);
        PgDialect::drop_catalog_tables(driver, &uuid, &main_branch).await?;
        Ok(())
    }

    /// Create branch partitions for all per-catalog log tables.
    pub async fn ensure_branch_partitions(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        PgDialect::ensure_branch_partitions(driver, &catalog, &branch).await?;
        Ok(())
    }

    /// Seed a forked child's `commit_seq_num` counter from the fork
    /// commit's seq. See [`PgDialect::seed_commit_seq_num_from_fork`]. Run after
    /// [`Self::ensure_branch_partitions`] and before the child's first commit
    /// (the fork/materialization tx). `fork_seq` = `commit_seq_num(T)`, resolved
    /// once under PersistBranch.
    pub async fn seed_commit_seq_num_from_fork(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        child_branch_uuid: &str,
        fork_seq: i64,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let child = parse_uuid(child_branch_uuid);
        PgDialect::seed_commit_seq_num_from_fork(driver, &catalog, &child, fork_seq).await?;
        Ok(())
    }

    /// Drop branch partitions for all per-catalog log tables.
    pub async fn drop_branch_partitions(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        PgDialect::drop_branch_partitions(driver, &catalog, &branch).await?;
        Ok(())
    }

    /// Create per-table upsert and delete log tables from an Arrow schema.
    ///
    /// `primary_keys` widens the delete_log schema to carry the
    /// PK columns alongside `row_uuid` / `tx_uuid`.
    pub async fn create_data_tables(
        driver: &impl DbDriver<Row = PgRow>,
        table_uuid: &str,
        branch_uuid: &str,
        arrow_schema: &ArrowSchema,
        primary_keys: &[String],
    ) -> Result<()> {
        let table = parse_uuid(table_uuid);
        let branch = parse_uuid(branch_uuid);
        PgDialect::create_data_tables(driver, &table, &branch, arrow_schema, primary_keys).await?;
        Ok(())
    }

    /// Drop per-table data tables (insert + update + delete logs).
    pub async fn drop_data_tables(
        driver: &impl DbDriver<Row = PgRow>,
        table_uuid: &str,
        branch_uuid: &str,
    ) -> Result<()> {
        let table = parse_uuid(table_uuid);
        let branch = parse_uuid(branch_uuid);
        PgDialect::drop_data_tables(driver, &table, &branch).await?;
        Ok(())
    }

    /// Add new columns to a data log table for additive schema evolution.
    pub async fn evolve_data_log_schema(
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        old_schema: &ArrowSchema,
        new_schema: &ArrowSchema,
    ) -> Result<()> {
        PgDialect::evolve_data_log_schema(driver, table_name, old_schema, new_schema).await?;
        Ok(())
    }
}
