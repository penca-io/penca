//! Retention coalescing helpers shared by [`crate::QueryManager`] and
//! [`crate::WriteManager`].
//!
//! Effective retention follows ADR 0011 §4: each field on a `Table` is
//! resolved as `table → schema → catalog`, with the first set value
//! winning. The policy is stamped onto `Table.retention_config` for
//! every read response (`GetTableResponse`, `ListTablesResponse`) and
//! for `UpdateTableResponse` so the same proto type carries the same
//! semantics regardless of which RPC produced it.

use penca_db::driver::DbDriver;
use penca_dl::driver::DlDriver;
use penca_proto::external::v1::{RetentionConfig, Schema, Table};
use penca_storage_meta::LifecycleManager;

use crate::query::QueryManager;
use sqlx::postgres::PgRow;

use crate::error::ApiError;

/// Coalesce retention `table → schema`, per field. Pure; no I/O. Use this when
/// the schema parent is already in hand (e.g. inside a `list_tables` loop after
/// [`fetch_parent_retention`]).
///
/// CHA-433: schema is the broadest retention scope — the catalog no longer
/// carries a policy, so there is no catalog arm.
pub(crate) fn coalesce_retention(
    table_rc: &Option<RetentionConfig>,
    schema_rc: &Option<RetentionConfig>,
) -> RetentionConfig {
    let duration = table_rc
        .as_ref()
        .and_then(|r| r.retention_duration_seconds)
        .or_else(|| {
            schema_rc
                .as_ref()
                .and_then(|r| r.retention_duration_seconds)
        });

    let density = table_rc
        .as_ref()
        .and_then(|r| r.snapshot_density_seconds)
        .or_else(|| schema_rc.as_ref().and_then(|r| r.snapshot_density_seconds));

    RetentionConfig {
        retention_duration_seconds: duration,
        snapshot_density_seconds: density,
    }
}

/// Fetch the schema parent's retention config for a table. One metadata read.
/// Hoist this above any loop that coalesces N tables sharing the same schema —
/// the parent is invariant within a `(catalog, schema)` pair.
///
/// CHA-433: schema is the broadest retention scope, so this is a single schema
/// read (the catalog no longer carries a policy).
pub(crate) async fn fetch_parent_retention<L>(
    query_manager: &QueryManager,
    driver: &impl DbDriver<Row = PgRow>,
    dl_driver: &L,
    catalog_uuid: &str,
    schema_uuid: &str,
) -> Result<Option<RetentionConfig>, ApiError>
where
    L: DlDriver + ?Sized,
{
    // Schema retention coalesces from main per ADR 0011 §4 — pass no
    // branch / open_tx so the lookup hits the canonical row regardless
    // of the request's branch context. CHA-86: pin to pg_now rather than
    // an unbounded read.
    let snapshot = LifecycleManager::now_snapshot(driver).await?;
    let schema = query_manager
        .meta_get_schema(
            driver,
            dl_driver,
            catalog_uuid,
            Some(schema_uuid),
            None,
            None,
            &snapshot,
        )
        .await?;
    Ok(schema.as_ref().and_then(|s| s.default_retention_config))
}

/// Coalesce a single Table's retention with its parents and stamp the
/// effective policy onto the table. For multi-table responses
/// ([`crate::QueryManager::list_tables`]) prefer
/// [`fetch_parent_retention`] + [`coalesce_retention`] so the parent
/// fetch fans out once instead of N times.
pub(crate) async fn apply_effective_retention<L>(
    query_manager: &QueryManager,
    driver: &impl DbDriver<Row = PgRow>,
    dl_driver: &L,
    catalog_uuid: &str,
    schema_uuid: &str,
    table: &mut Table,
) -> Result<(), ApiError>
where
    L: DlDriver + ?Sized,
{
    let schema_rc =
        fetch_parent_retention(query_manager, driver, dl_driver, catalog_uuid, schema_uuid).await?;
    let effective = coalesce_retention(&table.retention_config, &schema_rc);
    table.retention_config = Some(effective);
    Ok(())
}

/// CHA-433: the effective retention duration for a read/audit plan's floor
/// (`None` = retention disabled). Uses the in-scope schema when present (a
/// by-name resolve — the SQL server's hot path — so zero extra roundtrips),
/// else reads the schema's retention (a by-uuid resolve, off the hot path).
/// Coalesces `table -> schema`.
pub(crate) async fn effective_retention_duration<L>(
    query_manager: &QueryManager,
    driver: &impl DbDriver<Row = PgRow>,
    dl_driver: &L,
    catalog_uuid: &str,
    schema_uuid: &str,
    schema_row: Option<&Schema>,
    table_retention: &Option<RetentionConfig>,
) -> Result<Option<i64>, ApiError>
where
    L: DlDriver + ?Sized,
{
    // The table pins a duration → the coalesce result is fully determined by
    // it; skip the schema read entirely (avoids a metadata roundtrip on the
    // by-uuid path when the table already resolves a duration).
    if let Some(table_duration) = table_retention
        .as_ref()
        .and_then(|r| r.retention_duration_seconds)
    {
        return Ok(Some(table_duration));
    }
    let schema_retention = match schema_row {
        Some(schema) => schema.default_retention_config,
        None => {
            fetch_parent_retention(query_manager, driver, dl_driver, catalog_uuid, schema_uuid)
                .await?
        }
    };
    Ok(coalesce_retention(table_retention, &schema_retention).retention_duration_seconds)
}
