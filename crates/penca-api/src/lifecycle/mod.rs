//! Storage lifecycle operations: persist, purge, purge_tx_log,
//! compact, sweep, snapshot.
//!
//! [`LifecycleManager`] orchestrates hot→cold movement and cold-tier
//! reorganization. Methods accept and return proto messages directly.
//! All cold IO (reads, writes, deletes) flows through
//! [`ColdStorageClient`](penca_storage_cold::ColdStorageClient) so
//! the hot+cold boundary stays in one place.
//!
//! Each op lives in its own submodule and re-opens `impl LifecycleManager`;
//! this module holds only the struct, the shared
//! [`resolve_catalog_branch_and_table`](LifecycleManager::resolve_catalog_branch_and_table)
//! helper, and the wiring.

mod batch_util;
mod branch_op;
mod chunker;
mod compact;
mod compact_op;
mod compact_plan;
mod durable_writer;
mod list_tables;
mod packer;
mod persist;
mod persist_tx_log;
mod purge;
mod purge_tx_log;
mod retire;
mod snapshot_op;
mod sweep;

use penca_core::Format;
use penca_db::driver::DbDriver;
use penca_dl::driver::DlDriver;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::error::ApiError;
use crate::resolve::{parse_resolved_uuid, resolve_branch, resolve_catalog};

/// Penca storage lifecycle manager (persist, compact, snapshot).
///
/// Stateless service-level config. Storage clients and drivers are passed
/// per-call so the same manager can orchestrate against any driver/pool.
#[derive(Clone)]
pub struct LifecycleManager {
    pub base_uri: String,
    pub storage_format: Format,
    pub max_segment_bytes: i64,
    /// Max in-flight segment reads during the all-cold merge's snapshot phase.
    /// Memory-safety knob — each read materializes a whole segment.
    pub segment_read_concurrency: usize,
    /// Universal grace window applied to destructive lifecycle steps
    /// (Purge of hot rows, compaction GC), in micros. Must equal the
    /// query service's cap — see ADR 0019. Stored in micros so SQL
    /// math against `commit_micros` stays in the same unit.
    pub query_timeout_micros: i64,
    /// Hot-purge grace window (ADR 0027), in micros. The expired-begin ledger
    /// GC drops a timed-out tx's `begin_tx_log` / `tx_table_log` only once it
    /// is older than `max(purge_sweep_interval_micros, this)` — floored at one
    /// Purge sweep so Purge has already cleared the tx's (invisible) hot rows.
    pub hot_purge_grace_micros: i64,
    /// Purge sweep cadence, in micros (the scheduler's tick interval). Floors
    /// the expired-begin ledger-GC grace (above). `0` when the scheduler is
    /// disabled — then the hot-grace window alone bounds the grace.
    pub purge_sweep_interval_micros: i64,
    /// Handle for the metadata reads lifecycle needs (segment readers, schema
    /// reads, table resolvers — ADR 0028). MUST be built with caches disabled:
    /// lifecycle never serves cached reads.
    pub query_manager: crate::query::QueryManager,
}

impl LifecycleManager {
    /// Resolve the three-tuple a per-table lifecycle op operates on,
    /// reading metadata under `snapshot`.
    ///
    /// The by-UUID path resolves **catalog-wide** (no schema scoping), so an
    /// `__penca_system__` table addressed by uuid under a convenient schema
    /// still resolves. The by-name path requires the schema parent.
    ///
    /// Callers MUST pass `AsOfMicros(step1_now)`, the timestamp taken once at
    /// the top of the op before any lock or metadata read, and thread it
    /// through every subsequent read: an unbounded read here sees the live
    /// view, which can advance past `step1_now` if a concurrent DDL (rename,
    /// `DeleteTable`) commits mid-op.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn resolve_catalog_branch_and_table<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl_driver: &L,
        catalog_uuid: Option<&str>,
        catalog_name: Option<&str>,
        schema_uuid: Option<&str>,
        schema_name: Option<&str>,
        branch_uuid: Option<&str>,
        branch_name: Option<&str>,
        table_uuid: Option<&str>,
        table_name: Option<&str>,
        snapshot: &penca_merge::ReadSnapshot,
    ) -> Result<(Uuid, Uuid, Uuid), ApiError>
    where
        L: DlDriver + ?Sized,
    {
        let catalog_obj = resolve_catalog(driver, catalog_uuid, catalog_name).await?;
        let catalog = parse_resolved_uuid(&catalog_obj.catalog_uuid, "catalog_uuid")?;
        let branch_obj = resolve_branch(driver, &catalog, branch_uuid, branch_name).await?;
        let branch = parse_resolved_uuid(&branch_obj.branch_uuid, "branch_uuid")?;
        let branch_str = branch.to_string();
        let table = if let Some(table_uuid) = table_uuid {
            // This read exists to surface an absent table_uuid as NOT_FOUND.
            let resolved = self
                .query_manager
                .resolve_table_by_uuid(
                    driver,
                    dl_driver,
                    &catalog,
                    table_uuid,
                    Some(&branch_str),
                    snapshot,
                )
                .await?;
            parse_resolved_uuid(&resolved.table_uuid, "table_uuid")?
        } else if let Some(table_name) = table_name {
            let schema_obj = self
                .query_manager
                .resolve_schema(
                    driver,
                    dl_driver,
                    &catalog,
                    schema_uuid,
                    schema_name,
                    Some(&branch_str),
                    snapshot,
                )
                .await?;
            let schema = parse_resolved_uuid(&schema_obj.schema_uuid, "schema_uuid")?;
            let resolved = self
                .query_manager
                .resolve_table_by_name(
                    driver,
                    dl_driver,
                    &catalog,
                    &schema,
                    table_name,
                    Some(&branch_str),
                    snapshot,
                )
                .await?;
            parse_resolved_uuid(&resolved.table_uuid, "table_uuid")?
        } else {
            return Err(ApiError::InvalidRequest(
                "must provide table_uuid, or schema (uuid or name) + table_name".into(),
            ));
        };
        Ok((catalog, branch, table))
    }
}

/// The advisory-lock key shared by snapshot creation
/// ([`LifecycleManager::snapshot`]) and snapshot retirement
/// ([`LifecycleManager::retire_snapshots`]). Both ops MUST take the same key so
/// snapshot-file reference counts change only serialized with snapshot creation
/// (ADR 0024 §4); deriving it in one place keeps the two call sites from
/// drifting apart.
fn snapshot_lock_key(table_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    format!("snapshot:{table_uuid}:{branch_uuid}")
}
