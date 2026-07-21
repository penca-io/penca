//! Public contract types for cold-tier log tables.
//!
//! Split from [`crate::provider`] because these types cross the crate
//! boundary — they're the schema/name contract that
//! [`crate::driver::DlDriver::execute_sql`] takes as a parameter and
//! that `penca-merge`'s SQL builder references when emitting DataFusion
//! SQL. The provider module is an implementation detail; this module is
//! the public surface.

use arrow::datatypes::SchemaRef;

/// Well-known name the merge-on-read SQL builder uses for the upsert log.
pub const UPSERT_LOG_TABLE: &str = "upsert_log";
/// Well-known name the merge-on-read SQL builder uses for the delete log.
pub const DELETE_LOG_TABLE: &str = "delete_log";

/// Well-known name the CHA-411 snapshot scan registers the
/// [`crate::provider::SnapshotTableProvider`] under (aliased `l`, matching the
/// `l.`-qualified merge filter convention). Shared between
/// `build_snapshot_session` (registration) and
/// `penca_merge::sql::build_cold_snapshot_scan` (SQL generation) so the two
/// cannot desync — mirrors `UPSERT_LOG_TABLE` / `DELETE_LOG_TABLE`.
pub const SNAPSHOT_TABLE: &str = "l";
/// Well-known name the snapshot scan registers the single-column `row_uuid`
/// exclusion `MemTable` under (the anti-join target).
pub const EXCLUSION_TABLE: &str = "exclusion";

/// Arrow schemas for the two log tables (`upsert_log`, `delete_log`).
///
/// Consumed by [`crate::driver::DlDriver::execute_sql`]; the production
/// impl uses them to register DataFusion `TableProvider`s under the
/// well-known log-table names.
///
/// CHA-218: commit_tx_log is hot-only — cold has no commit_tx_log table to register.
/// Per-row tx metadata is carried inline on each upsert/delete row.
pub struct LogSchemas {
    pub upsert: SchemaRef,
    pub delete: SchemaRef,
}
