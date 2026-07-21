//! Write- and lifecycle-side metadata store: catalog / schema / table /
//! branch / transaction / log-segment / snapshot writes plus the
//! lifecycle-side reads (persist, purge, compact, snapshot). CHA-472 (ADR
//! 0028) rehomed the query-side reads — the read-plan assembly and the
//! `__penca_system__` resolves/getters — onto `penca_api::QueryManager`, so
//! this struct is the write/lifecycle remainder, renamed [`LifecycleManager`].
//!
//! This is the Rust port of
//! `packages/penca/src/penca/lib/storage/metadata.py`.

#![allow(clippy::too_many_arguments)]

mod branch;
mod catalog;
mod compact;
// CHA-472: `convert` + `helpers` are now consumed cross-crate by the read
// methods rehomed onto `penca_api::QueryManager` (the `rb_*` row decoders,
// `extract_first_binary`, and the `parse_uuid`/`qi`/`epoch`/`resolve_branch`
// call-site aliases), so both modules are `pub`.
pub mod convert;
mod ddl;
pub mod helpers;
mod index;
mod lifecycle;
mod persist;
mod purge;
mod schema;
mod segment_index;
mod snapshot;
mod table;
mod tx_log;
pub mod watermarks;

// Load-bearing re-export: `penca-api::write` imports the `rb_*`
// helpers as `use penca_storage_meta::{LifecycleManager, rb_binary,
// rb_opt_i32, rb_opt_i64, rb_str, rb_string_list, rb_uuid_str};`.
// See CHA-295's Phase-2 plan edit log for the verification.
pub use convert::{rb_binary, rb_opt_i32, rb_opt_i64, rb_str, rb_string_list, rb_uuid_str};
pub use snapshot::{RetentionFloor, retention_floor_select, retention_window_start_expr};
pub use tx_log::{TxLogSegment, tx_log_arrow_schema};

use penca_core::SnapshotIndexDef;
use penca_core::SnapshotSegment;
use penca_db::dialect::pg::DataTableError;

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error(transparent)]
    DataTable(#[from] DataTableError),

    #[error(transparent)]
    Uuid(#[from] uuid::Error),
}

pub type Result<T> = std::result::Result<T, MetadataError>;

/// Result of `penca_api::QueryManager::read_snapshot_segments_for_table`.
pub struct SnapshotResult {
    /// `snapshotted_at_micros` of the latest committed snapshot, or
    /// `None` when no committed snapshot exists for the table.
    pub snapshotted_at_micros: Option<i64>,
    /// `commit_seq_num` watermark `W_snap` of the latest committed snapshot
    /// (CHA-443), or `None` when no committed snapshot exists. The seq sibling
    /// of `snapshotted_at_micros` — the seq-aware picker (IMPL-3) and the read
    /// Plan (IMPL-4) consume it.
    pub commit_seq_num: Option<i64>,
    pub snapshot_segments: Vec<SnapshotSegment>,
    /// CHA-485: user-index defs declared for the picked snapshot (parent rows
    /// with non-NULL `key_columns`), sorted by `index_uuid`. Planner
    /// covering-index candidates; empty when none are declared.
    pub indexes: Vec<SnapshotIndexDef>,
    /// The latest snapshot's recorded write-time partition keys (CHA-404
    /// parent column). `None` = SQL NULL (a pre-CHA-404 parent row →
    /// carry-forward ineligible, force full rewrite); `Some(vec![])` =
    /// `{}` (known: the table declares no partition keys). CHA-406's
    /// key-change detection relies on the NULL-vs-empty distinction.
    pub partition_keys: Option<Vec<String>>,
    /// The latest snapshot's recorded write-time clustering keys (already
    /// resolved to the primary-key default at write time). Same
    /// `None` = NULL / `Some(vec![])` = `{}` semantics as
    /// [`SnapshotResult::partition_keys`].
    pub clustering_keys: Option<Vec<String>>,
}

/// One untouched prior-snapshot segment carried forward by reference
/// (CHA-406): a new `table_snapshot_segment_uuid` under the new
/// snapshot pointing at the SAME prior file (`object_uri` + `offset` +
/// `length`), assigned the next dense `chunk_idx` in label order.
///
/// Consumed by [`LifecycleManager::insert_carried_snapshot_segments`],
/// which copies the prior row's storage columns server-side rather than
/// re-reading the file. Produced by the pack stream's carried-interleave
/// (`penca_api::lifecycle::packer`).
#[derive(Debug, Clone)]
pub struct CarriedSegmentSpec {
    /// Deterministic `table_snapshot_segment_uuid` of the new carried
    /// row: `table_snapshot_segment_uuid(new_snap_uuid, chunk_idx)`.
    pub new_seg_uuid_str: String,
    /// The new row's dense label-ordered segment index in the new
    /// snapshot cycle.
    pub chunk_idx: u32,
    /// The prior committed segment row whose storage columns are copied.
    pub prior_seg_uuid_str: String,
}

/// A committed `table_snapshot_segment_index_metadata` child sidecar row
/// (CHA-412). The planning-read shape
/// ([`LifecycleManager::list_segment_index_metadata`]) consumed by the cold index
/// seek (CHA-454) and the lifecycle GC enqueue.
#[derive(Debug, Clone)]
pub struct SegmentIndexMetadata {
    pub segment_index_uuid: String,
    pub segment_uuid: String,
    /// The parent `table_snapshot_index_metadata` row this sidecar belongs to.
    /// The index identity (internal `row_uuid` vs a user index) lives on that
    /// parent's `index_uuid`; the child does not duplicate it.
    pub table_snapshot_index_uuid: String,
    pub object_uri: String,
    pub offset: i64,
    pub length: i64,
    pub format: String,
    pub size_bytes: i64,
    /// Indexed-key min/max bounds (binary) the CHA-454 cold seek consults
    /// in-planner. The internal `row_uuid` index leaves this empty (`&[]`) —
    /// a uniform hash has no useful bounds; ordered user indexes (CHA-463)
    /// populate it.
    pub statistics: Vec<u8>,
}

/// A committed `table_snapshot_index_metadata` parent row (CHA-412): the
/// per-`(snapshot, index)` header the planner reads ("does snapshot S have
/// index X?"). The internal `row_uuid` index is the row with `index_uuid` NULL.
#[derive(Debug, Clone)]
pub struct TableSnapshotIndexMetadata {
    pub table_snapshot_index_uuid: String,
    /// `None` ⇒ the strictly-internal `row_uuid` identity index; otherwise a
    /// logical reference to `__penca_system__.indexes` (ADR 0015, not an FK).
    pub index_uuid: Option<String>,
}

/// Write- and lifecycle-side metadata store backed by Postgres (CHA-472: the
/// query-side reads were rehomed onto `penca_api::QueryManager`).
///
/// Stateless unit struct — all methods take an explicit driver.
/// The caller owns the driver and transaction lifecycle.
pub struct LifecycleManager;
