//! In-process storage read-plan + cold-segment types (CHA-445).
//!
//! These were proto messages on the `StorageMetadataService.Plan` RPC.
//! With planning now an in-process function (no wire — the RPC is
//! deleted), they are native owned structs: no prost, no `i32` format
//! codes (the `Format` enum is carried directly), and no proto3
//! `optional` ceremony on always-present fields. Built by
//! `penca_storage_meta::QueryManager::plan` and consumed by the query
//! read path (penca-merge / penca-dl) and lifecycle snapshot/compact.
//!
//! `penca-core` stays free of any `penca-proto` dependency (see
//! `format.rs`), so the plan's committed-at window is the native
//! [`CommittedAtBounds`] rather than the proto `TimestampFilter`.

use crate::Format;
use uuid::Uuid;

/// Microsecond window on `commit_micros`: inclusive lower
/// (`min_micros`), exclusive upper (`max_micros`); both optional. Native
/// in-process replacement for the proto `TimestampFilter` on the plan.
/// Field names match the proto so consumers (e.g. the merge SQL
/// committed-at clause) migrate without touching field access.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommittedAtBounds {
    /// Inclusive lower bound (microseconds since Unix epoch).
    pub min_micros: Option<i64>,
    /// Exclusive upper bound (microseconds since Unix epoch).
    pub max_micros: Option<i64>,
}

/// Window on the gapless commit-order serial `commit_seq_num` — the seq-axis
/// sibling of [`CommittedAtBounds`]. CHA-443: the hot↔cold tier fence moves
/// off `commit_micros` onto `commit_seq_num`, which is an exact total order
/// with no same-microsecond ties, so the partition needs no `+1` clamp and the
/// fence is exact at `W_persist`. The merge SQL composes these with the as-of
/// visibility predicate.
///
/// Bounds are **exclusive lower** (`min_seq`) and **inclusive upper**
/// (`max_seq`); either may be absent (an unbounded edge). The two tiers use
/// complementary halves of the fence at `W_persist` (the persist watermark):
/// - **Hot** sets `min_seq = W_persist` (serves `commit_seq_num > W_persist`),
///   `max_seq = None` (the as-of predicate caps the upper).
/// - **Cold** sets `max_seq = W_persist` (serves `commit_seq_num <= W_persist`),
///   `min_seq = None` — the cold read needs no per-row lower bound; the segment
///   fetch and the snapshot exclusion anti-join own the baseline overlap.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitSeqBounds {
    /// Exclusive lower bound on `commit_seq_num`.
    pub min_seq: Option<i64>,
    /// Inclusive upper bound on `commit_seq_num`.
    pub max_seq: Option<i64>,
}

/// Format-specific metadata for Parquet segments — lets the reader
/// compute which row groups overlap a requested row range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParquetMetadata {
    /// Row-group size used when the file was written.
    pub row_group_size: i64,
}

/// A persist segment in cold storage (one `upsert_log` / `delete_log`
/// segment), addressed by `(uri, offset, length)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistSegment {
    pub segment_uuid: String,
    /// Object storage URI (e.g. `s3://bucket/path/file.lance`).
    pub uri: String,
    /// Columnar file format of the segment.
    pub format: Format,
    pub row_count: i64,
    pub size_bytes: i64,
    pub metadata_json: String,
    pub statistics: Vec<u8>,
    /// Row offset into the file where this segment's data begins
    /// (set by `compact_persist_segments`; unset ⇒ start of file).
    pub offset: Option<i64>,
    /// Number of rows from `offset` (set by `compact_persist_segments`;
    /// unset ⇒ to end-of-file).
    pub length: Option<i64>,
    /// Inclusive per-row `commit_seq_num` ceiling: this segment contributes
    /// rows at or below it and nothing above. `None` ⇒ unbounded.
    ///
    /// **Prescriptive, not descriptive** (CHA-539). For any segment an ordinary
    /// Persist wrote the recorded value already IS the largest `commit_seq_num`
    /// in the file, so applying it is a no-op — which is what lets one uniform
    /// rule cover both cases with no "is this a copied row" branch. It bites
    /// only where a row deliberately claims less than its file holds: a fork
    /// materializes the parent's persist segments as its own reference rows,
    /// clamped to the fork position, because a fork point is an arbitrary
    /// commit-order position and the parent's segments routinely straddle it.
    ///
    /// Distinct from `PersistPlan.commit_seq.max_seq`, which is the plan-wide
    /// as-of / tier fence. The effective ceiling is the `min` of the two.
    ///
    /// `None` is load-bearing for the tx_log carriers built via
    /// `..PersistSegment::default()`: a cold `tx_log` segment reuses this type
    /// but holds commit metadata, not data rows, and has no ceiling.
    pub max_commit_seq_num: Option<i64>,
    /// `xxh3_128` of the segment's typed in-memory Arrow batch, recorded at
    /// write time and inherited verbatim by reference copies. Keys the segment
    /// cache, so a fork and its parent share one decoded entry for a byte range
    /// they both reference — which the row uuid cannot express, because a
    /// reference copy mints a new uuid over unchanged bytes (CHA-545).
    pub content_hash: Uuid,
}

/// The internal `row_uuid` index sidecar attached to a snapshot segment — the
/// resolved read-coordinates of the per-segment `(key, row_offset)` artifact
/// (CHA-412), joined onto the plan so the cold point-lookup seek (CHA-454) can
/// read it without a metadata-DB round-trip. `None` when the snapshot did not
/// materialize the internal index for this segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexSidecar {
    /// Object-storage URI of the sidecar artifact file.
    pub object_uri: String,
    /// Row offset into the file where this sidecar's rows begin.
    pub offset: i64,
    /// Number of rows in this sidecar.
    pub length: i64,
    /// Columnar file format of the sidecar.
    pub format: Format,
    /// Globally-unique id of the sidecar row, in a distinct deterministic-UUID
    /// namespace from `table_snapshot_segment_uuid`. Row identity only — the
    /// segment-cache key is `content_hash`, because a reference copy mints a
    /// fresh uuid over bytes it did not rewrite (CHA-545).
    pub segment_index_uuid: String,
    /// In-memory Arrow footprint, for the shared segment cache's byte budget.
    pub size_bytes: i64,
    /// `xxh3_128` of the sidecar's typed in-memory Arrow batch. Same role as on
    /// [`SnapshotSegment`]: sidecars are read through the same cache and
    /// reference-copied by the same paths, so they duplicate for the same
    /// reason and dedup by the same key (CHA-545).
    pub content_hash: Uuid,
}

/// A snapshot segment in cold storage (read-optimized baseline).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotSegment {
    pub table_snapshot_segment_uuid: String,
    pub table_snapshot_uuid: String,
    /// Object storage URI (e.g. `s3://bucket/path/file.lance`).
    pub uri: String,
    /// Columnar file format of the segment.
    pub format: Format,
    /// Row offset into the file where this segment's rows begin.
    /// Multiple segments may share one packed file (CHA-404).
    pub offset: i64,
    /// Number of rows in this segment's range, starting at `offset`.
    pub length: i64,
    /// Set when `format == Format::Parquet`.
    pub parquet_metadata: Option<ParquetMetadata>,
    pub row_count: i64,
    pub size_bytes: i64,
    pub metadata_json: String,
    pub statistics: Vec<u8>,
    /// Resolved internal `row_uuid` index sidecar (CHA-454), joined onto the
    /// plan from `table_snapshot_segment_index_metadata`. `None` ⇒ no internal
    /// index materialized for this segment ⇒ the cold seek falls back to a
    /// full scan.
    pub row_uuid_index_sidecar: Option<IndexSidecar>,
    /// Keyed non-identity index sidecars for this segment, keyed by
    /// `index_uuid` and sorted by it for deterministic seek iteration:
    /// user secondary indexes (CHA-485, the covering-index candidates listed
    /// in [`SnapshotPlan::indexes`]) AND the built-in system-table composite
    /// name index (CHA-481/CHA-484, keyed by its deterministic
    /// `system_name_index_uuid` — present only on the three `__penca_system__`
    /// tables, never a planner candidate). The internal identity sidecar stays
    /// in its dedicated `row_uuid_index_sidecar` slot.
    pub index_sidecars: Vec<(String, IndexSidecar)>,
    /// `xxh3_128` of the segment's typed in-memory Arrow batch, recorded at
    /// write time and inherited verbatim by reference copies (carry-forward,
    /// fork copy). Keys the segment cache — see [`PersistSegment::content_hash`].
    pub content_hash: Uuid,
}

/// A user secondary index declared for the plan's snapshot (CHA-485) — the
/// covering-index candidates the planner pass matches equality predicates
/// against. Only user indexes are listed (their snapshot parent rows carry
/// `key_columns`); the internal identity index and the built-in system name
/// index are never planner-selectable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotIndexDef {
    pub index_uuid: String,
    /// Declared key columns in sort-priority order — the sidecar's
    /// lexicographic sort order, and the order probe tuples bind by.
    pub key_columns: Vec<String>,
}

/// Snapshot-baseline portion of a cold read plan: the segments plus the
/// baseline watermark, so the merge layer knows which change-log entries
/// post-date the snapshot and must be merged on read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotPlan {
    pub segments: Vec<SnapshotSegment>,
    /// User secondary indexes declared for this snapshot (CHA-485), sorted by
    /// `index_uuid`. Empty ⇒ nothing for the planner pass to select.
    pub indexes: Vec<SnapshotIndexDef>,
    /// `commit_micros` of the latest tx included in the snapshot.
    pub snapshotted_at_micros: i64,
    /// `commit_seq_num` watermark `W_snap` — the max commit-order serial of any tx
    /// in the baseline (CHA-443 / CHA-457). The seq sibling of
    /// `snapshotted_at_micros`: the seq-aware picker bounds on it and the merge
    /// layer uses it as the change-log lower bound. `0` until IMPL-4 stamps it
    /// from the cold inputs.
    pub commit_seq_num: i64,
}

/// Persist portion of a cold read plan: upsert + delete segments
/// committed after the snapshot, with the cold committed-at window.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistPlan {
    pub upsert_segments: Vec<PersistSegment>,
    pub delete_segments: Vec<PersistSegment>,
    /// Per-row cold committed-at filter applied by the merge layer. CHA-443:
    /// retained for the `AsOfMicros` as-of visibility arm (its `max_micros` is
    /// now `as_of + 1`, exclusive — pure visibility, no longer entangled with
    /// the tier cutoff) and the audit `commit_micros` path.
    pub committed_at: Option<CommittedAtBounds>,
    /// CHA-443: the cold-tier seq fence — `(W_snap, W_persist]`. `min_seq` is
    /// the snapshot watermark `W_snap` (exclusive: change-log entries strictly
    /// after the baseline) and `max_seq` is the persist watermark `W_persist`
    /// (inclusive: the last tx the cold tier serves). The merge layer applies
    /// this instead of the old `committed_at`-based tier lower/upper bounds.
    pub commit_seq: Option<CommitSeqBounds>,
}

/// Cold-tier portion of a read plan: the snapshot baseline and/or the
/// persist segments past it. Either side may be absent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColdStoragePlan {
    pub snapshot: Option<SnapshotPlan>,
    pub persist: Option<PersistPlan>,
}

/// Hot-tier (Postgres) portion of a read plan: the per-table log table
/// names plus the committed-at window. Its presence tells the query
/// engine to include Postgres in the read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HotStoragePlan {
    pub upsert_table_name: String,
    pub delete_table_name: String,
    pub commit_tx_log_table_name: String,
    /// CHA-443: retained for the `AsOfMicros` as-of visibility arm (its
    /// `max_micros` stays the requested `as_of`); the `min_micros` hot-tier
    /// lower bound is gone — the tier fence is now `commit_seq` below.
    pub committed_at: Option<CommittedAtBounds>,
    /// CHA-443: the hot-tier seq fence — hot serves `commit_seq_num > W_persist`,
    /// so only `min_seq` (= `W_persist`, exclusive) is set; `max_seq` is left
    /// `None` (hot has no seq upper bound — the as-of cap is the visibility
    /// predicate composed on top).
    pub commit_seq: Option<CommitSeqBounds>,
}

/// CHA-178: the parent branch's cold tier as a second cold source for a
/// forked branch's read. Reuses [`ColdStoragePlan`] verbatim — the only
/// difference from the branch's own cold tier is the per-row seq ceiling,
/// which lives here rather than on the segment types.
///
/// The merge layer resolves this source at `commit_seq_ceiling`
/// (= `min(fork_commit_seq_num, as_of_seq)`) and folds it in *below* the
/// child (`hot > child-cold > parent-cold`) via a `row_uuid` anti-join: a
/// parent row survives iff the child never touched that `row_uuid`. This is
/// exact because the child's seqs (`> fork_seed`) strictly dominate the
/// parent's (`<= fork_seed`), so no cross-source `commit_micros` comparison
/// is needed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BaseColdStorage {
    /// The parent branch's cold plan (snapshot baseline and/or persist
    /// segments), enumerated keyed on the parent `branch_uuid`.
    pub cold: ColdStoragePlan,
    /// Inclusive per-row `commit_seq_num` upper bound for the parent source
    /// = `min(fork_commit_seq_num, as_of_seq)`. Caps the parent at the fork
    /// so the child never sees the parent's post-fork commits.
    pub commit_seq_ceiling: i64,
}

/// A complete read plan spanning hot + cold tiers. In-process
/// replacement for the proto `PlanResponse`. The query engine executes
/// both portions and merges the results.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub hot_storage: Option<HotStoragePlan>,
    pub cold_storage: Option<ColdStoragePlan>,
    /// CHA-178: for a forked branch, the parent's cold tier as a second
    /// source (with its own seq ceiling). `None` for non-forked branches
    /// and once the child's own snapshot covers the fork — so a non-forked
    /// read is byte-identical to before this field existed.
    pub base_cold_storage: Option<BaseColdStorage>,
}

// `Default` is provided manually for the segment types so callers (and
// test fixtures, which previously leaned on prost's derived `Default`)
// can `..Default::default()`. `Format` itself stays default-free by
// design (no `Unspecified` — see `format.rs`), so the segments pick
// `Format::Parquet` as the placeholder; any test that cares sets it.
//
// `content_hash` defaults to `Uuid::nil()`, which would be a shared cache key
// if it ever reached the cache. It cannot: the only production caller of these
// `Default`s is the cold `tx_log` carrier path, whose segments hold commit
// metadata and are read by `read_tx_log_batches`, never through `SegmentCache`
// (CHA-545). Every cache-read segment is built field-by-field from a metadata
// row whose `content_hash` is `NOT NULL`. `IndexSidecar` deliberately has no
// `Default` for the same reason — it has no such non-cached carrier path, so a
// nil hash there would be reachable.
impl Default for PersistSegment {
    fn default() -> Self {
        Self {
            segment_uuid: String::new(),
            uri: String::new(),
            format: Format::Parquet,
            row_count: 0,
            size_bytes: 0,
            metadata_json: String::new(),
            statistics: Vec::new(),
            offset: None,
            length: None,
            max_commit_seq_num: None,
            content_hash: Uuid::nil(),
        }
    }
}

impl Default for SnapshotSegment {
    fn default() -> Self {
        Self {
            table_snapshot_segment_uuid: String::new(),
            table_snapshot_uuid: String::new(),
            uri: String::new(),
            format: Format::Parquet,
            offset: 0,
            length: 0,
            parquet_metadata: None,
            row_count: 0,
            size_bytes: 0,
            metadata_json: String::new(),
            statistics: Vec::new(),
            row_uuid_index_sidecar: None,
            index_sidecars: Vec::new(),
            content_hash: Uuid::nil(),
        }
    }
}
