//! Data-lake driver abstraction.
//!
//! Peer to [`penca_db::driver::DbDriver`]. Where `DbDriver` issues SQL
//! against a transactional database, [`DlDriver`] issues SQL against an
//! analytical engine whose tables (`upsert_log`, `delete_log`) are
//! backed by cold-storage persist segments.
//!
//! The trait is intentionally generic — it takes a raw SQL string and
//! returns a [`RecordBatch`]. The caller builds SQL via the shared
//! `penca-merge::sql` builders (or any other SQL source) and hands it
//! here for execution.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::{SessionContext, SessionState};
use penca_core::{ColdStoragePlan, IndexSidecar, PersistSegment, SnapshotSegment};
use penca_format::reader::{FormatError, FormatReader};
use penca_storage_cold::{COMMIT_SEQ_NUM_COLUMN, ColdStorageError};
use tracing::Instrument as _;
use uuid::Uuid;

use crate::cache::SegmentCache;
use crate::provider::{build_persist_session, build_snapshot_session};
use crate::schema::LogSchemas;
use crate::session_template::derive_cold_session;

/// Errors raised by [`DlDriver`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum DlError {
    #[error(transparent)]
    ColdStorage(#[from] ColdStorageError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
}

/// Abstraction over the cold (data-lake) tier for penca read paths.
///
/// `execute_sql` runs arbitrary SQL against an engine that has the two
/// cold log tables (`upsert_log`, `delete_log`) registered under their
/// well-known names. `scan_snapshot` reads the snapshot tier through a
/// registered `SnapshotTableProvider`, applying the exclusion anti-join and
/// residual in the scan plan (CHA-411).
#[async_trait]
pub trait DlDriver: Send + Sync {
    /// Derive a fresh cold-tier [`SessionContext`] from the driver's
    /// process-wide session template — a microsecond clone of the function
    /// registry + analyzer/optimizer rules with a fresh, isolated catalog.
    /// Cold-read paths must obtain sessions this way rather than through
    /// `SessionContext::new()`, which reassembles the registry map and the
    /// rule lists on every call.
    fn derive_session(&self) -> SessionContext;

    /// Execute `sql` against the cold tier's log tables for this plan.
    ///
    /// The impl is responsible for registering the two log tables
    /// (`upsert_log` / `delete_log`) under their well-known names before
    /// running the query.
    ///
    /// commit_tx_log is hot-only — cold rows carry tx metadata inline.
    async fn execute_sql(
        &self,
        plan: &ColdStoragePlan,
        sql: &str,
        log_schemas: &LogSchemas,
    ) -> Result<RecordBatch, DlError>;

    /// Scan the (already-pruned) snapshot `segments` through a registered
    /// `SnapshotTableProvider`, applying the exclusion-set anti-join and the
    /// residual filter expressed in `sql` (CHA-411). Returns a stream of
    /// post-filter batches.
    ///
    /// The impl registers the provider under `l` and a single-column
    /// `row_uuid` exclusion `MemTable` under `exclusion`, then runs `sql`
    /// (built by the caller, e.g. `penca_merge::sql::build_cold_snapshot_scan`).
    /// `full_schema` is the unprojected decode schema; `out_schema` is the
    /// declared output. `segments` are already pruned upstream (ADR 0022).
    ///
    /// A wide seam by design: this is the one crossing from penca-merge
    /// into the provider; bundling into a request struct would force
    /// every impl (incl. test fakes) through a builder for no call-site
    /// gain.
    #[allow(clippy::too_many_arguments)]
    async fn scan_snapshot(
        &self,
        segments: &[SnapshotSegment],
        full_schema: &SchemaRef,
        out_schema: &SchemaRef,
        exclusion: &[String],
        sql: &str,
        segment_read_concurrency: usize,
        order: SegmentOrder,
        // Seek entries: per scanned segment the provider resolves each entry to
        // its sidecar, seeks, and INTERSECTS the offsets before decoding.
        // Unresolved entries are skipped and a fully unresolved set falls back
        // to the full scan — always correct, because `sql`'s residual re-applies
        // exactness (ADR 0023).
        seeks: Option<Arc<Vec<SeekSpec>>>,
    ) -> Result<SendableRecordBatchStream, DlError>;

    /// DataFusion-free snapshot point seek: serve an exact index-seek selection
    /// straight from the snapshot segments' sidecars, with no merge plan.
    /// `index_uuid` selects the sidecar (`None` = internal `row_uuid` identity,
    /// `Some` = a name or user index); `key_columns` names that index's key
    /// columns so the sidecar key types are read from the table schema (empty =
    /// the all-Utf8 identity/name shape), letting a typed non-Utf8 user index
    /// decode on the bypass too.
    ///
    /// Returns `Ok(None)` when the seek cannot be served — no seek kernel, an
    /// in-scope segment lacking the selected sidecar, or a key column absent
    /// from the schema. The caller falls back to the merge pipeline either way,
    /// so overriding is purely an optimization, never a correctness requirement.
    async fn seek_snapshot_point(
        &self,
        _segments: &[SnapshotSegment],
        _probe_tuples: &[Vec<String>],
        _index_uuid: Option<&Uuid>,
        _key_columns: &[String],
        _full_decode_schema: &SchemaRef,
        _out_schema: &SchemaRef,
    ) -> Result<Option<RecordBatch>, DlError> {
        Ok(None)
    }
}

/// Snapshot-segment delivery order for [`DlDriver::scan_snapshot`].
///
/// `ByCompletion` overlaps reads and yields as each segment finishes —
/// the query path's default (segment order is not client-observable).
/// `ByPlan` preserves the planned segment order with bounded readahead,
/// still capped by `segment_read_concurrency`; the snapshot writer depends
/// on it for label-sorted partition runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentOrder {
    ByCompletion,
    ByPlan,
}

/// One seek entry at the penca-merge → provider boundary — the mirror of
/// `penca_merge::IndexSeek` (the dependency direction forbids sharing the
/// type). `index_uuid: None` is the internal identity index (resolved against
/// the segment's dedicated `row_uuid_index_sidecar`); `Some` is a keyed index
/// (resolved against the segment's `index_sidecars`). Within an entry the
/// tuples are a union (IN-list of composite keys); across entries the
/// per-segment offsets INTERSECT before the base decode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeekSpec {
    pub index_uuid: Option<String>,
    /// Key column names in sort-priority order; empty for the identity
    /// entry. The seek maps these through the table schema to decode a
    /// typed (non-Utf8) sidecar against its real key schema.
    pub key_columns: Vec<String>,
    pub tuples: Vec<Vec<String>>,
}

/// Resolve a seek's requested index to a segment's sidecar by `index_uuid` —
/// `None` = the internal `row_uuid` identity index (dedicated
/// `row_uuid_index_sidecar` slot), `Some(uuid)` = a keyed index looked up in
/// `index_sidecars`. A requested index no sidecar records resolves to `None`
/// → the caller's fallback, never a mis-seek.
fn selected_sidecar<'s>(
    segment: &'s SnapshotSegment,
    index_uuid: Option<&Uuid>,
) -> Option<&'s IndexSidecar> {
    // `index_sidecars` is keyed by the uuid's string form (the DB
    // `parent_index_uuid`).
    let uuid_str = index_uuid.map(Uuid::to_string);
    crate::provider::sidecar_for_index(segment, uuid_str.as_deref())
}

/// Production [`DlDriver`] backed by penca-datafusion + a
/// [`FormatReader`] map keyed by format discriminant.
pub struct DatafusionDlDriver<R: FormatReader + 'static> {
    readers: Arc<HashMap<i32, R>>,
    /// Process-lifetime cache of decoded snapshot segments. Shared across all
    /// per-query drivers; `SegmentCache::disabled()` for services that never
    /// serve cached snapshot reads.
    cache: Arc<SegmentCache>,
    /// Process-wide cold-session template: the default function registry +
    /// analyzer/optimizer rules, built once per service and injected here.
    /// Every per-unit cold `SessionContext` is a ~71 µs clone of it against
    /// ~128 µs for `SessionContext::new()` (release measurements).
    template: Arc<SessionState>,
}

impl<R: FormatReader + 'static> DatafusionDlDriver<R> {
    pub fn new(
        readers: Arc<HashMap<i32, R>>,
        cache: Arc<SegmentCache>,
        template: Arc<SessionState>,
    ) -> Self {
        Self {
            readers,
            cache,
            template,
        }
    }
}

/// Cacheable miss: decode the WHOLE segment (all columns, no filter
/// pushdown) so the cached entry is reusable across any projection, insert
/// it under `weight`, and return the full superset. The caller has already
/// decided this segment is admissible.
async fn read_and_cache_full<R: FormatReader>(
    reader: &R,
    cache: &SegmentCache,
    segment: &SnapshotSegment,
    full_schema: &SchemaRef,
    weight: u64,
) -> Result<RecordBatch, DlError> {
    let full_cols: Vec<&str> = full_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let batch = reader
        .read_segment(
            &segment.uri,
            Some(segment.offset),
            Some(segment.length),
            full_schema,
            Some(&full_cols),
        )
        .await
        .map_err(ColdStorageError::from)?;
    let batch = Arc::new(batch);
    cache.insert(
        segment.table_snapshot_segment_uuid.clone(),
        Arc::clone(&batch),
        weight,
    );
    tracing::debug!(
        rows = batch.num_rows(),
        "snapshot segment cached full decode"
    );
    Ok((*batch).clone())
}

/// Non-cacheable miss: a projected read of just `out_schema`, not cached.
/// An oversized segment is decoded to the narrow output schema rather than
/// the full schema, so it is never widened just to be discarded.
async fn read_projected_uncached<R: FormatReader>(
    reader: &R,
    segment: &SnapshotSegment,
    out_schema: &SchemaRef,
) -> Result<RecordBatch, DlError> {
    let out_cols: Vec<&str> = out_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let batch = reader
        .read_segment(
            &segment.uri,
            Some(segment.offset),
            Some(segment.length),
            out_schema,
            Some(&out_cols),
        )
        .await
        .map_err(ColdStorageError::from)?;
    tracing::debug!(
        rows = batch.num_rows(),
        "snapshot segment projected (uncached)"
    );
    Ok(batch)
}

/// Cache-aware read of a single snapshot segment. Returns the full decoded
/// superset on hit / miss-cached, or the projected `out_schema` batch on the
/// non-cacheable (oversized) path. No predicate is pushed (ADR 0023); the
/// caller projects / null-fills downstream.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        segment_uuid = %segment.table_snapshot_segment_uuid,
        format = %segment.format,
        cache = tracing::field::Empty,
    ),
)]
pub(crate) async fn read_cached_snapshot_segment<R: FormatReader + 'static>(
    readers: &HashMap<i32, R>,
    cache: &SegmentCache,
    segment: &SnapshotSegment,
    full_schema: &SchemaRef,
    out_schema: &SchemaRef,
) -> Result<RecordBatch, DlError> {
    let span = tracing::Span::current();
    let uuid = segment.table_snapshot_segment_uuid.as_str();

    if let Some(full) = cache.get(uuid) {
        span.record("cache", "hit");
        tracing::debug!(rows = full.num_rows(), "snapshot segment cache hit");
        return Ok((*full).clone());
    }

    let code = segment.format.as_wire_code();
    let reader = readers
        .get(&code)
        .ok_or(ColdStorageError::UnknownFormat(code))?;
    // size_bytes is the in-memory Arrow footprint, not the encoded file size —
    // the same units as the cache's byte budget.
    let weight = segment.size_bytes.max(0) as u64;

    if cache.admits(weight) {
        span.record("cache", "miss-cached");
        read_and_cache_full(reader, cache, segment, full_schema, weight).await
    } else {
        span.record("cache", "miss-uncached");
        read_projected_uncached(reader, segment, out_schema).await
    }
}

/// Cacheable persist miss: decode the WHOLE persist segment (all columns) so the
/// cached entry serves any projection, insert it under `weight`, and return the
/// full superset.
async fn read_and_cache_full_persist<R: FormatReader>(
    reader: &R,
    cache: &SegmentCache,
    segment: &PersistSegment,
    full_schema: &SchemaRef,
    weight: u64,
) -> Result<RecordBatch, DlError> {
    let full_cols: Vec<&str> = full_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let batch = reader
        .read_segment(
            &segment.uri,
            segment.offset,
            segment.length,
            full_schema,
            Some(&full_cols),
        )
        .await
        .map_err(ColdStorageError::from)?;
    let batch = Arc::new(batch);
    cache.insert(segment.segment_uuid.clone(), Arc::clone(&batch), weight);
    tracing::debug!(
        rows = batch.num_rows(),
        "persist segment decoded and cached"
    );
    Ok((*batch).clone())
}

/// Non-cacheable persist miss: a projected read of just `out_schema`, not cached.
/// An oversized persist segment is read narrow rather than widened only to be
/// discarded.
///
/// Exception: a segment carrying a `max_commit_seq_num` ceiling is widened by
/// `commit_seq_num` when the projection drops it, because the caller has to
/// filter on that column and cannot recover it afterwards. Narrowing here is an
/// optimization; the ceiling is a correctness bound, so the bound wins. Without
/// this the ceiling would hold on the cached path and silently vanish on the
/// uncached one — wrong rows, no signal, and cache-state dependent.
async fn read_projected_uncached_persist<R: FormatReader>(
    reader: &R,
    segment: &PersistSegment,
    out_schema: &SchemaRef,
    full_schema: &SchemaRef,
) -> Result<RecordBatch, DlError> {
    let read_schema = match segment.max_commit_seq_num {
        Some(_) if out_schema.index_of(COMMIT_SEQ_NUM_COLUMN).is_err() => {
            let mut fields: Vec<_> = out_schema.fields().iter().cloned().collect();
            fields.push(
                full_schema
                    .field(
                        full_schema
                            .index_of(COMMIT_SEQ_NUM_COLUMN)
                            .map_err(|e| ColdStorageError::from(FormatError::Arrow(e)))?,
                    )
                    .clone()
                    .into(),
            );
            Arc::new(Schema::new(fields))
        }
        _ => out_schema.clone(),
    };
    let out_cols: Vec<&str> = read_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let batch = reader
        .read_segment(
            &segment.uri,
            segment.offset,
            segment.length,
            &read_schema,
            Some(&out_cols),
        )
        .await
        .map_err(ColdStorageError::from)?;
    tracing::debug!(
        rows = batch.num_rows(),
        "persist segment projected (uncached)"
    );
    Ok(batch)
}

/// Cache-aware read of a single persist segment. A persist segment file is
/// immutable once written and keyed by its globally-unique `segment_uuid`, so it
/// shares the process-lifetime [`SegmentCache`] with snapshot segments under one
/// byte budget, with NO TTL — W-TinyLFU eviction plus the `admits` budget gate
/// is the whole mechanism. (The *resolved* persist tier is mutable under
/// retention compaction, which is why the tier is re-resolved live on every
/// read; the per-uuid *file bytes* this caches are not.) Returns the full
/// decoded superset on hit / miss-cached, or the projected `out_schema` batch on
/// the non-cacheable (oversized) path; the caller projects / null-fills
/// downstream.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        segment_uuid = %segment.segment_uuid,
        format = %segment.format,
        cache = tracing::field::Empty,
    ),
)]
pub(crate) async fn read_cached_persist_segment<R: FormatReader + 'static>(
    readers: &HashMap<i32, R>,
    cache: &SegmentCache,
    segment: &PersistSegment,
    full_schema: &SchemaRef,
    out_schema: &SchemaRef,
) -> Result<RecordBatch, DlError> {
    let span = tracing::Span::current();
    let uuid = segment.segment_uuid.as_str();

    if let Some(full) = cache.get(uuid) {
        span.record("cache", "hit");
        tracing::debug!(rows = full.num_rows(), "persist segment cache hit");
        return Ok((*full).clone());
    }

    let code = segment.format.as_wire_code();
    let reader = readers
        .get(&code)
        .ok_or(ColdStorageError::UnknownFormat(code))?;
    // size_bytes is the in-memory Arrow footprint the persist write path records
    // (`in_memory_bytes`) — the same units as the snapshot weigher, so both
    // tiers share one consistent byte budget.
    let weight = segment.size_bytes.max(0) as u64;

    if cache.admits(weight) {
        span.record("cache", "miss-cached");
        read_and_cache_full_persist(reader, cache, segment, full_schema, weight).await
    } else {
        span.record("cache", "miss-uncached");
        read_projected_uncached_persist(reader, segment, out_schema, full_schema).await
    }
}

/// Load a sorted `(key, row_offset)` index sidecar through the shared snapshot
/// cache, keyed by its own `segment_index_uuid` — a distinct deterministic-UUID
/// namespace from the base segment uuid, so the two never collide in one cache.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        segment_index_uuid = %sidecar.segment_index_uuid,
        cache = tracing::field::Empty,
    ),
)]
async fn read_cached_index_sidecar<R: FormatReader + 'static>(
    readers: &HashMap<i32, R>,
    cache: &SegmentCache,
    sidecar: &IndexSidecar,
    key_types: &[arrow::datatypes::DataType],
) -> Result<RecordBatch, DlError> {
    let span = tracing::Span::current();
    if let Some(batch) = cache.get(&sidecar.segment_index_uuid) {
        span.record("cache", "hit");
        return Ok((*batch).clone());
    }
    span.record("cache", "miss");
    let code = sidecar.format.as_wire_code();
    let reader = readers
        .get(&code)
        .ok_or(ColdStorageError::UnknownFormat(code))?;
    // The sidecar's key schema is the indexed columns' native types; the
    // identity/name sidecars are the all-Utf8 special case.
    let schema = penca_format::index::segment_index_schema(key_types);
    let batch = reader
        .read_segment(
            &sidecar.object_uri,
            Some(sidecar.offset),
            Some(sidecar.length),
            &schema,
            None,
        )
        .await
        .map_err(ColdStorageError::from)?;
    let batch = Arc::new(batch);
    // `insert` self-gates on `cache.admits(weight)`, so an oversize sidecar is
    // decoded-but-not-cached rather than evicting the whole budget.
    cache.insert(
        sidecar.segment_index_uuid.clone(),
        Arc::clone(&batch),
        sidecar.size_bytes.max(0) as u64,
    );
    Ok((*batch).clone())
}

/// Index-driven selective read: binary-search the segment's index sidecar for
/// `probe_tuples`, then `take` exactly the matching rows from the base segment
/// — so the provider emits O(matches) rows instead of streaming the whole
/// segment. The cold-MISS path still full-decodes the base (selective row-group
/// decode is TODO(CHA-469)).
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        segment_uuid = %segment.table_snapshot_segment_uuid,
        probe_tuples = probe_tuples.len(),
        matched = tracing::field::Empty,
    ),
)]
// Bundling into a struct buys nothing for an internal fn threaded from a
// single caller.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_seeked_snapshot_segment<R: FormatReader + 'static>(
    readers: &HashMap<i32, R>,
    cache: &SegmentCache,
    segment: &SnapshotSegment,
    sidecar: &IndexSidecar,
    // The sidecar's key column types, derived from the table schema by the
    // caller via `key_column_types` (identity/name sidecars are all-Utf8).
    key_types: &[arrow::datatypes::DataType],
    full_schema: &SchemaRef,
    out_schema: &SchemaRef,
    probe_tuples: &[&[&str]],
) -> Result<RecordBatch, DlError> {
    // An empty probe set seeks nothing — skip the sidecar decode entirely, as
    // its composite key arity is undefined with no probe tuple.
    if probe_tuples.is_empty() {
        return Ok(RecordBatch::new_empty(full_schema.clone()));
    }
    let offsets = seek_entry_offsets(readers, cache, sidecar, key_types, probe_tuples).await?;
    tracing::Span::current().record("matched", offsets.len());
    take_matched_rows(readers, cache, segment, full_schema, out_schema, offsets).await
}

/// Seek one entry's sidecar for its probe tuples → the matching sorted,
/// deduped segment-relative offsets.
async fn seek_entry_offsets<R: FormatReader + 'static>(
    readers: &HashMap<i32, R>,
    cache: &SegmentCache,
    sidecar: &IndexSidecar,
    key_types: &[arrow::datatypes::DataType],
    probe_tuples: &[&[&str]],
) -> Result<Vec<i64>, DlError> {
    let sidecar_batch = read_cached_index_sidecar(readers, cache, sidecar, key_types).await?;
    penca_format::index::seek_row_offsets(&sidecar_batch, probe_tuples)
        .map_err(|e| ColdStorageError::from(e).into())
}

/// Decode the base segment and `take` the matched offsets — or skip the
/// decode entirely on zero matches. A candidate segment that passes coarse
/// pruning but doesn't contain the probed key must not pay a full base
/// decode just to `take` zero rows (the common cross-segment-lookup miss).
async fn take_matched_rows<R: FormatReader + 'static>(
    readers: &HashMap<i32, R>,
    cache: &SegmentCache,
    segment: &SnapshotSegment,
    full_schema: &SchemaRef,
    out_schema: &SchemaRef,
    offsets: Vec<i64>,
) -> Result<RecordBatch, DlError> {
    if offsets.is_empty() {
        return Ok(RecordBatch::new_empty(full_schema.clone()));
    }
    let base =
        read_cached_snapshot_segment(readers, cache, segment, full_schema, out_schema).await?;
    let indices = arrow::array::Int64Array::from(offsets);
    let taken = arrow::compute::take_record_batch(&base, &indices)?;
    Ok(taken)
}

/// Seek SEVERAL resolved entries against one segment and decode the
/// INTERSECTION of their offsets — AND across the covering indexes the
/// planner selected. Each entry's offsets are sorted + deduped (kernel
/// contract), so the intersection is a two-pointer merge; an entry with an
/// empty probe set, or any empty per-entry result, short-circuits to the
/// empty intersection.
#[tracing::instrument(
    level = "trace",
    skip_all,
    fields(
        segment_uuid = %segment.table_snapshot_segment_uuid,
        seek_entries = entries.len(),
        index_seek_offsets = tracing::field::Empty,
    ),
)]
pub(crate) async fn read_intersect_seeked_snapshot_segment<R: FormatReader + 'static>(
    readers: &HashMap<i32, R>,
    cache: &SegmentCache,
    segment: &SnapshotSegment,
    entries: &[(&IndexSidecar, &SeekSpec)],
    full_schema: &SchemaRef,
    out_schema: &SchemaRef,
) -> Result<RecordBatch, DlError> {
    let mut intersected: Option<Vec<i64>> = None;
    for (sidecar, spec) in entries {
        if spec.tuples.is_empty() {
            intersected = Some(Vec::new());
            break;
        }

        let Some(key_types) = entry_key_types(spec, full_schema) else {
            tracing::debug!(
                index_uuid = ?spec.index_uuid,
                "seek entry key column missing from table schema; skipping entry"
            );
            continue;
        };
        let probe_refs: Vec<Vec<&str>> = spec
            .tuples
            .iter()
            .map(|tuple| tuple.iter().map(String::as_str).collect())
            .collect();
        let probes: Vec<&[&str]> = probe_refs.iter().map(Vec::as_slice).collect();
        let offsets = seek_entry_offsets(readers, cache, sidecar, &key_types, &probes).await?;
        intersected = Some(match intersected {
            None => offsets,
            Some(prev) => intersect_sorted(&prev, &offsets),
        });
        if intersected.as_ref().is_some_and(Vec::is_empty) {
            break;
        }
    }
    // Every entry skipped (e.g. key columns missing from the schema) means
    // NO selection was performed — that must be the full scan, never an
    // empty result (an empty intersection is only valid when at least one
    // entry actually seeked).
    let Some(offsets) = intersected else {
        return read_cached_snapshot_segment(readers, cache, segment, full_schema, out_schema)
            .await;
    };
    tracing::Span::current().record("index_seek_offsets", offsets.len());
    take_matched_rows(readers, cache, segment, full_schema, out_schema, offsets).await
}

/// The sidecar key schema one seek entry decodes against: the identity
/// entry (no key columns) is the all-Utf8 shape (arity from the probes); a
/// user entry maps its key column names through the table schema to their
/// native types. `None` when a name is missing from the schema — the caller
/// skips the entry (over-selection is safe under the residual).
///
/// Precondition: `spec.tuples` is non-empty — the intersect loop
/// short-circuits an empty probe set to the empty intersection BEFORE
/// resolving types (an empty set is a selection decision, not a skip).
fn entry_key_types(
    spec: &SeekSpec,
    full_schema: &SchemaRef,
) -> Option<Vec<arrow::datatypes::DataType>> {
    debug_assert!(
        !spec.tuples.is_empty(),
        "caller short-circuits empty probe sets before type resolution"
    );
    key_column_types(&spec.key_columns, spec.tuples[0].len(), full_schema)
}

/// The sidecar key types for a seek's key columns, read straight from the
/// table's Arrow schema — the index build writes sidecar keys in the columns'
/// native types, so a lookup by name recovers them with no round-trip. Empty
/// `key_columns` is the identity/name shape (all-Utf8, `arity` from the probe
/// tuple). `None` when a key column is absent from the schema — the caller
/// falls back to the full merge (over-selection is safe under the residual).
fn key_column_types(
    key_columns: &[String],
    arity: usize,
    full_schema: &SchemaRef,
) -> Option<Vec<arrow::datatypes::DataType>> {
    if key_columns.is_empty() {
        return Some(vec![arrow::datatypes::DataType::Utf8; arity]);
    }
    key_columns
        .iter()
        .map(|column| {
            full_schema
                .field_with_name(column)
                .map(|field| field.data_type().clone())
                .ok()
        })
        .collect()
}

/// Intersection of two sorted, deduped offset vectors (two-pointer merge).
fn intersect_sorted(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

#[async_trait]
impl<R: FormatReader + 'static> DlDriver for DatafusionDlDriver<R> {
    /// DataFusion-free aggregate cold seek for a default-current-time,
    /// snapshot-only point read. Yields the same ROW SET as `stream_all_cold`
    /// for this plan shape (snapshot-only ⇒ empty exclusion set, and the seek
    /// key is an exact selection) without building a DataFusion plan. Row
    /// *order* is unspecified in both paths (neither advertises an
    /// `output_ordering`), so the equivalence is over the row set only.
    ///
    /// `index_uuid` is the seek entry's index selector: `None` seeks the
    /// internal `row_uuid` identity sidecar; `Some(uuid)` seeks the keyed
    /// sidecar with that uuid in `index_sidecars`. Multi-entry intersection is
    /// the scan path's job, not this aggregate seek's.
    ///
    /// Returns `Ok(None)` when any in-scope segment has not materialized the
    /// selected sidecar; the caller then falls back to the merge pipeline. This
    /// is a hard-resolution fallback (the snapshot did not build the index),
    /// **never** a residency one — residency is handled entirely inside the
    /// kernel. `out_schema` / `full_decode_schema` are the
    /// `snapshot_read_schema`-shaped projected / full-decode schemas, exactly as
    /// [`DlDriver::scan_snapshot`].
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            segments = segments.len(),
            seek_tuples = probe_tuples.len(),
            rows = tracing::field::Empty,
        ),
    )]
    async fn seek_snapshot_point(
        &self,
        segments: &[SnapshotSegment],
        probe_tuples: &[Vec<String>],
        index_uuid: Option<&Uuid>,
        key_columns: &[String],
        full_decode_schema: &SchemaRef,
        out_schema: &SchemaRef,
    ) -> Result<Option<RecordBatch>, DlError> {
        // Bail to the merge-pipeline fallback BEFORE any seeking, so the
        // fallback pays no partial-seek cost — otherwise the sidecar GETs /
        // base decodes on the leading indexed segments are wasted work that
        // the full re-run repeats.
        if segments
            .iter()
            .any(|seg| selected_sidecar(seg, index_uuid).is_none())
        {
            return Ok(None);
        }
        let arity = probe_tuples.first().map_or(0, Vec::len);
        let Some(key_types) = key_column_types(key_columns, arity, full_decode_schema) else {
            return Ok(None);
        };
        let probe_refs: Vec<Vec<&str>> = probe_tuples
            .iter()
            .map(|tuple| tuple.iter().map(String::as_str).collect())
            .collect();
        let probes: Vec<&[&str]> = probe_refs.iter().map(Vec::as_slice).collect();
        let mut per_segment: Vec<RecordBatch> = Vec::with_capacity(segments.len());
        for segment in segments {
            // The pre-scan above guarantees a sidecar; bind defensively rather
            // than panicking if that ever stops holding.
            let Some(sidecar) = selected_sidecar(segment, index_uuid) else {
                return Ok(None);
            };
            let batch = read_seeked_snapshot_segment(
                &self.readers,
                &self.cache,
                segment,
                sidecar,
                &key_types,
                full_decode_schema,
                out_schema,
                &probes,
            )
            .await?;
            // The kernel can return the full-decode superset, the projected out
            // schema, or an empty full-schema batch, so every result needs the
            // same null-fill normalization the provider's partition stream does.
            per_segment.push(crate::provider::project_batch_to_schema(
                &batch, out_schema,
            )?);
        }
        // No segments ⇒ an empty out-schema batch, matching what
        // `stream_all_cold` yields.
        if per_segment.is_empty() {
            return Ok(Some(RecordBatch::new_empty(out_schema.clone())));
        }
        let merged = arrow::compute::concat_batches(out_schema, &per_segment)?;
        tracing::Span::current().record("rows", merged.num_rows());
        Ok(Some(merged))
    }

    fn derive_session(&self) -> SessionContext {
        derive_cold_session(&self.template)
    }

    #[tracing::instrument(
        skip_all,
        fields(
            sql_len = sql.len(),
            upsert_segments = plan.persist.as_ref().map_or(0, |p| p.upsert_segments.len()),
            delete_segments = plan.persist.as_ref().map_or(0, |p| p.delete_segments.len()),
        ),
    )]
    async fn execute_sql(
        &self,
        plan: &ColdStoragePlan,
        sql: &str,
        log_schemas: &LogSchemas,
    ) -> Result<RecordBatch, DlError> {
        let ctx = build_persist_session(
            &self.template,
            plan,
            self.readers.clone(),
            self.cache.clone(),
            log_schemas,
        )?;
        let batch = collect_single_batch(&ctx, sql).await?;
        tracing::debug!(rows = batch.num_rows(), "execute_sql complete");
        Ok(batch)
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            segments = segments.len(),
            exclusion = exclusion.len(),
            sql_len = sql.len(),
            segment_order = ?order,
            seek_entries = seeks.as_ref().map_or(0, |k| k.len()),
        ),
    )]
    async fn scan_snapshot(
        &self,
        segments: &[SnapshotSegment],
        full_schema: &SchemaRef,
        out_schema: &SchemaRef,
        exclusion: &[String],
        sql: &str,
        segment_read_concurrency: usize,
        order: SegmentOrder,
        seeks: Option<Arc<Vec<SeekSpec>>>,
    ) -> Result<SendableRecordBatchStream, DlError> {
        let ctx = tracing::trace_span!("ss_build_session").in_scope(|| {
            build_snapshot_session(
                &self.template,
                segments,
                self.readers.clone(),
                self.cache.clone(),
                full_schema.clone(),
                out_schema.clone(),
                exclusion,
                segment_read_concurrency,
                order,
                seeks,
            )
        })?;
        let df = ctx
            .sql(sql)
            .instrument(tracing::trace_span!("ss_ctx_sql"))
            .await?;
        let stream = df
            .execute_stream()
            .instrument(tracing::trace_span!("ss_execute_stream"))
            .await?;
        Ok(stream)
    }
}

async fn collect_single_batch(ctx: &SessionContext, sql: &str) -> Result<RecordBatch, DlError> {
    let df = ctx.sql(sql).await?;
    let output_schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;
    Ok(arrow::compute::concat_batches(&output_schema, &batches)?)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use penca_core::{Format, SnapshotSegment};
    use penca_format::reader::{FormatError, FormatReader};

    use super::*;
    use crate::cache::SegmentCache;

    /// Object-safety check: `&dyn DlDriver` must compile.
    #[allow(dead_code)]
    fn assert_object_safe(_: &dyn DlDriver) {}

    /// A `FormatReader` that counts `read_segment` calls, returning a fixed
    /// batch. Lets the cache tests assert how many times storage was actually hit.
    struct CountingFormatReader {
        batch: RecordBatch,
        reads: Arc<AtomicUsize>,
    }

    impl FormatReader for CountingFormatReader {
        /// Honors `projection` when it can satisfy it, and returns the full
        /// batch when it cannot.
        ///
        /// Honoring it at all is necessary: returning the batch verbatim
        /// regardless makes any test about *which* columns were requested
        /// vacuous — it would pass whether or not the caller widened its
        /// projection, which is exactly what
        /// `uncached_oversized_segment_honors_its_seq_ceiling` must distinguish
        /// (verified by defeating the widening and watching that test fail).
        ///
        /// Falling back is equally necessary: `scan_snapshot_schema_tolerance`
        /// deliberately projects a column this batch does NOT have, so the
        /// CALLER's null-filling tolerance is what's under test there. Erroring
        /// would move the failure into the double and hide the behavior.
        async fn read_segment(
            &self,
            _uri: &str,
            _offset: Option<i64>,
            _length: Option<i64>,
            _schema: &SchemaRef,
            projection: Option<&[&str]>,
        ) -> Result<RecordBatch, FormatError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let Some(cols) = projection else {
                return Ok(self.batch.clone());
            };
            let Ok(indices) = cols
                .iter()
                .map(|name| self.batch.schema().index_of(name))
                .collect::<Result<Vec<usize>, _>>()
            else {
                return Ok(self.batch.clone());
            };

            Ok(self.batch.project(&indices)?)
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]))
    }

    fn test_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1", "r2"])),
                Arc::new(Int32Array::from(vec![0, 1, 2])),
            ],
        )
        .unwrap()
    }

    fn segment(uuid: &str, size_bytes: i64) -> SnapshotSegment {
        SnapshotSegment {
            table_snapshot_segment_uuid: uuid.to_string(),
            format: Format::Parquet,
            size_bytes,
            ..Default::default()
        }
    }

    /// Driver whose single reader (Parquet) is a counting reader; returns the
    /// driver plus a handle on the read counter.
    fn driver_with(
        cache: Arc<SegmentCache>,
        batch: RecordBatch,
    ) -> (DatafusionDlDriver<CountingFormatReader>, Arc<AtomicUsize>) {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = CountingFormatReader {
            batch,
            reads: reads.clone(),
        };
        let mut readers = HashMap::new();
        readers.insert(Format::Parquet.as_wire_code(), reader);
        (
            DatafusionDlDriver::new(
                Arc::new(readers),
                cache,
                Arc::new(crate::build_cold_session_template()),
            ),
            reads,
        )
    }

    async fn read_seg(
        dl: &DatafusionDlDriver<CountingFormatReader>,
        seg: &SnapshotSegment,
        full: &SchemaRef,
        out: &SchemaRef,
    ) -> Result<RecordBatch, DlError> {
        read_cached_snapshot_segment(dl.readers.as_ref(), &dl.cache, seg, full, out).await
    }

    /// A `FormatReader` that routes `read_segment` by URI, so one reader serves
    /// both a base segment file and its index sidecar file.
    struct RoutingReader {
        by_uri: HashMap<String, RecordBatch>,
    }

    impl FormatReader for RoutingReader {
        async fn read_segment(
            &self,
            uri: &str,
            _offset: Option<i64>,
            _length: Option<i64>,
            _schema: &SchemaRef,
            _projection: Option<&[&str]>,
        ) -> Result<RecordBatch, FormatError> {
            Ok(self
                .by_uri
                .get(uri)
                .unwrap_or_else(|| panic!("unexpected read of {uri}"))
                .clone())
        }
    }

    fn routing_driver(
        cache: Arc<SegmentCache>,
        by_uri: HashMap<String, RecordBatch>,
    ) -> DatafusionDlDriver<RoutingReader> {
        let mut readers = HashMap::new();
        readers.insert(Format::Parquet.as_wire_code(), RoutingReader { by_uri });
        DatafusionDlDriver::new(
            Arc::new(readers),
            cache,
            Arc::new(crate::build_cold_session_template()),
        )
    }

    /// Build a base snapshot segment over `(keys, vals)` plus its canonical
    /// `row_uuid` index sidecar, registering both files in `by_uri`.
    fn indexed_segment(
        by_uri: &mut HashMap<String, RecordBatch>,
        schema: &SchemaRef,
        name: &str,
        keys: &[&str],
        vals: &[i32],
    ) -> SnapshotSegment {
        let base = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int32Array::from(vals.to_vec())),
            ],
        )
        .unwrap();
        let sidecar_batch =
            penca_format::index::build_segment_index(std::slice::from_ref(base.column(0))).unwrap();
        let base_uri = format!("s3://t/{name}.parquet");
        let side_uri = format!("s3://t/{name}.idx");
        by_uri.insert(base_uri.clone(), base);
        by_uri.insert(side_uri.clone(), sidecar_batch);
        SnapshotSegment {
            table_snapshot_segment_uuid: format!("seg-{name}"),
            uri: base_uri,
            format: Format::Parquet,
            length: keys.len() as i64,
            row_count: keys.len() as i64,
            size_bytes: 1024,
            row_uuid_index_sidecar: Some(IndexSidecar {
                object_uri: side_uri,
                offset: 0,
                length: keys.len() as i64,
                format: Format::Parquet,
                segment_index_uuid: format!("idx-{name}"),
                size_bytes: 256,
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn seek_snapshot_point_falls_back_when_sidecar_missing() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = test_schema();
        let (dl, reads) = driver_with(cache, test_batch(&schema));
        let seg = segment("seg-no-sidecar", 1024); // row_uuid_index_sidecar: None
        let res = dl
            .seek_snapshot_point(
                &[seg],
                &[vec!["r1".to_string()]],
                None,
                &[],
                &schema,
                &schema,
            )
            .await
            .unwrap();
        assert!(
            res.is_none(),
            "missing sidecar must signal fallback (Ok(None))"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "fallback must not read storage"
        );
    }

    #[tokio::test]
    async fn seek_snapshot_point_seeks_and_concats_across_segments() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = test_schema();
        let mut by_uri = HashMap::new();
        let seg_a = indexed_segment(&mut by_uri, &schema, "a", &["r0", "r1"], &[0, 1]);
        let seg_b = indexed_segment(&mut by_uri, &schema, "b", &["r2", "r3"], &[2, 3]);
        let dl = routing_driver(cache, by_uri);

        let res = dl
            .seek_snapshot_point(
                &[seg_a, seg_b],
                &[vec!["r1".to_string()], vec!["r3".to_string()]],
                None,
                &[],
                &schema,
                &schema,
            )
            .await
            .unwrap()
            .expect("all segments indexed => Some");

        let expected = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r1", "r3"])),
                Arc::new(Int32Array::from(vec![1, 3])),
            ],
        )
        .unwrap();
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn seek_snapshot_point_empty_segments_yields_empty_batch() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = test_schema();
        let dl = routing_driver(cache, HashMap::new());
        let res = dl
            .seek_snapshot_point(&[], &[vec!["r1".to_string()]], None, &[], &schema, &schema)
            .await
            .unwrap()
            .expect("no segments => Some(empty), not a fallback");
        assert_eq!(res.num_rows(), 0);
        assert_eq!(res.schema(), schema);
    }

    /// Here `out_schema` strictly projects `full_decode_schema`, dropping `v`,
    /// so this pins the full-decode → out-schema projection the aggregate owns.
    #[tokio::test]
    async fn seek_snapshot_point_zero_match_normalizes_to_out_schema() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let full_decode_schema = test_schema(); // (row_uuid, v)
        let out_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "row_uuid",
            DataType::Utf8,
            false,
        )]));
        let mut by_uri = HashMap::new();
        let seg = indexed_segment(
            &mut by_uri,
            &full_decode_schema,
            "z",
            &["r0", "r1"],
            &[0, 1],
        );
        let dl = routing_driver(cache, by_uri);

        let res = dl
            .seek_snapshot_point(
                &[seg],
                &[vec!["does-not-exist".to_string()]],
                None,
                &[],
                &full_decode_schema,
                &out_schema,
            )
            .await
            .unwrap()
            .expect("indexed segment => Some, even with zero matches");
        assert_eq!(res.num_rows(), 0);
        assert_eq!(res.schema(), out_schema, "result normalized to out_schema");
    }

    // The composite-key tests below assert against the residual-equivalent
    // selection: a `key0 = 's1' AND key1 = 't1'` scan returns the same rows.

    /// A 3-column base `(key0, key1, v)` segment plus its 2-column composite
    /// `(key0, key1)` index sidecar, both registered in `by_uri`.
    fn composite_indexed_segment(
        by_uri: &mut HashMap<String, RecordBatch>,
        schema: &SchemaRef,
        name: &str,
        key0s: &[&str],
        key1s: &[&str],
        vals: &[i32],
    ) -> SnapshotSegment {
        let base = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(key0s.to_vec())),
                Arc::new(StringArray::from(key1s.to_vec())),
                Arc::new(Int32Array::from(vals.to_vec())),
            ],
        )
        .unwrap();
        // Composite key = the leading two columns, in order.
        let sidecar_batch = penca_format::index::build_segment_index(&[
            Arc::clone(base.column(0)),
            Arc::clone(base.column(1)),
        ])
        .unwrap();
        let base_uri = format!("s3://t/{name}.parquet");
        let side_uri = format!("s3://t/{name}.idx");
        by_uri.insert(base_uri.clone(), base);
        by_uri.insert(side_uri.clone(), sidecar_batch);
        SnapshotSegment {
            table_snapshot_segment_uuid: format!("seg-{name}"),
            uri: base_uri,
            format: Format::Parquet,
            length: key0s.len() as i64,
            row_count: key0s.len() as i64,
            size_bytes: 1024,
            row_uuid_index_sidecar: Some(IndexSidecar {
                object_uri: side_uri,
                offset: 0,
                length: key0s.len() as i64,
                format: Format::Parquet,
                segment_index_uuid: format!("idx-{name}"),
                size_bytes: 256,
            }),
            ..Default::default()
        }
    }

    /// `(key0, key1, v)` base schema for the composite tests.
    fn composite_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Utf8, false),
            Field::new("key1", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]))
    }

    /// Pins the pure derivation only; end-to-end typed decode is covered by
    /// `integration_user_index_seek`.
    #[test]
    fn key_column_types_reads_native_types_from_schema() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ]));
        // Empty key columns = identity/name shape: all-Utf8, arity from probes.
        assert_eq!(
            key_column_types(&[], 2, &schema),
            Some(vec![DataType::Utf8, DataType::Utf8])
        );
        // A typed user index recovers Int64 straight from the schema.
        assert_eq!(
            key_column_types(&["score".to_string()], 1, &schema),
            Some(vec![DataType::Int64])
        );
        // Composite mixed types keep declared order.
        assert_eq!(
            key_column_types(&["name".to_string(), "score".to_string()], 2, &schema),
            Some(vec![DataType::Utf8, DataType::Int64])
        );
        // A key column absent from the schema → None (caller falls to merge).
        assert_eq!(key_column_types(&["ghost".to_string()], 1, &schema), None);
    }

    /// Collect the `(key0, key1, v)` rows of a result batch as comparable tuples.
    fn composite_rows(batch: &RecordBatch) -> BTreeSet<(String, String, i32)> {
        let k0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let k1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let v = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| (k0.value(i).to_string(), k1.value(i).to_string(), v.value(i)))
            .collect()
    }

    #[tokio::test]
    async fn seek_snapshot_point_composite_2col_hit() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = composite_schema();
        let mut by_uri = HashMap::new();
        // rows: (s2,t1,0), (s1,t2,1), (s1,t1,2).
        let seg = composite_indexed_segment(
            &mut by_uri,
            &schema,
            "c",
            &["s2", "s1", "s1"],
            &["t1", "t2", "t1"],
            &[0, 1, 2],
        );
        let dl = routing_driver(cache, by_uri);

        let probes = vec![vec!["s1".to_string(), "t1".to_string()]];
        let res = dl
            .seek_snapshot_point(&[seg], &probes, None, &[], &schema, &schema)
            .await
            .unwrap()
            .expect("indexed segment => Some");
        assert_eq!(
            composite_rows(&res),
            BTreeSet::from([("s1".to_string(), "t1".to_string(), 2)]),
            "(s1,t1) selects only its row, not (s1,t2) or (s2,t1)"
        );
    }

    #[tokio::test]
    async fn seek_snapshot_point_composite_in_list_unions() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = composite_schema();
        let mut by_uri = HashMap::new();
        let seg = composite_indexed_segment(
            &mut by_uri,
            &schema,
            "c",
            &["s2", "s1", "s1"],
            &["t1", "t2", "t1"],
            &[0, 1, 2],
        );
        let dl = routing_driver(cache, by_uri);

        let probes = vec![
            vec!["s1".to_string(), "t2".to_string()],
            vec!["s2".to_string(), "t1".to_string()],
        ];
        let res = dl
            .seek_snapshot_point(&[seg], &probes, None, &[], &schema, &schema)
            .await
            .unwrap()
            .expect("indexed segment => Some");
        assert_eq!(
            composite_rows(&res),
            BTreeSet::from([
                ("s1".to_string(), "t2".to_string(), 1),
                ("s2".to_string(), "t1".to_string(), 0),
            ]),
        );
    }

    #[tokio::test]
    async fn seek_snapshot_point_composite_multi_segment_concats() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = composite_schema();
        let mut by_uri = HashMap::new();
        let seg_a = composite_indexed_segment(
            &mut by_uri,
            &schema,
            "a",
            &["s1", "s1"],
            &["t1", "t2"],
            &[10, 11],
        );
        let seg_b = composite_indexed_segment(
            &mut by_uri,
            &schema,
            "b",
            &["s2", "s2"],
            &["t1", "t2"],
            &[20, 21],
        );
        let dl = routing_driver(cache, by_uri);

        let probes = vec![
            vec!["s1".to_string(), "t2".to_string()],
            vec!["s2".to_string(), "t1".to_string()],
        ];
        let res = dl
            .seek_snapshot_point(&[seg_a, seg_b], &probes, None, &[], &schema, &schema)
            .await
            .unwrap()
            .expect("indexed segments => Some");
        assert_eq!(
            composite_rows(&res),
            BTreeSet::from([
                ("s1".to_string(), "t2".to_string(), 11),
                ("s2".to_string(), "t1".to_string(), 20),
            ]),
            "matched rows from both segments concatenated",
        );
    }

    /// Some(empty), not a fallback — the sidecars exist, the key just misses.
    #[tokio::test]
    async fn seek_snapshot_point_composite_miss_returns_empty() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = composite_schema();
        let mut by_uri = HashMap::new();
        let seg = composite_indexed_segment(
            &mut by_uri,
            &schema,
            "c",
            &["s2", "s1", "s1"],
            &["t1", "t2", "t1"],
            &[0, 1, 2],
        );
        let dl = routing_driver(cache, by_uri);

        // key0 present, key1 present, but never PAIRED — a prefix match is no hit.
        let probes = vec![vec!["s1".to_string(), "t9".to_string()]];
        let res = dl
            .seek_snapshot_point(&[seg], &probes, None, &[], &schema, &schema)
            .await
            .unwrap()
            .expect("indexed segment => Some, even with zero matches");
        assert_eq!(res.num_rows(), 0);
    }

    #[tokio::test]
    async fn seek_snapshot_point_identity_arity1_preserved() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = test_schema(); // (row_uuid, v)
        let mut by_uri = HashMap::new();
        let seg = indexed_segment(&mut by_uri, &schema, "id", &["r0", "r1", "r2"], &[0, 1, 2]);
        let dl = routing_driver(cache, by_uri);

        let probes = vec![vec!["r1".to_string()]];
        let res = dl
            .seek_snapshot_point(&[seg], &probes, None, &[], &schema, &schema)
            .await
            .unwrap()
            .expect("indexed segment => Some");
        assert_eq!(res.num_rows(), 1);
        let k = res
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(k.value(0), "r1");
    }

    /// Guards the `probe_tuples.is_empty()` early return that also makes the
    /// later `probe_tuples[0]` arity index panic-free.
    #[tokio::test]
    async fn seek_snapshot_point_empty_probe_skips_sidecar_decode() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = composite_schema();
        let mut by_uri = HashMap::new();
        let seg = composite_indexed_segment(&mut by_uri, &schema, "c", &["s1"], &["t1"], &[0]);
        // Reader with NO files — any read panics, proving the empty-probe path
        // never touches the base segment or its sidecar.
        let dl = routing_driver(cache, HashMap::new());

        let probes: Vec<Vec<String>> = vec![];
        let res = dl
            .seek_snapshot_point(&[seg], &probes, None, &[], &schema, &schema)
            .await
            .unwrap()
            .expect("indexed segment => Some, even with an empty probe set");
        assert_eq!(res.num_rows(), 0);
    }

    /// The identity slot deliberately points at an unregistered URI — reading
    /// it would panic the routing reader — so a correct result proves the keyed
    /// sidecar served the seek.
    #[tokio::test]
    async fn seek_snapshot_point_some_index_uuid_seeks_keyed_sidecar() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = composite_schema();
        let mut by_uri = HashMap::new();
        // rows: (s2,t1,0), (s1,t2,1), (s1,t1,2) — composite name key (key0, key1).
        let mut seg = composite_indexed_segment(
            &mut by_uri,
            &schema,
            "n",
            &["s2", "s1", "s1"],
            &["t1", "t2", "t1"],
            &[0, 1, 2],
        );
        // The helper builds the composite sidecar into the identity slot; move
        // it into `index_sidecars` keyed by the index it serves, and leave the
        // identity slot pointing at a decoy URI.
        let name_index = Uuid::new_v4();
        let keyed_sidecar = seg
            .row_uuid_index_sidecar
            .take()
            .expect("helper built the identity sidecar");
        seg.index_sidecars = vec![(name_index.to_string(), keyed_sidecar)];
        seg.row_uuid_index_sidecar = Some(IndexSidecar {
            object_uri: "s3://t/never-read.idx".to_string(),
            offset: 0,
            length: 3,
            format: Format::Parquet,
            segment_index_uuid: "idx-identity-never-read".to_string(),
            size_bytes: 256,
        });
        let dl = routing_driver(cache, by_uri);

        let probes = vec![vec!["s1".to_string(), "t1".to_string()]];
        let res = dl
            .seek_snapshot_point(&[seg], &probes, Some(&name_index), &[], &schema, &schema)
            .await
            .unwrap()
            .expect("keyed-indexed segment => Some");
        assert_eq!(
            composite_rows(&res),
            BTreeSet::from([("s1".to_string(), "t1".to_string(), 2)]),
            "the (s1, t1) probe must seek the keyed sidecar and return its row"
        );
    }

    #[tokio::test]
    async fn seek_snapshot_point_missing_keyed_sidecar_falls_back() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = test_schema();
        let (dl, reads) = driver_with(cache, test_batch(&schema));
        let mut seg = segment("seg-identity-only", 1024); // index_sidecars empty
        seg.row_uuid_index_sidecar = Some(IndexSidecar {
            object_uri: "s3://t/identity.idx".to_string(),
            offset: 0,
            length: 1,
            format: Format::Parquet,
            segment_index_uuid: "idx-identity".to_string(),
            size_bytes: 256,
        });
        let name_index = Uuid::new_v4();
        let res = dl
            .seek_snapshot_point(
                &[seg],
                &[vec!["r1".to_string()]],
                Some(&name_index),
                &[],
                &schema,
                &schema,
            )
            .await
            .unwrap();
        assert!(
            res.is_none(),
            "missing NAME sidecar must signal fallback even when the identity sidecar exists"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "fallback must not read storage"
        );
    }

    /// A `Some(uuid)` naming an index absent from `index_sidecars` must fall
    /// back, never mis-seek some OTHER keyed sidecar that happens to be there.
    #[tokio::test]
    async fn seek_snapshot_point_unrecorded_index_uuid_falls_back() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = test_schema();
        let (dl, reads) = driver_with(cache, test_batch(&schema));
        let mut seg = segment("seg-other-keyed-index", 1024);
        seg.index_sidecars = vec![(
            Uuid::new_v4().to_string(), // some OTHER keyed index
            IndexSidecar {
                object_uri: "s3://t/other.idx".to_string(),
                offset: 0,
                length: 1,
                format: Format::Parquet,
                segment_index_uuid: "idx-other".to_string(),
                size_bytes: 256,
            },
        )];
        let requested = Uuid::new_v4(); // != the keyed index present
        let res = dl
            .seek_snapshot_point(
                &[seg],
                &[vec!["r1".to_string()]],
                Some(&requested),
                &[],
                &schema,
                &schema,
            )
            .await
            .unwrap();
        assert!(
            res.is_none(),
            "a requested index the sidecar does not record must fall back"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "fallback must not read storage"
        );
    }

    /// The production system-table shape: a segment carrying BOTH sidecars. The
    /// keyed sidecar deliberately points at an unregistered URI, so a
    /// keyed-sidecar preference sneaking into the `None` arm would panic the
    /// routing reader instead of silently mis-seeking.
    #[tokio::test]
    async fn seek_snapshot_point_none_seeks_identity_when_both_sidecars_present() {
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = test_schema();
        let mut by_uri = HashMap::new();
        let mut seg = indexed_segment(&mut by_uri, &schema, "both", &["r0", "r1"], &[0, 1]);
        seg.index_sidecars = vec![(
            Uuid::new_v4().to_string(),
            IndexSidecar {
                object_uri: "s3://t/keyed-never-read.idx".to_string(),
                offset: 0,
                length: 2,
                format: Format::Parquet,
                segment_index_uuid: "idx-keyed-never-read".to_string(),
                size_bytes: 256,
            },
        )];
        let dl = routing_driver(cache, by_uri);

        let res = dl
            .seek_snapshot_point(
                &[seg],
                &[vec!["r1".to_string()]],
                None,
                &[],
                &schema,
                &schema,
            )
            .await
            .unwrap()
            .expect("identity-indexed segment => Some");
        let expected = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r1"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        assert_eq!(res, expected);
    }

    // Discriminator: a sentinel `SessionConfig` value the default registry never
    // carries. A default-UDF `ptr_eq` can't tell a derived session from a fresh
    // one — DataFusion's defaults are process-global `OnceLock` singletons.
    #[tokio::test]
    async fn driver_and_providers_derive_from_injected_template() {
        const SENTINEL: usize = 4242;

        fn sentinel_template() -> SessionState {
            let mut template = crate::build_cold_session_template();
            template.config_mut().options_mut().execution.batch_size = SENTINEL;
            template
        }

        fn batch_size_of(ctx: &SessionContext) -> usize {
            ctx.state().config().options().execution.batch_size
        }

        let template = sentinel_template();
        let readers: Arc<HashMap<i32, CountingFormatReader>> = Arc::new(HashMap::new());
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let schema = test_schema();

        let dl =
            DatafusionDlDriver::new(readers.clone(), cache.clone(), Arc::new(template.clone()));
        assert_eq!(
            batch_size_of(&dl.derive_session()),
            SENTINEL,
            "DlDriver::derive_session must clone the injected template, not SessionContext::new()",
        );

        let log_schemas = crate::schema::LogSchemas {
            upsert: schema.clone(),
            delete: schema.clone(),
        };
        let plan = ColdStoragePlan {
            snapshot: None,
            persist: None,
        };
        let persist_ctx = crate::provider::build_persist_session(
            &template,
            &plan,
            readers.clone(),
            cache.clone(),
            &log_schemas,
        )
        .unwrap();
        assert_eq!(
            batch_size_of(&persist_ctx),
            SENTINEL,
            "build_persist_session must derive its session from the template",
        );

        let snapshot_ctx = crate::provider::build_snapshot_session(
            &template,
            &[],
            readers.clone(),
            cache.clone(),
            schema.clone(),
            schema.clone(),
            &[],
            4,
            SegmentOrder::ByCompletion,
            None,
        )
        .unwrap();
        assert_eq!(
            batch_size_of(&snapshot_ctx),
            SENTINEL,
            "build_snapshot_session must derive its session from the template",
        );
    }

    #[tokio::test]
    async fn second_read_of_same_segment_is_cache_hit_and_arc_shared() {
        let schema = test_schema();
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let (dl, reads) = driver_with(cache, test_batch(&schema));
        let seg = segment("seg-a", 128);

        let first = read_seg(&dl, &seg, &schema, &schema).await.unwrap();
        let second = read_seg(&dl, &seg, &schema, &schema).await.unwrap();

        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "second read served from cache, not storage"
        );
        assert_eq!(
            first.column(0).to_data().buffers()[0].as_ptr(),
            second.column(0).to_data().buffers()[0].as_ptr(),
            "cache hit is an Arc::clone — same backing buffer, no copy"
        );
    }

    #[tokio::test]
    async fn oversized_segment_not_cached_uses_uncached_read() {
        let schema = test_schema();
        let cache = Arc::new(SegmentCache::new(64));
        let (dl, reads) = driver_with(cache.clone(), test_batch(&schema));
        let seg = segment("big", 4096);

        read_seg(&dl, &seg, &schema, &schema).await.unwrap();
        read_seg(&dl, &seg, &schema, &schema).await.unwrap();

        assert_eq!(
            reads.load(Ordering::SeqCst),
            2,
            "oversized segment is never cached — both accesses re-read storage"
        );
        assert!(cache.get("big").is_none(), "oversized segment not stored");
    }

    #[tokio::test]
    async fn evicted_segment_re_reads_from_storage() {
        let schema = test_schema();
        let cache = Arc::new(SegmentCache::new(200));
        let (dl, reads) = driver_with(cache.clone(), test_batch(&schema));

        read_seg(&dl, &segment("a", 150), &schema, &schema)
            .await
            .unwrap(); // read 1, cache a
        read_seg(&dl, &segment("b", 150), &schema, &schema)
            .await
            .unwrap(); // read 2, cache b -> over the 200-byte budget
        cache.run_pending();

        // moka evicted one of {a,b} to honor the budget (we don't assert which
        // — that is moka's W-TinyLFU choice). Re-reading the evicted key must
        // hit storage again.
        let evicted = if cache.get("a").is_none() { "a" } else { "b" };
        read_seg(&dl, &segment(evicted, 150), &schema, &schema)
            .await
            .unwrap();
        assert_eq!(
            reads.load(Ordering::SeqCst),
            3,
            "evicted segment re-read from storage"
        );
    }

    // The RAW `ColdStorageClient::read_persist_segments` path — lifecycle
    // compaction / snapshot building / AuditData reads — stays cacheless by
    // design. Only the merge-on-read QUERY path caches, via the provider; see
    // `persist_query_read_is_cached_within_process`.
    #[tokio::test]
    async fn raw_read_persist_segments_is_uncached() {
        use futures::StreamExt;
        use penca_core::PersistSegment;
        use penca_storage_cold::ColdStorageClient;

        let schema = test_schema();
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = CountingFormatReader {
            batch: test_batch(&schema),
            reads: reads.clone(),
        };
        let mut readers = HashMap::new();
        readers.insert(Format::Parquet.as_wire_code(), reader);
        let seg = PersistSegment {
            segment_uuid: "p".into(),
            format: Format::Parquet,
            ..Default::default()
        };

        for _ in 0..2 {
            let mut stream = ColdStorageClient::read_persist_segments(
                &readers,
                std::slice::from_ref(&seg),
                &schema,
                None,
            );
            while let Some(b) = stream.next().await {
                b.unwrap();
            }
        }
        assert_eq!(
            reads.load(Ordering::SeqCst),
            2,
            "persist path never caches — both reads hit storage"
        );
    }

    #[tokio::test]
    async fn persist_query_read_is_cached_within_process() {
        use penca_core::{PersistPlan, PersistSegment};

        let schema = test_schema();
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let (dl, reads) = driver_with(cache, test_batch(&schema));

        let seg = PersistSegment {
            segment_uuid: "p".into(),
            format: Format::Parquet,
            size_bytes: 256, // nonzero -> admits-ible
            ..Default::default()
        };
        let plan = ColdStoragePlan {
            snapshot: None,
            persist: Some(PersistPlan {
                upsert_segments: vec![seg],
                ..Default::default()
            }),
        };
        let log_schemas = LogSchemas {
            upsert: schema.clone(),
            delete: schema.clone(),
        };

        let first = dl
            .execute_sql(&plan, "SELECT row_uuid, v FROM upsert_log", &log_schemas)
            .await
            .unwrap();
        let second = dl
            .execute_sql(&plan, "SELECT row_uuid, v FROM upsert_log", &log_schemas)
            .await
            .unwrap();

        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "second persist query must be served from cache (1 storage read, not 2)"
        );
        assert_eq!(
            first, second,
            "cached persist read returns identical content, not just the same row count"
        );
    }

    /// The ceiling must survive the UNCACHED read path. `read_cached_persist_segment`
    /// returns a batch already projected to the output schema when
    /// `cache.admits` is false, so a scan projecting `commit_seq_num` away would
    /// leave the bound unenforceable unless the read widens for it.
    ///
    /// Distinguishing: with the widening, the read asks for
    /// `(row_uuid, commit_seq_num)` and the ceiling drops the over-bound row;
    /// without it, the read asks for `row_uuid` alone and
    /// `apply_segment_seq_ceiling` errors rather than silently passing rows
    /// through. Either way the assertion below fails if the widening is removed.
    #[tokio::test]
    async fn uncached_oversized_segment_honors_its_seq_ceiling() {
        use penca_core::{PersistPlan, PersistSegment};

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("commit_seq_num", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1", "r2"])),
                Arc::new(arrow::array::Int64Array::from(vec![10_i64, 20, 30])),
            ],
        )
        .unwrap();
        // Budget below size_bytes so `admits` is false and the read takes the
        // uncached, already-projected path.
        let cache = Arc::new(SegmentCache::new(64));
        let (dl, _reads) = driver_with(cache, batch);

        let seg = PersistSegment {
            segment_uuid: "big-with-ceiling".into(),
            format: Format::Parquet,
            size_bytes: 4096,
            max_commit_seq_num: Some(20),
            ..Default::default()
        };
        let plan = ColdStoragePlan {
            snapshot: None,
            persist: Some(PersistPlan {
                upsert_segments: vec![seg],
                ..Default::default()
            }),
        };
        let log_schemas = LogSchemas {
            upsert: schema.clone(),
            delete: schema.clone(),
        };

        // Projection deliberately drops `commit_seq_num`.
        let out = dl
            .execute_sql(&plan, "SELECT row_uuid FROM upsert_log", &log_schemas)
            .await
            .expect("the ceiling must be enforceable on the uncached path");
        let rows = out.num_rows();
        assert_eq!(
            rows, 2,
            "ceiling 20 must drop the seq-30 row even though the projection omits \
             commit_seq_num; saw {rows} rows"
        );
    }

    #[tokio::test]
    async fn oversized_persist_segment_not_cached() {
        use penca_core::{PersistPlan, PersistSegment};

        let schema = test_schema();
        let cache = Arc::new(SegmentCache::new(64));
        let (dl, reads) = driver_with(cache, test_batch(&schema));

        let seg = PersistSegment {
            segment_uuid: "big".into(),
            format: Format::Parquet,
            size_bytes: 4096, // > 64-byte budget -> not admits-ible
            ..Default::default()
        };
        let plan = ColdStoragePlan {
            snapshot: None,
            persist: Some(PersistPlan {
                upsert_segments: vec![seg],
                ..Default::default()
            }),
        };
        let log_schemas = LogSchemas {
            upsert: schema.clone(),
            delete: schema.clone(),
        };

        dl.execute_sql(&plan, "SELECT row_uuid, v FROM upsert_log", &log_schemas)
            .await
            .unwrap();
        dl.execute_sql(&plan, "SELECT row_uuid, v FROM upsert_log", &log_schemas)
            .await
            .unwrap();

        assert_eq!(
            reads.load(Ordering::SeqCst),
            2,
            "oversized persist segment never cached — both queries re-read storage"
        );
    }

    // Projection-independence: one cached persist entry (decoded whole on the
    // miss) serves queries with DIFFERENT projections.
    #[tokio::test]
    async fn persist_cache_entry_serves_different_projections() {
        use penca_core::{PersistPlan, PersistSegment};

        let schema = test_schema(); // (row_uuid, v)
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let (dl, reads) = driver_with(cache, test_batch(&schema));

        let seg = PersistSegment {
            segment_uuid: "p".into(),
            format: Format::Parquet,
            size_bytes: 256,
            ..Default::default()
        };
        let plan = ColdStoragePlan {
            snapshot: None,
            persist: Some(PersistPlan {
                upsert_segments: vec![seg],
                ..Default::default()
            }),
        };
        let log_schemas = LogSchemas {
            upsert: schema.clone(),
            delete: schema.clone(),
        };

        // A miss that decodes the whole segment and caches the (row_uuid, v)
        // superset.
        let narrow = dl
            .execute_sql(&plan, "SELECT row_uuid FROM upsert_log", &log_schemas)
            .await
            .unwrap();
        let wide = dl
            .execute_sql(&plan, "SELECT v FROM upsert_log", &log_schemas)
            .await
            .unwrap();

        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "one decode-whole serves both projections (second is a cache hit)"
        );

        assert_eq!(narrow.num_columns(), 1);
        assert_eq!(narrow.schema().field(0).name(), "row_uuid");
        let row_uuids = narrow
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            row_uuids,
            &StringArray::from(vec!["r0", "r1", "r2"]),
            "narrow projection returns the row_uuid column from the cached superset"
        );

        assert_eq!(wide.num_columns(), 1);
        assert_eq!(wide.schema().field(0).name(), "v");
        let vs = wide
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            vs,
            &Int32Array::from(vec![0, 1, 2]),
            "different projection returns the v column from the same cached superset"
        );
    }

    // The driver returns the full superset; merge narrows downstream via
    // project_to_output.
    #[tokio::test]
    async fn one_entry_serves_different_projections() {
        let full_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
            Field::new("extra", DataType::Int32, false),
        ]));
        let full_batch = RecordBatch::try_new(
            full_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1"])),
                Arc::new(Int32Array::from(vec![0, 1])),
                Arc::new(Int32Array::from(vec![9, 9])),
            ],
        )
        .unwrap();
        let narrow_out: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let (dl, reads) = driver_with(cache, full_batch);
        let seg = segment("p", 256);

        let first = read_seg(&dl, &seg, &full_schema, &narrow_out)
            .await
            .unwrap();
        assert_eq!(
            first.num_columns(),
            3,
            "driver returns the full superset; merge narrows downstream"
        );
        let second = read_seg(&dl, &seg, &full_schema, &full_schema)
            .await
            .unwrap();
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "one decode serves both projections — no re-read"
        );
        assert_eq!(second.num_columns(), 3);
    }

    // penca-dl cannot depend on penca-merge, so the scan_snapshot tests below
    // build the snapshot-scan SQL inline — the shape
    // `penca_merge::sql::build_cold_snapshot_scan` emits.

    fn rd_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn rd_batch() -> RecordBatch {
        RecordBatch::try_new(
            rd_schema(),
            vec![
                Arc::new(StringArray::from(vec!["r1", "r2"])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(Int32Array::from(vec![1, 9])),
            ],
        )
        .unwrap()
    }

    async fn collect_scan_uuids(stream: SendableRecordBatchStream) -> Vec<String> {
        use arrow::array::Array;
        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let idx = b.schema().index_of("row_uuid").unwrap();
            let col = b
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.push(col.value(i).to_string());
            }
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn scan_snapshot_applies_exclusion_and_residual() {
        let schema = rd_schema();
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let (dl, _reads) = driver_with(cache, rd_batch());
        let seg = segment("seg1", 4096);
        let sql = "SELECT l.row_uuid, l.\"name\", l.\"value\" FROM l \
                   WHERE l.row_uuid NOT IN (SELECT row_uuid FROM exclusion) \
                   AND (l.value > 0)";
        let stream = dl
            .scan_snapshot(
                std::slice::from_ref(&seg),
                &schema,
                &schema,
                &["r1".to_string()],
                sql,
                4,
                SegmentOrder::ByCompletion,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            collect_scan_uuids(stream).await,
            vec!["r2".to_string()],
            "r1 excluded by anti-join; r2 passes value>0",
        );
    }

    #[tokio::test]
    async fn scan_snapshot_empty_exclusion_keeps_all() {
        let schema = rd_schema();
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let (dl, _reads) = driver_with(cache, rd_batch());
        let seg = segment("seg1", 4096);
        let sql = "SELECT l.row_uuid, l.\"name\", l.\"value\" FROM l \
                   WHERE l.row_uuid NOT IN (SELECT row_uuid FROM exclusion)";
        let stream = dl
            .scan_snapshot(
                std::slice::from_ref(&seg),
                &schema,
                &schema,
                &[],
                sql,
                4,
                SegmentOrder::ByCompletion,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            collect_scan_uuids(stream).await,
            vec!["r1".to_string(), "r2".to_string()],
            "empty exclusion keeps all rows (anti-join over an empty set)",
        );
    }

    #[tokio::test]
    async fn scan_snapshot_schema_tolerance() {
        // Segment decoded against the OLDER narrow schema {row_uuid, name};
        // out schema carries a nullable `value` to null-fill.
        let narrow_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let narrow = RecordBatch::try_new(
            narrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["r1", "r2"])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();
        let out_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]));
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let (dl, _reads) = driver_with(cache, narrow);
        let seg = segment("seg1", 4096);
        let sql = "SELECT l.row_uuid FROM l \
                   WHERE l.row_uuid NOT IN (SELECT row_uuid FROM exclusion) \
                   AND (l.value IS NULL)";
        let stream = dl
            .scan_snapshot(
                std::slice::from_ref(&seg),
                &out_schema,
                &out_schema,
                &[],
                sql,
                4,
                SegmentOrder::ByCompletion,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            collect_scan_uuids(stream).await,
            vec!["r1".to_string(), "r2".to_string()],
            "null-filled `value` matches IS NULL through scan_snapshot",
        );
    }

    #[test]
    fn intersect_sorted_edges() {
        assert_eq!(intersect_sorted(&[1, 3, 5], &[3, 4, 5]), vec![3, 5]);
        assert_eq!(intersect_sorted(&[1, 2], &[3, 4]), Vec::<i64>::new());
        assert_eq!(intersect_sorted(&[], &[1]), Vec::<i64>::new());
        assert_eq!(intersect_sorted(&[2, 7, 9], &[2, 7, 9]), vec![2, 7, 9]);
        assert_eq!(intersect_sorted(&[5], &[1, 5, 6]), vec![5]);
    }
}
