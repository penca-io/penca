//! DataFusion `TableProvider` for cold-storage persist segments.
//!
//! Registered into a per-query [`SessionContext`] under the well-known
//! log table names (`upsert_log`, `delete_log`) so the shared
//! merge-on-read SQL builder can run the same query against either tier
//! by swapping only the dialect.
//!
//! CHA-218: commit_tx_log is hot-only — cold has no commit_tx_log table. Per-row tx
//! metadata is carried inline on each upsert/delete cold segment row.
//!
//! The provider reads segments through a [`FormatReader`] and pushes
//! column projection into the reader. Filter pushdown stays
//! `Unsupported` for the persist tier: per ADR 0022 persist segments
//! are NOT pruned by user filter to preserve CHA-142's exclusion-set
//! invariant. Persist `TableProvider::statistics()` does expose a
//! `Precision::Inexact` aggregate (row counts, per-column min/max)
//! for DataFusion's CBO cardinality estimation (CHA-82).

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::new_null_array;
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::{StreamExt, TryStreamExt};
use penca_core::{ColdStoragePlan, IndexSidecar, PersistSegment, SnapshotSegment};
use penca_format::reader::FormatReader;

use crate::cache::SegmentCache;
use crate::driver::SegmentOrder;
use crate::driver::{
    SeekSpec, read_cached_persist_segment, read_cached_snapshot_segment,
    read_intersect_seeked_snapshot_segment,
};
use crate::schema::{
    DELETE_LOG_TABLE, EXCLUSION_TABLE, LogSchemas, SNAPSHOT_TABLE, UPSERT_LOG_TABLE,
};
use crate::session_template::{derive_cold_session, derive_cold_session_single_partition};

/// `TableProvider` that exposes a list of cold-storage persist segments
/// as a queryable table.
///
/// Projection is pushed down into the [`FormatReader`]; filter pushdown
/// stays `Unsupported` (see ADR 0022 — persist segments are not pruned
/// by user filter). The scan advertises `output_ordering = [commit_seq_num
/// ASC, write_seq_num ASC]` (CHA-410 / CHA-431) — the write side sorts each
/// segment by the composite `(commit_seq_num, write_seq_num)` and the plan lists
/// segments in `commit_seq_num` order, so the single concatenated stream is
/// genuinely ordered on the total version order. That advertised order
/// survives to a downstream operator only on an un-repartitioned plan
/// (single output partition); an order-aware consumer must keep the
/// persist scan from being repartitioned (cf. the snapshot `ByPlan`
/// `target_partitions=1` pin in `derive_cold_session_single_partition`).
/// No consumer requires this ordering yet — it is latent today. Kept
/// `pub(crate)` because external callers go through
/// [`build_persist_session`] — the provider itself is an implementation
/// detail of penca-dl's DataFusion backend.
pub(crate) struct PersistTableProvider<R: FormatReader + 'static> {
    segments: Arc<Vec<PersistSegment>>,
    readers: Arc<HashMap<i32, R>>,
    /// Process-lifetime decoded-segment cache shared with the snapshot tier
    /// (CHA-474). A persist segment file is immutable once written and keyed by
    /// its globally-unique `segment_uuid`, so it shares the one byte budget with
    /// no TTL — W-TinyLFU eviction is the whole mechanism, as for snapshot.
    cache: Arc<SegmentCache>,
    schema: SchemaRef,
    // CHA-82: parsed once at construction; used by `statistics()` to
    // fold a Precision::Inexact table-level summary for DataFusion's
    // CBO cardinality estimation. Persist segments are NOT pruned by
    // user filter (ADR 0022), but the aggregate is consumed.
    parsed_stats: Vec<crate::stats::ParsedSegmentStats>,
}

impl<R: FormatReader + 'static> PersistTableProvider<R> {
    pub(crate) fn new(
        segments: Vec<PersistSegment>,
        readers: Arc<HashMap<i32, R>>,
        schema: SchemaRef,
        cache: Arc<SegmentCache>,
    ) -> Self {
        let parsed_stats = segments
            .iter()
            .map(|s| crate::stats::parse_segment_statistics(&s.statistics, &schema))
            .collect();
        Self {
            segments: Arc::new(segments),
            readers,
            cache,
            schema,
            parsed_stats,
        }
    }
}

impl<R: FormatReader + 'static> std::fmt::Debug for PersistTableProvider<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistTableProvider")
            .field("segments", &self.segments.len())
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl<R: FormatReader + 'static> TableProvider for PersistTableProvider<R> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let output_schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };

        // CHA-410 / CHA-431: advertise the persist total version order
        // `(commit_seq_num, write_seq_num)`. Write-side (`chunk_persist_batch` sorts
        // each segment by the composite) + plan-side segment-list ordering make
        // the single concatenated stream globally non-decreasing in the
        // composite, so the optimizer can elide a redundant `SortExec` and pick
        // order-aware operators. NULLS LAST matches DataFusion's `ORDER BY`
        // default; honest because both columns are non-nullable. `write_seq_num`
        // is appended only when BOTH columns are emitted — a projection that
        // drops `write_seq_num` falls back to the honest `[commit_seq_num]` prefix,
        // and dropping `commit_seq_num` drops the ordering entirely (we can't claim
        // an order over a column we don't emit). Planner metadata only — never
        // an execution seek (CHA-454).
        let asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let output_ordering = col("commit_seq_num", &output_schema)
            .ok()
            .and_then(|tx_expr| {
                let mut exprs = vec![PhysicalSortExpr::new(tx_expr, asc)];
                if let Ok(write_expr) = col("write_seq_num", &output_schema) {
                    exprs.push(PhysicalSortExpr::new(write_expr, asc));
                }

                LexOrdering::new(exprs)
            });

        let partition = PersistPartitionStream {
            segments: self.segments.clone(),
            readers: self.readers.clone(),
            cache: self.cache.clone(),
            full_schema: self.schema.clone(),
            output_schema: output_schema.clone(),
        };

        Ok(Arc::new(StreamingTableExec::try_new(
            output_schema,
            vec![Arc::new(partition)],
            None,
            output_ordering,
            false,
            None,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Persist segments aren't pruned by user filter —
        // see docs/decisions/0022-no-persist-segment-pruning.md.
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    fn statistics(&self) -> Option<datafusion::common::stats::Statistics> {
        Some(crate::stats::aggregate_table_statistics(
            &self.parsed_stats,
            &self.schema,
        ))
    }
}

struct PersistPartitionStream<R: FormatReader + 'static> {
    segments: Arc<Vec<PersistSegment>>,
    readers: Arc<HashMap<i32, R>>,
    cache: Arc<SegmentCache>,
    full_schema: SchemaRef,
    output_schema: SchemaRef,
}

impl<R: FormatReader + 'static> std::fmt::Debug for PersistPartitionStream<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistPartitionStream")
            .field("segments", &self.segments.len())
            .finish()
    }
}

impl<R: FormatReader + 'static> PartitionStream for PersistPartitionStream<R> {
    fn schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::execution::SendableRecordBatchStream {
        let segments = self.segments.clone();
        let readers = self.readers.clone();
        let cache = self.cache.clone();
        let full_schema = self.full_schema.clone();
        let output_schema = self.output_schema.clone();

        let stream = async_stream::try_stream! {
            // CHA-474: cache-aware per-segment reads through the shared
            // SegmentCache. A miss decodes the whole segment (so one
            // cached entry serves any projection) and admits it; a hit is an
            // Arc::clone. Each segment is then normalized to `output_schema`.
            // Sequential, preserving the plan order the advertised
            // (commit_seq_num, write_seq_num) ordering relies on — a reorder
            // would violate CHA-410/CHA-431. Empty batches are dropped; the
            // StreamingTableExec carries `output_schema` for an empty scan.
            for segment in segments.iter() {
                let batch = read_cached_persist_segment(
                    readers.as_ref(),
                    cache.as_ref(),
                    segment,
                    &full_schema,
                    &output_schema,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let projected = project_batch_to_schema(&batch, &output_schema)
                    .map_err(DataFusionError::from)?;
                if projected.num_rows() > 0 {
                    yield projected;
                }
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        ))
    }
}

/// Build a [`SessionContext`] with the two log tables (`upsert_log`,
/// `delete_log`) registered under their well-known names.
///
/// Empty segment lists are still registered — the merge-on-read SQL is
/// dialect-agnostic and expects every table to exist even when a tier
/// has no unmerged log data for it.
pub(crate) fn build_persist_session<R: FormatReader + 'static>(
    template: &SessionState,
    plan: &ColdStoragePlan,
    readers: Arc<HashMap<i32, R>>,
    cache: Arc<SegmentCache>,
    schemas: &LogSchemas,
) -> Result<SessionContext> {
    // CHA-421: derive a per-query context from the process template (microsecond
    // clone of the registry + rules, fresh isolated catalog) instead of paying
    // the ~1.4 ms `SessionContext::new()` registration per cold read.
    let ctx = derive_cold_session(template);

    let (upsert_segs, delete_segs) = match &plan.persist {
        Some(persist) => (
            persist.upsert_segments.clone(),
            persist.delete_segments.clone(),
        ),
        None => (vec![], vec![]),
    };

    register_persist(
        &ctx,
        UPSERT_LOG_TABLE,
        upsert_segs,
        readers.clone(),
        cache.clone(),
        schemas.upsert.clone(),
    )?;
    register_persist(
        &ctx,
        DELETE_LOG_TABLE,
        delete_segs,
        readers,
        cache,
        schemas.delete.clone(),
    )?;

    Ok(ctx)
}

fn register_persist<R: FormatReader + 'static>(
    ctx: &SessionContext,
    name: &str,
    segments: Vec<PersistSegment>,
    readers: Arc<HashMap<i32, R>>,
    cache: Arc<SegmentCache>,
    schema: SchemaRef,
) -> Result<()> {
    let provider = PersistTableProvider::new(segments, readers, schema, cache);
    ctx.register_table(name, Arc::new(provider))?;
    Ok(())
}

/// `TableProvider` exposing pruned cold-storage snapshot segments as a
/// queryable table (CHA-411). Mirrors [`PersistTableProvider`] but reads
/// through the process-lifetime [`SegmentCache`] with bounded
/// concurrency and null-fills each batch to the declared output schema
/// (CHA-252) before yielding.
///
/// Pruning is done **externally** in `penca_merge::stream_merged` (snapshot-tier
/// only, ADR 0022); the surviving segments are handed to [`Self::new`]. The
/// provider advertises no `PruningStatistics` and pushes no filter into the
/// reader (`supports_filters_pushdown` → `Unsupported`, ADR 0023). No
/// `output_ordering` is advertised — that is CHA-459.
pub(crate) struct SnapshotTableProvider<R: FormatReader + 'static> {
    segments: Arc<Vec<SnapshotSegment>>,
    readers: Arc<HashMap<i32, R>>,
    cache: Arc<SegmentCache>,
    /// Unprojected schema the cache decodes against, so one cached entry serves
    /// any projection (CHA-252).
    full_decode_schema: SchemaRef,
    /// Declared/projected output schema; each read batch is null-filled to this.
    out_schema: SchemaRef,
    segment_read_concurrency: usize,
    order: SegmentOrder,
    /// CHA-454/CHA-485 seek entries. `None` ⇒ the scan streams every segment
    /// (the CHA-411 path); `Some` ⇒ per segment, each entry resolving to a
    /// sidecar (identity → `row_uuid_index_sidecar`, keyed → `index_sidecars`)
    /// is seeked and the offsets INTERSECT before the decode; a segment
    /// resolving none falls back to the full scan. Set via
    /// [`Self::with_seeks`]; defaults to `None`.
    seeks: Option<Arc<Vec<SeekSpec>>>,
}

impl<R: FormatReader + 'static> SnapshotTableProvider<R> {
    pub(crate) fn new(
        segments: Vec<SnapshotSegment>,
        readers: Arc<HashMap<i32, R>>,
        cache: Arc<SegmentCache>,
        full_decode_schema: SchemaRef,
        out_schema: SchemaRef,
        segment_read_concurrency: usize,
        order: SegmentOrder,
    ) -> Self {
        Self {
            segments: Arc::new(segments),
            readers,
            cache,
            full_decode_schema,
            out_schema,
            segment_read_concurrency,
            order,
            seeks: None,
        }
    }

    /// Attach seek entries (CHA-454 identity / CHA-485 covering-index).
    /// Chainable at the construction site (`build_snapshot_session`); a
    /// `None` argument leaves the full-scan path.
    pub(crate) fn with_seeks(mut self, seeks: Option<Arc<Vec<SeekSpec>>>) -> Self {
        self.seeks = seeks;
        self
    }
}

impl<R: FormatReader + 'static> std::fmt::Debug for SnapshotTableProvider<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotTableProvider")
            .field("segments", &self.segments.len())
            .field("out_schema", &self.out_schema)
            .finish()
    }
}

#[async_trait]
impl<R: FormatReader + 'static> TableProvider for SnapshotTableProvider<R> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.out_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // DataFusion requests (in `projection`) every column the query
        // references — the output columns plus any the residual `FilterExec`
        // needs — so the partition null-fills each batch to exactly this
        // projected schema (CHA-252). No filter is pushed into the read
        // (ADR 0023); no ordering is advertised (CHA-459).
        let output_schema = match projection {
            Some(indices) => Arc::new(self.out_schema.project(indices)?),
            None => self.out_schema.clone(),
        };

        let partition = SnapshotPartitionStream {
            segments: self.segments.clone(),
            readers: self.readers.clone(),
            cache: self.cache.clone(),
            full_decode_schema: self.full_decode_schema.clone(),
            output_schema: output_schema.clone(),
            segment_read_concurrency: self.segment_read_concurrency.max(1),
            order: self.order,
            seeks: self.seeks.clone(),
        };

        Ok(Arc::new(StreamingTableExec::try_new(
            output_schema,
            vec![Arc::new(partition)],
            None,
            vec![],
            false,
            None,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // ADR 0022/0023: snapshot pruning is external (stream_merged); the user
        // filter is a DataFusion `FilterExec`, never pushed into the reader.
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }
}

/// Resolve one index (`index_uuid`) to a segment's sidecar — the single source
/// of truth for the identity-vs-keyed selection, shared by the snapshot-scan
/// seek (`resolve_seek_entries`) and the DataFusion-free aggregate seek
/// (`driver::seek_snapshot_point` via `selected_sidecar`). `None` → the
/// dedicated `row_uuid_index_sidecar` identity slot; `Some(uuid)` → the keyed
/// `index_sidecars` (user secondary indexes + the built-in system name index).
/// Unresolved (`None`) ⇒ the caller full-scans; over-selection is safe — the
/// residual `FilterExec` re-applies the exact predicate (ADR 0023).
pub(crate) fn sidecar_for_index<'s>(
    segment: &'s SnapshotSegment,
    index_uuid: Option<&str>,
) -> Option<&'s IndexSidecar> {
    match index_uuid {
        None => segment.row_uuid_index_sidecar.as_ref(),
        Some(uuid) => segment
            .index_sidecars
            .iter()
            .find(|(id, _)| id == uuid)
            .map(|(_, sidecar)| sidecar),
    }
}

/// CHA-454/CHA-485: pair each seek entry with the sidecar it resolves to on one
/// segment (via [`sidecar_for_index`]). Entries that don't resolve are dropped
/// (the caller full-scans when NONE resolve).
fn resolve_seek_entries<'seg, 'spec>(
    segment: &'seg SnapshotSegment,
    seeks: Option<&'spec [SeekSpec]>,
) -> Vec<(&'seg IndexSidecar, &'spec SeekSpec)> {
    seeks
        .map(|entries| {
            entries
                .iter()
                .filter_map(|spec| {
                    sidecar_for_index(segment, spec.index_uuid.as_deref()).map(|sc| (sc, spec))
                })
                .collect()
        })
        .unwrap_or_default()
}

struct SnapshotPartitionStream<R: FormatReader + 'static> {
    segments: Arc<Vec<SnapshotSegment>>,
    readers: Arc<HashMap<i32, R>>,
    cache: Arc<SegmentCache>,
    full_decode_schema: SchemaRef,
    output_schema: SchemaRef,
    segment_read_concurrency: usize,
    order: SegmentOrder,
    seeks: Option<Arc<Vec<SeekSpec>>>,
}

impl<R: FormatReader + 'static> std::fmt::Debug for SnapshotPartitionStream<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotPartitionStream")
            .field("segments", &self.segments.len())
            .field("output_schema", &self.output_schema)
            .finish()
    }
}

impl<R: FormatReader + 'static> PartitionStream for SnapshotPartitionStream<R> {
    fn schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::execution::SendableRecordBatchStream {
        let segments = self.segments.clone();
        let readers = self.readers.clone();
        let cache = self.cache.clone();
        let full_decode_schema = self.full_decode_schema.clone();
        let output_schema = self.output_schema.clone();
        let concurrency = self.segment_read_concurrency;
        let seeks = self.seeks.clone();

        let order = self.order;
        let stream = async_stream::try_stream! {
            // Bounded-concurrency cache-aware reads. ByCompletion preserves
            // the pre-CHA-411 `buffer_unordered(segment_read_concurrency)`
            // from stream_merged Phase 3 — segment order not observable to
            // queries (CHA-459). ByPlan (CHA-404 snapshot writer) keeps the
            // planned segment order with `buffered` readahead, same
            // concurrency cap. Each segment is null-filled to
            // `output_schema` before yielding (CHA-252).
            let segs: Vec<SnapshotSegment> = segments.iter().cloned().collect();
            let read_futures = futures::stream::iter(segs)
                .map(|segment| {
                    let readers = readers.clone();
                    let cache = cache.clone();
                    let full = full_decode_schema.clone();
                    let out = output_schema.clone();
                    let seeks = seeks.clone();
                    async move {
                        // CHA-454/CHA-485: seek + intersect when at least one
                        // entry resolves, else stream the whole segment (the
                        // CHA-411 full-scan path).
                        let resolved =
                            resolve_seek_entries(&segment, seeks.as_deref().map(Vec::as_slice));
                        let batch = if resolved.is_empty() {
                            read_cached_snapshot_segment(
                                readers.as_ref(),
                                cache.as_ref(),
                                &segment,
                                &full,
                                &out,
                            )
                            .await
                        } else {
                            read_intersect_seeked_snapshot_segment(
                                readers.as_ref(),
                                cache.as_ref(),
                                &segment,
                                &resolved,
                                &full,
                                &out,
                            )
                            .await
                        }
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        project_batch_to_schema(&batch, &out).map_err(DataFusionError::from)
                    }
                });
            let mut reads: std::pin::Pin<Box<dyn futures::Stream<Item = Result<RecordBatch, DataFusionError>> + Send>> =
                match order {
                    SegmentOrder::ByPlan => Box::pin(read_futures.buffered(concurrency)),
                    SegmentOrder::ByCompletion => Box::pin(read_futures.buffer_unordered(concurrency)),
                };

            while let Some(batch) = reads.try_next().await? {
                if batch.num_rows() > 0 {
                    yield batch;
                }
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        ))
    }
}

/// Adapt `batch` to `out_schema`'s columns in order, null-filling any column
/// in `out_schema` the decoded segment lacks (CHA-252: a cache entry decoded
/// against an older table schema, before an `ALTER TABLE ADD COLUMN`). A
/// non-nullable missing column is a hard error. Mirrors the schema-tolerant
/// `penca_merge::output::project_to_output` (which adapts the resolved batch
/// for the Phase-1 emit); the two are kept separate by error type — `ArrowError`
/// here (inside a DataFusion partition stream) vs `MergeError` there.
pub(crate) fn project_batch_to_schema(
    batch: &RecordBatch,
    out_schema: &SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    let columns = out_schema
        .fields()
        .iter()
        .map(|field| match batch.schema().index_of(field.name()) {
            Ok(idx) => Ok(batch.column(idx).clone()),
            Err(_) if field.is_nullable() => {
                Ok(new_null_array(field.data_type(), batch.num_rows()))
            }
            Err(_) => Err(ArrowError::SchemaError(format!(
                "cold segment batch missing non-nullable column `{}`",
                field.name()
            ))),
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    RecordBatch::try_new(out_schema.clone(), columns)
}

/// Build a per-query [`SessionContext`] for the CHA-411 snapshot scan: the
/// [`SnapshotTableProvider`] registered under `l`, plus a single-column
/// `row_uuid` exclusion `MemTable` under `exclusion`. `segments` are the
/// already-pruned survivors (snapshot-tier pruning is upstream, ADR 0022). The
/// caller's SQL (`penca_merge::sql::build_cold_snapshot_scan`) joins the two.
//
// CHA-421's `template` param crosses clippy's 7-arg default. The eight inputs
// are all distinct snapshot-session-construction inputs with no clean data
// clump to bundle (the two schemas serve different roles — `full_decode_schema`
// feeds the segment cache, `out_schema` the projected output); a
// parameter-object refactor is out of scope for this perf change.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_snapshot_session<R: FormatReader + 'static>(
    template: &SessionState,
    segments: &[SnapshotSegment],
    readers: Arc<HashMap<i32, R>>,
    cache: Arc<SegmentCache>,
    full_decode_schema: SchemaRef,
    out_schema: SchemaRef,
    exclusion: &[String],
    segment_read_concurrency: usize,
    order: SegmentOrder,
    seeks: Option<Arc<Vec<SeekSpec>>>,
) -> Result<SessionContext> {
    // CHA-421: derive from the process template (see build_persist_session).
    // ByPlan pins target_partitions = 1 so the physical optimizer never
    // inserts a RepartitionExec above the provider (order-destroying).
    let ctx = match order {
        SegmentOrder::ByPlan => derive_cold_session_single_partition(template),
        SegmentOrder::ByCompletion => derive_cold_session(template),
    };

    let provider = SnapshotTableProvider::new(
        segments.to_vec(),
        readers,
        cache,
        full_decode_schema,
        out_schema,
        segment_read_concurrency,
        order,
    )
    .with_seeks(seeks);
    ctx.register_table(SNAPSHOT_TABLE, Arc::new(provider))?;

    let exclusion_schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("row_uuid", arrow::datatypes::DataType::Utf8, false),
    ]));
    let exclusion_refs: Vec<&str> = exclusion.iter().map(String::as_str).collect();
    let exclusion_batch = RecordBatch::try_new(
        exclusion_schema.clone(),
        vec![Arc::new(arrow::array::StringArray::from(exclusion_refs))],
    )?;
    let exclusion_table =
        datafusion::datasource::MemTable::try_new(exclusion_schema, vec![vec![exclusion_batch]])?;
    ctx.register_table(EXCLUSION_TABLE, Arc::new(exclusion_table))?;

    Ok(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use penca_core::{Format, PersistPlan};
    use penca_format::reader::{AnyFormatReader, FormatError};

    fn user_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("tx_uuid", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn empty_plan() -> ColdStoragePlan {
        ColdStoragePlan {
            snapshot: None,
            persist: Some(PersistPlan {
                upsert_segments: vec![],
                delete_segments: vec![],
                committed_at: None,
                commit_seq: None,
            }),
        }
    }

    #[tokio::test]
    async fn session_registers_both_log_tables() {
        let readers: Arc<HashMap<i32, AnyFormatReader>> = Arc::new(HashMap::new());
        let schema = user_schema();
        let schemas = LogSchemas {
            upsert: schema.clone(),
            delete: schema.clone(),
        };

        let ctx = build_persist_session(
            &crate::build_cold_session_template(),
            &empty_plan(),
            readers,
            Arc::new(SegmentCache::new(1 << 20)),
            &schemas,
        )
        .unwrap();

        for table in [UPSERT_LOG_TABLE, DELETE_LOG_TABLE] {
            assert!(
                ctx.table_exist(table).unwrap(),
                "expected {table} to be registered",
            );
        }
    }

    #[tokio::test]
    async fn empty_segments_scan_yields_zero_rows() {
        let readers: Arc<HashMap<i32, AnyFormatReader>> = Arc::new(HashMap::new());
        let schema = user_schema();
        let schemas = LogSchemas {
            upsert: schema.clone(),
            delete: schema.clone(),
        };

        let ctx = build_persist_session(
            &crate::build_cold_session_template(),
            &empty_plan(),
            readers,
            Arc::new(SegmentCache::new(1 << 20)),
            &schemas,
        )
        .unwrap();
        let batches = ctx
            .sql("SELECT row_uuid FROM upsert_log")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn projection_narrows_output_schema() {
        let readers: Arc<HashMap<i32, AnyFormatReader>> = Arc::new(HashMap::new());
        let provider = PersistTableProvider::new(
            vec![],
            readers,
            user_schema(),
            Arc::new(SegmentCache::new(1 << 20)),
        );

        assert_eq!(provider.schema().fields().len(), 3);

        let ctx = SessionContext::new();
        ctx.register_table("upsert_log", Arc::new(provider))
            .unwrap();

        let df = ctx.sql("SELECT name FROM upsert_log").await.unwrap();
        let output_schema = df.schema();
        assert_eq!(output_schema.fields().len(), 1);
        assert_eq!(output_schema.field(0).name(), "name");
    }

    // CHA-82 R6: persist provider statistics aggregate

    fn r6_fixture_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]))
    }

    fn make_persist_segment_with_amount_stats(
        uuid: &str,
        num_rows: usize,
        amount_min: i64,
        amount_max: i64,
    ) -> PersistSegment {
        use arrow::array::{ArrayRef, Int64Array};
        let row_uuids: Vec<String> = (0..num_rows).map(|i| format!("u-{uuid}-{i}")).collect();
        let row_uuid_refs: Vec<&str> = row_uuids.iter().map(|s| s.as_str()).collect();
        let amounts: Vec<i64> = if num_rows == 0 {
            vec![]
        } else if num_rows == 1 {
            vec![amount_min]
        } else {
            let mut v = vec![amount_min; num_rows];
            v[num_rows - 1] = amount_max;
            v
        };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(row_uuid_refs)),
            Arc::new(Int64Array::from(amounts)),
        ];
        let batch =
            RecordBatch::try_new(r6_fixture_schema(), columns).expect("valid r6 fixture batch");
        let stats = crate::stats::compute_segment_statistics(&batch);
        PersistSegment {
            segment_uuid: uuid.to_string(),
            uri: format!("s3://test/{uuid}"),
            format: Format::Parquet,
            row_count: num_rows as i64,
            size_bytes: 0,
            metadata_json: String::new(),
            statistics: stats,
            offset: None,
            length: None,
        }
    }

    #[tokio::test]
    async fn test_persist_table_provider_statistics_aggregate() {
        // CHA-82 R6: validates that PersistTableProvider::statistics()
        // returns a table-level Precision::Inexact aggregate folded over
        // the per-segment stats. This is the CBO cardinality contract per
        // ADR 0022 — persist segments carry stats for this aggregate,
        // even though they are NOT pruned by user filter.
        //
        // Today's failure: PersistTableProvider doesn't override
        // statistics(), so the default returns Statistics::new_unknown
        // (all Precision::Absent). The first assertion fires with
        // Absent != Inexact(35).
        //
        // After CHA-82 I1 makes compute_segment_statistics +
        // parse_segment_statistics + aggregate_table_statistics real,
        // and CHA-82 I2 wires PersistTableProvider::statistics() to call
        // aggregate_table_statistics(&self.parsed, &self.schema), this
        // test flips green without modification — the fixture goes
        // through real compute (writer) → real parse (reader at
        // provider construction) → real aggregate end-to-end.
        use datafusion::common::stats::{Precision, Statistics};
        use datafusion::scalar::ScalarValue;

        let readers: Arc<HashMap<i32, AnyFormatReader>> = Arc::new(HashMap::new());
        let schema = r6_fixture_schema();
        let segments = vec![
            make_persist_segment_with_amount_stats("seg0", 10, 0, 50),
            make_persist_segment_with_amount_stats("seg1", 20, 40, 80),
            make_persist_segment_with_amount_stats("seg2", 5, 70, 120),
        ];
        let provider = PersistTableProvider::new(
            segments,
            readers,
            schema.clone(),
            Arc::new(SegmentCache::new(1 << 20)),
        );

        // Default TableProvider::statistics() returns not_impl_err today;
        // fold that into Statistics::new_unknown so the assertion below
        // hits the Absent-vs-Inexact comparison rather than a panic
        // unwrapping the error. After CHA-82 I2, statistics() returns
        // Ok(aggregate) and the unwrap_or_else is a no-op.
        let stats = provider
            .statistics()
            .unwrap_or_else(|| Statistics::new_unknown(&schema));

        assert_eq!(
            stats.num_rows,
            Precision::Inexact(35),
            "expected num_rows == sum of per-segment row counts (10+20+5); got {:?}",
            stats.num_rows
        );
        // Field index 1 in r6_fixture_schema is `amount`.
        let amount_col = &stats.column_statistics[1];
        assert_eq!(
            amount_col.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(0))),
            "amount min should be min-of-mins across segments (min of 0,40,70); got {:?}",
            amount_col.min_value
        );
        assert_eq!(
            amount_col.max_value,
            Precision::Inexact(ScalarValue::Int64(Some(120))),
            "amount max should be max-of-maxes across segments (max of 50,80,120); got {:?}",
            amount_col.max_value
        );
        assert_eq!(
            amount_col.null_count,
            Precision::Inexact(0),
            "amount null_count should be sum across segments (all 0); got {:?}",
            amount_col.null_count
        );
    }

    // CHA-410: persist-tier output_ordering advertisement

    /// Schema carrying the Int64 `commit_seq_num` + `write_seq_num` columns — the
    /// persist total-version-order axes CHA-410 / CHA-431 advertise.
    fn seq_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("commit_seq_num", DataType::Int64, false),
            Field::new("write_seq_num", DataType::Int64, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    #[tokio::test]
    async fn persist_provider_scan_advertises_composite_seq_ordering() {
        // CHA-410 / CHA-431: PersistTableProvider declares its scan output is
        // ordered by (commit_seq_num ASC, write_seq_num ASC) — the total version
        // order — so EnforceSorting can elide a redundant SortExec and
        // order-aware operators can stream. Planner metadata only.
        let readers: Arc<HashMap<i32, AnyFormatReader>> = Arc::new(HashMap::new());
        let provider = PersistTableProvider::new(
            vec![],
            readers,
            seq_schema(),
            Arc::new(SegmentCache::new(1 << 20)),
        );
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let ordering = plan.properties().output_ordering();
        assert!(
            ordering.is_some(),
            "persist scan must advertise an output_ordering (CHA-410)",
        );
        let rendered = format!("{:?}", ordering.unwrap());
        let tx_pos = rendered.find("commit_seq_num");
        let write_pos = rendered.find("write_seq_num");
        assert!(
            tx_pos.is_some() && write_pos.is_some() && tx_pos < write_pos,
            "advertised ordering must be (commit_seq_num, write_seq_num) in that order \
             (CHA-431); got {rendered}",
        );
        assert!(
            rendered.contains("descending: false") || rendered.contains("ASC"),
            "advertised ordering must be ASC; got {rendered}",
        );
    }

    #[tokio::test]
    async fn persist_provider_falls_back_to_commit_seq_num_when_write_seq_num_projected_out() {
        // CHA-431: projecting write_seq_num away leaves the honest [commit_seq_num]
        // prefix — a weaker but still-true ordering over the emitted columns.
        let readers: Arc<HashMap<i32, AnyFormatReader>> = Arc::new(HashMap::new());
        let provider = PersistTableProvider::new(
            vec![],
            readers,
            seq_schema(),
            Arc::new(SegmentCache::new(1 << 20)),
        );
        let ctx = SessionContext::new();
        let state = ctx.state();
        // Project row_uuid(0) + commit_seq_num(1); write_seq_num(2) excluded.
        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[], None)
            .await
            .unwrap();
        let ordering = plan.properties().output_ordering();
        assert!(
            ordering.is_some(),
            "commit_seq_num is still emitted, so the [commit_seq_num] ordering must remain",
        );
        let rendered = format!("{:?}", ordering.unwrap());
        assert!(
            rendered.contains("commit_seq_num") && !rendered.contains("write_seq_num"),
            "with write_seq_num projected out the advertised order must be the \
             [commit_seq_num] prefix only; got {rendered}",
        );
    }

    #[tokio::test]
    async fn persist_provider_ordering_dropped_when_commit_seq_num_projected_out() {
        // CHA-410: cannot honestly advertise an order over a column the scan
        // does not emit — projecting commit_seq_num away drops the ordering.
        let readers: Arc<HashMap<i32, AnyFormatReader>> = Arc::new(HashMap::new());
        let provider = PersistTableProvider::new(
            vec![],
            readers,
            seq_schema(),
            Arc::new(SegmentCache::new(1 << 20)),
        );
        let ctx = SessionContext::new();
        let state = ctx.state();
        // Project only row_uuid (index 0); commit_seq_num (index 1) excluded.
        let plan = provider
            .scan(&state, Some(&vec![0]), &[], None)
            .await
            .unwrap();
        assert!(
            plan.properties().output_ordering().is_none(),
            "ordering must be dropped when commit_seq_num is projected out",
        );
    }

    #[tokio::test]
    async fn persist_scan_elides_sortexec_for_commit_seq_num_order() {
        // CHA-410: with the advertised ordering, an ORDER BY commit_seq_num over the
        // persist scan elides its SortExec. This pins the *mechanism* under its
        // minimal condition: target_partitions=1 keeps the single ordered
        // partition stream from being repartitioned before the sort. Production
        // persist reads run under `derive_cold_session` (multi-partition), so
        // this elision is LATENT there — no current consumer carries an
        // `ORDER BY commit_seq_num` (the merge window sorts by row_uuid-partitioned
        // `commit_seq_num DESC`, a different requirement). A future order-aware
        // consumer must keep the persist scan un-repartitioned to use the
        // advertised order, the way the snapshot ByPlan path pins
        // target_partitions=1 (`derive_cold_session_single_partition`).
        use datafusion::physical_plan::displayable;
        let readers: Arc<HashMap<i32, AnyFormatReader>> = Arc::new(HashMap::new());
        let provider = PersistTableProvider::new(
            vec![],
            readers,
            seq_schema(),
            Arc::new(SegmentCache::new(1 << 20)),
        );
        let config = datafusion::execution::context::SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_table("upsert_log", Arc::new(provider))
            .unwrap();
        let plan = ctx
            .sql("SELECT row_uuid, commit_seq_num FROM upsert_log ORDER BY commit_seq_num")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let display = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !display.contains("SortExec"),
            "ORDER BY commit_seq_num must elide SortExec given the advertised ordering:\n{display}"
        );
    }

    // CHA-411 R-A: SnapshotTableProvider session behavior

    /// In-test `FormatReader` returning a preset decoded batch (a stand-in for a
    /// snapshot segment / file). Ignores the requested projection — the test
    /// controls the batch shape; the provider null-fills / projects downstream.
    struct FixtureReader {
        batch: RecordBatch,
    }

    impl FormatReader for FixtureReader {
        async fn read_segment(
            &self,
            _uri: &str,
            _offset: Option<i64>,
            _length: Option<i64>,
            _schema: &SchemaRef,
            _projection: Option<&[&str]>,
        ) -> Result<RecordBatch, FormatError> {
            Ok(self.batch.clone())
        }
    }

    fn snapshot_seg(uuid: &str, size_bytes: i64) -> SnapshotSegment {
        SnapshotSegment {
            table_snapshot_segment_uuid: uuid.to_string(),
            format: Format::Parquet,
            size_bytes,
            ..Default::default()
        }
    }

    /// Register a `SnapshotTableProvider` (over a one-segment `FixtureReader`)
    /// as `l` plus a single-column `exclusion(row_uuid)` `MemTable`, then run
    /// `sql` against the session and collect the result.
    async fn run_snapshot_sql(
        decoded: RecordBatch,
        full_decode_schema: SchemaRef,
        out_schema: SchemaRef,
        exclusion: &[&str],
        sql: &str,
    ) -> Vec<RecordBatch> {
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            FixtureReader { batch: decoded },
        );
        let cache = Arc::new(SegmentCache::new(1 << 20));
        let provider = SnapshotTableProvider::new(
            vec![snapshot_seg("seg1", 4096)],
            Arc::new(readers),
            cache,
            full_decode_schema,
            out_schema,
            4,
            SegmentOrder::ByCompletion,
        );
        let ctx = SessionContext::new();
        ctx.register_table("l", Arc::new(provider)).unwrap();

        let excl_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "row_uuid",
            DataType::Utf8,
            false,
        )]));
        let excl_batch = RecordBatch::try_new(
            excl_schema.clone(),
            vec![Arc::new(StringArray::from(exclusion.to_vec()))],
        )
        .unwrap();
        let excl = MemTable::try_new(excl_schema, vec![vec![excl_batch]]).unwrap();
        ctx.register_table("exclusion", Arc::new(excl)).unwrap();

        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    }

    fn ra_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn ra_batch() -> RecordBatch {
        RecordBatch::try_new(
            ra_schema(),
            vec![
                Arc::new(StringArray::from(vec!["r1", "r2"])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(Int32Array::from(vec![1, 9])),
            ],
        )
        .unwrap()
    }

    fn collect_uuids(batches: &[RecordBatch]) -> Vec<String> {
        let mut out = Vec::new();
        for b in batches {
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
    async fn snapshot_provider_anti_join_drops_excluded() {
        let schema = ra_schema();
        let out = run_snapshot_sql(
            ra_batch(),
            schema.clone(),
            schema,
            &["r1"],
            "SELECT l.row_uuid FROM l WHERE l.row_uuid NOT IN (SELECT row_uuid FROM exclusion)",
        )
        .await;
        assert_eq!(
            collect_uuids(&out),
            vec!["r2".to_string()],
            "r1 dropped by the exclusion anti-join",
        );
    }

    #[tokio::test]
    async fn snapshot_provider_residual_filter() {
        let schema = ra_schema();
        let out = run_snapshot_sql(
            ra_batch(),
            schema.clone(),
            schema,
            &[],
            "SELECT l.row_uuid FROM l WHERE l.value > 5",
        )
        .await;
        assert_eq!(
            collect_uuids(&out),
            vec!["r2".to_string()],
            "only value>5 (r2) survives the residual filter",
        );
    }

    #[tokio::test]
    async fn snapshot_provider_null_fills_added_column() {
        // Decoded against the OLDER narrow schema {row_uuid, name} — `value`
        // was added later (CHA-252). The provider's declared out_schema carries
        // a nullable `value` that must be null-filled.
        let narrow_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let narrow = RecordBatch::try_new(
            narrow_schema.clone(),
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
        let out = run_snapshot_sql(
            narrow,
            narrow_schema,
            out_schema,
            &[],
            "SELECT l.row_uuid, l.value FROM l WHERE l.value IS NULL",
        )
        .await;
        assert_eq!(
            collect_uuids(&out),
            vec!["r1".to_string(), "r2".to_string()],
            "both rows null-filled `value` and match IS NULL",
        );
    }

    #[tokio::test]
    async fn snapshot_provider_projection_narrows() {
        let schema = ra_schema();
        let out = run_snapshot_sql(
            ra_batch(),
            schema.clone(),
            schema,
            &[],
            "SELECT l.name FROM l",
        )
        .await;
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "both rows stream through the scan");
        let out_schema = out[0].schema();
        let cols: Vec<&str> = out_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(cols, vec!["name"], "projection narrows output to `name`");
    }

    #[tokio::test]
    async fn snapshot_provider_scan_advertises_no_ordering() {
        // CHA-459 (not this ticket) owns output_ordering — the scan must
        // advertise no sort order.
        let schema = ra_schema();
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            FixtureReader { batch: ra_batch() },
        );
        let provider = SnapshotTableProvider::new(
            vec![snapshot_seg("seg1", 4096)],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        );
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        assert!(
            plan.properties().output_ordering().is_none(),
            "SnapshotTableProvider must not advertise output_ordering (CHA-459)",
        );
    }

    #[test]
    fn snapshot_provider_filter_pushdown_is_unsupported() {
        // ADR 0022/0023: pruning is external; the user filter is a DataFusion
        // FilterExec, never pushed into the reader.
        use datafusion::logical_expr::{col, lit};
        let schema = ra_schema();
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            FixtureReader { batch: ra_batch() },
        );
        let provider = SnapshotTableProvider::new(
            vec![],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        );
        let predicate = col("value").gt(lit(5));
        let pushdown = provider.supports_filters_pushdown(&[&predicate]).unwrap();
        assert_eq!(
            pushdown,
            vec![TableProviderFilterPushDown::Unsupported],
            "snapshot provider must not push the user filter into the reader",
        );
    }

    // CHA-454 R3: provider index-driven selective read

    /// A `FormatReader` serving a different preset batch per uri — lets the seek
    /// test register both the base segment and its index sidecar.
    struct MapReader {
        batches: HashMap<String, RecordBatch>,
        reads: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl FormatReader for MapReader {
        async fn read_segment(
            &self,
            uri: &str,
            _offset: Option<i64>,
            _length: Option<i64>,
            _schema: &SchemaRef,
            _projection: Option<&[&str]>,
        ) -> Result<RecordBatch, FormatError> {
            self.reads.lock().unwrap().push(uri.to_string());
            Ok(self.batches.get(uri).cloned().expect("uri registered"))
        }
    }

    #[tokio::test]
    async fn scan_snapshot_index_seek_emits_only_matches() {
        // Base rows r0..r4; the internal row_uuid sidecar is build_segment_index
        // over the row_uuid column. Seeking "r3" must emit ONLY r3 (O(matches)),
        // not the whole segment (O(rows)). RED until I4 wires the seek path.
        let schema = ra_schema();
        let base = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1", "r2", "r3", "r4"])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
            ],
        )
        .unwrap();
        let sidecar =
            penca_format::index::build_segment_index(std::slice::from_ref(base.column(0))).unwrap();

        let mut batches = HashMap::new();
        batches.insert("mem://base".to_string(), base);
        batches.insert("mem://sidecar".to_string(), sidecar);
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            MapReader {
                batches,
                reads: Arc::new(std::sync::Mutex::new(Vec::new())),
            },
        );

        let segment = SnapshotSegment {
            table_snapshot_segment_uuid: "seg1".to_string(),
            uri: "mem://base".to_string(),
            format: Format::Parquet,
            size_bytes: 4096,
            row_uuid_index_sidecar: Some(penca_core::IndexSidecar {
                object_uri: "mem://sidecar".to_string(),
                offset: 0,
                length: 5,
                format: Format::Parquet,
                segment_index_uuid: "sidecar1".to_string(),
                size_bytes: 256,
            }),
            ..Default::default()
        };

        let provider = SnapshotTableProvider::new(
            vec![segment],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        )
        .with_seeks(Some(Arc::new(vec![SeekSpec {
            index_uuid: None,
            key_columns: vec![],
            tuples: vec![vec!["r3".to_string()]],
        }])));

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(
            collect_uuids(&out),
            vec!["r3".to_string()],
            "index seek must emit ONLY the matched row r3 (O(matches)), not the \
             whole segment",
        );
    }

    #[tokio::test]
    async fn scan_snapshot_index_seek_absent_key_skips_base_decode() {
        // A key absent from the segment emits zero rows AND must not decode the
        // base at all (the zero-match short-circuit) — only the sidecar is read.
        let schema = ra_schema();
        let base = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1", "r2"])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int32Array::from(vec![0, 1, 2])),
            ],
        )
        .unwrap();
        let sidecar =
            penca_format::index::build_segment_index(std::slice::from_ref(base.column(0))).unwrap();
        let mut batches = HashMap::new();
        batches.insert("mem://base".to_string(), base);
        batches.insert("mem://sidecar".to_string(), sidecar);
        let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            MapReader {
                batches,
                reads: reads.clone(),
            },
        );
        let segment = SnapshotSegment {
            table_snapshot_segment_uuid: "seg1".to_string(),
            uri: "mem://base".to_string(),
            format: Format::Parquet,
            size_bytes: 4096,
            row_uuid_index_sidecar: Some(penca_core::IndexSidecar {
                object_uri: "mem://sidecar".to_string(),
                offset: 0,
                length: 3,
                format: Format::Parquet,
                segment_index_uuid: "sidecar1".to_string(),
                size_bytes: 256,
            }),
            ..Default::default()
        };
        let provider = SnapshotTableProvider::new(
            vec![segment],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        )
        .with_seeks(Some(Arc::new(vec![SeekSpec {
            index_uuid: None,
            key_columns: vec![],
            tuples: vec![vec!["rZ".to_string()]],
        }])));
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "absent key emits no rows");
        let reads = reads.lock().unwrap();
        assert!(
            reads.contains(&"mem://sidecar".to_string()),
            "the sidecar is consulted",
        );
        assert!(
            !reads.contains(&"mem://base".to_string()),
            "absent key must NOT decode the base (zero-match short-circuit)",
        );
    }

    #[tokio::test]
    async fn scan_snapshot_seek_keys_but_no_sidecar_full_scans() {
        // seek_keys present but the segment has no row_uuid_index_sidecar ⇒ the
        // CHA-411 full scan still streams every row (the seam must not break it).
        let schema = ra_schema();
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            FixtureReader { batch: ra_batch() },
        );
        let provider = SnapshotTableProvider::new(
            vec![snapshot_seg("seg1", 4096)],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        )
        .with_seeks(Some(Arc::new(vec![SeekSpec {
            index_uuid: None,
            key_columns: vec![],
            tuples: vec![vec!["r1".to_string()]],
        }])));
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(
            collect_uuids(&out),
            vec!["r1".to_string(), "r2".to_string()],
            "no row_uuid_index_sidecar ⇒ full scan despite seek_keys",
        );
    }

    #[tokio::test]
    async fn scan_snapshot_sidecar_but_no_seek_keys_full_scans() {
        // row_uuid_index_sidecar present but seek_keys = None ⇒ full scan (CHA-411).
        let schema = ra_schema();
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            FixtureReader { batch: ra_batch() },
        );
        let mut seg = snapshot_seg("seg1", 4096);
        seg.row_uuid_index_sidecar = Some(penca_core::IndexSidecar {
            object_uri: "mem://sidecar".to_string(),
            offset: 0,
            length: 2,
            format: Format::Parquet,
            segment_index_uuid: "sc".to_string(),
            size_bytes: 64,
        });
        let provider = SnapshotTableProvider::new(
            vec![seg],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        );
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(
            collect_uuids(&out),
            vec!["r1".to_string(), "r2".to_string()],
            "no seek_keys ⇒ full scan even with a sidecar present",
        );
    }

    #[tokio::test]
    async fn scan_snapshot_multi_entry_seek_intersects_offsets() {
        // CHA-485: identity entry matches {r1, r3}; a user index over the
        // `name` column probes "x" matching {r1, r2}. The AND across entries
        // must emit ONLY the intersection {r1}.
        let schema = ra_schema();
        let base = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1", "r2", "r3"])),
                Arc::new(StringArray::from(vec!["a", "x", "x", "d"])),
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
            ],
        )
        .unwrap();
        let identity_sidecar =
            penca_format::index::build_segment_index(std::slice::from_ref(base.column(0))).unwrap();
        let user_sidecar =
            penca_format::index::build_segment_index(std::slice::from_ref(base.column(1))).unwrap();

        let mut batches = HashMap::new();
        batches.insert("mem://base".to_string(), base);
        batches.insert("mem://identity-sidecar".to_string(), identity_sidecar);
        batches.insert("mem://user-sidecar".to_string(), user_sidecar);
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            MapReader {
                batches,
                reads: Arc::new(std::sync::Mutex::new(Vec::new())),
            },
        );

        let user_index_uuid = "22222222-2222-2222-2222-222222222222";
        let segment = SnapshotSegment {
            table_snapshot_segment_uuid: "seg1".to_string(),
            uri: "mem://base".to_string(),
            format: Format::Parquet,
            size_bytes: 4096,
            row_uuid_index_sidecar: Some(penca_core::IndexSidecar {
                object_uri: "mem://identity-sidecar".to_string(),
                offset: 0,
                length: 4,
                format: Format::Parquet,
                segment_index_uuid: "sidecar-identity".to_string(),
                size_bytes: 256,
            }),
            index_sidecars: vec![(
                user_index_uuid.to_string(),
                penca_core::IndexSidecar {
                    object_uri: "mem://user-sidecar".to_string(),
                    offset: 0,
                    length: 4,
                    format: Format::Parquet,
                    segment_index_uuid: "sidecar-user".to_string(),
                    size_bytes: 256,
                },
            )],
            ..Default::default()
        };

        let provider = SnapshotTableProvider::new(
            vec![segment],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        )
        .with_seeks(Some(Arc::new(vec![
            SeekSpec {
                index_uuid: None,
                key_columns: vec![],
                tuples: vec![vec!["r1".to_string()], vec!["r3".to_string()]],
            },
            SeekSpec {
                index_uuid: Some(user_index_uuid.to_string()),
                key_columns: vec!["name".to_string()],
                tuples: vec![vec!["x".to_string()]],
            },
        ])));

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(
            collect_uuids(&out),
            vec!["r1".to_string()],
            "AND across entries must emit only the offset intersection",
        );
    }

    #[tokio::test]
    async fn scan_snapshot_unresolved_entry_is_skipped() {
        // CHA-485: an entry whose index has no sidecar on this segment is
        // skipped — the remaining resolved (identity) entry still seeks, and
        // the result over-selects relative to the full AND (the residual
        // FilterExec's job, ADR 0023) rather than erroring or full-scanning.
        let schema = ra_schema();
        let base = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1", "r2"])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int32Array::from(vec![0, 1, 2])),
            ],
        )
        .unwrap();
        let identity_sidecar =
            penca_format::index::build_segment_index(std::slice::from_ref(base.column(0))).unwrap();
        let mut batches = HashMap::new();
        batches.insert("mem://base".to_string(), base);
        batches.insert("mem://identity-sidecar".to_string(), identity_sidecar);
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            MapReader {
                batches,
                reads: Arc::new(std::sync::Mutex::new(Vec::new())),
            },
        );

        let segment = SnapshotSegment {
            table_snapshot_segment_uuid: "seg1".to_string(),
            uri: "mem://base".to_string(),
            format: Format::Parquet,
            size_bytes: 4096,
            row_uuid_index_sidecar: Some(penca_core::IndexSidecar {
                object_uri: "mem://identity-sidecar".to_string(),
                offset: 0,
                length: 3,
                format: Format::Parquet,
                segment_index_uuid: "sidecar-identity".to_string(),
                size_bytes: 256,
            }),
            // No keyed sidecars: the user entry below cannot resolve.
            ..Default::default()
        };

        let provider = SnapshotTableProvider::new(
            vec![segment],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        )
        .with_seeks(Some(Arc::new(vec![
            SeekSpec {
                index_uuid: None,
                key_columns: vec![],
                tuples: vec![vec!["r2".to_string()]],
            },
            SeekSpec {
                index_uuid: Some("99999999-9999-9999-9999-999999999999".to_string()),
                key_columns: vec!["name".to_string()],
                tuples: vec![vec!["never".to_string()]],
            },
        ])));

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(
            collect_uuids(&out),
            vec!["r2".to_string()],
            "the resolved identity entry seeks; the unresolved user entry is skipped",
        );
    }

    #[test]
    fn resolve_seek_entries_routes_identity_keyed_and_unresolved() {
        let user = "22222222-2222-2222-2222-222222222222";
        let segment = SnapshotSegment {
            row_uuid_index_sidecar: Some(penca_core::IndexSidecar {
                object_uri: "mem://identity".to_string(),
                offset: 0,
                length: 1,
                format: Format::Parquet,
                segment_index_uuid: "id-sc".to_string(),
                size_bytes: 1,
            }),
            index_sidecars: vec![(
                user.to_string(),
                penca_core::IndexSidecar {
                    object_uri: "mem://user".to_string(),
                    offset: 0,
                    length: 1,
                    format: Format::Parquet,
                    segment_index_uuid: "user-sc".to_string(),
                    size_bytes: 1,
                },
            )],
            ..Default::default()
        };
        let specs = vec![
            SeekSpec {
                index_uuid: None,
                key_columns: vec![],
                tuples: vec![vec!["r1".to_string()]],
            },
            SeekSpec {
                index_uuid: Some(user.to_string()),
                key_columns: vec!["name".to_string()],
                tuples: vec![vec!["x".to_string()]],
            },
            SeekSpec {
                index_uuid: Some("99999999-9999-9999-9999-999999999999".to_string()),
                key_columns: vec!["name".to_string()],
                tuples: vec![vec!["y".to_string()]],
            },
        ];
        let resolved = resolve_seek_entries(&segment, Some(&specs));
        assert_eq!(resolved.len(), 2, "unknown-index entry drops");
        assert_eq!(resolved[0].0.object_uri, "mem://identity");
        assert_eq!(resolved[1].0.object_uri, "mem://user");
        // No seeks at all — and no sidecars resolving — both give empty.
        assert!(resolve_seek_entries(&segment, None).is_empty());
        let bare = SnapshotSegment::default();
        assert!(resolve_seek_entries(&bare, Some(&specs)).is_empty());
    }

    #[tokio::test]
    async fn scan_snapshot_typed_user_sidecar_seeks() {
        // CHA-485: a user index over the Int32 `value` column — the sidecar's
        // key schema is typed, decoded via the entry's key_columns mapped
        // through the table schema (NOT the all-Utf8 identity shape).
        let schema = ra_schema();
        let base = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1", "r2", "r3"])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![2, 9, 10, 100])),
            ],
        )
        .unwrap();
        let user_sidecar =
            penca_format::index::build_segment_index(std::slice::from_ref(base.column(2))).unwrap();
        let mut batches = HashMap::new();
        batches.insert("mem://base".to_string(), base);
        batches.insert("mem://value-sidecar".to_string(), user_sidecar);
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            MapReader {
                batches,
                reads: Arc::new(std::sync::Mutex::new(Vec::new())),
            },
        );

        let user_index_uuid = "33333333-3333-3333-3333-333333333333";
        let segment = SnapshotSegment {
            table_snapshot_segment_uuid: "seg1".to_string(),
            uri: "mem://base".to_string(),
            format: Format::Parquet,
            size_bytes: 4096,
            index_sidecars: vec![(
                user_index_uuid.to_string(),
                penca_core::IndexSidecar {
                    object_uri: "mem://value-sidecar".to_string(),
                    offset: 0,
                    length: 4,
                    format: Format::Parquet,
                    segment_index_uuid: "sidecar-value".to_string(),
                    size_bytes: 256,
                },
            )],
            ..Default::default()
        };

        let provider = SnapshotTableProvider::new(
            vec![segment],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        )
        .with_seeks(Some(Arc::new(vec![SeekSpec {
            index_uuid: Some(user_index_uuid.to_string()),
            key_columns: vec!["value".to_string()],
            tuples: vec![vec!["10".to_string()]],
        }])));

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        // Native Int32 ordering: 10 matches ONLY r2 — a lexicographic decode
        // of the sidecar would mis-seek entirely.
        assert_eq!(collect_uuids(&out), vec!["r2".to_string()]);
    }

    #[tokio::test]
    async fn scan_snapshot_fully_unresolved_seeks_full_scans() {
        // A Some(seeks) set where NOTHING resolves on the segment (user entry
        // only, no keyed sidecars) must stream the whole segment — never an
        // empty result (the residual filter owns exactness).
        let schema = ra_schema();
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            FixtureReader { batch: ra_batch() },
        );
        let provider = SnapshotTableProvider::new(
            vec![snapshot_seg("seg1", 4096)],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        )
        .with_seeks(Some(Arc::new(vec![SeekSpec {
            index_uuid: Some("99999999-9999-9999-9999-999999999999".to_string()),
            key_columns: vec!["name".to_string()],
            tuples: vec![vec!["never".to_string()]],
        }])));

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "fully unresolved ⇒ full scan of both rows");
    }

    #[tokio::test]
    async fn scan_snapshot_all_entries_skipped_full_scans() {
        // A resolved entry whose key column is missing from the table schema
        // is skipped INSIDE the intersect seek; with every entry skipped the
        // read must degrade to the full scan — an empty batch here would be
        // silent under-selection.
        let schema = ra_schema();
        let base = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1"])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![0, 1])),
            ],
        )
        .unwrap();
        let sidecar =
            penca_format::index::build_segment_index(std::slice::from_ref(base.column(1))).unwrap();
        let mut batches = HashMap::new();
        batches.insert("mem://base".to_string(), base);
        batches.insert("mem://sidecar".to_string(), sidecar);
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            MapReader {
                batches,
                reads: Arc::new(std::sync::Mutex::new(Vec::new())),
            },
        );
        let user_index_uuid = "44444444-4444-4444-4444-444444444444";
        let segment = SnapshotSegment {
            table_snapshot_segment_uuid: "seg1".to_string(),
            uri: "mem://base".to_string(),
            format: Format::Parquet,
            size_bytes: 4096,
            index_sidecars: vec![(
                user_index_uuid.to_string(),
                penca_core::IndexSidecar {
                    object_uri: "mem://sidecar".to_string(),
                    offset: 0,
                    length: 2,
                    format: Format::Parquet,
                    segment_index_uuid: "sidecar-x".to_string(),
                    size_bytes: 128,
                },
            )],
            ..Default::default()
        };
        let provider = SnapshotTableProvider::new(
            vec![segment],
            Arc::new(readers),
            Arc::new(SegmentCache::new(1 << 20)),
            schema.clone(),
            schema,
            4,
            SegmentOrder::ByCompletion,
        )
        .with_seeks(Some(Arc::new(vec![SeekSpec {
            index_uuid: Some(user_index_uuid.to_string()),
            key_columns: vec!["not_a_column".to_string()],
            tuples: vec![vec!["whatever".to_string()]],
        }])));

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "all entries skipped ⇒ full scan, not empty");
    }
}

/// CHA-404: the `ByPlan` ordered-scan contract the snapshot writer's
/// label-sorted run-grouping depends on.
#[cfg(test)]
mod by_plan_order_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::displayable;
    use penca_core::{Format, SnapshotSegment};
    use penca_format::reader::{FormatError, FormatReader};

    use super::build_snapshot_session;
    use crate::build_cold_session_template;
    use crate::cache::SegmentCache;
    use crate::driver::SegmentOrder;

    // The ByPlan production SQL is the PLAIN scan — no exclusion
    // anti-join (it would build the hash table over the snapshot side;
    // penca-merge applies the exclusion per batch instead).
    const PLAIN_SCAN_SQL: &str = "SELECT l.row_uuid FROM l";

    fn row_uuid_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "row_uuid",
            DataType::Utf8,
            false,
        )]))
    }

    fn one_row_batch(row_uuid: &str) -> RecordBatch {
        RecordBatch::try_new(
            row_uuid_schema(),
            vec![Arc::new(StringArray::from(vec![row_uuid]))],
        )
        .unwrap()
    }

    fn seg(uuid: &str, uri: &str) -> SnapshotSegment {
        SnapshotSegment {
            table_snapshot_segment_uuid: uuid.to_string(),
            uri: uri.to_string(),
            format: Format::Parquet,
            size_bytes: 64,
            ..Default::default()
        }
    }

    /// Per-uri preset batches; sleeps on uris containing "slow" so a
    /// completion-ordered stream would let later segments overtake.
    struct SlowFirstReader {
        batches: HashMap<String, RecordBatch>,
    }

    impl FormatReader for SlowFirstReader {
        async fn read_segment(
            &self,
            uri: &str,
            _offset: Option<i64>,
            _length: Option<i64>,
            _schema: &SchemaRef,
            _projection: Option<&[&str]>,
        ) -> Result<RecordBatch, FormatError> {
            if uri.contains("slow") {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(self.batches[uri].clone())
        }
    }

    fn session_for(
        order: SegmentOrder,
        exclusion: &[String],
    ) -> datafusion::execution::context::SessionContext {
        let mut batches = HashMap::new();
        batches.insert("memory://slow-a".to_string(), one_row_batch("a"));
        batches.insert("memory://fast-b".to_string(), one_row_batch("b"));
        batches.insert("memory://fast-c".to_string(), one_row_batch("c"));
        let mut readers = HashMap::new();
        readers.insert(Format::Parquet.as_wire_code(), SlowFirstReader { batches });
        let template = build_cold_session_template();
        build_snapshot_session(
            &template,
            &[
                seg("s-a", "memory://slow-a"),
                seg("s-b", "memory://fast-b"),
                seg("s-c", "memory://fast-c"),
            ],
            Arc::new(readers),
            Arc::new(SegmentCache::disabled()),
            row_uuid_schema(),
            row_uuid_schema(),
            exclusion,
            4,
            order,
            None,
        )
        .unwrap()
    }

    /// ByPlan output rows follow plan order even when the FIRST segment
    /// is the slowest — `buffered` readahead must not let b/c overtake a
    /// (with `buffer_unordered` they reliably would, 50ms vs ~0).
    #[tokio::test]
    async fn by_plan_output_follows_plan_order_despite_slow_head() {
        let ctx = session_for(SegmentOrder::ByPlan, &[]);
        let batches = ctx
            .sql(PLAIN_SCAN_SQL)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                (0..b.num_rows())
                    .map(|i| col.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            rows,
            vec!["a", "b", "c"],
            "plan order must survive to the output"
        );
    }

    /// The ByPlan physical plan must be a bare streaming projection:
    /// no order-destroying RepartitionExec and — decisively — no
    /// HashJoinExec at all. The first version of this test proved the
    /// in-plan `NOT IN` anti-join builds its hash table over the
    /// SNAPSHOT side (CollectLeft LeftAnti), materializing the whole
    /// prior snapshot and emitting hash order; the production ByPlan
    /// SQL therefore carries no exclusion join (penca-merge applies
    /// the exclusion per batch instead).
    #[tokio::test]
    async fn by_plan_physical_plan_shape_is_memory_safe() {
        let ctx = session_for(SegmentOrder::ByPlan, &[]);
        let plan = ctx
            .sql(PLAIN_SCAN_SQL)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let display = displayable(plan.as_ref()).indent(true).to_string();

        assert!(
            !display.contains("RepartitionExec"),
            "ByPlan session (target_partitions=1) must not repartition:\n{display}"
        );
        assert!(
            !display.contains("HashJoinExec"),
            "ByPlan scan must not join (exclusion is per-batch in penca-merge):\n{display}"
        );
        // Harden against future optimizer/caller changes: any partition
        // merge or sort above the provider would also break plan-order
        // delivery.
        assert!(
            !display.contains("CoalescePartitionsExec"),
            "ByPlan plan must not coalesce partitions:\n{display}"
        );
        assert!(
            !display.contains("SortExec"),
            "ByPlan plan must not re-sort:\n{display}"
        );
        assert!(
            display.contains("StreamingTableExec"),
            "snapshot provider must stream:\n{display}"
        );
    }
}
