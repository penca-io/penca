//! CHA-482: the shared cold-read entry — lifts CHA-476's snapshot-only
//! DataFusion bypass out of `read_data`'s inline dispatch into one helper
//! callable by both `read_data` and (CHA-484) the by-name metadata resolves.
//!
//! "Build the merge request → maybe-seek (DataFusion-free) → else `stream_*`":
//! when a default-current-time, snapshot-only read carries a single internal
//! index seek and no value filter, the seek IS the exact selection, so the read
//! is served by `seek_snapshot_point` (CHA-454 kernel, composite per CHA-480)
//! with no DataFusion plan. Otherwise it falls through to the merge pipeline.
//! The bypass stays on the sanctioned selection-not-filtering side of ADR
//! 0023/0029 (`filter.is_none()` gate unchanged); cache residency only tiers
//! the physical read inside the kernel, never gates the path.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures_util::StreamExt;
use penca_core::Plan;
use penca_db::driver::DbDriver;
use penca_dl::driver::DlDriver;
use penca_merge::{
    IndexSeek, MergeReadRequest, ReadSnapshot, SegmentOrder, snapshot_read_schema, stream_all_cold,
    stream_merged,
};
use sqlx::postgres::PgRow;

use super::{BatchStream, is_all_cold, is_direct_seek_eligible};
use crate::error::ApiError;

/// Drop `row_uuid` (column 0) → the user-column output schema. Both the bypass
/// and the merge tail apply this before yielding; collapsed here from the two
/// duplicated sites the lift removed from `read_data`.
fn project_drop_row_uuid(
    batch: &RecordBatch,
    schema_ref: &SchemaRef,
) -> Result<RecordBatch, ApiError> {
    // For a 0-col projection `user_indices = []`, `RecordBatch::project(&[])`
    // preserves `num_rows` via its internal `with_row_count` — keeping the
    // `SELECT COUNT(*)` cardinality intact.
    let user_indices: Vec<usize> = (1..=schema_ref.fields().len()).collect();
    batch.project(&user_indices).map_err(ApiError::Arrow)
}

/// Shared cold-read: the DataFusion-free snapshot-only seek bypass, else the
/// `stream_all_cold` / `stream_merged` merge pipeline — both projected to the
/// user-column output schema. The single seek entry (identity, or via CHA-484 a
/// name index) drives the bypass; multi-entry intersection is CHA-485.
///
/// Serves both `read_data` (user tables) and `read_system_table` (metadata,
/// CHA-380) — the last divergences between them dissolved once the system rows
/// exposed first-class entity-uuid columns: metadata drops `row_uuid` here like
/// `read_data`, and its name/prefix seeks ride the merge fallback as selection
/// accelerators exactly like a user covering index (penca-merge accepts any
/// non-identity seek that carries a residual `filter`; only a filterless one is
/// a fail-fast). So there is no metadata-specific parameter — the caller-shaped
/// `SystemSelection -> (seek, residual, exact)` resolution stays upstream in
/// `read_system_table`, and this kernel is identical for both callers.
///
/// The args are not bundled into a `MergeReadRequest`: the bypass needs `dl`,
/// `schema_ref`, and `exact_selection` before a request is built, so the
/// request's fields stay positional here.
#[allow(clippy::too_many_arguments)]
pub(crate) fn stream_cold_read<'a, D, L>(
    driver: &'a D,
    dl: &'a L,
    plan: &'a Plan,
    schema_ref: &'a SchemaRef,
    full_schema: &'a SchemaRef,
    snapshot: &'a ReadSnapshot,
    filter: Option<&'a str>,
    seeks: Option<Vec<IndexSeek>>,
    exact_selection: bool,
    segment_read_concurrency: usize,
    snapshot_prune_min_segments: usize,
) -> BatchStream<'a>
where
    D: DbDriver<Row = PgRow>,
    L: DlDriver + ?Sized,
{
    Box::pin(async_stream::try_stream! {
        // CHA-476/482/492/501 bypass gate (shared with metadata's `read_system_table`
        // via `is_direct_seek_eligible`): a snapshot-only plan whose selection is
        // EXACT, on ANY snapshot axis (CHA-501: axis-independent). `exact_selection`
        // is computed by the caller (CHA-492: the structured seek set fully
        // covers the request — no residual conjunct left for the merge to apply,
        // the request carried no value filter). The seek is the exact selection,
        // so DataFusion (predicate eval / tier merge / visibility pruning) is not
        // load-bearing; the `filter` here is only the merge-fallback residual (and
        // the SQL-server FilterExec re-applies the exact predicate regardless).
        //
        // On top of the shared eligibility, the bypass adds one structural check:
        // exactly one COVERING seek entry whose key columns the DataFusion-free
        // `seek_snapshot_point` can decode — identity (`ids`, empty key columns,
        // Utf8 `row_uuid`) or a Utf8-keyed secondary index (CHA-492). A typed
        // (non-Utf8) index sidecar falls through to the merge scan (which
        // re-types via `SeekSpec.key_columns`; `seek_snapshot_point` takes no key
        // schema, so it can only decode Utf8). A segment whose snapshot never
        // materialized the selected index also returns `None` → falls through
        // (residual `filter` re-applies).
        if is_direct_seek_eligible(plan, exact_selection)
            && let Some([seek_entry]) = seeks.as_deref()
        {
            let snapshot_plan = plan
                .cold_storage
                .as_ref()
                .and_then(|cold| cold.snapshot.as_ref())
                .ok_or_else(|| {
                    ApiError::Internal(
                        "is_snapshot_only gate passed but plan has no snapshot leg".into(),
                    )
                })?;
            let out_schema = snapshot_read_schema(schema_ref);
            let full_decode_schema = snapshot_read_schema(full_schema);
            if let Some(batch) = dl
                .seek_snapshot_point(
                    &snapshot_plan.segments,
                    &seek_entry.tuples,
                    // CHA-484: the entry's index selects the sidecar (None =
                    // identity; read_data's seeks are identity by construction).
                    seek_entry.index_uuid.as_ref(),
                    // CHA-492: the index's key columns → the sidecar's key types
                    // (via the table schema), so a typed non-Utf8 index decodes
                    // on the bypass too. Empty for the identity seek (all-Utf8).
                    &seek_entry.key_columns,
                    &full_decode_schema,
                    &out_schema,
                )
                .await
                .map_err(ApiError::Dl)?
            {
                // The DataFusion-free point-read scrape seam (CHA-476). Fires
                // for both user-data and metadata reads — they are one path.
                tracing::debug!(
                    direct_point_read = true,
                    tier_shape = "snapshot_only",
                    "direct snapshot-only point read"
                );
                let projected = project_drop_row_uuid(&batch, schema_ref)?;
                if projected.num_rows() > 0 {
                    yield projected;
                }
                return;
            }
        }

        // Fallback: the full merge pipeline. The seek entries' restrictions are
        // threaded by the merge layer — identity (row_uuid) plus, CHA-492, any
        // materialized secondary-index seek (rides the merge as a scan
        // accelerator, the residual `filter` re-applying exactness, ADR 0023).
        // A single covering seek reaches here only when its bypass attempt
        // missed a segment's sidecar; a defined-but-unmaterialized index never
        // produced an entry and rides the residual `filter` alone. Metadata
        // name/prefix seeks (CHA-380) ride here too — penca-merge accepts any
        // non-identity seek accompanied by its residual `filter` as a selection
        // accelerator, so they need no special-casing.
        let req = MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan,
            driver,
            dl,
            user_schema: schema_ref,
            full_schema,
            snapshot,
            filter,
            seeks,
            segment_read_concurrency,
            snapshot_prune_min_segments,
        };
        let mut stream = if is_all_cold(plan) {
            stream_all_cold(req)
        } else {
            stream_merged(req)
        };
        while let Some(item) = stream.next().await {
            let batch = item.map_err(ApiError::Merge)?;
            yield project_drop_row_uuid(&batch, schema_ref)?;
        }
    })
}
