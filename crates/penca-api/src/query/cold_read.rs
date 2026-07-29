//! The shared cold-read entry: maybe-seek (DataFusion-free) → else `stream_*`.
//!
//! When a snapshot-only read carries a single index seek and no value filter,
//! the seek IS the exact selection, so `seek_snapshot_point` serves it with no
//! DataFusion plan; anything else falls through to the merge pipeline. The
//! bypass must stay on the selection-not-filtering side of ADR 0023/0029 —
//! cache residency only tiers the physical read inside the kernel, it must
//! never gate which path is taken.

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

/// Drop `row_uuid` (column 0) → the user-column output schema.
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
/// user-column output schema. Only a single seek entry drives the bypass.
///
/// Identical for `read_data` (user tables) and `read_system_table` (metadata):
/// there is deliberately no metadata-specific parameter, because penca-merge
/// accepts any non-identity seek that carries a residual `filter` as a
/// selection accelerator (only a *filterless* one is a fail-fast). The
/// caller-shaped `SystemSelection -> (seek, residual, exact)` resolution stays
/// upstream in `read_system_table`.
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
        // Bypass gate: a snapshot-only plan whose selection is EXACT (the
        // caller-computed `exact_selection` — the structured seek set fully
        // covers the request, no residual conjunct and no value filter). The
        // seek being the exact selection is what makes DataFusion's predicate
        // eval / tier merge / visibility pruning non-load-bearing; `filter`
        // here is only the merge-fallback residual.
        //
        // On top of that, the bypass needs exactly one COVERING seek entry that
        // `seek_snapshot_point` can decode. It takes no key schema, so it
        // decodes Utf8 keys only — a typed (non-Utf8) index sidecar must fall
        // through to the merge scan, which re-types via `SeekSpec.key_columns`.
        // A segment whose snapshot never materialized the selected index
        // returns `None` and likewise falls through, residual `filter` applying.
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
                    // Selects the sidecar; `None` = identity.
                    seek_entry.index_uuid.as_ref(),
                    // Empty for the identity seek (all-Utf8).
                    &seek_entry.key_columns,
                    &full_decode_schema,
                    &out_schema,
                )
                .await
                .map_err(ApiError::Dl)?
            {
                // Integration tests scrape this event to assert the bypass
                // fired (`tests/integration/integration_direct_point_read_test.py`
                // and friends) — the field names are a test contract.
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

        // Fallback: the full merge pipeline. Seeks ride it as scan
        // accelerators only — the residual `filter` is what re-applies
        // exactness (ADR 0023), so a seek that misses a segment's sidecar is
        // still correct here.
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
