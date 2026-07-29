//! Merge-on-read algorithm — symmetric per-tier resolve.
//!
//! One SQL builder per tier resolves the committed log delta as a two-arm
//! `UNION ALL` — visible upserts (`is_delete = false`) and winning tombstones
//! (`is_delete = true`) — so a single scan per tier yields both the live rows
//! and every touched `row_uuid`:
//!   - Hot (Postgres) — [`sql::build_merge_resolved`] JOINs the hot
//!     `commit_tx_log` partition to recover each row's `commit_micros`.
//!     Executed via [`penca_storage_hot::execute_query_as_batch`].
//!   - Cold (DataFusion over arrow) — [`sql::build_cold_merge_resolved`].
//!     Cold rows carry `commit_micros` inline (denormalized at persist time per
//!     ADR 0017), so the cold side reads as a pure scan — no JOIN against
//!     commit_tx_log. Executed via [`penca_dl::driver::DlDriver`].
//!
//! Pipeline:
//!   1. **Resolve** (per tier): run the two-arm resolve against hot and cold,
//!      union the batches, dedup by `row_uuid` keeping the latest
//!      `commit_micros`.
//!   2. **Exclusion set + live delta**: the full `row_uuid` set of the composed
//!      resolve IS the exclusion set (every touched row shadows any snapshot row
//!      with the same `row_uuid`); the `is_delete = false` subset is the live
//!      delta. Both are derived from the UNFILTERED resolve, before the user
//!      `WHERE` residual, so a filtered-out current version can't let a
//!      stale snapshot version resurface. The user predicate is then applied once
//!      as a DataFusion residual ([`apply_resolved_residual`]), the single
//!      filter engine across tiers (ADR 0023).
//!   3. **Snapshot scan**: scan the surviving snapshot segments through a
//!      registered `SnapshotTableProvider`, with the exclusion-set anti-join
//!      and residual filter expressed in the scan SQL.

pub mod sql;

mod audit;
mod error;
mod output;
mod resolve;
mod schema;
mod snapshot;

pub use audit::cold_audit_batches;
pub use error::MergeError;
pub use schema::{cold_delete_schema, cold_upsert_schema, snapshot_read_schema};
pub use snapshot::ReadSnapshot;

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures_core::Stream;
use futures_util::{StreamExt, TryStreamExt};
use penca_core::{BaseColdStorage, Plan, SnapshotPlan};
use penca_db::driver::DbDriver;
use penca_dl::dialect::DfDialect;
pub use penca_dl::driver::SegmentOrder;
use penca_dl::driver::{DlDriver, SeekSpec};
use penca_dl::schema::{EXCLUSION_TABLE, LogSchemas, SNAPSHOT_TABLE};
use sqlx::postgres::PgRow;
use tracing_futures::Instrument as _;
use uuid::Uuid;

use crate::output::{full_plan_predicate, project_to_output};
// Re-exported so the planner's covering-index pass (penca-api index_select)
// extracts equality bindings through the same parse machinery the pruning path
// uses — predicate semantics can never diverge between pruning and selection,
// and DataFusion types stay inside this crate.
pub use crate::output::equality_bindings;
// Re-exported so the all-hot read path (`penca_api::query::stream_all_hot`)
// applies the user predicate through this same helper — hot and cold filter
// through one engine (`full_plan_predicate`). `ResidualFilter` is the
// compile-once/apply-per-batch form the multi-batch hot stream needs.
pub use crate::output::{ResidualFilter, apply_resolved_residual};
use crate::resolve::{
    build_cold_resolved_and_exclusion_set, build_resolved_and_exclusion_set, collect_row_uuids,
    filter_live_rows, resolve_cold,
};
use crate::schema::cold_persist_schemas;
use crate::sql::{build_cold_snapshot_scan, build_cold_snapshot_scan_plain};

/// An internal (never-proto) index-seek entry — a named (possibly composite)
/// index and the tuple probes to union within it. The identity `row_uuid` index
/// is `index_uuid: None` (per CHA-412 `index_uuid IS NULL`); a name/user index
/// carries its stable `index_uuid` (stable across rename). Within an entry the
/// tuples are a union (IN-list of composite keys); across entries the seek
/// INTERSECTS (AND over the covering indexes the planner selected).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexSeek {
    /// `None` = the internal row_uuid identity index; `Some` = a named index.
    pub index_uuid: Option<Uuid>,
    /// The index's key column names in sort-priority order. Empty for the
    /// identity index (its single Utf8 `row_uuid` key needs no lookup); for a
    /// user index the seek derives each sidecar key column's native DataType
    /// from the table schema through these names, so typed (non-Utf8) sidecars
    /// decode against their real schema.
    pub key_columns: Vec<String>,
    /// Union of composite-key probe tuples; arity = the index's key-column count
    /// (1 for the identity index).
    pub tuples: Vec<Vec<String>>,
}

impl IndexSeek {
    /// The identity (row_uuid) seek entry: each `row_uuid` as an arity-1 tuple.
    pub fn identity(row_uuids: &[Uuid]) -> Self {
        Self {
            index_uuid: None,
            key_columns: Vec::new(),
            tuples: row_uuids.iter().map(|u| vec![u.to_string()]).collect(),
        }
    }

    /// Build the single-entry identity seek a read's `row_uuid` restriction maps
    /// to, or `None` when unrestricted. The canonical `Option<&[Uuid]>` ->
    /// `MergeReadRequest.seeks` construction.
    pub fn identity_seeks(row_uuids: Option<&[Uuid]>) -> Option<Vec<Self>> {
        row_uuids.map(|uuids| vec![Self::identity(uuids)])
    }
}

/// Extract the identity (row_uuid) restriction the merge fallback applies.
/// `Ok(None)` when there is no seek (or only accelerator entries).
///
/// Non-identity entries (`index_uuid.is_some()`) are legal here **iff a
/// NON-EMPTY residual `filter` accompanies them** — `filter_present` must use
/// the same emptiness predicate the SQL builder does, so the gate can never
/// pass while no residual WHERE is emitted. With a residual they are pure
/// selection accelerators on the snapshot scan (a skipped or over-selecting
/// entry stays correct because the residual re-applies the exact predicate) and
/// never restrict the exclusion set. Without one they are malformed — the
/// exact-selection no-filter shape is served by the snapshot-only bypass — and
/// fail fast, as does a non-arity-1 identity tuple.
fn identity_row_uuids(
    seeks: Option<&[IndexSeek]>,
    filter_present: bool,
) -> Result<Option<Vec<Uuid>>, MergeError> {
    let Some(seeks) = seeks else {
        return Ok(None);
    };
    if seeks.is_empty() {
        return Ok(None);
    }
    let mut row_uuids = Vec::new();
    let mut saw_identity = false;
    for entry in seeks {
        if entry.index_uuid.is_some() {
            if filter_present {
                continue;
            }

            return Err(MergeError::InvalidPlan(
                "name-index seek must never reach the merge fallback; CHA-484 serves it \
                 from the metadata bypass or degrades it to a SQL residual upstream"
                    .to_string(),
            ));
        }
        saw_identity = true;
        for tuple in &entry.tuples {
            let [row_uuid_str] = tuple.as_slice() else {
                return Err(MergeError::InvalidPlan(format!(
                    "identity (row_uuid) seek tuples must be arity 1, found arity {}",
                    tuple.len()
                )));
            };
            let row_uuid = Uuid::parse_str(row_uuid_str).map_err(|e| {
                MergeError::InvalidPlan(format!("invalid row_uuid in identity seek: {e}"))
            })?;
            row_uuids.push(row_uuid);
        }
    }
    if !saw_identity {
        return Ok(None);
    }

    Ok(Some(row_uuids))
}

/// Count of identity-seek tuples, for the `ids_rows` span field (PII-gated to a
/// count, same as `filter`). Non-failing — the fail-fast lives in
/// [`identity_row_uuids`].
fn identity_seek_len(seeks: Option<&[IndexSeek]>) -> u64 {
    seeks.map_or(0, |s| {
        s.iter()
            .filter(|e| e.index_uuid.is_none())
            .map(|e| e.tuples.len())
            .sum::<usize>() as u64
    })
}

/// Map the accelerator entries (non-identity) onto the penca-dl boundary shape
/// for the provider seek. `None` when there are none; the identity restriction
/// stays on its own dedicated threading because it also restricts the exclusion
/// set — these never do.
fn user_seek_specs(seeks: Option<&[IndexSeek]>) -> Option<Arc<Vec<SeekSpec>>> {
    let specs: Vec<SeekSpec> = seeks?
        .iter()
        .filter(|entry| entry.index_uuid.is_some())
        .map(|entry| SeekSpec {
            index_uuid: entry.index_uuid.map(|u| u.to_string()),
            key_columns: entry.key_columns.clone(),
            tuples: entry.tuples.clone(),
        })
        .collect();
    (!specs.is_empty()).then(|| Arc::new(specs))
}

/// The seek bundle the snapshot stream executes against — the parsed identity
/// restriction plus the planner's covering-index accelerator entries —
/// threaded as ONE value from the `*_parts` entry points down to the scan.
/// External callers construct it via `Default` (the snapshot writer seeks
/// nothing).
#[derive(Debug, Default)]
pub struct SnapshotSeeks {
    /// Identity (row_uuid) restriction: the ByCompletion scan SQL's
    /// `l.row_uuid IN` residual, the ByPlan per-batch included-filter, and
    /// the identity seek entry handed to the provider.
    pub(crate) identity: Option<Vec<Uuid>>,
    /// Covering-index accelerator entries (provider seek only — never the
    /// SQL, never the exclusion set).
    pub(crate) accelerators: Option<Arc<Vec<SeekSpec>>>,
}

impl SnapshotSeeks {
    /// The raw identity slice for the ByCompletion SQL builder.
    fn identity(&self) -> Option<&[Uuid]> {
        self.identity.as_deref()
    }

    /// The full entry set for [`DlDriver::scan_snapshot`] — the identity
    /// entry first (composed from the parsed restriction), accelerators
    /// after. Copy-free when only accelerators are present.
    fn to_scan_specs(&self) -> Option<Arc<Vec<SeekSpec>>> {
        match (&self.identity, &self.accelerators) {
            (None, None) => None,
            (None, Some(accelerators)) => Some(Arc::clone(accelerators)),
            (Some(uuids), accelerators) => {
                let mut specs =
                    Vec::with_capacity(1 + accelerators.as_ref().map_or(0, |a| a.len()));
                specs.push(SeekSpec {
                    index_uuid: None,
                    key_columns: Vec::new(),
                    tuples: uuids.iter().map(|u| vec![u.to_string()]).collect(),
                });
                if let Some(accelerators) = accelerators {
                    specs.extend(accelerators.iter().cloned());
                }
                Some(Arc::new(specs))
            }
        }
    }

    /// The stringified identity set for the ByPlan per-batch included-filter.
    fn identity_strings(&self) -> Option<HashSet<String>> {
        self.identity
            .as_ref()
            .map(|uuids| uuids.iter().map(|u| u.to_string()).collect())
    }
}

pub struct MergeReadRequest<'a, D, L: ?Sized> {
    pub plan: &'a Plan,
    pub driver: &'a D,
    pub dl: &'a L,
    pub user_schema: &'a SchemaRef,
    /// The table's full (unprojected) user schema. `user_schema` is the
    /// projected view this read returns; `full_schema` is what the snapshot
    /// segment cache decodes so one cached entry serves any projection.
    /// Equal to `user_schema` when the read is unprojected.
    pub full_schema: &'a SchemaRef,
    pub snapshot: &'a ReadSnapshot,
    pub filter: Option<&'a str>,
    /// Internal index seeks (never on the proto); `None` = unrestricted.
    ///
    /// Only the identity entry restricts the log tiers + exclusion probes
    /// (threaded below the latest-wins dedup). Non-identity entries
    /// (planner-selected covering user indexes) ride the snapshot scan as
    /// selection accelerators — per segment, each entry's sidecar offsets are
    /// seeked and INTERSECTED (AND across indexes) before the decode; they
    /// require a residual `filter` (over-selection is re-filtered; ADR 0023)
    /// and never touch the exclusion set. A no-filter non-identity entry is
    /// served by the snapshot-only bypass, never this path.
    ///
    /// TODO(CHA-489): range probes (a probe-enum variant on `IndexSeek`) —
    /// tuples express equality/IN only.
    pub seeks: Option<Vec<IndexSeek>>,
    /// Snapshot-segment delivery order. `ByCompletion` for every read path;
    /// `ByPlan` only for the snapshot writer's label-sorted run-grouping.
    pub segment_order: SegmentOrder,
    /// Caps how many snapshot segments are read in flight during phase 3. A
    /// memory-safety knob — each read materializes a segment in memory — so it
    /// should be set to `floor(reader_memory_budget / max_segment_bytes)`.
    pub segment_read_concurrency: usize,
    /// Skip segment pruning (build no pruning predicate; read every planned
    /// segment) unless the planned snapshot segment count *exceeds* this.
    /// Pruning builds a full DataFusion plan of the filter (~hundreds of µs),
    /// worth it only when there are enough segments to skip; the residual
    /// filter enforces correctness regardless. `0` always prunes; the
    /// deployment default is `1` (skip the single-segment case).
    pub snapshot_prune_min_segments: usize,
}

/// Stream merge-on-read results for a single table.
///
/// The algorithm is symmetric across the hot and cold tiers — the same
/// logical SQL runs against both (dialect-specialized for Postgres vs.
/// DataFusion), the two resolved batches are unioned and deduped, and
/// then the snapshot tier is scanned through a registered
/// `SnapshotTableProvider` with the exclusion anti-join + residual in the plan.
///
/// `req.snapshot` selects which view of the data this read draws from; the
/// variant determines the visibility predicate emitted by the SQL builder.
///
/// `req.filter` is an optional SQL WHERE fragment. The per-tier resolves run
/// UNFILTERED (so the exclusion set derives from the full row_uuid set before
/// any filtering), then the predicate is applied once as a residual
/// ([`apply_resolved_residual`]) and inside each snapshot segment scan.
pub fn stream_merged<'a, D, L>(
    req: MergeReadRequest<'a, D, L>,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, MergeError>> + Send + 'a>>
where
    D: DbDriver<Row = PgRow>,
    L: DlDriver + ?Sized,
{
    // Plan-shape fields live on `stream_merged_parts`'s span (the
    // canonical pipeline boundary); this stream-lifetime span carries
    // only the fact the composition owns — total rows emitted.
    let span = tracing::debug_span!("stream_merged", rows_emitted = tracing::field::Empty);

    interleave_parts(span, stream_merged_parts(req))
}

/// All-cold merge read for plans with no hot tier
/// (`plan.hot_storage == None`): composes only the cold arms —
/// `resolve_cold` (the two-arm resolve whose row_uuid set is the exclusion
/// set) + the snapshot scan — with no hot probes in the flow at all. Issues
/// the same per-tier SQL an all-cold plan takes through [`stream_merged`]
/// (whose hot arms self-skip at runtime); this entry makes the absence
/// structural.
pub fn stream_all_cold<'a, D, L>(
    req: MergeReadRequest<'a, D, L>,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, MergeError>> + Send + 'a>>
where
    D: Sync,
    L: DlDriver + ?Sized,
{
    let span = tracing::debug_span!("stream_all_cold", rows_emitted = tracing::field::Empty);
    interleave_parts(span, stream_all_cold_parts(req))
}

/// Interleaved consumption of [`MergeReadParts`]: emit the resolved
/// log-tier batch first (projected to `row_uuid` + user cols), then
/// drive the snapshot stream, recording total rows emitted on `span`
/// at exhaustion.
fn interleave_parts<'a, F>(
    span: tracing::Span,
    parts_fut: F,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, MergeError>> + Send + 'a>>
where
    F: Future<Output = Result<MergeReadParts<'a>, MergeError>> + Send + 'a,
{
    Box::pin(
        async_stream::try_stream! {
            let mut rows_emitted: i64 = 0;
            let MergeReadParts {
                resolved,
                snapshot_stream,
            } = parts_fut.await?;

            if resolved.num_rows() > 0 {
                rows_emitted += resolved.num_rows() as i64;
                yield resolved;
            }

            let mut stream = snapshot_stream;
            while let Some(item) = stream.next().await {
                let out_batch = item?;
                rows_emitted += out_batch.num_rows() as i64;
                yield out_batch;
            }

            tracing::Span::current().record("rows_emitted", rows_emitted);
        }
        .instrument(span),
    )
}

/// The two halves of a merge read, split for callers that consume the
/// resolved (log-tier) rows and the snapshot tier differently — the
/// snapshot writer collects the delta but streams the prior snapshot
/// through its partition packer.
pub struct MergeReadParts<'a> {
    /// Phases 1 + 2: resolved hot+cold log rows, projected to the
    /// output schema (`row_uuid` + user cols). Empty batch when the
    /// logs resolve to nothing.
    pub resolved: RecordBatch,
    /// Phase 3: the snapshot-tier stream — segment pruning, then the
    /// exclusion expressed in the scan plan (ByCompletion) or applied
    /// per batch after a plain scan (ByPlan). Empty stream when the
    /// plan has no snapshot leg.
    pub snapshot_stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, MergeError>> + Send + 'a>>,
}

/// Run phases 1 + 2 of a merge read and hand back the snapshot-tier
/// stream unconsumed. [`stream_merged`] is the interleaved composition of
/// this; see its doc for the read algorithm.
#[tracing::instrument(
    skip_all,
    level = "debug",
    fields(
        snapshot = ?req.snapshot,
        has_hot_plan = req.plan.hot_storage.is_some(),
        has_cold_plan = req.plan.cold_storage.is_some(),
        filter_present = req.filter.is_some(),
        // Count only — PK values are PII-gated, same as `filter`.
        ids_rows = identity_seek_len(req.seeks.as_deref()),
        segment_order = ?req.segment_order,
        segment_read_concurrency = req.segment_read_concurrency,
        snapshot_segments_planned = req
            .plan
            .cold_storage
            .as_ref()
            .and_then(|c| c.snapshot.as_ref())
            .map(|s| s.segments.len())
            .unwrap_or(0),
        resolved_rows = tracing::field::Empty,
    ),
)]
pub async fn stream_merged_parts<'a, D, L>(
    req: MergeReadRequest<'a, D, L>,
) -> Result<MergeReadParts<'a>, MergeError>
where
    D: DbDriver<Row = PgRow>,
    L: DlDriver + ?Sized,
{
    let MergeReadRequest {
        plan,
        driver,
        dl,
        user_schema,
        full_schema,
        snapshot,
        filter,
        seeks,
        segment_order,
        segment_read_concurrency,
        snapshot_prune_min_segments,
    } = req;

    let row_uuids = identity_row_uuids(seeks.as_deref(), filter.is_some_and(|f| !f.is_empty()))?;
    // Mapped at the penca-dl boundary: penca-merge owns IndexSeek, the provider
    // owns SeekSpec — a dep cycle forbids sharing the type.
    let accelerators = user_seek_specs(seeks.as_deref());

    let user_cols: Vec<&str> = user_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let log_schemas = cold_persist_schemas(user_schema);

    // Phases 1 + 2. The resolves carry no user filter: the exclusion set must be
    // derived from the UNFILTERED resolved batch, so the residual is applied to
    // the resolved rows inside `assemble_parts` (after cross-tier dedup), never
    // per-source.
    let (resolved, exclusion_set) = build_resolved_and_exclusion_set(
        plan,
        driver,
        dl,
        &user_cols,
        user_schema,
        &log_schemas,
        snapshot,
        row_uuids.as_deref(),
    )
    .await?;

    // Fold the parent (base) cold source in below the child at the resolved
    // schema (pre-projection); assemble_parts projects the combined batch.
    let (resolved, exclusion_set) = fold_base_if_present(
        plan,
        dl,
        &user_cols,
        user_schema,
        &log_schemas,
        filter,
        row_uuids.as_deref(),
        resolved,
        exclusion_set,
        false,
    )
    .await?;

    assemble_parts(
        plan,
        dl,
        user_schema,
        full_schema,
        filter,
        SnapshotSeeks {
            identity: row_uuids,
            accelerators,
        },
        resolved,
        exclusion_set,
        SnapshotStreamTuning {
            segment_read_concurrency,
            snapshot_prune_min_segments,
            segment_order,
        },
    )
    .await
}

/// Run phases 1 + 2 of an all-cold merge read and hand back the
/// snapshot-tier stream unconsumed. [`stream_all_cold`] is the
/// interleaved composition of this; the snapshot writer is the
/// split-consumption caller (its plan is cold-only by construction).
///
/// The plan must be all-cold (`hot_storage == None`): the hot tier is
/// never probed here, so accepting a hot plan would silently drop its
/// upserts and delete-exclusions from the read. Misroutes fail fast
/// with [`MergeError::InvalidPlan`].
#[tracing::instrument(
    skip_all,
    level = "debug",
    fields(
        snapshot = ?req.snapshot,
        has_cold_plan = req.plan.cold_storage.is_some(),
        filter_present = req.filter.is_some(),
        // Count only — PK values are PII-gated, same as `filter`.
        ids_rows = identity_seek_len(req.seeks.as_deref()),
        segment_order = ?req.segment_order,
        segment_read_concurrency = req.segment_read_concurrency,
        snapshot_segments_planned = req
            .plan
            .cold_storage
            .as_ref()
            .and_then(|c| c.snapshot.as_ref())
            .map(|s| s.segments.len())
            .unwrap_or(0),
        resolved_rows = tracing::field::Empty,
    ),
)]
pub async fn stream_all_cold_parts<'a, D, L>(
    req: MergeReadRequest<'a, D, L>,
) -> Result<MergeReadParts<'a>, MergeError>
where
    D: Sync,
    L: DlDriver + ?Sized,
{
    // The all-cold fail-fast lives in `resolve_log_tiers`.
    let ResolvedLogTiers {
        resolved,
        exclusion_set,
    } = resolve_log_tiers(&req).await?;

    // CHA-482: identity restriction for the snapshot seeks. The base-cold fold
    // that used to sit here moved into `resolve_log_tiers` (CHA-531), which
    // parses its own copy.
    let row_uuids = identity_row_uuids(
        req.seeks.as_deref(),
        req.filter.is_some_and(|f| !f.is_empty()),
    )?;

    // CHA-485 accelerator entries ride alongside the identity restriction for
    // the provider seek.
    let seeks = SnapshotSeeks {
        identity: row_uuids,
        accelerators: user_seek_specs(req.seeks.as_deref()),
    };

    Ok(MergeReadParts {
        resolved,
        snapshot_stream: plan_snapshot_stream(
            req.plan,
            req.dl,
            req.user_schema,
            req.full_schema,
            req.filter,
            seeks,
            exclusion_set,
            SnapshotStreamTuning {
                segment_read_concurrency: req.segment_read_concurrency,
                snapshot_prune_min_segments: req.snapshot_prune_min_segments,
                segment_order: req.segment_order,
            },
        ),
    })
}

/// Tail of [`stream_merged_parts`] (its only caller): project the resolved
/// batch to the output schema (recording `resolved_rows` on the caller's span),
/// construct the phase-3 snapshot stream via [`plan_snapshot_stream`] when the
/// plan has a snapshot leg, and assemble the [`MergeReadParts`].
#[allow(clippy::too_many_arguments)]
async fn assemble_parts<'a, L>(
    plan: &'a Plan,
    dl: &'a L,
    user_schema: &'a SchemaRef,
    full_schema: &'a SchemaRef,
    filter: Option<&'a str>,
    // Owned so the returned 'a snapshot stream captures the parsed seek bundle
    // by move (it can't borrow a caller-local parse).
    seeks: SnapshotSeeks,
    resolved: RecordBatch,
    exclusion_set: HashSet<String>,
    tuning: SnapshotStreamTuning,
) -> Result<MergeReadParts<'a>, MergeError>
where
    L: DlDriver + ?Sized,
{
    // The resolved log-tier batch arrives UNFILTERED; apply the residual here,
    // once, after the cross-tier dedup that produced it — never per-source. The
    // exclusion set was already derived from the unfiltered resolved rows
    // upstream, so trimming rows here cannot let a shadowed snapshot version
    // leak. The snapshot leg applies the identical `full_plan_predicate` inside
    // its own scan, so both tiers evaluate one predicate.
    let resolved = apply_resolved_residual(&dl.derive_session(), filter, resolved).await?;
    let resolved = project_resolved(resolved, user_schema)?;
    tracing::Span::current().record("resolved_rows", resolved.num_rows() as i64);

    Ok(MergeReadParts {
        resolved,
        snapshot_stream: plan_snapshot_stream(
            plan,
            dl,
            user_schema,
            full_schema,
            filter,
            seeks,
            exclusion_set,
            tuning,
        ),
    })
}

/// Fold the parent (base) cold source into a forked branch's read,
/// when the plan carries one. No-op (returns the inputs unchanged) for a
/// non-forked branch.
#[allow(clippy::too_many_arguments)]
async fn fold_base_if_present<L>(
    plan: &Plan,
    dl: &L,
    user_cols: &[&str],
    user_schema: &SchemaRef,
    log_schemas: &LogSchemas,
    filter: Option<&str>,
    row_uuids: Option<&[Uuid]>,
    resolved: RecordBatch,
    exclusion_set: HashSet<String>,
    project_base_to_output: bool,
) -> Result<(RecordBatch, HashSet<String>), MergeError>
where
    L: DlDriver + ?Sized,
{
    match &plan.base_cold_storage {
        Some(base) => {
            fold_in_base_cold_source(
                base,
                dl,
                user_cols,
                user_schema,
                log_schemas,
                filter,
                row_uuids,
                resolved,
                exclusion_set,
                project_base_to_output,
            )
            .await
        }
        None => Ok((resolved, exclusion_set)),
    }
}

/// Resolve the parent branch's cold source (at its own seq ceiling)
/// and fold it in *below* the child (`hot > child-cold > parent-cold`) via a
/// row_uuid anti-join: a parent row survives iff the child never touched that
/// row_uuid (∉ `exclusion_set`). Survivors are disjoint from the child rows on
/// `row_uuid` — child seqs (> fork_seed) strictly dominate parent seqs
/// (<= fork_seed) — so the union is a plain concat, no `commit_micros` dedup.
/// The returned exclusion set gains the parent's shadowing + delete uuids so
/// the base snapshot scan ([`plan_snapshot_stream`]) drops them too.
///
/// `project_base_to_output` matches the parent's resolved batch to the child
/// batch's schema: the merged path folds at the resolved schema (carries
/// `commit_micros`), the all-cold path at the output schema (already
/// projected).
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    level = "trace",
    skip_all,
    fields(
        commit_seq_ceiling = base.commit_seq_ceiling,
        base_resolved_rows = tracing::field::Empty,
    )
)]
async fn fold_in_base_cold_source<L>(
    base: &BaseColdStorage,
    dl: &L,
    user_cols: &[&str],
    user_schema: &SchemaRef,
    log_schemas: &LogSchemas,
    filter: Option<&str>,
    row_uuids: Option<&[Uuid]>,
    child_resolved: RecordBatch,
    mut exclusion_set: HashSet<String>,
    project_base_to_output: bool,
) -> Result<(RecordBatch, HashSet<String>), MergeError>
where
    L: DlDriver + ?Sized,
{
    // Cold-only view of the parent source; the ceiling caps the parent at the
    // fork on the seq axis (both the snapshot pick and the per-row persist).
    let base_plan = Plan {
        hot_storage: None,
        cold_storage: Some(base.cold.clone()),
        base_cold_storage: None,
    };
    let ceiling = Some(base.commit_seq_ceiling);

    // The full `row_uuid` set of the resolve is the base's exclusion
    // contribution (every parent-touched uuid, upsert-winner or
    // tombstone-winner); its `is_delete = false` subset is the base's live delta.
    let resolved_base = resolve_cold(
        &base_plan,
        dl,
        user_cols,
        user_schema,
        log_schemas,
        ceiling,
        row_uuids,
    )
    .await?;

    // Base exclusion = EVERY parent-touched row_uuid, UNFILTERED — collected
    // before the live/residual split so a parent row failing the user filter
    // still shadows its snapshot version.
    let base_uuids = collect_row_uuids(&resolved_base)?;

    let live_base = filter_live_rows(&resolved_base)?;
    // Where the user residual hits the base depends on which path folds it (both
    // are the same `project_base_to_output` condition):
    //   - all-cold (`true`): the child was already residual-filtered + projected
    //     in `resolve_log_tiers` and the combined batch is NOT re-filtered
    //     downstream, so apply the same residual to the base's live rows here and
    //     project to match the child's output schema.
    //   - mixed (`false`): `assemble_parts` applies the residual to the whole
    //     combined batch AFTER this fold, so leave the base at the resolved
    //     schema, unfiltered.
    let live_base = if project_base_to_output {
        let filtered = apply_resolved_residual(&dl.derive_session(), filter, live_base).await?;
        project_resolved(filtered, user_schema)?
    } else {
        live_base
    };
    tracing::Span::current().record("base_resolved_rows", live_base.num_rows() as i64);

    // Anti-join against the CHILD exclusion set (captured before it is extended
    // with the parent's own uuids below). Survivors are disjoint from the child
    // rows on `row_uuid`.
    let filtered_base = filter_excluded_row_uuids(&live_base, &exclusion_set)?;
    let combined = concat_resolved(child_resolved, filtered_base)?;

    // The base snapshot scan must also drop every parent-touched row_uuid
    // (shadowing upserts + winning tombstones).
    for uuid in base_uuids {
        exclusion_set.insert(uuid);
    }

    Ok((combined, exclusion_set))
}

/// Concatenate two resolved batches that are disjoint on `row_uuid` (no dedup).
/// Both must share a schema.
fn concat_resolved(a: RecordBatch, b: RecordBatch) -> Result<RecordBatch, MergeError> {
    if b.num_rows() == 0 {
        return Ok(a);
    }
    if a.num_rows() == 0 {
        return Ok(b);
    }
    Ok(arrow::compute::concat_batches(&a.schema(), &[a, b])?)
}

/// The phase-3 stream for a whole plan: [`snapshot_segment_stream`]
/// over the plan's snapshot leg when present, empty otherwise.
#[allow(clippy::too_many_arguments)]
fn plan_snapshot_stream<'a, L>(
    plan: &'a Plan,
    dl: &'a L,
    user_schema: &'a SchemaRef,
    full_schema: &'a SchemaRef,
    filter: Option<&'a str>,
    seeks: SnapshotSeeks,
    exclusion_set: HashSet<String>,
    tuning: SnapshotStreamTuning,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, MergeError>> + Send + 'a>>
where
    L: DlDriver + ?Sized,
{
    // Prefer the child's own snapshot leg; fall back to the parent baseline
    // (`base_cold_storage`). These are mutually exclusive by the planner gate —
    // the base source is enumerated only when the picker found no fork-covering
    // child snapshot — but keeping the child leg authoritative here is the safe
    // ordering if that ever changes.
    let snapshot_plan = plan
        .cold_storage
        .as_ref()
        .and_then(|cold_plan| cold_plan.snapshot.as_ref())
        .or_else(|| {
            plan.base_cold_storage
                .as_ref()
                .and_then(|base| base.cold.snapshot.as_ref())
        });
    if let Some(snapshot_plan) = snapshot_plan {
        snapshot_segment_stream(
            snapshot_plan,
            dl,
            user_schema,
            full_schema,
            filter,
            seeks,
            exclusion_set,
            tuning,
        )
    } else {
        Box::pin(futures_util::stream::empty())
    }
}

/// Project the resolved log-tier batch to the output schema
/// (`row_uuid` + user cols); empty batches map to an empty batch of
/// that schema without a projection pass.
fn project_resolved(
    resolved: RecordBatch,
    user_schema: &SchemaRef,
) -> Result<RecordBatch, MergeError> {
    let out_schema = snapshot_read_schema(user_schema);
    if resolved.num_rows() > 0 {
        project_to_output(&resolved, &out_schema)
    } else {
        Ok(RecordBatch::new_empty(out_schema))
    }
}

/// Phases 1 + 2 of an all-cold merge read as a separately-callable
/// half: the resolved log-tier delta plus the exclusion set the
/// snapshot scan must apply.
///
/// Carry-forward's split consumption: resolve the delta ONCE here,
/// derive the touched-partition set from it, then stream only the
/// touched subset of the planned snapshot segments through
/// [`snapshot_segment_stream`] with this same `exclusion_set` —
/// without re-reading the delta. [`stream_all_cold_parts`] is the
/// unsplit recomposition of the two halves.
pub struct ResolvedLogTiers {
    /// Resolved cold log rows, projected to the output schema
    /// (`row_uuid` + user cols). Empty batch when the logs resolve to
    /// nothing.
    pub resolved: RecordBatch,
    /// Exclusion set built from the UNFILTERED logs with the resolved
    /// row_uuids folded in — apply to any snapshot-tier scan of the
    /// same plan, whole or subset.
    pub exclusion_set: HashSet<String>,
}

/// Run phases 1 + 2 of an all-cold merge read: probe the cold log
/// arms, compose the (resolved batch, exclusion set) pair, and project
/// the resolved rows to the output schema. Records `resolved_rows` on
/// the current span (the `*_parts` instrument span when called from
/// [`stream_all_cold_parts`]).
///
/// Same all-cold contract as [`stream_all_cold_parts`]: a hot plan
/// fails fast with [`MergeError::InvalidPlan`] — this entry never
/// probes the hot tier, so accepting one would silently drop hot
/// upserts and delete-exclusions.
///
/// `plan.base_cold_storage`, when set, is folded in underneath (CHA-178,
/// moved here in CHA-531 so the field is honoured on every plan this
/// entry accepts). **A split consumer must enumerate the base's delete
/// segments itself.** The fold runs [`filter_live_rows`] over the base,
/// so a base tombstone reaches `exclusion_set` but never `resolved` —
/// which is complete for a full-stream consumer, since the exclusion set
/// covers the whole snapshot leg, and incomplete for one that streams
/// only a touched subset. `snapshot_op`'s carry-forward writer is the
/// latter: it reads `base.cold.persist.delete_segments` directly so the
/// base's deletes still mark their partitions touched.
pub async fn resolve_log_tiers<'a, D, L>(
    req: &MergeReadRequest<'a, D, L>,
) -> Result<ResolvedLogTiers, MergeError>
where
    D: Sync,
    L: DlDriver + ?Sized,
{
    if req.plan.hot_storage.is_some() {
        return Err(MergeError::InvalidPlan(
            "all-cold merge entry requires an all-cold plan (hot_storage == None)".to_string(),
        ));
    }
    let user_cols: Vec<&str> = req
        .user_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let log_schemas = cold_persist_schemas(req.user_schema);
    // Only the identity entry restricts the log tiers; accelerator entries are
    // consumed by the snapshot scan.
    let row_uuids = identity_row_uuids(
        req.seeks.as_deref(),
        req.filter.is_some_and(|f| !f.is_empty()),
    )?;

    let (resolved, exclusion_set) = build_cold_resolved_and_exclusion_set(
        req.plan,
        req.dl,
        &user_cols,
        req.user_schema,
        &log_schemas,
        req.snapshot,
        row_uuids.as_deref(),
    )
    .await?;
    // The cold resolve arrives UNFILTERED; the exclusion set above was derived
    // from those unfiltered rows. Apply the user filter as the single
    // DataFusion residual after the dedup, before projection.
    let resolved = apply_resolved_residual(&req.dl.derive_session(), req.filter, resolved).await?;
    let resolved = project_resolved(resolved, req.user_schema)?;

    // CHA-178: fold the parent (base) cold source in below the child. The
    // all-cold `resolved` is already projected to the output schema, so the
    // parent's resolved batch is projected to match before the concat.
    //
    // CHA-531: this fold lives here rather than in each caller so that
    // `base_cold_storage` means the same thing on every plan this entry
    // accepts. It used to sit in `stream_all_cold_parts` alone, which made the
    // field a silent no-op for the snapshot writer's carry-forward delta —
    // dropping a fork parent's post-snapshot persist tail.
    let (resolved, exclusion_set) = fold_base_if_present(
        req.plan,
        req.dl,
        &user_cols,
        req.user_schema,
        &log_schemas,
        req.filter,
        row_uuids.as_deref(),
        resolved,
        exclusion_set,
        true,
    )
    .await?;
    tracing::Span::current().record("resolved_rows", resolved.num_rows() as i64);
    Ok(ResolvedLogTiers {
        resolved,
        exclusion_set,
    })
}

/// Keep only rows whose `row_uuid` is in the ids restriction — the
/// ByPlan per-batch form of the `l.row_uuid IN (...)` the ByCompletion
/// scan expresses in SQL.
fn filter_to_included_row_uuids(
    batch: &RecordBatch,
    included: &HashSet<String>,
) -> Result<RecordBatch, MergeError> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let col = resolve::string_column(batch, "row_uuid")?;
    let keep: arrow::array::BooleanArray = (0..batch.num_rows())
        .map(|i| Some(included.contains(col.value(i))))
        .collect();
    arrow::compute::filter_record_batch(batch, &keep).map_err(MergeError::from)
}

/// Drop rows whose `row_uuid` is in the exclusion set — the ByPlan
/// per-batch form of the exclusion the ByCompletion path expresses as
/// an in-plan anti-join. Returns the batch unchanged when the set is
/// empty.
pub(crate) fn filter_excluded_row_uuids(
    batch: &RecordBatch,
    exclusion_set: &HashSet<String>,
) -> Result<RecordBatch, MergeError> {
    if exclusion_set.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let col = resolve::string_column(batch, "row_uuid")?;
    let keep: arrow::array::BooleanArray = (0..batch.num_rows())
        .map(|i| Some(!exclusion_set.contains(col.value(i))))
        .collect();
    arrow::compute::filter_record_batch(batch, &keep).map_err(MergeError::from)
}

/// Per-read tuning for the snapshot stream, threaded from
/// [`MergeReadRequest`]. Bundled so [`snapshot_segment_stream`] stays
/// under the argument-count lint.
pub struct SnapshotStreamTuning {
    /// See [`MergeReadRequest::segment_read_concurrency`].
    pub segment_read_concurrency: usize,
    /// See [`MergeReadRequest::snapshot_prune_min_segments`].
    pub snapshot_prune_min_segments: usize,
    /// See [`MergeReadRequest::segment_order`].
    pub segment_order: SegmentOrder,
}

/// Phase 3 of `stream_merged`: prune snapshot segments by user filter
/// (snapshot-tier only per ADR 0022), then stream the surviving segments
/// with bounded concurrency, applying the exclusion-set filter and the
/// SQL filter per batch and projecting to the output schema.
///
/// Segment reads are independent by construction — the exclusion set is fully
/// built before this fn is called, each read hits its own cold-storage file,
/// and clients do not observe segment order — so `segment_read_concurrency`
/// worth of IO can overlap while one batch is being filtered + yielded.
///
/// The caller may pass a `SnapshotPlan` holding a SUBSET of the planned
/// segments (carry-forward's touched partitions, in original plan order)
/// together with the exclusion set a prior [`resolve_log_tiers`] produced. The
/// exclusion set must stay the one built from the UNFILTERED logs regardless of
/// how the segment list is narrowed.
#[allow(clippy::too_many_arguments)]
pub fn snapshot_segment_stream<'a, L>(
    snapshot_plan: &'a SnapshotPlan,
    dl: &'a L,
    user_schema: &'a SchemaRef,
    full_schema: &'a SchemaRef,
    filter: Option<&'a str>,
    // Owned — moved into the returned 'a stream.
    seeks: SnapshotSeeks,
    exclusion_set: HashSet<String>,
    tuning: SnapshotStreamTuning,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, MergeError>> + Send + 'a>>
where
    L: DlDriver + ?Sized,
{
    let SnapshotStreamTuning {
        segment_read_concurrency,
        snapshot_prune_min_segments,
        segment_order,
    } = tuning;
    // `full_decode_schema` is the unprojected schema the snapshot cache decodes
    // against, so one cached entry serves any projection.
    let out_schema = snapshot_read_schema(user_schema);
    let full_decode_schema = snapshot_read_schema(full_schema);
    let user_cols: Vec<String> = user_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    Box::pin(async_stream::try_stream! {
        // Segment pruning by user filter is snapshot-tier ONLY (ADR 0022): the
        // persist tier must read all segments unfiltered to preserve the
        // exclusion-set invariant.
        //
        // The pruning predicate is the full-plan of the filter — the same
        // filter string the residual `FilterExec` inside the scan derives from —
        // so the two cannot diverge: a segment pruning skips holds no residual
        // match. Pruning keeps-all on any build failure, since it is only an
        // optimization.
        let pruning_predicate = if snapshot_plan.segments.len() > snapshot_prune_min_segments {
            match filter {
                Some(f) if !f.is_empty() => {
                    full_plan_predicate(&dl.derive_session(), f, user_schema)
                        .await
                        .ok()
                }
                _ => None,
            }
        } else {
            None
        };
        let pruned_indices = penca_dl::stats::prune_segments_by_stats(
            &snapshot_plan.segments,
            |s| s.statistics.as_slice(),
            user_schema,
            pruning_predicate.as_ref(),
        );
        let segments_total = snapshot_plan.segments.len();
        let segments_pruned = segments_total - pruned_indices.len();
        tracing::info!(
            target: "penca_merge::snapshot_pruning",
            segments_total,
            segments_pruned,
            "snapshot segment pruning"
        );

        let segments: Vec<_> = pruned_indices
            .into_iter()
            .map(|i| snapshot_plan.segments[i].clone())
            .collect();
        if segments.is_empty() {
            return;
        }

        // ByCompletion (queries): the exclusion anti-join and residual
        // filter are expressed in the scan SQL. The filter is never
        // pushed into the format read (ADR 0023).
        //
        // ByPlan (snapshot writer): the in-plan NOT IN builds the
        // anti-join hash table over the SNAPSHOT side (CollectLeft
        // LeftAnti) — materializing the prior snapshot and destroying
        // plan order. Scan WITHOUT the join and apply the exclusion set
        // per batch here instead; the set is already resident (O(delta))
        // and the scan plan stays a bare streaming projection.
        let user_col_refs: Vec<&str> = user_cols.iter().map(String::as_str).collect();
        match segment_order {
            SegmentOrder::ByCompletion => {
                let exclusion: Vec<String> = exclusion_set.into_iter().collect();
                let sql = build_cold_snapshot_scan::<DfDialect>(
                    SNAPSHOT_TABLE,
                    EXCLUSION_TABLE,
                    &user_col_refs,
                    filter,
                    seeks.identity(),
                );
                // A segment carrying the matching sidecar(s) does a selective,
                // offset-intersected seek instead of a full scan. The sql still
                // carries the `row_uuid IN` residual + user filter for
                // exactness (ADR 0023): the seek is selection, never the answer.
                let mut stream = dl
                    .scan_snapshot(
                        &segments,
                        &full_decode_schema,
                        &out_schema,
                        &exclusion,
                        &sql,
                        segment_read_concurrency,
                        segment_order,
                        seeks.to_scan_specs(),
                    )
                    .await?;
                while let Some(batch) = stream.try_next().await? {
                    if batch.num_rows() > 0 {
                        yield batch;
                    }
                }
            }
            SegmentOrder::ByPlan => {
                let sql = build_cold_snapshot_scan_plain::<DfDialect>(
                    SNAPSHOT_TABLE,
                    &user_col_refs,
                    filter,
                );
                let mut stream = dl
                    .scan_snapshot(
                        &segments,
                        &full_decode_schema,
                        &out_schema,
                        &[],
                        &sql,
                        segment_read_concurrency,
                        segment_order,
                        // ByPlan is the snapshot-writer path — no point lookups.
                        None,
                    )
                    .await?;
                // Hot loop: aggregate the per-batch drops and emit one
                // summary event at exhaustion. The two filters are
                // counted separately — over/under-exclusion is the
                // diagnostic for snapshot row-count mismatches, and on a
                // restricted read the ids drops would otherwise swamp it.
                let mut batches: u64 = 0;
                let mut rows_excluded_total: u64 = 0;
                let mut rows_ids_filtered_total: u64 = 0;
                // `build_cold_snapshot_scan_plain` carries no ids restriction in
                // its SQL; apply it per batch so a restricted ByPlan read cannot
                // over-return.
                let included: Option<HashSet<String>> = seeks.identity_strings();
                while let Some(batch) = stream.try_next().await? {
                    let rows_before = batch.num_rows();
                    let batch = filter_excluded_row_uuids(&batch, &exclusion_set)?;
                    rows_excluded_total += (rows_before - batch.num_rows()) as u64;
                    let rows_after_exclusion = batch.num_rows();
                    let batch = match &included {
                        Some(included) => filter_to_included_row_uuids(&batch, included)?,
                        None => batch,
                    };
                    rows_ids_filtered_total +=
                        (rows_after_exclusion - batch.num_rows()) as u64;
                    batches += 1;
                    if batch.num_rows() > 0 {
                        yield batch;
                    }
                }
                tracing::debug!(
                    target: "penca_merge::snapshot_scan",
                    batches,
                    rows_excluded_total,
                    rows_ids_filtered_total,
                    "by-plan snapshot scan complete"
                );
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::resolve::string_column;
    use super::schema::resolved_schema;
    use super::schema::test_fixtures::{resolved_batch_nullable, test_user_schema};
    use super::*;

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::common::DFSchema;
    use datafusion::execution::context::SessionContext;
    use datafusion::sql::TableReference;
    use futures_util::TryStreamExt;
    use penca_core::{
        BaseColdStorage, ColdStoragePlan, Format, HotStoragePlan, PersistPlan, PersistSegment,
        SnapshotSegment,
    };
    use penca_dl::driver::DlError;
    use penca_dl::schema::LogSchemas;

    // A cross-type compare (`Int32` column vs `Int64` literal) is the canonical
    // case the planner's TypeCoercion must fix — it errors at eval otherwise.
    // Non-nullable fields exercise the all-nullable planning schema (the null
    // dummy row would fail `RecordBatch::try_new` otherwise).
    #[tokio::test]
    async fn full_plan_predicate_coerces_cross_type() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let pred = full_plan_predicate(&SessionContext::new(), "count > 5", &schema)
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 6, 10])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let out = apply_predicate(&batch, &pred);

        assert_eq!(out.num_rows(), 2, "count > 5 should keep 6 and 10");
        let names: Vec<&str> = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(names, vec!["b", "c"]);
    }

    // A filter the optimizer folds to a constant drops the FilterExec
    // (always-true → scan only; always-false → EmptyExec). full_plan_predicate
    // must return a keep-all / drop-all predicate, not error.
    #[tokio::test]
    async fn full_plan_predicate_handles_constant_fold() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let keep_all = full_plan_predicate(&SessionContext::new(), "1 = 1", &schema)
            .await
            .unwrap();
        assert_eq!(apply_predicate(&batch, &keep_all).num_rows(), 3);

        let drop_all = full_plan_predicate(&SessionContext::new(), "1 = 2", &schema)
            .await
            .unwrap();
        assert_eq!(apply_predicate(&batch, &drop_all).num_rows(), 0);
    }

    // full_plan_predicate must plan on the PASSED session (the driver's
    // template-derived session) rather than a fresh SessionContext::new().
    // Observable discriminator: it registers its planning table `l` into the
    // session it uses, so after the call the passed session must carry `l`.
    #[tokio::test]
    async fn full_plan_predicate_plans_on_the_passed_session() {
        let session = SessionContext::new();
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let _ = full_plan_predicate(&session, "value > 5", &schema)
            .await
            .unwrap();
        assert!(
            session.table_exist("l").unwrap(),
            "full_plan_predicate must register its planning table `l` into the \
             passed session, not a fresh SessionContext::new()",
        );
    }

    /// Evaluate a compiled physical predicate against a batch, exercising
    /// `full_plan_predicate`'s coercion / constant-fold — which drives snapshot
    /// segment pruning.
    fn apply_predicate(
        batch: &RecordBatch,
        predicate: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    ) -> RecordBatch {
        use arrow::array::{Array, BooleanArray};
        let array = predicate
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let mask = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        arrow::compute::filter_record_batch(batch, mask).unwrap()
    }

    // Isolates the warm cost of `SessionContext::new()` (the per-merge context
    // the snapshot filter parse builds) vs a cached `parse_sql_expr` — the
    // saving a process-wide cached context buys. Run with:
    //   cargo test -p penca-merge bench_snapshot_parse_ctx -- --ignored --nocapture
    #[test]
    #[ignore = "timing microbench"]
    fn bench_snapshot_parse_ctx() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let df_schema =
            DFSchema::try_from_qualified_schema(TableReference::bare("l"), schema.as_ref())
                .unwrap();
        let filter = "l.value = 50";
        let n = 500u32;

        // Warmup (page in code, registries, allocator).
        for _ in 0..50 {
            let ctx = SessionContext::new();
            let _ = ctx.parse_sql_expr(filter, &df_schema);
        }

        // (a) SessionContext::new() alone.
        let t = Instant::now();
        for _ in 0..n {
            let ctx = SessionContext::new();
            std::hint::black_box(&ctx);
        }
        let new_us = t.elapsed().as_micros() as f64 / n as f64;

        // (b) parse on a cached ctx.
        let ctx = SessionContext::new();
        let t = Instant::now();
        for _ in 0..n {
            let e = ctx.parse_sql_expr(filter, &df_schema).unwrap();
            std::hint::black_box(e);
        }
        let cached_parse_us = t.elapsed().as_micros() as f64 / n as f64;

        // (c) new()+parse each iter — the per-merge behavior.
        let t = Instant::now();
        for _ in 0..n {
            let ctx = SessionContext::new();
            let e = ctx.parse_sql_expr(filter, &df_schema).unwrap();
            std::hint::black_box(e);
        }
        let combined_us = t.elapsed().as_micros() as f64 / n as f64;

        println!("\n=== CHA-353 lever-1 microbench (warm, n={n}) ===");
        println!("(a) SessionContext::new() alone     : {new_us:8.1} µs/call");
        println!("(b) parse on cached ctx (lever-1)   : {cached_parse_us:8.1} µs/call");
        println!("(c) new()+parse (current per-merge) : {combined_us:8.1} µs/call");
        println!(
            "lever-1 warm saving per merge       : {:8.1} µs/call\n",
            combined_us - cached_parse_us
        );
    }

    // Every method returns nothing, so hot resolve stays empty and the tests
    // below exercise the cold-only arms.
    struct MockDriver;

    impl DbDriver for MockDriver {
        type Row = PgRow;

        async fn execute(&self, _query: &str) -> Result<Vec<PgRow>, sqlx::Error> {
            Ok(vec![])
        }

        async fn execute_no_result(&self, _query: &str) -> Result<(), sqlx::Error> {
            Ok(())
        }

        async fn execute_many(&self, _queries: &[String]) -> Result<(), sqlx::Error> {
            Ok(())
        }

        async fn execute_params(
            &self,
            _query: &str,
            _params: &[penca_db::driver::SqlValue],
        ) -> Result<Vec<PgRow>, sqlx::Error> {
            Ok(vec![])
        }

        async fn execute_no_result_params(
            &self,
            _query: &str,
            _params: &[penca_db::driver::SqlValue],
        ) -> Result<(), sqlx::Error> {
            Ok(())
        }

        async fn fetch_optional(
            &self,
            _query: &str,
            _params: &[penca_db::driver::SqlValue],
        ) -> Result<Option<PgRow>, sqlx::Error> {
            Ok(None)
        }

        async fn close(&self) {}

        fn fetch_stream<'a>(
            &'a self,
            _query: &'a str,
            _params: &'a [penca_db::driver::SqlValue],
        ) -> Pin<Box<dyn Stream<Item = Result<PgRow, sqlx::Error>> + Send + 'a>> {
            Box::pin(futures_util::stream::empty())
        }
    }

    #[derive(Default)]
    struct MockDlDriver {
        resolved: Option<RecordBatch>,
        snapshots: HashMap<String, RecordBatch>,
        /// The (pruned) segment uuids handed to each scan_snapshot — the
        /// pruning tests assert which segments survived filter-based pruning.
        snapshot_read_log: Arc<std::sync::Mutex<Vec<String>>>,
        /// The exclusion set handed to scan_snapshot, so a test can assert
        /// stream_merged folded the resolved row_uuids into it (the provider
        /// applies the anti-join, covered in penca-dl).
        scan_exclusion_log: Arc<std::sync::Mutex<Vec<String>>>,
        /// The SQL stream_merged built. The provider runs it in production;
        /// this lets a merge test guard the table names + user cols + residual
        /// the merge layer emits, otherwise unexercised across the crate
        /// boundary.
        scan_sql_log: Arc<std::sync::Mutex<Vec<String>>>,
        /// The resolve/exclusion SQL handed to execute_sql, so the
        /// sibling-equivalence test can pin log-tier pushdown parity
        /// (filter + ids) between stream_merged and stream_all_cold.
        exec_sql_log: Arc<std::sync::Mutex<Vec<String>>>,
        /// The seeks handed to each scan_snapshot, so a test pins that the
        /// ByCompletion arm derives them from row_uuids (and ByPlan passes
        /// None) — else a dropped derivation silently reverts to the full-scan
        /// path with still-correct rows.
        seeks_log: Arc<std::sync::Mutex<Vec<Option<Vec<SeekSpec>>>>>,
    }

    impl MockDlDriver {
        fn with_resolved(mut self, batch: RecordBatch) -> Self {
            self.resolved = Some(batch);
            self
        }

        fn with_snapshot(mut self, segment_uuid: &str, batch: RecordBatch) -> Self {
            self.snapshots.insert(segment_uuid.to_string(), batch);
            self
        }

        fn recorded_snapshot_reads(&self) -> Vec<String> {
            self.snapshot_read_log.lock().unwrap().clone()
        }

        fn recorded_scan_exclusion(&self) -> Vec<String> {
            self.scan_exclusion_log.lock().unwrap().clone()
        }

        fn recorded_scan_sql(&self) -> Vec<String> {
            self.scan_sql_log.lock().unwrap().clone()
        }

        fn recorded_exec_sql(&self) -> Vec<String> {
            self.exec_sql_log.lock().unwrap().clone()
        }

        fn recorded_seeks(&self) -> Vec<Option<Vec<SeekSpec>>> {
            self.seeks_log.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DlDriver for MockDlDriver {
        fn derive_session(&self) -> SessionContext {
            SessionContext::new()
        }

        async fn execute_sql(
            &self,
            _plan: &ColdStoragePlan,
            sql: &str,
            _log_schemas: &LogSchemas,
        ) -> Result<RecordBatch, DlError> {
            self.exec_sql_log.lock().unwrap().push(sql.to_string());
            Ok(match &self.resolved {
                Some(b) => b.clone(),
                None => RecordBatch::new_empty(resolved_schema(&test_user_schema())),
            })
        }

        async fn scan_snapshot(
            &self,
            segments: &[SnapshotSegment],
            _full_schema: &SchemaRef,
            out_schema: &SchemaRef,
            exclusion: &[String],
            sql: &str,
            _segment_read_concurrency: usize,
            _order: SegmentOrder,
            seeks: Option<Arc<Vec<SeekSpec>>>,
        ) -> Result<datafusion::execution::SendableRecordBatchStream, DlError> {
            // The exclusion anti-join + residual are the provider+SQL's job in
            // production (covered in penca-dl); the mock deliberately does not
            // apply them — it tests stream_merged's orchestration only.
            self.scan_sql_log.lock().unwrap().push(sql.to_string());
            self.seeks_log
                .lock()
                .unwrap()
                .push(seeks.as_ref().map(|k| (**k).clone()));
            self.scan_exclusion_log
                .lock()
                .unwrap()
                .extend(exclusion.iter().cloned());
            let mut reads = self.snapshot_read_log.lock().unwrap();
            let mut batches: Vec<Result<RecordBatch, datafusion::error::DataFusionError>> =
                Vec::new();
            for seg in segments {
                reads.push(seg.table_snapshot_segment_uuid.clone());
                if let Some(b) = self.snapshots.get(&seg.table_snapshot_segment_uuid) {
                    batches.push(Ok(b.clone()));
                }
            }
            drop(reads);
            Ok(Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    out_schema.clone(),
                    futures_util::stream::iter(batches),
                ),
            ))
        }
    }

    /// A resolved batch of live (`is_delete = false`) upsert rows.
    fn make_resolved_batch(
        row_uuids: &[&str],
        names: &[&str],
        values: &[i32],
        committed_ats: &[i64],
    ) -> RecordBatch {
        let is_deletes = vec![false; row_uuids.len()];
        make_resolved_batch_flagged(row_uuids, names, values, committed_ats, &is_deletes)
    }

    /// Like [`make_resolved_batch`] but with explicit `is_delete` flags — a
    /// `true` row is a winning tombstone: it contributes its row_uuid to the
    /// exclusion set but is dropped from the live delta.
    fn make_resolved_batch_flagged(
        row_uuids: &[&str],
        names: &[&str],
        values: &[i32],
        committed_ats: &[i64],
        is_deletes: &[bool],
    ) -> RecordBatch {
        resolved_batch_nullable(
            row_uuids,
            &names.iter().copied().map(Some).collect::<Vec<_>>(),
            &values.iter().copied().map(Some).collect::<Vec<_>>(),
            committed_ats,
            is_deletes,
        )
    }

    fn make_snapshot_batch(row_uuids: &[&str], names: &[&str], values: &[i32]) -> RecordBatch {
        let schema = snapshot_read_schema(&test_user_schema());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(row_uuids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Int32Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn snapshot_segment(uuid: &str) -> SnapshotSegment {
        SnapshotSegment {
            table_snapshot_segment_uuid: uuid.to_string(),
            table_snapshot_uuid: "snap1".to_string(),
            uri: format!("s3://test/{uuid}"),
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
        }
    }

    fn plan_with_snapshot(segments: Vec<SnapshotSegment>) -> Plan {
        Plan {
            hot_storage: None,
            cold_storage: Some(ColdStoragePlan {
                snapshot: Some(SnapshotPlan {
                    segments,
                    snapshotted_at_micros: 1000,
                    ..Default::default()
                }),
                persist: None,
            }),
            base_cold_storage: None,
        }
    }

    fn persist_segment(uuid: &str) -> PersistSegment {
        PersistSegment {
            segment_uuid: uuid.to_string(),
            uri: format!("s3://test/{uuid}"),
            format: Format::Parquet,
            row_count: 0,
            size_bytes: 0,
            metadata_json: String::new(),
            statistics: Vec::new(),
            offset: None,
            length: None,
        }
    }

    /// Like [`plan_with_snapshot`] but carries a non-empty `persist`
    /// plan so the cold-tier mock is consulted. `resolve_cold` short-circuits
    /// when `cold_plan.persist.is_none()`, so cross-tier tests that feed cold
    /// rows via the mock must model a present persist tier.
    fn plan_with_snapshot_and_persist(segments: Vec<SnapshotSegment>) -> Plan {
        Plan {
            hot_storage: None,
            cold_storage: Some(ColdStoragePlan {
                snapshot: Some(SnapshotPlan {
                    segments,
                    snapshotted_at_micros: 1000,
                    ..Default::default()
                }),
                persist: Some(PersistPlan {
                    upsert_segments: vec![persist_segment("p1")],
                    delete_segments: vec![],
                    committed_at: None,
                    commit_seq: None,
                }),
            }),
            base_cold_storage: None,
        }
    }

    async fn collect_stream(
        stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, MergeError>> + Send + '_>>,
    ) -> Result<Vec<RecordBatch>, MergeError> {
        stream.try_collect().await
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn all_row_uuids(batches: &[RecordBatch]) -> Vec<String> {
        let mut uuids = Vec::new();
        for batch in batches {
            let col = string_column(batch, "row_uuid").unwrap();
            for i in 0..batch.num_rows() {
                uuids.push(col.value(i).to_string());
            }
        }
        uuids
    }

    /// A base (parent) cold source with a persist tier so `resolve_cold` reaches
    /// the mock driver — it short-circuits to an empty batch when
    /// `persist.is_none()`, which would starve `fold_in_base_cold_source`.
    fn base_cold(commit_seq_ceiling: i64) -> BaseColdStorage {
        BaseColdStorage {
            cold: ColdStoragePlan {
                snapshot: None,
                persist: Some(PersistPlan {
                    upsert_segments: vec![persist_segment("bp1")],
                    delete_segments: vec![],
                    committed_at: None,
                    commit_seq: None,
                }),
            },
            commit_seq_ceiling,
        }
    }

    fn str_set(uuids: &[&str]) -> HashSet<String> {
        uuids.iter().map(|s| s.to_string()).collect()
    }

    /// The base's exclusion contribution is EVERY parent-touched row_uuid —
    /// upsert-winner AND tombstone-winner — but a tombstone never reaches the
    /// output. Locks the two-arm split: exclusion from the full resolve,
    /// live delta from the `is_delete = false` subset.
    #[tokio::test]
    async fn fold_base_exclusion_includes_tombstone_but_output_drops_it() {
        let user_schema = test_user_schema();
        let base = base_cold(i64::MAX);
        let dl = MockDlDriver::default().with_resolved(make_resolved_batch_flagged(
            &["u_a", "u_d"],
            &["a", "d"],
            &[1, 0],
            &[100, 200],
            &[false, true], // u_a live upsert, u_d winning tombstone
        ));
        let child = RecordBatch::new_empty(resolved_schema(&user_schema));
        let (combined, exclusion) = fold_in_base_cold_source(
            &base,
            &dl,
            &["name", "value"],
            &user_schema,
            &cold_persist_schemas(&user_schema),
            None,
            None,
            child,
            HashSet::new(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            all_row_uuids(std::slice::from_ref(&combined)),
            vec!["u_a".to_string()],
            "tombstone u_d must not surface in the folded output",
        );
        assert_eq!(
            exclusion,
            str_set(&["u_a", "u_d"]),
            "both the live upsert AND the tombstone shadow the base snapshot",
        );
    }

    /// A base row whose row_uuid is already in the CHILD exclusion set (the
    /// child holds a newer version) is anti-joined out of the fold; unshadowed
    /// base rows survive.
    #[tokio::test]
    async fn fold_base_drops_rows_shadowed_by_child() {
        let user_schema = test_user_schema();
        let base = base_cold(i64::MAX);
        let dl = MockDlDriver::default().with_resolved(make_resolved_batch_flagged(
            &["u_a", "u_b"],
            &["a", "b"],
            &[1, 2],
            &[100, 100],
            &[false, false],
        ));
        let child = RecordBatch::new_empty(resolved_schema(&user_schema));
        let (combined, exclusion) = fold_in_base_cold_source(
            &base,
            &dl,
            &["name", "value"],
            &user_schema,
            &cold_persist_schemas(&user_schema),
            None,
            None,
            child,
            str_set(&["u_a"]), // child already touched u_a
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            all_row_uuids(std::slice::from_ref(&combined)),
            vec!["u_b".to_string()],
            "u_a is shadowed by the child and must drop from the fold",
        );
        assert!(exclusion.contains("u_a") && exclusion.contains("u_b"));
    }

    /// All-cold path (`project_base_to_output = true`): the child was already
    /// residual-filtered upstream, so the base is filtered HERE. A base row
    /// failing the filter drops from the output but STILL shadows the base
    /// snapshot, because the exclusion set is unfiltered.
    #[tokio::test]
    async fn fold_base_all_cold_path_filters_output_but_not_exclusion() {
        let user_schema = test_user_schema();
        let base = base_cold(i64::MAX);
        let dl = MockDlDriver::default().with_resolved(make_resolved_batch_flagged(
            &["u_hi", "u_lo"],
            &["hi", "lo"],
            &[5, 1], // u_hi passes `value > 3`, u_lo fails it
            &[100, 100],
            &[false, false],
        ));
        // All-cold child is at the projected output schema.
        let child = RecordBatch::new_empty(snapshot_read_schema(&user_schema));
        let (combined, exclusion) = fold_in_base_cold_source(
            &base,
            &dl,
            &["name", "value"],
            &user_schema,
            &cold_persist_schemas(&user_schema),
            Some("value > 3"),
            None,
            child,
            HashSet::new(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            all_row_uuids(std::slice::from_ref(&combined)),
            vec!["u_hi".to_string()],
            "the residual drops u_lo from the output on the all-cold path",
        );
        assert!(
            exclusion.contains("u_hi") && exclusion.contains("u_lo"),
            "u_lo still shadows the base snapshot even though it failed the filter",
        );
    }

    /// Mixed path (`project_base_to_output = false`): the base is folded in
    /// UNFILTERED — `assemble_parts` applies the residual to the combined batch
    /// after the fold. A base row failing the filter must still be present here.
    #[tokio::test]
    async fn fold_base_mixed_path_leaves_base_unfiltered() {
        let user_schema = test_user_schema();
        let base = base_cold(i64::MAX);
        let dl = MockDlDriver::default().with_resolved(make_resolved_batch_flagged(
            &["u_lo"],
            &["lo"],
            &[1], // fails `value > 3`, but must survive the fold
            &[100],
            &[false],
        ));
        let child = RecordBatch::new_empty(resolved_schema(&user_schema));
        let (combined, _exclusion) = fold_in_base_cold_source(
            &base,
            &dl,
            &["name", "value"],
            &user_schema,
            &cold_persist_schemas(&user_schema),
            Some("value > 3"),
            None,
            child,
            HashSet::new(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            all_row_uuids(std::slice::from_ref(&combined)),
            vec!["u_lo".to_string()],
            "mixed path folds the base unfiltered; the residual is deferred to assemble_parts",
        );
    }

    #[tokio::test]
    async fn empty_plan_yields_empty() {
        let plan = Plan {
            hot_storage: None,
            cold_storage: None,
            base_cold_storage: None,
        };
        let driver = MockDriver;
        let dl = MockDlDriver::default();
        let schema = test_user_schema();

        let batches = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();
        assert_eq!(total_rows(&batches), 0);
    }

    #[tokio::test]
    async fn snapshot_only_passthrough() {
        let plan = plan_with_snapshot(vec![snapshot_segment("seg1")]);
        let driver = MockDriver;
        let dl = MockDlDriver::default().with_snapshot(
            "seg1",
            make_snapshot_batch(&["r1", "r2"], &["alice", "bob"], &[10, 20]),
        );
        let schema = test_user_schema();

        let batches = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();
        assert_eq!(total_rows(&batches), 2);
        let uuids = all_row_uuids(&batches);
        assert!(uuids.contains(&"r1".to_string()));
        assert!(uuids.contains(&"r2".to_string()));
    }

    #[tokio::test]
    async fn by_completion_scan_receives_seek_keys_from_row_uuids() {
        // A restricted (ids) read must hand the stringified row_uuids to
        // scan_snapshot as seek_keys so the provider can take the index-seek
        // path — a dropped derivation would silently revert to full scan with
        // still-correct rows.
        let plan = plan_with_snapshot(vec![snapshot_segment("seg1")]);
        let driver = MockDriver;
        let dl = MockDlDriver::default()
            .with_snapshot("seg1", make_snapshot_batch(&["r1"], &["alice"], &[10]));
        let schema = test_user_schema();
        let uuid = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: Some(vec![IndexSeek::identity(&[uuid])]),
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();

        assert_eq!(
            dl.recorded_seeks(),
            vec![Some(vec![SeekSpec {
                index_uuid: None,
                key_columns: vec![],
                tuples: vec![vec![uuid.to_string()]],
            }])],
            "ByCompletion must pass the identity entry down as a seek spec",
        );

        // Negative half: an unrestricted read (no ids) threads None down, so the
        // provider keeps the full-scan path.
        let dl_unrestricted = MockDlDriver::default()
            .with_snapshot("seg1", make_snapshot_batch(&["r1"], &["alice"], &[10]));
        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl_unrestricted,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();
        assert_eq!(
            dl_unrestricted.recorded_seeks(),
            vec![None],
            "an unrestricted read passes no seek_keys",
        );
    }

    #[tokio::test]
    async fn resolved_row_emitted_alongside_snapshot_row() {
        let plan = plan_with_snapshot_and_persist(vec![snapshot_segment("seg1")]);
        let driver = MockDriver;
        let dl = MockDlDriver::default()
            .with_snapshot("seg1", make_snapshot_batch(&["r1"], &["alice"], &[10]))
            .with_resolved(make_resolved_batch(&["r2"], &["bob"], &[20], &[2000]));
        let schema = test_user_schema();

        let batches = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();
        assert_eq!(total_rows(&batches), 2);
        let uuids = all_row_uuids(&batches);
        assert!(uuids.contains(&"r1".to_string()));
        assert!(uuids.contains(&"r2".to_string()));
    }

    #[tokio::test]
    async fn resolved_uuid_passed_to_snapshot_exclusion() {
        // stream_merged folds every resolved row_uuid into the exclusion set
        // (they shadow same-uuid snapshot rows) and hands it to scan_snapshot;
        // the provider applies the anti-join in production. This asserts the
        // seam only: the resolved uuid reaches scan_snapshot's exclusion.
        let plan = plan_with_snapshot_and_persist(vec![snapshot_segment("seg1")]);
        let driver = MockDriver;
        let dl = MockDlDriver::default()
            .with_snapshot("seg1", make_snapshot_batch(&["r1"], &["alice"], &[10]))
            .with_resolved(make_resolved_batch(&["r1"], &["alice_v2"], &[99], &[2000]));
        let schema = test_user_schema();

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();

        assert!(
            dl.recorded_scan_exclusion().contains(&"r1".to_string()),
            "resolved r1 must be folded into the exclusion set handed to \
             scan_snapshot; got {:?}",
            dl.recorded_scan_exclusion(),
        );
    }

    #[tokio::test]
    async fn by_plan_parts_apply_exclusion_per_batch() {
        // The ByPlan path scans WITHOUT the in-plan anti-join (which would
        // build its hash table over the snapshot side) and drops excluded
        // row_uuids per batch here in penca-merge. The mock never applies
        // exclusions itself, so r1 disappearing from the snapshot stream
        // proves the per-batch filter ran.
        let plan = plan_with_snapshot_and_persist(vec![snapshot_segment("seg1")]);
        let driver = MockDriver;
        let dl = MockDlDriver::default()
            .with_snapshot(
                "seg1",
                make_snapshot_batch(&["r1", "r2"], &["alice", "bob"], &[10, 20]),
            )
            .with_resolved(make_resolved_batch(&["r1"], &["alice_v2"], &[99], &[2000]));
        let schema = test_user_schema();

        let parts = stream_merged_parts(MergeReadRequest {
            segment_order: SegmentOrder::ByPlan,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        })
        .await
        .unwrap();

        let resolved_uuids = all_row_uuids(std::slice::from_ref(&parts.resolved));
        assert_eq!(resolved_uuids, vec!["r1"], "delta carries the new version");

        let snapshot_batches = collect_stream(parts.snapshot_stream).await.unwrap();
        let snapshot_uuids = all_row_uuids(&snapshot_batches);
        assert_eq!(
            snapshot_uuids,
            vec!["r2"],
            "excluded r1 must be dropped per batch by stream_merged_parts"
        );

        assert!(
            dl.recorded_scan_exclusion().is_empty(),
            "ByPlan hands no exclusion to the scan (no in-plan anti-join)"
        );
        let sqls = dl.scan_sql_log.lock().unwrap().clone();
        assert!(
            sqls.iter().all(|q| !q.contains("NOT IN")),
            "ByPlan scan SQL must not carry the anti-join: {sqls:?}"
        );
    }

    #[tokio::test]
    async fn by_plan_parts_apply_ids_restriction_per_batch() {
        // The ByPlan plain scan carries no ids restriction in its SQL, so the
        // per-batch keep-filter is the sole enforcement on that arm.
        let keep = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let keep_str = keep.to_string();
        let plan = plan_with_snapshot_and_persist(vec![snapshot_segment("seg1")]);
        let driver = MockDriver;
        let dl = MockDlDriver::default().with_snapshot(
            "seg1",
            make_snapshot_batch(&[&keep_str, "r-other"], &["alice", "bob"], &[10, 20]),
        );
        let schema = test_user_schema();
        let restriction = vec![keep];

        let parts = stream_merged_parts(MergeReadRequest {
            segment_order: SegmentOrder::ByPlan,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: Some(vec![IndexSeek::identity(&restriction)]),
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        })
        .await
        .unwrap();

        let snapshot_batches = collect_stream(parts.snapshot_stream).await.unwrap();
        let snapshot_uuids = all_row_uuids(&snapshot_batches);
        assert_eq!(
            snapshot_uuids,
            vec![keep_str],
            "non-matching snapshot row must be dropped by the ids keep-filter"
        );

        let sqls = dl.scan_sql_log.lock().unwrap().clone();
        assert!(
            sqls.iter().all(|q| !q.contains("row_uuid IN (")),
            "ByPlan scan SQL must not carry the ids restriction: {sqls:?}"
        );
    }

    #[tokio::test]
    async fn scan_snapshot_sql_references_tables_filter_and_cols() {
        // Guards the merge-side SQL construction across the crate boundary:
        // the mock ignores the SQL (the provider runs it in production), so
        // this pins exactly what stream_merged emits, otherwise unexercised.
        let plan = plan_with_snapshot(vec![snapshot_segment("seg1")]);
        let driver = MockDriver;
        let dl = MockDlDriver::default()
            .with_snapshot("seg1", make_snapshot_batch(&["r1"], &["alice"], &[10]));
        let schema = test_user_schema();

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: Some("l.value > 5"),
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();

        let sqls = dl.recorded_scan_sql();
        assert_eq!(sqls.len(), 1, "exactly one snapshot scan; got {sqls:?}");
        let sql = &sqls[0];
        // Registered table names (shared consts), user col, outer-WHERE residual.
        assert!(
            sql.contains("FROM \"l\" l"),
            "scans the registered snapshot table: {sql}",
        );
        assert!(
            sql.contains("NOT IN (SELECT row_uuid FROM \"exclusion\")"),
            "anti-joins the exclusion table: {sql}",
        );
        assert!(sql.contains("l.\"value\""), "selects the user col: {sql}");
        assert!(
            sql.ends_with(" AND (l.value > 5)"),
            "residual filter at the outer WHERE: {sql}",
        );
    }

    #[tokio::test]
    async fn cross_tier_dedup_keeps_latest_committed_at() {
        // Cold has r1 with older committed_at; emulate by only feeding cold
        // (hot is empty via MockDriver). A present persist tier is required
        // for the mock to be consulted at all — snapshot-only plans
        // short-circuit resolve_cold to empty.
        let plan = Plan {
            hot_storage: None,
            cold_storage: Some(ColdStoragePlan {
                snapshot: None,
                persist: Some(PersistPlan {
                    upsert_segments: vec![persist_segment("p1")],
                    delete_segments: vec![],
                    committed_at: None,
                    commit_seq: None,
                }),
            }),
            base_cold_storage: None,
        };
        let driver = MockDriver;
        let dl = MockDlDriver::default().with_resolved(make_resolved_batch(
            &["r1", "r1"],
            &["older", "newer"],
            &[1, 2],
            &[100, 200],
        ));
        let schema = test_user_schema();

        let batches = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();
        assert_eq!(total_rows(&batches), 1);
        let name_col = string_column(&batches[0], "name").unwrap();
        assert_eq!(name_col.value(0), "newer");
    }

    #[test]
    fn cold_persist_schemas_layout() {
        // Both merge-path schemas are narrowed to exactly the columns
        // `build_cold_merge_resolved` references. The audit/compact paths use
        // the wider `cold_upsert_schema` / `cold_delete_schema`, exercised by
        // the audit-path tests.
        let user_schema = test_user_schema();
        let s = cold_persist_schemas(&user_schema);
        let upsert_cols: Vec<&str> = s
            .upsert
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        // The merge orders/filters on commit_seq_num, so both merge-path
        // schemas declare it (trailing).
        assert_eq!(
            upsert_cols,
            vec![
                "row_uuid",
                "name",
                "value",
                "commit_micros",
                "write_seq_num",
                "commit_seq_num",
            ]
        );
        let delete_cols: Vec<&str> = s
            .delete
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            delete_cols,
            vec![
                "row_uuid",
                "commit_micros",
                "write_seq_num",
                "commit_seq_num",
            ]
        );
    }

    #[test]
    fn tighten_for_hot_no_hot_max_is_identity() {
        assert_eq!(
            ReadSnapshot::AsOfMicros(100).tighten_for_hot(None),
            ReadSnapshot::AsOfMicros(100)
        );
        let open = ReadSnapshot::OpenTx {
            began_at_seq_num: 500,
            tx_uuid: uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        };
        assert_eq!(open.tighten_for_hot(None), open);
    }

    #[test]
    fn tighten_for_hot_picks_tighter_as_of() {
        assert_eq!(
            ReadSnapshot::AsOfMicros(100).tighten_for_hot(Some(50)),
            ReadSnapshot::AsOfMicros(50)
        );
        assert_eq!(
            ReadSnapshot::AsOfMicros(100).tighten_for_hot(Some(200)),
            ReadSnapshot::AsOfMicros(100)
        );
    }

    #[test]
    fn tighten_for_hot_open_tx_is_identity() {
        // OpenTx pins the SEQ axis (`commit_seq_num < began_at_seq_num`), an
        // exact bound. `hot_max` is a `commit_micros` upper bound, so there is
        // nothing to intersect against the seq bound — tighten_for_hot is
        // identity for OpenTx regardless of hot_max. The committed_at hot fence
        // still applies separately via the plan's `committed_at > hot_min`.
        let tx = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let open = ReadSnapshot::OpenTx {
            began_at_seq_num: 500,
            tx_uuid: tx,
        };
        assert_eq!(open.tighten_for_hot(Some(100)), open);
        assert_eq!(open.tighten_for_hot(Some(10_000)), open);
        assert_eq!(open.tighten_for_hot(None), open);
    }

    /// Build a SnapshotSegment whose `statistics` field is produced by
    /// `penca_dl::stats::compute_segment_statistics` over a 2-row batch
    /// bracketed by `min` and `max` on the `value` Int32 column.
    ///
    /// **The batch schema is `snapshot_read_schema(test_user_schema())` =
    /// `[row_uuid, name, value]`, not bare `test_user_schema` =
    /// `[name, value]`.** This matches the production writer path:
    /// `durable_writer.rs` runs `compute_segment_statistics` over a
    /// batch whose schema is `snapshot_read_schema(user_schema)`, while
    /// the reader at `stream_merged` Phase 3 passes bare `user_schema`. A
    /// positional stats encoding would offset the `value` lookup by one
    /// (row_uuid stats would land where reader expects `name`), and
    /// PerSegmentBuilders' silent type-mismatch fallthrough would mask
    /// the bug entirely. The wire format is keyed by column name to
    /// survive this disagreement; using the wider schema in the test
    /// fixture is what makes the unit test discriminating.
    fn snapshot_segment_with_value_stats(uuid: &str, min: i32, max: i32) -> SnapshotSegment {
        let user_schema = test_user_schema();
        let prod_schema = snapshot_read_schema(&user_schema);
        let batch = RecordBatch::try_new(
            prod_schema,
            vec![
                Arc::new(StringArray::from(vec![
                    format!("uuid-{uuid}-0"),
                    format!("uuid-{uuid}-1"),
                ])),
                Arc::new(StringArray::from(vec!["x", "x"])),
                Arc::new(Int32Array::from(vec![min, max])),
            ],
        )
        .expect("valid stats fixture batch");
        let stats = penca_dl::stats::compute_segment_statistics(&batch);
        SnapshotSegment {
            statistics: stats,
            ..snapshot_segment(uuid)
        }
    }

    #[tokio::test]
    async fn test_snapshot_pruning_skips_segments_outside_min_max() {
        // 3 SnapshotSegments with disjoint `value` stats: [0,99], [100,199],
        // [200,299]. With filter `value BETWEEN 110 AND 150` only the middle
        // segment can possibly match; snapshot pruning should skip the other
        // two and stream_merged should only fetch the middle one.
        let plan = plan_with_snapshot(vec![
            snapshot_segment_with_value_stats("seg-low", 0, 99),
            snapshot_segment_with_value_stats("seg-mid", 100, 199),
            snapshot_segment_with_value_stats("seg-high", 200, 299),
        ]);
        let driver = MockDriver;
        let dl = MockDlDriver::default();
        let schema = test_user_schema();

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: Some("value BETWEEN 110 AND 150"),
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .expect("stream_merged completes");

        let reads = dl.recorded_snapshot_reads();
        assert_eq!(
            reads.len(),
            1,
            "expected snapshot pruning to leave only the [100,199] segment readable; got reads = {reads:?}"
        );
        assert_eq!(reads, vec!["seg-mid".to_string()]);
    }

    #[tokio::test]
    async fn test_snapshot_pruning_union_stats_prune_and_read() {
        // One segment whose stats span a wide union range [0, 299] — e.g. a
        // multi-partition packed file's whole-file stats.
        //
        // Two subcases:
        //   subcase 2: filter `value > 999` can't match stats [0,299]
        //     → segment IS pruned → recorded reads == 0.
        //   subcase 1 (correctness sanity): filter `value > 200` DOES
        //     intersect [0,299] → segment IS read → recorded reads == 1.

        // subcase 2: filter pruning excludes the segment
        let plan = plan_with_snapshot(vec![snapshot_segment_with_value_stats(
            "seg-merged",
            0,
            299,
        )]);
        let driver = MockDriver;
        let dl = MockDlDriver::default();
        let schema = test_user_schema();

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: Some("value > 999"),
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .expect("stream_merged completes");

        assert_eq!(
            dl.recorded_snapshot_reads().len(),
            0,
            "segment with union stats [0,299] should be pruned by `value > 999`; got reads = {:?}",
            dl.recorded_snapshot_reads()
        );

        // subcase 1: filter intersects the segment, so it IS read
        let plan2 = plan_with_snapshot(vec![snapshot_segment_with_value_stats(
            "seg-merged",
            0,
            299,
        )]);
        let dl2 = MockDlDriver::default();

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan2,
            driver: &driver,
            dl: &dl2,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: Some("value > 200"),
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .expect("stream_merged completes");

        assert_eq!(
            dl2.recorded_snapshot_reads().len(),
            1,
            "segment with union stats [0,299] should be read for `value > 200`; got reads = {:?}",
            dl2.recorded_snapshot_reads()
        );
    }

    /// System-internal stream_merged callers (`penca-storage-meta`,
    /// `penca-sql-server` DML) always qualify their filter columns with
    /// `l.` — the alias the hot/cold SQL paths agree on. The snapshot
    /// pruning parse must accept the same qualifier so pruning stays
    /// active on those reads; if the parse rejects it the code falls back
    /// to keep-all and the pruning silently stops happening.
    #[tokio::test]
    async fn snapshot_pruning_works_with_l_qualified_filter() {
        let plan = plan_with_snapshot(vec![snapshot_segment_with_value_stats(
            "seg-merged",
            0,
            299,
        )]);
        let driver = MockDriver;
        let dl = MockDlDriver::default();
        let schema = test_user_schema();

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: Some("l.value > 999"),
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .expect("stream_merged completes");

        assert_eq!(
            dl.recorded_snapshot_reads().len(),
            0,
            "segment with stats [0,299] should be pruned by `l.value > 999`; got reads = {:?}",
            dl.recorded_snapshot_reads()
        );
    }

    /// A tombstone whose user columns are NULL — production's actual
    /// shape once a Snapshot has fenced the row's upsert out of `latest` — must
    /// flow through the read, not abort it. The table declares `name`/`value`
    /// non-nullable (`test_user_schema`), which is exactly the condition that
    /// used to make `RecordBatch::try_new` reject the resolve and wedge every
    /// later read.
    ///
    /// The schema assertion is the half that distinguishes this fix from the
    /// silent-corruption alternative: relaxing the OUTPUT contract too would
    /// also make the read succeed, while handing clients an all-nullable schema
    /// for a table they declared strict.
    #[tokio::test]
    async fn null_carrying_tombstone_resolves_without_wedging_the_read() {
        let plan = plan_with_snapshot_and_persist(vec![snapshot_segment("s1")]);
        let schema = test_user_schema();
        let dl = MockDlDriver::default()
            .with_resolved(resolved_batch_nullable(
                &["u1", "d1"],
                &[Some("a"), None],
                &[Some(1), None],
                &[100, 150],
                &[false, true],
            ))
            .with_snapshot("s1", make_snapshot_batch(&["s-row"], &["s"], &[9]));

        let batches = collect_stream(stream_all_cold(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &MockDriver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(2_000),
            filter: None,
            seeks: None,
            segment_read_concurrency: 2,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .expect("a NULL-carrying tombstone must not abort the read");

        // Static panic messages, nothing derived from `emitted` interpolated
        // into them: row_uuids are PK-derived, and CodeQL's cleartext-logging
        // rule treats an assert message as a log sink and taints anything
        // reached from them — including `.len()`, whose receiver carries the
        // taint. `assert!` evaluates its condition without printing it.
        let emitted = all_row_uuids(&batches);
        assert!(
            emitted.len() == 2,
            "expected exactly the live row and the snapshot row"
        );
        assert!(
            !emitted.contains(&"d1".to_string()),
            "the tombstone must be dropped from the live delta"
        );
        assert!(
            emitted.contains(&"u1".to_string()),
            "the live row must survive"
        );
        assert!(
            dl.recorded_scan_exclusion().contains(&"d1".to_string()),
            "the tombstone must still shadow its snapshot version"
        );
        for batch in &batches {
            assert_eq!(
                batch.schema(),
                snapshot_read_schema(&test_user_schema()),
                "the strict output contract must survive the carrier's relaxation"
            );
        }
    }

    /// For an all-cold plan, the dedicated cold path must produce the
    /// exact batches the merged path produces (whose hot arms self-skip
    /// at runtime), hand the same exclusion set to the snapshot scan,
    /// and build the same scan SQL. Pins the sibling-equivalence claim
    /// so a later edit to either entry can't silently diverge them.
    #[tokio::test]
    async fn stream_all_cold_matches_stream_merged_on_all_cold_plan() {
        let plan = plan_with_snapshot_and_persist(vec![snapshot_segment("s1")]);
        let schema = test_user_schema();
        let snapshot = ReadSnapshot::AsOfMicros(2_000);
        // Non-trivial filter + ids restriction so the recorded SQL pins
        // pushdown parity — the one dimension along which the two call
        // sites could realistically drift (everything downstream is
        // shared code by construction).
        let filter = Some("l.value > 0");
        let ids = vec![Uuid::nil()];
        // Live upserts (u1, u2) plus a winning tombstone (d1, is_delete = true).
        // d1 lands in the exclusion set but not the live delta, exercising the
        // derived-exclusion path along which the two entries could drift.
        let make_dl = || {
            MockDlDriver::default()
                .with_resolved(make_resolved_batch_flagged(
                    &["u1", "u2", "u1", "d1"],
                    &["a", "b", "a2", "d"],
                    &[1, 2, 3, 0],
                    &[100, 100, 200, 150],
                    &[false, false, false, true],
                ))
                .with_snapshot("s1", make_snapshot_batch(&["s-row"], &["s"], &[9]))
        };
        let driver = MockDriver;

        let dl_merged = make_dl();
        let merged = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl_merged,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &snapshot,
            filter,
            seeks: Some(vec![IndexSeek::identity(&ids)]),
            segment_read_concurrency: 2,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .expect("stream_merged completes");

        let dl_cold = make_dl();
        let cold = collect_stream(stream_all_cold(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl_cold,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &snapshot,
            filter,
            seeks: Some(vec![IndexSeek::identity(&ids)]),
            segment_read_concurrency: 2,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .expect("stream_all_cold completes");

        assert_eq!(merged, cold, "all-cold plan must stream identical batches");

        // The exclusion set is materialized from a HashSet, so compare
        // order-insensitively; the scan SQL must match exactly.
        let mut excl_merged = dl_merged.recorded_scan_exclusion();
        let mut excl_cold = dl_cold.recorded_scan_exclusion();
        excl_merged.sort();
        excl_cold.sort();
        assert_eq!(excl_merged, excl_cold, "exclusion sets must match");
        assert_eq!(
            dl_merged.recorded_scan_sql(),
            dl_cold.recorded_scan_sql(),
            "snapshot scan SQL must match"
        );

        // Log-tier parity: both entries must hand the single cold `resolve_cold`
        // the same SQL (ids pushdown included; the resolve carries no user
        // filter). Sorted for order-insensitive comparison.
        let mut exec_merged = dl_merged.recorded_exec_sql();
        let mut exec_cold = dl_cold.recorded_exec_sql();
        exec_merged.sort();
        exec_cold.sort();
        assert!(!exec_merged.is_empty(), "resolve-leg SQL must be recorded");
        assert_eq!(exec_merged, exec_cold, "resolve SQL must match");
    }

    /// Misrouting a plan with a hot tier into the all-cold entry must
    /// fail fast with `InvalidPlan` — never silently drop hot upserts
    /// and delete-exclusions from the read.
    #[tokio::test]
    async fn stream_all_cold_rejects_hot_plan() {
        let mut plan = plan_with_snapshot_and_persist(vec![snapshot_segment("s1")]);
        plan.hot_storage = Some(HotStoragePlan::default());
        let driver = MockDriver;
        let dl = MockDlDriver::default();
        let schema = test_user_schema();

        let result = stream_all_cold_parts(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(2_000),
            filter: None,
            seeks: None,
            segment_read_concurrency: 2,
            snapshot_prune_min_segments: 0,
        })
        .await;

        assert!(
            matches!(result, Err(MergeError::InvalidPlan(_))),
            "hot plan must be rejected, got {result:?}",
            result = result.as_ref().map(|_| "Ok(MergeReadParts)")
        );
    }

    /// CHA-531: the base-cold fold must fire at THIS entry, not only at
    /// [`stream_all_cold_parts`]. Asserted here rather than on
    /// `fold_in_base_cold_source` because the bug this pins was exactly a
    /// correct helper that the entry never called — every other plan-level
    /// test sets `base_cold_storage: None`, so moving the call back out
    /// leaves them all green.
    ///
    /// The child's cold plan is empty (`persist: None` short-circuits
    /// `resolve_cold`), so everything in the output came from the base.
    #[tokio::test]
    async fn resolve_log_tiers_folds_base_cold_storage() {
        let schema = test_user_schema();
        let dl = MockDlDriver::default().with_resolved(make_resolved_batch_flagged(
            &["u_a", "u_d"],
            &["a", "d"],
            &[1, 0],
            &[100, 200],
            &[false, true], // u_a live upsert, u_d winning tombstone
        ));
        let empty_child = Plan {
            hot_storage: None,
            cold_storage: Some(ColdStoragePlan {
                snapshot: None,
                persist: None,
            }),
            base_cold_storage: None,
        };
        let with_base = Plan {
            base_cold_storage: Some(base_cold(i64::MAX)),
            ..empty_child.clone()
        };
        let snapshot = ReadSnapshot::AsOfMicros(i64::MAX);
        let driver = MockDriver;

        let without = resolve_log_tiers(&MergeReadRequest {
            segment_order: SegmentOrder::ByPlan,
            plan: &empty_child,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &snapshot,
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        })
        .await
        .unwrap();
        assert_eq!(
            without.resolved.num_rows(),
            0,
            "control: with no base the empty child plan resolves to nothing",
        );
        assert!(without.exclusion_set.is_empty());

        let tiers = resolve_log_tiers(&MergeReadRequest {
            segment_order: SegmentOrder::ByPlan,
            plan: &with_base,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &snapshot,
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        })
        .await
        .unwrap();
        assert_eq!(
            all_row_uuids(std::slice::from_ref(&tiers.resolved)),
            vec!["u_a".to_string()],
            "the base's live row must reach `resolved` through this entry",
        );
        assert_eq!(
            tiers.exclusion_set,
            str_set(&["u_a", "u_d"]),
            "the base's tombstone shadows the snapshot leg via the exclusion \
             set even though it is filtered out of `resolved`",
        );
    }

    /// Split-consumption contract: resolve the log tiers once,
    /// then stream a SUBSET `SnapshotPlan` (one of two planned segments)
    /// through the now-public `snapshot_segment_stream` with the
    /// exclusion set that resolve produced. Only the subset segment is
    /// scanned, and the resolved-row exclusion is still applied to it —
    /// pinning the seam carry-forward relies on (touched-subset stream +
    /// global exclusion). The carry-forward path uses `ByPlan`, which
    /// applies the exclusion PER BATCH after a plain scan (not in the
    /// scan SQL), so the exclusion is asserted on the OUTPUT: the stale
    /// `r1` living in the subset segment is dropped, `r2` survives.
    #[tokio::test]
    async fn snapshot_segment_stream_over_subset_uses_resolved_exclusion() {
        let plan = plan_with_snapshot_and_persist(vec![
            snapshot_segment("seg1"),
            snapshot_segment("seg2"),
        ]);
        let driver = MockDriver;
        let dl = MockDlDriver::default()
            .with_snapshot("seg1", make_snapshot_batch(&["r0"], &["zzz"], &[1]))
            // seg2 (the touched subset) holds the stale r1 plus r2.
            .with_snapshot(
                "seg2",
                make_snapshot_batch(&["r1", "r2"], &["alice", "bob"], &[10, 20]),
            )
            // A resolved delta row supersedes r1 → r1 folds into the
            // exclusion set.
            .with_resolved(make_resolved_batch(&["r1"], &["alice_v2"], &[99], &[2000]));
        let schema = test_user_schema();

        // Phase A: resolve once.
        let tiers = resolve_log_tiers(&MergeReadRequest {
            segment_order: SegmentOrder::ByPlan,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: None,
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        })
        .await
        .unwrap();
        assert!(
            tiers.exclusion_set.contains("r1"),
            "resolved r1 must be in the exclusion set; got {:?}",
            tiers.exclusion_set
        );

        // Phase C: stream only seg2 (the "touched subset") with that
        // exclusion set.
        let subset = SnapshotPlan {
            segments: vec![snapshot_segment("seg2")],
            snapshotted_at_micros: 1000,
            ..Default::default()
        };
        let stream = snapshot_segment_stream(
            &subset,
            &dl,
            &schema,
            &schema,
            None,
            SnapshotSeeks::default(),
            tiers.exclusion_set,
            SnapshotStreamTuning {
                segment_read_concurrency: 4,
                snapshot_prune_min_segments: 0,
                segment_order: SegmentOrder::ByPlan,
            },
        );
        let out = collect_stream(stream).await.unwrap();

        assert_eq!(
            dl.recorded_snapshot_reads(),
            vec!["seg2".to_string()],
            "only the subset segment must be scanned, not the whole plan"
        );
        let uuids = all_row_uuids(&out);
        assert!(
            !uuids.contains(&"r1".to_string()),
            "the resolved exclusion must drop stale r1 from the subset stream; got {uuids:?}"
        );
        assert!(
            uuids.contains(&"r2".to_string()),
            "the unsuperseded r2 must survive the subset stream; got {uuids:?}"
        );
    }

    /// A single identity `IndexSeek` entry must thread to every tier: the
    /// provider receives the uuid strings as seek_keys and the snapshot scan
    /// SQL carries the `row_uuid IN` residual.
    #[tokio::test]
    async fn seeks_identity_subsumes_row_uuids() {
        let plan = plan_with_snapshot(vec![snapshot_segment("s1")]);
        let schema = test_user_schema();
        let dl =
            MockDlDriver::default().with_snapshot("s1", make_snapshot_batch(&["r1"], &["a"], &[1]));
        let driver = MockDriver;
        let ids = vec![Uuid::nil()];

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: Some(vec![IndexSeek::identity(&ids)]),
            segment_read_concurrency: 2,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .expect("stream_merged completes");

        assert_eq!(
            dl.recorded_seeks(),
            vec![Some(vec![SeekSpec {
                index_uuid: None,
                key_columns: vec![],
                tuples: vec![vec![Uuid::nil().to_string()]],
            }])],
            "identity seek entry must reach the provider as the identity seek spec",
        );
        let sql = dl.recorded_scan_sql();
        assert!(
            sql.iter().any(|s| s.contains(&Uuid::nil().to_string())),
            "snapshot scan SQL must carry the row_uuid IN residual; got {sql:?}",
        );
    }

    /// A non-identity seek entry (`index_uuid: Some`) reaching the merge
    /// fallback with NO residual `filter` is malformed and fails fast — a
    /// filter-accompanied one is legal and rides as a selection accelerator.
    /// Without this guard a filterless name seek would silently full-scan.
    #[tokio::test]
    async fn seeks_name_index_in_merge_fallback_fails_fast() {
        let plan = plan_with_snapshot(vec![snapshot_segment("s1")]);
        let schema = test_user_schema();
        let dl = MockDlDriver::default();
        let driver = MockDriver;
        let name_seek = IndexSeek {
            key_columns: Vec::new(),
            index_uuid: Some(Uuid::nil()),
            tuples: vec![vec!["s1".to_string(), "t1".to_string()]],
        };

        let err = collect_stream(stream_all_cold(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: None,
            seeks: Some(vec![name_seek]),
            segment_read_concurrency: 2,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .expect_err("name-index seek must fail fast in the merge fallback");
        match err {
            MergeError::InvalidPlan(msg) => assert!(
                msg.contains("name-index seek"),
                "expected the CHA-484 name-index fail-fast, got: {msg}",
            ),
            other => panic!("expected InvalidPlan, got {other:?}"),
        }
    }

    /// `identity_row_uuids` fail-fast: an identity entry tuple that isn't arity-1
    /// is malformed (a multi-column tuple silently truncated to its first element
    /// would be a correctness bug) → InvalidPlan.
    #[test]
    fn identity_row_uuids_rejects_non_arity1_tuple() {
        let seeks = vec![IndexSeek {
            index_uuid: None,
            key_columns: Vec::new(),
            tuples: vec![vec!["a".to_string(), "b".to_string()]],
        }];
        match identity_row_uuids(Some(&seeks), false) {
            Err(MergeError::InvalidPlan(msg)) => {
                assert!(msg.contains("arity 1"), "expected arity guard, got: {msg}")
            }
            other => panic!("expected InvalidPlan arity error, got {other:?}"),
        }
    }

    /// `identity_row_uuids` fail-fast: a non-UUID string in an identity tuple is
    /// rejected rather than silently injected → InvalidPlan.
    #[test]
    fn identity_row_uuids_rejects_non_uuid_string() {
        let seeks = vec![IndexSeek {
            index_uuid: None,
            key_columns: Vec::new(),
            tuples: vec![vec!["not-a-uuid".to_string()]],
        }];
        match identity_row_uuids(Some(&seeks), false) {
            Err(MergeError::InvalidPlan(msg)) => assert!(
                msg.contains("invalid row_uuid"),
                "expected parse error, got: {msg}"
            ),
            other => panic!("expected InvalidPlan parse error, got {other:?}"),
        }
    }

    /// A non-identity entry with a residual filter is a legal accelerator —
    /// skipped by the identity extraction — while the no-filter shape keeps
    /// the fail-fast guard.
    #[test]
    fn identity_row_uuids_accelerator_entries_require_filter() {
        let accelerator = IndexSeek {
            index_uuid: Some(Uuid::nil()),
            key_columns: vec!["x_col".to_string()],
            tuples: vec![vec!["x".to_string()]],
        };
        let uuid = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let mixed = vec![accelerator.clone(), IndexSeek::identity(&[uuid])];
        // With a filter: the accelerator is skipped, the identity extracted.
        assert_eq!(
            identity_row_uuids(Some(&mixed), true).unwrap(),
            Some(vec![uuid]),
        );
        // Accelerator-only + filter: no identity restriction at all (None,
        // not Some(empty) — an empty restriction would exclude every row).
        let only = vec![accelerator];
        assert_eq!(identity_row_uuids(Some(&only), true).unwrap(), None);
        // Without a filter the fail-fast guard stands.
        assert!(matches!(
            identity_row_uuids(Some(&only), false),
            Err(MergeError::InvalidPlan(_))
        ));
    }

    /// A filtered read carrying identity + covering-index entries
    /// threads BOTH to the provider — identity first (the only
    /// exclusion-restricting pass), accelerators after, in caller order.
    #[tokio::test]
    async fn by_completion_scan_receives_multi_entry_seeks() {
        let plan = plan_with_snapshot(vec![snapshot_segment("seg1")]);
        let driver = MockDriver;
        let dl = MockDlDriver::default()
            .with_snapshot("seg1", make_snapshot_batch(&["r1"], &["alice"], &[10]));
        let schema = test_user_schema();
        let uuid = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let user_index = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: Some("value > 0"),
            seeks: Some(vec![
                IndexSeek::identity(&[uuid]),
                IndexSeek {
                    index_uuid: Some(user_index),
                    key_columns: vec!["name".to_string()],
                    tuples: vec![vec!["alice".to_string()]],
                },
            ]),
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();

        assert_eq!(
            dl.recorded_seeks(),
            vec![Some(vec![
                SeekSpec {
                    index_uuid: None,
                    key_columns: vec![],
                    tuples: vec![vec![uuid.to_string()]],
                },
                SeekSpec {
                    index_uuid: Some(user_index.to_string()),
                    key_columns: vec!["name".to_string()],
                    tuples: vec![vec!["alice".to_string()]],
                },
            ])],
            "identity entry first, covering-index accelerators after",
        );
    }

    /// Accelerator-only shape — no identity restriction, a filter,
    /// one covering-index entry. Pins `to_scan_specs`'s copy-free
    /// `(None, Some)` arm end-to-end: the provider receives exactly the
    /// accelerator entries (no identity spec is synthesized).
    #[tokio::test]
    async fn by_completion_scan_receives_accelerator_only_seeks() {
        let plan = plan_with_snapshot(vec![snapshot_segment("seg1")]);
        let driver = MockDriver;
        let dl = MockDlDriver::default()
            .with_snapshot("seg1", make_snapshot_batch(&["r1"], &["alice"], &[10]));
        let schema = test_user_schema();
        let user_index = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();

        let _ = collect_stream(stream_merged(MergeReadRequest {
            segment_order: SegmentOrder::ByCompletion,
            plan: &plan,
            driver: &driver,
            dl: &dl,
            user_schema: &schema,
            full_schema: &schema,
            snapshot: &ReadSnapshot::AsOfMicros(i64::MAX),
            filter: Some("value > 0"),
            seeks: Some(vec![IndexSeek {
                index_uuid: Some(user_index),
                key_columns: vec!["name".to_string()],
                tuples: vec![vec!["alice".to_string()]],
            }]),
            segment_read_concurrency: 4,
            snapshot_prune_min_segments: 0,
        }))
        .await
        .unwrap();

        assert_eq!(
            dl.recorded_seeks(),
            vec![Some(vec![SeekSpec {
                index_uuid: Some(user_index.to_string()),
                key_columns: vec!["name".to_string()],
                tuples: vec![vec!["alice".to_string()]],
            }])],
            "accelerator-only reads hand exactly the accelerator entries down",
        );
    }
}
