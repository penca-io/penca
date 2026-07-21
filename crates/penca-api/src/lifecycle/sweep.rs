//! Sweep: grace-window drain for cold segment files (CHA-233 / ADR 0019).
//!
//! [`LifecycleManager::sweep_segments`] reads every
//! `segment_delete_set` row on the branch whose `written_at_micros`
//! is past the grace window and deletes the cold file followed by the
//! row.

use penca_db::driver::pg::PgDriver;
use penca_format::writer::FormatWriter;
use penca_proto::external::v1::{SweepSegmentsRequest, SweepSegmentsResponse};
use penca_storage_cold::ColdStorageClient;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;
use crate::resolve::{parse_resolved_uuid, resolve_branch, resolve_catalog};

impl LifecycleManager {
    /// Physically delete cold segment files queued for removal by past
    /// compact waves (CHA-233 / ADR 0019 §"Four-part mechanism" item 3).
    ///
    /// Reads every `segment_delete_set` row on the branch whose
    /// `written_at_micros + query_timeout < now`, deletes the cold
    /// file, then deletes the set row. The order is load-bearing:
    /// the row only goes away once cold has confirmed the file is
    /// gone, so a transient cold-storage failure leaves the row in
    /// place for the next sweep to retry. The file delete passes
    /// `ignore_missing = true` so a successful prior sweep that
    /// crashed between file-delete and row-delete also drains cleanly.
    ///
    /// No advisory lock — the per-row PK DELETE on
    /// `segment_delete_set` is safe under concurrent sweeps; two
    /// sweepers racing on the same row both succeed (idempotent
    /// DELETE).
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn sweep_segments<W>(
        &self,
        pool: &PgDriver,
        writer: &W,
        request: &SweepSegmentsRequest,
    ) -> Result<SweepSegmentsResponse, ApiError>
    where
        W: FormatWriter,
    {
        let catalog = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.branch_uuid.as_deref(),
            request.branch_name.as_deref(),
        )
        .await?;
        let branch_uuid = parse_resolved_uuid(&branch.branch_uuid, "branch_uuid")?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));

        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();

        let now_micros = penca_storage_meta::LifecycleManager::now_micros(pool).await?;
        let eligible = penca_storage_meta::LifecycleManager::eligible_segment_delete_set_rows(
            pool,
            &catalog_str,
            &branch_str,
            now_micros,
            self.query_timeout_micros,
        )
        .await?;

        let eligible_count = eligible.len();
        let mut deleted_count = 0usize;
        for (segment_delete_uuid, object_uri) in eligible {
            if ColdStorageClient::delete_segment(writer, &object_uri, true)
                .await
                .is_ok()
            {
                penca_storage_meta::LifecycleManager::delete_segment_delete_set_row(
                    pool,
                    &catalog_str,
                    &branch_str,
                    &segment_delete_uuid.to_string(),
                )
                .await?;
                deleted_count += 1;
            }
        }

        // `eligible` already excludes refcount-pinned rows (CHA-405),
        // so a persistent 0 here while the delete set grows reads as
        // "everything still referenced" — the first triage signal for
        // a standing blocked set (see CHA-435). `deleted` can lag
        // `eligible` when a cold-file delete fails and the row is left
        // for the next sweep to retry.
        tracing::debug!(
            eligible = eligible_count,
            deleted = deleted_count,
            "sweep_segments complete"
        );

        Ok(SweepSegmentsResponse {})
    }
}
