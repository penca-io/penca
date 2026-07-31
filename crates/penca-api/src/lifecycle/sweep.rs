//! Sweep: grace-window drain for cold segment files (ADR 0019).

use penca_db::driver::pg::PgDriver;
use penca_format::writer::FormatWriter;
use penca_proto::external::v1::{SweepSegmentsRequest, SweepSegmentsResponse};
use penca_storage_cold::ColdStorageClient;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;
use crate::resolve::{parse_resolved_uuid, resolve_catalog};

impl LifecycleManager {
    /// Physically delete cold segment files queued for removal by past
    /// compact waves (ADR 0019 §"Four-part mechanism" item 3), once
    /// `written_at_micros + query_timeout < now`.
    ///
    /// Catalog-scoped, not branch-scoped (CHA-531): carry-forward makes one
    /// cold file reachable from any branch, so the delete set holds one row
    /// per file for the whole catalog and the refcount gate has to see every
    /// branch's references at once. There is nothing left for a branch
    /// argument to select.
    ///
    /// File-delete-then-row-delete ordering is load-bearing: the row only goes
    /// away once cold has confirmed the file is gone, so a transient
    /// cold-storage failure leaves the row for the next sweep to retry.
    /// `ignore_missing = true` covers the reverse case — a prior sweep that
    /// crashed between the two deletes.
    ///
    /// No advisory lock: the per-row PK DELETE is idempotent, so two sweepers
    /// racing on the same row both succeed.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(catalog_uuid = tracing::field::Empty),
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

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));

        let catalog_str = catalog_uuid.to_string();

        let now_micros = penca_storage_meta::LifecycleManager::now_micros(pool).await?;

        // Reap first: drop past-grace rows whose URI still has a COMMITTED
        // reference, so the eligibility scan below walks a set that does not
        // accumulate. Nothing else ever removes such a row — the unlink path only
        // deletes after a successful delete — so before this they sat in the
        // expired range forever, and enqueue-only branch teardown made that growth
        // monotonic in "forks ever deleted". Whoever later drops the URI's last
        // committed reference re-enqueues it, so reaping loses nothing. Rows
        // pinned only by an in-flight snapshot's UNCOMMITTED carried refs are
        // deliberately left alone; that path's cleanup does not enqueue.
        let reaped_count =
            penca_storage_meta::LifecycleManager::reap_referenced_segment_delete_set_rows(
                pool,
                &catalog_str,
                now_micros,
                self.query_timeout_micros,
            )
            .await?;

        let eligible = penca_storage_meta::LifecycleManager::eligible_segment_delete_set_rows(
            pool,
            &catalog_str,
            now_micros,
            self.query_timeout_micros,
        )
        .await?;

        let eligible_count = eligible.len();
        let mut deleted_count = 0usize;
        for object_uri in eligible {
            if ColdStorageClient::delete_segment(writer, &object_uri, true)
                .await
                .is_ok()
            {
                penca_storage_meta::LifecycleManager::delete_segment_delete_set_row(
                    pool,
                    &catalog_str,
                    &object_uri,
                )
                .await?;
                deleted_count += 1;
            }
        }

        // Triage guide for these three fields: `eligible` already excludes
        // refcount-pinned rows, so a persistent 0 while the delete set grows
        // reads as "everything still referenced". `deleted` lagging `eligible`
        // means cold-file deletes are failing and the rows await a retry.
        // `reaped` is the still-referenced rows dropped from the set this pass —
        // it is what distinguishes a healthy idle sweep from the old failure mode
        // where `eligible` and `deleted` both read 0 forever while the set grew
        // without bound.
        tracing::debug!(
            eligible = eligible_count,
            deleted = deleted_count,
            reaped = reaped_count,
            "sweep_segments complete"
        );

        Ok(SweepSegmentsResponse {})
    }
}
