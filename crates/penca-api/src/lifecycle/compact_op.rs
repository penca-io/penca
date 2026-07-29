//! Compact: per-scope active+sealed merge for persist segments.
//!
//! Snapshot segments are immutable and never compact (ADR 0024).

use std::collections::HashMap;

use penca_db::driver::pg::PgDriver;
use penca_dl::driver::DlDriver;
use penca_format::reader::FormatReader;
use penca_format::writer::FormatWriter;
use penca_proto::external::v1::{CompactPersistSegmentsRequest, CompactPersistSegmentsResponse};

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;
use crate::lifecycle::compact::{PersistScope, compact_one_scope};
use crate::pagination::timestamp_bounds;

impl LifecycleManager {
    /// Compact persist-log segments for a single table.
    ///
    /// Per-scope active+sealed algorithm. Each `(table_uuid, log_kind)`
    /// scope on the branch maintains at most one "active" merged file
    /// (rows have `is_sealed = false`) plus zero or more "sealed"
    /// merged files (`is_sealed = true`). A compact wave on a scope
    /// either *extends* the active by folding in adjacent uncompacted
    /// segments, or — when the active is at the size threshold and the
    /// next uncompacted would breach it — *seals* the prior active in
    /// the same tx and starts a fresh active from the next uncompacted
    /// segment.
    ///
    /// Sealed rows never participate in another compact wave; the only
    /// concurrency primitive is the row-level `SELECT FOR UPDATE` on
    /// the unsealed scope rows inside each per-scope merge tx. No
    /// advisory lock. Two compacts on the same scope serialize on the
    /// row locks; the loser re-runs against the post-winner state and
    /// plans from there, so a stale URI from before the winner's
    /// commit can't trip it. Plan-inside-the-locking-tx ordering is
    /// load-bearing.
    ///
    /// The optional `persisted_at` filter scopes which segments are
    /// eligible by their per-row `commit_micros`. Omitted = every
    /// committed segment for T on the branch.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn compact_persist_segments<L, R, W>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        readers: &HashMap<i32, R>,
        writer: &W,
        request: &CompactPersistSegmentsRequest,
    ) -> Result<CompactPersistSegmentsResponse, ApiError>
    where
        L: DlDriver + ?Sized,
        R: FormatReader,
        W: FormatWriter,
    {
        // See `persist`'s `step1_now` doc-comment.
        let step1_now = penca_storage_meta::LifecycleManager::now_micros(pool).await?;
        let snapshot = penca_merge::ReadSnapshot::AsOfMicros(step1_now);
        let (catalog_uuid, branch_uuid, table_uuid) = self
            .resolve_catalog_branch_and_table(
                pool,
                dl_driver,
                request.catalog_uuid.as_deref(),
                request.catalog_name.as_deref(),
                request.schema_uuid.as_deref(),
                request.schema_name.as_deref(),
                request.branch_uuid.as_deref(),
                request.branch_name.as_deref(),
                request.table_uuid.as_deref(),
                request.table_name.as_deref(),
                &snapshot,
            )
            .await?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("table_uuid", tracing::field::display(&table_uuid));

        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let (min_persisted, max_persisted) = timestamp_bounds(request.persisted_at.as_ref());

        // Filter table_uuid in SQL so a per-tick scheduler over N tables
        // stays O(N), not O(N²).
        let table_str = table_uuid.to_string();
        let scopes = penca_storage_meta::LifecycleManager::list_unsealed_persist_scopes_on_table(
            pool,
            &catalog_str,
            &branch_str,
            &table_str,
            min_persisted,
            max_persisted,
        )
        .await?;

        let mut merged_object_uris: Vec<String> = Vec::new();
        for log_kind in scopes {
            let scope = PersistScope {
                log_kind,
                snapshot: &snapshot,
                query_manager: &self.query_manager,
            };
            if let Some(uri) = compact_one_scope(
                &scope,
                pool,
                dl_driver,
                readers,
                writer,
                catalog_uuid,
                branch_uuid,
                table_uuid,
                min_persisted,
                max_persisted,
                self.max_segment_bytes,
                &self.base_uri,
                self.storage_format,
            )
            .await?
            {
                merged_object_uris.push(uri);
            }
        }

        Ok(CompactPersistSegmentsResponse { merged_object_uris })
    }
}
