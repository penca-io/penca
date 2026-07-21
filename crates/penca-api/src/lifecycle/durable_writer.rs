//! Auto-commit per-segment write loop shared by Persist (phase 1) and
//! Snapshot (phase 1b).
//!
//! [`DurableSegmentWriter`] runs the four-step
//! `insert_segment → write_file → update_size → commit_segment`
//! auto-commit sequence one file at a time, accumulating one
//! [`SegmentGroup`] per step so [`DurableSegmentWriter::cleanup_on_err`]
//! can roll back per group — file first, then the rows that shared it —
//! on any error. The trait
//! [`SegmentScope`] is the policy switch between persist and snapshot:
//! it abstracts the metadata insert/update/commit/delete calls, the
//! cold-storage write/delete calls, and the per-segment data shape.
//!
//! Parent metadata rows (`table_persist_metadata` /
//! `table_snapshot_metadata`) stay outside this helper — their shapes
//! diverge (persist's per-`log_kind` row has no snapshot analogue) and
//! the unification cost would exceed the savings.

use std::future::Future;

use arrow::record_batch::RecordBatch;
use penca_core::Format;
use penca_db::driver::pg::PgDriver;
use penca_format::writer::FormatWriter;
use penca_storage_cold::ColdStorageClient;
use penca_storage_meta::LifecycleManager;

use crate::error::ApiError;

/// Policy switch between persist and snapshot durable-segment writes.
///
/// Five hooks split the two state machines apart: insert, write_file,
/// update_size, commit_segment, delete_uncommitted_segment. Plus two
/// pure accessors that name the seg_uuid / uri slices inside the
/// per-scope `Step` shape.
pub(super) trait SegmentScope: Send + Sync {
    type Step: Send + Sync;

    fn seg_uuid_str(step: &Self::Step) -> &str;
    fn uri(step: &Self::Step) -> &str;
    /// The segment's standalone in-memory Arrow footprint (CHA-347):
    /// recorded as `size_bytes` so it compares like-for-like against
    /// `max_segment_bytes`. Carried on the step from the chunker, not
    /// re-derived from the on-disk file the writer produces.
    fn size_bytes(step: &Self::Step) -> i64;

    fn insert_segment(
        &self,
        pool: &PgDriver,
        step: &Self::Step,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    fn write_file<W: FormatWriter>(
        writer: &W,
        step: &Self::Step,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    fn update_size(
        &self,
        pool: &PgDriver,
        step: &Self::Step,
        size_bytes: i64,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    fn commit_segment(
        &self,
        pool: &PgDriver,
        step: &Self::Step,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    fn delete_uncommitted_segment(
        &self,
        pool: &PgDriver,
        seg_uuid_str: &str,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;
}

/// The segment rows that share one durable FILE — the unit of
/// crash-safe write and rollback. One group per durable-write step: a
/// persist group is a single segment (`write_segment`); a packed
/// snapshot group has one row per partition (`write_segment_group`,
/// CHA-404).
///
/// `written_uri` is the group's file once the file write has
/// succeeded; `None` means the rows are file-less (inserted, file
/// write pending or failed) and therefore always safe for cleanup to
/// delete.
#[derive(Default)]
struct SegmentGroup {
    written_uri: Option<String>,
    seg_uuid_strs: Vec<String>,
}

/// Drive a series of per-segment writes through a [`SegmentScope`],
/// accumulating one [`SegmentGroup`] per durable-write step for
/// cleanup-on-error.
pub(super) struct DurableSegmentWriter<S: SegmentScope> {
    scope: S,
    segment_groups: Vec<SegmentGroup>,
}

impl<S: SegmentScope> DurableSegmentWriter<S> {
    pub(super) fn new(scope: S) -> Self {
        Self {
            scope,
            segment_groups: Vec::new(),
        }
    }

    /// Insert metadata, write the file, update size, commit metadata.
    /// The segment row is recorded in a fresh group immediately after
    /// the insert (so cleanup can drop the uncommitted row even if the
    /// file write fails); the group's uri is set only once the file
    /// write succeeds.
    pub(super) async fn write_segment<W: FormatWriter>(
        &mut self,
        pool: &PgDriver,
        writer: &W,
        step: &S::Step,
    ) -> Result<(), ApiError> {
        self.open_group();
        self.scope.insert_segment(pool, step).await?;
        self.current_group()
            .seg_uuid_strs
            .push(S::seg_uuid_str(step).to_string());

        S::write_file(writer, step).await?;
        self.current_group().written_uri = Some(S::uri(step).to_string());

        // CHA-347: record the standalone in-memory footprint carried on
        // the step (from the chunker) so `size_bytes` compares
        // like-for-like against `max_segment_bytes`. `write_file` no
        // longer surfaces the on-disk size — that unit is gone from the
        // write path entirely.
        self.scope
            .update_size(pool, step, S::size_bytes(step))
            .await?;
        self.scope.commit_segment(pool, step).await?;
        Ok(())
    }

    /// Best-effort GC, per group: delete the group's file (if it was
    /// written), then — only once the file is confirmed gone (deleted
    /// now, or never written) — the group's uncommitted segment rows.
    /// Rows of a surviving file stay: the NULL parent keeps them
    /// unreachable, and the deterministic-uuid retry path reuses them.
    /// Groups are independent files, so per-group ordering is the only
    /// ordering that matters. Errors are swallowed; the caller
    /// surfaces the original failure.
    pub(super) async fn cleanup_on_err<W: FormatWriter>(&self, pool: &PgDriver, writer: &W) {
        for group in &self.segment_groups {
            let file_gone = match &group.written_uri {
                Some(uri) => ColdStorageClient::delete_segment(writer, uri, true)
                    .await
                    .is_ok(),
                None => true,
            };
            if !file_gone {
                continue;
            }
            for seg_uuid in &group.seg_uuid_strs {
                let _ = self.scope.delete_uncommitted_segment(pool, seg_uuid).await;
            }
        }
    }

    /// Open the group for the next durable-write step. Rows recorded
    /// after this belong to the new group.
    fn open_group(&mut self) {
        self.segment_groups.push(SegmentGroup::default());
    }

    /// The group opened by the in-flight durable-write step.
    fn current_group(&mut self) -> &mut SegmentGroup {
        self.segment_groups
            .last_mut()
            .expect("open_group precedes row recording")
    }
}

// ---------------------------------------------------------------------------
// Persist scope
// ---------------------------------------------------------------------------

/// Per-segment data for one persist-side write step. Owned because
/// `phase1_durable_writes` borrows each chunk from `chunk_row_ranges`
/// inside a loop; an owned struct sidesteps the borrow contortions
/// without an extra clone (the batch is `Arc`-backed).
pub(super) struct PersistSegmentStep {
    pub seg_uuid_str: String,
    pub table_persist_uuid_str: String,
    pub chunk_idx: u32,
    pub min_committed_at: i64,
    pub max_committed_at: i64,
    pub min_commit_seq_num: i64,
    pub max_commit_seq_num: i64,
    pub uri: String,
    pub num_rows: i64,
    pub size_bytes: i64,
    pub batch: RecordBatch,
}

pub(super) struct PersistSegmentScope<'a> {
    pub catalog_str: &'a str,
    pub branch_str: &'a str,
    pub table_str: &'a str,
    pub storage_format: Format,
}

impl<'a> SegmentScope for PersistSegmentScope<'a> {
    type Step = PersistSegmentStep;

    fn seg_uuid_str(step: &Self::Step) -> &str {
        &step.seg_uuid_str
    }

    fn uri(step: &Self::Step) -> &str {
        &step.uri
    }

    fn size_bytes(step: &Self::Step) -> i64 {
        step.size_bytes
    }

    async fn insert_segment(&self, pool: &PgDriver, step: &Self::Step) -> Result<(), ApiError> {
        let statistics = penca_dl::stats::compute_segment_statistics(&step.batch);
        LifecycleManager::insert_table_persist_segment(
            pool,
            self.catalog_str,
            self.branch_str,
            self.table_str,
            &step.seg_uuid_str,
            &step.table_persist_uuid_str,
            step.chunk_idx,
            step.min_committed_at,
            step.max_committed_at,
            step.min_commit_seq_num,
            step.max_commit_seq_num,
            &step.uri,
            step.num_rows,
            self.storage_format.extension(),
            &statistics,
        )
        .await?;
        Ok(())
    }

    async fn write_file<W: FormatWriter>(writer: &W, step: &Self::Step) -> Result<(), ApiError> {
        ColdStorageClient::write_table_persist_segment(writer, &step.uri, &step.batch).await?;
        Ok(())
    }

    async fn update_size(
        &self,
        pool: &PgDriver,
        step: &Self::Step,
        size_bytes: i64,
    ) -> Result<(), ApiError> {
        LifecycleManager::update_table_persist_segment_size(
            pool,
            self.catalog_str,
            self.branch_str,
            &step.seg_uuid_str,
            size_bytes,
        )
        .await?;
        Ok(())
    }

    async fn commit_segment(&self, pool: &PgDriver, step: &Self::Step) -> Result<(), ApiError> {
        LifecycleManager::commit_table_persist_segment(
            pool,
            self.catalog_str,
            self.branch_str,
            &step.seg_uuid_str,
        )
        .await?;
        Ok(())
    }

    async fn delete_uncommitted_segment(
        &self,
        pool: &PgDriver,
        seg_uuid_str: &str,
    ) -> Result<(), ApiError> {
        LifecycleManager::delete_uncommitted_table_persist_segment(
            pool,
            self.catalog_str,
            self.branch_str,
            seg_uuid_str,
        )
        .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Snapshot scope
// ---------------------------------------------------------------------------

/// Per-segment data for one snapshot-side write step.
pub(super) struct SnapshotSegmentStep {
    pub seg_uuid_str: String,
    pub snap_uuid_str: String,
    pub chunk_idx: u32,
    pub uri: String,
    pub num_rows: i64,
    pub size_bytes: i64,
    pub batch: RecordBatch,
}

pub(super) struct SnapshotSegmentScope<'a> {
    pub catalog_str: &'a str,
    pub branch_str: &'a str,
    pub table_str: &'a str,
    pub storage_format: Format,
}

impl<'a> SegmentScope for SnapshotSegmentScope<'a> {
    type Step = SnapshotSegmentStep;

    fn seg_uuid_str(step: &Self::Step) -> &str {
        &step.seg_uuid_str
    }

    fn uri(step: &Self::Step) -> &str {
        &step.uri
    }

    fn size_bytes(step: &Self::Step) -> i64 {
        step.size_bytes
    }

    async fn insert_segment(&self, pool: &PgDriver, step: &Self::Step) -> Result<(), ApiError> {
        let statistics = penca_dl::stats::compute_segment_statistics(&step.batch);
        LifecycleManager::insert_snapshot_segment(
            pool,
            self.catalog_str,
            self.branch_str,
            self.table_str,
            &step.seg_uuid_str,
            &step.snap_uuid_str,
            step.chunk_idx,
            &step.uri,
            // A single-segment file is the whole-file range (CHA-404).
            // (This write_segment arm currently has no caller —
            // snapshot writes go through `write_segment_group`.)
            0,
            step.num_rows,
            step.num_rows,
            self.storage_format.extension(),
            &statistics,
        )
        .await?;
        Ok(())
    }

    async fn write_file<W: FormatWriter>(writer: &W, step: &Self::Step) -> Result<(), ApiError> {
        ColdStorageClient::write_snapshot_segment(writer, &step.uri, &step.batch).await?;
        Ok(())
    }

    async fn update_size(
        &self,
        pool: &PgDriver,
        step: &Self::Step,
        size_bytes: i64,
    ) -> Result<(), ApiError> {
        LifecycleManager::update_snapshot_segment_size(
            pool,
            self.catalog_str,
            self.branch_str,
            &step.seg_uuid_str,
            size_bytes,
        )
        .await?;
        Ok(())
    }

    async fn commit_segment(&self, pool: &PgDriver, step: &Self::Step) -> Result<(), ApiError> {
        LifecycleManager::commit_snapshot_segment(
            pool,
            self.catalog_str,
            self.branch_str,
            &step.seg_uuid_str,
        )
        .await?;
        Ok(())
    }

    async fn delete_uncommitted_segment(
        &self,
        pool: &PgDriver,
        seg_uuid_str: &str,
    ) -> Result<(), ApiError> {
        LifecycleManager::delete_uncommitted_snapshot_segment(
            pool,
            self.catalog_str,
            self.branch_str,
            seg_uuid_str,
        )
        .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Packed snapshot files (CHA-404)
// ---------------------------------------------------------------------------

/// One packed snapshot segment FILE: several whole partitions
/// concatenated into `file_batch`, written to one `uri`, with one
/// metadata row per partition (`segment_rows`). Produced by
/// `lifecycle::packer::SegmentPacker`.
pub(super) struct SnapshotFileStep {
    pub snap_uuid_str: String,
    pub uri: String,
    pub file_batch: RecordBatch,
    pub segment_rows: Vec<SnapshotSegmentRowSpec>,
}

/// One per-partition segment metadata row inside a packed file.
/// `offset`/`length` are the partition's row range within the file —
/// `length` doubles as the catalog `row_count` (a packed row IS its
/// row range; the columns are NOT NULL since CHA-407). `size_bytes`
/// and `statistics` are computed over the slice only, so pruning
/// stats stay partition-tight.
pub(super) struct SnapshotSegmentRowSpec {
    pub seg_uuid_str: String,
    pub chunk_idx: u32,
    /// The partition this row covers — the packer's grouping identity,
    /// pinned by the snapshot_op packing tests (not a DB column since
    /// CHA-407). TODO(CHA-406): carry-forward's per-partition rewrite
    /// is the production consumer; drop the dead_code allow when it
    /// lands.
    #[cfg_attr(not(test), allow(dead_code))]
    pub partition_value: Option<String>,
    pub offset: i64,
    pub length: i64,
    pub size_bytes: i64,
    pub statistics: Vec<u8>,
}

impl<'a> DurableSegmentWriter<SnapshotSegmentScope<'a>> {
    /// Crash-safe write of one packed file: insert all N segment rows
    /// (NULL `commit_micros`), write the file once, then update
    /// sizes and commit each row. Generalizes `write_segment`'s four-step
    /// auto-commit sequence to N rows per file; the rows and their
    /// file are one [`SegmentGroup`], so `cleanup_on_err`'s per-group
    /// rollback covers them structurally.
    pub(super) async fn write_segment_group<W: FormatWriter>(
        &mut self,
        pool: &PgDriver,
        writer: &W,
        step: &SnapshotFileStep,
    ) -> Result<(), ApiError> {
        self.open_group();
        for row in &step.segment_rows {
            LifecycleManager::insert_snapshot_segment(
                pool,
                self.scope.catalog_str,
                self.scope.branch_str,
                self.scope.table_str,
                &row.seg_uuid_str,
                &step.snap_uuid_str,
                row.chunk_idx,
                &step.uri,
                row.offset,
                row.length,
                row.length,
                self.scope.storage_format.extension(),
                &row.statistics,
            )
            .await?;
            self.current_group()
                .seg_uuid_strs
                .push(row.seg_uuid_str.clone());
        }

        ColdStorageClient::write_snapshot_segment(writer, &step.uri, &step.file_batch).await?;
        self.current_group().written_uri = Some(step.uri.clone());

        for row in &step.segment_rows {
            LifecycleManager::update_snapshot_segment_size(
                pool,
                self.scope.catalog_str,
                self.scope.branch_str,
                &row.seg_uuid_str,
                row.size_bytes,
            )
            .await?;
        }

        for row in &step.segment_rows {
            LifecycleManager::commit_snapshot_segment(
                pool,
                self.scope.catalog_str,
                self.scope.branch_str,
                &row.seg_uuid_str,
            )
            .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod bookkeeping_tests {
    //! CHA-404: drive the REAL `write_segment` and `cleanup_on_err`
    //! paths with a no-IO scope (the `&PgDriver` is a never-connected
    //! lazy pool that only flows into scope hooks, which don't touch
    //! it; the file delete goes through the injectable `FormatWriter`)
    //! and pin the [`SegmentGroup`] recording plus the cleanup
    //! decision — including the file-delete-fails → rows-kept branch.
    //! The packed `write_segment_group` records via the same
    //! open/push/set-uri sequence but calls `LifecycleManager` directly,
    //! so its unit seam is blocked; the lifecycle integration suite
    //! covers it end-to-end.

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::record_batch::RecordBatch;
    use penca_format::reader::FormatError;
    use penca_format::writer::FormatWriter;
    use sqlx::postgres::PgPool;

    use super::{DurableSegmentWriter, SegmentScope};
    use crate::error::ApiError;
    use penca_db::driver::pg::PgDriver;

    struct TestStep {
        seg_uuid: String,
        uri: String,
    }

    fn step(seg_uuid: &str, uri: &str) -> TestStep {
        TestStep {
            seg_uuid: seg_uuid.to_string(),
            uri: uri.to_string(),
        }
    }

    /// No-IO scope: metadata hooks succeed without touching the pool
    /// (or fail on demand), and deletions are recorded for assertions.
    #[derive(Default)]
    struct RecordingScope {
        /// Fail `insert_segment` once this many inserts have happened.
        fail_insert_after: Option<usize>,
        inserts: AtomicUsize,
        deleted_rows: Mutex<Vec<String>>,
    }

    impl SegmentScope for RecordingScope {
        type Step = TestStep;

        fn seg_uuid_str(step: &Self::Step) -> &str {
            &step.seg_uuid
        }

        fn uri(step: &Self::Step) -> &str {
            &step.uri
        }

        fn size_bytes(_: &Self::Step) -> i64 {
            0
        }

        async fn insert_segment(&self, _: &PgDriver, _: &Self::Step) -> Result<(), ApiError> {
            let n = self.inserts.fetch_add(1, Ordering::SeqCst);
            if self.fail_insert_after == Some(n) {
                return Err(ApiError::Internal("injected insert failure".into()));
            }
            Ok(())
        }

        async fn write_file<W: FormatWriter>(_: &W, _: &Self::Step) -> Result<(), ApiError> {
            Ok(())
        }

        async fn update_size(&self, _: &PgDriver, _: &Self::Step, _: i64) -> Result<(), ApiError> {
            Ok(())
        }

        async fn commit_segment(&self, _: &PgDriver, _: &Self::Step) -> Result<(), ApiError> {
            Ok(())
        }

        async fn delete_uncommitted_segment(
            &self,
            _: &PgDriver,
            seg_uuid_str: &str,
        ) -> Result<(), ApiError> {
            self.deleted_rows
                .lock()
                .unwrap()
                .push(seg_uuid_str.to_string());
            Ok(())
        }
    }

    /// The scope's file-write hook is static (no `&self`), so the
    /// fail-file-write case needs its own scope type.
    #[derive(Default)]
    struct FailFileWriteScope(RecordingScope);

    impl SegmentScope for FailFileWriteScope {
        type Step = TestStep;

        fn seg_uuid_str(step: &Self::Step) -> &str {
            &step.seg_uuid
        }

        fn uri(step: &Self::Step) -> &str {
            &step.uri
        }

        fn size_bytes(_: &Self::Step) -> i64 {
            0
        }

        async fn insert_segment(&self, pool: &PgDriver, step: &Self::Step) -> Result<(), ApiError> {
            self.0.insert_segment(pool, step).await
        }

        async fn write_file<W: FormatWriter>(_: &W, _: &Self::Step) -> Result<(), ApiError> {
            Err(ApiError::Internal("injected file-write failure".into()))
        }

        async fn update_size(
            &self,
            pool: &PgDriver,
            step: &Self::Step,
            size: i64,
        ) -> Result<(), ApiError> {
            self.0.update_size(pool, step, size).await
        }

        async fn commit_segment(&self, pool: &PgDriver, step: &Self::Step) -> Result<(), ApiError> {
            self.0.commit_segment(pool, step).await
        }

        async fn delete_uncommitted_segment(
            &self,
            pool: &PgDriver,
            seg_uuid_str: &str,
        ) -> Result<(), ApiError> {
            self.0.delete_uncommitted_segment(pool, seg_uuid_str).await
        }
    }

    /// Cold-storage stand-in: never writes; deletes succeed except for
    /// uris listed in `fail_delete`.
    #[derive(Default)]
    struct FakeColdWriter {
        fail_delete: Vec<String>,
    }

    impl FormatWriter for FakeColdWriter {
        async fn write(&self, _: &str, _: &RecordBatch) -> Result<usize, FormatError> {
            unreachable!("the scope's write_file hook is the storage seam in these tests")
        }

        async fn delete(&self, uri: &str, _missing_ok: bool) -> Result<(), FormatError> {
            if self.fail_delete.iter().any(|u| u == uri) {
                // NoSegments stands in for any delete failure; the
                // cleanup path only branches on Ok-vs-Err.
                return Err(FormatError::NoSegments);
            }
            Ok(())
        }
    }

    /// A real-but-never-connected `PgDriver`: the no-IO scope hooks are
    /// the only consumers of the pool, and they ignore it.
    fn lazy_pool() -> PgDriver {
        PgDriver::from_pool(
            PgPool::connect_lazy("postgres://unused:unused@localhost:1/unused").unwrap(),
        )
    }

    /// Successful `write_segment` calls record one singleton group each,
    /// uri set — driven through the production path, not a simulation.
    #[tokio::test]
    async fn write_segment_records_singleton_groups() {
        let pool = lazy_pool();
        let cold = FakeColdWriter::default();
        let mut w = DurableSegmentWriter::new(RecordingScope::default());

        w.write_segment(&pool, &cold, &step("s0", "memory://f0"))
            .await
            .unwrap();
        w.write_segment(&pool, &cold, &step("s1", "memory://f1"))
            .await
            .unwrap();

        assert_eq!(w.segment_groups.len(), 2);
        assert_eq!(w.segment_groups[0].seg_uuid_strs, vec!["s0"]);
        assert_eq!(
            w.segment_groups[0].written_uri.as_deref(),
            Some("memory://f0")
        );
        assert_eq!(w.segment_groups[1].seg_uuid_strs, vec!["s1"]);
        assert_eq!(
            w.segment_groups[1].written_uri.as_deref(),
            Some("memory://f1")
        );
    }

    /// A failed file write leaves the production-recorded group
    /// file-less (row recorded, no uri) — the state cleanup needs to
    /// delete the orphan row.
    #[tokio::test]
    async fn failed_file_write_leaves_fileless_group() {
        let pool = lazy_pool();
        let cold = FakeColdWriter::default();
        let mut w = DurableSegmentWriter::new(FailFileWriteScope::default());

        let err = w
            .write_segment(&pool, &cold, &step("orphan", "memory://f0"))
            .await;
        assert!(err.is_err());
        assert_eq!(w.segment_groups.len(), 1);
        assert!(w.segment_groups[0].written_uri.is_none());
        assert_eq!(w.segment_groups[0].seg_uuid_strs, vec!["orphan"]);
    }

    /// A failed insert leaves an opened, EMPTY file-less group — and
    /// cleanup over it is a harmless no-op.
    #[tokio::test]
    async fn failed_insert_leaves_empty_group() {
        let pool = lazy_pool();
        let cold = FakeColdWriter::default();
        let scope = RecordingScope {
            fail_insert_after: Some(0),
            ..Default::default()
        };
        let mut w = DurableSegmentWriter::new(scope);

        assert!(
            w.write_segment(&pool, &cold, &step("never", "memory://f0"))
                .await
                .is_err()
        );
        assert_eq!(w.segment_groups.len(), 1);
        assert!(w.segment_groups[0].written_uri.is_none());
        assert!(w.segment_groups[0].seg_uuid_strs.is_empty());
    }

    /// The cleanup decision, end to end through the production path:
    /// a group whose file delete FAILS keeps its rows (the previously
    /// untested branch); a group whose file deletes loses its rows; a
    /// file-less group always loses its rows.
    #[tokio::test]
    async fn cleanup_keeps_rows_of_surviving_files() {
        let pool = lazy_pool();
        let mut w = DurableSegmentWriter::new(RecordingScope::default());

        // Two written groups via the production path, plus one
        // hand-recorded row-bearing file-less group standing in for the
        // failed-file-write state (it can't be produced here because
        // that failure mode needs FailFileWriteScope, a different scope
        // type that can't share this writer).
        let cold = FakeColdWriter::default();
        w.write_segment(&pool, &cold, &step("kept", "memory://survives"))
            .await
            .unwrap();
        w.write_segment(&pool, &cold, &step("freed", "memory://deleted"))
            .await
            .unwrap();
        w.open_group();
        w.current_group().seg_uuid_strs.push("fileless".to_string());

        let failing_cold = FakeColdWriter {
            fail_delete: vec!["memory://survives".to_string()],
        };
        w.cleanup_on_err(&pool, &failing_cold).await;

        let deleted = w.scope.deleted_rows.lock().unwrap().clone();
        assert_eq!(
            deleted,
            vec!["freed", "fileless"],
            "rows of the surviving file must NOT be deleted; deleted-file and \
             file-less rows must be"
        );
    }
}
