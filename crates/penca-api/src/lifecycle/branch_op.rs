//! Catalog-wide branch persist / snapshot.
//!
//! [`LifecycleManager::persist_branch`], [`LifecycleManager::snapshot_branch`],
//! and [`LifecycleManager::persist_and_snapshot_branch`] loop the per-table
//! [`persist`](LifecycleManager::persist) / [`snapshot`](LifecycleManager::snapshot)
//! primitives over a `(catalog, branch)`'s dirty set. Each returns `Some(T)`
//! when every table succeeded and `None` when any did not — see the
//! continue-on-error paragraph below.
//!
//! Only the **Persist** side is bounded at `T` (`persist_request` sets
//! `target_micros = T.commit_micros`), which is what makes a fork flush capture
//! a single consistent position. Snapshot bounds itself at each table's own
//! latest committed persist instead, so it needs no per-table micros bound.
//!
//! `T` is a commit-order position ([`Watermark`] — no tx_uuid). It is either
//! supplied by the caller in `BranchOpRequest.target` (CreateBranch's write path
//! resolves its fork tx to a position first, which structurally can't reference
//! an uncommitted tx) or, when absent, the branch head (`MAX(commit_seq_num)`
//! row, `resolve_head_watermark` — the scheduler's per-tick sweep). Returning the
//! position is what lets CreateBranch seed the child from the exact fork seq
//! instead of a racy `MAX` re-read.
//!
//! The Persist side enumerates only MODIFIED tables (via
//! [`LifecycleManager::list_modified_tables`], as the scheduler does) — an
//! already-persisted table is already durable, and a table whose only writes are
//! past `T` is a no-op under the per-table `target_micros = T.commit_micros`
//! bound. The Snapshot side enumerates PERSISTED tables instead, so a table
//! persisted then dropped from hot is still re-snapshotted.
//!
//! All three are **continue-on-error** per table: a failure is logged, the loop
//! proceeds, and the returned watermark is withheld (`None`). Aborting instead
//! would be unsafe — both dirty sets are enumerated oldest-timestamp-first, so a
//! table whose op keeps failing sorts first on every subsequent sweep and would
//! starve everything behind it forever.
//!
//! An absent watermark is therefore the partial-completion signal, and callers
//! needing an all-or-nothing flush (CreateBranch, whose child reads the parent's
//! COLD tier) MUST treat it as an error rather than record the fork.
//!
//! The one step that still fails fast is `persist_tx_log`, which is a
//! correctness prerequisite for every table (CHA-507) rather than per-table
//! best-effort work.

use std::collections::HashMap;

use penca_core::naming::commit_tx_log_partition;
use penca_db::dialect::Dialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::DbDriver;
use penca_db::driver::pg::PgDriver;
use penca_dl::driver::DlDriver;
use penca_format::reader::FormatReader;
use penca_format::writer::FormatWriter;
use penca_proto::external::v1::{
    BranchOpRequest, ListModifiedTablesRequest, ListPersistedTablesRequest, PaginationRequest,
    PersistRequest, SnapshotRequest, Watermark,
};
use penca_storage_hot::HotStorageClient;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;
use crate::resolve::{parse_resolved_uuid, resolve_branch, resolve_catalog};

impl LifecycleManager {
    /// Persist (flush hot→cold) every MODIFIED table on the `(catalog, branch)`
    /// named by `request`, bounded at the fork commit `T`.
    ///
    /// Returns `Some(T)` only when EVERY table flushed. A per-table failure is
    /// logged and the loop continues, and the watermark is then **withheld**
    /// (`None`) — the absent watermark is the partial-flush signal.
    ///
    /// Continue-on-error is load-bearing for the scheduler's persist loop: the
    /// dirty-set enumeration is `ORDER BY MAX(modified_at_micros) ASC`
    /// (`penca-storage-meta/src/lifecycle.rs:192`), and a table whose Persist
    /// keeps failing never advances its `modified_at`, so it sorts FIRST on every
    /// subsequent tick. Aborting on it would permanently starve the rest of the
    /// branch, growing hot storage without bound.
    ///
    /// Callers needing an all-or-nothing flush (CreateBranch) MUST treat `None`
    /// as a failure — a partial flush leaves the fork's child reading a parent
    /// cold tier that is missing the unflushed tables' rows.
    ///
    /// The `persist_tx_log` call is the one step that still fails fast: it is a
    /// correctness prerequisite for every table (CHA-507), not per-table
    /// best-effort work.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog = ?request.catalog_uuid,
            branch = ?request.branch_uuid,
            target = ?request.target,
        ),
    )]
    pub async fn persist_branch<L, W>(
        &self,
        pool: &PgDriver,
        hot: &HotStorageClient,
        dl_driver: &L,
        writer: &W,
        request: &BranchOpRequest,
    ) -> Result<Option<Watermark>, ApiError>
    where
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        let (catalog_uuid, branch_uuid, watermark) = self.resolve_branch_op(pool, request).await?;

        // Flush the cold tx_log FIRST — before any data-table persist
        // can flip a segment visible. Cold data segments drop author/comment
        // and depend on the cold tx_log join to reattach them, so the tx_log
        // covering `<= T` must be durable before any data segment referencing
        // those seqs is visible (else `audit_data(include_tx_metadata)` would
        // join a tx_log missing those rows). Fails fast because it is a
        // prerequisite for EVERY table on the branch, not per-table best-effort
        // work — unlike the data-table loop below, there is no partial success
        // worth keeping. (CreateBranch's all-or-nothing property comes from its
        // own watermark-presence check, not from this `?`.)
        self.persist_tx_log(
            pool,
            writer,
            &catalog_uuid,
            &branch_uuid,
            watermark.commit_seq_num,
        )
        .await?;

        let table_uuids = self
            .list_all_modified_table_uuids(pool, &catalog_uuid, &branch_uuid)
            .await?;
        let failed = self
            .persist_each(
                pool,
                hot,
                dl_driver,
                writer,
                &catalog_uuid,
                &branch_uuid,
                &table_uuids,
                &watermark,
            )
            .await;
        Ok(branch_op_watermark(
            &catalog_uuid,
            &branch_uuid,
            watermark,
            failed,
            table_uuids.len(),
            "Persist",
        ))
    }

    /// Snapshot (all-cold merge) every PERSISTED table on the `(catalog, branch)`
    /// named by `request`. Snapshot bounds itself at each table's latest
    /// committed persist, so no per-table micros bound is threaded here.
    ///
    /// Returns `Some(T)` only when EVERY table snapshotted; a per-table failure
    /// is logged, the loop continues, and the watermark is withheld. Same
    /// starvation argument as [`Self::persist_branch`] — the persisted-set
    /// enumeration is `ORDER BY MAX(commit_micros) ASC`
    /// (`penca-storage-meta/src/lifecycle.rs:262`), so a poison table would sort
    /// first forever. It compounds there: Purge's committed axis is gated on
    /// `Pu = W_snap`, so a starved tail's hot rows are never reclaimed either.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog = ?request.catalog_uuid,
            branch = ?request.branch_uuid,
            target = ?request.target,
        ),
    )]
    pub async fn snapshot_branch<R, L, W>(
        &self,
        pool: &PgDriver,
        readers: &HashMap<i32, R>,
        dl_driver: &L,
        writer: &W,
        request: &BranchOpRequest,
    ) -> Result<Option<Watermark>, ApiError>
    where
        R: FormatReader,
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        let (catalog_uuid, branch_uuid, watermark) = self.resolve_branch_op(pool, request).await?;
        let table_uuids = self
            .list_all_persisted_table_uuids(pool, &catalog_uuid, &branch_uuid)
            .await?;
        let failed = self
            .snapshot_each(
                pool,
                readers,
                dl_driver,
                writer,
                &catalog_uuid,
                &branch_uuid,
                &table_uuids,
            )
            .await;
        Ok(branch_op_watermark(
            &catalog_uuid,
            &branch_uuid,
            watermark,
            failed,
            table_uuids.len(),
            "Snapshot",
        ))
    }

    /// Persist every MODIFIED table, then Snapshot every PERSISTED table, on the
    /// `(catalog, branch)` named by `request`, per table (non-atomic).
    ///
    /// The two phases enumerate **distinct dirty-sets** — Persist the hot-modified
    /// set, Snapshot the post-persist persisted set. See the inline note on the
    /// Snapshot phase for why.
    ///
    /// **No server-side caller.** The scheduler drives [`Self::persist_branch`]
    /// and [`Self::snapshot_branch`] from its two independently-paced loops; this
    /// stays as a client-facing convenience for callers wanting both phases in
    /// one round-trip.
    ///
    /// Continue-on-error like its two halves — a per-table failure is logged, the
    /// loop proceeds, and the watermark is withheld. Snapshot is additionally
    /// gated on the same table's Persist succeeding: Snapshot's watermark is
    /// bounded by the latest committed persist, so running it after a failed
    /// Persist would no-op or replay stale state. Here that gating is structural
    /// rather than explicit — a table whose Persist failed writes no
    /// `table_persist_metadata` row, so it never enters the persisted set the
    /// Snapshot phase enumerates.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog = ?request.catalog_uuid,
            branch = ?request.branch_uuid,
            target = ?request.target,
        ),
    )]
    #[allow(clippy::too_many_arguments)]
    pub async fn persist_and_snapshot_branch<R, L, W>(
        &self,
        pool: &PgDriver,
        hot: &HotStorageClient,
        readers: &HashMap<i32, R>,
        dl_driver: &L,
        writer: &W,
        request: &BranchOpRequest,
    ) -> Result<Option<Watermark>, ApiError>
    where
        R: FormatReader,
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        let (catalog_uuid, branch_uuid, watermark) = self.resolve_branch_op(pool, request).await?;

        // See `persist_branch` for the tx_log-before-data visibility invariant.
        // Deliberate exception to this function's continue-on-error philosophy:
        // the tx_log flush is a correctness prerequisite for EVERY table, not a
        // per-table best-effort step, so a persistent failure should stall the
        // whole branch until resolved (the scheduler retries next tick).
        self.persist_tx_log(
            pool,
            writer,
            &catalog_uuid,
            &branch_uuid,
            watermark.commit_seq_num,
        )
        .await?;

        // The two phases enumerate distinct dirty-sets: Persist over
        // hot-modified tables, Snapshot over persisted ones. That is what
        // implements "Persist failed → skip Snapshot" — a table whose Persist
        // fails gets no `table_persist_metadata` row, so it never enters the
        // persisted set.
        let modified = self
            .list_all_modified_table_uuids(pool, &catalog_uuid, &branch_uuid)
            .await?;
        let persist_failed = self
            .persist_each(
                pool,
                hot,
                dl_driver,
                writer,
                &catalog_uuid,
                &branch_uuid,
                &modified,
                &watermark,
            )
            .await;
        let persisted = self
            .list_all_persisted_table_uuids(pool, &catalog_uuid, &branch_uuid)
            .await?;
        let snapshot_failed = self
            .snapshot_each(
                pool,
                readers,
                dl_driver,
                writer,
                &catalog_uuid,
                &branch_uuid,
                &persisted,
            )
            .await;
        if persist_failed + snapshot_failed > 0 {
            tracing::warn!(
                catalog = %catalog_uuid,
                branch = %branch_uuid,
                persist_failed,
                persist_total = modified.len(),
                snapshot_failed,
                snapshot_total = persisted.len(),
                "branch PersistAndSnapshot incomplete; withholding watermark"
            );
            return Ok(None);
        }
        Ok(Some(watermark))
    }

    /// Persist each table in an already-enumerated set, returning how many
    /// failed. A per-table failure is logged and the loop continues — see the
    /// module doc for why aborting would starve the branch.
    ///
    /// Takes the set rather than enumerating it because the caller needs the
    /// count: `persist_and_snapshot_branch` sums this against
    /// [`Self::snapshot_each`]'s before deciding whether to withhold the
    /// watermark, so the decision cannot move in here.
    ///
    /// Kept separate from [`Self::snapshot_each`] rather than parameterized —
    /// they call different primitives with different argument sets, so unifying
    /// would need a closure or a mode flag.
    #[allow(clippy::too_many_arguments)]
    async fn persist_each<L, W>(
        &self,
        pool: &PgDriver,
        hot: &HotStorageClient,
        dl_driver: &L,
        writer: &W,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuids: &[String],
        watermark: &Watermark,
    ) -> usize
    where
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        let mut failed = 0usize;
        for table_uuid in table_uuids {
            if let Err(e) = self
                .persist(
                    pool,
                    hot,
                    dl_driver,
                    writer,
                    &persist_request(catalog_uuid, branch_uuid, table_uuid, watermark),
                )
                .await
            {
                failed += 1;
                tracing::warn!(
                    catalog = %catalog_uuid,
                    branch = %branch_uuid,
                    table = %table_uuid,
                    error = %e,
                    "branch Persist failed; continuing"
                );
            }
        }
        failed
    }

    /// Snapshot each table in an already-enumerated set, returning how many
    /// failed. Sibling of [`Self::persist_each`], which carries the shared
    /// rationale for the set parameter and for keeping the two separate.
    #[allow(clippy::too_many_arguments)]
    async fn snapshot_each<R, L, W>(
        &self,
        pool: &PgDriver,
        readers: &HashMap<i32, R>,
        dl_driver: &L,
        writer: &W,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuids: &[String],
    ) -> usize
    where
        R: FormatReader,
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        let mut failed = 0usize;
        for table_uuid in table_uuids {
            if let Err(e) = self
                .snapshot(
                    pool,
                    readers,
                    dl_driver,
                    writer,
                    &snapshot_request(catalog_uuid, branch_uuid, table_uuid),
                )
                .await
            {
                failed += 1;
                tracing::warn!(
                    catalog = %catalog_uuid,
                    branch = %branch_uuid,
                    table = %table_uuid,
                    error = %e,
                    "branch Snapshot failed; continuing"
                );
            }
        }
        failed
    }

    /// Shared skeleton entry: resolve the `(catalog, branch)` the branch op
    /// scopes to and the fork [`Watermark`] `T` — once, before any per-table
    /// work, so `T` is atomic w.r.t. the persist loop that follows.
    async fn resolve_branch_op(
        &self,
        pool: &PgDriver,
        request: &BranchOpRequest,
    ) -> Result<(Uuid, Uuid, Watermark), ApiError> {
        let catalog_obj = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog_obj.catalog_uuid, "catalog_uuid")?;
        let branch_obj = resolve_branch(
            pool,
            &catalog_uuid,
            request.branch_uuid.as_deref(),
            request.branch_name.as_deref(),
        )
        .await?;
        let branch_uuid = parse_resolved_uuid(&branch_obj.branch_uuid, "branch_uuid")?;
        let watermark = match &request.target {
            Some(target) => *target,
            None => {
                self.resolve_head_watermark(pool, &catalog_uuid, &branch_uuid)
                    .await?
            }
        };
        Ok((catalog_uuid, branch_uuid, watermark))
    }

    /// The branch head as a [`Watermark`] — the `MAX(commit_seq_num)` committed
    /// row on `branch_uuid`'s `commit_tx_log` partition. Used only when a branch
    /// op has no explicit `target` (the scheduler's per-tick sweep bounds at
    /// head). Partition-direct, mirroring the write path's `ensure_fast_forward`
    /// read.
    ///
    /// An empty branch (no committed tx — e.g. a freshly forked child swept
    /// before its first write) has no head: return a default watermark so the
    /// persist loop (bounded at commit_micros = 0) no-ops instead of erroring.
    async fn resolve_head_watermark(
        &self,
        pool: &PgDriver,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
    ) -> Result<Watermark, ApiError> {
        let tx_part = commit_tx_log_partition(catalog_uuid, branch_uuid);
        let tx_q = PgDialect::quote_identifier(&tx_part);
        let sql = format!(
            "SELECT commit_seq_num, commit_micros FROM {tx_q} \
             ORDER BY commit_seq_num DESC LIMIT 1"
        );
        match pool.fetch_optional(&sql, &[]).await? {
            Some(row) => watermark_from_row(&row),
            None => Ok(Watermark::default()),
        }
    }

    /// Every MODIFIED `table_uuid` on `(catalog, branch)`, paginated to
    /// exhaustion. Unbounded window — the per-table `target_micros = T` bound in
    /// the persist loop, not this enumeration, is what cuts the flush at the fork.
    async fn list_all_modified_table_uuids(
        &self,
        pool: &PgDriver,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
    ) -> Result<Vec<String>, ApiError> {
        let mut all = Vec::new();
        let mut page_token = String::new();
        loop {
            let response = self
                .list_modified_tables(
                    pool,
                    &ListModifiedTablesRequest {
                        catalog_uuid: catalog_uuid.to_string(),
                        branch_uuid: branch_uuid.to_string(),
                        modified_at: None,
                        pagination: Some(PaginationRequest {
                            page_size: 0,
                            page_token: page_token.clone(),
                        }),
                    },
                )
                .await?;
            all.extend(response.table_uuids);
            match response.next_page_token {
                Some(token) if !token.is_empty() => page_token = token,
                _ => break,
            }
        }
        Ok(all)
    }

    /// Every table on `(catalog, branch)` with committed cold persist data,
    /// paginated to exhaustion (unbounded window). The snapshot sweep enumerates
    /// on **persisted** rather than **modified** so a table whose hot rows were
    /// purged past its snapshot still gets re-snapshotted (the load-shed
    /// recovery path). While Purge stays snapshot-gated (`Pu = W_snap`),
    /// `persisted ⊆ modified` and the choice is behavior-equivalent; it becomes
    /// load-bearing once a purge-past-snapshot valve (`Pu > W_snap`) can drop a
    /// still-unsnapshotted table out of the modified set.
    async fn list_all_persisted_table_uuids(
        &self,
        pool: &PgDriver,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
    ) -> Result<Vec<String>, ApiError> {
        let mut all = Vec::new();
        let mut page_token = String::new();
        loop {
            let response = self
                .list_persisted_tables(
                    pool,
                    &ListPersistedTablesRequest {
                        catalog_uuid: catalog_uuid.to_string(),
                        branch_uuid: branch_uuid.to_string(),
                        persisted_at: None,
                        pagination: Some(PaginationRequest {
                            page_size: 0,
                            page_token: page_token.clone(),
                        }),
                    },
                )
                .await?;
            all.extend(response.table_uuids);
            match response.next_page_token {
                Some(token) if !token.is_empty() => page_token = token,
                _ => break,
            }
        }
        Ok(all)
    }
}

/// The branch op's watermark, withheld when any table failed.
///
/// An absent watermark is the partial-completion signal on `BranchOpResponse`
/// — richer per-table response metadata is deferred until a caller needs it.
fn branch_op_watermark(
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    watermark: Watermark,
    failed: usize,
    total: usize,
    op: &str,
) -> Option<Watermark> {
    if failed == 0 {
        return Some(watermark);
    }
    // `op` is a field, not interpolated into the message, so these events group
    // and filter like the sibling per-table warns.
    tracing::warn!(
        catalog = %catalog_uuid,
        branch = %branch_uuid,
        op,
        failed,
        total,
        "branch op incomplete; withholding watermark"
    );
    None
}

/// The persist request for one table in a branch flush: bounded at the fork
/// `T.commit_micros` so persist's strict-advance gate no-ops any table already
/// durable past the fork.
///
/// TODO(CHA-500): the fork bound is `T.commit_micros`, but `T` is fundamentally
/// a `commit_seq_num` — and `watermark.commit_seq_num` is already resolved and
/// in hand here. `commit_micros` is only *non-strictly* monotonic, so a
/// same-micros/higher-seq source commit can leak into the source cold tier
/// (harmless for the flush — idempotent — but see the read-side fence in
/// `WriteManager::create_branch`). Once CHA-500 gives Persist a `target_seq`,
/// switch this to `target_seq: Some(watermark.commit_seq_num)` and the flush
/// becomes seq-exact.
fn persist_request(
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    table_uuid: &str,
    watermark: &Watermark,
) -> PersistRequest {
    PersistRequest {
        catalog_uuid: Some(catalog_uuid.to_string()),
        branch_uuid: Some(branch_uuid.to_string()),
        table_uuid: Some(table_uuid.to_string()),
        target_micros: Some(watermark.commit_micros),
        ..Default::default()
    }
}

/// The snapshot request for one table in a branch snapshot. Snapshot bounds
/// itself at the latest committed persist, so no explicit micros bound.
fn snapshot_request(catalog_uuid: &Uuid, branch_uuid: &Uuid, table_uuid: &str) -> SnapshotRequest {
    SnapshotRequest {
        catalog_uuid: Some(catalog_uuid.to_string()),
        branch_uuid: Some(branch_uuid.to_string()),
        table_uuid: Some(table_uuid.to_string()),
        ..Default::default()
    }
}

/// Build a [`Watermark`] position from a `commit_tx_log` row (`commit_seq_num` /
/// `commit_micros` BIGINT). `tx_uuid` is intentionally not read — the watermark
/// is a commit-order position, not a tx identity.
fn watermark_from_row(row: &PgRow) -> Result<Watermark, ApiError> {
    let commit_seq_num = row
        .try_get::<i64, _>("commit_seq_num")
        .map_err(|e| ApiError::Metadata(e.into()))?;
    let commit_micros = row
        .try_get::<i64, _>("commit_micros")
        .map_err(|e| ApiError::Metadata(e.into()))?;
    Ok(Watermark {
        commit_seq_num,
        commit_micros,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn watermark() -> Watermark {
        Watermark {
            commit_seq_num: 42,
            commit_micros: 1_700_000_000_000_000,
        }
    }

    /// The all-succeeded path returns the fork position the op was bounded at.
    #[test]
    fn no_failures_yields_the_watermark() {
        let got = branch_op_watermark(&Uuid::nil(), &Uuid::nil(), watermark(), 0, 3, "Persist");

        assert_eq!(got, Some(watermark()));
    }

    /// A withheld watermark is the ONLY partial-completion signal on
    /// `BranchOpResponse` — CreateBranch aborts the fork on absence, and both
    /// scheduler loops warn on it. One failure is enough to withhold; a caller
    /// must not be able to read "mostly succeeded" as success.
    #[test]
    fn any_failure_withholds_the_watermark() {
        for (failed, total) in [(1, 3), (2, 3), (3, 3), (1, 1)] {
            assert_eq!(
                branch_op_watermark(
                    &Uuid::nil(),
                    &Uuid::nil(),
                    watermark(),
                    failed,
                    total,
                    "Persist"
                ),
                None,
                "{failed}/{total} failures must withhold"
            );
        }
    }

    /// An empty dirty set is success, not partial completion — a branch with
    /// nothing to flush must still hand CreateBranch a usable fork position.
    #[test]
    fn an_empty_sweep_yields_the_watermark() {
        let got = branch_op_watermark(&Uuid::nil(), &Uuid::nil(), watermark(), 0, 0, "Snapshot");

        assert_eq!(got, Some(watermark()));
    }
}
