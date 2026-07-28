//! Catalog-wide branch persist / snapshot (CHA-273).
//!
//! [`LifecycleManager::persist_branch`], [`LifecycleManager::snapshot_branch`],
//! and [`LifecycleManager::persist_and_snapshot_branch`] loop the per-table
//! [`persist`](LifecycleManager::persist) / [`snapshot`](LifecycleManager::snapshot)
//! primitives over a `(catalog, branch)`'s dirty set, all bounded at the one fork
//! commit `T` — so the whole catalog-wide flush captures a single consistent
//! snapshot at the fork point. Each returns `T`'s [`Watermark`].
//!
//! `T` is a commit-order position ([`Watermark`] — no tx_uuid). It is either
//! supplied by the caller in `BranchOpRequest.target` (CreateBranch's write path
//! resolves its fork tx to a position first, which structurally can't reference
//! an uncommitted tx) or, when absent, the branch head (`MAX(commit_seq_num)`
//! row, `resolve_head_watermark` — the scheduler's per-tick sweep). Returning the
//! position is what lets CreateBranch seed the child from the exact fork seq
//! (CHA-487) instead of a racy `MAX` re-read.
//!
//! The Persist side enumerates only MODIFIED tables (via
//! [`LifecycleManager::list_modified_tables`], as the scheduler does) — an
//! already-persisted table is already durable, and a table whose only writes are
//! past `T` is a no-op under the per-table `target_micros = T.commit_micros`
//! bound. The Snapshot side enumerates PERSISTED tables instead, so a table
//! persisted then dropped from hot is still re-snapshotted.
//!
//! [`LifecycleManager::persist_branch`] and [`LifecycleManager::snapshot_branch`]
//! fail fast on the first per-table error: the flush is synchronous inside
//! CreateBranch, so a partial flush must surface, not silently succeed.
//! [`LifecycleManager::persist_and_snapshot_branch`] instead continues on error —
//! see its doc.

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
    /// named by `request`, bounded at the fork commit `T`. Returns `T`'s
    /// [`Watermark`]. Supersedes the former inline `flush_branch_to_cold`
    /// (CHA-273 rework): enumerates MODIFIED tables rather than every table, and
    /// returns the resolved fork `T` so the caller can seed from it.
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
    ) -> Result<Watermark, ApiError>
    where
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        let (catalog_uuid, branch_uuid, watermark) = self.resolve_branch_op(pool, request).await?;

        // CHA-507: flush the cold tx_log FIRST — before any data-table persist
        // can flip a segment visible. Cold data segments drop author/comment
        // and depend on the cold tx_log join to reattach them, so the tx_log
        // covering `<= T` must be durable before any data segment referencing
        // those seqs is visible (else `audit_data(include_tx_metadata)` would
        // join a tx_log missing those rows). Fail-fast: CreateBranch needs an
        // all-or-nothing fork flush.
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
        for table_uuid in &table_uuids {
            self.persist(
                pool,
                hot,
                dl_driver,
                writer,
                &persist_request(&catalog_uuid, &branch_uuid, table_uuid, &watermark),
            )
            .await?;
        }
        Ok(watermark)
    }

    /// Snapshot (all-cold merge) every PERSISTED table on the `(catalog, branch)`
    /// named by `request`. Returns the fork [`Watermark`] `T`. Snapshot bounds
    /// itself at each table's latest committed persist, so no per-table micros
    /// bound is threaded here (mirrors the scheduler's `snapshot_one`).
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
    ) -> Result<Watermark, ApiError>
    where
        R: FormatReader,
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        let (catalog_uuid, branch_uuid, watermark) = self.resolve_branch_op(pool, request).await?;
        let table_uuids = self
            .list_all_persisted_table_uuids(pool, &catalog_uuid, &branch_uuid)
            .await?;
        for table_uuid in &table_uuids {
            self.snapshot(
                pool,
                readers,
                dl_driver,
                writer,
                &snapshot_request(&catalog_uuid, &branch_uuid, table_uuid),
            )
            .await?;
        }
        Ok(watermark)
    }

    /// Persist every MODIFIED table, then Snapshot every PERSISTED table, on the
    /// `(catalog, branch)` named by `request`, per table (non-atomic). Returns
    /// the fork [`Watermark`] `T`.
    ///
    /// The two phases enumerate **distinct dirty-sets** — Persist the hot-modified
    /// set, Snapshot the post-persist persisted set. See the inline note on the
    /// Snapshot phase for why.
    ///
    /// **Continue-on-error**, matching the old `ops::persist_and_snapshot` shape
    /// the scheduler relied on: a per-table failure is logged and the loop
    /// proceeds, so a persistent poison table cannot starve the healthy tail of
    /// the modified set (which the deterministic `list_modified_tables` order
    /// would otherwise return first every tick). Each table self-heals when it
    /// re-enters a future sweep. Snapshot is gated on the same table's Persist
    /// succeeding — Snapshot's watermark is bounded by the latest committed
    /// persist, so running it after a failed Persist would no-op or replay stale
    /// state. This differs from [`Self::persist_branch`], which fails fast because
    /// its CreateBranch caller needs an all-or-nothing fork flush.
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
    ) -> Result<Watermark, ApiError>
    where
        R: FormatReader,
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        let (catalog_uuid, branch_uuid, watermark) = self.resolve_branch_op(pool, request).await?;

        // CHA-507: flush the cold tx_log FIRST, before any data-table persist
        // (see `persist_branch` for the visibility invariant). Fail the whole
        // persist phase if it errors — a data segment must never flip visible
        // without its tx metadata durable in cold; the scheduler retries next
        // tick. This is a deliberate exception to this function's otherwise
        // continue-on-error philosophy: the tx_log flush is a correctness
        // prerequisite for every table, not a per-table best-effort step, so a
        // persistent failure correctly stalls the whole branch until resolved.
        self.persist_tx_log(
            pool,
            writer,
            &catalog_uuid,
            &branch_uuid,
            watermark.commit_seq_num,
        )
        .await?;

        // CHA-509: two phases with distinct dirty-sets. Persist enumerates
        // hot-modified tables; Snapshot enumerates persisted tables (the
        // post-persist superset — see `list_all_persisted_table_uuids`). A table
        // whose Persist fails gets no `table_persist_metadata` row, so it never
        // enters the persisted set and is not snapshotted — preserving the old
        // "Persist failed → skip Snapshot" continue-on-error semantics.
        let modified = self
            .list_all_modified_table_uuids(pool, &catalog_uuid, &branch_uuid)
            .await?;
        for table_uuid in &modified {
            if let Err(e) = self
                .persist(
                    pool,
                    hot,
                    dl_driver,
                    writer,
                    &persist_request(&catalog_uuid, &branch_uuid, table_uuid, &watermark),
                )
                .await
            {
                tracing::warn!(
                    catalog = %catalog_uuid,
                    branch = %branch_uuid,
                    table = %table_uuid,
                    error = %e,
                    "branch Persist failed; skipping Snapshot, continuing"
                );
            }
        }
        let persisted = self
            .list_all_persisted_table_uuids(pool, &catalog_uuid, &branch_uuid)
            .await?;
        for table_uuid in &persisted {
            if let Err(e) = self
                .snapshot(
                    pool,
                    readers,
                    dl_driver,
                    writer,
                    &snapshot_request(&catalog_uuid, &branch_uuid, table_uuid),
                )
                .await
            {
                tracing::warn!(
                    catalog = %catalog_uuid,
                    branch = %branch_uuid,
                    table = %table_uuid,
                    error = %e,
                    "branch Snapshot failed; continuing"
                );
            }
        }
        Ok(watermark)
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
        // The fork position is either supplied by the caller (a resolved
        // `Watermark` — CreateBranch's write path resolves its fork tx to a
        // position first, which structurally can't reference an uncommitted tx)
        // or, when unset, the branch head (the scheduler's per-tick sweep).
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
    /// purged past its snapshot still gets re-snapshotted — the load-shed
    /// recovery path (CHA-509). Today `persisted ⊆ modified` (Purge is
    /// snapshot-gated, `Pu = W_snap`), so this is behavior-equivalent; it
    /// becomes load-bearing once a purge-past-snapshot valve (`Pu > W_snap`)
    /// drops a still-unsnapshotted table out of the modified set.
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

/// The persist request for one table in a branch flush: bounded at the fork
/// `T.commit_micros` so persist's strict-advance gate no-ops any table already
/// durable past the fork.
///
/// TODO(CHA-500): the fork bound is `T.commit_micros`, but `T` is fundamentally
/// a `commit_seq_num` — and `watermark.commit_seq_num` is already resolved and
/// in hand here. `commit_micros` is only *non-strictly* monotonic, so a
/// same-micros/higher-seq source commit can leak into the source cold tier
/// (harmless for the flush — idempotent — but see the CHA-178 read-side fence in
/// `WriteManager::create_branch`). Once CHA-500 gives Persist a `target_seq`,
/// switch this line to `target_seq: Some(watermark.commit_seq_num)` and the
/// flush becomes seq-exact.
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
