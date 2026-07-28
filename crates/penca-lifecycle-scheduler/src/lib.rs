//! v0 single-replica tick-loop microservice driving per-table
//! `Persist → Snapshot → Purge` across every `(catalog, branch)`.
//!
//! ## Tick loop
//!
//! Every `SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS` (the single loop still
//! drives all three ops; `SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS` is parsed
//! but unused until the two-loop split — TODO(CHA-513)):
//!
//! ```text
//! for each catalog (ListCatalogs):
//!   for each branch in catalog (ListBranches):
//!     now = system clock micros
//!
//!     // Persist + Snapshot every modified table server-side in one RPC
//!     // (the per-table loop lives in LifecycleManager now, CHA-273).
//!     PersistAndSnapshotBranch(catalog, branch)
//!     // Purge each modified table in the same pass (CHA-444) — still a
//!     // per-table client loop because Purge is a per-table RPC.
//!     modified = paginate ListModifiedTables(catalog, branch,
//!                  [last_modified_tick, now))
//!     for T in modified:
//!       Purge(T)
//!     last_modified_tick[catalog, branch] = now
//!
//!     // Tables whose latest committed persist has cleared the
//!     // universal grace window (ADR 0019) → Purge.
//!     purge_upper = now - QUERY_TIMEOUT_SECONDS_micros
//!     if purge_upper > last_purge_tick:
//!       persisted = paginate ListPersistedTables(catalog, branch,
//!                     [last_purge_tick, purge_upper))
//!       for T in persisted:
//!         Purge(T)
//!       last_purge_tick[catalog, branch] = purge_upper
//!
//!     // Branch-scoped GC of the four hot tx-log family tables
//!     // (CHA-221). Unconditional per tick — the RPC's own
//!     // empty-set fast-path is the no-op gate, no scheduler
//!     // watermark needed.
//!     PurgeTxLog(catalog, branch)
//! ```
//!
//! `last_modified_tick` and `last_purge_tick` live in process memory
//! only. Restart resets both to `0`, making the first post-restart
//! tick a full sweep — safe because `Persist`, `Snapshot`, and `Purge`
//! are idempotent (each no-ops when the watermark already covers the
//! requested range).
//!
//! ## Failure semantics
//!
//! Per-table errors inside a tick are logged and swallowed; the watermark
//! still advances. The next tick re-enumerates only tables that fall in
//! the next window, so a one-off transient failure on a table that then
//! goes idle is **not** retried automatically — its cold migration waits
//! until the table receives further committed writes (for
//! `Persist`+`Snapshot`) or further committed persists (for `Purge`),
//! at which point the table re-enters the enumeration window and is
//! retried. Tables with continuing traffic self-heal on the next sweep.
//! Durable per-table retry queues are deferred past v0.
//!
//! ## Mechanism contract
//!
//! The scheduler is a pure gRPC client. It does NOT import
//! `LifecycleManager` or talk to Postgres directly — all data access
//! flows through `QueryServiceClient` and `LifecycleServiceClient`
//! (CHA-445 rehomed the dirty-set listing RPCs onto Lifecycle, dropping
//! the StorageMetadataService client). Persist + Snapshot over a branch's
//! modified tables is one server-side RPC
//! (`LifecycleServiceClient::persist_and_snapshot_branch`, CHA-273 — the
//! per-table loop moved into `LifecycleManager`); Purge and PurgeTxLog stay
//! the existing per-table / per-branch RPCs.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use penca_proto::external::v1::{Branch, BranchOpRequest, Catalog, PurgeTxLogRequest};
use tonic::transport::Channel;

pub mod config;
mod discovery;
mod ops;
mod paginate;

pub use crate::config::SchedulerConfig;

/// Errors surfaced at the scheduler's algorithmic boundary
/// (`tick` / `tick_branch`).
///
/// Today every variant wraps a `tonic::Status` since the scheduler is a
/// pure gRPC client, but the enum exists so future non-RPC errors
/// (config validation, durable-watermark serialization, signal handling)
/// can join the type cleanly without re-wrapping as `tonic::Status`.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("gRPC error: {0}")]
    Transport(#[from] tonic::Status),
}

/// One in-memory watermark pair per `(catalog_uuid, branch_uuid)`.
#[derive(Default, Clone, Copy)]
struct BranchWatermarks {
    /// Upper bound of the most recent `ListModifiedTables` sweep on
    /// this branch. The next sweep's lower bound is this value, so the
    /// half-open `[last, now)` windows tile with no gap.
    last_modified_tick: i64,
    /// Upper bound of the most recent `ListPersistedTables` sweep on
    /// this branch. The next sweep's lower bound is this value.
    last_purge_tick: i64,
}

/// V0 scheduler.
///
/// Owns the three gRPC clients plus the in-memory per-branch
/// watermarks. `tick()` runs one sweep over the whole cluster;
/// `run()` is the steady-state `loop { tick(); sleep }`.
pub struct Scheduler {
    query: QueryServiceClient<Channel>,
    lifecycle: LifecycleServiceClient<Channel>,

    /// `None` disables the tick loop — [`Self::run`] logs a warning
    /// and idles forever. Used by the integration test profile so the
    /// suite can assert RPC behavior without the autonomous loop
    /// racing manual lifecycle calls.
    tick_interval: Option<Duration>,
    list_page_size: i32,
    /// Universal grace window in micros; matches the lifecycle service's
    /// `query_timeout_micros`. Used to bound the Purge enumeration's
    /// upper edge at `now - grace_window`.
    grace_window_micros: i64,

    /// Per-`(catalog, branch)` enumeration watermarks. Never persisted
    /// — restart resets to zero, making the first post-restart tick a
    /// full sweep over committed history.
    watermarks: HashMap<(String, String), BranchWatermarks>,
}

impl Scheduler {
    /// Build a scheduler from already-connected channels and the
    /// resolved config. `main` typically calls
    /// [`Self::from_config_and_channels`] instead.
    pub fn new(
        query: QueryServiceClient<Channel>,
        lifecycle: LifecycleServiceClient<Channel>,
        tick_interval: Option<Duration>,
        list_page_size: i32,
        grace_window_micros: i64,
    ) -> Self {
        Self {
            query,
            lifecycle,
            tick_interval,
            list_page_size,
            grace_window_micros,
            watermarks: HashMap::new(),
        }
    }

    /// Run forever: `tick` + `sleep(tick_interval)`. Errors inside a
    /// tick are logged and the loop continues — every lifecycle op is
    /// idempotent, so transient failures self-heal on any subsequent
    /// sweep that re-enumerates the table (see "Failure semantics" on
    /// the module doc for when a table is re-enumerated).
    ///
    /// When `tick_interval` is `None` (a non-positive configured cadence),
    /// the scheduler logs a warning and idles forever without firing any
    /// lifecycle ops.
    pub async fn run(&mut self) -> ! {
        let Some(interval) = self.tick_interval else {
            tracing::warn!(
                env_var = "SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS",
                "scheduler disabled: configured cadence is non-positive, \
                 no lifecycle ops will fire"
            );
            std::future::pending::<()>().await;
            unreachable!("std::future::pending never resolves");
        };
        loop {
            let _ = self.tick().await;
            tokio::time::sleep(interval).await;
        }
    }

    /// One sweep over every `(catalog, branch)`. Public for testability
    /// and so future work (admin RPC, hot-reload) can poke the
    /// scheduler externally.
    #[tracing::instrument(skip_all, err)]
    pub async fn tick(&mut self) -> Result<(), SchedulerError> {
        let catalogs = discovery::list_all_catalogs(&mut self.query, self.list_page_size).await?;
        for catalog in catalogs {
            let branches = discovery::list_all_branches(
                &mut self.query,
                self.list_page_size,
                &catalog.catalog_uuid,
            )
            .await?;
            for branch in branches {
                if let Err(e) = self.tick_branch(&catalog, &branch).await {
                    tracing::warn!(
                        catalog = %catalog.catalog_uuid,
                        branch = %branch.branch_uuid,
                        error = %e,
                        "tick_branch failed; will retry next tick"
                    );
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %catalog.catalog_uuid,
            branch = %branch.branch_uuid,
        ),
    )]
    async fn tick_branch(
        &mut self,
        catalog: &Catalog,
        branch: &Branch,
    ) -> Result<(), SchedulerError> {
        let key = (catalog.catalog_uuid.clone(), branch.branch_uuid.clone());
        let mut wm = self.watermarks.get(&key).copied().unwrap_or_default();
        let now = system_now_micros();

        self.sweep_modified(catalog, branch, &mut wm, now).await?;
        self.sweep_persisted(catalog, branch, &mut wm, now).await?;
        self.sweep_tx_log(catalog, branch).await;

        self.watermarks.insert(key, wm);
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %catalog.catalog_uuid,
            branch = %branch.branch_uuid,
            window_start = wm.last_modified_tick,
            window_end = now,
        ),
    )]
    async fn sweep_modified(
        &mut self,
        catalog: &Catalog,
        branch: &Branch,
        wm: &mut BranchWatermarks,
        now: i64,
    ) -> Result<(), SchedulerError> {
        // CHA-273 rework: Persist + Snapshot every modified table server-side in
        // one RPC — the per-table loop now lives in
        // `LifecycleManager::persist_and_snapshot_branch` (fail-fast there). A
        // failure is logged and this pass continues, so Purge still runs and the
        // watermark still advances; the branch re-enters the next sweep and
        // retries. `target` unset ⇒ the op bounds at the branch head.
        if let Err(e) = self
            .lifecycle
            .persist_and_snapshot_branch(BranchOpRequest {
                catalog_uuid: Some(catalog.catalog_uuid.clone()),
                branch_uuid: Some(branch.branch_uuid.clone()),
                ..Default::default()
            })
            .await
        {
            tracing::warn!(
                catalog = %catalog.catalog_uuid,
                branch = %branch.branch_uuid,
                error = %e,
                "PersistAndSnapshotBranch failed"
            );
        }

        // Purge each modified table in the same pass (CHA-444 / ADR 0027).
        // `list_modified_tables` unions committed AND aborted writers, so this
        // clears committed hot rows (advancing Pu to the just-committed W_snap),
        // aborted hot rows, and expired-begin garbage — including for aborts-only
        // tables that never reach `list_persisted`. Corrected Purge needs no
        // grace (MVCC over the early-materialized hot read), so it is safe in the
        // modified pass. The enumeration stays a client-side per-table loop
        // because Purge is still a per-table RPC.
        // TODO(CHA-502): move this loop server-side behind a `PurgeBranch` RPC,
        // symmetric with `persist_and_snapshot_branch` above.
        let modified = discovery::paginate_modified_tables(
            &mut self.lifecycle,
            self.list_page_size,
            &catalog.catalog_uuid,
            &branch.branch_uuid,
            wm.last_modified_tick,
            now,
        )
        .await?;
        tracing::debug!(tables_modified = modified.len(), "sweep_modified complete");
        for table_uuid in &modified {
            ops::purge_one(
                &mut self.lifecycle,
                &catalog.catalog_uuid,
                &branch.branch_uuid,
                table_uuid,
            )
            .await;
        }
        wm.last_modified_tick = now;
        Ok(())
    }

    /// Purge tables whose persist has aged past the grace window. CHA-444:
    /// a table that is both modified and persisted within one tick is purged
    /// by `sweep_modified` first and again here — the second `purge_one`
    /// re-reads `Pu = W_snap` (plus the `Pa = 0` no-op guard) and early-returns
    /// with no new row, so the overlap is an intentional, idempotent defensive
    /// re-run (it covers persisted-but-not-modified tables `sweep_modified`
    /// skipped), not a missed dedupe.
    ///
    /// TODO(CHA-502): move this per-table purge loop server-side too. Its
    /// enumeration is `list_persisted_tables` over the grace window (not
    /// `list_modified_tables`), so it needs a windowed `PurgeBranch` variant
    /// distinct from the modified-pass one.
    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %catalog.catalog_uuid,
            branch = %branch.branch_uuid,
            window_start = wm.last_purge_tick,
            window_end = tracing::field::Empty,
        ),
    )]
    async fn sweep_persisted(
        &mut self,
        catalog: &Catalog,
        branch: &Branch,
        wm: &mut BranchWatermarks,
        now: i64,
    ) -> Result<(), SchedulerError> {
        let purge_upper = now - self.grace_window_micros;
        tracing::Span::current().record("window_end", purge_upper);
        if purge_upper > wm.last_purge_tick {
            let persisted = discovery::paginate_persisted_tables(
                &mut self.lifecycle,
                self.list_page_size,
                &catalog.catalog_uuid,
                &branch.branch_uuid,
                wm.last_purge_tick,
                purge_upper,
            )
            .await?;
            tracing::debug!(tables_purged = persisted.len(), "sweep_persisted complete");
            for table_uuid in &persisted {
                ops::purge_one(
                    &mut self.lifecycle,
                    &catalog.catalog_uuid,
                    &branch.branch_uuid,
                    table_uuid,
                )
                .await;
            }
            wm.last_purge_tick = purge_upper;
        }
        Ok(())
    }

    /// Branch-scoped tx-log family GC (CHA-221). Unconditional per
    /// tick — the RPC's own empty-set fast-path is the no-op gate, so
    /// no scheduler-side watermark is needed (unlike
    /// `last_modified_tick` / `last_purge_tick` which guard the
    /// listing round-trips). Errors logged and swallowed.
    async fn sweep_tx_log(&mut self, catalog: &Catalog, branch: &Branch) {
        if let Err(e) = self
            .lifecycle
            .purge_tx_log(PurgeTxLogRequest {
                catalog_uuid: Some(catalog.catalog_uuid.clone()),
                branch_uuid: Some(branch.branch_uuid.clone()),
                ..Default::default()
            })
            .await
        {
            tracing::warn!(
                catalog = %catalog.catalog_uuid,
                branch = %branch.branch_uuid,
                error = %e,
                "PurgeTxLog failed"
            );
        }
    }
}

/// Wallclock micros since Unix epoch. Used as the upper bound of each
/// tick's `ListModifiedTables` / `ListPersistedTables` window, and as
/// the new lower bound stored back into the watermark
/// (`last_*_tick = now`). "No gap between consecutive windows" holds
/// because the same scheduler-wallclock value is read once per tick and
/// written back to the watermark.
///
/// This mixes scheduler wallclock with stored `commit_micros`
/// timestamps from the database. Under bounded NTP skew this is
/// harmless — every lifecycle op is idempotent (ADR 0018), so any
/// timestamp swung past `now` by clock drift either lands inside the
/// next window naturally or is replayed without effect. Tighter clock
/// coordination (DB-clock probe, monotonic-per-branch tick) is deferred
/// past v0.
fn system_now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_micros() as i64
}
