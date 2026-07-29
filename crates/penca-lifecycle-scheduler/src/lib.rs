//! v0 single-replica microservice driving `Persist → Snapshot → Purge`
//! across every `(catalog, branch)` on **two independently-paced loops**.
//!
//! ## Two loops
//!
//! Persist wants a short cadence — it is the hot→cold memory-relief sweep, and
//! falling behind means hot grows. Snapshot, Purge and tx-log GC are compaction
//! and cleanup, cheaper to amortize over a longer one. A single interval forced
//! one compromise between the two, so they are split:
//!
//! - [`persist_loop::PersistLoop`] — `SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS`
//! - [`snapshot_loop::SnapshotLoop`] — `SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS`
//!
//! Each module doc carries its own sweep pseudocode. Either loop is disabled
//! alone by a non-positive cadence; the integration-test profile disables both.
//!
//! The loops share no mutable state. The persist loop is stateless (its RPC
//! resolves its dirty set server-side), and the snapshot loop owns the entire
//! per-branch watermark map — so no `Arc`/`Mutex` is involved, only two cheap
//! `Channel` clones.
//!
//! ## Failure semantics
//!
//! Nothing here retries in-process: recovery is always "the table is still in
//! its dirty set on a later tick". The **full tier-by-tier breakdown lives in
//! `docs/services/lifecycle-scheduler.md`, section "Failure handling"** — it is
//! the operator-facing contract and the single source of truth. Do not restate
//! it here; this doc and that one drifted repeatedly while both carried it.
//!
//! The one fact worth having at the code: the branch ops enumerate their dirty
//! sets **unwindowed** server-side, so a failed table is retried every tick with
//! no dependence on new traffic, whereas the two Purge passes are windowed and
//! only retry once a table re-enters their window.
//!
//! ## Mechanism contract
//!
//! The scheduler is a pure gRPC client. It does NOT import `LifecycleManager`
//! or talk to Postgres directly — all data access flows through
//! `QueryServiceClient` and `LifecycleServiceClient` (CHA-445 rehomed the
//! dirty-set listing RPCs onto Lifecycle). Persist and Snapshot are each one
//! server-side per-branch RPC whose per-table loop lives in `LifecycleManager`
//! (CHA-273); Purge and PurgeTxLog stay the existing per-table / per-branch
//! RPCs.

use std::time::{SystemTime, UNIX_EPOCH};

pub mod config;
mod discovery;
mod ops;
mod paginate;
pub mod persist_loop;
pub mod snapshot_loop;

pub use crate::config::SchedulerConfig;
pub use crate::persist_loop::PersistLoop;
pub use crate::snapshot_loop::SnapshotLoop;

/// Errors surfaced at the scheduler's algorithmic boundary
/// (`tick` / `tick_branch`).
///
/// Today every variant wraps a `tonic::Status` since the scheduler is a
/// pure gRPC client, but the enum exists so future non-RPC errors
/// (config validation, durable-watermark serialization, signal handling)
/// can join the type cleanly without re-wrapping as `tonic::Status`.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SchedulerError {
    #[error("gRPC error: {0}")]
    Transport(#[from] tonic::Status),
}

/// Composition root: owns both loops and drives them concurrently.
///
/// `run` uses `tokio::join!` rather than `tokio::spawn` — both loops are
/// I/O-bound on gRPC calls and sleeps, so one task interleaving at await points
/// is sufficient, and it avoids the `Send + 'static` plumbing spawning would
/// require. Neither loop's future ever completes, so neither does `join!`.
///
/// The loops return `()` rather than `!` so the never type does not reach the
/// `join!` expansion, where it would make the macro's own tail unreachable and
/// trip the denied `unreachable_code` lint.
pub struct Scheduler {
    persist: PersistLoop,
    snapshot: SnapshotLoop,
}

impl Scheduler {
    pub fn new(persist: PersistLoop, snapshot: SnapshotLoop) -> Self {
        Self { persist, snapshot }
    }

    pub async fn run(self) -> ! {
        tokio::join!(self.persist.run(), self.snapshot.run());
        unreachable!("both loops sleep forever; neither ever returns")
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
