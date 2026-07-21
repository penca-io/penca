//! Scheduler configuration.
//!
//! All values are required from environment variables — defaults live in
//! deployment config (`docker/compose.yml`, k8s manifests), not in Rust.

use std::time::Duration;

use penca_core::config::{required_env, required_env_parsed};

/// Configuration for the v0 lifecycle scheduler.
///
/// The scheduler is a pure gRPC client of two services:
/// `QueryService` (catalog/branch discovery) and `LifecycleService`
/// (per-table `Persist→Snapshot→Purge` plus the `ListModifiedTables` /
/// `ListPersistedTables` dirty-set discovery RPCs, rehomed onto Lifecycle
/// by CHA-445). There is no Postgres pool by design — all data access
/// goes through the servicer RPCs.
pub struct SchedulerConfig {
    pub query_addr: String,
    pub lifecycle_addr: String,

    /// Time between the end of one tick and the start of the next.
    /// Negative values disable the tick loop: the scheduler binary
    /// boots, logs a warning, and idles forever — useful for the
    /// integration test profile, which asserts RPC behavior directly
    /// and doesn't want the autonomous loop racing manual lifecycle
    /// calls.
    pub tick_interval_seconds: i64,

    /// Max `table_uuid`s requested per list-tables page. The scheduler
    /// drains every page before moving to the next branch.
    pub list_page_size: u32,

    /// Universal grace window in seconds — MUST equal the value the
    /// lifecycle + query servicers read from this same env var (ADR
    /// 0019). The scheduler bounds `ListPersistedTables` upper at
    /// `now - QUERY_TIMEOUT_SECONDS` so only tables whose persist has
    /// already cleared the server-side grace gate are enumerated for
    /// `Purge`.
    pub query_timeout_seconds: i64,
}

impl SchedulerConfig {
    pub fn from_env() -> Self {
        Self {
            query_addr: required_env("QUERY_SERVICE_ADDR"),
            lifecycle_addr: required_env("LIFECYCLE_SERVICE_ADDR"),
            tick_interval_seconds: required_env_parsed("SCHEDULER_TICK_INTERVAL_SECONDS"),
            list_page_size: required_env_parsed("SCHEDULER_LIST_PAGE_SIZE"),
            query_timeout_seconds: required_env_parsed("QUERY_TIMEOUT_SECONDS"),
        }
    }

    pub fn tick_interval(&self) -> Option<Duration> {
        if self.tick_interval_seconds < 0 {
            None
        } else {
            Some(Duration::from_secs(self.tick_interval_seconds as u64))
        }
    }

    pub fn grace_window_micros(&self) -> i64 {
        self.query_timeout_seconds * 1_000_000
    }

    pub fn list_page_size(&self) -> i32 {
        self.list_page_size as i32
    }
}
