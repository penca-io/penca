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

    /// Cadence of the persist loop — hot→cold memory relief, so it wants a
    /// SHORT interval. Negative disables that loop alone.
    pub persist_tick_interval_seconds: i64,

    /// Cadence of the snapshot loop — compaction plus Purge and tx-log GC,
    /// cheaper to amortize, so it wants a LONGER interval. Negative disables
    /// that loop alone; the two are independent.
    pub snapshot_tick_interval_seconds: i64,

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
            // Transitional: both loops read the single legacy var so the stack
            // keeps booting between this red commit and the config split that
            // retires it for SCHEDULER_{PERSIST,SNAPSHOT}_TICK_INTERVAL_SECONDS.
            persist_tick_interval_seconds: required_env_parsed("SCHEDULER_TICK_INTERVAL_SECONDS"),
            snapshot_tick_interval_seconds: required_env_parsed("SCHEDULER_TICK_INTERVAL_SECONDS"),
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

    pub fn persist_tick_interval(&self) -> Option<Duration> {
        todo!("CHA-513: delegate to tick_interval(self.persist_tick_interval_seconds)")
    }

    pub fn snapshot_tick_interval(&self) -> Option<Duration> {
        todo!("CHA-513: delegate to tick_interval(self.snapshot_tick_interval_seconds)")
    }

    pub fn grace_window_micros(&self) -> i64 {
        self.query_timeout_seconds * 1_000_000
    }

    pub fn list_page_size(&self) -> i32 {
        self.list_page_size as i32
    }
}

/// A loop's configured cadence, or `None` when that loop is disabled.
///
/// Negative seconds disable the loop: the binary boots, logs a warning, and
/// idles forever without firing any lifecycle op. The integration-test profile
/// relies on this so the suite's manual lifecycle calls cannot race a sweep.
fn tick_interval(_seconds: i64) -> Option<Duration> {
    todo!("CHA-513: negative -> None, otherwise Some(Duration::from_secs(seconds))")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(persist: i64, snapshot: i64) -> SchedulerConfig {
        SchedulerConfig {
            query_addr: String::new(),
            lifecycle_addr: String::new(),
            tick_interval_seconds: 0,
            persist_tick_interval_seconds: persist,
            snapshot_tick_interval_seconds: snapshot,
            list_page_size: 100,
            query_timeout_seconds: 900,
        }
    }

    #[test]
    fn negative_seconds_disable_the_loop() {
        assert_eq!(tick_interval(-1), None);
        assert_eq!(tick_interval(-60), None);
    }

    #[test]
    fn zero_seconds_is_a_zero_duration() {
        assert_eq!(tick_interval(0), Some(Duration::ZERO));
    }

    #[test]
    fn positive_seconds_map_to_that_many_seconds() {
        for seconds in [1_i64, 5, 30] {
            assert_eq!(
                tick_interval(seconds),
                Some(Duration::from_secs(seconds as u64))
            );
        }
    }

    /// The two loops are paced independently — including the case where only
    /// one of them is disabled.
    #[test]
    fn the_two_intervals_are_independent() {
        let both_on = config(1, 30);
        assert_eq!(both_on.persist_tick_interval(), Some(Duration::from_secs(1)));
        assert_eq!(
            both_on.snapshot_tick_interval(),
            Some(Duration::from_secs(30))
        );

        let persist_off = config(-1, 30);
        assert_eq!(persist_off.persist_tick_interval(), None);
        assert_eq!(
            persist_off.snapshot_tick_interval(),
            Some(Duration::from_secs(30))
        );

        let snapshot_off = config(1, -1);
        assert_eq!(
            snapshot_off.persist_tick_interval(),
            Some(Duration::from_secs(1))
        );
        assert_eq!(snapshot_off.snapshot_tick_interval(), None);
    }
}
