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

    /// Cadence of the persist loop — hot→cold memory relief, so it wants a
    /// SHORT interval. Non-positive disables that loop alone
    /// (see [`interval_from_seconds`]).
    pub persist_tick_interval_seconds: i64,

    /// Cadence of the snapshot loop — compaction plus Purge and tx-log GC,
    /// cheaper to amortize, so it wants a LONGER interval. Non-positive
    /// disables that loop alone (see [`interval_from_seconds`]); the two are
    /// independent.
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
            persist_tick_interval_seconds: required_env_parsed(
                "SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS",
            ),
            snapshot_tick_interval_seconds: required_env_parsed(
                "SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS",
            ),
            list_page_size: required_env_parsed("SCHEDULER_LIST_PAGE_SIZE"),
            query_timeout_seconds: required_env_parsed("QUERY_TIMEOUT_SECONDS"),
        }
    }

    pub fn persist_tick_interval(&self) -> Option<Duration> {
        interval_from_seconds(self.persist_tick_interval_seconds)
    }

    pub fn snapshot_tick_interval(&self) -> Option<Duration> {
        interval_from_seconds(self.snapshot_tick_interval_seconds)
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
/// **Non-positive** seconds disable the loop: the binary boots, logs a warning,
/// and idles forever without firing any lifecycle op. The integration-test
/// profile relies on this so the suite's manual lifecycle calls cannot race a
/// sweep.
///
/// Zero disables rather than yielding `Duration::ZERO`, which would make
/// `loop { tick(); sleep(interval) }` a backoff-free hot loop hammering the
/// lifecycle service. Splitting one cadence knob into two doubles the chance of
/// a stray `0` reaching a deployment env file, and "sweep as fast as possible"
/// is not a mode anyone wants — so `<= 0` has exactly one meaning, "off".
fn interval_from_seconds(seconds: i64) -> Option<Duration> {
    if seconds <= 0 {
        None
    } else {
        Some(Duration::from_secs(seconds as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(persist: i64, snapshot: i64) -> SchedulerConfig {
        SchedulerConfig {
            query_addr: String::new(),
            lifecycle_addr: String::new(),
            persist_tick_interval_seconds: persist,
            snapshot_tick_interval_seconds: snapshot,
            list_page_size: 100,
            query_timeout_seconds: 900,
        }
    }

    /// Zero is grouped with the negatives deliberately: `Duration::ZERO` would
    /// turn the tick loop into a backoff-free hot loop against the lifecycle
    /// service, so `<= 0` means "off" and nothing else.
    #[test]
    fn non_positive_seconds_disable_the_loop() {
        assert_eq!(interval_from_seconds(-60), None);
        assert_eq!(interval_from_seconds(-1), None);
        assert_eq!(interval_from_seconds(0), None);
    }

    #[test]
    fn positive_seconds_map_to_that_many_seconds() {
        for seconds in [1_i64, 5, 30] {
            assert_eq!(
                interval_from_seconds(seconds),
                Some(Duration::from_secs(seconds as u64))
            );
        }
    }

    /// The two loops are paced independently — including the case where only
    /// one of them is disabled.
    #[test]
    fn the_two_intervals_are_independent() {
        // Zero must reach the accessors through the helper — an accessor wired
        // to the legacy `< 0` rule instead would pass every `-1` case.
        let persist_zero = config(0, 30);
        assert_eq!(persist_zero.persist_tick_interval(), None);
        assert_eq!(
            persist_zero.snapshot_tick_interval(),
            Some(Duration::from_secs(30))
        );

        let snapshot_zero = config(1, 0);
        assert_eq!(
            snapshot_zero.persist_tick_interval(),
            Some(Duration::from_secs(1))
        );
        assert_eq!(snapshot_zero.snapshot_tick_interval(), None);

        let both_on = config(1, 30);
        assert_eq!(
            both_on.persist_tick_interval(),
            Some(Duration::from_secs(1))
        );
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
