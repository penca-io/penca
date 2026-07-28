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
        let tick_interval_seconds = required_env_parsed("SCHEDULER_TICK_INTERVAL_SECONDS");
        Self {
            query_addr: required_env("QUERY_SERVICE_ADDR"),
            lifecycle_addr: required_env("LIFECYCLE_SERVICE_ADDR"),
            tick_interval_seconds,
            // TODO(CHA-513): both loops alias the single legacy var so the stack
            // keeps booting until the config split retires it for
            // SCHEDULER_{PERSIST,SNAPSHOT}_TICK_INTERVAL_SECONDS.
            // `LifecycleServiceConfig::from_env` carries the same alias — the two
            // crates and docker/{compose.yml,dev.env,test.env} MUST flip in one
            // commit, or `required_env_parsed` panics at boot on the retired name.
            persist_tick_interval_seconds: tick_interval_seconds,
            snapshot_tick_interval_seconds: tick_interval_seconds,
            list_page_size: required_env_parsed("SCHEDULER_LIST_PAGE_SIZE"),
            query_timeout_seconds: required_env_parsed("QUERY_TIMEOUT_SECONDS"),
        }
    }

    /// TODO(CHA-513): deleted by the config split, along with
    /// `tick_interval_seconds`. Until then this is the only path `main` drives,
    /// and it still spells "disabled" as `< 0` rather than delegating to
    /// [`interval_from_seconds`] — so a deployment override of `0` is still the
    /// backoff-free hot loop that helper exists to prevent. Delegate or delete;
    /// do not let the two rules outlive the red phase.
    pub fn tick_interval(&self) -> Option<Duration> {
        if self.tick_interval_seconds < 0 {
            None
        } else {
            Some(Duration::from_secs(self.tick_interval_seconds as u64))
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
fn interval_from_seconds(_seconds: i64) -> Option<Duration> {
    todo!("CHA-513: seconds <= 0 -> None, otherwise Some(Duration::from_secs(seconds))")
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
