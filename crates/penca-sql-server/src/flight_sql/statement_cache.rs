//! Per-connection logical-plan cache for statement-query plan reuse.
//!
//! A Flight SQL statement query is planned twice today: once in
//! `GetFlightInfo` (to return the result schema) and again in `DoGet` (to
//! execute), because the ticket carries only the SQL string. This cache lets
//! `GetFlightInfo` stash the [`LogicalPlan`] it already built under a
//! server-minted `statement_uuid`; `DoGet` looks it up by that key and executes
//! it directly, falling back to a re-plan on a miss.
//!
//! ## Statement UUIDs are globally unique
//!
//! A `statement_uuid` is a random UUID (v4), so no two plans across the whole
//! server — across connections *and* across process restarts — ever share one.
//! This is a *safety* property, not an optimisation: the response ticket is
//! opaque to the client but client-controlled, so a `statement_uuid` minted on
//! connection A can be replayed against connection B, or replayed against a
//! fresh process after a restart. Because no other cache ever minted that UUID,
//! the lookup deterministically **misses** and the server re-plans from the
//! ticket's SQL — never a false hit returning an unrelated plan. Two weaker
//! schemes both fail here: a per-cache counter collides across connections
//! (every conn mints `"0"`, `"1"`, …), and a process-global counter reuses the
//! key space across a restart, letting a stale replayed ticket false-hit a
//! reused value. A v4 UUID avoids both without any cross-connection
//! coordination.
//!
//! ## Why the entry holds only the plan
//!
//! The [`StatementCacheEntry`] stores ONLY the plan — no `SessionSnapshot`, no catalog / branch
//! / transaction state. A reused plan is the *same* plan `GetFlightInfo` built,
//! and `statement_uuid` uniqueness (above) already guarantees a key resolves
//! only to its own plan, so no catalog/branch guard is needed. Transaction
//! visibility is not stored either: the cached plan's
//! `PencaTableProvider::scan` reads the live `ConnScope.open_tx_cell` at
//! execution time, so a reused plan sees the same transaction state a
//! re-planned one would. Reuse changes *which plan runs*, not *which rows it
//! sees*.
//!
//! ## Lifecycle
//!
//! Bounded by `capacity` with insertion-order (FIFO) eviction; no TTL, because
//! the per-connection lifetime already bounds the cache. A `capacity` of 0
//! disables the cache: nothing is stored and every lookup misses, which is the
//! deterministic miss lever the cache-miss path is validated against. A miss is
//! always safe — `DoGet` re-plans from the SQL string — so there is no
//! correctness dependence on a hit.
//!
//! Entries age out via FIFO only: `insert` runs on every `GetFlightInfo` (even
//! one with no following `DoGet` — a schema-only probe, or an abandoned
//! endpoint), and a `get` hit does **not** remove or refresh the entry (no
//! removal-on-use, no LRU recency). So the steady-state resident set is
//! `min(capacity, distinct recent GetFlightInfo plans on the conn)`, regardless
//! of how many were actually `DoGet`'d.
//!
//! ## Concurrency
//!
//! All HTTP/2 streams on one TCP connection share a single
//! `Arc<ConnSession>`, hence one `Arc<StatementCache>`; the inner [`Mutex`]
//! serialises concurrent `insert` / `get` across those streams.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;

use datafusion::logical_expr::LogicalPlan;
use uuid::Uuid;

/// A statement-cache value: the reusable artifacts keyed by `statement_uuid`.
/// Today that is only the [`LogicalPlan`]; it is a struct rather than a bare
/// plan so further per-statement reusable artifacts can join it later without
/// re-threading every callsite.
#[derive(Clone)]
pub(crate) struct StatementCacheEntry {
    pub(crate) plan: LogicalPlan,
}

/// Mutable interior of [`StatementCache`]: the key→entry map plus a FIFO queue
/// of keys in insertion order for capacity-bound eviction.
#[derive(Default)]
struct StatementCacheInner {
    entries: HashMap<String, StatementCacheEntry>,
    order: VecDeque<String>,
}

/// Per-connection cache of `GetFlightInfo` logical plans keyed by a
/// process-globally-unique `statement_uuid`. See the module docs for the design
/// rationale.
pub(crate) struct StatementCache {
    inner: Mutex<StatementCacheInner>,
    capacity: usize,
}

impl StatementCache {
    /// Create a cache holding at most `capacity` plans. `capacity == 0`
    /// disables caching (every lookup misses).
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(StatementCacheInner::default()),
            capacity,
        }
    }

    /// Register `plan` under a freshly minted `statement_uuid` and return it.
    ///
    /// The `statement_uuid` is always freshly minted (so callers can stamp it on
    /// the outgoing ticket unconditionally), but when `capacity == 0` the plan
    /// is not stored, so a later `get` of that key misses and the `DoGet` leg
    /// re-plans.
    pub(crate) fn insert(&self, plan: LogicalPlan) -> String {
        let statement_uuid = Uuid::new_v4().to_string();
        if self.capacity == 0 {
            return statement_uuid;
        }
        let mut inner = self.inner.lock().unwrap();
        inner
            .entries
            .insert(statement_uuid.clone(), StatementCacheEntry { plan });
        inner.order.push_back(statement_uuid.clone());
        while inner.order.len() > self.capacity {
            if let Some(evicted) = inner.order.pop_front() {
                inner.entries.remove(&evicted);
            }
        }
        statement_uuid
    }

    /// Look up a [`StatementCacheEntry`] by key, returning a clone. `None` on a
    /// miss (unknown, evicted, or minted under a disabled cache) — the caller
    /// re-plans.
    pub(crate) fn get(&self, key: &str) -> Option<StatementCacheEntry> {
        self.inner.lock().unwrap().entries.get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::EmptyRelation;
    use std::sync::Arc;

    /// A trivial `EmptyRelation` plan — enough to exercise insert/get/evict
    /// without standing up a DataFusion context. The tests assert on
    /// presence/absence by statement_uuid, not on plan identity, so a single
    /// fixed shape suffices.
    fn plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        })
    }

    #[test]
    fn insert_then_get_roundtrips() {
        let cache = StatementCache::new(4);
        let statement_uuid = cache.insert(plan());
        assert!(matches!(
            cache.get(&statement_uuid),
            Some(StatementCacheEntry {
                plan: LogicalPlan::EmptyRelation(_)
            })
        ));
    }

    #[test]
    fn each_insert_mints_a_distinct_statement_uuid() {
        let cache = StatementCache::new(4);
        let a = cache.insert(plan());
        let b = cache.insert(plan());
        assert_ne!(a, b);
    }

    #[test]
    fn unknown_key_misses() {
        let cache = StatementCache::new(4);
        assert!(cache.get("nope").is_none());
    }

    #[test]
    fn eviction_drops_oldest_past_capacity() {
        let cache = StatementCache::new(2);
        let a = cache.insert(plan());
        let b = cache.insert(plan());
        let c = cache.insert(plan());
        // Capacity 2: inserting `c` evicts the oldest (`a`); `b` and `c` stay.
        assert!(cache.get(&a).is_none());
        assert!(cache.get(&b).is_some());
        assert!(cache.get(&c).is_some());
    }

    #[test]
    fn zero_capacity_always_misses_but_still_mints() {
        let cache = StatementCache::new(0);
        let statement_uuid = cache.insert(plan());
        assert!(
            !statement_uuid.is_empty(),
            "the statement_uuid is still minted for the ticket"
        );
        assert!(
            cache.get(&statement_uuid).is_none(),
            "nothing is stored at capacity 0"
        );
    }
}
