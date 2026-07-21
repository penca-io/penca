//! Process-lifetime cache of snapshot segment *lists* (CHA-441).
//!
//! A read resolves the latest snapshot's segment list — the immutable baseline
//! `(segments, W_snap)` for a `(catalog, branch, table)` — on its way to the
//! cold-snapshot scan. That list is immutable between snapshot commits, so a
//! warm current-time read can skip the Postgres round-trip `LifecycleManager`
//! otherwise issues every read. This caches the **list** only; the decoded
//! segment *bytes* live in the separate [`crate::cache::SegmentCache`]
//! (CHA-252) — different artifact, different invalidation.
//!
//! **W_snap-keyed (CHA-492).** The key includes the resolved snapshot's `W_snap`
//! (`commit_seq_num`), so an entry is content-addressed by snapshot version:
//! every read — current-time OR time-travel — keys on the immutable snapshot it
//! resolves to, and a new snapshot simply mints a new key (the superseded entry
//! ages out under moka's `max_capacity` LFU). There is no staleness to guard
//! against (same `W_snap` ⇒ same immutable segment list), so the old
//! `LatestSeq`-only restriction and the per-hit frontier check the
//! `(catalog,branch,table)`-only key needed are both gone; all snapshot reads
//! consult it.
//!
//! **TTL now bounds only the retire grace.** Entries expire after a
//! `time_to_live` the hosting service sets `<=` the snapshot-retire GC grace
//! (`QUERY_TIMEOUT_SECONDS`): a `W_snap`-keyed entry names a *specific*
//! snapshot's files, so once that snapshot is retired and its files GC'd the
//! entry must not outlive them. The TTL is purely that safety bound, not a
//! staleness knob (there is no invalidation hook).

use std::sync::Arc;
use std::time::Duration;

use moka::sync::Cache;
use penca_core::SnapshotIndexDef;
use penca_core::SnapshotSegment;

/// A cached snapshot segment list plus the metadata a cold-snapshot scan needs.
///
/// Mirrors the fields of `penca_storage_meta::SnapshotResult` (held behind an
/// `Arc` so a cache hit is a refcount bump, not a `Vec` copy). `penca-dl`
/// owns this shape rather than depending on `penca-storage-meta` (which
/// depends on `penca-dl`); the metadata layer converts at the boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedSnapshotList {
    /// The snapshot's segment list, in `chunk_idx` (write) order.
    pub segments: Vec<SnapshotSegment>,
    /// CHA-485: user-index defs declared for the snapshot — cached alongside
    /// the segments so a cache-served default read can still seek.
    pub indexes: Vec<SnapshotIndexDef>,
    /// `W_snap` — the snapshot seq watermark (max `commit_seq_num` in the baseline).
    pub commit_seq_num: i64,
    /// The `commit_micros` watermark the snapshot represents.
    pub snapshotted_at_micros: i64,
    /// Parent-level layout keys (`None` = SQL NULL, a pre-CHA-404 parent;
    /// `Some(vec![])` = known-no-keys). Preserved for CHA-406 carry-forward.
    pub partition_keys: Option<Vec<String>>,
    pub clustering_keys: Option<Vec<String>>,
}

/// Cache key: `(catalog, branch, table, W_snap)` — content-addressed by the
/// resolved snapshot's seq watermark (CHA-492), so each read keys on the
/// immutable snapshot it resolves to and a new snapshot mints a new key.
type Key = (String, String, String, i64);

/// In-process TTL cache of snapshot segment lists, keyed
/// `(catalog_uuid, branch_uuid, table_uuid, W_snap)` and bounded by an entry
/// count.
///
/// Cheaply cloneable: `moka::sync::Cache` is internally an `Arc`, so callers
/// hold one `SnapshotListCache` behind an outer `Arc` shared across the process.
pub struct SnapshotListCache {
    inner: Cache<Key, Arc<CachedSnapshotList>>,
    /// 0 ⇒ permanently disabled (see [`Self::disabled`]); [`Self::admits`]
    /// returns false so nothing is stored.
    max_entries: u64,
}

impl SnapshotListCache {
    /// Build a cache holding up to `max_entries` lists, each expiring `ttl`
    /// after insertion. The deployment sets `ttl <= min(snapshot interval, GC
    /// grace)`; this type takes both as plain values and never shadows a
    /// default. `max_entries == 0` yields a permanently-disabled cache.
    pub fn new(ttl: Duration, max_entries: u64) -> Self {
        let inner = Cache::builder()
            .max_capacity(max_entries)
            .time_to_live(ttl)
            .build();
        Self { inner, max_entries }
    }

    /// A permanently-disabled cache. Used by services that resolve snapshots
    /// but must always read fresh (write, lifecycle, and the system-table /
    /// time-travel read paths).
    pub fn disabled() -> Self {
        Self::new(Duration::ZERO, 0)
    }

    /// Whether the cache stores anything — false for the disabled cache.
    pub fn admits(&self) -> bool {
        self.max_entries > 0
    }

    /// Fetch a cached snapshot list, bumping its frequency estimate. The hit
    /// returned here is an `Arc::clone` — copy-free at the cache boundary. (A
    /// consumer that needs an owned `SnapshotResult` rather than the shared
    /// `Arc` pays one `Vec` copy when it rebuilds it; the cache itself does
    /// not.)
    pub fn get(&self, key: &Key) -> Option<Arc<CachedSnapshotList>> {
        self.inner.get(key)
    }

    /// Insert a snapshot list under its `(catalog, branch, table)` key. No-op
    /// when the cache is disabled. moka enforces `max_capacity` + `time_to_live`.
    pub fn insert(&self, key: Key, list: Arc<CachedSnapshotList>) {
        if !self.admits() {
            return;
        }

        self.inner.insert(key, list);
    }

    /// Force pending eviction/maintenance to run synchronously (tests only).
    #[cfg(test)]
    pub(crate) fn run_pending(&self) {
        self.inner.run_pending_tasks();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(w_snap: i64) -> Arc<CachedSnapshotList> {
        Arc::new(CachedSnapshotList {
            segments: vec![],
            indexes: vec![],
            commit_seq_num: w_snap,
            snapshotted_at_micros: 1_000,
            partition_keys: Some(vec!["p".into()]),
            clustering_keys: None,
        })
    }

    fn key() -> Key {
        ("cat".into(), "branch".into(), "table".into(), 7)
    }

    #[test]
    fn hit_returns_same_arc() {
        let cache = SnapshotListCache::new(Duration::from_secs(60), 16);
        let original = list(7);
        cache.insert(key(), original.clone());
        cache.run_pending();
        let hit = cache.get(&key()).expect("cached");
        // A hit is a refcount bump on the same allocation, not a copy.
        assert!(Arc::ptr_eq(&original, &hit));
        assert_eq!(hit.commit_seq_num, 7);
    }

    #[test]
    fn miss_on_distinct_key() {
        let cache = SnapshotListCache::new(Duration::from_secs(60), 16);
        cache.insert(key(), list(1));
        cache.run_pending();
        let other: Key = ("cat".into(), "branch".into(), "other_table".into(), 7);
        assert!(
            cache.get(&other).is_none(),
            "a different (catalog,branch,table) must not collide"
        );
    }

    #[test]
    fn miss_on_distinct_w_snap() {
        // CHA-492: same (catalog,branch,table) but a newer snapshot mints a
        // distinct key — the superseded entry is never served to the new read.
        let cache = SnapshotListCache::new(Duration::from_secs(60), 16);
        cache.insert(key(), list(7));
        cache.run_pending();
        let newer: Key = ("cat".into(), "branch".into(), "table".into(), 10);
        assert!(
            cache.get(&newer).is_none(),
            "a newer W_snap must not collide with the superseded snapshot's entry"
        );
    }

    #[test]
    fn disabled_stores_nothing() {
        let cache = SnapshotListCache::disabled();
        assert!(!cache.admits());
        cache.insert(key(), list(1));
        cache.run_pending();
        assert!(
            cache.get(&key()).is_none(),
            "disabled cache (max_entries 0) must never serve a hit"
        );
    }
}
