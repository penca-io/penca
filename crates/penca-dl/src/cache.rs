//! Process-lifetime, zero-copy cache of decoded cold segments.
//!
//! A repeat read of the same cold segment within the process lifetime is served
//! as an `Arc::clone` of the already-decoded Arrow batches, skipping the S3 GET +
//! Parquet/Lance decode. It holds snapshot segments, persist data segments and
//! index sidecars under one byte budget, all keyed by the SLICE they name plus
//! the schema they were decoded to (`slice_cache_key`). The slice is immutable,
//! so the key→value mapping is stable and needs no invalidation: a given
//! `uri[offset .. offset + length]` resolves to identical bytes for the life of
//! the process, and although a *resolved persist tier* is mutable under
//! retention compaction, an individual persist *file* is not. There is no TTL —
//! immutability makes W-TinyLFU eviction the whole reclaim mechanism.
//!
//! Eviction is W-TinyLFU (frequency-based, scan-resistant, aged) via `moka`,
//! bounded by a byte budget: each entry is weighed by its segment's
//! `size_bytes`, the in-memory Arrow footprint Penca records at write time.
//! Eviction and misses degrade gracefully to an S3 re-read, which is the
//! OOM-safety story since this is heap memory, not reclaimable OS page cache.
//! The budget is env-configured by the hosting service; this type takes it as a
//! plain `u64` and never shadows a default.
//!
//! No single-flight: two concurrent queries missing the same cacheable segment
//! both decode it before one `insert` wins. This transiently duplicates one
//! segment's decode work, bounded by `segment_read_concurrency`; it is an
//! accepted trade-off (moka `get_with` entry-coalescing is the escape hatch if
//! duplicate large decodes ever prove costly).

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use moka::sync::Cache;

/// In-process W-TinyLFU cache of decoded cold segments, keyed by slice identity
/// and bounded by a byte budget.
///
/// The key is `uri#offset+length` plus a fingerprint of the schema the batch was
/// decoded to — see `slice_cache_key`. NOT the row's uuid: carry-forward and
/// CHA-539's fork copy both mint a fresh uuid for a slice another branch already
/// holds, so uuid keying stored one entry per referencing branch of identical
/// bytes. The decode schema is part of the identity because the cached batch is
/// null-filled to the CALLER's schema, and two branches can diverge on a
/// column's type while naming the same slice.
///
/// `format` is intentionally absent — it is functionally determined by the URI,
/// and every value is a format-agnostic decoded [`RecordBatch`]; `format` is
/// consulted only on the miss path (by the caller) to pick the reader.
///
/// Sidecars share this cache with base segments. They no longer occupy a
/// separate uuid namespace, so the separation now rests on a URI invariant: a
/// sidecar's `object_uri` (an `index/` path) is never a base segment's.
///
/// Cheaply cloneable: `moka::sync::Cache` is internally an `Arc`, so callers
/// typically hold a `SegmentCache` behind one outer `Arc` shared
/// across the process.
pub struct SegmentCache {
    /// Value carries its own weight so the weigher can charge the
    /// caller-supplied `size_bytes` rather than the batch's runtime memory.
    inner: Cache<String, (Arc<RecordBatch>, u32)>,
    budget_bytes: u64,
}

impl SegmentCache {
    /// Build a cache bounded to `budget_bytes` of total resident weight.
    ///
    /// A `budget_bytes` of 0 yields a permanently-disabled cache (see
    /// [`Self::disabled`]) — [`Self::admits`] returns false so nothing is ever
    /// stored.
    pub fn new(budget_bytes: u64) -> Self {
        let inner = Cache::builder()
            .max_capacity(budget_bytes)
            // Weight in the same byte unit as `max_capacity` so the budget is a
            // real RAM bound; the stored `u32` is the entry's `size_bytes`.
            .weigher(|_key: &String, (_batch, weight): &(Arc<RecordBatch>, u32)| *weight)
            .build();
        Self {
            inner,
            budget_bytes,
        }
    }

    /// A permanently-disabled cache. Used by services that construct a driver
    /// but never serve cached snapshot reads (write, lifecycle).
    pub fn disabled() -> Self {
        Self::new(0)
    }

    /// Whether a segment of `weight_bytes` is worth caching: the cache must be
    /// enabled and the segment must fit the whole budget. A segment larger than
    /// the entire budget could never stay resident (it would evict everything,
    /// then itself), so the caller reads it whole-or-pushed-down without
    /// caching. This is the cacheable-vs-pushdown gate on the read path.
    ///
    /// Rejects `weight_bytes == 0`: moka's eviction is purely weight-based (no
    /// max-entry count), so a zero-weight entry contributes nothing to
    /// `weighted_size` and is never evicted under budget pressure — it would
    /// pin forever and escape the RAM bound. `size_bytes` is `DEFAULT 0` and is
    /// backfilled by a post-write UPDATE, so a 0 is plausible for empty or
    /// legacy segments; those fall to the pushdown branch.
    ///
    /// Also rejects `weight_bytes > u32::MAX`: moka's weigher is `-> u32`, so an
    /// entry's charge is stored as `u32`. With a multi-GB budget a >4 GiB
    /// segment would otherwise pass the budget check but be truncated by the
    /// `as u32` cast in [`insert`](Self::insert), silently under-charging and
    /// breaking the RAM bound. Such a segment can never be a stable resident
    /// entry anyway, so it is non-admissible (read whole/pushed-down instead).
    pub fn admits(&self, weight_bytes: u64) -> bool {
        self.budget_bytes > 0
            && weight_bytes > 0
            && weight_bytes <= self.budget_bytes
            && weight_bytes <= u32::MAX as u64
    }

    /// Fetch a decoded segment by slice key, bumping its frequency estimate. A
    /// hit is an `Arc::clone` — no buffer copy.
    pub fn get(&self, key: &str) -> Option<Arc<RecordBatch>> {
        self.inner.get(key).map(|(batch, _weight)| batch)
    }

    /// Insert a decoded segment under its slice key, charged `weight_bytes`
    /// against the budget. No-op when the segment is not
    /// [`admits`](Self::admits)-ible. moka enforces `max_capacity` via W-TinyLFU;
    /// there is no manual eviction loop here.
    pub fn insert(&self, key: String, batch: Arc<RecordBatch>, weight_bytes: u64) {
        if !self.admits(weight_bytes) {
            return;
        }
        self.inner.insert(key, (batch, weight_bytes as u32));
    }

    /// Force pending eviction/maintenance to run synchronously. moka does
    /// maintenance lazily/amortized; tests call this to observe a deterministic
    /// post-eviction state.
    #[cfg(test)]
    pub(crate) fn run_pending(&self) {
        self.inner.run_pending_tasks();
    }

    /// Current total resident weight in bytes. moka keeps this `<= budget_bytes`
    /// after maintenance.
    #[cfg(test)]
    pub(crate) fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::SegmentCache;

    /// One-column batch of `n` i32 rows; a stand-in decoded segment.
    fn batch(n: usize) -> Arc<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let col = Int32Array::from((0..n as i32).collect::<Vec<_>>());
        Arc::new(RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap())
    }

    #[test]
    fn admits_predicate() {
        let cache = SegmentCache::new(100);
        assert!(cache.admits(1));
        assert!(cache.admits(100), "weight == budget fits");
        assert!(!cache.admits(101), "weight > budget never resident");
        // Zero-weight entries are never evicted under weight-based capacity, so
        // they must not be cached — they fall to the pushdown branch instead.
        assert!(!cache.admits(0), "zero footprint never cached");

        let disabled = SegmentCache::disabled();
        assert!(!disabled.admits(1), "budget 0 admits nothing");
        assert!(!disabled.admits(0));
    }

    #[test]
    fn zero_weight_segment_is_not_cached() {
        let cache = SegmentCache::new(1 << 20);
        cache.insert("zero".into(), batch(4), 0);
        cache.run_pending();
        assert!(
            cache.get("zero").is_none(),
            "weight-0 segment must not be pinned in the cache"
        );
    }

    #[test]
    fn admits_rejects_weight_over_u32_max() {
        // Multi-GB budget, but a single entry heavier than u32::MAX would be
        // truncated by moka's u32 weigher — reject it at the boundary so the
        // budget stays a real RAM bound.
        let budget = 8 * 1024 * 1024 * 1024; // 8 GiB
        let cache = SegmentCache::new(budget);
        let over_u32 = u32::MAX as u64 + 1;
        assert!(over_u32 <= budget, "precondition: fits the byte budget");
        assert!(
            !cache.admits(over_u32),
            "but u32-weigher truncation rejects it"
        );
        assert!(cache.admits(u32::MAX as u64), "exactly u32::MAX is fine");

        cache.insert("huge".into(), batch(8), over_u32);
        cache.run_pending();
        assert!(cache.get("huge").is_none(), "over-u32 weight never stored");
    }

    #[test]
    fn over_budget_insert_is_noop() {
        let cache = SegmentCache::new(100);
        cache.insert("too-big".into(), batch(8), 200);
        cache.run_pending();
        assert!(cache.get("too-big").is_none(), "over-budget never stored");

        let disabled = SegmentCache::disabled();
        disabled.insert("x".into(), batch(8), 1);
        disabled.run_pending();
        assert!(disabled.get("x").is_none(), "disabled never stores");
    }

    #[test]
    fn budget_enforced_total_weight() {
        // Budget holds ~2 entries of weight 40. Insert 5 distinct entries
        // summing to 200 bytes; moka must bound the resident weight to the
        // budget. We assert the byte bound, not which keys survive (that is
        // moka's W-TinyLFU choice, not Penca's contract).
        let cache = SegmentCache::new(100);
        for i in 0..5 {
            cache.insert(format!("seg-{i}"), batch(10), 40);
        }
        cache.run_pending();
        assert!(
            cache.weighted_size() <= 100,
            "resident weight {} exceeds budget 100",
            cache.weighted_size()
        );
    }

    #[test]
    fn hit_returns_arc_clone_same_buffers() {
        let cache = SegmentCache::new(1_000);
        let original = batch(16);
        cache.insert("seg".into(), original.clone(), 40);
        cache.run_pending();
        let hit = cache.get("seg").expect("cached");
        // Same backing column buffer — a hit is a refcount bump, no copy.
        assert_eq!(
            original.column(0).to_data().buffers()[0].as_ptr(),
            hit.column(0).to_data().buffers()[0].as_ptr(),
        );
    }
}
