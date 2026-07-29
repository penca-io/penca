//! Workload-correctness guard for the cold point-lookup execution floor.
//!
//! The `cold_point_lookup_floor` bench times the real `scan_snapshot`
//! (`SnapshotTableProvider`) running the production cold point-lookup plan — the
//! exclusion anti-join (over an empty exclusion) plus a PK residual — over a fixed
//! in-memory cold base, as segment size grows. A throughput number is only meaningful
//! if the residual actually selects the *intended* row, so this guard pins both count
//! and content: a point lookup over a base of N rows returns exactly the one matching
//! row (the target present, its neighbour absent), and a miss predicate returns zero
//! rows.
//!
//! `scan_snapshot`'s own semantics are owned by penca-dl's tests; this just
//! confirms the bench measures a real residual point-lookup over the right row,
//! not a passthrough.

#[path = "../benches/floor_support.rs"]
mod floor_support;
use floor_support::{
    base_batch, base_schema, base_segment, driver_for, point_lookup_sql, scan_uuids,
};

#[tokio::test]
async fn residual_point_lookup_returns_exactly_the_target() {
    const N: u64 = 100;

    let dl = driver_for(base_batch(N));
    let seg = base_segment(1 << 16);
    let schema = base_schema();

    // HIT: `WHERE row_uuid = 'r7'` selects exactly that one row.
    let kept = scan_uuids(
        &dl,
        std::slice::from_ref(&seg),
        &schema,
        &[],
        &point_lookup_sql(7),
    )
    .await;
    // Pins content, not just count: exactly the one target row, nothing else. A
    // prefix/range bug (e.g. `LIKE 'r7%'` keeping r7, r70..r79) would lengthen
    // `kept` and fail this equality.
    assert_eq!(
        kept,
        vec!["r7".to_string()],
        "residual selects exactly the target row — equality, not prefix/range",
    );

    // MISS: a key past the end of the base selects no rows.
    let miss = scan_uuids(
        &dl,
        std::slice::from_ref(&seg),
        &schema,
        &[],
        &point_lookup_sql(999),
    )
    .await;
    assert!(miss.is_empty(), "a non-existent key returns zero rows");
}
