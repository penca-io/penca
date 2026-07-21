//! Workload-correctness guard for the hot+cold merge fan-in floor (CHA-415 #3).
//!
//! The `merge_fanin_floor` bench (I3) times the real `scan_snapshot` (CHA-411
//! `SnapshotTableProvider`) exclusion anti-join over a fixed in-memory cold base
//! as hot churn (the exclusion-set size) grows. A throughput number is only
//! meaningful if the exclusion is actually applied to the *intended* rows, so
//! this guard pins both count and content: scanning a base of N rows with H
//! excluded row_uuids returns exactly the N − H *non-excluded* rows (the
//! excluded ones absent, the rest present), and an empty exclusion keeps all N.
//!
//! Characterization guard — `scan_snapshot` is existing correct production code
//! (its semantics are owned by penca-dl's own tests); this just confirms the
//! bench measures a real anti-join over the right rows, not a passthrough.

#[path = "../benches/floor_support.rs"]
mod floor_support;
use floor_support::{SCAN_SQL, base_batch, base_schema, base_segment, driver_for, scan_uuids};

#[tokio::test]
async fn scan_snapshot_anti_joins_the_hot_overlay() {
    const N: u64 = 100;
    const H: usize = 10;

    let dl = driver_for(base_batch(N));
    let seg = base_segment(1 << 16);
    let schema = base_schema();

    // H shadowed row_uuids (r0..r{H-1}) are anti-joined out of the base.
    let exclusion: Vec<String> = (0..H).map(|i| format!("r{i}")).collect();
    let kept = scan_uuids(
        &dl,
        std::slice::from_ref(&seg),
        &schema,
        &exclusion,
        SCAN_SQL,
    )
    .await;
    assert_eq!(
        kept.len(),
        N as usize - H,
        "exclusion anti-join removes exactly H rows",
    );
    // Content, not just count: the excluded uuids are gone and the rest survive,
    // so a wrong-rows anti-join (e.g. excluding r50..r59) would fail here.
    for i in 0..H {
        assert!(
            !kept.contains(&format!("r{i}")),
            "excluded r{i} must be absent",
        );
    }
    assert!(
        kept.contains(&"r50".to_string()),
        "a non-excluded mid row must survive",
    );
    assert!(
        kept.contains(&format!("r{}", N - 1)),
        "the last base row must survive",
    );

    // Empty exclusion (no hot churn) keeps every base row.
    let all = scan_uuids(&dl, std::slice::from_ref(&seg), &schema, &[], SCAN_SQL).await;
    assert_eq!(all.len(), N as usize, "empty exclusion keeps all base rows");
}
