//! DataFusion-bound cold point-lookup execution floor.
//!
//! Drives the REAL `penca_dl::DatafusionDlDriver::scan_snapshot` (the
//! `SnapshotTableProvider` path) over a fixed in-memory cold base, measuring the
//! **production cold point-lookup plan** — the exclusion anti-join over an empty
//! exclusion plus a PK residual (`build_cold_snapshot_scan`'s shape) — as the cold
//! segment grows. An in-memory `FormatReader` serves the decoded base, so no PG and
//! no Lance/S3 read are in the path — only the DataFusion scan + anti-join + filter.
//! Without a sidecar index the cold provider does an O(rows)
//! full-scan-and-filter (no pushdown, ADR 0023); the seek group below shows the
//! O(rows)→O(log n) improvement. See also `merge_fanin_floor` and
//! `cold_segment_read_floor`.
//!
//! Env: PERF_FLOOR_MAX=1m adds the 1M-row cold base (default 100k). The shared
//! harness lives in `floor_support.rs` (also used by the guard test).

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

#[path = "floor_support.rs"]
mod floor_support;
use floor_support::{
    base_batch, base_schema, base_segment, base_segment_with_sidecar, bases, driver_for,
    driver_for_seek, point_lookup_sql, scan_rows, scan_rows_seek,
};

fn cold_point_lookup_floor(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let schema = base_schema();

    let mut g = c.benchmark_group("cold_point_lookup");
    g.sample_size(10);
    for &base in &bases() {
        // Fixed cold base, served in-memory; warm cache after the first scan.
        let dl = driver_for(base_batch(base));
        let seg = base_segment(1 << 20);

        // PK point predicate: a guaranteed hit at the tail of the base. The cold
        // provider scans every row and filters (O(rows)), so the key's position
        // does not change the cost — this is an honest full-scan floor.
        let sql = point_lookup_sql(base - 1);

        g.throughput(Throughput::Elements(base));
        g.bench_with_input(
            BenchmarkId::new(format!("base{base}"), "pk_eq"),
            &base,
            |b, _| {
                b.iter(|| {
                    let rows = rt.block_on(scan_rows(
                        &dl,
                        std::slice::from_ref(&seg),
                        &schema,
                        &[],
                        &sql,
                    ));
                    black_box(rows);
                });
            },
        );
    }
    g.finish();
}

/// Seek variant of the point-lookup floor: the SAME cold base + point predicate,
/// but the segment carries its internal `row_uuid` index sidecar and the read
/// passes `seek_keys`, so the provider binary-searches the sidecar and `take`s
/// the single matched row instead of the O(rows) full scan above. Run both
/// groups to read the O(rows) → O(log n) improvement off the throughput numbers.
fn cold_point_lookup_seek(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let schema = base_schema();

    let mut g = c.benchmark_group("cold_point_lookup_seek");
    g.sample_size(10);
    for &base in &bases() {
        let dl = driver_for_seek(base_batch(base));
        let seg = base_segment_with_sidecar(1 << 20, base as i64);
        let sql = point_lookup_sql(base - 1);
        let seek_keys = Arc::new(vec![format!("r{}", base - 1)]);

        g.throughput(Throughput::Elements(base));
        g.bench_with_input(
            BenchmarkId::new(format!("base{base}"), "pk_eq_seek"),
            &base,
            |b, _| {
                b.iter(|| {
                    let rows = rt.block_on(scan_rows_seek(
                        &dl,
                        std::slice::from_ref(&seg),
                        &schema,
                        &sql,
                        seek_keys.clone(),
                    ));
                    black_box(rows);
                });
            },
        );
    }
    g.finish();
}

criterion_group!(benches, cold_point_lookup_floor, cold_point_lookup_seek);
criterion_main!(benches);
