//! DataFusion-bound hot+cold merge fan-in floor.
//!
//! Drives the REAL `penca_dl::DatafusionDlDriver::scan_snapshot` (the
//! `SnapshotTableProvider` path) over a fixed in-memory cold base, measuring the
//! DataFusion **exclusion anti-join** + snapshot scan as hot churn (the
//! exclusion-set size) grows. An in-memory `FormatReader` serves the decoded
//! base, so no PG and no Lance/S3 read are in the path — only the DataFusion
//! merge compute — see `cold_segment_read_floor` for the cold *read* floor and
//! `cold_point_lookup_floor` for the point-lookup *execution* floor.
//!
//! Env: PERF_FLOOR_MAX=1m adds the 1M-row cold base (default 100k). The shared
//! harness lives in `floor_support.rs` (also used by the guard test).

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

#[path = "floor_support.rs"]
mod floor_support;
use floor_support::{
    SCAN_SQL, base_batch, base_schema, base_segment, bases, driver_for, scan_rows,
};

const OVERLAYS: [u64; 4] = [0, 100, 1_000, 10_000];

fn merge_fanin_floor(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let schema = base_schema();

    let mut g = c.benchmark_group("merge_fanin");
    g.sample_size(10);
    for &base in &bases() {
        // Fixed cold base, served in-memory; warm cache after the first scan.
        let dl = driver_for(base_batch(base));
        let seg = base_segment(1 << 20);

        for &h in &OVERLAYS {
            // Hot churn: the first `h` base row_uuids are shadowed → anti-joined out.
            let exclusion: Vec<String> = (0..h).map(|i| format!("r{i}")).collect();

            g.throughput(Throughput::Elements(base));
            g.bench_with_input(
                BenchmarkId::new(format!("base{base}"), format!("overlay{h}")),
                &h,
                |b, _| {
                    b.iter(|| {
                        let rows = rt.block_on(scan_rows(
                            &dl,
                            std::slice::from_ref(&seg),
                            &schema,
                            &exclusion,
                            SCAN_SQL,
                        ));
                        black_box(rows);
                    });
                },
            );
        }
    }
    g.finish();
}

criterion_group!(benches, merge_fanin_floor);
criterion_main!(benches);
