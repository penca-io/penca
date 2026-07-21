//! CHA-415 primitive #2a — Lance/object-store cold-segment read floor.
//!
//! Times the two CHA-348 read arms against a cold segment — (a) whole-segment
//! read + client-side filter for one row, (b) `(offset, length)` range read (the
//! pushdown) — plus the segment-write throughput. Pins `FormatReader` /
//! `FormatWriter` directly (L1 object-store GET + L2 decode); no DataFusion, no
//! merge. The workload is the one `tests/cold_segment_read_workload.rs` locks.
//!
//! Uses an in-memory object store, so it measures the decode-dominated scan
//! cost (the Lance row-group/scan cost the point-lookup tickets target). The
//! real-S3 first-touch GET, plus a cached-vs-uncached arm, are deferred to
//! CHA-422.
//!
//! Throughput is normalized to the segment size `n` for BOTH arms (the whole
//! arm scans `n` rows; the range arm reads 1), so the two arms are compared by
//! elapsed time and the range arm's higher rows/s reflects the pushdown speedup
//! — it is "rows-scanned-equivalent per second", not rows returned.
//!
//! Env: PERF_FLOOR_MAX=1m adds the 1M-row segment (default just 100k);
//! PERF_FLOOR_PARQUET=1 adds the Parquet arm (CHA-61). Setup helpers are
//! duplicated from the guard test (benches and tests can't share a module);
//! the orch:run-cleanup pass extracts the shared kernel.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams, ObjectStoreRegistry,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use penca_format::reader::lance::LanceFormatReader;
use penca_format::reader::parquet::ParquetFormatReader;
use penca_format::reader::{AnyFormatReader, FormatReader};
use penca_format::writer::FormatWriter;
use penca_format::writer::lance::LanceFormatWriter;
use penca_format::writer::parquet::ParquetFormatWriter;
use tokio::runtime::Runtime;
use url::Url;

const SEGMENT_URI: &str = "seg";

#[derive(Debug, Clone, Copy)]
enum Fmt {
    Lance,
    Parquet,
}

fn formats() -> Vec<Fmt> {
    if std::env::var("PERF_FLOOR_PARQUET").is_ok() {
        vec![Fmt::Lance, Fmt::Parquet]
    } else {
        vec![Fmt::Lance]
    }
}

fn sizes() -> Vec<i64> {
    if matches!(
        std::env::var("PERF_FLOOR_MAX").as_deref(),
        Ok("1m") | Ok("1M")
    ) {
        vec![100_000, 1_000_000]
    } else {
        vec![100_000]
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("row_uuid", DataType::Utf8, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

fn segment_batch(n: i64) -> RecordBatch {
    let uuids: Vec<String> = (0..n).map(|i| format!("u-{i}")).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(
                uuids.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())),
        ],
    )
    .expect("valid segment batch")
}

async fn lance_store_and_raw() -> (Arc<LanceObjectStore>, Arc<dyn ObjectStore>) {
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    #[allow(deprecated)]
    let params = ObjectStoreParams {
        object_store: Some((raw.clone(), Url::parse("memory:///").unwrap())),
        ..Default::default()
    };
    let (lance_store, _) = LanceObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        "memory:///",
        &params,
    )
    .await
    .expect("lance object store");
    (lance_store, raw)
}

/// Write `batch` to a fresh store and return a reader rooted there.
async fn write_and_reader(fmt: Fmt, batch: &RecordBatch) -> AnyFormatReader {
    match fmt {
        Fmt::Parquet => {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            ParquetFormatWriter::new(store.clone(), String::new())
                .write(SEGMENT_URI, batch)
                .await
                .expect("parquet write");
            AnyFormatReader::Parquet(ParquetFormatReader::new(store, String::new()))
        }
        Fmt::Lance => {
            let (lance_store, raw) = lance_store_and_raw().await;
            LanceFormatWriter::new(lance_store.clone(), raw.clone(), String::new())
                .write(SEGMENT_URI, batch)
                .await
                .expect("lance write");
            AnyFormatReader::Lance(LanceFormatReader::new(lance_store, String::new()))
        }
    }
}

/// Write `batch` to a fresh store (no reader) — the write-floor routine.
async fn write_only(fmt: Fmt, batch: &RecordBatch) {
    match fmt {
        Fmt::Parquet => {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            ParquetFormatWriter::new(store, String::new())
                .write(SEGMENT_URI, batch)
                .await
                .expect("parquet write");
        }
        Fmt::Lance => {
            let (lance_store, raw) = lance_store_and_raw().await;
            LanceFormatWriter::new(lance_store, raw, String::new())
                .write(SEGMENT_URI, batch)
                .await
                .expect("lance write");
        }
    }
}

fn cold_segment_read_floor(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let sch = schema();

    // READ floor: whole-segment + client filter vs (offset,length) range.
    let mut rg = c.benchmark_group("cold_segment_read");
    rg.sample_size(10);
    for &fmt in &formats() {
        for &n in &sizes() {
            let reader = rt.block_on(write_and_reader(fmt, &segment_batch(n)));
            let target = format!("u-{}", n / 2);
            rg.throughput(Throughput::Elements(n as u64));
            // Arm (a): read whole segment, client-side filter for the target row.
            rg.bench_with_input(
                BenchmarkId::new(format!("{fmt:?}_whole_filter"), n),
                &n,
                |b, _| {
                    b.iter(|| {
                        let batch = rt
                            .block_on(reader.read_segment(SEGMENT_URI, None, None, &sch, None))
                            .expect("whole read");
                        let col = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let hit = (0..batch.num_rows()).find(|&i| col.value(i) == target);
                        black_box(hit);
                    });
                },
            );
            // Arm (b): (offset, length) range read — the pushdown.
            rg.bench_with_input(
                BenchmarkId::new(format!("{fmt:?}_range_pushdown"), n),
                &n,
                |b, &n| {
                    b.iter(|| {
                        let batch = rt
                            .block_on(reader.read_segment(
                                SEGMENT_URI,
                                Some(n / 2),
                                Some(1),
                                &sch,
                                None,
                            ))
                            .expect("range read");
                        black_box(batch.num_rows());
                    });
                },
            );
        }
    }
    rg.finish();

    // WRITE floor: encode + persist a segment.
    let mut wg = c.benchmark_group("cold_segment_write");
    wg.sample_size(10);
    for &fmt in &formats() {
        for &n in &sizes() {
            let batch = segment_batch(n);
            wg.throughput(Throughput::Elements(n as u64));
            wg.bench_with_input(
                BenchmarkId::new(format!("{fmt:?}_write"), n),
                &batch,
                |b, batch| {
                    b.iter(|| {
                        rt.block_on(write_only(fmt, batch));
                    });
                },
            );
        }
    }
    wg.finish();
}

criterion_group!(benches, cold_segment_read_floor);
criterion_main!(benches);
