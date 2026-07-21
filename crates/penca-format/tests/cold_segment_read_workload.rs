//! Workload-correctness guard for the cold-segment read floor (CHA-415 #2a).
//!
//! The `cold_segment_read_floor` bench (CHA-415 I2a) times the two CHA-348 read
//! arms against a Lance/Parquet segment: (a) whole-segment read + client-side
//! filter, (b) `(offset, length)` range read (the pushdown). A throughput
//! number is only meaningful if both arms actually return the intended row, so
//! this test locks that: it writes a segment, reads the target row each way,
//! and asserts the two arms agree.
//!
//! This pins `FormatReader::read_segment` read *semantics* directly (whole vs
//! range), over an in-memory object store — so it locks arm-agreement, not
//! cold-tier I/O latency (the real GET+decode floor is the bench's concern).
//! Runs over both formats so the optional Parquet arm (CHA-61) is covered.
//! Characterization guard:
//! `read_segment` is existing correct production code, so this passes on first
//! write (not fail-first TDD); its job is to keep the bench measuring a real
//! point lookup and to catch a future read-contract regression.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
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
use url::Url;

const SEGMENT_URI: &str = "seg";

#[derive(Debug, Clone, Copy)]
enum Fmt {
    Lance,
    Parquet,
}

const ALL_FORMATS: [Fmt; 2] = [Fmt::Lance, Fmt::Parquet];

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

/// `row_uuid: Utf8`, `val: Int64`.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("row_uuid", DataType::Utf8, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

/// `n` rows in insertion order: `row_uuid = "u-{i}"`, `val = i`.
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

fn val_of(batch: &RecordBatch, row: usize) -> i64 {
    batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("val column")
        .value(row)
}

fn ruuid_of(batch: &RecordBatch, row: usize) -> String {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("row_uuid column")
        .value(row)
        .to_string()
}

/// Both CHA-348 read arms return the same target row from a cold segment.
#[tokio::test]
async fn cold_point_lookup_arms_agree() {
    const N: i64 = 100;
    const TARGET: usize = 42; // row_uuid "u-42", val 42, at offset 42

    for fmt in ALL_FORMATS {
        let reader = write_and_reader(fmt, &segment_batch(N)).await;
        let sch = schema();

        // Arm (a): whole-segment read + client-side filter for the target PK.
        let whole = reader
            .read_segment(SEGMENT_URI, None, None, &sch, None)
            .await
            .unwrap_or_else(|e| panic!("{fmt:?}: whole read failed: {e}"));
        assert_eq!(
            whole.num_rows(),
            N as usize,
            "{fmt:?}: whole returns all rows"
        );
        let hit = (0..whole.num_rows())
            .find(|&r| ruuid_of(&whole, r) == format!("u-{TARGET}"))
            .expect("target present in whole-segment read");
        let whole_val = val_of(&whole, hit);
        assert_eq!(whole_val, TARGET as i64, "{fmt:?}: whole-arm value");

        // Arm (b): (offset, length) range read — the pushdown.
        let sliced = reader
            .read_segment(SEGMENT_URI, Some(TARGET as i64), Some(1), &sch, None)
            .await
            .unwrap_or_else(|e| panic!("{fmt:?}: sliced read failed: {e}"));
        assert_eq!(
            sliced.num_rows(),
            1,
            "{fmt:?}: range returns exactly one row"
        );
        assert_eq!(
            ruuid_of(&sliced, 0),
            format!("u-{TARGET}"),
            "{fmt:?}: range arm row identity"
        );
        let slice_val = val_of(&sliced, 0);

        // The two arms agree (A/B is cost-only, not semantics).
        assert_eq!(
            whole_val, slice_val,
            "{fmt:?}: whole and range arms must agree"
        );
    }
}
