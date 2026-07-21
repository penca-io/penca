//! Format-reader acceptance tests, parameterized over every storage format.
//!
//! The `FormatReader` trait has two implementations (Lance, the deploy
//! default, and Parquet) selected per-deployment by `OBJECT_STORAGE_FORMAT`.
//! The read contract must hold identically for both: no predicate filtering
//! (ADR 0022/0023 — the reader returns every row in the slice, projected),
//! schema-evolution null-fill (CHA-252), and `(offset, length)` slicing
//! (CHA-168). These tests write a fixture through each format's writer and
//! assert the contract through its reader, so a format-specific divergence
//! (the class of bug CHA-369 fixed) fails here rather than only in production.
//!
//! The Python integration suite (`integration_query_test.py`) exercises only
//! the configured write format (Lance) end-to-end; the full-stack
//! per-format pass is tracked separately (see CHA-373).

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
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

/// The storage formats every contract test runs over.
#[derive(Debug, Clone, Copy)]
enum Fmt {
    Lance,
    Parquet,
}

const ALL_FORMATS: [Fmt; 2] = [Fmt::Lance, Fmt::Parquet];

/// Build a fresh in-memory Lance object store plus the raw `object_store`
/// backing it (the Lance writer needs both — one for IO, one for deletes).
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

/// Write `batch` to a fresh in-memory store via `fmt`'s writer and return an
/// [`AnyFormatReader`] rooted at the same store. One round-trip per call.
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

/// `amount: Int64`, `row_uuid: Utf8`.
fn base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Int64, false),
        Field::new("row_uuid", DataType::Utf8, false),
    ]))
}

/// `n` rows: `amount = 0..n`, `row_uuid = "u-{i}"`.
fn base_batch(n: i64) -> RecordBatch {
    let amounts: Vec<i64> = (0..n).collect();
    let uuids: Vec<String> = (0..n).map(|i| format!("u-{i}")).collect();
    RecordBatch::try_new(
        base_schema(),
        vec![
            Arc::new(Int64Array::from(amounts)),
            Arc::new(StringArray::from(
                uuids.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("valid base batch")
}

/// The reader returns every row — it does no predicate filtering. Run over an
/// `Int32` column too (the shape the deleted parquet `RowFilter` aborted on,
/// CHA-369) to pin that a non-default column type round-trips on both formats.
#[tokio::test]
async fn format_reader_returns_all_rows() {
    for fmt in ALL_FORMATS {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Int32, false),
            Field::new("row_uuid", DataType::Utf8, false),
        ]));
        let uuids: Vec<String> = (0..30).map(|i| format!("u-{i}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from((0..30i32).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    uuids.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("valid int32 batch");

        let reader = write_and_reader(fmt, &batch).await;
        let out = reader
            .read_segment(SEGMENT_URI, None, None, &schema, None)
            .await
            .unwrap_or_else(|e| panic!("{fmt:?}: read must succeed, got {e}"));

        assert_eq!(out.num_rows(), 30, "{fmt:?}: reader returns every row");
        assert_eq!(out.num_columns(), 2, "{fmt:?}: both columns returned");
    }
}

/// CHA-252: a read whose output schema adds a column absent from the segment
/// (schema evolution — `ALTER TABLE ADD COLUMN` does not rewrite old segments)
/// must succeed by null-filling the added column. Both readers route through
/// `present_columns` + `null_fill_to_schema`, so the behavior must match.
#[tokio::test]
async fn format_reader_null_fills_added_column() {
    for fmt in ALL_FORMATS {
        let reader = write_and_reader(fmt, &base_batch(30)).await;
        // File has {amount, row_uuid}; request an evolved schema adding `added`.
        let evolved: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Int64, false),
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("added", DataType::Int64, true),
        ]));

        let out = reader
            .read_segment(SEGMENT_URI, None, None, &evolved, None)
            .await
            .unwrap_or_else(|e| panic!("{fmt:?}: evolved read must null-fill, got {e}"));

        assert_eq!(out.num_columns(), 3, "{fmt:?}: added column present");
        assert_eq!(out.num_rows(), 30, "{fmt:?}: row count preserved");
        let added = out.schema().index_of("added").expect("added column");
        assert_eq!(
            out.column(added).null_count(),
            30,
            "{fmt:?}: column absent from the file is null-filled",
        );
    }
}

/// CHA-168: a compacted segment is read as a `(offset, length)` slice of the
/// merged file. Parquet expresses this as a `RowSelection`, Lance as a
/// `ReadBatchParams::Range`; both must yield exactly the requested sub-range.
#[tokio::test]
async fn format_reader_slices_offset_length() {
    for fmt in ALL_FORMATS {
        let reader = write_and_reader(fmt, &base_batch(30)).await;
        let out = reader
            .read_segment(SEGMENT_URI, Some(10), Some(10), &base_schema(), None)
            .await
            .unwrap_or_else(|e| panic!("{fmt:?}: sliced read failed: {e}"));

        assert_eq!(out.num_rows(), 10, "{fmt:?}: slice returns `length` rows");
        let amounts = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("amount column");
        let got: Vec<i64> = amounts.iter().flatten().collect();
        assert_eq!(
            got,
            (10..20).collect::<Vec<_>>(),
            "{fmt:?}: slice covers rows [offset, offset+length)",
        );
    }
}

/// A projection narrows the read to the named columns, in the requested order.
#[tokio::test]
async fn format_reader_projection_narrows_columns() {
    for fmt in ALL_FORMATS {
        let reader = write_and_reader(fmt, &base_batch(5)).await;
        let out = reader
            .read_segment(SEGMENT_URI, None, None, &base_schema(), Some(&["row_uuid"]))
            .await
            .unwrap_or_else(|e| panic!("{fmt:?}: projected read failed: {e}"));

        assert_eq!(
            out.num_columns(),
            1,
            "{fmt:?}: projection narrows to one column"
        );
        assert_eq!(
            out.schema().field(0).name(),
            "row_uuid",
            "{fmt:?}: correct column"
        );
        assert_eq!(out.num_rows(), 5, "{fmt:?}: row count preserved");
    }
}
