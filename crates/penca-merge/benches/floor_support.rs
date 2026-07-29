//! Shared harness for the merge fan-in floor bench.
//!
//! Drives the REAL `penca_dl::DatafusionDlDriver::scan_snapshot` (the
//! `SnapshotTableProvider` path) over an in-memory cold base, so the bench
//! measures the actual DataFusion exclusion anti-join + snapshot scan as hot
//! churn (the exclusion-set size) grows. An in-memory `FormatReader` serves the
//! decoded base directly, so no Lance/S3 read cost is in the path — only the
//! DataFusion compute. `#[path]`-included by `benches/merge_fanin_floor.rs` and
//! `tests/merge_fanin_floor_workload.rs` (benches and integration tests cannot
//! share a normal module).
#![allow(dead_code)]

use penca_merge::SegmentOrder;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures_util::TryStreamExt;
use penca_core::{Format, IndexSidecar, SnapshotSegment};
use penca_dl::build_cold_session_template;
use penca_dl::cache::SegmentCache;
use penca_dl::driver::{DatafusionDlDriver, DlDriver, SeekSpec};
use penca_format::reader::{FormatError, FormatReader};

/// Cold-base schema: `row_uuid, name, value`.
pub fn base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("row_uuid", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]))
}

/// Cold-base row counts the floor benches sweep: the 100k base by default, plus
/// the 1M base when `PERF_FLOOR_MAX=1m`. Shared by the point-lookup and merge
/// fan-in benches (both `#[path]`-include this harness).
pub fn bases() -> Vec<u64> {
    if matches!(
        std::env::var("PERF_FLOOR_MAX").as_deref(),
        Ok("1m") | Ok("1M")
    ) {
        vec![100_000, 1_000_000]
    } else {
        vec![100_000]
    }
}

/// A cold base of `n` rows: `row_uuid = "r{i}"`, `name = "n{i}"`, `value = i`.
pub fn base_batch(n: u64) -> RecordBatch {
    let uuids: Vec<String> = (0..n).map(|i| format!("r{i}")).collect();
    let names: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
    let vals: Vec<i32> = (0..n).map(|i| i as i32).collect();
    RecordBatch::try_new(
        base_schema(),
        vec![
            Arc::new(StringArray::from(
                uuids.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                names.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(vals)),
        ],
    )
    .expect("valid base batch")
}

/// In-memory `FormatReader`: returns a fixed decoded batch (no object-store GET
/// / decode), so the bench isolates the DataFusion anti-join, not Lance read.
pub struct InMemoryFormatReader {
    pub batch: RecordBatch,
}

impl FormatReader for InMemoryFormatReader {
    async fn read_segment(
        &self,
        _uri: &str,
        _offset: Option<i64>,
        _length: Option<i64>,
        _schema: &SchemaRef,
        _projection: Option<&[&str]>,
    ) -> Result<RecordBatch, FormatError> {
        Ok(self.batch.clone())
    }
}

/// Build a real `DatafusionDlDriver` whose only reader (Parquet) serves `batch`
/// in-memory, with a warm cache so repeated scans skip re-decode. The reader is
/// keyed by the segment's format wire code so cold-scan dispatch resolves it.
pub fn driver_for(batch: RecordBatch) -> DatafusionDlDriver<InMemoryFormatReader> {
    let mut readers = HashMap::new();
    readers.insert(
        Format::Parquet.as_wire_code(),
        InMemoryFormatReader { batch },
    );
    let cache = Arc::new(SegmentCache::new(1 << 30));
    let template = Arc::new(build_cold_session_template());
    DatafusionDlDriver::new(Arc::new(readers), cache, template)
}

/// One snapshot segment (Parquet) standing for the cold base.
pub fn base_segment(size_bytes: i64) -> SnapshotSegment {
    SnapshotSegment {
        table_snapshot_segment_uuid: "floor-seg".to_string(),
        format: Format::Parquet,
        size_bytes,
        ..Default::default()
    }
}

/// The merge fan-in snapshot-scan SQL: project the user cols, anti-join the
/// exclusion set.
/// Mirrors the (crate-private) `build_cold_snapshot_scan` shape, as the penca-dl
/// `scan_snapshot` tests do — the provider registers the segment under `l` and
/// the exclusion `row_uuid`s under `exclusion`.
pub const SCAN_SQL: &str = "SELECT l.row_uuid, l.\"name\", l.\"value\" FROM l \
     WHERE l.row_uuid NOT IN (SELECT row_uuid FROM exclusion)";

/// The cold point-lookup SQL: the production point-lookup plan — the
/// exclusion anti-join **plus** a PK equality residual, the exact shape
/// `build_cold_snapshot_scan` always emits (`NOT IN (SELECT row_uuid FROM
/// exclusion) AND (<residual>)`; the residual is never short-circuited, even for
/// an empty exclusion — `stream_merged` calls it unconditionally). The bench passes
/// an EMPTY exclusion, so the anti-join runs over an empty set — the plan a point
/// lookup with no hot overlay actually executes. DataFusion scans the full
/// segment and applies the anti-semi-join + the residual `FilterExec` (ADR 0023 —
/// no predicate pushdown); that O(rows) scan-and-filter is what the sort-order
/// and secondary-index work turns into O(log n). `target` selects the existing
/// `row_uuid`
/// `r{target}`; a `target` past the base size selects no rows.
pub fn point_lookup_sql(target: u64) -> String {
    format!(
        "SELECT l.row_uuid, l.\"name\", l.\"value\" FROM l \
         WHERE l.row_uuid NOT IN (SELECT row_uuid FROM exclusion) \
         AND (l.row_uuid = 'r{target}')"
    )
}

/// Run `scan_snapshot` over `segments` with `exclusion`, planning `sql`; return
/// the row count.
pub async fn scan_rows(
    dl: &DatafusionDlDriver<InMemoryFormatReader>,
    segments: &[SnapshotSegment],
    schema: &SchemaRef,
    exclusion: &[String],
    sql: &str,
) -> usize {
    let stream = dl
        .scan_snapshot(
            segments,
            schema,
            schema,
            exclusion,
            sql,
            4,
            SegmentOrder::ByCompletion,
            None,
        )
        .await
        .expect("scan_snapshot");
    let batches: Vec<RecordBatch> = stream.try_collect().await.expect("collect scan");
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Like [`scan_rows`] but returns the surviving `row_uuid`s (sorted), so a
/// caller can assert WHICH rows the scan kept — not just how many.
pub async fn scan_uuids(
    dl: &DatafusionDlDriver<InMemoryFormatReader>,
    segments: &[SnapshotSegment],
    schema: &SchemaRef,
    exclusion: &[String],
    sql: &str,
) -> Vec<String> {
    use arrow::array::{Array, StringArray};
    let stream = dl
        .scan_snapshot(
            segments,
            schema,
            schema,
            exclusion,
            sql,
            4,
            SegmentOrder::ByCompletion,
            None,
        )
        .await
        .expect("scan_snapshot");
    let batches: Vec<RecordBatch> = stream.try_collect().await.expect("collect scan");
    let mut out = Vec::new();
    for b in &batches {
        let idx = b.schema().index_of("row_uuid").expect("row_uuid column");
        let col = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("row_uuid is utf8");
        for i in 0..b.num_rows() {
            out.push(col.value(i).to_string());
        }
    }
    out.sort();
    out
}

// CHA-454 seek variant

/// In-memory reader serving the sorted index sidecar for the sidecar uri and the
/// base batch for everything else — the index seek reads both.
pub struct SeekFormatReader {
    pub base: RecordBatch,
    pub sidecar: RecordBatch,
}

impl FormatReader for SeekFormatReader {
    async fn read_segment(
        &self,
        uri: &str,
        _offset: Option<i64>,
        _length: Option<i64>,
        _schema: &SchemaRef,
        _projection: Option<&[&str]>,
    ) -> Result<RecordBatch, FormatError> {
        if uri == "mem://sidecar" {
            Ok(self.sidecar.clone())
        } else {
            Ok(self.base.clone())
        }
    }
}

/// A `DatafusionDlDriver` whose reader serves `base` + its `row_uuid` sidecar.
pub fn driver_for_seek(base: RecordBatch) -> DatafusionDlDriver<SeekFormatReader> {
    let sidecar = penca_format::index::build_segment_index(std::slice::from_ref(base.column(0)))
        .expect("build sidecar");
    let mut readers = HashMap::new();
    readers.insert(
        Format::Parquet.as_wire_code(),
        SeekFormatReader { base, sidecar },
    );
    let cache = Arc::new(SegmentCache::new(1 << 30));
    let template = Arc::new(build_cold_session_template());
    DatafusionDlDriver::new(Arc::new(readers), cache, template)
}

/// The cold base segment carrying its internal `row_uuid` index sidecar, so the
/// provider takes the CHA-454 index-seek path.
pub fn base_segment_with_sidecar(size_bytes: i64, rows: i64) -> SnapshotSegment {
    SnapshotSegment {
        table_snapshot_segment_uuid: "floor-seg".to_string(),
        uri: "mem://base".to_string(),
        format: Format::Parquet,
        size_bytes,
        row_uuid_index_sidecar: Some(IndexSidecar {
            object_uri: "mem://sidecar".to_string(),
            offset: 0,
            length: rows,
            format: Format::Parquet,
            segment_index_uuid: "floor-sidecar".to_string(),
            size_bytes: 1 << 16,
        }),
        ..Default::default()
    }
}

/// Seek variant of [`scan_rows`]: passes an identity seek entry, so the
/// provider binary-searches the sidecar and `take`s only the matched rows
/// (O(matches)) instead of the O(rows) full scan + residual filter.
pub async fn scan_rows_seek(
    dl: &DatafusionDlDriver<SeekFormatReader>,
    segments: &[SnapshotSegment],
    schema: &SchemaRef,
    sql: &str,
    seek_keys: Arc<Vec<String>>,
) -> usize {
    let seeks = Arc::new(vec![SeekSpec {
        index_uuid: None,
        key_columns: vec![],
        tuples: seek_keys.iter().map(|k| vec![k.clone()]).collect(),
    }]);
    let stream = dl
        .scan_snapshot(
            segments,
            schema,
            schema,
            &[],
            sql,
            4,
            SegmentOrder::ByCompletion,
            Some(seeks),
        )
        .await
        .expect("scan_snapshot");
    let batches: Vec<RecordBatch> = stream.try_collect().await.expect("collect scan");
    batches.iter().map(|b| b.num_rows()).sum()
}
