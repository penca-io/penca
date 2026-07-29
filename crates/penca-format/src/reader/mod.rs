//! Cold storage format readers.
//!
//! [`FormatReader`] defines the interface for reading columnar segment files
//! from object storage. Implementations handle format-specific logic
//! (row group navigation for Parquet, Lance file reader, etc.).

pub mod lance;
pub mod parquet;

use std::future::Future;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// Reads a single columnar segment file from cold storage.
///
/// Methods return `impl Future<...> + Send` instead of using `async fn`
/// so that the returned futures are guaranteed `Send`. This allows callers
/// (e.g. `ColdStorageClient`) to use the trait with `Send`-bounded streams.
///
/// Per-segment, not per-batch: the caller (orchestrator) owns the fan-out
/// across multiple segments and the concat policy. Both `PersistSegment`
/// and `SnapshotSegment` carry `uri` plus `(offset, length)` slice
/// bounds (optional on the persist side; always set for snapshot
/// segments), so the trait takes those three primitives
/// rather than either proto type.
///
/// `projection`, when `Some`, narrows the read to the named columns — the
/// returned `RecordBatch` has only those columns (in the given order).
/// When `None`, all columns in `schema` are read.
///
/// **Validation is the caller's responsibility.** Projection names are
/// expected to be pre-validated against the table schema at the servicer
/// / API boundary so failures happen up front instead of mid-stream. The
/// defensive [`FormatError::UnknownProjectionColumn`] check here exists
/// only so a bug in a caller surfaces a clear error rather than a cryptic
/// panic from the underlying Parquet/Lance reader.
pub trait FormatReader: Send + Sync {
    /// Read one segment file, optionally sliced to `(offset, length)`.
    ///
    /// `offset` and `length` are row counts (set at compact time) that
    /// narrow the read to a sub-range of the file. When either is `None`,
    /// the full file is read.
    ///
    /// The reader does no predicate filtering: it returns every row in the
    /// requested slice, projected to `projection`. Row filtering is owned by
    /// DataFusion in the merge-on-read layer, never delegated to a format
    /// engine (ADR 0023). Callers apply the predicate after the read.
    fn read_segment(
        &self,
        uri: &str,
        offset: Option<i64>,
        length: Option<i64>,
        schema: &SchemaRef,
        projection: Option<&[&str]>,
    ) -> impl Future<Output = Result<RecordBatch, FormatError>> + Send;
}

/// Errors from format read/write operations.
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Parquet(#[from] ::parquet::errors::ParquetError),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    Lance(#[from] lance_core::Error),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("no segments to read")]
    NoSegments,

    #[error("projection column not in schema: {0}")]
    UnknownProjectionColumn(String),

    #[error("column {0} absent from segment and not nullable; cannot null-fill")]
    NonNullableMissingColumn(String),
}

/// Enum dispatch reader supporting both Parquet and Lance formats.
///
/// Used when a single `HashMap<i32, AnyFormatReader>` must hold readers for
/// multiple storage formats. Implements `FormatReader` by delegating to the
/// inner concrete reader.
pub enum AnyFormatReader {
    Parquet(parquet::ParquetFormatReader),
    Lance(lance::LanceFormatReader),
}

impl FormatReader for AnyFormatReader {
    async fn read_segment(
        &self,
        uri: &str,
        offset: Option<i64>,
        length: Option<i64>,
        schema: &SchemaRef,
        projection: Option<&[&str]>,
    ) -> Result<RecordBatch, FormatError> {
        match self {
            Self::Parquet(r) => {
                r.read_segment(uri, offset, length, schema, projection)
                    .await
            }
            Self::Lance(r) => {
                r.read_segment(uri, offset, length, schema, projection)
                    .await
            }
        }
    }
}

/// Create an empty `RecordBatch` matching the given schema.
pub fn empty_batch(schema: &SchemaRef) -> RecordBatch {
    RecordBatch::new_empty(schema.clone())
}

/// Resolve the effective output schema given an optional column projection.
///
/// Returns `schema` unchanged when `projection` is `None`; otherwise returns
/// a new `SchemaRef` containing only the named columns in the given order.
pub(crate) fn project_schema(
    schema: &SchemaRef,
    projection: Option<&[&str]>,
) -> Result<SchemaRef, FormatError> {
    let Some(names) = projection else {
        return Ok(schema.clone());
    };
    let indices: Vec<usize> = names
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| FormatError::UnknownProjectionColumn((*name).to_string()))
        })
        .collect::<Result<_, _>>()?;
    Ok(Arc::new(schema.project(&indices)?))
}

/// Adapt `batch` to exactly `output_schema`, in `output_schema`'s field order.
///
/// A column present in `batch` is taken by name; a column absent from `batch`
/// is filled with nulls — it post-dates this segment (schema evolution:
/// `ALTER TABLE ADD COLUMN` does not rewrite existing segment files, so an old
/// segment legitimately lacks a newer column, whose value for those rows is
/// NULL). The absent field must be nullable; a non-nullable absent column is a
/// real error (`NonNullableMissingColumn`) rather than a silent null-fill.
pub(crate) fn null_fill_to_schema(
    batch: &RecordBatch,
    output_schema: &SchemaRef,
) -> Result<RecordBatch, FormatError> {
    if batch.schema() == *output_schema {
        return Ok(batch.clone());
    }
    let num_rows = batch.num_rows();
    let columns = output_schema
        .fields()
        .iter()
        .map(|field| match batch.schema().index_of(field.name()) {
            Ok(idx) => Ok(batch.column(idx).clone()),
            Err(_) if field.is_nullable() => {
                Ok(arrow::array::new_null_array(field.data_type(), num_rows))
            }
            Err(_) => Err(FormatError::NonNullableMissingColumn(field.name().clone())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

/// The subset of `names` that actually exist as columns in `file_schema`,
/// preserving the requested order. Used by readers to project only the columns
/// physically present in a segment before null-filling the rest.
pub(crate) fn present_columns<'a>(file_schema: &SchemaRef, names: &[&'a str]) -> Vec<&'a str> {
    names
        .iter()
        .copied()
        .filter(|name| file_schema.index_of(name).is_ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Int64, false),
        ]))
    }

    #[test]
    fn project_schema_none_returns_input_schema() {
        let schema = test_schema();
        let got = project_schema(&schema, None).unwrap();
        assert_eq!(got.fields().len(), 3);
        assert_eq!(got.field(0).name(), "a");
    }

    #[test]
    fn project_schema_some_reorders_and_narrows() {
        let schema = test_schema();
        let got = project_schema(&schema, Some(&["c", "a"])).unwrap();
        assert_eq!(got.fields().len(), 2);
        assert_eq!(got.field(0).name(), "c");
        assert_eq!(got.field(1).name(), "a");
    }

    #[test]
    fn project_schema_missing_column_errors() {
        let schema = test_schema();
        let err = project_schema(&schema, Some(&["a", "zzz"])).unwrap_err();
        assert!(matches!(err, FormatError::UnknownProjectionColumn(name) if name == "zzz"));
    }

    #[test]
    fn present_columns_keeps_only_existing_in_file_order() {
        // file has {a, b}; request {b, missing, a} -> keep {b, a} in request order.
        let file = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        assert_eq!(
            present_columns(&file, &["b", "missing", "a"]),
            vec!["b", "a"]
        );
        assert_eq!(present_columns(&file, &["missing"]), Vec::<&str>::new());
    }

    #[test]
    fn null_fill_adds_absent_nullable_column() {
        use arrow::array::Int32Array;
        // batch has {a}; output wants {a, added(nullable)} -> added is all-null.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("added", DataType::Int64, true),
        ]));
        let out = null_fill_to_schema(&batch, &out_schema).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.num_rows(), 3);
        assert_eq!(out.column(1).null_count(), 3, "added column is all-null");
        assert_eq!(out.column(1).len(), 3);
    }

    #[test]
    fn null_fill_reorders_to_output_schema() {
        use arrow::array::Int32Array;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));
        let out = null_fill_to_schema(&batch, &out_schema).unwrap();
        assert_eq!(out.schema().field(0).name(), "b");
        assert_eq!(out.schema().field(1).name(), "a");
    }

    #[test]
    fn null_fill_non_nullable_absent_column_errors() {
        use arrow::array::Int32Array;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("added", DataType::Int64, false), // NOT nullable
        ]));
        let err = null_fill_to_schema(&batch, &out_schema).unwrap_err();
        assert!(matches!(err, FormatError::NonNullableMissingColumn(name) if name == "added"));
    }
}
