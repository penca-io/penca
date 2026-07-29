//! Segment statistics for cold-tier pruning.
//!
//! Persist-side reader-tier pruning is OUT OF SCOPE; persist segments
//! carry stats for `TableProvider::statistics()` aggregation only.
//! See ADR 0022 (`docs/decisions/0022-no-persist-segment-pruning.md`).
//!
//! ## Encoding (v0)
//!
//! Per-segment stats are serde_json bytes carrying a [`SegmentStatsV0`]
//! payload, NOT the canonical Arrow Statistics Schema. The canonical form
//! requires complex MapArray + DenseUnion construction (~300 LOC) whose only
//! payoff is interop with external tools that consume Arrow stats, and Penca
//! segments stay within Penca. The bytes encoding is a private impl detail;
//! the observable `PruningPredicate` behavior is identical, so this can migrate
//! if cross-tool interop ever becomes a requirement.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder,
    Decimal256Builder, Float16Builder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder, UInt8Builder,
    UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit, i256};
use arrow::record_batch::RecordBatch;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
// Re-exported because `ParsedColumnStats` exposes `ScalarValue` in this
// module's public API, and consumers without a datafusion dependency must
// still be able to name it.
pub use datafusion::scalar::ScalarValue;
use half::f16;
use penca_core::types::CanonicalType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct ParsedColumnStats {
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub null_count: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ParsedSegmentStats {
    pub row_count: usize,
    pub per_column: Vec<ParsedColumnStats>,
}

// Per-column entries are keyed by **column name**, not by position. The
// writer's batch schema does not always match the reader's user schema:
// snapshot/persist writes go through `snapshot_read_schema(user_schema)`
// which prepends `row_uuid`, so a positional encoding would offset every
// user-column lookup by one (and silently degrade to keep-all via
// PerSegmentBuilders' type-mismatch fallthrough). Keying by name lets the two
// sides disagree on schema shape and still resolve correctly.

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentStatsV0 {
    row_count: u64,
    per_column: Vec<ColumnStatsV0>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnStatsV0 {
    name: String,
    min: Option<TypedScalarV0>,
    max: Option<TypedScalarV0>,
    null_count: Option<u64>,
}

/// Typed scalar wire representation for the v0 stats format. Only the
/// types that can drive `PruningPredicate` are encoded; unsupported
/// types are stored as `None` and degrade to "no pruning" for that
/// column at read time.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum TypedScalarV0 {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    // Half-precision float stored by its raw IEEE-754 bits (f16 has no
    // direct serde impl); reconstructed via f16::from_bits.
    Float16(u16),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    // Milliseconds since the Unix epoch (Arrow Date64).
    Date64(i64),
    // Timezone-naive microsecond timestamp (Phase-1 declarable form).
    TimestampMicros(i64),
    // Carries precision + scale so the reader reconstructs the exact
    // ScalarValue::Decimal128 variant.
    Decimal128 {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
    // i256 has no serde impl; store its little-endian bytes (lossless,
    // infallible round-trip) plus precision + scale.
    Decimal256 {
        unscaled_le: [u8; 32],
        precision: u8,
        scale: i8,
    },
}

impl TypedScalarV0 {
    fn from_scalar(s: &ScalarValue) -> Option<Self> {
        match s {
            ScalarValue::Boolean(Some(v)) => Some(Self::Bool(*v)),
            ScalarValue::Int8(Some(v)) => Some(Self::Int8(*v)),
            ScalarValue::Int16(Some(v)) => Some(Self::Int16(*v)),
            ScalarValue::Int32(Some(v)) => Some(Self::Int32(*v)),
            ScalarValue::Int64(Some(v)) => Some(Self::Int64(*v)),
            ScalarValue::UInt8(Some(v)) => Some(Self::UInt8(*v)),
            ScalarValue::UInt16(Some(v)) => Some(Self::UInt16(*v)),
            ScalarValue::UInt32(Some(v)) => Some(Self::UInt32(*v)),
            ScalarValue::UInt64(Some(v)) => Some(Self::UInt64(*v)),
            ScalarValue::Float16(Some(v)) => Some(Self::Float16(v.to_bits())),
            ScalarValue::Float32(Some(v)) => Some(Self::Float32(*v)),
            ScalarValue::Float64(Some(v)) => Some(Self::Float64(*v)),
            ScalarValue::Utf8(Some(v)) => Some(Self::Utf8(v.clone())),
            ScalarValue::Date32(Some(v)) => Some(Self::Date32(*v)),
            ScalarValue::Date64(Some(v)) => Some(Self::Date64(*v)),
            ScalarValue::TimestampMicrosecond(Some(v), _) => Some(Self::TimestampMicros(*v)),
            ScalarValue::Decimal128(Some(v), precision, scale) => Some(Self::Decimal128 {
                unscaled: *v,
                precision: *precision,
                scale: *scale,
            }),
            ScalarValue::Decimal256(Some(v), precision, scale) => Some(Self::Decimal256 {
                unscaled_le: v.to_le_bytes(),
                precision: *precision,
                scale: *scale,
            }),
            _ => None,
        }
    }

    fn to_scalar(&self) -> ScalarValue {
        match self {
            Self::Bool(v) => ScalarValue::Boolean(Some(*v)),
            Self::Int8(v) => ScalarValue::Int8(Some(*v)),
            Self::Int16(v) => ScalarValue::Int16(Some(*v)),
            Self::Int32(v) => ScalarValue::Int32(Some(*v)),
            Self::Int64(v) => ScalarValue::Int64(Some(*v)),
            Self::UInt8(v) => ScalarValue::UInt8(Some(*v)),
            Self::UInt16(v) => ScalarValue::UInt16(Some(*v)),
            Self::UInt32(v) => ScalarValue::UInt32(Some(*v)),
            Self::UInt64(v) => ScalarValue::UInt64(Some(*v)),
            Self::Float16(bits) => ScalarValue::Float16(Some(f16::from_bits(*bits))),
            Self::Float32(v) => ScalarValue::Float32(Some(*v)),
            Self::Float64(v) => ScalarValue::Float64(Some(*v)),
            Self::Utf8(v) => ScalarValue::Utf8(Some(v.clone())),
            Self::Date32(v) => ScalarValue::Date32(Some(*v)),
            Self::Date64(v) => ScalarValue::Date64(Some(*v)),
            Self::TimestampMicros(v) => ScalarValue::TimestampMicrosecond(Some(*v), None),
            Self::Decimal128 {
                unscaled,
                precision,
                scale,
            } => ScalarValue::Decimal128(Some(*unscaled), *precision, *scale),
            Self::Decimal256 {
                unscaled_le,
                precision,
                scale,
            } => {
                ScalarValue::Decimal256(Some(i256::from_le_bytes(*unscaled_le)), *precision, *scale)
            }
        }
    }
}

fn data_type_is_prunable(dt: &DataType) -> bool {
    // Both gates are load-bearing: marking a type prunable without a
    // `PerSegmentBuilders` arm would panic in `build_array`.
    CanonicalType::from_arrow(dt).is_ok_and(|ct| ct.is_prunable() && stats_has_builder(&ct))
}

/// Canonical types the segment-stats machinery can currently min/max and
/// build pruning arrays for (a subset of the prunable target set).
///
/// `Time32`/`Time64` and tz-aware / non-microsecond `Timestamp` are
/// `is_prunable()` but intentionally absent here: their multi-unit /
/// tz-carrying stats machinery is TODO(CHA-390), so they ship null stats
/// (keep-all) — the same safe degrade `LargeUtf8`/`Utf8View` use.
fn stats_has_builder(ct: &CanonicalType) -> bool {
    matches!(
        ct,
        CanonicalType::Boolean
            | CanonicalType::Int8
            | CanonicalType::Int16
            | CanonicalType::Int32
            | CanonicalType::Int64
            | CanonicalType::UInt8
            | CanonicalType::UInt16
            | CanonicalType::UInt32
            | CanonicalType::UInt64
            | CanonicalType::Float16
            | CanonicalType::Float32
            | CanonicalType::Float64
            | CanonicalType::Utf8
            | CanonicalType::Date32
            | CanonicalType::Date64
            | CanonicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                tz: None,
            }
            | CanonicalType::Decimal128 { .. }
            | CanonicalType::Decimal256 { .. }
    )
}

pub fn compute_segment_statistics(batch: &RecordBatch) -> Vec<u8> {
    let schema = batch.schema();
    let row_count = batch.num_rows() as u64;
    let per_column = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let name = field.name().clone();
            if !data_type_is_prunable(field.data_type()) {
                return ColumnStatsV0 {
                    name,
                    min: None,
                    max: None,
                    null_count: Some(batch.column(idx).null_count() as u64),
                };
            }
            let array = batch.column(idx);
            let (min, max) = column_min_max(array, field.data_type());
            ColumnStatsV0 {
                name,
                min: min.as_ref().and_then(TypedScalarV0::from_scalar),
                max: max.as_ref().and_then(TypedScalarV0::from_scalar),
                null_count: Some(array.null_count() as u64),
            }
        })
        .collect();

    let payload = SegmentStatsV0 {
        row_count,
        per_column,
    };
    match serde_json::to_vec(&payload) {
        Ok(bytes) => bytes,
        Err(err) => {
            // Unreachable for any input this fn produces: every
            // TypedScalarV0 variant is a primitive / String shape serde
            // round-trips. If it ever fires the segment ships without stats
            // (keep-all), so the event is the only signal of the degrade.
            tracing::error!(
                target: "penca_dl::stats",
                err = %err,
                row_count,
                n_columns = schema.fields().len(),
                "compute_segment_statistics serialization failed; segment will ship without stats"
            );
            Vec::new()
        }
    }
}

fn column_min_max(array: &ArrayRef, dt: &DataType) -> (Option<ScalarValue>, Option<ScalarValue>) {
    use arrow::array::{
        BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array, Float16Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::compute::kernels::aggregate::{
        max as max_kernel, max_boolean, max_string, min as min_kernel, min_boolean, min_string,
    };

    match dt {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>();
            a.map(|a| {
                (
                    min_boolean(a).map(|v| ScalarValue::Boolean(Some(v))),
                    max_boolean(a).map(|v| ScalarValue::Boolean(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Int32(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::Int32(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Int64(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::Int64(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Float32(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::Float32(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Float16 => {
            let a = array.as_any().downcast_ref::<Float16Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Float16(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::Float16(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Float64(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::Float64(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>();
            a.map(|a| {
                (
                    min_string(a).map(|v| ScalarValue::Utf8(Some(v.to_string()))),
                    max_string(a).map(|v| ScalarValue::Utf8(Some(v.to_string()))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Int16(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::Int16(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Int8(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::Int8(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::UInt8(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::UInt8(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::UInt16(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::UInt16(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::UInt32(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::UInt32(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::UInt64(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::UInt64(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Date32(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::Date32(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Date64 => {
            let a = array.as_any().downcast_ref::<Date64Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Date64(Some(v))),
                    max_kernel(a).map(|v| ScalarValue::Date64(Some(v))),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let a = array.as_any().downcast_ref::<TimestampMicrosecondArray>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::TimestampMicrosecond(Some(v), None)),
                    max_kernel(a).map(|v| ScalarValue::TimestampMicrosecond(Some(v), None)),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Decimal128(precision, scale) => {
            let (precision, scale) = (*precision, *scale);
            let a = array.as_any().downcast_ref::<Decimal128Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Decimal128(Some(v), precision, scale)),
                    max_kernel(a).map(|v| ScalarValue::Decimal128(Some(v), precision, scale)),
                )
            })
            .unwrap_or((None, None))
        }
        DataType::Decimal256(precision, scale) => {
            let (precision, scale) = (*precision, *scale);
            let a = array.as_any().downcast_ref::<Decimal256Array>();
            a.map(|a| {
                (
                    min_kernel(a).map(|v| ScalarValue::Decimal256(Some(v), precision, scale)),
                    max_kernel(a).map(|v| ScalarValue::Decimal256(Some(v), precision, scale)),
                )
            })
            .unwrap_or((None, None))
        }
        _ => (None, None),
    }
}

pub fn parse_segment_statistics(bytes: &[u8], schema: &SchemaRef) -> ParsedSegmentStats {
    let empty = ParsedSegmentStats {
        row_count: 0,
        per_column: (0..schema.fields().len())
            .map(|_| ParsedColumnStats {
                min: None,
                max: None,
                null_count: None,
            })
            .collect(),
    };
    if bytes.is_empty() {
        return empty;
    }
    let payload: SegmentStatsV0 = match serde_json::from_slice(bytes) {
        Ok(p) => p,
        Err(_) => return empty,
    };
    // Resolve by name, not position: reader/writer schemas can disagree on
    // column ordering or presence. An unknown column degrades to "no stats for
    // that column"; duplicates resolve to the first match.
    let per_column = schema
        .fields()
        .iter()
        .map(|field| {
            payload
                .per_column
                .iter()
                .find(|c| c.name == *field.name())
                .map(|c| ParsedColumnStats {
                    min: c.min.as_ref().map(TypedScalarV0::to_scalar),
                    max: c.max.as_ref().map(TypedScalarV0::to_scalar),
                    null_count: c.null_count.map(|n| n as usize),
                })
                .unwrap_or(ParsedColumnStats {
                    min: None,
                    max: None,
                    null_count: None,
                })
        })
        .collect();
    ParsedSegmentStats {
        row_count: payload.row_count as usize,
        per_column,
    }
}

pub struct SegmentPruningStats {
    pub schema: SchemaRef,
    pub parsed: Vec<ParsedSegmentStats>,
}

impl SegmentPruningStats {
    fn column_index(&self, name: &str) -> Option<usize> {
        self.schema.fields().iter().position(|f| f.name() == name)
    }

    fn build_array<F>(&self, name: &str, mut emit: F) -> Option<ArrayRef>
    where
        F: FnMut(&ParsedColumnStats, &mut PerSegmentBuilders, usize),
    {
        let idx = self.column_index(name)?;
        let dt = self.schema.field(idx).data_type().clone();
        // `None` here means "no stats for this column", which DataFusion
        // treats as keep-all — correct for a column we never compute stats
        // over. Short-circuiting at planning time (rather than per-segment in
        // push_scalar) also keeps `PerSegmentBuilders::new` from ever seeing
        // an unprunable type, which would hit its panic arm.
        if !data_type_is_prunable(&dt) {
            return None;
        }
        let mut builders = PerSegmentBuilders::new(&dt, name);
        for (seg_idx, seg) in self.parsed.iter().enumerate() {
            let col = seg.per_column.get(idx);
            match col {
                Some(c) => emit(c, &mut builders, seg_idx),
                None => builders.push_null(),
            }
        }
        builders.finish()
    }
}

struct PerSegmentBuilders {
    kind: BuilderKind,
    /// Schema column name this builder is accumulating stats for; reported in
    /// `warn_mismatch` so the operator sees *which* column drifted.
    /// `"<row-count>"` / `"<null-count>"` for the U64 metadata-array variants.
    column_name: String,
}

enum BuilderKind {
    Bool(BooleanBuilder),
    I8(Int8Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    U8(UInt8Builder),
    U16(UInt16Builder),
    U32(UInt32Builder),
    F16(Float16Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Date32(Date32Builder),
    Date64(Date64Builder),
    TsMicro(TimestampMicrosecondBuilder),
    Dec128(Decimal128Builder),
    Dec256(Decimal256Builder),
    /// `UInt64` carries double duty: a real `UInt64` *column* builder (via
    /// `push_scalar`) AND the `null_counts` / `row_counts` metadata arrays,
    /// which DataFusion requires to be `UInt64` regardless of the source
    /// column type (via `push_u64`). The two entry points must never mix on
    /// one instance: `new_u64` builders only ever see `push_u64`, `new`
    /// builders only ever see `push_scalar`.
    U64(UInt64Builder),
}

impl PerSegmentBuilders {
    /// Construct a builder for a prunable column type. `build_array`
    /// short-circuits on unprunable types before calling this, so
    /// every DataType reaching here is one of the supported prunable
    /// variants — the panic in the catchall is a programming-error
    /// guard, not a user-reachable path.
    fn new(dt: &DataType, column_name: &str) -> Self {
        let kind = match dt {
            DataType::Boolean => BuilderKind::Bool(BooleanBuilder::new()),
            DataType::Int8 => BuilderKind::I8(Int8Builder::new()),
            DataType::Int16 => BuilderKind::I16(Int16Builder::new()),
            DataType::Int32 => BuilderKind::I32(Int32Builder::new()),
            DataType::Int64 => BuilderKind::I64(Int64Builder::new()),
            DataType::UInt8 => BuilderKind::U8(UInt8Builder::new()),
            DataType::UInt16 => BuilderKind::U16(UInt16Builder::new()),
            DataType::UInt32 => BuilderKind::U32(UInt32Builder::new()),
            DataType::UInt64 => BuilderKind::U64(UInt64Builder::new()),
            DataType::Float16 => BuilderKind::F16(Float16Builder::new()),
            DataType::Float32 => BuilderKind::F32(Float32Builder::new()),
            DataType::Float64 => BuilderKind::F64(Float64Builder::new()),
            DataType::Utf8 => BuilderKind::Utf8(StringBuilder::new()),
            DataType::Date32 => BuilderKind::Date32(Date32Builder::new()),
            DataType::Date64 => BuilderKind::Date64(Date64Builder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                BuilderKind::TsMicro(TimestampMicrosecondBuilder::new())
            }
            DataType::Decimal128(precision, scale) => BuilderKind::Dec128(
                Decimal128Builder::new()
                    .with_precision_and_scale(*precision, *scale)
                    // (precision, scale) come from a materialized
                    // Decimal128Array, so the pair is Arrow-valid.
                    .expect("Decimal128 precision/scale already valid for a materialized array"),
            ),
            DataType::Decimal256(precision, scale) => BuilderKind::Dec256(
                Decimal256Builder::new()
                    .with_precision_and_scale(*precision, *scale)
                    // (precision, scale) come from a materialized
                    // Decimal256Array, so the pair is Arrow-valid.
                    .expect("Decimal256 precision/scale already valid for a materialized array"),
            ),
            other => panic!(
                "PerSegmentBuilders::new called on unprunable DataType `{other:?}` for column \
                 `{column_name}` — build_array must short-circuit unprunable types before \
                 constructing a builder. This is a programming error."
            ),
        };
        Self {
            kind,
            column_name: column_name.to_string(),
        }
    }

    fn new_u64(column_name: &str) -> Self {
        Self {
            kind: BuilderKind::U64(UInt64Builder::new()),
            column_name: column_name.to_string(),
        }
    }

    /// Append a typed scalar to whichever per-segment builder this
    /// `PerSegmentBuilders` holds. A `None` scalar, or a matching variant
    /// carrying `None`, appends a null.
    ///
    /// A scalar variant that does NOT match the builder kind appends a null and
    /// warns. It should be structurally impossible — name-keyed
    /// `SegmentStatsV0` makes writer and reader resolve the same `ScalarValue`
    /// variant per column — so firing means either a column type changed
    /// without updating both ends, or a new `TypedScalarV0` variant was added
    /// without a matching `BuilderKind`. The null (rather than a skip) is
    /// required: builder length must stay equal to `num_containers()` or
    /// `PruningPredicate::prune` errors.
    fn push_scalar(&mut self, value: &Option<ScalarValue>, seg_idx: usize) {
        match (&mut self.kind, value) {
            (BuilderKind::Bool(b), Some(ScalarValue::Boolean(Some(v)))) => b.append_value(*v),
            (BuilderKind::Bool(b), Some(ScalarValue::Boolean(None)) | None) => b.append_null(),
            (BuilderKind::Bool(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "Bool", other);
                b.append_null();
            }
            (BuilderKind::I32(b), Some(ScalarValue::Int32(Some(v)))) => b.append_value(*v),
            (BuilderKind::I32(b), Some(ScalarValue::Int32(None)) | None) => b.append_null(),
            (BuilderKind::I32(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "I32", other);
                b.append_null();
            }
            (BuilderKind::I64(b), Some(ScalarValue::Int64(Some(v)))) => b.append_value(*v),
            (BuilderKind::I64(b), Some(ScalarValue::Int64(None)) | None) => b.append_null(),
            (BuilderKind::I64(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "I64", other);
                b.append_null();
            }
            (BuilderKind::F16(b), Some(ScalarValue::Float16(Some(v)))) => b.append_value(*v),
            (BuilderKind::F16(b), Some(ScalarValue::Float16(None)) | None) => b.append_null(),
            (BuilderKind::F16(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "F16", other);
                b.append_null();
            }
            (BuilderKind::F32(b), Some(ScalarValue::Float32(Some(v)))) => b.append_value(*v),
            (BuilderKind::F32(b), Some(ScalarValue::Float32(None)) | None) => b.append_null(),
            (BuilderKind::F32(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "F32", other);
                b.append_null();
            }
            (BuilderKind::F64(b), Some(ScalarValue::Float64(Some(v)))) => b.append_value(*v),
            (BuilderKind::F64(b), Some(ScalarValue::Float64(None)) | None) => b.append_null(),
            (BuilderKind::F64(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "F64", other);
                b.append_null();
            }
            (BuilderKind::Utf8(b), Some(ScalarValue::Utf8(Some(v)))) => b.append_value(v),
            (BuilderKind::Utf8(b), Some(ScalarValue::Utf8(None)) | None) => b.append_null(),
            (BuilderKind::Utf8(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "Utf8", other);
                b.append_null();
            }
            (BuilderKind::I16(b), Some(ScalarValue::Int16(Some(v)))) => b.append_value(*v),
            (BuilderKind::I16(b), Some(ScalarValue::Int16(None)) | None) => b.append_null(),
            (BuilderKind::I16(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "I16", other);
                b.append_null();
            }
            (BuilderKind::I8(b), Some(ScalarValue::Int8(Some(v)))) => b.append_value(*v),
            (BuilderKind::I8(b), Some(ScalarValue::Int8(None)) | None) => b.append_null(),
            (BuilderKind::I8(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "I8", other);
                b.append_null();
            }
            (BuilderKind::U8(b), Some(ScalarValue::UInt8(Some(v)))) => b.append_value(*v),
            (BuilderKind::U8(b), Some(ScalarValue::UInt8(None)) | None) => b.append_null(),
            (BuilderKind::U8(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "U8", other);
                b.append_null();
            }
            (BuilderKind::U16(b), Some(ScalarValue::UInt16(Some(v)))) => b.append_value(*v),
            (BuilderKind::U16(b), Some(ScalarValue::UInt16(None)) | None) => b.append_null(),
            (BuilderKind::U16(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "U16", other);
                b.append_null();
            }
            (BuilderKind::U32(b), Some(ScalarValue::UInt32(Some(v)))) => b.append_value(*v),
            (BuilderKind::U32(b), Some(ScalarValue::UInt32(None)) | None) => b.append_null(),
            (BuilderKind::U32(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "U32", other);
                b.append_null();
            }
            (BuilderKind::Date32(b), Some(ScalarValue::Date32(Some(v)))) => b.append_value(*v),
            (BuilderKind::Date32(b), Some(ScalarValue::Date32(None)) | None) => b.append_null(),
            (BuilderKind::Date32(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "Date32", other);
                b.append_null();
            }
            (BuilderKind::Date64(b), Some(ScalarValue::Date64(Some(v)))) => b.append_value(*v),
            (BuilderKind::Date64(b), Some(ScalarValue::Date64(None)) | None) => b.append_null(),
            (BuilderKind::Date64(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "Date64", other);
                b.append_null();
            }
            (BuilderKind::TsMicro(b), Some(ScalarValue::TimestampMicrosecond(Some(v), _))) => {
                b.append_value(*v)
            }
            (BuilderKind::TsMicro(b), Some(ScalarValue::TimestampMicrosecond(None, _)) | None) => {
                b.append_null()
            }
            (BuilderKind::TsMicro(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "TsMicro", other);
                b.append_null();
            }
            (BuilderKind::Dec128(b), Some(ScalarValue::Decimal128(Some(v), _, _))) => {
                b.append_value(*v)
            }
            (BuilderKind::Dec128(b), Some(ScalarValue::Decimal128(None, _, _)) | None) => {
                b.append_null()
            }
            (BuilderKind::Dec128(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "Dec128", other);
                b.append_null();
            }
            (BuilderKind::Dec256(b), Some(ScalarValue::Decimal256(Some(v), _, _))) => {
                b.append_value(*v)
            }
            (BuilderKind::Dec256(b), Some(ScalarValue::Decimal256(None, _, _)) | None) => {
                b.append_null()
            }
            (BuilderKind::Dec256(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "Dec256", other);
                b.append_null();
            }
            (BuilderKind::U64(b), Some(ScalarValue::UInt64(Some(v)))) => b.append_value(*v),
            (BuilderKind::U64(b), Some(ScalarValue::UInt64(None)) | None) => b.append_null(),
            (BuilderKind::U64(b), Some(other)) => {
                Self::warn_mismatch(&self.column_name, seg_idx, "U64", other);
                b.append_null();
            }
        }
    }

    fn warn_mismatch(column_name: &str, seg_idx: usize, kind: &'static str, scalar: &ScalarValue) {
        tracing::warn!(
            target: "penca_dl::stats",
            column = column_name,
            seg_idx = seg_idx,
            builder_kind = kind,
            scalar_type = ?scalar.data_type(),
            "PerSegmentBuilders::push_scalar: scalar/builder type mismatch; appending null. \
             Likely cause: writer/reader column type drift (proto/DDL changed on one end only) \
             or a new TypedScalarV0 variant added without a BuilderKind arm."
        );
    }

    fn push_u64(&mut self, v: Option<u64>) {
        if let BuilderKind::U64(b) = &mut self.kind {
            match v {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    fn push_null(&mut self) {
        match &mut self.kind {
            BuilderKind::Bool(b) => b.append_null(),
            BuilderKind::I8(b) => b.append_null(),
            BuilderKind::I16(b) => b.append_null(),
            BuilderKind::I32(b) => b.append_null(),
            BuilderKind::I64(b) => b.append_null(),
            BuilderKind::U8(b) => b.append_null(),
            BuilderKind::U16(b) => b.append_null(),
            BuilderKind::U32(b) => b.append_null(),
            BuilderKind::F16(b) => b.append_null(),
            BuilderKind::F32(b) => b.append_null(),
            BuilderKind::F64(b) => b.append_null(),
            BuilderKind::Utf8(b) => b.append_null(),
            BuilderKind::Date32(b) => b.append_null(),
            BuilderKind::Date64(b) => b.append_null(),
            BuilderKind::TsMicro(b) => b.append_null(),
            BuilderKind::Dec128(b) => b.append_null(),
            BuilderKind::Dec256(b) => b.append_null(),
            BuilderKind::U64(b) => b.append_null(),
        }
    }

    fn finish(mut self) -> Option<ArrayRef> {
        match &mut self.kind {
            BuilderKind::Bool(b) => Some(Arc::new(b.finish())),
            BuilderKind::I8(b) => Some(Arc::new(b.finish())),
            BuilderKind::I16(b) => Some(Arc::new(b.finish())),
            BuilderKind::I32(b) => Some(Arc::new(b.finish())),
            BuilderKind::I64(b) => Some(Arc::new(b.finish())),
            BuilderKind::U8(b) => Some(Arc::new(b.finish())),
            BuilderKind::U16(b) => Some(Arc::new(b.finish())),
            BuilderKind::U32(b) => Some(Arc::new(b.finish())),
            BuilderKind::F16(b) => Some(Arc::new(b.finish())),
            BuilderKind::F32(b) => Some(Arc::new(b.finish())),
            BuilderKind::F64(b) => Some(Arc::new(b.finish())),
            BuilderKind::Utf8(b) => Some(Arc::new(b.finish())),
            BuilderKind::Date32(b) => Some(Arc::new(b.finish())),
            BuilderKind::Date64(b) => Some(Arc::new(b.finish())),
            BuilderKind::TsMicro(b) => Some(Arc::new(b.finish())),
            BuilderKind::Dec128(b) => Some(Arc::new(b.finish())),
            BuilderKind::Dec256(b) => Some(Arc::new(b.finish())),
            BuilderKind::U64(b) => Some(Arc::new(b.finish())),
        }
    }
}

impl PruningStatistics for SegmentPruningStats {
    fn min_values(&self, column: &datafusion::common::Column) -> Option<ArrayRef> {
        self.build_array(&column.name, |c, b, seg_idx| b.push_scalar(&c.min, seg_idx))
    }
    fn max_values(&self, column: &datafusion::common::Column) -> Option<ArrayRef> {
        self.build_array(&column.name, |c, b, seg_idx| b.push_scalar(&c.max, seg_idx))
    }
    fn null_counts(&self, column: &datafusion::common::Column) -> Option<ArrayRef> {
        // null_counts arrays are UInt64 per DataFusion's convention,
        // regardless of the source column's type.
        let idx = self.column_index(&column.name)?;
        let mut builders = PerSegmentBuilders::new_u64(&column.name);
        for seg in &self.parsed {
            let nc = seg.per_column.get(idx).and_then(|c| c.null_count);
            builders.push_u64(nc.map(|n| n as u64));
        }
        builders.finish()
    }
    fn row_counts(&self, _column: &datafusion::common::Column) -> Option<ArrayRef> {
        // row_counts are per-container (segment), not per-column.
        let mut builders = PerSegmentBuilders::new_u64("<row-count>");
        for seg in &self.parsed {
            builders.push_u64(Some(seg.row_count as u64));
        }
        builders.finish()
    }
    fn contained(
        &self,
        _column: &datafusion::common::Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
    fn num_containers(&self) -> usize {
        self.parsed.len()
    }
}

/// Prune snapshot segments by per-column statistics using a pre-built
/// physical predicate.
///
/// `predicate` is the residual filter's physical predicate. This pruning and
/// the residual filter come from the same full-planning path
/// (`penca_merge::full_plan_predicate`) over the same filter string, so a
/// segment pruned here is guaranteed to contain no row the residual would keep
/// — the two layers cannot disagree. `None` (no filter) keeps every segment.
/// Any failure to build or evaluate the [`PruningPredicate`] degrades to
/// keep-all: pruning is an optimization, and the residual filter still enforces
/// correctness on the rows that are read.
pub fn prune_segments_by_stats<S>(
    segments: &[S],
    get_stats: impl Fn(&S) -> &[u8],
    schema: &SchemaRef,
    predicate: Option<&Arc<dyn PhysicalExpr>>,
) -> Vec<usize> {
    let Some(predicate) = predicate else {
        return (0..segments.len()).collect();
    };
    let pruner = match PruningPredicate::try_new(predicate.clone(), schema.clone()) {
        Ok(p) => p,
        Err(_) => return (0..segments.len()).collect(),
    };
    let parsed: Vec<ParsedSegmentStats> = segments
        .iter()
        .map(|s| parse_segment_statistics(get_stats(s), schema))
        .collect();
    let stats = SegmentPruningStats {
        schema: schema.clone(),
        parsed,
    };
    match pruner.prune(&stats) {
        Ok(mask) => mask
            .into_iter()
            .enumerate()
            .filter_map(|(i, keep)| keep.then_some(i))
            .collect(),
        Err(_) => (0..segments.len()).collect(),
    }
}

pub fn aggregate_table_statistics(parsed: &[ParsedSegmentStats], schema: &SchemaRef) -> Statistics {
    if parsed.is_empty() {
        return Statistics::new_unknown(schema);
    }
    let total_rows: usize = parsed.iter().map(|s| s.row_count).sum();
    let column_statistics: Vec<ColumnStatistics> = (0..schema.fields().len())
        .map(|idx| fold_column_stats(parsed, idx))
        .collect();
    Statistics {
        num_rows: Precision::Inexact(total_rows),
        total_byte_size: Precision::Absent,
        column_statistics,
    }
}

/// Fold the per-segment stats at `col_idx` into a single `ColumnStatistics`.
/// `null_count` is `Absent` iff zero segments reported a value,
/// `Inexact(sum)` otherwise.
fn fold_column_stats(parsed: &[ParsedSegmentStats], col_idx: usize) -> ColumnStatistics {
    let mut min_acc: Option<ScalarValue> = None;
    let mut max_acc: Option<ScalarValue> = None;
    let mut null_acc: usize = 0;
    let mut have_null = false;
    for seg in parsed {
        if let Some(c) = seg.per_column.get(col_idx) {
            if let Some(m) = &c.min {
                min_acc = Some(match &min_acc {
                    None => m.clone(),
                    Some(cur) => {
                        if scalar_less(m, cur).unwrap_or(false) {
                            m.clone()
                        } else {
                            cur.clone()
                        }
                    }
                });
            }
            if let Some(m) = &c.max {
                max_acc = Some(match &max_acc {
                    None => m.clone(),
                    Some(cur) => {
                        if scalar_less(cur, m).unwrap_or(false) {
                            m.clone()
                        } else {
                            cur.clone()
                        }
                    }
                });
            }
            if let Some(n) = c.null_count {
                null_acc += n;
                have_null = true;
            }
        }
    }
    ColumnStatistics {
        null_count: if have_null {
            Precision::Inexact(null_acc)
        } else {
            Precision::Absent
        },
        max_value: max_acc.map(Precision::Inexact).unwrap_or(Precision::Absent),
        min_value: min_acc.map(Precision::Inexact).unwrap_or(Precision::Absent),
        sum_value: Precision::Absent,
        distinct_count: Precision::Absent,
        byte_size: Precision::Absent,
    }
}

fn scalar_less(a: &ScalarValue, b: &ScalarValue) -> Option<bool> {
    use ScalarValue::*;
    match (a, b) {
        (Boolean(Some(x)), Boolean(Some(y))) => Some(x < y),
        (Int8(Some(x)), Int8(Some(y))) => Some(x < y),
        (Int16(Some(x)), Int16(Some(y))) => Some(x < y),
        (Int32(Some(x)), Int32(Some(y))) => Some(x < y),
        (Int64(Some(x)), Int64(Some(y))) => Some(x < y),
        (UInt8(Some(x)), UInt8(Some(y))) => Some(x < y),
        (UInt16(Some(x)), UInt16(Some(y))) => Some(x < y),
        (UInt32(Some(x)), UInt32(Some(y))) => Some(x < y),
        (UInt64(Some(x)), UInt64(Some(y))) => Some(x < y),
        (Float16(Some(x)), Float16(Some(y))) => Some(x < y),
        (Float32(Some(x)), Float32(Some(y))) => Some(x < y),
        (Float64(Some(x)), Float64(Some(y))) => Some(x < y),
        (Utf8(Some(x)), Utf8(Some(y))) => Some(x < y),
        (Date32(Some(x)), Date32(Some(y))) => Some(x < y),
        (Date64(Some(x)), Date64(Some(y))) => Some(x < y),
        (TimestampMicrosecond(Some(x), _), TimestampMicrosecond(Some(y), _)) => Some(x < y),
        // Comparing raw unscaled values is only valid at equal scale. The
        // aggregate fold pairs min-with-min / max-with-max from one column, so
        // the guard holds by construction and never silently drops a compare.
        (Decimal128(Some(x), _, sx), Decimal128(Some(y), _, sy)) if sx == sy => Some(x < y),
        (Decimal256(Some(x), _, sx), Decimal256(Some(y), _, sy)) if sx == sy => Some(x < y),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Date32Array, Date64Array, Decimal128Array, Decimal256Array, Float16Array, Int8Array,
        Int16Array, Int32Array, StringArray, TimestampMicrosecondArray, UInt64Array,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};

    fn int32_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn make_batch_i32(names: Vec<&str>, values: Vec<i32>) -> RecordBatch {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(values)),
        ];
        RecordBatch::try_new(int32_schema(), cols).unwrap()
    }

    #[test]
    fn round_trip_int32_and_utf8() {
        let batch = make_batch_i32(vec!["a", "b", "c"], vec![10, 30, 20]);
        let bytes = compute_segment_statistics(&batch);
        let parsed = parse_segment_statistics(&bytes, &int32_schema());
        assert_eq!(parsed.row_count, 3);
        assert_eq!(
            parsed.per_column[0].min,
            Some(ScalarValue::Utf8(Some("a".into())))
        );
        assert_eq!(
            parsed.per_column[0].max,
            Some(ScalarValue::Utf8(Some("c".into())))
        );
        assert_eq!(parsed.per_column[1].min, Some(ScalarValue::Int32(Some(10))));
        assert_eq!(parsed.per_column[1].max, Some(ScalarValue::Int32(Some(30))));
        assert_eq!(parsed.per_column[0].null_count, Some(0));
        assert_eq!(parsed.per_column[1].null_count, Some(0));
    }

    #[test]
    fn empty_bytes_degrades_cleanly() {
        let parsed = parse_segment_statistics(&[], &int32_schema());
        assert_eq!(parsed.row_count, 0);
        assert_eq!(parsed.per_column.len(), 2);
        assert!(
            parsed
                .per_column
                .iter()
                .all(|c| c.min.is_none() && c.max.is_none())
        );
    }

    #[test]
    fn aggregate_folds_min_max_null_row_count() {
        let s0 = ParsedSegmentStats {
            row_count: 10,
            per_column: vec![
                ParsedColumnStats {
                    min: None,
                    max: None,
                    null_count: Some(0),
                },
                ParsedColumnStats {
                    min: Some(ScalarValue::Int32(Some(0))),
                    max: Some(ScalarValue::Int32(Some(50))),
                    null_count: Some(0),
                },
            ],
        };
        let s1 = ParsedSegmentStats {
            row_count: 20,
            per_column: vec![
                ParsedColumnStats {
                    min: None,
                    max: None,
                    null_count: Some(0),
                },
                ParsedColumnStats {
                    min: Some(ScalarValue::Int32(Some(40))),
                    max: Some(ScalarValue::Int32(Some(80))),
                    null_count: Some(0),
                },
            ],
        };
        let s2 = ParsedSegmentStats {
            row_count: 5,
            per_column: vec![
                ParsedColumnStats {
                    min: None,
                    max: None,
                    null_count: Some(0),
                },
                ParsedColumnStats {
                    min: Some(ScalarValue::Int32(Some(70))),
                    max: Some(ScalarValue::Int32(Some(120))),
                    null_count: Some(0),
                },
            ],
        };
        let stats = aggregate_table_statistics(&[s0, s1, s2], &int32_schema());
        assert_eq!(stats.num_rows, Precision::Inexact(35));
        assert_eq!(
            stats.column_statistics[1].min_value,
            Precision::Inexact(ScalarValue::Int32(Some(0)))
        );
        assert_eq!(
            stats.column_statistics[1].max_value,
            Precision::Inexact(ScalarValue::Int32(Some(120)))
        );
        assert_eq!(stats.column_statistics[1].null_count, Precision::Inexact(0));
    }

    #[test]
    fn aggregate_fold_orders_date32_across_segments() {
        // Guards `scalar_less` coverage for each prunable type: the global
        // min/max deliberately sit OUTSIDE the first segment, because a missing
        // `scalar_less` arm silently pins the fold to the first segment.
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
        let seg = |lo: i32, hi: i32| ParsedSegmentStats {
            row_count: 1,
            per_column: vec![ParsedColumnStats {
                min: Some(ScalarValue::Date32(Some(lo))),
                max: Some(ScalarValue::Date32(Some(hi))),
                null_count: Some(0),
            }],
        };
        let stats =
            aggregate_table_statistics(&[seg(100, 150), seg(10, 90), seg(200, 260)], &schema);
        assert_eq!(
            stats.column_statistics[0].min_value,
            Precision::Inexact(ScalarValue::Date32(Some(10)))
        );
        assert_eq!(
            stats.column_statistics[0].max_value,
            Precision::Inexact(ScalarValue::Date32(Some(260)))
        );
    }

    #[test]
    fn decimal128_and_timestamp_min_max_round_trip() {
        let dec: ArrayRef = Arc::new(
            Decimal128Array::from(vec![12345i128, 100, 99999])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let (min, max) = column_min_max(&dec, &DataType::Decimal128(10, 2));
        assert_eq!(min, Some(ScalarValue::Decimal128(Some(100), 10, 2)));
        assert_eq!(max, Some(ScalarValue::Decimal128(Some(99999), 10, 2)));

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![300i64, 100, 200]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ts]).unwrap();
        let parsed = parse_segment_statistics(&compute_segment_statistics(&batch), &schema);
        assert_eq!(
            parsed.per_column[0].min,
            Some(ScalarValue::TimestampMicrosecond(Some(100), None))
        );
        assert_eq!(
            parsed.per_column[0].max,
            Some(ScalarValue::TimestampMicrosecond(Some(300), None))
        );
    }

    #[test]
    fn unprunable_column_short_circuits_min_max() {
        // Without `build_array`'s `data_type_is_prunable` short-circuit, the
        // Binary column would reach `PerSegmentBuilders::new`'s `panic!`
        // unprunable arm — so no panic here is itself the assertion.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Int32, false),
            Field::new("when", DataType::Binary, false),
        ]));
        let stats = SegmentPruningStats {
            schema: schema.clone(),
            parsed: vec![ParsedSegmentStats {
                row_count: 1,
                per_column: vec![
                    ParsedColumnStats {
                        min: Some(ScalarValue::Int32(Some(10))),
                        max: Some(ScalarValue::Int32(Some(20))),
                        null_count: Some(0),
                    },
                    // Binary is unprunable, so the writer records None min/max.
                    ParsedColumnStats {
                        min: None,
                        max: None,
                        null_count: Some(0),
                    },
                ],
            }],
        };

        let amount_col = datafusion::common::Column::new_unqualified("amount");
        let amount_min = stats.min_values(&amount_col);
        assert!(
            amount_min.is_some(),
            "prunable Int32 column should yield a min_values array"
        );
        assert_eq!(amount_min.unwrap().len(), 1);

        let when_col = datafusion::common::Column::new_unqualified("when");
        assert!(
            stats.min_values(&when_col).is_none(),
            "unprunable Binary column should short-circuit to None"
        );
        assert!(
            stats.max_values(&when_col).is_none(),
            "unprunable Binary column should short-circuit to None for max_values too"
        );
        // null_counts is intentionally NOT short-circuited: the writer records
        // null_count for every column regardless of prunability.
        assert!(
            stats.null_counts(&when_col).is_some(),
            "null_counts should be available for any column, prunable or not"
        );
    }

    #[test]
    fn ordered_phase1_types_are_prunable() {
        // The Phase-1 gap set (SQL-declarable today). Time32/Time64 are
        // Phase-2 (no SQL TIME yet) and gain prunability with the temporal
        // widen task.
        for dt in [
            DataType::Int16,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Decimal128(10, 2),
        ] {
            assert!(
                data_type_is_prunable(&dt),
                "{dt:?} is an ordered scalar declarable today — must be prunable"
            );
        }
    }

    #[test]
    fn column_min_max_yields_bounds_for_int16_and_date32() {
        let i16_arr: ArrayRef = Arc::new(Int16Array::from(vec![5i16, 1, 9]));
        let (min, max) = column_min_max(&i16_arr, &DataType::Int16);
        assert_eq!(min, Some(ScalarValue::Int16(Some(1))));
        assert_eq!(max, Some(ScalarValue::Int16(Some(9))));

        let d32_arr: ArrayRef = Arc::new(Date32Array::from(vec![100i32, 50, 200]));
        let (dmin, dmax) = column_min_max(&d32_arr, &DataType::Date32);
        assert_eq!(dmin, Some(ScalarValue::Date32(Some(50))));
        assert_eq!(dmax, Some(ScalarValue::Date32(Some(200))));
    }

    #[test]
    fn unsigned_and_int8_prunable_with_min_max_round_trip() {
        // The u64::MAX case matters: it exceeds i64, so a signed intermediate
        // in the serde round-trip would corrupt it.
        for dt in [
            DataType::Int8,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            assert!(data_type_is_prunable(&dt), "{dt:?} must be prunable");
        }
        let i8_arr: ArrayRef = Arc::new(Int8Array::from(vec![5i8, -1, 9]));
        let (min, max) = column_min_max(&i8_arr, &DataType::Int8);
        assert_eq!(min, Some(ScalarValue::Int8(Some(-1))));
        assert_eq!(max, Some(ScalarValue::Int8(Some(9))));

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("u", DataType::UInt64, false)]));
        let u64_arr: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX, 0, 42]));
        let batch = RecordBatch::try_new(schema.clone(), vec![u64_arr]).unwrap();
        let parsed = parse_segment_statistics(&compute_segment_statistics(&batch), &schema);
        assert_eq!(parsed.per_column[0].min, Some(ScalarValue::UInt64(Some(0))));
        assert_eq!(
            parsed.per_column[0].max,
            Some(ScalarValue::UInt64(Some(u64::MAX)))
        );
    }

    #[test]
    fn float16_and_decimal256_prunable_with_round_trip() {
        // Both round-trip through indirect serde forms — f16 bits, i256
        // little-endian bytes — so min/max are the real assertion here.
        for dt in [DataType::Float16, DataType::Decimal256(40, 10)] {
            assert!(data_type_is_prunable(&dt), "{dt:?} must be prunable");
        }
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("f", DataType::Float16, false)]));
        let f16_arr: ArrayRef = Arc::new(Float16Array::from(vec![
            f16::from_f32(1.5),
            f16::from_f32(-2.0),
            f16::from_f32(3.25),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![f16_arr]).unwrap();
        let parsed = parse_segment_statistics(&compute_segment_statistics(&batch), &schema);
        assert_eq!(
            parsed.per_column[0].min,
            Some(ScalarValue::Float16(Some(f16::from_f32(-2.0))))
        );
        assert_eq!(
            parsed.per_column[0].max,
            Some(ScalarValue::Float16(Some(f16::from_f32(3.25))))
        );

        let dec: ArrayRef = Arc::new(
            Decimal256Array::from(vec![
                i256::from_i128(100),
                i256::from_i128(99999),
                i256::from_i128(500),
            ])
            .with_precision_and_scale(40, 10)
            .unwrap(),
        );
        let (min, max) = column_min_max(&dec, &DataType::Decimal256(40, 10));
        assert_eq!(
            min,
            Some(ScalarValue::Decimal256(Some(i256::from_i128(100)), 40, 10))
        );
        assert_eq!(
            max,
            Some(ScalarValue::Decimal256(
                Some(i256::from_i128(99999)),
                40,
                10
            ))
        );
    }

    #[test]
    fn date64_prunable_temporal_pruning_deferred() {
        // Time32/Time64 and tz-aware Timestamp round-trip through the codec
        // but ship null stats (keep-all) — pruning is TODO(CHA-390).
        assert!(data_type_is_prunable(&DataType::Date64));
        for dt in [
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ] {
            assert!(
                !data_type_is_prunable(&dt),
                "{dt:?} pruning is deferred to CHA-390"
            );
        }
        let d64: ArrayRef = Arc::new(Date64Array::from(vec![
            172_800_000i64,
            86_400_000,
            259_200_000,
        ]));
        let (min, max) = column_min_max(&d64, &DataType::Date64);
        assert_eq!(min, Some(ScalarValue::Date64(Some(86_400_000))));
        assert_eq!(max, Some(ScalarValue::Date64(Some(259_200_000))));
    }

    #[test]
    fn date32_segment_stats_round_trip() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("d", DataType::Date32, false),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Date32Array::from(vec![100i32, 50, 200])),
        ];
        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
        let bytes = compute_segment_statistics(&batch);
        let parsed = parse_segment_statistics(&bytes, &schema);
        assert_eq!(
            parsed.per_column[1].min,
            Some(ScalarValue::Date32(Some(50)))
        );
        assert_eq!(
            parsed.per_column[1].max,
            Some(ScalarValue::Date32(Some(200)))
        );
    }
}
