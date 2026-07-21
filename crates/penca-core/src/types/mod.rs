//! Canonical Arrow type registry (CHA-386).
//!
//! The single source of truth for "which column types does Penca
//! support, and how does each subsystem treat them". Before this module
//! the answer was re-enumerated independently in every subsystem
//! (`sql_type_to_arrow`, `arrow_type_to_sql`, the hot row codec, segment
//! stats, the lifecycle chunker), and the subsets had drifted out of
//! sync — a column could be declarable yet un-round-trippable.
//!
//! [`CanonicalType`] is an identity-preserving, curated subset of Arrow's
//! [`DataType`]: [`CanonicalType::from_arrow`] is the one gate that
//! decides support, and every consumer dispatches by matching on
//! `CanonicalType` **with no `_` catch-all arm**. That is the
//! cross-crate exhaustiveness guarantee — adding a variant here is a
//! compile error in every consumer that has not handled it yet. Keep it
//! that way: a wildcard arm in a consumer silently defeats the whole
//! point.
//!
//! Phasing (CHA-386): the enum covers the **full** Arrow⇄Postgres target
//! set from the start, because the widest subsystem (`PgDialect`'s
//! `arrow_type_to_sql`) and the lifecycle chunker already handled most of
//! it — narrowing them to centralize would only churn capability. The
//! incremental work is in the consumers that did *not* yet handle the
//! wide set: the hot row codec and segment stats fill their per-variant
//! arms family by family (each gated by a red round-trip / prunability
//! test), starting from an explicit "not yet supported" arm that
//! preserves today's behavior. The compiler still forces every consumer
//! to name every variant.
//!
//! Identity round-trip note: `to_arrow(from_arrow(dt)) == dt` holds for
//! the scalar set, so a declared type survives storage. For container
//! types the *stored* IPC schema remains authoritative — consumers
//! dispatch on `CanonicalType` but build against the original
//! [`Field`](arrow::datatypes::Field), so a list's child-field metadata
//! is preserved by the stored schema, not reconstructed from
//! [`to_arrow`](CanonicalType::to_arrow).
//!
//! Dialect neutrality: this module owns the *set* and the dialect-neutral
//! classification (codec / stats / chunker). The Arrow → Postgres column
//! DDL strings stay in `penca-db`'s `PgDialect`, driven by this enum so
//! the PG mapping is total over the canonical set by construction.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};

/// An Arrow type Penca supports as a column type. Curated, identity
/// preserving subset of [`DataType`] — see the module docs for the
/// no-catch-all dispatch contract and the phasing of consumer arms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Boolean,
    Utf8,
    LargeUtf8,
    Utf8View,
    Binary,
    LargeBinary,
    BinaryView,
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Timestamp {
        unit: TimeUnit,
        tz: Option<Arc<str>>,
    },
    /// Single-level list of a scalar — the child is guaranteed
    /// [`is_scalar`](CanonicalType::is_scalar) (nested containers are
    /// rejected by [`from_arrow`](CanonicalType::from_arrow)).
    List(Box<CanonicalType>),
    LargeList(Box<CanonicalType>),
    FixedSizeList(Box<CanonicalType>, i32),
}

/// An Arrow [`DataType`] outside the canonical supported set. Returned by
/// [`CanonicalType::from_arrow`]; each crate maps it to its own error
/// type (`Status`, `ArrowTypeError`, `HotStorageError`, `ApiError`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unsupported column type: {0}")]
pub struct UnsupportedType(pub DataType);

impl CanonicalType {
    /// The one supported-set gate: classify an Arrow [`DataType`], or
    /// return [`UnsupportedType`]. A list's child must itself be
    /// supported **and** scalar — nested-of-nested is rejected loudly so
    /// the codec recursion stays single-level (`Struct`/`Map` and
    /// list-of-list stay out of scope).
    pub fn from_arrow(dt: &DataType) -> Result<Self, UnsupportedType> {
        let ct = match dt {
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Boolean => Self::Boolean,
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Utf8View => Self::Utf8View,
            DataType::Binary => Self::Binary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::BinaryView => Self::BinaryView,
            // The gate is authoritative on *precision*: reject
            // out-of-Arrow-range precision here rather than letting it pass
            // as "supported" and fail later at array materialization. Arrow
            // caps Decimal128 at precision 38 and Decimal256 at 76.
            // Out-of-range (and precision 0) falls through to the
            // `UnsupportedType` arm. Scale-range validation is intentionally
            // left to Arrow array construction — Arrow allows negative
            // scales, so the precision-relative bound is not a simple range.
            DataType::Decimal128(precision, scale) if (1..=38).contains(precision) => {
                Self::Decimal128 {
                    precision: *precision,
                    scale: *scale,
                }
            }
            DataType::Decimal256(precision, scale) if (1..=76).contains(precision) => {
                Self::Decimal256 {
                    precision: *precision,
                    scale: *scale,
                }
            }
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Time32(unit) => Self::Time32(*unit),
            DataType::Time64(unit) => Self::Time64(*unit),
            DataType::Timestamp(unit, tz) => Self::Timestamp {
                unit: *unit,
                tz: tz.clone(),
            },
            DataType::List(field) => Self::List(Box::new(Self::scalar_child(field, dt)?)),
            DataType::LargeList(field) => Self::LargeList(Box::new(Self::scalar_child(field, dt)?)),
            DataType::FixedSizeList(field, len) => {
                Self::FixedSizeList(Box::new(Self::scalar_child(field, dt)?), *len)
            }
            // Out of scope (fall through to UnsupportedType): Struct, Map,
            // Union, Dictionary, nested-of-nested lists, and the
            // `ListView`/`LargeListView` view-list encodings. The target
            // set is List/LargeList/FixedSizeList only (CHA-386); the
            // dialect's prior ad-hoc `ListView` arm is dropped with it.
            other => return Err(UnsupportedType(other.clone())),
        };
        Ok(ct)
    }

    /// Classify a list child field, requiring it to be supported **and**
    /// scalar. `parent` is the originating list type, used so the error
    /// names the column type the user actually declared.
    fn scalar_child(field: &Field, parent: &DataType) -> Result<Self, UnsupportedType> {
        let child =
            Self::from_arrow(field.data_type()).map_err(|_| UnsupportedType(parent.clone()))?;
        if !child.is_scalar() {
            return Err(UnsupportedType(parent.clone()));
        }
        Ok(child)
    }

    /// The canonical Arrow [`DataType`] for this variant. Inverse of
    /// [`from_arrow`](CanonicalType::from_arrow) on the scalar set; list
    /// children use the Arrow-conventional `"item"` field name (the
    /// stored schema stays authoritative for container field metadata —
    /// see the module docs).
    pub fn to_arrow(&self) -> DataType {
        match self {
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Float16 => DataType::Float16,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Utf8 => DataType::Utf8,
            Self::LargeUtf8 => DataType::LargeUtf8,
            Self::Utf8View => DataType::Utf8View,
            Self::Binary => DataType::Binary,
            Self::LargeBinary => DataType::LargeBinary,
            Self::BinaryView => DataType::BinaryView,
            Self::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
            Self::Decimal256 { precision, scale } => DataType::Decimal256(*precision, *scale),
            Self::Date32 => DataType::Date32,
            Self::Date64 => DataType::Date64,
            Self::Time32(unit) => DataType::Time32(*unit),
            Self::Time64(unit) => DataType::Time64(*unit),
            Self::Timestamp { unit, tz } => DataType::Timestamp(*unit, tz.clone()),
            Self::List(child) => DataType::List(Self::item_field(child)),
            Self::LargeList(child) => DataType::LargeList(Self::item_field(child)),
            Self::FixedSizeList(child, len) => {
                DataType::FixedSizeList(Self::item_field(child), *len)
            }
        }
    }

    fn item_field(child: &CanonicalType) -> Arc<Field> {
        Arc::new(Field::new("item", child.to_arrow(), true))
    }

    /// Whether this is a scalar (non-container) type. A `List` child is
    /// required to be scalar, which bounds the codec/stats recursion to
    /// one level.
    pub fn is_scalar(&self) -> bool {
        !matches!(
            self,
            Self::List(_) | Self::LargeList(_) | Self::FixedSizeList(_, _)
        )
    }

    /// Whether segment min/max pruning applies. Ordered scalars are
    /// prunable; the Large/View string variants, all binary, and the
    /// list containers are not (they ship null stats → DataFusion keeps
    /// all segments, the correct degrade for a type we do not order).
    pub fn is_prunable(&self) -> bool {
        match self {
            Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::UInt8
            | Self::UInt16
            | Self::UInt32
            | Self::UInt64
            | Self::Float16
            | Self::Float32
            | Self::Float64
            | Self::Boolean
            | Self::Utf8
            | Self::Decimal128 { .. }
            | Self::Decimal256 { .. }
            | Self::Date32
            | Self::Date64
            | Self::Time32(_)
            | Self::Time64(_)
            | Self::Timestamp { .. } => true,
            // LargeUtf8 / Utf8View are byte-ordered like Utf8 and *could*
            // prune; pruning stays off until the stats consumer grows
            // min/max arms for them (CHA-386 widen). Binary and the list
            // containers are genuinely unordered → null stats / keep-all.
            Self::LargeUtf8
            | Self::Utf8View
            | Self::Binary
            | Self::LargeBinary
            | Self::BinaryView
            | Self::List(_)
            | Self::LargeList(_)
            | Self::FixedSizeList(_, _) => false,
        }
    }

    /// Per-row standalone in-memory byte footprint for a fixed-width
    /// type, or `None` for variable-width types (the lifecycle chunker
    /// walks those per row). The `+1` is the per-row share of the
    /// validity bitmap, matching the chunker's existing arithmetic.
    pub fn fixed_width_bytes(&self) -> Option<i64> {
        let payload = match self {
            Self::Boolean | Self::Int8 | Self::UInt8 => 1,
            Self::Int16 | Self::UInt16 | Self::Float16 => 2,
            Self::Int32 | Self::UInt32 | Self::Float32 | Self::Date32 | Self::Time32(_) => 4,
            Self::Int64
            | Self::UInt64
            | Self::Float64
            | Self::Date64
            | Self::Time64(_)
            | Self::Timestamp { .. } => 8,
            Self::Decimal128 { .. } => 16,
            Self::Decimal256 { .. } => 32,
            Self::Utf8
            | Self::LargeUtf8
            | Self::Utf8View
            | Self::Binary
            | Self::LargeBinary
            | Self::BinaryView
            | Self::List(_)
            | Self::LargeList(_)
            | Self::FixedSizeList(_, _) => return None,
        };
        Some(payload + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every scalar in the target set, for exhaustive round-trip /
    /// classification assertions.
    fn all_scalars() -> Vec<DataType> {
        vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Boolean,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::Decimal128(20, 4),
            DataType::Decimal256(40, 10),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ]
    }

    #[test]
    fn scalar_round_trip_is_identity() {
        for dt in all_scalars() {
            let ct = CanonicalType::from_arrow(&dt)
                .unwrap_or_else(|e| panic!("{dt:?} should be supported: {e}"));
            assert_eq!(ct.to_arrow(), dt, "to_arrow∘from_arrow must be identity");
            assert!(ct.is_scalar(), "{dt:?} should classify as scalar");
        }
    }

    #[test]
    fn list_variants_round_trip() {
        let item = || Arc::new(Field::new("item", DataType::Int32, true));
        for dt in [
            DataType::List(item()),
            DataType::LargeList(item()),
            DataType::FixedSizeList(item(), 3),
        ] {
            let ct = CanonicalType::from_arrow(&dt).unwrap_or_else(|e| panic!("{dt:?}: {e}"));
            assert_eq!(ct.to_arrow(), dt);
            assert!(!ct.is_scalar());
            assert!(!ct.is_prunable());
            assert_eq!(ct.fixed_width_bytes(), None);
        }
    }

    #[test]
    fn nested_and_unsupported_containers_are_rejected() {
        let inner = Field::new("item", DataType::Utf8, true);
        let nested = DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(inner)),
            true,
        )));
        assert!(
            CanonicalType::from_arrow(&nested).is_err(),
            "nested list must be rejected (single-level only)"
        );
        let st = DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into());
        assert!(
            CanonicalType::from_arrow(&st).is_err(),
            "Struct out of scope"
        );
    }

    #[test]
    fn out_of_range_decimal_precision_is_rejected() {
        // The gate owns the supported set: out-of-Arrow-range precision is
        // rejected at the boundary, not deferred to array materialization.
        assert!(CanonicalType::from_arrow(&DataType::Decimal128(39, 2)).is_err());
        assert!(CanonicalType::from_arrow(&DataType::Decimal128(0, 0)).is_err());
        assert!(CanonicalType::from_arrow(&DataType::Decimal256(77, 2)).is_err());
        // In-range still accepted.
        assert!(CanonicalType::from_arrow(&DataType::Decimal128(38, 2)).is_ok());
        assert!(CanonicalType::from_arrow(&DataType::Decimal256(76, 2)).is_ok());
    }

    #[test]
    fn prunable_set_excludes_large_view_binary_and_lists() {
        let non_prunable = [
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
        ];
        for dt in non_prunable {
            assert!(
                !CanonicalType::from_arrow(&dt).unwrap().is_prunable(),
                "{dt:?} must be non-prunable"
            );
        }
        // Ordered scalars are prunable.
        for dt in [
            DataType::Int8,
            DataType::Date64,
            DataType::Decimal256(40, 10),
        ] {
            assert!(
                CanonicalType::from_arrow(&dt).unwrap().is_prunable(),
                "{dt:?}"
            );
        }
    }

    #[test]
    fn fixed_width_matches_validity_padded_arithmetic() {
        let cases = [
            (DataType::Boolean, Some(2)),
            (DataType::Int8, Some(2)),
            (DataType::Int16, Some(3)),
            (DataType::Float16, Some(3)),
            (DataType::Int32, Some(5)),
            (DataType::Date32, Some(5)),
            (DataType::Time32(TimeUnit::Millisecond), Some(5)),
            (DataType::Int64, Some(9)),
            (DataType::Date64, Some(9)),
            (DataType::Timestamp(TimeUnit::Microsecond, None), Some(9)),
            (DataType::Decimal128(20, 4), Some(17)),
            (DataType::Decimal256(40, 10), Some(33)),
            (DataType::Utf8, None),
            (DataType::LargeUtf8, None),
            (DataType::Binary, None),
        ];
        for (dt, want) in cases {
            let ct = CanonicalType::from_arrow(&dt).unwrap();
            assert_eq!(ct.fixed_width_bytes(), want, "{dt:?}");
        }
    }
}
