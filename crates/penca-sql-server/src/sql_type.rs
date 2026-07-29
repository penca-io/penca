//! SQL parsed-type → Arrow data-type translator for the auto-commit DDL
//! path.
//!
//! Called from `crate::ddl::execute_create_table` once per `ColumnDef`.
//! This is a pure *translation* layer: it maps each SQL type token onto a
//! [`CanonicalType`] (rejecting tokens with no Arrow mapping with a
//! SQL-aware error rather than DataFusion's internal "no coercion
//! possible" wording), then materializes the canonical Arrow type. It is
//! NOT the supported-set gate — that lives once at the gRPC
//! `WriteService::create_table` boundary, so the two layers
//! reject at different points (token-translation vs. schema-gate) by
//! design.

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::sql::sqlparser::ast::{DataType as SqlDataType, ExactNumberInfo, TimezoneInfo};
use penca_core::types::CanonicalType;
use tonic::Status;

/// Translate a parsed SQL type into the Arrow data type Penca stores
/// for that column, via the canonical type registry. Returns
/// `Status::invalid_argument` for SQL tokens with no Arrow mapping; the
/// message names the offending type and points at the
/// `penca-core::types` registry for the canonical supported set.
pub(crate) fn sql_type_to_arrow(sql: &SqlDataType) -> Result<DataType, Status> {
    let canonical = match sql {
        SqlDataType::Int(_) | SqlDataType::Integer(_) => CanonicalType::Int32,
        SqlDataType::BigInt(_) => CanonicalType::Int64,
        SqlDataType::SmallInt(_) => CanonicalType::Int16,
        SqlDataType::Boolean | SqlDataType::Bool => CanonicalType::Boolean,
        SqlDataType::Varchar(_) | SqlDataType::Text | SqlDataType::String(_) => CanonicalType::Utf8,
        SqlDataType::Timestamp(_, TimezoneInfo::None) => CanonicalType::Timestamp {
            unit: TimeUnit::Microsecond,
            tz: None,
        },
        // TIMESTAMP WITH TIME ZONE -> tz-aware microsecond Timestamp. PG
        // stores TIMESTAMPTZ as a UTC instant, so the declared tz is
        // normalized to UTC; read-back restores the tz from the stored
        // schema.
        SqlDataType::Timestamp(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
            CanonicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                tz: Some("UTC".into()),
            }
        }
        SqlDataType::Decimal(ExactNumberInfo::PrecisionAndScale(p, s)) => {
            CanonicalType::Decimal128 {
                precision: *p as u8,
                scale: *s as i8,
            }
        }
        SqlDataType::Float(_) | SqlDataType::Real => CanonicalType::Float32,
        SqlDataType::Double(_) | SqlDataType::DoublePrecision => CanonicalType::Float64,
        SqlDataType::Date => CanonicalType::Date32,
        other => {
            return Err(Status::invalid_argument(format!(
                "unsupported SQL type `{other}` — see the penca-core::types registry \
                 for the canonical supported set (INT/BIGINT/SMALLINT, BOOLEAN, \
                 VARCHAR/TEXT, TIMESTAMP, DECIMAL(p,s), FLOAT/DOUBLE, DATE) (CHA-386)"
            )));
        }
    };
    Ok(canonical.to_arrow())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse_type(sql: &str) -> SqlDataType {
        let dialect = GenericDialect {};
        Parser::new(&dialect)
            .try_with_sql(sql)
            .unwrap()
            .parse_data_type()
            .unwrap_or_else(|e| panic!("parse `{sql}` failed: {e}"))
    }

    fn assert_maps(sql: &str, want: DataType) {
        let got = sql_type_to_arrow(&parse_type(sql))
            .unwrap_or_else(|e| panic!("expected {sql} → {want:?}, got error: {e}"));
        assert_eq!(got, want, "{sql}");
    }

    fn assert_rejects(sql: &str) {
        let parsed = parse_type(sql);
        let err =
            sql_type_to_arrow(&parsed).expect_err(&format!("expected `{sql}` to reject; got Ok"));
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{sql}");
        assert!(
            err.message().contains("core::types"),
            "{sql} rejection must cite the registry; got: {}",
            err.message()
        );
    }

    #[test]
    fn int_variants_map_to_int32() {
        assert_maps("INT", DataType::Int32);
        assert_maps("INTEGER", DataType::Int32);
    }

    #[test]
    fn bigint_maps_to_int64() {
        assert_maps("BIGINT", DataType::Int64);
    }

    #[test]
    fn smallint_maps_to_int16() {
        assert_maps("SMALLINT", DataType::Int16);
    }

    #[test]
    fn boolean_variants_map_to_boolean() {
        assert_maps("BOOLEAN", DataType::Boolean);
        assert_maps("BOOL", DataType::Boolean);
    }

    #[test]
    fn varchar_text_string_all_map_to_utf8() {
        assert_maps("VARCHAR(64)", DataType::Utf8);
        assert_maps("TEXT", DataType::Utf8);
        assert_maps("STRING", DataType::Utf8);
    }

    #[test]
    fn timestamp_without_tz_maps_to_microsecond() {
        assert_maps(
            "TIMESTAMP",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
    }

    #[test]
    fn decimal_with_precision_and_scale_maps_to_decimal128() {
        assert_maps("DECIMAL(10, 2)", DataType::Decimal128(10, 2));
        assert_maps("DECIMAL(38, 9)", DataType::Decimal128(38, 9));
    }

    #[test]
    fn float_and_real_map_to_float32() {
        assert_maps("FLOAT", DataType::Float32);
        assert_maps("REAL", DataType::Float32);
    }

    #[test]
    fn double_variants_map_to_float64() {
        assert_maps("DOUBLE", DataType::Float64);
        assert_maps("DOUBLE PRECISION", DataType::Float64);
    }

    #[test]
    fn date_maps_to_date32() {
        assert_maps("DATE", DataType::Date32);
    }

    #[test]
    fn nested_array_rejects() {
        // sqlparser's GenericDialect parses `INT ARRAY` as bare `Int(None)`
        // (dropping the ARRAY suffix), so use the explicit postgres-array
        // bracket form which reliably produces `DataType::Array(_)`.
        assert_rejects("INT[]");
    }

    #[test]
    fn uuid_custom_type_rejects() {
        // `UUID` is parsed as `Custom("uuid", _)` in GenericDialect — a
        // representative unsupported variant that proves the catch-all
        // fires for types Penca doesn't have a wire mapping for.
        assert_rejects("UUID");
    }

    #[test]
    fn timestamp_with_timezone_maps_to_utc_microsecond() {
        // Tz-aware TIMESTAMP maps to a UTC-normalized microsecond Timestamp:
        // PG TIMESTAMPTZ stores a UTC instant, and read-back restores the tz
        // from the stored schema.
        assert_maps(
            "TIMESTAMP WITH TIME ZONE",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        );
    }

    #[test]
    fn decimal_without_explicit_precision_rejects() {
        // Bare `DECIMAL` (no precision/scale) falls through —
        // `ExactNumberInfo::None` and `PrecisionOnly(_)` carry
        // different semantics than `PrecisionAndScale` and the wire
        // type needs both.
        let parsed = parse_type("DECIMAL");
        let err = sql_type_to_arrow(&parsed).expect_err("bare DECIMAL must reject");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("core::types"), "{}", err.message());
    }
}
