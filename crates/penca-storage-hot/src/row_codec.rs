//! Arrow ↔ Postgres row codec helpers shared across the hot-tier modules.
//!
//! Both directions dispatch through [`penca_core::types::CanonicalType`]
//! (CHA-386): `from_arrow` is the supported-set gate (a miss here is an
//! internal-invariant violation — the column type was validated at
//! `WriteService::create_table` before any row reached storage), and the
//! per-variant match is exhaustive with **no `_` arm**, so a new canonical
//! type is a compile error until both directions handle it. Types whose
//! codec arms have not landed yet return `UnsupportedType` explicitly; the
//! Phase-2 widen tasks replace those arms with real encode/decode.

use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit, i256};
use arrow::record_batch::RecordBatch;
use half::f16;
use penca_core::types::CanonicalType;
use penca_db::dialect::DbDialect;
use penca_db::dialect::pg::PgDialect;
use sqlx::postgres::PgRow;
use sqlx::types::BigDecimal;
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sqlx::{Column, Row, TypeInfo};

use crate::HotStorageError;

fn unix_epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date")
}

/// Create a zero-row RecordBatch preserving the schema.
pub(crate) fn empty_batch(schema: &SchemaRef) -> RecordBatch {
    RecordBatch::new_empty(schema.clone())
}

/// Convert sqlx `PgRow`s to an Arrow `RecordBatch`.
///
/// Dispatches on the canonical type of each field. For `Utf8` fields,
/// checks the underlying Postgres column type to correctly handle UUID
/// columns (returned as `uuid::Uuid` by sqlx) vs TEXT columns.
pub(crate) fn rows_to_batch(
    rows: &[PgRow],
    schema: &SchemaRef,
) -> Result<RecordBatch, HotStorageError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        let dt = field.data_type().clone();
        let ct =
            CanonicalType::from_arrow(&dt).map_err(|e| HotStorageError::UnsupportedType(e.0))?;
        let array: ArrayRef = match ct {
            CanonicalType::Utf8 => {
                let is_uuid = rows[0].column(i).type_info().name() == "UUID";
                let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 36);
                for row in rows {
                    if is_uuid {
                        match row.try_get::<Option<uuid::Uuid>, _>(i)? {
                            Some(u) => builder.append_value(u.to_string()),
                            None => builder.append_null(),
                        }
                    } else {
                        match row.try_get::<Option<String>, _>(i)? {
                            Some(s) => builder.append_value(s),
                            None => builder.append_null(),
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Int64 => {
                let mut builder = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<i64>, _>(i)? {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::UInt64 => {
                // PG NUMERIC (arbitrary precision) holds the full u64
                // range; BIGINT would truncate values above i64::MAX.
                let mut builder = UInt64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<BigDecimal>, _>(i)? {
                        Some(bd) => builder.append_value(bigdecimal_to_u64(&bd)?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Int8 => {
                // PG SMALLINT (i16) narrowed back to the declared i8.
                let mut builder = Int8Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<i16>, _>(i)? {
                        Some(v) => builder
                            .append_value(i8::try_from(v).map_err(|_| narrow_err(v, "Int8"))?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::UInt8 => {
                // PG SMALLINT (i16) narrowed back to u8.
                let mut builder = UInt8Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<i16>, _>(i)? {
                        Some(v) => builder
                            .append_value(u8::try_from(v).map_err(|_| narrow_err(v, "UInt8"))?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::UInt16 => {
                // PG INTEGER (i32) narrowed back to u16.
                let mut builder = UInt16Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<i32>, _>(i)? {
                        Some(v) => builder
                            .append_value(u16::try_from(v).map_err(|_| narrow_err(v, "UInt16"))?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::UInt32 => {
                // PG BIGINT (i64) narrowed back to u32.
                let mut builder = UInt32Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<i64>, _>(i)? {
                        Some(v) => builder
                            .append_value(u32::try_from(v).map_err(|_| narrow_err(v, "UInt32"))?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Int32 => {
                let mut builder = Int32Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<i32>, _>(i)? {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Int16 => {
                // PG SMALLINT round-trips as i16 via sqlx.
                let mut builder = Int16Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<i16>, _>(i)? {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<bool>, _>(i)? {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Float64 => {
                let mut builder = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<f64>, _>(i)? {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Float32 => {
                let mut builder = Float32Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<f32>, _>(i)? {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Float16 => {
                // PG REAL is f32; narrow to the declared half-precision f16.
                let mut builder = Float16Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<f32>, _>(i)? {
                        Some(v) => builder.append_value(f16::from_f32(v)),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Date32 => {
                // PG DATE → days since the Unix epoch (Arrow Date32).
                let epoch = unix_epoch_date();
                let mut builder = Date32Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<NaiveDate>, _>(i)? {
                        Some(d) => builder.append_value((d - epoch).num_days() as i32),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Date64 => {
                // PG DATE → Arrow Date64 (milliseconds since the Unix epoch
                // at midnight UTC).
                let epoch = unix_epoch_date();
                let mut builder = Date64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.try_get::<Option<NaiveDate>, _>(i)? {
                        Some(d) => builder.append_value((d - epoch).num_days() * 86_400_000),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Time32(unit) => time32_array(rows, i, unit, &dt)?,
            CanonicalType::Time64(unit) => time64_array(rows, i, unit, &dt)?,
            CanonicalType::Timestamp { unit, tz } => {
                // PG TIMESTAMP / TIMESTAMPTZ → epoch instant, stored in the
                // declared unit. tz=None reads NaiveDateTime; tz=Some reads
                // a UTC instant from TIMESTAMPTZ. PG resolves to
                // microseconds, so a declared sub-µs unit is limited by that.
                let micros: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| -> Result<Option<i64>, HotStorageError> {
                        if tz.is_some() {
                            Ok(row
                                .try_get::<Option<DateTime<Utc>>, _>(i)?
                                .map(|dt| dt.timestamp_micros()))
                        } else {
                            Ok(row
                                .try_get::<Option<NaiveDateTime>, _>(i)?
                                .map(|ts| ts.and_utc().timestamp_micros()))
                        }
                    })
                    .collect::<Result<_, _>>()?;
                timestamp_array(micros, unit, tz)
            }
            CanonicalType::Decimal128 { precision, scale } => {
                // PG NUMERIC → Arrow Decimal128 at the declared scale.
                let mut builder = Decimal128Builder::with_capacity(rows.len())
                    .with_precision_and_scale(precision, scale)?;
                for row in rows {
                    match row.try_get::<Option<BigDecimal>, _>(i)? {
                        Some(bd) => builder.append_value(bigdecimal_to_i128(&bd, scale)?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Decimal256 { precision, scale } => {
                // PG NUMERIC → Arrow Decimal256 at the declared scale.
                let mut builder = Decimal256Builder::with_capacity(rows.len())
                    .with_precision_and_scale(precision, scale)?;
                for row in rows {
                    match row.try_get::<Option<BigDecimal>, _>(i)? {
                        Some(bd) => builder.append_value(bigdecimal_to_i256(&bd, scale)?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Binary => {
                // PG `bytea` round-trips as `Vec<u8>` via sqlx. Used by
                // `__penca_system__.tables.arrow_schema` (IPC-serialized
                // schema bytes).
                let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
                for row in rows {
                    match row.try_get::<Option<Vec<u8>>, _>(i)? {
                        Some(v) => builder.append_value(&v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::LargeUtf8 => {
                // PG TEXT → Arrow LargeUtf8 (identity-preserving: must read
                // back as LargeUtf8, not collapse to Utf8).
                let mut builder = LargeStringBuilder::new();
                for row in rows {
                    match row.try_get::<Option<String>, _>(i)? {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::Utf8View => {
                // PG TEXT → Arrow Utf8View (identity-preserving).
                let mut builder = StringViewBuilder::new();
                for row in rows {
                    match row.try_get::<Option<String>, _>(i)? {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::LargeBinary => {
                // PG `bytea` → Arrow LargeBinary (identity-preserving).
                let mut builder = LargeBinaryBuilder::new();
                for row in rows {
                    match row.try_get::<Option<Vec<u8>>, _>(i)? {
                        Some(v) => builder.append_value(&v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            CanonicalType::BinaryView => {
                // PG `bytea` → Arrow BinaryView (identity-preserving).
                let mut builder = BinaryViewBuilder::new();
                for row in rows {
                    match row.try_get::<Option<Vec<u8>>, _>(i)? {
                        Some(v) => builder.append_value(&v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            // PG `elem[]` → List / LargeList / FixedSizeList of any
            // supported scalar child (e.g. `text[]` backs the system
            // tables' `{partition,clustering,primary}_keys`). The child is
            // guaranteed scalar by `from_arrow` (nested-of-nested rejected).
            CanonicalType::List(_)
            | CanonicalType::LargeList(_)
            | CanonicalType::FixedSizeList(_, _) => read_scalar_list(rows, i, &dt)?,
        };
        columns.push(array);
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Read a PG `elem[]` column into a List / LargeList / FixedSizeList
/// array matching `dt`, dispatching on the (scalar) child type. The child
/// field metadata comes from `dt`, so the rebuilt array matches the stored
/// schema exactly. Null list rows are encoded by the offset buffer
/// (List/LargeList) or by padding `len` child nulls (FixedSizeList, whose
/// values buffer must stay `len * num_rows`). The child is guaranteed
/// scalar by `from_arrow`, so the recursion is single-level.
fn read_scalar_list(rows: &[PgRow], i: usize, dt: &DataType) -> Result<ArrayRef, HotStorageError> {
    let (child_field, fixed_len): (&Arc<Field>, Option<i32>) = match dt {
        DataType::List(f) | DataType::LargeList(f) => (f, None),
        DataType::FixedSizeList(f, n) => (f, Some(*n)),
        _ => return Err(HotStorageError::UnsupportedType(dt.clone())),
    };
    let child_ct = CanonicalType::from_arrow(child_field.data_type())
        .map_err(|e| HotStorageError::UnsupportedType(e.0))?;

    // Read each row's `Vec<Option<T>>`, appending elements to a flat child
    // builder and recording per-row element counts (`None` = SQL NULL row,
    // padded with `fixed_len` child nulls for FixedSizeList).
    macro_rules! read_flat {
        ($sqlx:ty, $builder:expr, $append:expr) => {{
            let mut child = $builder;
            let mut lens: Vec<Option<i32>> = Vec::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<Vec<Option<$sqlx>>>, _>(i)? {
                    Some(items) => {
                        for item in &items {
                            match item {
                                Some(v) => $append(&mut child, v)?,
                                None => child.append_null(),
                            }
                        }
                        lens.push(Some(items.len() as i32));
                    }
                    None => {
                        if let Some(flen) = fixed_len {
                            for _ in 0..flen {
                                child.append_null();
                            }
                        }
                        lens.push(None);
                    }
                }
            }
            (Arc::new(child.finish()) as ArrayRef, lens)
        }};
    }

    let (values, lens): (ArrayRef, Vec<Option<i32>>) = match &child_ct {
        CanonicalType::Boolean => {
            read_flat!(bool, BooleanBuilder::new(), |b: &mut BooleanBuilder,
                                                     v: &bool|
             -> Result<
                (),
                HotStorageError,
            > {
                b.append_value(*v);
                Ok(())
            })
        }
        CanonicalType::Int8 => read_flat!(i16, Int8Builder::new(), |b: &mut Int8Builder,
                                                                    v: &i16|
         -> Result<
            (),
            HotStorageError,
        > {
            b.append_value(i8::try_from(*v).map_err(|_| narrow_err(*v, "Int8"))?);
            Ok(())
        }),
        CanonicalType::Int16 => read_flat!(i16, Int16Builder::new(), |b: &mut Int16Builder,
                                                                      v: &i16|
         -> Result<
            (),
            HotStorageError,
        > {
            b.append_value(*v);
            Ok(())
        }),
        CanonicalType::Int32 => read_flat!(i32, Int32Builder::new(), |b: &mut Int32Builder,
                                                                      v: &i32|
         -> Result<
            (),
            HotStorageError,
        > {
            b.append_value(*v);
            Ok(())
        }),
        CanonicalType::Int64 => read_flat!(i64, Int64Builder::new(), |b: &mut Int64Builder,
                                                                      v: &i64|
         -> Result<
            (),
            HotStorageError,
        > {
            b.append_value(*v);
            Ok(())
        }),
        CanonicalType::UInt8 => read_flat!(i16, UInt8Builder::new(), |b: &mut UInt8Builder,
                                                                      v: &i16|
         -> Result<
            (),
            HotStorageError,
        > {
            b.append_value(u8::try_from(*v).map_err(|_| narrow_err(*v, "UInt8"))?);
            Ok(())
        }),
        CanonicalType::UInt16 => read_flat!(i32, UInt16Builder::new(), |b: &mut UInt16Builder,
                                                                        v: &i32|
         -> Result<
            (),
            HotStorageError,
        > {
            b.append_value(u16::try_from(*v).map_err(|_| narrow_err(*v, "UInt16"))?);
            Ok(())
        }),
        CanonicalType::UInt32 => read_flat!(i64, UInt32Builder::new(), |b: &mut UInt32Builder,
                                                                        v: &i64|
         -> Result<
            (),
            HotStorageError,
        > {
            b.append_value(u32::try_from(*v).map_err(|_| narrow_err(*v, "UInt32"))?);
            Ok(())
        }),
        CanonicalType::UInt64 => {
            read_flat!(BigDecimal, UInt64Builder::new(), |b: &mut UInt64Builder,
                                                          v: &BigDecimal|
             -> Result<
                (),
                HotStorageError,
            > {
                b.append_value(bigdecimal_to_u64(v)?);
                Ok(())
            })
        }
        CanonicalType::Float16 => {
            read_flat!(f32, Float16Builder::new(), |b: &mut Float16Builder,
                                                    v: &f32|
             -> Result<
                (),
                HotStorageError,
            > {
                b.append_value(f16::from_f32(*v));
                Ok(())
            })
        }
        CanonicalType::Float32 => {
            read_flat!(f32, Float32Builder::new(), |b: &mut Float32Builder,
                                                    v: &f32|
             -> Result<
                (),
                HotStorageError,
            > {
                b.append_value(*v);
                Ok(())
            })
        }
        CanonicalType::Float64 => {
            read_flat!(f64, Float64Builder::new(), |b: &mut Float64Builder,
                                                    v: &f64|
             -> Result<
                (),
                HotStorageError,
            > {
                b.append_value(*v);
                Ok(())
            })
        }
        CanonicalType::Utf8 => read_flat!(String, StringBuilder::new(), |b: &mut StringBuilder,
                                                                         v: &String|
         -> Result<
            (),
            HotStorageError,
        > {
            b.append_value(v);
            Ok(())
        }),
        CanonicalType::LargeUtf8 => {
            read_flat!(
                String,
                LargeStringBuilder::new(),
                |b: &mut LargeStringBuilder, v: &String| -> Result<(), HotStorageError> {
                    b.append_value(v);
                    Ok(())
                }
            )
        }
        CanonicalType::Utf8View => read_flat!(
            String,
            StringViewBuilder::new(),
            |b: &mut StringViewBuilder, v: &String| -> Result<(), HotStorageError> {
                b.append_value(v);
                Ok(())
            }
        ),
        CanonicalType::Binary => {
            read_flat!(Vec<u8>, BinaryBuilder::new(), |b: &mut BinaryBuilder,
                                                       v: &Vec<u8>|
             -> Result<
                (),
                HotStorageError,
            > {
                b.append_value(v);
                Ok(())
            })
        }
        CanonicalType::LargeBinary => {
            read_flat!(
                Vec<u8>,
                LargeBinaryBuilder::new(),
                |b: &mut LargeBinaryBuilder, v: &Vec<u8>| -> Result<(), HotStorageError> {
                    b.append_value(v);
                    Ok(())
                }
            )
        }
        CanonicalType::BinaryView => {
            read_flat!(
                Vec<u8>,
                BinaryViewBuilder::new(),
                |b: &mut BinaryViewBuilder, v: &Vec<u8>| -> Result<(), HotStorageError> {
                    b.append_value(v);
                    Ok(())
                }
            )
        }
        CanonicalType::Decimal128 { precision, scale } => {
            let (precision, scale) = (*precision, *scale);
            read_flat!(
                BigDecimal,
                Decimal128Builder::new().with_precision_and_scale(precision, scale)?,
                |b: &mut Decimal128Builder, v: &BigDecimal| -> Result<(), HotStorageError> {
                    b.append_value(bigdecimal_to_i128(v, scale)?);
                    Ok(())
                }
            )
        }
        CanonicalType::Decimal256 { precision, scale } => {
            let (precision, scale) = (*precision, *scale);
            read_flat!(
                BigDecimal,
                Decimal256Builder::new().with_precision_and_scale(precision, scale)?,
                |b: &mut Decimal256Builder, v: &BigDecimal| -> Result<(), HotStorageError> {
                    b.append_value(bigdecimal_to_i256(v, scale)?);
                    Ok(())
                }
            )
        }
        CanonicalType::Date32 => {
            let epoch = unix_epoch_date();
            read_flat!(NaiveDate, Date32Builder::new(), |b: &mut Date32Builder,
                                                         v: &NaiveDate|
             -> Result<
                (),
                HotStorageError,
            > {
                b.append_value((*v - epoch).num_days() as i32);
                Ok(())
            })
        }
        CanonicalType::Date64 => {
            let epoch = unix_epoch_date();
            read_flat!(NaiveDate, Date64Builder::new(), |b: &mut Date64Builder,
                                                         v: &NaiveDate|
             -> Result<
                (),
                HotStorageError,
            > {
                b.append_value((*v - epoch).num_days() * 86_400_000);
                Ok(())
            })
        }
        CanonicalType::Time32(unit) => match unit {
            TimeUnit::Second => read_flat!(
                NaiveTime,
                Time32SecondBuilder::new(),
                |b: &mut Time32SecondBuilder, v: &NaiveTime| -> Result<(), HotStorageError> {
                    b.append_value((*v - NaiveTime::MIN).num_seconds() as i32);
                    Ok(())
                }
            ),
            TimeUnit::Millisecond => {
                read_flat!(
                    NaiveTime,
                    Time32MillisecondBuilder::new(),
                    |b: &mut Time32MillisecondBuilder,
                     v: &NaiveTime|
                     -> Result<(), HotStorageError> {
                        b.append_value(naivetime_millis(*v));
                        Ok(())
                    }
                )
            }
            _ => return Err(HotStorageError::UnsupportedType(dt.clone())),
        },
        CanonicalType::Time64(unit) => match unit {
            TimeUnit::Microsecond => {
                read_flat!(
                    NaiveTime,
                    Time64MicrosecondBuilder::new(),
                    |b: &mut Time64MicrosecondBuilder,
                     v: &NaiveTime|
                     -> Result<(), HotStorageError> {
                        b.append_value(naivetime_micros(*v));
                        Ok(())
                    }
                )
            }
            TimeUnit::Nanosecond => {
                read_flat!(
                    NaiveTime,
                    Time64NanosecondBuilder::new(),
                    |b: &mut Time64NanosecondBuilder,
                     v: &NaiveTime|
                     -> Result<(), HotStorageError> {
                        b.append_value(naivetime_nanos(*v));
                        Ok(())
                    }
                )
            }
            _ => return Err(HotStorageError::UnsupportedType(dt.clone())),
        },
        CanonicalType::Timestamp { unit, tz } => {
            // Timestamp can't use the scalar-builder macro: the child array
            // must carry the tz + unit. Read each element to epoch micros,
            // then build the flat child with `timestamp_array`.
            let unit = *unit;
            let tz = tz.clone();
            let mut flat: Vec<Option<i64>> = Vec::new();
            let mut lens: Vec<Option<i32>> = Vec::with_capacity(rows.len());
            for row in rows {
                let items: Option<Vec<Option<i64>>> = if tz.is_some() {
                    row.try_get::<Option<Vec<Option<DateTime<Utc>>>>, _>(i)?
                        .map(|v| {
                            v.into_iter()
                                .map(|o| o.map(|d| d.timestamp_micros()))
                                .collect()
                        })
                } else {
                    row.try_get::<Option<Vec<Option<NaiveDateTime>>>, _>(i)?
                        .map(|v| {
                            v.into_iter()
                                .map(|o| o.map(|ts| ts.and_utc().timestamp_micros()))
                                .collect()
                        })
                };
                match items {
                    Some(items) => {
                        let n = items.len() as i32;
                        flat.extend(items);
                        lens.push(Some(n));
                    }
                    None => {
                        if let Some(flen) = fixed_len {
                            for _ in 0..flen {
                                flat.push(None);
                            }
                        }
                        lens.push(None);
                    }
                }
            }
            (timestamp_array(flat, unit, tz), lens)
        }
        // `from_arrow` rejects nested-of-nested, so a list child is never a
        // container; named explicitly to keep the match catch-all-free.
        CanonicalType::List(_)
        | CanonicalType::LargeList(_)
        | CanonicalType::FixedSizeList(_, _) => {
            return Err(HotStorageError::UnsupportedType(dt.clone()));
        }
    };

    let nulls = NullBuffer::from_iter(lens.iter().map(|l| l.is_some()));
    let field = child_field.clone();
    match dt {
        DataType::List(_) => {
            let mut offs: Vec<i32> = Vec::with_capacity(lens.len() + 1);
            offs.push(0);
            let mut acc = 0i32;
            for l in &lens {
                acc += l.unwrap_or(0);
                offs.push(acc);
            }
            ListArray::try_new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offs)),
                values,
                Some(nulls),
            )
            .map(|a| Arc::new(a) as ArrayRef)
            .map_err(HotStorageError::Arrow)
        }
        DataType::LargeList(_) => {
            let mut offs: Vec<i64> = Vec::with_capacity(lens.len() + 1);
            offs.push(0);
            let mut acc = 0i64;
            for l in &lens {
                acc += l.unwrap_or(0) as i64;
                offs.push(acc);
            }
            LargeListArray::try_new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offs)),
                values,
                Some(nulls),
            )
            .map(|a| Arc::new(a) as ArrayRef)
            .map_err(HotStorageError::Arrow)
        }
        DataType::FixedSizeList(_, n) => {
            // The fixed length `n` is a declared-schema property that the PG
            // `elem[]` column does not enforce. Every write path upholds it
            // (the codec only ever serializes Arrow `FixedSizeList` rows,
            // which are length-`n` by construction, and penca-merge
            // re-inserts through that same codec), so a stored row of a
            // different length can only arise from an out-of-band write
            // bypassing Penca. Guard it here with a column-scoped error
            // rather than letting `try_new`'s opaque buffer-length message
            // surface for the whole segment.
            for len in lens.iter().flatten() {
                if *len != *n {
                    return Err(HotStorageError::Arrow(
                        arrow::error::ArrowError::ComputeError(format!(
                            "FixedSizeList column has a stored row of length {len}, expected {n}; \
                             the fixed size is upheld by the write codec, not the PG `elem[]` \
                             column — a non-codec write produced a wrong-length row"
                        )),
                    ));
                }
            }
            FixedSizeListArray::try_new(field, *n, values, Some(nulls))
                .map(|a| Arc::new(a) as ArrayRef)
                .map_err(HotStorageError::Arrow)
        }
        _ => Err(HotStorageError::UnsupportedType(dt.clone())),
    }
}

/// Convert a `BigDecimal` (from a PG NUMERIC column) to the unscaled
/// `i128` an Arrow `Decimal128(_, scale)` array stores. Rescales to the
/// column's declared scale first so the unscaled integer matches.
fn bigdecimal_to_i128(bd: &BigDecimal, scale: i8) -> Result<i128, HotStorageError> {
    let (unscaled, _exp) = bd.with_scale(scale as i64).into_bigint_and_exponent();
    // Effectively unreachable: an Arrow Decimal128 caps precision at 38,
    // and a value at the declared scale fitting 38 decimal digits always
    // fits i128 (~38 digits). The fail-loud guard stays in case a PG
    // NUMERIC wider than the column's declared precision ever reaches
    // here — surfaced as a conversion error, not a pencaated "unsupported
    // type".
    i128::try_from(unscaled).map_err(|_| {
        HotStorageError::Arrow(arrow::error::ArrowError::ComputeError(format!(
            "NUMERIC value {bd} does not fit Arrow Decimal128 at scale {scale}"
        )))
    })
}

/// Convert a `BigDecimal` (from a PG NUMERIC column) to the unscaled
/// `i256` an Arrow `Decimal256(_, scale)` array stores — the
/// 256-bit analogue of [`bigdecimal_to_i128`]. Rescales to the
/// declared scale, then parses the unscaled integer text into `i256`,
/// failing loudly if it overflows 256 bits.
fn bigdecimal_to_i256(bd: &BigDecimal, scale: i8) -> Result<i256, HotStorageError> {
    let (unscaled, _exp) = bd.with_scale(scale as i64).into_bigint_and_exponent();
    i256::from_string(&unscaled.to_string()).ok_or_else(|| {
        HotStorageError::Arrow(arrow::error::ArrowError::ComputeError(format!(
            "NUMERIC value {bd} does not fit Arrow Decimal256 at scale {scale}"
        )))
    })
}

/// Convert a `BigDecimal` (from a PG NUMERIC column backing an Arrow
/// `UInt64`) to `u64`. `UInt64` widens to NUMERIC on the PG side
/// (BIGINT cannot hold values above `i64::MAX`); read-back narrows the
/// integer part back to `u64`, failing loudly if it does not fit (a
/// negative value, or one above `u64::MAX`). Any fractional part is
/// truncated toward zero by `with_scale(0)` — values written from a
/// `UInt64` column are always integral, so this only matters if a
/// fractional NUMERIC was inserted into the column out-of-band.
fn bigdecimal_to_u64(bd: &BigDecimal) -> Result<u64, HotStorageError> {
    let (int, _exp) = bd.with_scale(0).into_bigint_and_exponent();
    u64::try_from(int).map_err(|_| {
        HotStorageError::Arrow(arrow::error::ArrowError::ComputeError(format!(
            "NUMERIC value {bd} does not fit Arrow UInt64"
        )))
    })
}

/// Build a `Time32` array from a PG TIME column (seconds / milliseconds
/// since midnight). Arrow's `Time32` is only valid for second /
/// millisecond units; any other unit is an internal-invariant violation
/// (the gate would not have produced it) surfaced as `UnsupportedType`.
fn time32_array(
    rows: &[PgRow],
    i: usize,
    unit: TimeUnit,
    dt: &DataType,
) -> Result<ArrayRef, HotStorageError> {
    match unit {
        TimeUnit::Second => {
            let mut builder = Time32SecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<NaiveTime>, _>(i)? {
                    Some(t) => builder.append_value((t - NaiveTime::MIN).num_seconds() as i32),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        TimeUnit::Millisecond => {
            let mut builder = Time32MillisecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<NaiveTime>, _>(i)? {
                    Some(t) => builder.append_value(naivetime_millis(t)),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(HotStorageError::UnsupportedType(dt.clone())),
    }
}

/// Build a `Time64` array from a PG TIME column (microseconds /
/// nanoseconds since midnight). Arrow's `Time64` is only valid for
/// microsecond / nanosecond units.
fn time64_array(
    rows: &[PgRow],
    i: usize,
    unit: TimeUnit,
    dt: &DataType,
) -> Result<ArrayRef, HotStorageError> {
    match unit {
        TimeUnit::Microsecond => {
            let mut builder = Time64MicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<NaiveTime>, _>(i)? {
                    Some(t) => builder.append_value(naivetime_micros(t)),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        TimeUnit::Nanosecond => {
            let mut builder = Time64NanosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<NaiveTime>, _>(i)? {
                    Some(t) => builder.append_value(naivetime_nanos(t)),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(HotStorageError::UnsupportedType(dt.clone())),
    }
}

fn naivetime_millis(t: NaiveTime) -> i32 {
    (t - NaiveTime::MIN).num_milliseconds() as i32
}

fn naivetime_micros(t: NaiveTime) -> i64 {
    (t - NaiveTime::MIN)
        .num_microseconds()
        .expect("time-of-day fits i64 microseconds")
}

fn naivetime_nanos(t: NaiveTime) -> i64 {
    (t - NaiveTime::MIN)
        .num_nanoseconds()
        .expect("time-of-day fits i64 nanoseconds")
}

/// The UTC `NaiveDateTime` at `row` for a timestamp array of the given
/// `unit` (tz-aware arrays still yield the UTC instant). Used to render
/// the SQL literal.
fn timestamp_naive(array: &ArrayRef, row: usize, unit: TimeUnit) -> NaiveDateTime {
    match unit {
        TimeUnit::Second => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .value_as_datetime(row),
        TimeUnit::Millisecond => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value_as_datetime(row),
        TimeUnit::Microsecond => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value_as_datetime(row),
        TimeUnit::Nanosecond => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value_as_datetime(row),
    }
    .expect("non-null Timestamp value")
}

/// Build a timestamp array of the declared `unit` carrying `tz`, from
/// microsecond-since-epoch values (PG's TIMESTAMP/TIMESTAMPTZ
/// resolution). Sub-microsecond declared units cannot recover precision
/// PG never stored; the value is widened/narrowed by unit only.
fn timestamp_array(micros: Vec<Option<i64>>, unit: TimeUnit, tz: Option<Arc<str>>) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(
            TimestampSecondArray::from(
                micros
                    .into_iter()
                    .map(|m| m.map(|v| v.div_euclid(1_000_000)))
                    .collect::<Vec<_>>(),
            )
            .with_timezone_opt(tz),
        ),
        TimeUnit::Millisecond => Arc::new(
            TimestampMillisecondArray::from(
                micros
                    .into_iter()
                    .map(|m| m.map(|v| v.div_euclid(1_000)))
                    .collect::<Vec<_>>(),
            )
            .with_timezone_opt(tz),
        ),
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone_opt(tz))
        }
        TimeUnit::Nanosecond => Arc::new(
            TimestampNanosecondArray::from(
                micros
                    .into_iter()
                    .map(|m| m.map(|v| v * 1_000))
                    .collect::<Vec<_>>(),
            )
            .with_timezone_opt(tz),
        ),
    }
}

/// Error for a checked integer narrow that overflowed on read-back. The
/// wider PG physical type (SMALLINT/INTEGER/BIGINT) can in principle
/// carry a value outside the declared Arrow type's range; we fail loud
/// rather than silently wrapping with `as`.
fn narrow_err(value: impl std::fmt::Display, target: &str) -> HotStorageError {
    HotStorageError::Arrow(arrow::error::ArrowError::ComputeError(format!(
        "value {value} does not fit Arrow {target} on read-back"
    )))
}

/// Convert an Arrow array value at `row` to a SQL literal string.
///
/// Used for building multi-row INSERT statements from Arrow data.
/// Strings are escaped by doubling single quotes.
pub(crate) fn arrow_to_sql_literal(
    array: &ArrayRef,
    row: usize,
) -> Result<String, HotStorageError> {
    if array.is_null(row) {
        return Ok("NULL".into());
    }
    let dt = array.data_type().clone();
    let ct = CanonicalType::from_arrow(&dt).map_err(|e| HotStorageError::UnsupportedType(e.0))?;
    match ct {
        CanonicalType::Utf8 => {
            let v = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row);
            Ok(format!("'{}'", v.replace('\'', "''")))
        }
        CanonicalType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::UInt64 => Ok(array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::Int32 => Ok(array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::Int16 => Ok(array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::Int8 => Ok(array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::UInt8 => Ok(array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::UInt16 => Ok(array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::UInt32 => Ok(array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::Float64 => Ok(array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::Float32 => Ok(array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row)
            .to_string()),
        CanonicalType::Float16 => Ok(array
            .as_any()
            .downcast_ref::<Float16Array>()
            .unwrap()
            .value(row)
            .to_f32()
            .to_string()),
        CanonicalType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row);
            Ok(if v { "TRUE" } else { "FALSE" }.into())
        }
        CanonicalType::Date32 => {
            let d = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value_as_date(row)
                .expect("non-null Date32 value");
            Ok(format!("'{d}'::date"))
        }
        CanonicalType::Date64 => {
            // Date64 is "ms since epoch"; PG DATE has day granularity, so a
            // non-midnight time-of-day component (which Arrow permits) is
            // dropped on write and reads back at midnight. Matches PG DATE
            // semantics.
            let d = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap()
                .value_as_date(row)
                .expect("non-null Date64 value");
            Ok(format!("'{d}'::date"))
        }
        CanonicalType::Time32(unit) => {
            let t = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<Time32SecondArray>()
                    .unwrap()
                    .value_as_time(row),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .unwrap()
                    .value_as_time(row),
                _ => return Err(HotStorageError::UnsupportedType(dt)),
            }
            .expect("non-null Time32 value");
            Ok(format!("'{t}'::time"))
        }
        CanonicalType::Time64(unit) => {
            let t = match unit {
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap()
                    .value_as_time(row),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap()
                    .value_as_time(row),
                _ => return Err(HotStorageError::UnsupportedType(dt)),
            }
            .expect("non-null Time64 value");
            Ok(format!("'{t}'::time"))
        }
        CanonicalType::Timestamp { unit, tz } => {
            // PG resolves both TIMESTAMP and TIMESTAMPTZ to microseconds;
            // value_as_datetime yields the UTC naive datetime regardless of
            // the array's tz. tz=Some marks the literal as a UTC instant.
            let ts = timestamp_naive(array, row, unit);
            Ok(match tz {
                Some(_) => format!("'{ts}+00'::timestamptz"),
                None => format!("'{ts}'::timestamp"),
            })
        }
        CanonicalType::Decimal128 { .. } => {
            // PG NUMERIC literal is the plain decimal text, unquoted.
            Ok(array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value_as_string(row))
        }
        CanonicalType::Decimal256 { .. } => {
            // PG NUMERIC literal is the plain decimal text, unquoted.
            Ok(array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .value_as_string(row))
        }
        CanonicalType::Binary => {
            // PG bytea literal: `'\xHEXHEX'::bytea`.
            let bytes = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row);
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            Ok(format!(r"'\x{hex}'::bytea"))
        }
        CanonicalType::LargeUtf8 => {
            let v = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row);
            Ok(format!("'{}'", v.replace('\'', "''")))
        }
        CanonicalType::Utf8View => {
            let v = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(row);
            Ok(format!("'{}'", v.replace('\'', "''")))
        }
        CanonicalType::LargeBinary => {
            let bytes = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row);
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            Ok(format!(r"'\x{hex}'::bytea"))
        }
        CanonicalType::BinaryView => {
            let bytes = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap()
                .value(row);
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            Ok(format!(r"'\x{hex}'::bytea"))
        }
        // PG array literal `ARRAY[..]::elem[]` for List / LargeList /
        // FixedSizeList of any scalar child. Each element renders through
        // `arrow_to_sql_literal` recursively (the child is scalar, so the
        // recursion is one level), and the element cast type comes from the
        // dialect so it stays the single source of truth.
        CanonicalType::List(_)
        | CanonicalType::LargeList(_)
        | CanonicalType::FixedSizeList(_, _) => {
            let elements: ArrayRef = match ct {
                CanonicalType::List(_) => array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(row),
                CanonicalType::LargeList(_) => array
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .unwrap()
                    .value(row),
                _ => array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .value(row),
            };
            let child_sql = PgDialect::arrow_type_to_sql(elements.data_type()).map_err(|e| {
                HotStorageError::Arrow(arrow::error::ArrowError::ComputeError(format!(
                    "list element type not mappable to a PG type: {e}"
                )))
            })?;
            let literals = (0..elements.len())
                .map(|j| arrow_to_sql_literal(&elements, j))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("ARRAY[{}]::{}[]", literals.join(","), child_sql))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn bigdecimal_to_i128_rescales_to_declared_scale() {
        let bd = |s: &str| BigDecimal::from_str(s).unwrap();
        // Already at the declared scale.
        assert_eq!(bigdecimal_to_i128(&bd("123.45"), 2).unwrap(), 12345);
        // Fewer fractional digits than the scale → rescale up.
        assert_eq!(bigdecimal_to_i128(&bd("1.5"), 2).unwrap(), 150);
        // Negative value.
        assert_eq!(bigdecimal_to_i128(&bd("-0.07"), 2).unwrap(), -7);
        // Integer at scale 0.
        assert_eq!(bigdecimal_to_i128(&bd("42"), 0).unwrap(), 42);
    }

    #[test]
    fn date32_epoch_offset_is_days_since_1970() {
        let epoch = unix_epoch_date();
        let days = |y, m, d| (NaiveDate::from_ymd_opt(y, m, d).unwrap() - epoch).num_days() as i32;
        assert_eq!(days(1970, 1, 1), 0);
        assert_eq!(days(1970, 1, 2), 1);
        assert_eq!(days(1969, 12, 31), -1);
    }

    #[test]
    fn bigdecimal_to_u64_handles_boundaries() {
        let bd = |s: &str| BigDecimal::from_str(s).unwrap();
        assert_eq!(bigdecimal_to_u64(&bd("0")).unwrap(), 0);
        assert_eq!(
            bigdecimal_to_u64(&bd("18446744073709551615")).unwrap(),
            u64::MAX
        );
        // Negative and above-u64::MAX fail loud.
        assert!(bigdecimal_to_u64(&bd("-1")).is_err());
        assert!(bigdecimal_to_u64(&bd("18446744073709551616")).is_err());
        // Fractional truncates toward zero (only reachable via out-of-band
        // data; a UInt64 column always writes integral NUMERIC).
        assert_eq!(bigdecimal_to_u64(&bd("42.9")).unwrap(), 42);
    }

    #[test]
    fn bigdecimal_to_i256_rescales_and_guards_overflow() {
        let bd = |s: &str| BigDecimal::from_str(s).unwrap();
        assert_eq!(
            bigdecimal_to_i256(&bd("123.45"), 2).unwrap(),
            i256::from_i128(12345)
        );
        assert_eq!(
            bigdecimal_to_i256(&bd("1.5"), 2).unwrap(),
            i256::from_i128(150)
        );
        assert_eq!(
            bigdecimal_to_i256(&bd("-0.07"), 2).unwrap(),
            i256::from_i128(-7)
        );
        // Beyond 256 bits → Err (i256 caps at ~76-77 decimal digits).
        assert!(bigdecimal_to_i256(&bd(&"9".repeat(78)), 0).is_err());
    }

    /// Render the single-row literal for `arr` — `arrow_to_sql_literal` is
    /// PG-independent (no row decode), so the emitted SQL is unit-testable.
    fn lit<A: Array + 'static>(arr: A) -> String {
        arrow_to_sql_literal(&(Arc::new(arr) as ArrayRef), 0).unwrap()
    }

    #[test]
    fn arrow_to_sql_literal_temporal_shapes() {
        // Date64 (ms since epoch) → ::date; 86_400_000 ms = 1970-01-02.
        assert_eq!(
            lit(Date64Array::from(vec![86_400_000i64])),
            "'1970-01-02'::date"
        );
        // Time32(ms) / Time64(us) → ::time; 12:30:00.
        assert_eq!(
            lit(Time32MillisecondArray::from(vec![45_000_000i32])),
            "'12:30:00'::time"
        );
        assert_eq!(
            lit(Time64MicrosecondArray::from(vec![45_000_000_000i64])),
            "'12:30:00'::time"
        );
        // Timestamp tz=None → ::timestamp; tz=Some → +00 ::timestamptz.
        assert_eq!(
            lit(TimestampMicrosecondArray::from(vec![0i64])),
            "'1970-01-01 00:00:00'::timestamp"
        );
        assert_eq!(
            lit(TimestampMicrosecondArray::from(vec![0i64]).with_timezone("UTC")),
            "'1970-01-01 00:00:00+00'::timestamptz"
        );
    }

    #[test]
    fn arrow_to_sql_literal_large_and_view_shapes() {
        let mut ls = LargeStringBuilder::new();
        ls.append_value("x'y");
        assert_eq!(lit(ls.finish()), "'x''y'");
        let mut sv = StringViewBuilder::new();
        sv.append_value("ab");
        assert_eq!(lit(sv.finish()), "'ab'");
        let mut lb = LargeBinaryBuilder::new();
        lb.append_value([1u8, 2]);
        assert_eq!(lit(lb.finish()), r"'\x0102'::bytea");
        let mut bv = BinaryViewBuilder::new();
        bv.append_value([3u8]);
        assert_eq!(lit(bv.finish()), r"'\x03'::bytea");
    }

    #[test]
    fn arrow_to_sql_literal_list_recurses_with_child_cast() {
        // List<Int32> → ARRAY[..]::INTEGER[]; element cast from the dialect.
        let mut lb = ListBuilder::new(Int32Builder::new());
        lb.values().append_value(1);
        lb.values().append_value(2);
        lb.append(true);
        let arr: ArrayRef = Arc::new(lb.finish());
        assert_eq!(
            arrow_to_sql_literal(&arr, 0).unwrap(),
            "ARRAY[1,2]::INTEGER[]"
        );
    }
}
