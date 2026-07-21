//! Arrow IPC / Flight encoding helpers used by the trait-impl arms in
//! [`super::service`]. Vendored from datafusion-flight-sql-server
//! v0.4.16 (same provenance as the trait impl). The relocation is pure
//! file-move; behavior preserved exactly.
//!
//! Lives in its own module so `service.rs` stays focused on
//! [`arrow_flight::sql::server::FlightSqlService`] trait body + the
//! Penca-added per-request helpers, with the Arrow IPC codec details
//! one file over.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{IpcMessage, SchemaAsIpc};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Field, Schema, SchemaBuilder, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::common::ParamValues;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use prost::bytes::Bytes;
use tonic::Status;

pub(super) fn encode_schema(schema: &Schema) -> std::result::Result<Bytes, ArrowError> {
    let options = IpcWriteOptions::default();
    let message: std::result::Result<IpcMessage, ArrowError> =
        SchemaAsIpc::new(schema, &options).try_into();
    let IpcMessage(schema) = message?;
    Ok(schema)
}

pub(super) fn get_schema_for_plan(logical_plan: &LogicalPlan) -> SchemaRef {
    let schema: SchemaRef = Arc::new(logical_plan.schema().as_arrow().clone());
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::iter([]));
    flight_data_stream
        .known_schema()
        .expect("flight data schema should be known when explicitly provided via `with_schema`")
}

/// Reconcile an executed `DoGet` stream's schema with the schema
/// `get_flight_info` advertised, returning the schema the stream must encode to
/// — or `None` when no reconciliation is needed (the hot path).
///
/// CHA-402: `get_flight_info` advertises the *logical* plan's schema
/// ([`get_schema_for_plan`]); `DoGet` streams the *physical* plan's schema.
/// DataFusion's scalar-subquery decorrelation (`scalar_subquery_to_join`)
/// rewrites a correlated `COUNT` to a LEFT JOIN and over-marks the
/// semantically-non-null count as **nullable** in the physical plan, so the
/// streamed schema can be more nullable than the advertised one — and ADBC
/// rejects the divergence (`endpoint 0 returned inconsistent schema`). The
/// logical schema is the correct one (`COUNT(*)` is never null), so we tighten
/// the stream back to it rather than degrade the advertised schema.
///
/// Returns a schema with the stream's field names and **types** but the
/// advertised **nullability** where they differ. Keeping the stream's types
/// makes relabeling a batch a zero-copy [`arrow_array::RecordBatch::try_new`]
/// (which also validates that a now-non-null column carries no actual nulls — a
/// loud error beats a silently inconsistent stream). Returns `None` when
/// nullability already agrees field-for-field — the caller then encodes the
/// stream untouched, so a query whose schemas match pays only one cheap
/// allocation-free scan. A field-count divergence also returns `None` (it is not
/// a nullability mismatch; the encode surfaces the underlying error).
pub(super) fn reconcile_stream_to_advertised(
    stream_schema: &Schema,
    advertised_schema: &Schema,
) -> Option<SchemaRef> {
    let stream_fields = stream_schema.fields();
    let advertised_fields = advertised_schema.fields();
    if stream_fields.len() != advertised_fields.len() {
        return None;
    }
    // Hot path: an allocation-free scan. When every field already agrees on
    // nullability (the overwhelming common case), bail before building anything.
    let diverges = stream_fields.iter().zip(advertised_fields.iter()).any(
        |(stream_field, advertised_field)| {
            stream_field.is_nullable() != advertised_field.is_nullable()
        },
    );
    if !diverges {
        return None;
    }
    let reconciled: Vec<Arc<Field>> = stream_fields
        .iter()
        .zip(advertised_fields.iter())
        .map(|(stream_field, advertised_field)| {
            if stream_field.is_nullable() == advertised_field.is_nullable() {
                Arc::clone(stream_field)
            } else {
                Arc::new(
                    stream_field
                        .as_ref()
                        .clone()
                        .with_nullable(advertised_field.is_nullable()),
                )
            }
        })
        .collect();
    Some(Arc::new(Schema::new_with_metadata(
        reconciled,
        stream_schema.metadata().clone(),
    )))
}

pub(super) fn parameter_schema_for_plan(
    plan: &LogicalPlan,
) -> std::result::Result<SchemaRef, Box<Status>> {
    let parameters = plan
        .get_parameter_types()
        .map_err(super::error::df_error_to_status)?
        .into_iter()
        .map(|(name, dt)| {
            dt.map(|dt| (name.clone(), dt)).ok_or_else(|| {
                Status::internal(format!(
                    "unable to determine type of query parameter {name}"
                ))
            })
        })
        .collect::<std::result::Result<BTreeMap<_, _>, Status>>()?;
    let mut builder = SchemaBuilder::new();
    parameters
        .into_iter()
        .for_each(|(name, typ)| builder.push(Field::new(name, typ, false)));
    Ok(builder.finish().into())
}

/// CHA-333: decode bound parameters from a `DoPutPreparedStatementUpdate`
/// FlightData stream into `ParamValues`. The Apache flight-sql-jdbc-driver
/// sends parameters in this DoPut body — empty `VectorSchemaRoot` (no
/// `setXxx` calls) → `None`, schema with N fields and one row → `Some`
/// with the values in positional order.
///
/// Differs from [`decode_param_values`] in two ways: it consumes the
/// live FlightData stream (no IPC round-trip via the handle), and it
/// returns `None` for the no-params shape so the gateway can keep
/// routing `Statement.execute(...)` over JDBC (which also walks this
/// path) to the existing non-parameterized branch.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        param_cols = tracing::field::Empty,
        param_rows = tracing::field::Empty,
    ),
    err,
)]
pub(super) async fn decode_params_from_stream<S>(stream: S) -> Result<Option<ParamValues>, Status>
where
    S: futures::Stream<Item = std::result::Result<arrow_flight::FlightData, Status>>
        + Send
        + Unpin
        + 'static,
{
    let mut decoder = FlightDataDecoder::new(stream.map_err(super::error::status_to_flight_error));
    // Schema must arrive first; an empty stream means no parameters
    // (e.g. a non-prepared `Statement.execute(sql)` that the JDBC
    // driver internally walks through the prepared path with no
    // bindings — observed wire shape: a single empty
    // `VectorSchemaRoot.of(new FieldVector[0])`).
    let schema = loop {
        match decoder.try_next().await? {
            Some(msg) => match msg.payload {
                DecodedPayload::None => continue,
                DecodedPayload::Schema(s) => break s,
                DecodedPayload::RecordBatch(_) => {
                    return Err(Status::invalid_argument(
                        "parameter flight data must have a known schema before any record batch",
                    ));
                }
            },
            None => return Ok(None),
        }
    };
    tracing::Span::current().record("param_cols", schema.fields().len());
    if schema.fields().is_empty() {
        // Empty schema = no parameters bound. Drain the rest of the
        // stream so we don't leave bytes pending on the gRPC
        // connection (the JDBC driver still sends a final empty
        // batch in this case) and return None to steer the gateway
        // to its non-parameterized branch.
        while decoder.try_next().await?.is_some() {}
        return Ok(None);
    }
    let mut batches = Vec::new();
    while let Some(msg) = decoder.try_next().await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must contain a single schema",
                ));
            }
            DecodedPayload::RecordBatch(rb) => batches.push(rb),
        }
    }
    if batches.is_empty() {
        return Ok(None);
    }
    let batch =
        concat_batches(&schema, batches.iter()).map_err(super::error::arrow_error_to_status)?;
    tracing::Span::current().record("param_rows", batch.num_rows());
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    if batch.num_rows() > 1 {
        return Err(Status::invalid_argument(
            "parameters should contain a single row",
        ));
    }
    Ok(Some(
        record_to_param_values(&batch).map_err(super::error::df_error_to_status)?,
    ))
}

pub(super) async fn decode_schema(decoder: &mut FlightDataDecoder) -> Result<SchemaRef, Status> {
    while let Some(msg) = decoder.try_next().await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(schema) => {
                return Ok(schema);
            }
            DecodedPayload::RecordBatch(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must have a known schema",
                ));
            }
        }
    }
    Err(Status::invalid_argument(
        "parameter flight data must have a schema",
    ))
}

pub(super) fn decode_param_values(
    parameters: Option<&[u8]>,
) -> std::result::Result<Option<ParamValues>, ArrowError> {
    parameters
        .map(|parameters| {
            let decoder = StreamReader::try_new(parameters, None)?;
            let schema = decoder.schema();
            let batches = decoder
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let batch = concat_batches(&schema, batches.iter())?;
            Ok(record_to_param_values(&batch)?)
        })
        .transpose()
}

fn record_to_param_values(
    batch: &RecordBatch,
) -> std::result::Result<ParamValues, DataFusionError> {
    let mut param_values: Vec<(String, Option<usize>, ScalarValue)> = Vec::new();
    let mut is_list = true;
    for col_index in 0..batch.num_columns() {
        let array = batch.column(col_index);
        let scalar = ScalarValue::try_from_array(array, 0)?;
        let name = batch
            .schema_ref()
            .field(col_index)
            .name()
            .trim_start_matches('$')
            .to_string();
        let index = name.parse().ok();
        is_list &= index.is_some();
        param_values.push((name, index, scalar));
    }
    if is_list {
        let mut values: Vec<(Option<usize>, ScalarValue)> = param_values
            .into_iter()
            .map(|(_name, index, value)| (index, value))
            .collect();
        values.sort_by_key(|(index, _value)| *index);
        Ok(values
            .into_iter()
            .map(|(_index, value)| value)
            .collect::<Vec<ScalarValue>>()
            .into())
    } else {
        Ok(param_values
            .into_iter()
            .map(|(name, _index, value)| (name, value))
            .collect::<Vec<(String, ScalarValue)>>()
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::FlightData;
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use datafusion::arrow::datatypes::{DataType, Schema};
    use futures::TryStreamExt;

    #[test]
    fn reconcile_returns_none_when_nullability_agrees() {
        // Identical schemas (and any schema vs itself) need no reconciliation —
        // the DoGet hot path encodes the stream untouched.
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, false),
        ]);
        assert!(reconcile_stream_to_advertised(&schema, &schema).is_none());
    }

    #[test]
    fn reconcile_tightens_stream_to_advertised_nullability() {
        // The CHA-402 shape: the physical stream over-marks a COUNT column
        // nullable; the advertised (logical) schema has it non-null. Reconcile to
        // the advertised nullability while keeping the stream's field type.
        let stream = Schema::new(vec![
            Field::new("aid", DataType::Int64, false),
            Field::new("my_txns", DataType::Int64, true),
        ]);
        let advertised = Schema::new(vec![
            Field::new("aid", DataType::Int64, false),
            Field::new("my_txns", DataType::Int64, false),
        ]);
        let target = reconcile_stream_to_advertised(&stream, &advertised)
            .expect("nullability diverges, so a reconciled schema is returned");
        assert!(
            !target.field(1).is_nullable(),
            "my_txns tightened to non-null"
        );
        assert_eq!(
            target.field(1).data_type(),
            &DataType::Int64,
            "field type comes from the stream, untouched"
        );
        assert!(!target.field(0).is_nullable(), "agreeing field unchanged");
    }

    #[test]
    fn reconcile_returns_none_on_field_count_mismatch() {
        // Not a nullability mismatch — leave it to the encode/ADBC to surface.
        let one = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let two = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, false),
        ]);
        assert!(reconcile_stream_to_advertised(&one, &two).is_none());
    }

    /// `decode_params_from_stream` returns `None` when the stream's
    /// only payload is a zero-field schema (the wire shape the Apache
    /// flight-sql-jdbc-driver sends for non-prepared
    /// `Statement.execute(...)` — `VectorSchemaRoot.of(new
    /// FieldVector[0])`). Load-bearing for the existing
    /// non-parameterized callers: if an upstream `arrow-flight`
    /// version bump changed `FlightDataDecoder` semantics around the
    /// empty-schema arm, the bug would surface as
    /// `Status::unimplemented` from `gateway::execute_update`
    /// rejecting an unwanted-but-non-empty params payload.
    #[tokio::test]
    async fn decode_params_from_stream_returns_none_for_empty_schema() {
        // Build a FlightData payload carrying just a zero-field
        // schema. FlightDataEncoderBuilder emits one Schema message
        // for an empty input stream when `.with_schema(...)` is set.
        let empty_schema = Arc::new(Schema::empty());
        let flight_data: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .with_schema(empty_schema)
            .build(futures::stream::iter([]))
            .try_collect()
            .await
            .expect("encoder should produce schema-only stream");
        assert!(
            !flight_data.is_empty(),
            "encoder should emit at least one FlightData message"
        );

        let stream = futures::stream::iter(
            flight_data
                .into_iter()
                .map(Ok::<FlightData, Status>)
                .collect::<Vec<_>>(),
        );
        let out = decode_params_from_stream(stream).await.expect("decode ok");
        assert!(
            out.is_none(),
            "empty-schema stream must yield None, got {out:?}"
        );
    }
}
