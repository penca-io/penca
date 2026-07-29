//! Canonical PK-batch decode / validate / derive kernel.
//!
//! One Arrow IPC wire shape is shared by `Change.deletes` (write path)
//! and `ReadDataRequest.ids` / `AuditDataRequest.ids` (read paths): a
//! record batch carrying **exactly** the table's declared primary-key
//! columns, in declared order. The server derives `row_uuid` per row
//! via [`naming::row_uuid_for_pk`] — clients never compute row
//! identity. Keeping the validation + derivation here, in one module,
//! is what makes write-side and read-side row identity agree by
//! construction.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use penca_core::naming;
use uuid::Uuid;

use crate::error::ApiError;

/// Validate that `batch` carries exactly the table's declared
/// primary-key columns, in declared order, with the declared Arrow
/// types, and **no null values**.
///
/// Strict on column **name/order** and **data type**: a mismatch
/// would either mint a `row_uuid` that disagrees with the upsert side
/// — silently missing rows — or surface as a cryptic engine error
/// downstream; catching it here gives a clear caller-visible
/// `InvalidRequest`. Nulls are rejected because a NULL primary key
/// cannot derive a row identity — arrow's display formatting would
/// silently render it as the empty string, colliding with a
/// genuinely-empty-string key.
///
/// `field` names the request field the batch came from
/// (`"deletes"` / `"ids"`) so the error attributes the bad input.
fn validate_pk_batch_schema(
    batch: &RecordBatch,
    user_schema: &SchemaRef,
    primary_keys: &[String],
    field: &str,
) -> Result<(), ApiError> {
    let pk_cols: Vec<&str> = primary_keys.iter().map(String::as_str).collect();
    let schema = batch.schema();
    let batch_cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    if batch_cols != pk_cols {
        return Err(ApiError::InvalidRequest(format!(
            "{field} PK batch column order {batch_cols:?} does not match \
             table's declared primary_keys {pk_cols:?}"
        )));
    }

    for (pk, batch_field) in primary_keys.iter().zip(schema.fields().iter()) {
        let declared_type = user_schema
            .field_with_name(pk)
            .map_err(|_| {
                ApiError::InvalidRequest(format!("primary key '{pk}' not in user_schema"))
            })?
            .data_type();
        if batch_field.data_type() != declared_type {
            return Err(ApiError::InvalidRequest(format!(
                "{field} PK batch column '{pk}' has type {:?}, table declared {:?}",
                batch_field.data_type(),
                declared_type,
            )));
        }
    }

    let pk_columns: Vec<&dyn arrow::array::Array> =
        batch.columns().iter().map(|c| c.as_ref()).collect();
    ensure_no_null_pks(&pk_columns, primary_keys, field)?;

    Ok(())
}

/// Reject null values in PK columns — a NULL renders as `""` under
/// arrow display and would mint a row identity colliding with a
/// genuinely-empty-string key.
pub(crate) fn ensure_no_null_pks(
    pk_columns: &[&dyn arrow::array::Array],
    primary_keys: &[String],
    field: &str,
) -> Result<(), ApiError> {
    for (pk, column) in primary_keys.iter().zip(pk_columns.iter()) {
        if column.null_count() > 0 {
            return Err(ApiError::InvalidRequest(format!(
                "{field} PK column '{pk}' contains null values; \
                 primary keys cannot be null"
            )));
        }
    }

    Ok(())
}

/// Derive one `row_uuid` per batch row from all batch columns, in row
/// order. Private on purpose: deriving from an unvalidated batch would
/// silently mint identities that match nothing — go through
/// [`validated_row_uuids_from_batch`].
fn derive_row_uuids(batch: &RecordBatch, table_uuid: &Uuid) -> Result<Vec<Uuid>, ApiError> {
    let pk_columns: Vec<&dyn arrow::array::Array> =
        batch.columns().iter().map(|c| c.as_ref()).collect();
    let mut row_uuids: Vec<Uuid> = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        row_uuids.push(row_uuid_for_row(&pk_columns, row_idx, table_uuid)?);
    }

    Ok(row_uuids)
}

/// Derive the `row_uuid` for one row from the given PK column arrays
/// (in declared PK order) — THE row-identity kernel. Every server-side
/// derivation (write upserts, write deletes, read ids) goes through
/// this one stringify-then-hash step so the paths cannot diverge.
pub(crate) fn row_uuid_for_row(
    pk_columns: &[&dyn arrow::array::Array],
    row_idx: usize,
    table_uuid: &Uuid,
) -> Result<Uuid, ApiError> {
    let pk_values: Vec<String> = pk_columns
        .iter()
        .map(|column| {
            arrow::util::display::array_value_to_string(*column, row_idx).map_err(ApiError::Arrow)
        })
        .collect::<Result<_, _>>()?;
    let pk_refs: Vec<&str> = pk_values.iter().map(String::as_str).collect();
    Ok(naming::row_uuid_for_pk(table_uuid, &pk_refs))
}

/// The `ids` request-field gate shared by `read_data` and `plan_audit`: empty
/// bytes = no restriction (`None`), anything else decodes through
/// [`row_uuids_from_pk_ipc`]. Ids-specific by construction — the write path's
/// `deletes` field has no empty-means-unrestricted semantics.
pub(crate) fn optional_row_uuids(
    ids_bytes: &[u8],
    table_uuid: &Uuid,
    user_schema: &SchemaRef,
    primary_keys: &[String],
) -> Result<Option<Vec<Uuid>>, ApiError> {
    if ids_bytes.is_empty() {
        return Ok(None);
    }

    Ok(Some(row_uuids_from_pk_ipc(
        ids_bytes,
        table_uuid,
        user_schema,
        primary_keys,
        "ids",
    )?))
}

/// Validate `batch` against the table's declared primary keys and
/// derive one `row_uuid` per row — the single entry point both the
/// write path (`Change.deletes`) and the IPC decode below go through,
/// so validation can never be skipped ahead of derivation.
pub(crate) fn validated_row_uuids_from_batch(
    batch: &RecordBatch,
    table_uuid: &Uuid,
    user_schema: &SchemaRef,
    primary_keys: &[String],
    field: &str,
) -> Result<Vec<Uuid>, ApiError> {
    validate_pk_batch_schema(batch, user_schema, primary_keys, field)?;
    derive_row_uuids(batch, table_uuid)
}

/// Decode an Arrow IPC payload of PK batches, validate each batch, and
/// derive the flattened `row_uuid` set via
/// [`validated_row_uuids_from_batch`].
///
/// The caller gates on non-empty bytes (empty = no restriction, the
/// proto contract); a well-formed payload that nets **zero rows** is
/// rejected rather than silently treated as unrestricted — on a read
/// restriction that inversion would return the whole table to a caller
/// who named zero keys.
pub(crate) fn row_uuids_from_pk_ipc(
    ipc_bytes: &[u8],
    table_uuid: &Uuid,
    user_schema: &SchemaRef,
    primary_keys: &[String],
    field: &str,
) -> Result<Vec<Uuid>, ApiError> {
    let cursor = std::io::Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        ApiError::InvalidRequest(format!("{field} is not a valid Arrow IPC stream: {e}"))
    })?;

    let mut row_uuids: Vec<Uuid> = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            ApiError::InvalidRequest(format!("{field} Arrow IPC decode failed: {e}"))
        })?;
        if batch.num_rows() == 0 {
            continue;
        }

        row_uuids.extend(validated_row_uuids_from_batch(
            &batch,
            table_uuid,
            user_schema,
            primary_keys,
            field,
        )?);
    }

    if row_uuids.is_empty() {
        return Err(ApiError::InvalidRequest(format!(
            "{field} batch contains no rows; omit the field for an unrestricted read"
        )));
    }

    Ok(row_uuids)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;

    use super::*;

    fn user_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn pk_batch(names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names.to_vec()))]).unwrap()
    }

    fn ipc(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        buf
    }

    const TBL: Uuid = Uuid::from_u128(0x1234);

    #[test]
    fn derives_same_row_uuid_as_write_path() {
        // The whole point of the shared kernel: the derived uuid equals a
        // direct `row_uuid_for_pk` over the same values.
        let uuids = row_uuids_from_pk_ipc(
            &ipc(&pk_batch(&["alice"])),
            &TBL,
            &user_schema(),
            &["name".to_string()],
            "ids",
        )
        .unwrap();
        assert_eq!(uuids, vec![naming::row_uuid_for_pk(&TBL, &["alice"])]);
    }

    #[test]
    fn rejects_wrong_column_order_and_type() {
        let wrong_col = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let err = row_uuids_from_pk_ipc(
            &ipc(&wrong_col),
            &TBL,
            &user_schema(),
            &["name".to_string()],
            "ids",
        )
        .unwrap_err();
        assert!(matches!(err, ApiError::InvalidRequest(_)), "got {err:?}");

        let wrong_type = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "name",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let err = row_uuids_from_pk_ipc(
            &ipc(&wrong_type),
            &TBL,
            &user_schema(),
            &["name".to_string()],
            "ids",
        )
        .unwrap_err();
        assert!(matches!(err, ApiError::InvalidRequest(_)), "got {err:?}");
    }

    #[test]
    fn rejects_garbage_and_zero_row_payloads() {
        let err = row_uuids_from_pk_ipc(
            b"not arrow ipc",
            &TBL,
            &user_schema(),
            &["name".to_string()],
            "ids",
        )
        .unwrap_err();
        assert!(matches!(err, ApiError::InvalidRequest(_)), "got {err:?}");

        let err = row_uuids_from_pk_ipc(
            &ipc(&pk_batch(&[])),
            &TBL,
            &user_schema(),
            &["name".to_string()],
            "ids",
        )
        .unwrap_err();
        assert!(
            matches!(&err, ApiError::InvalidRequest(m) if m.contains("no rows")),
            "got {err:?}",
        );
    }

    #[test]
    fn composite_pk_parity_and_order_rejection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let pks = vec!["region".to_string(), "name".to_string()];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["eu"])),
                Arc::new(StringArray::from(vec!["alice"])),
            ],
        )
        .unwrap();

        // Derivation parity with a direct row_uuid_for_pk in declared order.
        let uuids = row_uuids_from_pk_ipc(&ipc_for(&batch), &TBL, &schema, &pks, "ids").unwrap();
        assert_eq!(uuids, vec![naming::row_uuid_for_pk(&TBL, &["eu", "alice"])]);

        // Swapped column order is rejected, not silently re-hashed.
        let swapped = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(StringArray::from(vec!["eu"])),
            ],
        )
        .unwrap();
        let err =
            row_uuids_from_pk_ipc(&ipc_for(&swapped), &TBL, &schema, &pks, "ids").unwrap_err();
        assert!(
            matches!(&err, ApiError::InvalidRequest(m) if m.contains("column order")),
            "got {err:?}",
        );
    }

    #[test]
    fn multi_batch_stream_flattens_in_order() {
        let b1 = pk_batch(&["alice", "bob"]);
        let b2 = pk_batch(&["carol"]);
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &b1.schema()).unwrap();
        writer.write(&b1).unwrap();
        writer.write(&b2).unwrap();
        writer.finish().unwrap();

        let uuids = row_uuids_from_pk_ipc(&buf, &TBL, &user_schema(), &["name".to_string()], "ids")
            .unwrap();
        assert_eq!(
            uuids,
            vec![
                naming::row_uuid_for_pk(&TBL, &["alice"]),
                naming::row_uuid_for_pk(&TBL, &["bob"]),
                naming::row_uuid_for_pk(&TBL, &["carol"]),
            ],
        );
    }

    #[test]
    fn rejects_null_pk_values() {
        // A NULL primary key cannot derive a row identity — arrow display
        // would render it as "" and collide with an empty-string key.
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("alice"), None]))],
        )
        .unwrap();
        let err = row_uuids_from_pk_ipc(
            &ipc_for(&batch),
            &TBL,
            &user_schema(),
            &["name".to_string()],
            "ids",
        )
        .unwrap_err();
        assert!(
            matches!(&err, ApiError::InvalidRequest(m) if m.contains("null")),
            "got {err:?}",
        );
    }

    /// IPC-encode an arbitrary batch (the `ipc` fixture is fixed to the
    /// single-PK schema).
    fn ipc_for(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        buf
    }

    #[test]
    fn optional_row_uuids_empty_bytes_is_unrestricted() {
        let got = optional_row_uuids(&[], &TBL, &user_schema(), &["name".to_string()]).unwrap();
        assert_eq!(got, None);
    }

    #[test]
    fn optional_row_uuids_decodes_non_empty_payload() {
        let got = optional_row_uuids(
            &ipc(&pk_batch(&["alice"])),
            &TBL,
            &user_schema(),
            &["name".to_string()],
        )
        .unwrap();
        assert_eq!(got, Some(vec![naming::row_uuid_for_pk(&TBL, &["alice"])]));
    }
}
