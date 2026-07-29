//! Covering-index seek selection — map a read's equality bindings onto the
//! user secondary indexes declared for the plan's snapshot, producing internal
//! `seeks` entries (never on the proto).
//!
//! Equalities arrive as structured `ReadDataRequest.indexes` wire tuples,
//! decoded and validated against the DEFINED index set by
//! [`decode_index_seek`]. [`select_from_bindings`] matches those bindings
//! against the snapshot's MATERIALIZED indexes:
//!
//! - a MATERIALIZED index (in `snapshot_plan.indexes`) becomes a seek entry —
//!   a pure accelerator riding the snapshot scan as *selection* (the residual
//!   still applies the exact predicate, ADR 0023);
//! - a DEFINED-but-unmaterialized index matches nothing here, so the read
//!   falls back to a merge scan with the residual (built in `decode_index_seek`).
//!
//! Under-selection is impossible by construction (a skip only ever leaves the
//! read on full-scan+filter) and over-selection is re-filtered.

use std::collections::{HashMap, HashSet};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::ipc::reader::StreamReader;
use penca_core::SnapshotPlan;
use penca_dl::driver::DlDriver;
use penca_merge::{IndexSeek, equality_bindings};
use penca_proto::external::v1::Index;
use uuid::Uuid;

use crate::error::ApiError;

/// The decoded structured `ReadDataRequest.indexes` seek: the per-column
/// equality bindings that feed [`select_from_bindings`], plus the SQL residual
/// carrying the same equality into the merge fallback (and the sole
/// restriction when the index is defined but not yet materialized).
#[derive(Debug)]
pub(crate) struct DecodedIndexSeek {
    pub(crate) bindings: HashMap<String, Vec<String>>,
    pub(crate) residual: String,
}

/// Decode + validate the wire `ReadDataRequest.indexes` seek batch, pre-plan.
/// The batch's columns are index-key columns carrying the equality values;
/// they may span the UNION of several covering indexes, and column ORDER is
/// not significant — `select_from_bindings` binds by name and orders each
/// probe by the index's own declared key columns.
/// Rejected with `InvalidRequest` when: the IPC is malformed; a column belongs
/// to NO defined index; a column's type disagrees with the table schema; a
/// value is null; or the batch nets zero rows. On success returns the equality
/// bindings (display-string values, the shape `select_from_bindings` matches)
/// and the SQL residual for the merge fallback.
pub(crate) fn decode_index_seek(
    indexes_bytes: &[u8],
    user_schema: &SchemaRef,
    defined_indexes: &[Index],
) -> Result<DecodedIndexSeek, ApiError> {
    let cursor = std::io::Cursor::new(indexes_bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        ApiError::InvalidRequest(format!("indexes is not a valid Arrow IPC stream: {e}"))
    })?;
    let mut batch: Option<RecordBatch> = None;
    for decoded in reader {
        let candidate = decoded.map_err(|e| {
            ApiError::InvalidRequest(format!("indexes Arrow IPC decode failed: {e}"))
        })?;
        if candidate.num_rows() == 0 {
            continue;
        }
        if batch.is_some() {
            return Err(ApiError::InvalidRequest(
                "indexes carries more than one record batch; the seek is a single batch".into(),
            ));
        }
        batch = Some(candidate);
    }
    let Some(batch) = batch else {
        return Err(ApiError::InvalidRequest(
            "indexes batch contains no rows; omit the field for an unrestricted read".into(),
        ));
    };

    let schema = batch.schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    // Every batch column must belong to SOME defined index. An exact
    // per-index column-set match would be too strict: the batch may be the
    // UNION of several covered indexes' key columns, and need reproduce
    // neither one index's exact set nor its key ORDER. Materialized-vs-not is
    // classified later; a column in no defined index is an undefined seek and
    // is rejected here, pre-plan.
    let defined_columns: HashSet<&str> = defined_indexes
        .iter()
        .flat_map(|ix| ix.columns.iter().map(String::as_str))
        .collect();
    for column in &columns {
        if !defined_columns.contains(column.as_str()) {
            return Err(ApiError::InvalidRequest(format!(
                "indexes references column '{column}' that is not a defined index key"
            )));
        }
    }
    // Types must match the table schema (a mismatch would seek the sidecar
    // against the wrong key type); nulls cannot form an equality key.
    let mut types: Vec<DataType> = Vec::with_capacity(columns.len());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let declared = user_schema
            .field_with_name(field.name())
            .map_err(|_| {
                ApiError::InvalidRequest(format!(
                    "indexes column '{}' not in table schema",
                    field.name()
                ))
            })?
            .data_type();
        if field.data_type() != declared {
            return Err(ApiError::InvalidRequest(format!(
                "indexes column '{}' has type {:?}, table declared {declared:?}",
                field.name(),
                field.data_type(),
            )));
        }
        if column.null_count() > 0 {
            return Err(ApiError::InvalidRequest(format!(
                "indexes column '{}' contains null values; a seek key cannot be null",
                field.name()
            )));
        }
        types.push(declared.clone());
    }

    let mut bindings: HashMap<String, Vec<String>> = HashMap::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let array = batch.column(idx).as_ref();
        let mut values = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            values.push(
                arrow::util::display::array_value_to_string(array, row).map_err(ApiError::Arrow)?,
            );
        }
        bindings.insert(column.clone(), values);
    }
    let residual = index_residual(&columns, &types, &batch)?;
    Ok(DecodedIndexSeek { bindings, residual })
}

/// The SQL residual equivalent of the seek: per row a parenthesized conjunction
/// over the key columns (`"col" = <literal>`), OR-joined across rows (IN-list
/// semantics). Applied in the merge fallback and — for a
/// defined-but-unmaterialized index — the only restriction, so it must always
/// be correct. Identifiers are double-quoted (`quote_ident`) and literals
/// type-formatted (`sql_literal`: numeric/bool bare, everything else
/// single-quoted) to match the quoted-identifier `filter` fragment the merge
/// already applies.
fn index_residual(
    columns: &[String],
    types: &[DataType],
    batch: &RecordBatch,
) -> Result<String, ApiError> {
    let mut clauses = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut conj = Vec::with_capacity(columns.len());
        for (idx, column) in columns.iter().enumerate() {
            let value =
                arrow::util::display::array_value_to_string(batch.column(idx).as_ref(), row)
                    .map_err(ApiError::Arrow)?;
            conj.push(format!(
                "{} = {}",
                quote_ident(column),
                sql_literal(&value, &types[idx])
            ));
        }
        clauses.push(format!("({})", conj.join(" AND ")));
    }
    Ok(clauses.join(" OR "))
}

/// Double-quote a SQL identifier (embedded `"` doubled) — the quoted-identifier
/// contract the merge filter expects. `exprs_to_where_fragment` emits
/// double-quoted identifiers under PostgreSqlDialect (penca-datafusion
/// `identifiers_are_quoted`), and this residual is concatenated into that SAME
/// filter string (via `combine_filters`), so an unquoted mixed-case or
/// reserved-word index column would case-fold to a "column not found" hard
/// error in the merge parser rather than the intended equality restriction.
fn quote_ident(column: &str) -> String {
    format!("\"{}\"", column.replace('"', "\"\""))
}

/// Format `value` as a SQL literal for the residual fragment. Bare ONLY for the
/// types whose arrow display is itself a valid bare SQL literal — integers,
/// floats, decimals, bool; everything else (strings, temporal, and atypical
/// binary/interval/duration key columns) is single-quoted (`'` doubled) so the
/// merge SQL planner coerces the quoted literal to the column type rather than
/// choking on a non-bare-safe display (`2024-01-01T00:00:00`, hex binary). The
/// quote-unknown default keeps the load-bearing residual (sole restriction for
/// an unmaterialized index) parseable for any key type.
fn sql_literal(value: &str, data_type: &DataType) -> String {
    let bare = matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Boolean
    );
    if bare {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

/// Select covering user indexes for `filter` by re-parsing the SQL fragment.
/// For callers that carry a `filter` but NO structured `indexes` batch (e.g. a
/// gRPC `ids`-restricted read with a covering filter) — the SQL query path
/// sends wire tuples and goes through [`decode_index_seek`] +
/// [`select_from_bindings`] instead. Empty when nothing covers.
/// `max_probe_tuples == 0` is the operator kill switch, honored before the
/// parse.
pub(crate) async fn select_index_seeks<L: DlDriver + ?Sized>(
    dl: &L,
    filter: &str,
    snapshot: &SnapshotPlan,
    user_schema: &SchemaRef,
    max_probe_tuples: usize,
) -> Vec<IndexSeek> {
    if max_probe_tuples == 0 {
        return Vec::new();
    }
    let bindings = equality_bindings(dl, filter, user_schema).await;
    let entries = select_from_bindings(&bindings, snapshot, max_probe_tuples);
    if !entries.is_empty() {
        // Scraped by tests/integration/integration_user_index_seek_test.py —
        // one event per read that selected covering indexes, entry count as a
        // bare-int field. The field names are a test contract.
        tracing::debug!(
            index_seek = true,
            index_seek_entries = entries.len(),
            "user index seek selected"
        );
    }

    entries
}

/// The pure matching policy: which declared indexes are fully bound, and their
/// probe tuples. Split out for direct unit testing (no session, no IO).
pub(crate) fn select_from_bindings(
    bindings: &HashMap<String, Vec<String>>,
    snapshot: &SnapshotPlan,
    max_probe_tuples: usize,
) -> Vec<IndexSeek> {
    if bindings.is_empty() {
        return Vec::new();
    }

    // TODO(CHA-491): emit-all is the decided v1 policy — every fully-bound
    // index becomes an entry (intersection only narrows; the residual
    // re-applies the predicate), deduped on identical key-column sets
    // keeping the lowest index_uuid. Subsumption pruning + selectivity
    // winner-picking are the profile-gated follow-up.
    let mut seen_key_sets: HashSet<Vec<String>> = HashSet::new();
    let mut entries: Vec<IndexSeek> = Vec::new();
    // `SnapshotPlan.indexes` arrives sorted by index_uuid, so the dedup
    // deterministically keeps the lowest uuid.
    for def in &snapshot.indexes {
        let mut sorted_keys = def.key_columns.clone();
        sorted_keys.sort();
        if seen_key_sets.contains(&sorted_keys) {
            continue;
        }

        let Some(tuples) = bind_index_tuples(&def.key_columns, bindings, max_probe_tuples) else {
            continue;
        };
        let Ok(index_uuid) = Uuid::parse_str(&def.index_uuid) else {
            tracing::debug!(
                index_uuid = %def.index_uuid,
                "index_select: malformed index_uuid on snapshot def; skipping"
            );
            continue;
        };
        seen_key_sets.insert(sorted_keys);
        entries.push(IndexSeek {
            index_uuid: Some(index_uuid),
            // The seek derives each sidecar key column's native type from
            // the table schema through these names (typed sidecars).
            key_columns: def.key_columns.clone(),
            tuples,
        });
    }
    entries
}

/// Probe tuples for one index: every key column must be bound, tuples are
/// the cartesian product of the per-column binding sets in key-column
/// (sort-priority) order. `None` when not covering or over the product cap.
fn bind_index_tuples(
    key_columns: &[String],
    bindings: &HashMap<String, Vec<String>>,
    max_probe_tuples: usize,
) -> Option<Vec<Vec<String>>> {
    let per_column: Vec<&Vec<String>> = key_columns
        .iter()
        .map(|column| bindings.get(column))
        .collect::<Option<_>>()?;
    let product: usize = per_column
        .iter()
        .try_fold(1usize, |acc, values| acc.checked_mul(values.len()))?;
    if product == 0 {
        return None;
    }
    if product > max_probe_tuples {
        tracing::debug!(
            product,
            cap = max_probe_tuples,
            "index_select: probe cartesian over cap; skipping index"
        );
        return None;
    }

    let mut tuples: Vec<Vec<String>> = vec![Vec::with_capacity(key_columns.len())];
    for values in per_column {
        let mut next = Vec::with_capacity(tuples.len() * values.len());
        for tuple in &tuples {
            for value in values {
                let mut extended = tuple.clone();
                extended.push(value.clone());
                next.push(extended);
            }
        }
        tuples = next;
    }

    Some(tuples)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use arrow::ipc::writer::StreamWriter;
    use penca_core::SnapshotIndexDef;

    use super::*;

    /// One-column Utf8 IPC seek batch, the wire shape `decode_index_seek` reads.
    fn utf8_batch(name: &str, values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))]).unwrap()
    }

    fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        buf
    }

    fn defined_index(columns: &[&str]) -> Index {
        Index {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            ..Default::default()
        }
    }

    fn schema_of(fields: &[(&str, DataType, bool)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt, nullable)| Field::new(*name, dt.clone(), *nullable))
                .collect::<Vec<_>>(),
        ))
    }

    const IDX_A: &str = "11111111-1111-1111-1111-111111111111";
    const IDX_B: &str = "22222222-2222-2222-2222-222222222222";
    /// Deployment-shaped cap for tests that aren't exercising the cap itself.
    const CAP: usize = 1024;

    fn snapshot_with(defs: &[(&str, &[&str])]) -> SnapshotPlan {
        SnapshotPlan {
            indexes: defs
                .iter()
                .map(|(uuid, columns)| SnapshotIndexDef {
                    index_uuid: uuid.to_string(),
                    key_columns: columns.iter().map(|c| c.to_string()).collect(),
                })
                .collect(),
            ..Default::default()
        }
    }

    fn bindings(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(column, values)| {
                (
                    column.to_string(),
                    values.iter().map(|v| v.to_string()).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn single_column_binding_selects() {
        let snapshot = snapshot_with(&[(IDX_A, &["city"])]);
        let entries = select_from_bindings(&bindings(&[("city", &["paris"])]), &snapshot, CAP);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index_uuid, Some(Uuid::parse_str(IDX_A).unwrap()));
        assert_eq!(entries[0].tuples, vec![vec!["paris".to_string()]]);
    }

    #[test]
    fn composite_binds_in_key_column_order() {
        let snapshot = snapshot_with(&[(IDX_A, &["city", "tier"])]);
        // Binding-map order is irrelevant; tuples follow key-column order.
        let entries = select_from_bindings(
            &bindings(&[("tier", &["gold"]), ("city", &["paris"])]),
            &snapshot,
            CAP,
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].tuples,
            vec![vec!["paris".to_string(), "gold".to_string()]]
        );
    }

    #[test]
    fn composite_prefix_binding_does_not_select() {
        let snapshot = snapshot_with(&[(IDX_A, &["city", "tier"])]);
        assert!(
            select_from_bindings(&bindings(&[("city", &["paris"])]), &snapshot, CAP).is_empty(),
            "v1 has no prefix scan — every key column must bind",
        );
    }

    #[test]
    fn in_list_cartesian_orders_probe_tuples() {
        let snapshot = snapshot_with(&[(IDX_A, &["city", "tier"])]);
        let entries = select_from_bindings(
            &bindings(&[("city", &["paris", "oslo"]), ("tier", &["gold"])]),
            &snapshot,
            CAP,
        );
        assert_eq!(
            entries[0].tuples,
            vec![
                vec!["paris".to_string(), "gold".to_string()],
                vec!["oslo".to_string(), "gold".to_string()],
            ]
        );
    }

    #[test]
    fn multiple_covering_indexes_emit_all_in_def_order() {
        let snapshot = snapshot_with(&[(IDX_A, &["city"]), (IDX_B, &["tier"])]);
        let entries = select_from_bindings(
            &bindings(&[("city", &["paris"]), ("tier", &["gold"])]),
            &snapshot,
            CAP,
        );
        assert_eq!(entries.len(), 2, "emit-all v1 policy (TODO(CHA-491))");
        assert_eq!(entries[0].index_uuid, Some(Uuid::parse_str(IDX_A).unwrap()));
        assert_eq!(entries[1].index_uuid, Some(Uuid::parse_str(IDX_B).unwrap()));
    }

    #[test]
    fn identical_key_sets_dedupe_to_first_def() {
        // SnapshotPlan.indexes arrives sorted by index_uuid, so "first def"
        // IS the lowest uuid.
        let snapshot = snapshot_with(&[(IDX_A, &["city"]), (IDX_B, &["city"])]);
        let entries = select_from_bindings(&bindings(&[("city", &["paris"])]), &snapshot, CAP);
        assert_eq!(entries.len(), 1, "duplicate key-column sets collapse");
        assert_eq!(entries[0].index_uuid, Some(Uuid::parse_str(IDX_A).unwrap()));
    }

    #[test]
    fn cartesian_over_cap_skips_index() {
        let snapshot = snapshot_with(&[(IDX_A, &["city", "tier"])]);
        let many: Vec<String> = (0..40).map(|i| format!("v{i}")).collect();
        let many_refs: Vec<&str> = many.iter().map(String::as_str).collect();
        // 40 x 40 = 1600 > the deployment-shaped 1024 cap.
        let entries = select_from_bindings(
            &bindings(&[("city", &many_refs), ("tier", &many_refs)]),
            &snapshot,
            CAP,
        );
        assert!(
            entries.is_empty(),
            "over-cap cartesian must skip, not truncate"
        );
    }

    #[test]
    fn zero_cap_is_a_kill_switch() {
        // `QUERY_INDEX_SEEK_MAX_PROBE_TUPLES=0`: every product (>= 1)
        // exceeds the cap, so no index is ever selected.
        let snapshot = snapshot_with(&[(IDX_A, &["city"])]);
        let entries = select_from_bindings(&bindings(&[("city", &["paris"])]), &snapshot, 0);
        assert!(entries.is_empty(), "cap 0 must disable index selection");
    }

    #[test]
    fn unbound_column_set_selects_nothing() {
        let snapshot = snapshot_with(&[(IDX_A, &["city"])]);
        assert!(select_from_bindings(&HashMap::new(), &snapshot, CAP).is_empty());
    }

    #[test]
    fn sql_literal_quotes_and_escapes_strings() {
        assert_eq!(sql_literal("al'ice", &DataType::Utf8), "'al''ice'");
    }

    #[test]
    fn sql_literal_emits_numbers_bare() {
        assert_eq!(sql_literal("42", &DataType::Int64), "42");
        assert_eq!(sql_literal("true", &DataType::Boolean), "true");
    }

    #[test]
    fn sql_literal_quotes_temporal() {
        // A bare timestamp display would not parse; quoting lets the merge
        // planner coerce the string literal to the column's temporal type.
        assert_eq!(
            sql_literal(
                "2024-01-01T00:00:00",
                &DataType::Timestamp(TimeUnit::Microsecond, None)
            ),
            "'2024-01-01T00:00:00'"
        );
    }

    #[test]
    fn quote_ident_double_quotes_and_preserves_case() {
        assert_eq!(quote_ident("OrderId"), "\"OrderId\"");
    }

    #[test]
    fn quote_ident_doubles_embedded_quote() {
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }

    #[test]
    fn index_residual_quotes_identifiers_and_types_literals() {
        // Composite (Utf8 city, Int64 Tier): quoted identifiers, string literal
        // quoted, int literal bare — composes with the quoted-identifier merge
        // filter even for the mixed-case `Tier`.
        let batch = RecordBatch::try_new(
            schema_of(&[
                ("city", DataType::Utf8, false),
                ("Tier", DataType::Int64, false),
            ]),
            vec![
                Arc::new(StringArray::from(vec!["paris"])),
                Arc::new(Int64Array::from(vec![5])),
            ],
        )
        .unwrap();
        let residual = index_residual(
            &["city".to_string(), "Tier".to_string()],
            &[DataType::Utf8, DataType::Int64],
            &batch,
        )
        .unwrap();
        assert_eq!(residual, r#"("city" = 'paris' AND "Tier" = 5)"#);
    }

    #[test]
    fn index_residual_or_joins_multiple_rows() {
        let batch = RecordBatch::try_new(
            schema_of(&[("city", DataType::Utf8, false)]),
            vec![Arc::new(StringArray::from(vec!["paris", "oslo"]))],
        )
        .unwrap();
        let residual = index_residual(&["city".to_string()], &[DataType::Utf8], &batch).unwrap();
        assert_eq!(residual, r#"("city" = 'paris') OR ("city" = 'oslo')"#);
    }

    #[test]
    fn decode_index_seek_accepts_defined_columns() {
        let batch = utf8_batch("city", &["paris"]);
        let decoded = decode_index_seek(
            &ipc_bytes(&batch),
            &schema_of(&[("city", DataType::Utf8, false)]),
            &[defined_index(&["city"])],
        )
        .unwrap();
        assert_eq!(
            decoded.bindings.get("city").unwrap(),
            &vec!["paris".to_string()]
        );
        assert_eq!(decoded.residual, r#"("city" = 'paris')"#);
    }

    #[test]
    fn decode_index_seek_rejects_undefined_column() {
        let batch = utf8_batch("ghost", &["x"]);
        let err = decode_index_seek(
            &ipc_bytes(&batch),
            &schema_of(&[("ghost", DataType::Utf8, false)]),
            &[defined_index(&["city"])],
        )
        .unwrap_err();
        assert!(matches!(err, ApiError::InvalidRequest(_)), "{err:?}");
    }

    #[test]
    fn decode_index_seek_rejects_type_mismatch() {
        // Batch column is Int64; the table schema declares city Utf8.
        let batch = RecordBatch::try_new(
            schema_of(&[("city", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![5]))],
        )
        .unwrap();
        let err = decode_index_seek(
            &ipc_bytes(&batch),
            &schema_of(&[("city", DataType::Utf8, false)]),
            &[defined_index(&["city"])],
        )
        .unwrap_err();
        assert!(matches!(err, ApiError::InvalidRequest(_)), "{err:?}");
    }

    #[test]
    fn decode_index_seek_rejects_null_value() {
        let batch = RecordBatch::try_new(
            schema_of(&[("city", DataType::Utf8, true)]),
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
        )
        .unwrap();
        let err = decode_index_seek(
            &ipc_bytes(&batch),
            &schema_of(&[("city", DataType::Utf8, true)]),
            &[defined_index(&["city"])],
        )
        .unwrap_err();
        assert!(matches!(err, ApiError::InvalidRequest(_)), "{err:?}");
    }
}
