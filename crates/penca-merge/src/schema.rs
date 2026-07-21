//! Arrow schemas the merge-on-read pipeline reads and writes.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use penca_dl::schema::LogSchemas;

use crate::MergeError;

/// Output schema for the merge stream: `row_uuid` + user columns.
pub fn snapshot_read_schema(user_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new("row_uuid", DataType::Utf8, false))];
    fields.extend(user_schema.fields().iter().cloned());
    Arc::new(Schema::new(fields))
}

/// Schema of a resolved row (Query A output):
/// `row_uuid, <user_cols>, commit_micros, is_delete`.
///
/// CHA-368: the resolve now returns the latest committed version per
/// `row_uuid` across BOTH logs — visible upserts (`is_delete = false`, user
/// cols carry values) and winning tombstones (`is_delete = true`, user cols
/// NULL). The full `row_uuid` set of this batch IS the exclusion set (it
/// replaces the retired Query-B probe); the `is_delete = false` subset is the
/// live rows the merge emits.
pub(crate) fn resolved_schema(user_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new("row_uuid", DataType::Utf8, false))];
    fields.extend(user_schema.fields().iter().cloned());
    fields.push(Arc::new(Field::new(
        "commit_micros",
        DataType::Int64,
        false,
    )));
    fields.push(Arc::new(Field::new("is_delete", DataType::Boolean, false)));
    Arc::new(Schema::new(fields))
}

/// On-disk schemas for the two cold log tables (CHA-218).
///
/// Must match what persist writes to cold storage — declaring extra
/// fields here makes DataFusion fail with "Field X not found" when it
/// tries to project them off the on-disk files.
///
/// `upsert` carries `(row_uuid, <user_cols>, <tx-metadata tail>)`; `delete`
/// carries `(row_uuid, <pk_cols>, <tx-metadata tail>)` (CHA-185), where the tail
/// is [`cold_tx_metadata_fields`].
///
/// Per-tx framing (tx_uuid, begin/abort/commit_tx_log) is hot-only. Cold rows
/// pre-join the timestamp/seq tx-metadata columns from `commit_tx_log` at
/// persist time so the cold side reads as a near-pure scan. CHA-507 stopped
/// denormalizing `author`/`comment` onto cold rows — they live once in the
/// durable cold `tx_log` and are reattached on demand by `audit_data`. See
/// `docs/decisions/0017-cold-data-segments-pre-joined-tx-metadata.md` and
/// `docs/decisions/0030-cold-commit-tx-log-and-audit-join.md`.
pub(crate) fn cold_persist_schemas(user_schema: &SchemaRef) -> LogSchemas {
    LogSchemas {
        upsert: cold_upsert_schema_for_merge(user_schema),
        delete: cold_delete_schema_for_merge(),
    }
}

/// Merge-path cold upsert log schema: `row_uuid + <user_cols> +
/// (commit_micros, write_seq_num, commit_seq_num)` — exactly the columns
/// `build_cold_merge_resolved` selects from the upsert log
/// ([`crate::sql::build_cold_merge_resolved`]).
///
/// The cold upsert Parquet file is wider on disk (carries
/// `began_at_micros`, `comment`, `author` for the audit path); the
/// declared subset relies on DataFusion's column pruning to skip the
/// unread chunks. Symmetric with [`cold_delete_schema_for_merge`] —
/// both views narrow the merge surface to what the SQL touches, so
/// neither over-pulls audit-only columns on the hot read path. The
/// audit-path [`cold_upsert_schema`] retains the full on-disk shape
/// for `cold_upsert_audit_batches` + the compact rewriter.
pub(crate) fn cold_upsert_schema_for_merge(user_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new("row_uuid", DataType::Utf8, false))];
    fields.extend(user_schema.fields().iter().cloned());
    fields.push(Arc::new(Field::new(
        "commit_micros",
        DataType::Int64,
        false,
    )));
    // CHA-431: the merge orders by `(commit_seq_num, write_seq_num)`; both are
    // declared so DataFusion projects them off the wider cold file.
    fields.push(Arc::new(Field::new(
        "write_seq_num",
        DataType::Int64,
        false,
    )));
    fields.push(Arc::new(Field::new(
        "commit_seq_num",
        DataType::Int64,
        false,
    )));
    Arc::new(Schema::new(fields))
}

/// Cold on-disk upsert log schema (CHA-218, CHA-431), audit-path view:
/// `row_uuid + <user_cols> + write_seq_num + (committed_at, began_at, comment, author)`.
///
/// CHA-431 carries `write_seq_num` (the within-tx mutation ordinal) in the
/// slot between `user_cols` and the tx metadata block. Persist's
/// `project_to_cold_layout` keeps the matching column from
/// `hot_upsert_read_schema` in the same position.
///
/// Only the audit + compact paths need this shape — see
/// [`cold_upsert_schema_for_merge`] for the merge-path subset that
/// drops the audit-only tx-metadata cols (`began_at_micros`,
/// `comment`, `author`).
pub fn cold_upsert_schema(user_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new("row_uuid", DataType::Utf8, false))];
    fields.extend(user_schema.fields().iter().cloned());
    // CHA-431: write_seq_num trails the user cols on the on-disk row; same
    // slot order as hot_upsert_read_schema.
    fields.push(Arc::new(Field::new(
        "write_seq_num",
        DataType::Int64,
        false,
    )));
    fields.extend(cold_tx_metadata_fields());
    Arc::new(Schema::new(fields))
}

/// Merge-path cold delete log schema: just the columns the cold-side
/// merge SQL actually references — `row_uuid` for the join identity
/// plus the `(commit_seq_num, write_seq_num)` composite tiebreaker used by
/// the latest/deletes CTEs
/// ([`crate::sql::build_cold_merge_resolved`]).
///
/// `write_seq_num` / `commit_seq_num` (CHA-431) are required because the merge
/// SQL selects them directly; DataFusion would fail to project a column
/// the declared schema doesn't include.
///
/// Distinct from the audit-path [`cold_delete_schema`] which also
/// surfaces PKs. Keeping the merge view PK-independent means a
/// `read_data` projection that excludes PKs no longer trips a
/// planning-time SchemaMismatch — the merge path has no PK dependency
/// to fail.
///
/// The cold delete Parquet file is wider than this on disk; the
/// declared subset relies on DataFusion's column pruning to skip the
/// unread chunks. Even if pruning ever regresses, the cap on overread
/// is bounded to "PKs + began_at + comment + author per cold row" —
/// merge stays correct because the SQL only names the declared cols.
pub(crate) fn cold_delete_schema_for_merge() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("row_uuid", DataType::Utf8, false),
        Field::new("commit_micros", DataType::Int64, false),
        // CHA-431: merge orders by (commit_seq_num, write_seq_num).
        Field::new("write_seq_num", DataType::Int64, false),
        Field::new("commit_seq_num", DataType::Int64, false),
    ]))
}

/// Cold on-disk delete log schema (CHA-185, CHA-431), audit-path view:
/// `row_uuid + <pk_cols> + write_seq_num + (committed_at, began_at, comment, author)`.
/// PK columns interleave between `row_uuid` and `write_seq_num` in
/// table-declared order; types are resolved from `user_schema` so the
/// cold delete segment renders PKs natively in `audit_data`.
///
/// Only the audit path needs this shape — see
/// [`cold_delete_schema_for_merge`] for the merge-path subset that
/// drops PKs.
///
/// `primary_keys` is structurally guaranteed to be a subset of
/// `user_schema` (validated at table creation by
/// `PgDialect::create_data_tables`). The fallible signature mirrors the
/// equivalent check on the hot side (`audit_delete_schema`,
/// `read_committed_deletes`) and surfaces a programmer error if the
/// invariant is ever violated — a panic in library code wouldn't.
pub fn cold_delete_schema(
    user_schema: &SchemaRef,
    primary_keys: &[String],
) -> Result<SchemaRef, MergeError> {
    let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new("row_uuid", DataType::Utf8, false))];
    for pk in primary_keys {
        let field = user_schema
            .field_with_name(pk)
            .map_err(|_| MergeError::SchemaMismatch { pk: pk.clone() })?
            .clone();
        fields.push(Arc::new(field));
    }
    // CHA-431: write_seq_num trails the pk cols on the on-disk row.
    fields.push(Arc::new(Field::new(
        "write_seq_num",
        DataType::Int64,
        false,
    )));
    fields.extend(cold_tx_metadata_fields());
    Ok(Arc::new(Schema::new(fields)))
}

/// Denormalized tx metadata columns appended to every cold upsert/delete row
/// at persist time (CHA-218), minus `author`/`comment`.
///
/// CHA-507: `author`/`comment` are no longer denormalized onto cold data rows.
/// They live once per tx in the cold `tx_log` and are reattached on demand by
/// `audit_data`'s `commit_seq_num` join (pay-for-what-you-use), so only the
/// commit-order + wall-clock axes stay inline. `commit_seq_num` (CHA-430) still
/// trails; its trailing position must match the hot-side JOIN tail
/// ([`penca_storage_hot`]'s `joined_tx_metadata_fields`) exactly — projected
/// position-for-position, so a divergence fails DataFusion projection. Only the
/// audit-path views ([`cold_upsert_schema`], [`cold_delete_schema`]) carry this.
fn cold_tx_metadata_fields() -> Vec<Arc<Field>> {
    vec![
        Arc::new(Field::new("commit_micros", DataType::Int64, false)),
        Arc::new(Field::new("began_at_micros", DataType::Int64, false)),
        Arc::new(Field::new("commit_seq_num", DataType::Int64, false)),
    ]
}

/// The canonical denormalized tx-metadata tail (CHA-218 + CHA-430 + CHA-507),
/// `(name, type, nullable)` per column. The persist-side JOIN result tail
/// (`penca_storage_hot`'s `joined_tx_metadata_fields`) and the cold on-disk
/// tail ([`cold_tx_metadata_fields`]) must BOTH equal this — they are projected
/// position-for-position across the crate boundary, so any silent drift would
/// only surface as a runtime DataFusion projection error. CHA-507 dropped
/// `author`/`comment` from this tail (now joined from the cold tx_log).
#[cfg(test)]
pub(crate) const CANONICAL_TX_METADATA_TAIL: &[(&str, DataType, bool)] = &[
    ("commit_micros", DataType::Int64, false),
    ("began_at_micros", DataType::Int64, false),
    ("commit_seq_num", DataType::Int64, false),
];

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_tail_matches(fields: &[Arc<Field>]) {
        let actual: Vec<(&str, DataType, bool)> = fields
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();
        let expected: Vec<(&str, DataType, bool)> = CANONICAL_TX_METADATA_TAIL
            .iter()
            .map(|(n, t, nul)| (*n, t.clone(), *nul))
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn cold_tx_metadata_tail_is_canonical() {
        assert_tail_matches(&cold_tx_metadata_fields());
    }

    #[test]
    fn cold_upsert_schema_ends_with_canonical_tail() {
        let user = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let schema = cold_upsert_schema(&user);
        let tail = &schema.fields()[schema.fields().len() - CANONICAL_TX_METADATA_TAIL.len()..];
        assert_tail_matches(tail);
    }

    #[test]
    fn cold_delete_schema_ends_with_canonical_tail() {
        let user = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let schema = cold_delete_schema(&user, &["name".to_string()]).unwrap();
        let tail = &schema.fields()[schema.fields().len() - CANONICAL_TX_METADATA_TAIL.len()..];
        assert_tail_matches(tail);
    }

    /// CHA-507 (RED): author/comment move off cold data segments into the
    /// joined cold tx_log, so the audit-path cold schemas must no longer carry
    /// them — while the columns the cold audit + merge paths still need remain.
    /// Fails on `main` (the tail still carries author/comment); GREEN after
    /// IMPL-4 drops them from `cold_tx_metadata_fields`.
    #[test]
    fn cold_segment_schema_omits_author_comment() {
        let upsert_user = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let upsert = cold_upsert_schema(&upsert_user);
        let delete_user = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let delete = cold_delete_schema(&delete_user, &["name".to_string()]).unwrap();

        for schema in [&upsert, &delete] {
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert!(
                !names.contains(&"author") && !names.contains(&"comment"),
                "cold data segments must not carry author/comment (CHA-507 moves them \
                 to the joined cold tx_log); got {names:?}"
            );
            for required in ["commit_seq_num", "commit_micros", "began_at_micros"] {
                assert!(
                    names.contains(&required),
                    "cold audit/merge still needs {required}; got {names:?}"
                );
            }
        }
    }
}
