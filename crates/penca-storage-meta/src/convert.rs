//! Row → proto conversion helpers.
//!
//! Each function maps a named-column database row (`PgRow` for the
//! catalog/branch/commit_tx_log paths that still go through plain SQL, or a
//! `RecordBatch` row for sys-table reads that route through
//! `stream_merged`) to the corresponding proto message. Column names
//! must match the projection used in the query.

use arrow::array::{
    Array, BinaryArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
};
use penca_proto::external::v1::{Branch, Catalog, Index, RetentionConfig, Schema, Table};
use sqlx::Row;
use sqlx::postgres::PgRow;

/// Construct a [`Catalog`] from a `catalog_store` row.
///
/// Expected columns: `catalog_uuid`, `catalog_name`, `catalog_owner`,
/// `description`.
pub fn catalog_from_row(row: &PgRow) -> Catalog {
    let uuid: uuid::Uuid = row.get("catalog_uuid");
    Catalog {
        catalog_uuid: uuid.to_string(),
        catalog_name: row.get("catalog_name"),
        owner: row.get("catalog_owner"),
        description: row.get("description"),
    }
}

/// Construct a [`Branch`] from a per-catalog `branch_store` row.
///
/// Expected columns: `branch_uuid`, `branch_name`, `fork_commit_seq_num`.
/// `catalog_uuid` is supplied by the caller — branches are catalog-scoped and
/// the catalog is implicit in the branch_store table name.
pub fn branch_from_row(catalog_uuid: &str, row: &PgRow) -> Branch {
    let uuid: uuid::Uuid = row.get("branch_uuid");
    Branch {
        branch_uuid: uuid.to_string(),
        catalog_uuid: catalog_uuid.to_string(),
        branch_name: row.get("branch_name"),
        fork_commit_seq_num: row.get("fork_commit_seq_num"),
    }
}

//
// stream_merged returns sys-table reads as Arrow RecordBatches. The
// helpers below extract typed columns at row index `i` so callers
// don't repeat the downcast boilerplate at every site.

/// Extract a uuid-as-string from a row in `column`.
pub fn rb_uuid_str(batch: &RecordBatch, column: &str, row: usize) -> Option<String> {
    let col = batch.column_by_name(column)?;
    let arr = col.as_any().downcast_ref::<StringArray>()?;
    if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row).to_string())
    }
}

/// Extract a `Vec<u8>` from a Binary column.
pub fn rb_binary(batch: &RecordBatch, column: &str, row: usize) -> Option<Vec<u8>> {
    let col = batch.column_by_name(column)?;
    let arr = col.as_any().downcast_ref::<BinaryArray>()?;
    if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row).to_vec())
    }
}

/// Extract a `Vec<String>` from a `List<Utf8>` column. Returns empty
/// if the row is null or the column is missing.
pub fn rb_string_list(batch: &RecordBatch, column: &str, row: usize) -> Vec<String> {
    let Some(col) = batch.column_by_name(column) else {
        return Vec::new();
    };
    let Some(list) = col.as_any().downcast_ref::<ListArray>() else {
        return Vec::new();
    };
    if list.is_null(row) {
        return Vec::new();
    }
    let inner = list.value(row);
    let strings = match inner.as_any().downcast_ref::<StringArray>() {
        Some(s) => s,
        None => return Vec::new(),
    };
    (0..strings.len())
        .filter(|j| !strings.is_null(*j))
        .map(|j| strings.value(j).to_string())
        .collect()
}

/// Extract a Utf8 cell. Empty string if null.
pub fn rb_str(batch: &RecordBatch, column: &str, row: usize) -> String {
    let Some(col) = batch.column_by_name(column) else {
        return String::new();
    };
    let Some(arr) = col.as_any().downcast_ref::<StringArray>() else {
        return String::new();
    };
    if arr.is_null(row) {
        String::new()
    } else {
        arr.value(row).to_string()
    }
}

/// Extract an `Option<i32>` from an Int32 column.
pub fn rb_opt_i32(batch: &RecordBatch, column: &str, row: usize) -> Option<i32> {
    let col = batch.column_by_name(column)?;
    let arr = col.as_any().downcast_ref::<Int32Array>()?;
    if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    }
}

/// Extract an `Option<i64>` from an Int64 column.
pub fn rb_opt_i64(batch: &RecordBatch, column: &str, row: usize) -> Option<i64> {
    let col = batch.column_by_name(column)?;
    let arr = col.as_any().downcast_ref::<Int64Array>()?;
    if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    }
}

/// Extract the first non-null Binary cell from a `stream_merged`-produced
/// batch list under the named column. Returns `None` if no batch
/// contains a non-null value.
pub fn extract_first_binary(batches: &[RecordBatch], column: &str) -> Option<Vec<u8>> {
    for batch in batches {
        let col = batch.column_by_name(column)?;
        let arr = col.as_any().downcast_ref::<BinaryArray>()?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                return Some(arr.value(i).to_vec());
            }
        }
    }
    None
}

/// Extract the first non-null `List<Utf8>` cell as a `Vec<String>`.
/// Returns an empty vec if the column is missing or all rows are null
/// (matches today's `text_array` semantics for sys-table partition_keys
/// reads).
pub fn extract_first_string_list(batches: &[RecordBatch], column: &str) -> Vec<String> {
    for batch in batches {
        let Some(col) = batch.column_by_name(column) else {
            continue;
        };
        let Some(list) = col.as_any().downcast_ref::<ListArray>() else {
            continue;
        };
        for i in 0..list.len() {
            if list.is_null(i) {
                continue;
            }
            let inner = list.value(i);
            let strings = inner
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("partition_keys inner is utf8");
            return (0..strings.len())
                .filter(|j| !strings.is_null(*j))
                .map(|j| strings.value(j).to_string())
                .collect();
        }
    }
    Vec::new()
}

/// Build a [`RetentionConfig`] from a `stream_merged` RecordBatch row's
/// nullable retention columns (schema/table retention, read via the merge
/// path). Returns `None` when both columns are absent.
fn retention_config_from_record_batch(batch: &RecordBatch, row: usize) -> Option<RetentionConfig> {
    let retention_duration_seconds = rb_opt_i64(batch, "retention_duration_seconds", row);
    let snapshot_density_seconds = rb_opt_i64(batch, "snapshot_density_seconds", row);
    if retention_duration_seconds.is_none() && snapshot_density_seconds.is_none() {
        return None;
    }
    Some(RetentionConfig {
        retention_duration_seconds,
        snapshot_density_seconds,
    })
}

/// Build a [`Schema`] proto from a `stream_merged`-produced row of
/// `__penca_system__.schemas`. Reads Arrow columns directly;
/// retention is taken straight from the schema row (callers that
/// need catalog-coalesced retention compose the call after this
/// conversion).
pub fn schema_from_record_batch(catalog_uuid: &str, batch: &RecordBatch, row: usize) -> Schema {
    // schema_uuid is a first-class column, distinct from the row_uuid.
    let schema_uuid = rb_uuid_str(batch, "schema_uuid", row).unwrap_or_default();
    let retention_config = retention_config_from_record_batch(batch, row);

    Schema {
        schema_uuid,
        catalog_uuid: catalog_uuid.to_string(),
        schema_name: rb_str(batch, "schema_name", row),
        description: rb_str(batch, "description", row),
        default_retention_config: retention_config,
    }
}

/// Build a [`Table`] proto from a `stream_merged`-produced row of
/// `__penca_system__.tables`. Reads Arrow columns directly; retention
/// is taken straight from the table row (callers that need
/// catalog/schema coalesce do it after this conversion).
pub fn table_from_record_batch(
    catalog_uuid: &str,
    schema_uuid: &str,
    batch: &RecordBatch,
    row: usize,
) -> Table {
    // table_uuid is a first-class column, distinct from the row_uuid.
    let table_uuid = rb_uuid_str(batch, "table_uuid", row).unwrap_or_default();
    let arrow_schema = rb_binary(batch, "arrow_schema", row).unwrap_or_default();
    let retention_config = retention_config_from_record_batch(batch, row);

    Table {
        table_uuid,
        schema_uuid: schema_uuid.to_string(),
        catalog_uuid: catalog_uuid.to_string(),
        table_name: rb_str(batch, "table_name", row),
        arrow_schema,
        primary_keys: rb_string_list(batch, "primary_keys", row),
        partition_keys: rb_string_list(batch, "partition_keys", row),
        clustering_keys: rb_string_list(batch, "clustering_keys", row),
        description: rb_str(batch, "description", row),
        retention_config,
        // The defined-index set is a separate `__penca_system__.indexes`
        // read; the metadata-resolve caller attaches it (this row-converter only
        // sees the `tables` row).
        indexes: Vec::new(),
    }
}

/// Build an [`Index`] from a `__penca_system__.indexes` resolved row.
/// `index_uuid` is a first-class PK column, distinct from `row_uuid`; the
/// remaining columns are the
/// `system_indexes_arrow_schema` user columns. `index_type` is the
/// `IndexType` enum stored as its i32 value.
pub fn index_from_record_batch(batch: &RecordBatch, row: usize) -> Index {
    Index {
        index_uuid: rb_uuid_str(batch, "index_uuid", row).unwrap_or_default(),
        table_uuid: rb_uuid_str(batch, "table_uuid", row).unwrap_or_default(),
        index_name: rb_str(batch, "index_name", row),
        columns: rb_string_list(batch, "columns", row),
        index_type: rb_opt_i32(batch, "index_type", row).unwrap_or(0),
    }
}
