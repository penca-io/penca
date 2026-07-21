// -- Global tables (fixed names) ------------------------------------------

use uuid::Uuid;

use super::uuids::{
    row_uuid_for_pk, system_indexes_table_uuid, system_name_index_uuid, system_schemas_table_uuid,
    system_tables_table_uuid,
};
use crate::LogKind;

pub const CATALOG_STORE: &str = "catalog_store";
pub const MAIN_BRANCH_NAME: &str = "main";
pub const PUBLIC_SCHEMA_NAME: &str = "public";
/// Reserved schema for Penca-internal metadata exposed as first-class
/// tables (CHA-164 Stage C). User DDL/DML against this schema is
/// rejected at the API layer (CHA-236) — the three structural-anchor
/// helpers below name its identity for server-internal write paths.
/// Bootstrapped at CreateCatalog time alongside `public`.
pub const SYSTEM_SCHEMA_NAME: &str = "__penca_system__";

/// Names of Penca Tables auto-bootstrapped inside `__penca_system__`
/// at `CreateCatalog` time (CHA-177 / ADR 0012). Each is a real
/// auditable-store Penca Table — same `{prefix}_data_{upsert,delete}_log`
/// shape as user data, addressed via
/// [`upsert_log_table`] / [`delete_log_table`] given the
/// system `table_uuid` and the branch.
pub const SYSTEM_SCHEMAS_TABLE_NAME: &str = "schemas";
pub const SYSTEM_TABLES_TABLE_NAME: &str = "tables";
/// CHA-455: index definitions as a third dogfooded auditable Penca Table
/// under `__penca_system__`, alongside `schemas` / `tables`. Written by
/// `CreateIndex` / inline `CreateTable.indexes`; same
/// `{prefix}_data_{upsert,delete}_log` shape, addressed via
/// [`upsert_log_table`] / [`delete_log_table`] on the system index
/// `table_uuid` + branch.
pub const SYSTEM_INDEXES_TABLE_NAME: &str = "indexes";

/// The built-in composite **name index** declared on a `__penca_system__`
/// table (CHA-481, chunk B-sys). Pairs the deterministic non-NULL `index_uuid`
/// ([`system_name_index_uuid`]) the snapshot build records on the parent index
/// row with the ordered key columns the per-segment sidecar sorts on.
///
/// This is the *single* source of the system-table → `(index_uuid, key
/// columns)` mapping, shared by the build (CHA-481) and the by-name read seek
/// (CHA-484); neither side keeps its own copy. `key_columns` are the snapshot
/// batch's user-column names (the `pg.rs` `system_*_arrow_schema` columns, all
/// `Utf8`) in seek-priority order, and match the `meta_resolve` by-name
/// predicates 1:1.
pub struct SystemNameIndexSpec {
    pub index_uuid: Uuid,
    pub key_columns: &'static [&'static str],
}

/// Classify a snapshot target by `table_uuid`: `Some(spec)` when it is one of
/// the three `__penca_system__` tables that carry a built-in name index, else
/// `None` (every user table and any other system table). The snapshot build
/// (CHA-481) declares the name-index parent + composite sidecars only when this
/// returns `Some`; the by-name read path (CHA-484) calls the same classifier to
/// recompute the `index_uuid` and key columns it seeks on.
///
/// Keys: `schemas → [schema_name]` (unique under the catalog),
/// `tables → [schema_uuid, table_name]` (the name alone over-selects across
/// schemas), `indexes → [table_uuid, index_name]` (the name is unique only
/// within a table).
pub fn system_name_index_spec(
    catalog_uuid: &Uuid,
    table_uuid: &Uuid,
) -> Option<SystemNameIndexSpec> {
    let key_columns: &'static [&'static str] =
        if *table_uuid == system_schemas_table_uuid(catalog_uuid) {
            &["schema_name"]
        } else if *table_uuid == system_tables_table_uuid(catalog_uuid) {
            &["schema_uuid", "table_name"]
        } else if *table_uuid == system_indexes_table_uuid(catalog_uuid) {
            &["table_uuid", "index_name"]
        } else {
            return None;
        };

    Some(SystemNameIndexSpec {
        index_uuid: system_name_index_uuid(table_uuid),
        key_columns,
    })
}

// -- TX log family (fixed names) ------------------------------------------
//
// Fixed-suffix strings shared by each tx-log family helper pair
// (`get_*_table` and `get_*_partition`). Mirrors the persist/snapshot
// family convention (e.g. [`TABLE_PERSIST_METADATA`]).

pub const COMMIT_TX_LOG: &str = "commit_tx_log";
pub const BEGIN_TX_LOG: &str = "begin_tx_log";
pub const ABORT_TX_LOG: &str = "abort_tx_log";
pub const TX_TABLE_LOG: &str = "tx_table_log";
/// CHA-428: per-branch gapless commit-order counter for `commit_tx_log`. Exactly
/// one row per branch (`branch_uuid` PK) holding the next `commit_seq_num` to
/// assign; allocated at commit under a row lock held to tx-end (see
/// penca-storage-hot). Lives in the per-branch stack alongside the commit_tx_log
/// family — NOT on `branch_store` / the global control plane — because the
/// counter UPDATE must be in the same transaction as the `commit_tx_log` INSERT.
/// LIST-partitioned by branch_uuid. The per-data-table mutation counter
/// (`write_seq_num`) is a separate table introduced in CHA-431
/// (`write_seq_num`).
pub const COMMIT_TX_LOG_SEQ_NUM: &str = "commit_tx_log_seq_num";
/// CHA-444 (ADR 0027): per-branch abort-order counter. A **dedicated** counter
/// — not a sample of [`COMMIT_TX_LOG_SEQ_NUM`] — so `aborted_at_seq_num` is strictly
/// monotone in allocation order and the purge abort watermark `Pa` can never
/// falsely cover a later abort. Strictly monotone, **not** gapless: a
/// degenerate abort whose `begin_tx_log` row is absent can bump the counter
/// without an `abort_tx_log` row consuming the value — harmless, since `Pa` GC
/// needs only monotonicity (the correctness-relevant property), not gaplessness.
/// Implemented like the commit counter (locked counter row, incremented in the
/// abort INSERT).
pub const ABORT_SEQ_NUM: &str = "abort_seq_num";

// -- Segment metadata tables (fixed names) --------------------------------

pub const TABLE_PERSIST_METADATA: &str = "table_persist_metadata";
pub const TABLE_PERSIST_SEGMENT_METADATA: &str = "table_persist_segment_metadata";
/// CHA-507: per-catalog cold `tx_log` segment index — one row per cold
/// tx_log file, with `branch_uuid` a column (slim, low-volume, unpartitioned).
pub const TX_LOG_PERSIST_SEGMENT_METADATA: &str = "tx_log_persist_segment_metadata";
pub const TABLE_PURGE_METADATA: &str = "table_purge_metadata";
pub const TABLE_SNAPSHOT_METADATA: &str = "table_snapshot_metadata";
pub const TABLE_SNAPSHOT_SEGMENT_METADATA: &str = "table_snapshot_segment_metadata";
/// CHA-202: in-flight compact merged-file tracking, distinct from
/// `table_persist_segment_metadata`. Row inserted before the Phase-1 file
/// write, `commit_micros` flipped inside the Phase-2 tx. Phase-0
/// sweep at the top of compact reads NULL-committed rows filtered by
/// `(branch_uuid, table_uuid)` — same scope as the held advisory lock —
/// to recover orphans from prior crashes. Cross-branch / cross-table
/// compaction is structurally impossible because every helper that
/// touches this table pins both scope columns.
pub const COMPACT_SEGMENT_METADATA: &str = "compact_segment_metadata";
/// CHA-233 / ADR 0019 §"Four-part mechanism" item 3: grace-bounded
/// set of cold segment files queued for physical deletion. Compact
/// enqueues a row for each replaced old URI inside its merge tx;
/// `sweep_segments` reads rows whose `written_at_micros +
/// query_timeout < now` and deletes the file then the row. Holds
/// both persist-compact and snapshot-compact deferred deletes — no
/// `kind` discriminator since the sweep only cares about
/// `object_uri`.
pub const SEGMENT_DELETE_SET: &str = "segment_delete_set";
/// CHA-412 / ADR 0026 §5: the cold-index materialization metadata, split
/// into a snapshot parent/child pair mirroring `table_snapshot_metadata`
/// → `table_snapshot_segment_metadata`. This is the parent: one row per
/// `(snapshot, index)` — the snapshot "has" this index. `index_uuid IS
/// NULL` ⇒ the strictly-internal `row_uuid` identity index; non-NULL ⇒ a
/// *declared* index — either a built-in system-table name index (CHA-481, a
/// deterministic [`system_name_index_uuid`] that is itself never a
/// `__penca_system__.indexes` row) or a user secondary index (CHA-463). A
/// fileless header, re-declared fresh each snapshot.
pub const TABLE_SNAPSHOT_INDEX_METADATA: &str = "table_snapshot_index_metadata";
/// CHA-412 / ADR 0026 §5: the child of `table_snapshot_index_metadata` —
/// one row per `(segment, index)` sidecar, shaped like the segment-metadata
/// tables (an index sidecar is itself a cold file with its own two-phase
/// commit + `segment_delete_set` GC participation). Carries forward by
/// reference with its base segment.
pub const TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA: &str = "table_snapshot_segment_index_metadata";

// -- Per-catalog tables (UUID-prefixed) -----------------------------------

pub fn branch_store_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_branch_store")
}

pub fn begin_tx_log_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{BEGIN_TX_LOG}")
}

pub fn abort_tx_log_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{ABORT_TX_LOG}")
}

pub fn commit_tx_log_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{COMMIT_TX_LOG}")
}

pub fn tx_table_log_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{TX_TABLE_LOG}")
}

pub fn commit_tx_log_seq_num_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{COMMIT_TX_LOG_SEQ_NUM}")
}

pub fn abort_seq_num_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{ABORT_SEQ_NUM}")
}

/// Shared name prefix for a `(table, branch)`'s hot data objects — the
/// `upsert_log` / `delete_log` tables and the CHA-431 `write_sequence`.
/// They share one prefix so they live in the table's data-object namespace
/// and carry no `catalog_uuid`: `row_uuid_for_pk(table_uuid, [branch_uuid])`.
fn data_object_prefix(table_uuid: &Uuid, branch_uuid: &Uuid) -> Uuid {
    row_uuid_for_pk(table_uuid, &[&branch_uuid.to_string()])
}

fn data_log_table(table_uuid: &Uuid, branch_uuid: &Uuid, kind: LogKind) -> String {
    format!(
        "{prefix}_data_{kind}",
        prefix = data_object_prefix(table_uuid, branch_uuid),
    )
}

pub fn upsert_log_table(table_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    data_log_table(table_uuid, branch_uuid, LogKind::UpsertLog)
}

pub fn delete_log_table(table_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    data_log_table(table_uuid, branch_uuid, LogKind::DeleteLog)
}

/// Per-(table, branch) `write_sequence` (CHA-431) — the lock-free Postgres
/// SEQUENCE the intra-tx `write_seq_num` mutation ordinal is allocated from via
/// `nextval`, shared across the table's upsert + delete logs. Named off the
/// same data-object prefix as the logs (no `catalog_uuid`); created/dropped
/// alongside them in `PgDialect::create_data_tables` / `drop_data_tables`
/// (`CREATE SEQUENCE ... START 0`).
pub fn write_sequence(table_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    format!(
        "{prefix}_data_write_seq",
        prefix = data_object_prefix(table_uuid, branch_uuid),
    )
}

// -- Partitions of per-catalog tables -------------------------------------
//
// The tx-log family (commit_tx_log, begin_tx_log, abort_tx_log) plus the
// per-tx affected-tables index (tx_table_log, CHA-181) are
// LIST-partitioned by branch_uuid (one leaf per branch). Schemas and
// tables are real Penca Tables under `__penca_system__.{schemas,tables}`
// (CHA-177) — each branch gets its own per-branch data tables named
// via `upsert_log_table(table_uuid, branch_uuid)` /
// `delete_log_table(...)`; no PG partitioning needed for them.
//
// Each partition_uuid derives directly from
// `row_uuid_for_pk(catalog_uuid, [branch_uuid, partition_tag])` where
// `partition_tag` is the fixed PG-name suffix (e.g. `"commit_tx_log"`). The
// tag doubles as the hash-input-space discriminator across the 12
// arity-2 catalog-rooted partition helpers below; see
// [`table_persist_uuid`]'s doc-comment for the catalog-rooted
// arity invariant.

fn partition_name(catalog_uuid: &Uuid, branch_uuid: &Uuid, tag: &str) -> String {
    let partition_uuid = row_uuid_for_pk(catalog_uuid, &[&branch_uuid.to_string(), tag]);
    format!("{partition_uuid}_{tag}_partition")
}

pub fn commit_tx_log_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, COMMIT_TX_LOG)
}

pub fn commit_tx_log_seq_num_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, COMMIT_TX_LOG_SEQ_NUM)
}

pub fn abort_seq_num_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, ABORT_SEQ_NUM)
}

pub fn begin_tx_log_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, BEGIN_TX_LOG)
}

pub fn abort_tx_log_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, ABORT_TX_LOG)
}

pub fn tx_table_log_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, TX_TABLE_LOG)
}

// -- Per-catalog persist + snapshot metadata parents (CHA-198) -------------
//
// Five segment-metadata tables move from global-bare to per-catalog
// `{catalog_uuid}_<base>` parents, each LIST-partitioned by `branch_uuid`.
// All five sit alongside the tx-log family (per-catalog parent + per-
// branch leaf partition). Identifier-length note: PG truncates names
// >63 chars deterministically — long bases (e.g.
// `{36-char-uuid}_table_snapshot_segment_metadata`, 69 chars) get
// truncated by PG, but as long as all sites build the name via these
// helpers, lookups land on the same physical name.

pub fn table_persist_metadata_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{TABLE_PERSIST_METADATA}")
}

pub fn table_persist_segment_metadata_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}")
}

pub fn tx_log_persist_segment_metadata_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{TX_LOG_PERSIST_SEGMENT_METADATA}")
}

pub fn table_purge_metadata_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{TABLE_PURGE_METADATA}")
}

pub fn table_snapshot_metadata_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}")
}

pub fn table_snapshot_segment_metadata_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}")
}

pub fn compact_segment_metadata_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{COMPACT_SEGMENT_METADATA}")
}

pub fn segment_delete_set_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{SEGMENT_DELETE_SET}")
}

pub fn table_snapshot_index_metadata_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{TABLE_SNAPSHOT_INDEX_METADATA}")
}

pub fn table_snapshot_segment_index_metadata_table(catalog_uuid: &Uuid) -> String {
    format!("{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA}")
}

pub fn table_persist_metadata_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, TABLE_PERSIST_METADATA)
}

pub fn table_persist_segment_metadata_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, TABLE_PERSIST_SEGMENT_METADATA)
}

pub fn table_purge_metadata_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, TABLE_PURGE_METADATA)
}

pub fn table_snapshot_metadata_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, TABLE_SNAPSHOT_METADATA)
}

pub fn table_snapshot_segment_metadata_partition(
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
) -> String {
    partition_name(catalog_uuid, branch_uuid, TABLE_SNAPSHOT_SEGMENT_METADATA)
}

pub fn segment_delete_set_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, SEGMENT_DELETE_SET)
}

pub fn compact_segment_metadata_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, COMPACT_SEGMENT_METADATA)
}

pub fn table_snapshot_index_metadata_partition(catalog_uuid: &Uuid, branch_uuid: &Uuid) -> String {
    partition_name(catalog_uuid, branch_uuid, TABLE_SNAPSHOT_INDEX_METADATA)
}

pub fn table_snapshot_segment_index_metadata_partition(
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
) -> String {
    partition_name(
        catalog_uuid,
        branch_uuid,
        TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA,
    )
}

#[cfg(test)]
mod tests {
    use super::super::uuids::{
        row_uuid_for_pk, system_indexes_table_uuid, system_name_index_uuid,
        system_schemas_table_uuid, system_tables_table_uuid,
    };
    use super::*;

    const CAT: Uuid = Uuid::from_u128(0xda79b0ca_d629_3c2d_ef98_a384b6fe9900_u128);
    const BR: Uuid = Uuid::from_u128(0xa7521c7f_adb4_0b78_8c9d_da7897469f17_u128);

    #[test]
    fn test_system_name_index_spec() {
        // CHA-481: each of the three system tables maps to its exact composite
        // name key (mirroring the meta_resolve by-name predicates + the pg.rs
        // system_*_arrow_schema columns); every other table_uuid is None.
        let schemas = system_schemas_table_uuid(&CAT);
        let tables = system_tables_table_uuid(&CAT);
        let indexes = system_indexes_table_uuid(&CAT);

        let s = system_name_index_spec(&CAT, &schemas).expect("schemas name index");
        assert_eq!(s.key_columns, ["schema_name"]);
        assert_eq!(s.index_uuid, system_name_index_uuid(&schemas));

        let t = system_name_index_spec(&CAT, &tables).expect("tables name index");
        assert_eq!(t.key_columns, ["schema_uuid", "table_name"]);
        assert_eq!(t.index_uuid, system_name_index_uuid(&tables));

        let i = system_name_index_spec(&CAT, &indexes).expect("indexes name index");
        assert_eq!(i.key_columns, ["table_uuid", "index_name"]);
        assert_eq!(i.index_uuid, system_name_index_uuid(&indexes));

        // A user table (random uuid) carries no built-in name index.
        assert!(system_name_index_spec(&CAT, &Uuid::from_u128(0x1234)).is_none());
        // The classifier is catalog-scoped: catalog A's system-table uuid is
        // not a system table under catalog B.
        let other_cat = Uuid::from_u128(0x9999);
        assert!(system_name_index_spec(&other_cat, &schemas).is_none());
    }

    #[test]
    fn test_public_schema_constants() {
        // PUBLIC_SCHEMA_NAME is the auto-created default schema bootstrapped
        // by `create_catalog_tables`; the SQL server's
        // SQL_SERVER_DEFAULT_SCHEMA env var defaults to this value too.
        assert_eq!(PUBLIC_SCHEMA_NAME, "public");
        assert_eq!(MAIN_BRANCH_NAME, "main");
    }

    #[test]
    fn test_table_naming() {
        let uuid_str = CAT.to_string();
        assert_eq!(branch_store_table(&CAT), format!("{uuid_str}_branch_store"));
        assert_eq!(
            commit_tx_log_table(&CAT),
            format!("{uuid_str}_commit_tx_log")
        );
        // Per-branch hot table helpers take `(table_uuid, branch_uuid)`
        // and derive the data-log prefix internally — same shape on
        // user tables and on `__penca_system__.{schemas,tables}`.
        assert_eq!(
            upsert_log_table(&CAT, &BR),
            format!(
                "{prefix}_data_upsert_log",
                prefix = row_uuid_for_pk(&CAT, &[&BR.to_string()])
            )
        );
        assert_eq!(
            delete_log_table(&CAT, &BR),
            format!(
                "{prefix}_data_delete_log",
                prefix = row_uuid_for_pk(&CAT, &[&BR.to_string()])
            )
        );
        // CHA-431: the per-table write_sequence shares the data-object
        // prefix with the upsert/delete logs (same (table_uuid, branch_uuid)).
        assert_eq!(
            write_sequence(&CAT, &BR),
            format!(
                "{prefix}_data_write_seq",
                prefix = row_uuid_for_pk(&CAT, &[&BR.to_string()])
            )
        );
        assert_eq!(abort_tx_log_table(&CAT), format!("{uuid_str}_abort_tx_log"));
        assert_eq!(tx_table_log_table(&CAT), format!("{uuid_str}_tx_table_log"));
        let sys_tables_uuid = system_tables_table_uuid(&CAT);
        assert!(
            upsert_log_table(&sys_tables_uuid, &BR).ends_with("_data_upsert_log"),
            "unexpected suffix",
        );
        assert!(
            abort_tx_log_partition(&CAT, &BR).ends_with("_abort_tx_log_partition"),
            "unexpected partition suffix",
        );
        assert!(
            tx_table_log_partition(&CAT, &BR).ends_with("_tx_table_log_partition"),
            "unexpected partition suffix",
        );
    }

    #[test]
    fn test_tx_table_log_partition_distinct_per_branch() {
        let main = Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111_u128);
        let feat = Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222_u128);
        assert_ne!(
            tx_table_log_partition(&CAT, &main),
            tx_table_log_partition(&CAT, &feat),
        );
    }

    #[test]
    fn test_partition_helpers_distinct_per_tag() {
        // All 12 partition helpers share the arity-3 catalog-rooted
        // shape `[catalog, branch, tag]`; distinct tags must yield
        // distinct partition UUIDs so partition names don't alias.
        let partitions = [
            commit_tx_log_partition(&CAT, &BR),
            begin_tx_log_partition(&CAT, &BR),
            abort_tx_log_partition(&CAT, &BR),
            tx_table_log_partition(&CAT, &BR),
            commit_tx_log_seq_num_partition(&CAT, &BR),
            table_persist_metadata_partition(&CAT, &BR),
            table_persist_segment_metadata_partition(&CAT, &BR),
            table_purge_metadata_partition(&CAT, &BR),
            table_snapshot_metadata_partition(&CAT, &BR),
            table_snapshot_segment_metadata_partition(&CAT, &BR),
            compact_segment_metadata_partition(&CAT, &BR),
            segment_delete_set_partition(&CAT, &BR),
            table_snapshot_index_metadata_partition(&CAT, &BR),
            table_snapshot_segment_index_metadata_partition(&CAT, &BR),
        ];
        for i in 0..partitions.len() {
            for j in (i + 1)..partitions.len() {
                assert_ne!(
                    partitions[i], partitions[j],
                    "partitions {i} and {j} collided",
                );
            }
        }
    }

    #[test]
    fn test_parity_abort_tx_log_table() {
        assert_eq!(
            abort_tx_log_table(&CAT),
            "da79b0ca-d629-3c2d-ef98-a384b6fe9900_abort_tx_log"
        );
    }

    #[test]
    fn test_parity_abort_tx_log_partition() {
        // CHA-236: partition_uuid recomputed via
        // `row_uuid_for_pk(catalog, [branch, "abort_tx_log"])`. Golden
        // updated to match the new hash input.
        assert_eq!(
            abort_tx_log_partition(&CAT, &BR),
            "7dbeac51-0321-0f5b-33ac-174a2b90730d_abort_tx_log_partition"
        );
    }

    #[test]
    fn test_parity_tx_table_log_table() {
        assert_eq!(
            tx_table_log_table(&CAT),
            "da79b0ca-d629-3c2d-ef98-a384b6fe9900_tx_table_log"
        );
    }

    #[test]
    fn test_parity_commit_tx_log_seq_num_table() {
        // CHA-428: fixed-suffix per-catalog counter table name.
        assert_eq!(
            commit_tx_log_seq_num_table(&CAT),
            "da79b0ca-d629-3c2d-ef98-a384b6fe9900_commit_tx_log_seq_num"
        );
    }

    #[test]
    fn test_parity_commit_tx_log_seq_num_partition() {
        // CHA-428: partition_uuid = row_uuid_for_pk(catalog, [branch,
        // "commit_tx_log_seq_num"]). Golden mirrors the Python parity suite.
        assert_eq!(
            commit_tx_log_seq_num_partition(&CAT, &BR),
            "4bb9308a-f277-9b8b-0631-bd6d5aa5c2f9_commit_tx_log_seq_num_partition"
        );
    }

    #[test]
    fn test_parity_tx_table_log_partition() {
        // CHA-236: partition_uuid recomputed via
        // `row_uuid_for_pk(catalog, [branch, "tx_table_log"])`.
        // Golden updated to match the new hash input.
        assert_eq!(
            tx_table_log_partition(&CAT, &BR),
            "6830ca7e-5210-6616-91cf-34c0e0d7c612_tx_table_log_partition"
        );
    }
}
