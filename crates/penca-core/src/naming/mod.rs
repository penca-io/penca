//! Naming conventions and identity functions for Penca.
//!
//! `catalog_store` is the only globally-named table (it's the catalog
//! registry itself). All other core tables (`branch_store`, `commit_tx_log`,
//! `begin_tx_log`, `abort_tx_log`, `tx_table_log`, etc.) are per-catalog
//! and prefixed with the owning catalog UUID. Per-branch data tables
//! — including the system tables under `__penca_system__` — derive
//! their PG names deterministically from `(table_uuid, branch_uuid)`
//! via [`upsert_log_table`] / [`delete_log_table`] (CHA-177).
//!
//! All identity functions in this module produce deterministic UUIDs
//! via xxh3_128. Internally these are [`uuid::Uuid`] values (backed by
//! u128) for zero-copy Postgres binding via sqlx. String formatting
//! only happens at the gRPC boundary.
//!
//! # Identity model
//!
//! Namespace UUIDs (`catalog_uuid`, `schema_uuid`, `branch_uuid`,
//! `table_uuid`) for user-created resources are server-minted random
//! at `Create*` time and persisted on the namespace row (CHA-236).
//! Their derivation lives in the API layer, not here. This module
//! only contains the deterministic UUIDs the storage layer needs to
//! address by well-known per-catalog identity:
//!
//! - **Structural anchors**: `__penca_system__` schema + its two
//!   bootstrap tables (`schemas`, `tables`). Deterministic from
//!   `catalog_uuid` so server-internal write paths can address them
//!   without state. See [`system_schema_uuid`],
//!   [`system_schemas_table_uuid`], [`system_tables_table_uuid`].
//! - **Per-branch partition leaves**: the tx-log family and the seven
//!   persist/snapshot/purge metadata partitions. Each leaf's
//!   `partition_uuid` derives directly from `(catalog_uuid,
//!   branch_uuid, partition_tag)`, where `partition_tag` is the fixed
//!   PG-name suffix (e.g. `"commit_tx_log"`, [`TABLE_PERSIST_METADATA`]).
//! - **Auditable-store row identity**: [`row_uuid_for_pk`] plus the
//!   persist + snapshot UUID chain ([`table_persist_uuid`],
//!   [`table_purge_uuid`], [`table_snapshot_uuid`], and their
//!   segment children). These take random `table_uuid` as input now;
//!   the chain structure is unchanged (ADR 0016).
//!
//! ## `__penca_system__.{schemas,tables,indexes}` identity
//!
//! Schema, table, and index metadata are first-class Penca Tables under
//! the reserved schema `__penca_system__` (ADR 0012). Each row in
//! `__penca_system__.tables` describes one user table on one branch.
//! The system tables follow the *universal* auditable-store pattern
//! (ADR 0013): each row's own entity uuid is a first-class PK column, and
//! `row_uuid = row_uuid_for_pk(system_<x>_table_uuid, [<entity>_uuid])`
//! like every other Penca table (CHA-380 removed the earlier
//! `row_uuid == <entity>_uuid` overload):
//!
//! - **`__penca_system__.schemas`**: PK column `schema_uuid`.
//! - **`__penca_system__.tables`**: PK column `table_uuid` (distinct from
//!   the `schema_uuid` foreign key naming its schema parent).
//! - **`__penca_system__.indexes`**: PK column `index_uuid` (distinct from
//!   the `table_uuid` foreign key naming the owning table).
//!
//! Branch isolation is *not* encoded in the `row_uuid`; it comes from
//! "which per-branch PG table the row physically lives in." Branch B's
//! `__penca_system__.tables` rows all live in
//! `upsert_log_table(sys_tables_table_uuid, B_branch_uuid)`, so
//! the row layer doesn't need to repeat what the table-name layer
//! already encodes.
//!
//! `CreateBranch` materializes parent rows under the child's per-branch
//! PG tables; the derivation is deterministic, so the child's row_uuids
//! equal the parent's, with a new `tx_uuid` (the synthetic `fork_tx`).
//!
//! ## Historical note
//!
//! Pre-CHA-177, Penca had a separate per-create-tx data-table
//! identifier. CHA-177 dropped it: each branch's data tables derive
//! deterministically from `(table_uuid, branch_uuid)` via the
//! two-arg [`upsert_log_table`] / [`delete_log_table`]
//! helpers below. See ADR 0011 / ADR 0012 for the historical record.
//!
//! Pre-CHA-236, namespace UUIDs were also deterministically hashed
//! from the corresponding name strings (`catalog_uuid = xxh3(name)`,
//! `table_uuid = xxh3(schema_uuid, name)`, etc.). CHA-236 flipped
//! namespace UUIDs to random at Create-time to support rename. The
//! hash-derivation helpers for those four UUIDs are gone; the
//! storage-internal structural anchors stay deterministic so
//! server-internal write paths can still address the system tables
//! and per-branch partitions without state.

mod tables;
mod uris;
mod uuids;

pub use tables::{
    ABORT_SEQ_NUM, ABORT_TX_LOG, BEGIN_TX_LOG, CATALOG_STORE, COMMIT_TX_LOG, COMMIT_TX_LOG_SEQ_NUM,
    COMPACT_SEGMENT_METADATA, MAIN_BRANCH_NAME, PUBLIC_SCHEMA_NAME, SEGMENT_DELETE_SET,
    SYSTEM_INDEXES_TABLE_NAME, SYSTEM_SCHEMA_NAME, SYSTEM_SCHEMAS_TABLE_NAME,
    SYSTEM_TABLES_TABLE_NAME, SystemNameIndexSpec, TABLE_PERSIST_METADATA,
    TABLE_PERSIST_SEGMENT_METADATA, TABLE_PURGE_METADATA, TABLE_SNAPSHOT_INDEX_METADATA,
    TABLE_SNAPSHOT_METADATA, TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA, TX_LOG_PERSIST_SEGMENT_METADATA, TX_TABLE_LOG,
    abort_seq_num_partition, abort_seq_num_table, abort_tx_log_partition, abort_tx_log_table,
    begin_tx_log_partition, begin_tx_log_table, branch_store_table, commit_tx_log_partition,
    commit_tx_log_seq_num_partition, commit_tx_log_seq_num_table, commit_tx_log_table,
    compact_segment_metadata_partition, compact_segment_metadata_table, delete_log_table,
    segment_delete_set_table, system_name_index_spec, table_persist_metadata_partition,
    table_persist_metadata_table, table_persist_segment_metadata_partition,
    table_persist_segment_metadata_table, table_purge_metadata_partition,
    table_purge_metadata_table, table_snapshot_index_metadata_partition,
    table_snapshot_index_metadata_table, table_snapshot_metadata_partition,
    table_snapshot_metadata_table, table_snapshot_segment_index_metadata_partition,
    table_snapshot_segment_index_metadata_table, table_snapshot_segment_metadata_partition,
    table_snapshot_segment_metadata_table, tx_log_persist_segment_metadata_table,
    tx_table_log_partition, tx_table_log_table, upsert_log_table, write_sequence,
};
pub use uris::{
    persist_segment_uri, segment_index_uri, snapshot_segment_uri, tx_log_persist_segment_uri,
};
pub use uuids::{
    deterministic_uuid_from, genesis_tx_uuid, row_uuid_for_pk, system_indexes_table_uuid,
    system_name_index_uuid, system_schema_uuid, system_schemas_table_uuid,
    system_tables_table_uuid, table_persist_segment_uuid, table_persist_uuid, table_purge_uuid,
    table_snapshot_index_uuid, table_snapshot_segment_uuid, table_snapshot_uuid,
    tx_log_persist_segment_uuid, version_uuid,
};
