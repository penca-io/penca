use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128;

use super::tables::SYSTEM_SCHEMA_NAME;
use crate::LogKind;

/// Generic deterministic UUID combiner.
///
/// Hashes the `\x00`-joined parts via xxh3_128 and returns a UUID.
/// Lower-level primitive behind every Penca deterministic UUID — the
/// three structural anchors below, the log-partition prefix computed
/// inside [`upsert_log_table`] / [`delete_log_table`], and the
/// persist + snapshot chain helpers ([`table_persist_uuid`],
/// [`table_purge_uuid`], [`table_snapshot_uuid`], etc., which
/// all go through [`row_uuid_for_pk`]).
///
/// For row identity within a parent table, use [`row_uuid_for_pk`]
/// instead — it captures the row-PK semantics explicitly.
pub fn deterministic_uuid_from(parts: &[&str]) -> Uuid {
    let input = parts.join("\x00");
    Uuid::from_u128(xxh3_128(input.as_bytes()))
}

/// Compute a deterministic row_uuid from a parent UUID + PK values.
///
/// Row-identity semantic: a row in some parent table identified by
/// `table_uuid`, keyed by the user PK values. Identical PKs in
/// different parent tables produce different row_uuids because
/// `table_uuid` is part of the hash input.
pub fn row_uuid_for_pk(table_uuid: &Uuid, pk_values: &[&str]) -> Uuid {
    let table_id = table_uuid.to_string();
    let mut parts: Vec<&str> = Vec::with_capacity(1 + pk_values.len());
    parts.push(&table_id);
    parts.extend_from_slice(pk_values);
    deterministic_uuid_from(&parts)
}

/// Deterministic `version_uuid` for an auditable-store row.
///
/// `version_uuid` is the PRIMARY KEY of every auditable-store table
/// (data + metadata). Deriving it deterministically from
/// `(row_uuid, tx_uuid)` means the PK alone enforces the
/// auditable-store invariant: at most one version per (entity, tx) —
/// a second insert with the same `(row_uuid, tx_uuid)` produces the
/// same `version_uuid` and trips the PK constraint. No separate
/// `UNIQUE(row_uuid, tx_uuid)` index needed. See ADR 0013.
pub fn version_uuid(row_uuid: &Uuid, tx_uuid: &Uuid) -> Uuid {
    deterministic_uuid_from(&[&row_uuid.to_string(), &tx_uuid.to_string()])
}

/// Deterministic genesis transaction UUID for a catalog.
///
/// The genesis tx is the first committed transaction in a catalog,
/// inserted at `CreateCatalog` into the catalog's `commit_tx_log` /
/// `begin_tx_log`. All branches reference it as their root.
pub fn genesis_tx_uuid(catalog_uuid: &Uuid) -> Uuid {
    deterministic_uuid_from(&[&catalog_uuid.to_string()])
}

// The `__penca_system__` schema and its two bootstrap tables are
// the only namespace objects whose UUIDs stay deterministic — server-
// internal write paths address them by well-known per-catalog identity
// (e.g. when bootstrapping rows in `__penca_system__.tables` from
// `CreateTable`). User-created schemas/tables are random-minted per
// CHA-236. The three anchors below all use arity-2 catalog-rooted
// `deterministic_uuid_from([catalog_str, tag])` with distinct tag
// strings — disjoint from each other and from every other
// catalog-rooted helper by arity-and-tag.

pub fn system_schema_uuid(catalog_uuid: &Uuid) -> Uuid {
    deterministic_uuid_from(&[&catalog_uuid.to_string(), SYSTEM_SCHEMA_NAME])
}

pub fn system_schemas_table_uuid(catalog_uuid: &Uuid) -> Uuid {
    deterministic_uuid_from(&[&catalog_uuid.to_string(), "__penca_system__.schemas"])
}

pub fn system_tables_table_uuid(catalog_uuid: &Uuid) -> Uuid {
    deterministic_uuid_from(&[&catalog_uuid.to_string(), "__penca_system__.tables"])
}

/// CHA-455: deterministic `table_uuid` for the `__penca_system__.indexes`
/// auditable store, the third dogfooded system Penca Table.
pub fn system_indexes_table_uuid(catalog_uuid: &Uuid) -> Uuid {
    deterministic_uuid_from(&[&catalog_uuid.to_string(), "__penca_system__.indexes"])
}

/// Deterministic `index_uuid` for the built-in composite **name index** on a
/// system table (CHA-481, chunk B-sys of the CHA-463 cold-index umbrella).
///
/// NON-NULL by design: CHA-412's strictly-internal `row_uuid` identity index is
/// the *only* `index_uuid IS NULL` record, and the `row_uuid` cold read plan
/// (`meta_plan.rs` / CHA-454) selects its sidecar via that `IS NULL` filter.
/// Giving the built-in name index a non-NULL id keeps it out of that join, so
/// the by-uuid metadata path (CHA-473) is unaffected. Derived from the *system
/// table's own* `table_uuid` so it is stable per table across snapshots — a
/// crash-retried snapshot build collapses via `ON CONFLICT`, and the by-name
/// read seek (CHA-484) recomputes the same id. It is a *built-in* index, never a
/// `__penca_system__.indexes` row / user `CREATE INDEX` object.
///
/// Value-space note: this shares the 128-bit [`row_uuid_for_pk`] space with
/// ordinary row identities — e.g. a `schemas` row for a schema literally named
/// `name_index` hashes to the bit-identical value (the `tables`/`indexes` tables
/// are safe by PK arity). That is harmless because an `index_uuid` and a data
/// `row_uuid` never share a column or get compared: this value lives only in
/// `table_snapshot_index_metadata.index_uuid` and the sidecar slug, never in a
/// data segment. The guarantee is **namespace separation**, not value-space
/// disjointness — do not introduce a path that treats both as keys in one column.
pub fn system_name_index_uuid(system_table_uuid: &Uuid) -> Uuid {
    row_uuid_for_pk(system_table_uuid, &["name_index"])
}

// Every row in the persist + snapshot metadata family carries a
// deterministic UUID derived from its parent + its own discriminators
// via `row_uuid_for_pk`. Phase-1 retries with identical inputs replay to
// identical UUIDs at every level and slot in via `ON CONFLICT DO UPDATE`.
//
// See ADR 0016 for the construction rule; ADR 0013 for the (separate)
// auditable-store invariant.

/// Deterministic table_persist UUID for one `(branch, table, persisted_at,
/// log_kind)`.
///
/// Chains directly off `catalog_uuid` — no intermediate branch-persist
/// parent (CHA-220 removed `branch_persist_metadata`). The four PK values
/// discriminate one persist row from every other within the catalog.
///
/// **Arity invariant** — collision-freedom against other catalog-rooted
/// helpers is structural, not by convention: `deterministic_uuid_from`
/// `\x00`-joins its parts, so every catalog-rooted helper that passes a
/// different-length PK tuple into `row_uuid_for_pk(catalog, ...)`
/// occupies a disjoint hash-input space. Current arities for
/// catalog-rooted helpers: partition helpers
/// ([`commit_tx_log_partition`] and siblings) use 2 PK values
/// `[branch, tag]`; [`table_snapshot_uuid`] uses 3;
/// [`table_persist_uuid`] (this) uses 4. Within arity-2, the
/// trailing `tag` string is unique per helper (one of 11 distinct
/// values matching the table-name constant the partition serves), so
/// the partition family also lives in disjoint hash-input subspaces.
/// Adding a sibling that reuses an existing arity+tag combo *would*
/// collide — give it a distinct arity or tag (or chain it off a
/// non-catalog parent, as [`table_purge_uuid`] does) instead.
pub fn table_persist_uuid(
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    table_uuid: &Uuid,
    persisted_at_micros: i64,
    log_kind: LogKind,
) -> Uuid {
    row_uuid_for_pk(
        catalog_uuid,
        &[
            &branch_uuid.to_string(),
            &table_uuid.to_string(),
            &persisted_at_micros.to_string(),
            log_kind.as_str(),
        ],
    )
}

/// Deterministic identity for a `table_purge_metadata` two-phase row.
///
/// Rooted on a catalog-scoped `"purge"` anchor rather than on `catalog_uuid`
/// directly: the anchor keeps the hash-input space disjoint from the other
/// catalog-rooted helpers without needing a discriminator tag in the outer
/// `row_uuid_for_pk` call.
///
/// CHA-444 (ADR 0027): seeded on the `(Pu, Pa)` watermark pair this purge
/// wave advances to — `pu_seq` = `last_purged_commit_seq_num` (committed read
/// fence) and `pa_seq` = `last_purged_aborted_seq_num` (abort cleanup
/// frontier). A wave records `-1` for an axis it did not advance. Each
/// advancing wave produces a distinct pair (a non-advancing wave no-ops and
/// writes nothing), so identical inputs replay to the same PK for idempotent
/// phase-1 retries while distinct waves never collide.
pub fn table_purge_uuid(
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    table_uuid: &Uuid,
    pu_seq: i64,
    pa_seq: i64,
) -> Uuid {
    let purge_anchor = deterministic_uuid_from(&[&catalog_uuid.to_string(), "purge"]);
    row_uuid_for_pk(
        &purge_anchor,
        &[
            &branch_uuid.to_string(),
            &table_uuid.to_string(),
            &pu_seq.to_string(),
            &pa_seq.to_string(),
        ],
    )
}

/// Deterministic UUID for a table_persist segment.
///
/// Chains off `table_persist_uuid`; `chunk_idx` distinguishes sibling
/// segments emitted by the persist-time chunker (CHA-215) within the
/// same persist event.
pub fn table_persist_segment_uuid(table_persist_uuid: &Uuid, chunk_idx: u32) -> Uuid {
    row_uuid_for_pk(table_persist_uuid, &[&chunk_idx.to_string()])
}

/// Deterministic UUID for a cold `tx_log` persist segment (CHA-507).
///
/// Rooted on a catalog-scoped `"tx_log_persist"` anchor (same disjointness
/// discipline as [`table_purge_uuid`]) keyed by `(branch, max_commit_seq_num)`.
/// The seq is the segment's inclusive upper bound, so a re-run of
/// `persist_tx_log` for the same range upserts the same row (idempotent),
/// while a later, higher persist point mints a distinct segment.
pub fn tx_log_persist_segment_uuid(
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    max_commit_seq_num: i64,
) -> Uuid {
    let anchor = deterministic_uuid_from(&[&catalog_uuid.to_string(), "tx_log_persist"]);
    row_uuid_for_pk(
        &anchor,
        &[&branch_uuid.to_string(), &max_commit_seq_num.to_string()],
    )
}

/// Deterministic UUID for a table snapshot.
pub fn table_snapshot_uuid(
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    table_uuid: &Uuid,
    snapshotted_at_micros: i64,
) -> Uuid {
    row_uuid_for_pk(
        catalog_uuid,
        &[
            &branch_uuid.to_string(),
            &table_uuid.to_string(),
            &snapshotted_at_micros.to_string(),
        ],
    )
}

/// Deterministic UUID for a snapshot segment.
///
/// `chunk_idx` distinguishes sibling segments emitted by the
/// snapshot-time chunker (CHA-215) within the same snapshot cycle.
pub fn table_snapshot_segment_uuid(table_snapshot_uuid: &Uuid, chunk_idx: u32) -> Uuid {
    row_uuid_for_pk(table_snapshot_uuid, &[&chunk_idx.to_string()])
}

/// Deterministic `table_snapshot_index_uuid` — the id of a cold-index PARENT
/// row (CHA-412), one per `(snapshot, index)`. `index_uuid` is `None` for the
/// strictly-internal `row_uuid` identity index. Deterministic so a
/// crash-retried snapshot build collapses via `ON CONFLICT`.
///
/// This is the **single source** of the parent id: the build computes it here,
/// and carry-forward resolves a carried child's parent by *looking up* the
/// row (a JOIN on `(table_snapshot_uuid, index_uuid)`), never by recomputing —
/// so there is no cross-language hash contract to drift.
pub fn table_snapshot_index_uuid(table_snapshot_uuid: &Uuid, index_uuid: Option<&Uuid>) -> Uuid {
    let index_disc = match index_uuid {
        Some(uuid) => uuid.to_string(),
        None => "row_uuid".to_string(),
    };
    row_uuid_for_pk(table_snapshot_uuid, &[&index_disc])
}

/// Deterministic UUID for a `segment_delete_set` row, identifying one
/// `(branch, table, object_uri)` deferred file delete.
///
/// Rooted on a catalog-scoped `"segment_delete"` anchor — same trick
/// as [`table_purge_uuid`]: the arity-3 PK tuple
/// `(branch, table, uri)` would collide with
/// [`table_snapshot_uuid`] if rooted on the catalog UUID, so the
/// anchor keeps the hash-input space disjoint without a discriminator
/// tag in the outer call.
///
/// Determinism also makes the in-tx INSERT replay-safe: a compact tx
/// that crashed after PG commit but before file delete cannot land a
/// duplicate row when the unsealed segments are picked up again.
pub fn segment_delete_uuid(
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    table_uuid: &Uuid,
    object_uri: &str,
) -> Uuid {
    let segment_delete_anchor =
        deterministic_uuid_from(&[&catalog_uuid.to_string(), "segment_delete"]);
    row_uuid_for_pk(
        &segment_delete_anchor,
        &[
            &branch_uuid.to_string(),
            &table_uuid.to_string(),
            object_uri,
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Historical reference: literal UUIDs reproducing the pre-CHA-236
    // `get_catalog_uuid("my_catalog")` / `get_branch_uuid(cat, "main")`
    // / `get_table_uuid(sch, "my_table")` outputs. Chain-helper parity
    // values stay comparable to pre-CHA-236 goldens because the chain
    // shape is unchanged.
    const CAT: Uuid = Uuid::from_u128(0xda79b0ca_d629_3c2d_ef98_a384b6fe9900_u128);
    const BR: Uuid = Uuid::from_u128(0xa7521c7f_adb4_0b78_8c9d_da7897469f17_u128);
    const TBL: Uuid = Uuid::from_u128(0xba899706_e405_7932_1733_13ba5f0eea66_u128);

    #[test]
    fn test_uuid_format() {
        let uuid = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        assert_eq!(uuid.to_string(), "01234567-89ab-cdef-0123-456789abcdef");
    }

    #[test]
    fn test_row_uuid_for_pk_deterministic() {
        let a = row_uuid_for_pk(&CAT, &["pk1", "pk2"]);
        let b = row_uuid_for_pk(&CAT, &["pk1", "pk2"]);
        assert_eq!(a, b);
    }

    #[test]
    fn test_row_uuid_for_pk_different_tables() {
        let t1 = Uuid::from_u128(1);
        let t2 = Uuid::from_u128(2);
        let a = row_uuid_for_pk(&t1, &["pk1"]);
        let b = row_uuid_for_pk(&t2, &["pk1"]);
        assert_ne!(a, b);
    }

    #[test]
    fn test_structural_anchors_distinct() {
        // The three structural anchors share arity-2
        // `[catalog_str, tag]` shape but use distinct tag strings, so
        // they live in disjoint hash-input subspaces.
        let sys_schema = system_schema_uuid(&CAT);
        let sys_schemas = system_schemas_table_uuid(&CAT);
        let sys_tables = system_tables_table_uuid(&CAT);
        // CHA-455: the fourth catalog-rooted anchor.
        let sys_indexes = system_indexes_table_uuid(&CAT);
        assert_ne!(sys_schema, sys_schemas);
        assert_ne!(sys_schema, sys_tables);
        assert_ne!(sys_schemas, sys_tables);
        assert_ne!(sys_indexes, sys_schema);
        assert_ne!(sys_indexes, sys_schemas);
        assert_ne!(sys_indexes, sys_tables);
    }

    #[test]
    fn test_system_name_index_uuid_deterministic_and_distinct() {
        // CHA-481: the built-in name index_uuid is deterministic per system
        // table, distinct across the three system tables, and (crucially)
        // NON-NULL / distinct from the system table_uuid itself — so the
        // `index_uuid IS NULL` row_uuid read plan never confuses the two.
        let schemas = system_schemas_table_uuid(&CAT);
        let tables = system_tables_table_uuid(&CAT);
        let indexes = system_indexes_table_uuid(&CAT);

        assert_eq!(
            system_name_index_uuid(&schemas),
            system_name_index_uuid(&schemas),
            "name index uuid must be deterministic for a given system table",
        );
        let name_schemas = system_name_index_uuid(&schemas);
        let name_tables = system_name_index_uuid(&tables);
        let name_indexes = system_name_index_uuid(&indexes);
        assert_ne!(name_schemas, name_tables);
        assert_ne!(name_schemas, name_indexes);
        assert_ne!(name_tables, name_indexes);
        // Distinct from the system table_uuid each is derived from.
        assert_ne!(name_schemas, schemas);
        assert_ne!(name_tables, tables);
        assert_ne!(name_indexes, indexes);
    }

    #[test]
    fn test_structural_anchors_disjoint_from_partitions() {
        // Arity-2 structural anchors (`[catalog, tag]`) vs arity-3
        // partition helpers (`[catalog, branch, tag]`) live in
        // structurally disjoint hash-input spaces because
        // `deterministic_uuid_from` `\x00`-joins parts (different
        // part counts produce different byte strings).
        let sys_schemas = system_schemas_table_uuid(&CAT);
        let commit_tx_log_partition_uuid =
            row_uuid_for_pk(&CAT, &[&BR.to_string(), "commit_tx_log"]);
        assert_ne!(sys_schemas, commit_tx_log_partition_uuid);
    }

    #[test]
    fn test_table_snapshot_segment_uuid_chunk_idx() {
        let snap = Uuid::from_u128(0xdead_beef_dead_beef_dead_beef_dead_beef_u128);
        let a = table_snapshot_segment_uuid(&snap, 0);
        let b = table_snapshot_segment_uuid(&snap, 0);
        assert_eq!(a, b);

        let c = table_snapshot_segment_uuid(&snap, 1);
        assert_ne!(a, c);
    }

    #[test]
    fn test_genesis_tx_uuid() {
        let genesis = genesis_tx_uuid(&CAT);
        assert_eq!(genesis, genesis_tx_uuid(&CAT));
        assert_ne!(genesis, CAT);
    }

    #[test]
    fn test_table_persist_segment_uuid() {
        let tf = Uuid::from_u128(1);
        let a = table_persist_segment_uuid(&tf, 0);
        let b = table_persist_segment_uuid(&tf, 0);
        assert_eq!(a, b);

        let c = table_persist_segment_uuid(&tf, 1);
        assert_ne!(a, c);
    }

    // Expected values generated by the Python implementation. If these fail,
    // the Rust and Python identity systems have diverged. Mirror file:
    // tests/static/static_naming_parity_test.py

    #[test]
    fn test_parity_genesis_tx_uuid() {
        assert_eq!(
            genesis_tx_uuid(&CAT).to_string(),
            "f0f17483-9020-5278-030c-8b2ca4878fb8"
        );
    }

    #[test]
    fn test_parity_row_uuid_for_pk() {
        assert_eq!(
            row_uuid_for_pk(&CAT, &["pk1", "pk2"]).to_string(),
            "60e3c840-3e39-ce48-64d0-036c1ddeb9fc"
        );
    }

    #[test]
    fn test_parity_system_name_index_uuid() {
        // CHA-481: cross-language golden for the built-in name index_uuid.
        // The Python mirror (static_naming_parity_test.py) pins the same value
        // for `system_name_index_uuid(TBL)`; the shared risk is the
        // `"name_index"` discriminator drifting between the two stacks.
        assert_eq!(
            system_name_index_uuid(&TBL).to_string(),
            "b1912f99-a103-3484-a251-ef9c967dd545"
        );
    }

    #[test]
    fn test_parity_table_persist_segment_uuid() {
        // CHA-215: signature reshaped to `(table_persist_uuid,
        // chunk_idx)`. The chain hash flows into the segment via
        // `table_persist_uuid`; `chunk_idx` is the sibling uniquifier.
        let tf = table_persist_uuid(&CAT, &BR, &TBL, 1000, LogKind::UpsertLog);
        assert_eq!(
            table_persist_segment_uuid(&tf, 0).to_string(),
            "9a7f445a-21fe-2917-280d-6d747215f54d"
        );
    }

    // Mirror of `tests/static/static_naming_parity_test.py` —
    // `test_uuid_chain_parity_*`. Rotating the hash function or hash-
    // input format on either side must update both test suites.

    #[test]
    fn test_parity_table_persist_uuid_per_kind() {
        assert_eq!(
            table_persist_uuid(&CAT, &BR, &TBL, 1000, LogKind::UpsertLog).to_string(),
            "65bf9889-d637-f44b-1e72-5c306b8a8384"
        );
        assert_eq!(
            table_persist_uuid(&CAT, &BR, &TBL, 1000, LogKind::DeleteLog).to_string(),
            "749d519b-c934-bb2c-cc61-21cd3884f4dd"
        );
    }

    #[test]
    fn test_parity_table_purge_uuid() {
        // CHA-236: anchor re-rooted to `deterministic_uuid_from([catalog_str,
        // "purge"])`. CHA-444 (ADR 0027): seed is now the `(Pu, Pa)` pair, so
        // the golden is recomputed for the two-arg hash input.
        assert_eq!(
            table_purge_uuid(&CAT, &BR, &TBL, 1000, -1).to_string(),
            "236258e6-d150-0854-9ddb-ff23cb4d1826"
        );
    }

    #[test]
    fn test_parity_table_snapshot_uuid() {
        assert_eq!(
            table_snapshot_uuid(&CAT, &BR, &TBL, 1000).to_string(),
            "03576be6-3377-9185-99e8-aef297ac7996"
        );
    }

    #[test]
    fn test_parity_table_snapshot_segment_uuid() {
        let snap = table_snapshot_uuid(&CAT, &BR, &TBL, 1000);
        assert_eq!(
            table_snapshot_segment_uuid(&snap, 0).to_string(),
            "8e2b58a1-c2e5-e39d-e8e1-90117e32cca2"
        );
    }

    #[test]
    fn test_table_snapshot_index_uuid() {
        let snap = table_snapshot_uuid(&CAT, &BR, &TBL, 1000);
        let internal = table_snapshot_index_uuid(&snap, None);
        // Deterministic — a crash-retried build collapses via ON CONFLICT.
        assert_eq!(internal, table_snapshot_index_uuid(&snap, None));
        // A user secondary index (non-NULL index_uuid) is distinct from the
        // internal `row_uuid` index on the same snapshot.
        let idx = Uuid::from_u128(0x1234_u128);
        assert_ne!(internal, table_snapshot_index_uuid(&snap, Some(&idx)));
        // Per-snapshot: a different snapshot yields a different parent id.
        let snap2 = table_snapshot_uuid(&CAT, &BR, &TBL, 2000);
        assert_ne!(internal, table_snapshot_index_uuid(&snap2, None));
    }
}
