"""Cross-language UUID parity between the Python and Rust naming modules.

The Rust side has the mirror of this file at
``crates/penca-core/src/naming.rs`` (search for ``test_parity_*``).
Both files share the same hardcoded expected values — if anyone rotates
the hash function or changes the hash-input format on either side, both
test suites must be updated together. The symmetry is the test: two
independent implementations agreeing on these fixed inputs is the
guarantee a Python writer and a Rust reader compute the same UUIDs.

CHA-236: namespace UUID hash helpers (``get_catalog_uuid`` /
``get_schema_uuid`` / ``get_branch_uuid`` / ``get_table_uuid``) were
deleted — those identifiers are now random server-side. The literal
``CAT`` / ``BR`` / ``TBL`` constants below reproduce the pre-CHA-236
outputs so chain-helper goldens stay comparable; the partition
helpers and ``table_purge_uuid`` carry new goldens reflecting the
re-rooted hash inputs.

These tests do not need Docker — they are pure source-input checks. They
live under ``tests/static/`` (run via ``just static-test``, also wired
into ``just check``).
"""

from __future__ import annotations

from penca_client.naming import (
    abort_tx_log_partition,
    abort_tx_log_table,
    commit_tx_log_seq_num_partition,
    commit_tx_log_seq_num_table,
    genesis_tx_uuid,
    row_uuid_for_pk,
    system_name_index_uuid,
    table_persist_segment_uuid,
    table_persist_uuid,
    table_purge_uuid,
    table_snapshot_segment_uuid,
    table_snapshot_uuid,
    tx_table_log_partition,
    tx_table_log_table,
    write_sequence,
)

# Literal UUIDs reproducing the pre-CHA-236
# ``get_catalog_uuid("my_catalog")`` / ``get_branch_uuid(cat, "main")``
# / ``get_table_uuid(sch, "my_table")`` outputs. Chain-helper parity
# values stay comparable to pre-CHA-236 goldens because the chain
# shape is unchanged. Mirror of the Rust unit-test constants in
# ``crates/penca-core/src/naming.rs``.
CAT = "da79b0ca-d629-3c2d-ef98-a384b6fe9900"
BR = "a7521c7f-adb4-0b78-8c9d-da7897469f17"
TBL = "ba899706-e405-7932-1733-13ba5f0eea66"


class TestNamingParity:
    def test_genesis_tx_uuid(self):
        assert genesis_tx_uuid(CAT) == "f0f17483-9020-5278-030c-8b2ca4878fb8"

    def test_row_uuid_for_pk(self):
        assert (
            row_uuid_for_pk(CAT, ["pk1", "pk2"])
            == "60e3c840-3e39-ce48-64d0-036c1ddeb9fc"
        )

    def test_system_name_index_uuid(self):
        # CHA-481: built-in name index_uuid = row_uuid_for_pk(system_table_uuid,
        # ["name_index"]). The Rust mirror pins the same value; the shared risk
        # is the "name_index" discriminator drifting between the two stacks.
        assert system_name_index_uuid(TBL) == "b1912f99-a103-3484-a251-ef9c967dd545"

    def test_table_persist_segment_uuid(self):
        # CHA-215: signature reshaped to ``(table_persist_uuid,
        # chunk_idx)`` — the persist-time chunker emits one row per chunk
        # under a single parent ``table_persist_uuid``, and ``chunk_idx``
        # is the only uniquifier between siblings.
        tf = table_persist_uuid(CAT, BR, TBL, 1000, "upsert_log")
        assert (
            table_persist_segment_uuid(tf, 0) == "9a7f445a-21fe-2917-280d-6d747215f54d"
        )

    def test_abort_tx_log_table(self):
        assert (
            abort_tx_log_table(CAT)
            == "da79b0ca-d629-3c2d-ef98-a384b6fe9900_abort_tx_log"
        )

    def test_abort_tx_log_partition(self):
        # CHA-236: partition_uuid recomputed via
        # ``row_uuid_for_pk(catalog, [branch, "abort_tx_log"])``. Golden
        # updated to match the new hash input.
        assert (
            abort_tx_log_partition(CAT, BR)
            == "7dbeac51-0321-0f5b-33ac-174a2b90730d_abort_tx_log_partition"
        )

    def test_tx_table_log_table(self):
        assert (
            tx_table_log_table(CAT)
            == "da79b0ca-d629-3c2d-ef98-a384b6fe9900_tx_table_log"
        )

    def test_tx_table_log_partition(self):
        # CHA-236: partition_uuid recomputed via
        # ``row_uuid_for_pk(catalog, [branch, "tx_table_log"])``. Golden
        # updated to match the new hash input.
        assert (
            tx_table_log_partition(CAT, BR)
            == "6830ca7e-5210-6616-91cf-34c0e0d7c612_tx_table_log_partition"
        )

    def test_commit_tx_log_seq_num_table(self):
        # CHA-428: per-branch commit-order counter table, fixed-suffix name.
        assert (
            commit_tx_log_seq_num_table(CAT)
            == "da79b0ca-d629-3c2d-ef98-a384b6fe9900_commit_tx_log_seq_num"
        )

    def test_commit_tx_log_seq_num_partition(self):
        # CHA-428: partition_uuid = row_uuid_for_pk(catalog, [branch,
        # "commit_tx_log_seq_num"]) — same derivation as the tx-log family.
        assert (
            commit_tx_log_seq_num_partition(CAT, BR)
            == "4bb9308a-f277-9b8b-0631-bd6d5aa5c2f9_commit_tx_log_seq_num_partition"
        )

    def test_write_sequence(self):
        # CHA-431: per-(table, branch) sequence; prefix =
        # row_uuid_for_pk(table_uuid, [branch_uuid]) — the same data-object
        # prefix as upsert_log/delete_log. Golden mirrors the Rust unit test
        # in crates/penca-core/src/naming/tables.rs (CAT as the table_uuid).
        assert (
            write_sequence(CAT, BR)
            == "dd2e0844-f72a-a0b7-f1ce-c28c26aa6bb3_data_write_seq"
        )

    # ── CHA-203: deterministic persist/snapshot UUID chain ──────────────
    # Cross-language parity goldens for the reshaped/new helpers.
    # The Rust mirror at ``crates/penca-core/src/naming.rs`` ships
    # matching ``test_parity_*`` cases with the same inputs and same
    # output UUIDs — rotating the hash function or hash-input format on
    # either side must update both test suites together.

    def test_uuid_chain_parity_table_persist_per_kind(self):
        # CHA-220: table_persist_uuid is chained directly off the catalog
        # UUID (no intermediate branch_persist parent). The four PK
        # values ``(branch_uuid, table_uuid, persisted_at_micros,
        # log_kind)`` discriminate one persist row from every other in
        # the catalog. Each log_kind feeds into a distinct
        # ``table_persist_uuid`` against the same ``(branch, table,
        # persisted_at)``.
        assert (
            table_persist_uuid(CAT, BR, TBL, 1000, "upsert_log")
            == "65bf9889-d637-f44b-1e72-5c306b8a8384"
        )
        assert (
            table_persist_uuid(CAT, BR, TBL, 1000, "delete_log")
            == "749d519b-c934-bb2c-cc61-21cd3884f4dd"
        )

    def test_uuid_chain_parity_table_purge(self):
        # CHA-236: purge anchor re-rooted on
        # ``deterministic_uuid_from(catalog_str, "purge")`` (the prior
        # ``__penca_system__.table_purge_metadata`` parent helper was
        # deleted along with the namespace hash family). Golden updated
        # to match the new hash input.
        assert (
            table_purge_uuid(CAT, BR, TBL, 1000)
            == "44732374-0641-a0ef-39ce-6df31462df6e"
        )

    def test_uuid_chain_parity_table_persist_segment(self):
        # Same input fixture chain as the reshaped
        # ``test_table_persist_segment_uuid`` above — pinned independently
        # here so the cross-language parity table is self-documenting.
        tf = table_persist_uuid(CAT, BR, TBL, 1000, "upsert_log")
        assert (
            table_persist_segment_uuid(tf, 0) == "9a7f445a-21fe-2917-280d-6d747215f54d"
        )

    def test_uuid_chain_parity_table_snapshot(self):
        # CHA-203: signature reshaped from
        # ``(data_log_prefix_uuid, snapshotted_at_micros)`` to
        # ``(catalog_uuid, branch_uuid, table_uuid,
        # snapshotted_at_micros)`` — identity-only inputs, no derived
        # prefix.
        assert (
            table_snapshot_uuid(CAT, BR, TBL, 1000)
            == "03576be6-3377-9185-99e8-aef297ac7996"
        )

    def test_uuid_chain_parity_table_snapshot_segment(self):
        # CHA-215: signature reshaped to ``(table_snapshot_uuid,
        # chunk_idx)`` — the snapshot-time chunker emits one row per
        # chunk under a single parent ``table_snapshot_uuid``, and
        # ``chunk_idx`` is the only uniquifier between siblings.
        snap = table_snapshot_uuid(CAT, BR, TBL, 1000)
        assert (
            table_snapshot_segment_uuid(snap, 0)
            == "8e2b58a1-c2e5-e39d-e8e1-90117e32cca2"
        )
