"""Penca naming conventions and identity functions.

Python mirror of ``crates/penca-core/src/naming.rs``. Lives on the
client package so callers writing ad-hoc Python tooling against a
Penca deployment (direct PG queries, custom orchestration, debug
scripts) can compute the same deterministic UUIDs the server uses
without round-tripping through the gRPC API. The runtime client itself
does not import this module — every method that needs a UUID either
receives it from the user or gets it back in a server response.

Cross-language parity is locked by
``tests/static/static_naming_parity_test.py`` against the same
hardcoded inputs the Rust unit tests use; rotating the hash function
or changing the hash-input format on either side must update both
test suites together.

``catalog_store`` is the only globally-named table (it's the catalog
registry itself). All other core tables (``branch_store``, ``commit_tx_log``,
``begin_tx_log``, ``abort_tx_log``, ``tx_table_log``, etc.) are
per-catalog and prefixed with the owning catalog UUID. Per-branch data
tables — including the system tables under ``__penca_system__`` —
derive their per-branch PG names deterministically from
``(table_uuid, branch_uuid)`` via :func:`upsert_log_table` /
:func:`delete_log_table` (CHA-177); no separate physical-table UUID
is stored.

``row_uuid_for_pk`` produces a deterministic row identity from primary
key values so the same logical row has a stable UUID across branches
and merges.

# Identity model

Namespace UUIDs (``catalog_uuid``, ``schema_uuid``, ``branch_uuid``,
``table_uuid``) for user-created resources are server-minted random
at ``Create*`` time and persisted on the namespace row (CHA-236).
Their derivation lives on the server, not here — clients capture
them from the ``Create*Response``. This module only contains the
deterministic UUIDs the storage layer needs to address by well-known
per-catalog identity:

- **Structural anchors**: ``__penca_system__`` schema + its two
  bootstrap tables (``schemas``, ``tables``). Deterministic from
  ``catalog_uuid`` so server-internal write paths can address them
  without state. See :func:`system_schema_uuid`,
  :func:`system_schemas_table_uuid`,
  :func:`system_tables_table_uuid`.
- **Per-branch partition leaves**: the tx-log family. Each leaf's
  ``partition_uuid`` derives directly from
  ``(catalog_uuid, branch_uuid, partition_tag)``, where
  ``partition_tag`` is the fixed PG-name suffix (e.g. ``"commit_tx_log"``).
- **Auditable-store row identity**: :func:`row_uuid_for_pk` plus the
  persist + snapshot UUID chain (:func:`table_persist_uuid`,
  :func:`table_purge_uuid`, :func:`table_snapshot_uuid`, and
  their segment children). These take random ``table_uuid`` as input
  now; the chain structure is unchanged (ADR 0016).
"""

from __future__ import annotations

from collections.abc import Sequence

from xxhash import xxh3_128_hexdigest

# Format wire codes — mirror of the Rust `penca_core::Format` discriminants
# (1 = Lance, 2 = Parquet). Formerly the `StorageFormat` proto enum, removed
# with `storage_metadata.proto` in CHA-445 (the format code is an internal
# storage detail, not a wire type, now that the metadata service is gone).
FORMAT_EXTENSIONS: dict[int, str] = {
    1: "lance",
    2: "parquet",
}
FORMAT_FROM_TEXT: dict[str, int] = {v: k for k, v in FORMAT_EXTENSIONS.items()}


def format_to_text(fmt: int) -> str:
    """Convert a format wire code to its text name (e.g. 1 → 'lance')."""
    return FORMAT_EXTENSIONS[fmt]


def format_from_text(text: str) -> int:
    """Convert a format text name to its wire code (e.g. 'parquet' → 2)."""
    return FORMAT_FROM_TEXT[text]


CATALOG_STORE = "catalog_store"

MAIN_BRANCH_NAME = "main"
PUBLIC_SCHEMA_NAME = "public"
# Reserved schema for Penca-internal metadata exposed as first-class
# tables (CHA-164 Stage C). User DDL/DML against this schema is
# rejected at the API layer (CHA-236); the structural-anchor helpers
# below name its identity for server-internal write paths.
# Bootstrapped at CreateCatalog time alongside `public`.
SYSTEM_SCHEMA_NAME = "__penca_system__"

# Names of the well-known Penca Tables auto-bootstrapped inside
# `__penca_system__` at `CreateCatalog` time (CHA-177 / ADR 0012).
# Each is a real auditable-store Penca Table — same
# `{prefix}_data_{upsert,delete}_log` shape as user data, addressed via
# the two-arg hot-table helpers
# (:func:`upsert_log_table` / :func:`delete_log_table`).
SYSTEM_SCHEMAS_TABLE_NAME = "schemas"
SYSTEM_TABLES_TABLE_NAME = "tables"

TABLE_PERSIST_METADATA = "table_persist_metadata"
TABLE_PERSIST_SEGMENT_METADATA = "table_persist_segment_metadata"
TABLE_PURGE_METADATA = "table_purge_metadata"
TABLE_SNAPSHOT_METADATA = "table_snapshot_metadata"
TABLE_SNAPSHOT_SEGMENT_METADATA = "table_snapshot_segment_metadata"
# CHA-202: in-flight compact merged-file tracking (scoped by
# (branch_uuid, table_uuid)).
COMPACT_SEGMENT_METADATA = "compact_segment_metadata"
# CHA-233 / ADR 0019 §"Four-part mechanism" item 3: grace-bounded set
# of cold segment files queued for physical deletion. Compact enqueues
# rows inside its merge tx; sweep_segments drains them past grace.
SEGMENT_DELETE_SET = "segment_delete_set"


def branch_store_table(catalog_uuid: str) -> str:
    """Per-catalog branch store table name."""
    return f"{catalog_uuid}_branch_store"


def begin_tx_log_table(catalog_uuid: str) -> str:
    """Per-catalog begin-transaction log table name."""
    return f"{catalog_uuid}_begin_tx_log"


def abort_tx_log_table(catalog_uuid: str) -> str:
    """Per-catalog aborted-transaction log table name."""
    return f"{catalog_uuid}_abort_tx_log"


def commit_tx_log_table(catalog_uuid: str) -> str:
    """Per-catalog committed-transaction log table name."""
    return f"{catalog_uuid}_commit_tx_log"


def tx_table_log_table(catalog_uuid: str) -> str:
    """Per-catalog (tx, table) summary index table name (CHA-181)."""
    return f"{catalog_uuid}_tx_table_log"


def commit_tx_log_seq_num_table(catalog_uuid: str) -> str:
    """Per-catalog gapless commit-order counter table name (CHA-428)."""
    return f"{catalog_uuid}_commit_tx_log_seq_num"


def upsert_log_table(table_uuid: str, branch_uuid: str) -> str:
    """Per-branch data upsert log table name.

    Derives the data-log prefix internally as
    ``row_uuid_for_pk(table_uuid, [branch_uuid])`` — the deterministic
    data-object prefix for this ``(table_uuid, branch_uuid)``. CHA-177.
    """
    prefix = row_uuid_for_pk(table_uuid, [branch_uuid])
    return f"{prefix}_data_upsert_log"


def delete_log_table(table_uuid: str, branch_uuid: str) -> str:
    """Per-branch data delete log table name. See :func:`upsert_log_table`."""
    prefix = row_uuid_for_pk(table_uuid, [branch_uuid])
    return f"{prefix}_data_delete_log"


def write_sequence(table_uuid: str, branch_uuid: str) -> str:
    """Per-(table, branch) ``write_sequence`` (CHA-431) — the lock-free
    Postgres SEQUENCE the intra-tx ``write_seq_num`` ordinal is allocated from
    via ``nextval``. Shares the data-object prefix with the upsert/delete logs
    (same ``row_uuid_for_pk(table_uuid, [branch_uuid])``); no ``catalog_uuid``.
    """
    prefix = row_uuid_for_pk(table_uuid, [branch_uuid])
    return f"{prefix}_data_write_seq"


# The tx-log family (commit_tx_log, begin_tx_log, abort_tx_log) plus the
# per-tx affected-tables index (tx_table_log, CHA-181) are
# LIST-partitioned by branch_uuid (one leaf per branch). Each
# partition_uuid derives directly from
# ``row_uuid_for_pk(catalog_uuid, [branch_uuid, partition_tag])`` where
# ``partition_tag`` is the fixed PG-name suffix (e.g. ``"commit_tx_log"``).
# Schemas and tables are real Penca Tables under
# ``__penca_system__.{schemas,tables}`` (CHA-177) — they get per-branch
# physicals via :func:`upsert_log_table` / :func:`delete_log_table`, no
# PG partitioning.


def commit_tx_log_partition(catalog_uuid: str, branch_uuid: str) -> str:
    """Partition of commit_tx_log for a specific branch."""
    partition_uuid = row_uuid_for_pk(catalog_uuid, [branch_uuid, "commit_tx_log"])
    return f"{partition_uuid}_commit_tx_log_partition"


def commit_tx_log_seq_num_partition(catalog_uuid: str, branch_uuid: str) -> str:
    """Partition of commit_tx_log_seq_num for a specific branch (CHA-428)."""
    partition_uuid = row_uuid_for_pk(
        catalog_uuid, [branch_uuid, "commit_tx_log_seq_num"]
    )
    return f"{partition_uuid}_commit_tx_log_seq_num_partition"


def begin_tx_log_partition(catalog_uuid: str, branch_uuid: str) -> str:
    """Partition of begin_tx_log for a specific branch."""
    partition_uuid = row_uuid_for_pk(catalog_uuid, [branch_uuid, "begin_tx_log"])
    return f"{partition_uuid}_begin_tx_log_partition"


def abort_tx_log_partition(catalog_uuid: str, branch_uuid: str) -> str:
    """Partition of abort_tx_log for a specific branch."""
    partition_uuid = row_uuid_for_pk(catalog_uuid, [branch_uuid, "abort_tx_log"])
    return f"{partition_uuid}_abort_tx_log_partition"


def tx_table_log_partition(catalog_uuid: str, branch_uuid: str) -> str:
    """Partition of tx_table_log for a specific branch (CHA-181)."""
    partition_uuid = row_uuid_for_pk(catalog_uuid, [branch_uuid, "tx_table_log"])
    return f"{partition_uuid}_tx_table_log_partition"


def deterministic_uuid_from(*parts: str) -> str:
    """Generic deterministic UUID combiner.

    Hashes the ``\\x00``-joined parts via xxh3_128 and formats as a
    UUID. Lower-level primitive behind every Penca deterministic UUID
    — the three structural anchors below, the log-partition prefix
    computed inside :func:`upsert_log_table` / :func:`delete_log_table`,
    and the persist + snapshot chain helpers.

    For row identity within a parent table, use :func:`row_uuid_for_pk`
    instead — it captures the row-PK semantics explicitly.
    """
    hex_digest = xxh3_128_hexdigest("\x00".join(parts))
    return _hash_to_uuid(hex_digest)


def row_uuid_for_pk(parent_uuid: str, pk_values: Sequence[object]) -> str:
    """Compute a deterministic row_uuid from a parent UUID + PK values.

    Row-identity semantic: a row in some parent table identified by
    ``parent_uuid`` (the table's ``table_uuid``), keyed by the user PK
    values. Identical PKs in different parent tables produce different
    row_uuids because the parent UUID is part of the hash input.
    """
    return deterministic_uuid_from(parent_uuid, *(str(v) for v in pk_values))


def version_uuid(row_uuid: str, tx_uuid: str) -> str:
    """Deterministic ``version_uuid`` for an auditable-store row.

    `version_uuid` is the PRIMARY KEY of every auditable-store table
    (data + metadata). Deriving it deterministically from
    `(row_uuid, tx_uuid)` means the PK alone enforces the
    auditable-store invariant: at most one version per (entity, tx) —
    a second insert with the same `(row_uuid, tx_uuid)` produces the
    same `version_uuid` and trips the PK constraint. No separate
    `UNIQUE(row_uuid, tx_uuid)` index needed.

    See ADR 0013 for the rationale (and ADR 0011 for the broader
    auditable-store-as-transactional-store framing).
    """
    return deterministic_uuid_from(row_uuid, tx_uuid)


def genesis_tx_uuid(catalog_uuid: str) -> str:
    """Deterministic genesis transaction UUID for a catalog.

    The genesis tx is the first committed transaction in a catalog,
    inserted at CreateCatalog into the catalog's commit_tx_log. All branches
    reference it as their root.
    """
    return deterministic_uuid_from(catalog_uuid)


# The `__penca_system__` schema and its two bootstrap tables are
# the only namespace objects whose UUIDs stay deterministic — server-
# internal write paths address them by well-known per-catalog identity
# (e.g. when bootstrapping rows in `__penca_system__.tables` from
# `CreateTable`). User-created schemas/tables are random-minted per
# CHA-236. The three anchors below all use arity-2 catalog-rooted
# `deterministic_uuid_from([catalog_str, tag])` with distinct tag
# strings.


def system_schema_uuid(catalog_uuid: str) -> str:
    """`schema_uuid` for the `__penca_system__` schema."""
    return deterministic_uuid_from(catalog_uuid, SYSTEM_SCHEMA_NAME)


def system_schemas_table_uuid(catalog_uuid: str) -> str:
    """`table_uuid` for `__penca_system__.schemas`."""
    return deterministic_uuid_from(catalog_uuid, "__penca_system__.schemas")


def system_tables_table_uuid(catalog_uuid: str) -> str:
    """`table_uuid` for `__penca_system__.tables`."""
    return deterministic_uuid_from(catalog_uuid, "__penca_system__.tables")


def system_indexes_table_uuid(catalog_uuid: str) -> str:
    """`table_uuid` for `__penca_system__.indexes` (CHA-455)."""
    return deterministic_uuid_from(catalog_uuid, "__penca_system__.indexes")


def system_name_index_uuid(system_table_uuid: str) -> str:
    """Deterministic non-NULL ``index_uuid`` for a system table's built-in
    composite name index (CHA-481). Derived from the system table's own
    ``table_uuid``; mirrors ``penca_core::naming::system_name_index_uuid``."""
    return row_uuid_for_pk(system_table_uuid, ["name_index"])


def _hash_to_uuid(hex_digest: str) -> str:
    return f"{hex_digest[:8]}-{hex_digest[8:12]}-{hex_digest[12:16]}-{hex_digest[16:20]}-{hex_digest[20:]}"


# CHA-203: persist + snapshot UUIDs derive deterministically from their
# parent UUID + own discriminators via :func:`row_uuid_for_pk`. Phase-1
# retries replay to identical UUIDs at every level — `ON CONFLICT DO
# UPDATE` keeps the writes idempotent. See ADR 0016.


def table_persist_uuid(
    catalog_uuid: str,
    branch_uuid: str,
    table_uuid: str,
    persisted_at_micros: int,
    log_kind: str,
) -> str:
    """Deterministic table_persist UUID for ``(branch, table, persisted_at, log_kind)``.

    CHA-220 — chained directly off ``catalog_uuid`` after removing the
    intermediate ``branch_persist_metadata`` parent.
    """
    return row_uuid_for_pk(
        catalog_uuid,
        [branch_uuid, table_uuid, persisted_at_micros, log_kind],
    )


def table_purge_uuid(
    catalog_uuid: str,
    branch_uuid: str,
    table_uuid: str,
    purged_at_micros: int,
) -> str:
    """Deterministic table_purge UUID for ``(branch, table, purged_at)``.

    Rooted on a catalog-scoped ``"purge"`` anchor rather than on
    ``catalog_uuid`` directly — same arity-3 PK tuple as
    :func:`table_snapshot_uuid` would otherwise collide. The
    ``deterministic_uuid_from(catalog_str, "purge")`` anchor keeps the
    hash-input space disjoint without needing a discriminator tag in
    the outer ``row_uuid_for_pk`` call.
    """
    purge_anchor = deterministic_uuid_from(catalog_uuid, "purge")
    return row_uuid_for_pk(
        purge_anchor,
        [branch_uuid, table_uuid, purged_at_micros],
    )


def table_persist_segment_uuid(
    table_persist_uuid: str,
    chunk_idx: int,
) -> str:
    """Deterministic UUID for a table persist segment.

    ``chunk_idx`` distinguishes sibling segments emitted by the
    persist-time chunker (CHA-215) within the same persist event.
    """
    return row_uuid_for_pk(table_persist_uuid, [chunk_idx])


def table_snapshot_uuid(
    catalog_uuid: str,
    branch_uuid: str,
    table_uuid: str,
    snapshotted_at_micros: int,
) -> str:
    """Deterministic UUID for a snapshot (table-level record)."""
    return row_uuid_for_pk(
        catalog_uuid,
        [branch_uuid, table_uuid, snapshotted_at_micros],
    )


def table_snapshot_segment_uuid(
    table_snapshot_uuid: str,
    chunk_idx: int,
) -> str:
    """Deterministic UUID for a snapshot segment.

    ``chunk_idx`` distinguishes sibling segments emitted by the
    snapshot-time chunker (CHA-215) within the same snapshot cycle.
    """
    return row_uuid_for_pk(table_snapshot_uuid, [chunk_idx])


# CHA-203: cold URIs live under
# ``{base_uri}/{catalog_uuid}/{branch_uuid}/{persist|snapshot}/{parent_uuid}/{segment_uuid}/data.{ext}``.


def persist_segment_uri(
    base_uri: str,
    catalog_uuid: str,
    branch_uuid: str,
    table_persist_uuid: str,
    segment_uuid: str,
    extension: str = "parquet",
) -> str:
    """URI for a cold persist segment file."""
    return (
        f"{base_uri}/{catalog_uuid}/{branch_uuid}/persist"
        f"/{table_persist_uuid}/{segment_uuid}/data.{extension}"
    )


def snapshot_segment_uri(
    base_uri: str,
    catalog_uuid: str,
    branch_uuid: str,
    table_snapshot_uuid: str,
    segment_uuid: str,
    extension: str = "parquet",
) -> str:
    """URI for a cold snapshot segment file."""
    return (
        f"{base_uri}/{catalog_uuid}/{branch_uuid}/snapshot"
        f"/{table_snapshot_uuid}/{segment_uuid}/data.{extension}"
    )
