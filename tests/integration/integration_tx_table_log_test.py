"""Integration tests for the per-(tx, table) summary index (CHA-181).

`tx_table_log` records, per penca tx, the distinct tables that tx
wrote to. CHA-5 (merge conflict detection) and CHA-168 (branch-
coordinated persist) both need the index — this suite covers what
CHA-181 itself owns: the table, write-path emit, genesis seed, and
branch-fork emit.

Run via ``just integration-test``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import (
    MAIN_BRANCH_NAME,
    genesis_tx_uuid,
    system_schemas_table_uuid,
    system_tables_table_uuid,
    tx_table_log_partition,
)
from psycopg.sql import Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
)


def _qi(name: str) -> str:
    return Identifier(name).as_string(None)


def _tx_table_rows(catalog_uuid: str, branch_uuid: str, tx_uuid: str | None = None):
    """Return rows from a branch's tx_table_log partition.

    Filters by `tx_uuid` if given; otherwise returns the full partition
    contents. Each row is `(tx_uuid, branch_uuid, table_uuid)`.
    """
    part = tx_table_log_partition(catalog_uuid, branch_uuid)
    if tx_uuid is None:
        return get_pg_driver().execute(
            f"SELECT tx_uuid, branch_uuid, table_uuid FROM {_qi(part)}"
        )

    return get_pg_driver().execute(
        f"SELECT tx_uuid, branch_uuid, table_uuid FROM {_qi(part)} "
        f"WHERE tx_uuid = %s::uuid",
        (tx_uuid,),
    )


class TestTxTableLogWritePath:
    def test_one_row_per_distinct_table(self):
        """Tx writing to N tables → exactly N rows tagged with that tx_uuid.

        Bulk row count per table is irrelevant — the index records
        membership, not row counts. Two WriteData calls in the same tx
        must be idempotent on overlapping `(tx_uuid, table_uuid)`
        pairs (PK conflict handles dedup across calls).
        """
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"ttl_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "s", catalog_uuid=catalog_uuid, author="tester", comment="test setup"
        )
        table_a = client.create_table(
            "table_a",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="tester",
            comment="test setup",
        )
        table_b = client.create_table(
            "table_b",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="tester",
            comment="test setup",
        )
        table_c = client.create_table(
            "table_c",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="tester",
            comment="test setup",
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

        # Call 1: A (1 row) then B (3 rows) — two single-table writes in the
        # same tx. Row counts differ on purpose — emit is per-table, not per-row.
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_a,
                upserts=pa.table({"name": ["a1"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_b,
                upserts=pa.table(
                    {"name": ["b1", "b2", "b3"], "value": [1, 2, 3]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

        # Call 2: B again (idempotent) then C (new) — two single-table writes.
        # The PK conflict on (tx, B) must drop the duplicate without raising.
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_b,
                upserts=pa.table({"name": ["b4"], "value": [4]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_c,
                upserts=pa.table(
                    {"name": ["c1", "c2"], "value": [1, 2]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        rows = _tx_table_rows(catalog_uuid, main_branch_uuid, tx.tx_uuid)
        observed_tables = {str(r[2]) for r in rows}
        assert observed_tables == {table_a, table_b, table_c}, (
            f"expected one row per distinct table, got {observed_tables}"
        )
        assert len(rows) == 3
        for row in rows:
            assert str(row[0]) == tx.tx_uuid
            assert str(row[1]) == main_branch_uuid

    def test_genesis_seeds_system_tables(self):
        """CreateCatalog seeds tx_table_log with genesis-tx → system-table rows.

        Both `__penca_system__.schemas` and `__penca_system__.tables`
        get a row tagged with the catalog's genesis_tx so consumers can
        resolve system-table membership the same way they do user
        tables.
        """
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"ttl_gen_{uuid4().hex[:8]}", "owner"
        )

        genesis_uuid = genesis_tx_uuid(catalog_uuid)
        sys_schemas_uuid = system_schemas_table_uuid(catalog_uuid)
        sys_tables_uuid = system_tables_table_uuid(catalog_uuid)

        rows = _tx_table_rows(catalog_uuid, main_branch_uuid, genesis_uuid)
        observed = {str(r[2]) for r in rows}
        assert observed == {sys_schemas_uuid, sys_tables_uuid}


class TestTxTableLogBranchFork:
    def test_child_branch_has_only_fork_tx_rows(self):
        """CreateBranch's child partition holds fork_tx rows, not parent tx_uuids.

        Parent's old user tx_uuids do not exist on the child's commit_tx_log
        partition, so any tx_table_log row referencing them on the
        child would be unjoinable noise. CHA-181's fork emit only
        records what fork_tx physically wrote on the child — today,
        rows in `__penca_system__.tables` (one per materialized user
        table). Parent tx_uuids must NOT appear under child's
        branch_uuid.
        """
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"ttl_fork_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "s", catalog_uuid=catalog_uuid, author="tester", comment="test setup"
        )
        client.create_table(
            "user_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="tester",
            comment="test setup",
        )

        # Commit a user tx on main so parent has a non-genesis tx_uuid
        # in its tx_table_log. The child must NOT inherit this tx_uuid
        # under its branch_uuid.
        parent_tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            parent_tx.tx_uuid,
            Mutation(
                table_name="user_table",
                upserts=pa.table({"name": ["x"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            parent_tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        child_branch_uuid = client.create_branch(
            "feature",
            "tester",
            "fork test",
            source_branch_name=MAIN_BRANCH_NAME,
            catalog_uuid=catalog_uuid,
        ).branch_uuid

        sys_tables_uuid = system_tables_table_uuid(catalog_uuid)

        child_rows = _tx_table_rows(catalog_uuid, child_branch_uuid)
        child_tx_uuids = {str(r[0]) for r in child_rows}
        child_table_uuids = {str(r[2]) for r in child_rows}

        # No parent user tx_uuid leaks through the fork.
        assert parent_tx.tx_uuid not in child_tx_uuids
        # The fork emit recorded fork_tx → __penca_system__.tables
        # (the only physical write fork_tx makes today).
        assert sys_tables_uuid in child_table_uuids
        # Every child row carries the child's branch_uuid.
        for row in child_rows:
            assert str(row[1]) == child_branch_uuid
