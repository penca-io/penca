"""Integration tests for index-definition branch-fork inheritance (CHA-455).

Index definitions live in the auditable ``__penca_system__.indexes``
store, so a CreateBranch must materialize the parent's index rows onto
the child (mirroring schemas/tables), and the two branches diverge
independently thereafter. Run via ``just integration-test query lifecycle``.

Red-phase: before the CHA-455 implementation lands, ``client.create_index``
does not exist (AttributeError) and there is no fork copy path, so the
single test below fails. Its three invariants (inherit, drop-isolation,
create-isolation) are asserted in sequence within one test.
"""

from __future__ import annotations

from uuid import uuid4

from .integration_helpers import (
    SCALAR_BTREE,
    make_client,
    setup_schema,
)


def _make_branch(client, catalog_uuid, name):
    branch = client.create_branch(
        f"{name}_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-455-fork",
    )
    return branch.branch_uuid


class TestIndexBranchFork:
    def test_index_inherited_then_independent(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch = setup_schema(client)

        # Define on main, then fork.
        client.create_index(
            table_name="write_table",
            index_name="inherited",
            columns=["name"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch,
            author="test",
            comment="parent",
        )
        child_branch = _make_branch(client, catalog_uuid, "child")

        # (1) Inherited on the child with identical definition.
        child_listed = client.list_indexes(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=child_branch,
        )
        assert [i.index_name for i in child_listed] == ["inherited"]
        child_idx = child_listed[0]
        assert list(child_idx.columns) == ["name"]
        assert child_idx.index_type == SCALAR_BTREE

        # (2) Dropping on the child leaves the parent untouched.
        client.delete_index(
            table_name="write_table",
            index_name="inherited",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=child_branch,
            author="test",
            comment="child-drop",
        )
        parent_listed = client.list_indexes(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch,
        )
        assert [i.index_name for i in parent_listed] == ["inherited"]

        # (3) A new index on the child is invisible on the parent.
        client.create_index(
            table_name="write_table",
            index_name="child_only",
            columns=["value"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=child_branch,
            author="test",
            comment="child-new",
        )
        parent_names = {
            i.index_name
            for i in client.list_indexes(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch,
            )
        }
        assert "child_only" not in parent_names
