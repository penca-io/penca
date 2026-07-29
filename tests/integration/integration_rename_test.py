"""Acceptance tests for non-deterministic namespace UUIDs + rename (CHA-236).

Three behavioral surfaces covered here:

1. **Random UUIDs + name uniqueness** (tests 1-5). ``Create{Catalog,
   Schema,Table,Branch}`` mints a random UUID server-side and persists
   it on the row; recreating after delete returns a new UUID. Name
   uniqueness is enforced server-side (PG ``UNIQUE`` for
   catalog/branch, within-tx existence check for schema/table).
2. **Rename via ``new_*_name`` on Update messages** (tests 6-14).
   ``Update{Catalog,Schema,Table,Branch}`` accept
   ``new_{catalog,schema,table,branch}_name``; the ``*_uuid`` stays
   put, data + persist chain unaffected, per-branch only.
3. **System-table lockdown** (tests 15-18). Every mutating handler
   rejects targets that resolve to the three structural anchors
   (``__penca_system__``, ``__penca_system__.schemas``,
   ``__penca_system__.tables``) with ``INVALID_ARGUMENT``.

Tests against the **future shape**: ``create_catalog`` returns
``(catalog_uuid, main_branch_uuid)``; ``update_*`` accept ``new_*_name``;
``update_branch`` exists. Tests written against the current shape will
be unsatisfiable in commit 1's red state — that's the right red,
distinct from a setup bug. See the pinned plan comment on CHA-236.

Run via ``just integration-test rename``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.errors import ApiError, InvalidRequestError, NotFoundError
from penca_client.naming import (
    MAIN_BRANCH_NAME,
    SYSTEM_SCHEMA_NAME,
    SYSTEM_SCHEMAS_TABLE_NAME,
    SYSTEM_TABLES_TABLE_NAME,
)

from .integration_helpers import USER_SCHEMA, make_client


def _new_catalog(client, prefix: str = "rename_cat") -> tuple[str, str, str]:
    """Create a fresh catalog and return ``(name, catalog_uuid, main_branch_uuid)``.

    Captures ``main_branch_uuid`` from the ``CreateCatalogResponse``
    rather than deriving it via the (deprecated) hash helper — CHA-236
    makes namespace UUIDs server-minted, so the response is the only
    source of truth for the main branch's UUID.
    """
    name = f"{prefix}_{uuid4().hex[:8]}"
    # Plan §"Python client": ``create_catalog`` returns
    # ``(catalog_uuid, main_branch_uuid)`` post-CHA-236.
    catalog_uuid, main_branch_uuid = client.create_catalog(name, "owner")
    return name, catalog_uuid, main_branch_uuid


def _make_schema_and_table(
    client,
    catalog_uuid: str,
    *,
    schema_name: str = "s1",
    table_name: str = "t1",
) -> tuple[str, str]:
    schema_uuid = client.create_schema(
        schema_name,
        catalog_uuid=catalog_uuid,
        author="test",
        comment="create_schema",
    )
    table_uuid = client.create_table(
        table_name,
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="create_table",
    )
    return schema_uuid, table_uuid


def _write_rows(
    client,
    *,
    catalog_uuid: str,
    schema_uuid: str,
    branch_uuid: str,
    table_uuid: str,
    rows: dict[str, list],
) -> None:
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    batch = pa.table(rows, schema=USER_SCHEMA)
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )


class TestRandomUuidsAndNameUniqueness:
    def test_create_catalog_returns_random_uuid_and_main_branch_uuid(self):
        """CHA-236 #1: CreateCatalog mints a random ``catalog_uuid`` and
        a random ``main_branch_uuid`` server-side. Two ``CreateCatalog``
        calls with the same name (after delete) return different UUIDs;
        the response carries a non-empty ``main_branch_uuid`` field.

        Today: ``catalog_uuid = xxh3(catalog_name)`` is deterministic,
        so name reuse returns the same UUID. ``CreateCatalogResponse``
        does not carry ``main_branch_uuid``.
        """
        client = make_client()
        name = f"random_cat_{uuid4().hex[:8]}"

        first_catalog_uuid, first_main_branch_uuid = client.create_catalog(
            name, "owner"
        )
        assert first_main_branch_uuid, (
            "CreateCatalog must return a non-empty main_branch_uuid"
        )
        client.delete_catalog(catalog_uuid=first_catalog_uuid)

        second_catalog_uuid, second_main_branch_uuid = client.create_catalog(
            name, "owner"
        )

        assert first_catalog_uuid != second_catalog_uuid, (
            "namespace UUIDs must be random — name reuse must not collide"
        )
        assert first_main_branch_uuid != second_main_branch_uuid, (
            "main_branch_uuid must also be random per catalog"
        )

    def test_create_table_returns_random_uuid(self):
        """CHA-236 #2: CreateTable mints a random ``table_uuid``. Two
        ``CreateTable`` calls with the same ``(schema, name)`` pair
        (after delete) return different UUIDs.
        """
        client = make_client()
        _, catalog_uuid, _ = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )

        first_uuid = client.create_table(
            "t",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        client.delete_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=first_uuid,
            author="test",
            comment="delete_table",
        )
        second_uuid = client.create_table(
            "t",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        assert first_uuid != second_uuid

    def test_create_table_same_name_same_branch_fails_already_exists(self):
        """CHA-236 #3: a second CreateTable with the same
        ``(schema, table_name)`` on the same branch returns
        ``ALREADY_EXISTS`` (server-side within-tx existence check, plan
        §"Validation surface")."""
        client = make_client()
        _, catalog_uuid, _ = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        client.create_table(
            "t",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        with pytest.raises(ApiError) as exc_info:
            client.create_table(
                "t",
                USER_SCHEMA,
                primary_keys=["name"],
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                author="test",
                comment="create_table",
            )

        assert (
            "ALREADY_EXISTS" in str(exc_info.value)
            or "already exists" in str(exc_info.value).lower()
        )

    def test_create_catalog_name_collision_fails(self):
        """CHA-236 #4: a second CreateCatalog with the same name fails
        loudly (PG ``UNIQUE (catalog_name)`` on ``catalog_store``)."""
        client = make_client()
        name = f"dup_cat_{uuid4().hex[:8]}"
        client.create_catalog(name, "owner")

        with pytest.raises(ApiError) as exc_info:
            client.create_catalog(name, "owner")

        assert (
            "ALREADY_EXISTS" in str(exc_info.value)
            or "already exists" in str(exc_info.value).lower()
        )

    def test_create_branch_name_collision_fails(self):
        """CHA-236 #5: a second CreateBranch with the same name within
        a catalog fails (PG ``UNIQUE (branch_name)`` on per-catalog
        ``branch_store``)."""
        client = make_client()
        _, catalog_uuid, _ = _new_catalog(client)
        client.create_branch(
            "feat",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )

        with pytest.raises(ApiError) as exc_info:
            client.create_branch(
                "feat",
                catalog_uuid=catalog_uuid,
                author="test",
                comment="create_branch",
            )

        assert (
            "ALREADY_EXISTS" in str(exc_info.value)
            or "already exists" in str(exc_info.value).lower()
        )


class TestRename:
    def test_rename_table_via_update_table_new_table_name(self):
        """CHA-236 #6: UpdateTable(new_table_name=...) renames the table.
        After rename, lookup by new name succeeds; lookup by old name
        returns NOT_FOUND; ``table_uuid`` is unchanged."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid, table_uuid = _make_schema_and_table(
            client, catalog_uuid, table_name="foo"
        )

        client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            new_table_name="bar",
            author="test",
            comment="rename foo to bar",
        )

        bar = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_name="bar",
            branch_uuid=main_branch_uuid,
        )
        assert bar.table_uuid == table_uuid
        assert bar.table_name == "bar"

        with pytest.raises(NotFoundError):
            client.get_table(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_name="foo",
                branch_uuid=main_branch_uuid,
            )

    def test_rename_table_preserves_data_and_persist_chain(self):
        """CHA-236 #7: rename is metadata-only. Rows written under the
        old name are readable under the new name; subsequent Persist +
        Snapshot complete on the renamed table; persist chain stays
        rooted on the unchanged ``table_uuid``."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid, table_uuid = _make_schema_and_table(
            client, catalog_uuid, table_name="foo"
        )

        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            rows={"name": ["alice", "bob"], "value": [1, 2]},
        )

        client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            new_table_name="bar",
            author="test",
            comment="rename foo to bar",
        )

        after = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_name="bar",
            branch_uuid=main_branch_uuid,
        )
        assert sorted(after.column("name").to_pylist()) == ["alice", "bob"]

        # Persist + Snapshot complete on the renamed table — addressed
        # by ``table_uuid`` so naming churn is invisible to lifecycle.
        persist = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        assert persist.persisted_at_micros > 0
        snap = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        assert snap.snapshotted_at_micros > 0

    def test_rename_table_collision_fails(self):
        """CHA-236 #8: UpdateTable(new_table_name="bar") when a different
        table on the branch is already named ``bar`` returns
        ``ALREADY_EXISTS``."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid, foo_uuid = _make_schema_and_table(
            client, catalog_uuid, table_name="foo"
        )
        client.create_table(
            "bar",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        with pytest.raises(ApiError) as exc_info:
            client.update_table(
                USER_SCHEMA,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
                table_uuid=foo_uuid,
                primary_keys=["name"],
                new_table_name="bar",
                author="test",
                comment="rename collision",
            )

        assert (
            "ALREADY_EXISTS" in str(exc_info.value)
            or "already exists" in str(exc_info.value).lower()
        )

    def test_rename_schema_via_update_schema_new_schema_name(self):
        """CHA-236 #9: UpdateSchema(new_table_name=...) renames the schema in
        place. ``schema_uuid`` unchanged; lookup-by-new-name succeeds;
        lookup-by-old-name returns NOT_FOUND."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s_old",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )

        client.update_schema(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            new_schema_name="s_new",
            author="test",
            comment="rename schema",
        )

        s_new = client.get_schema(
            catalog_uuid=catalog_uuid,
            schema_name="s_new",
            branch_uuid=main_branch_uuid,
        )
        assert s_new.schema_uuid == schema_uuid
        assert s_new.schema_name == "s_new"

        with pytest.raises(NotFoundError):
            client.get_schema(
                catalog_uuid=catalog_uuid,
                schema_name="s_old",
                branch_uuid=main_branch_uuid,
            )

    def test_rename_branch_via_update_branch_new_branch_name(self):
        """CHA-236 #10: UpdateBranch(new_schema_name=...) (new RPC) renames a
        branch. ``branch_uuid`` unchanged; ``get_branch`` by new name
        succeeds; by old name returns NOT_FOUND."""
        client = make_client()
        _, catalog_uuid, _ = _new_catalog(client)
        branch = client.create_branch(
            "old_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )

        client.update_branch(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
            new_branch_name="new_branch",
        )

        new_b = client.get_branch(
            catalog_uuid=catalog_uuid,
            branch_name="new_branch",
        )
        assert new_b.branch_uuid == branch.branch_uuid
        assert new_b.branch_name == "new_branch"

        with pytest.raises(NotFoundError):
            client.get_branch(
                catalog_uuid=catalog_uuid,
                branch_name="old_branch",
            )

    def test_rename_catalog_via_update_catalog_new_catalog_name(self):
        """CHA-236 #11: UpdateCatalog(new_branch_name=...) renames a catalog.
        ``catalog_uuid`` unchanged; lookup-by-new-name succeeds; old
        name returns NOT_FOUND."""
        client = make_client()
        original_name = f"orig_cat_{uuid4().hex[:8]}"
        new_name = f"renamed_cat_{uuid4().hex[:8]}"
        catalog_uuid, _ = client.create_catalog(original_name, "owner")

        client.update_catalog(
            catalog_uuid=catalog_uuid,
            new_catalog_name=new_name,
            owner="owner",
        )

        renamed = client.get_catalog(catalog_uuid=catalog_uuid)
        assert renamed.catalog_uuid == catalog_uuid
        assert renamed.catalog_name == new_name

        with pytest.raises(NotFoundError):
            # Older clients call ``update_catalog`` to address by name —
            # but ``get_catalog`` only takes UUID. We probe missing-name
            # via DeleteCatalog by name instead, which exercises the
            # same lookup path.
            client.delete_catalog(catalog_name=original_name)

    def test_rename_per_branch_only(self):
        """CHA-236 #12: rename on a child branch does not affect the
        parent branch. The same ``table_uuid`` resolves to different
        ``table_name`` on the two branches."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid, table_uuid = _make_schema_and_table(
            client, catalog_uuid, table_name="foo"
        )

        child = client.create_branch(
            "child",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )

        # Rename only on child.
        client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=child.branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            new_table_name="foo_on_child",
            author="test",
            comment="rename on child only",
        )

        on_main = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        on_child = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=child.branch_uuid,
        )

        assert on_main.table_name == "foo"
        assert on_child.table_name == "foo_on_child"
        assert on_main.table_uuid == on_child.table_uuid == table_uuid

    def test_uuid_addressing_survives_rename(self):
        """CHA-236 #13: a client that persists ``table_uuid`` (captured
        from ``CreateTableResponse``) can address the table by UUID
        after rename — the recommended-stability addressing form."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid, table_uuid = _make_schema_and_table(
            client, catalog_uuid, table_name="orig"
        )

        client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            new_table_name="renamed",
            author="test",
            comment="rename",
        )

        info = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert info.table_uuid == table_uuid
        assert info.table_name == "renamed"

    def test_lifecycle_unaffected_by_rename(self):
        """CHA-236 #14: full Persist + Snapshot + Purge cycle on a
        table renamed mid-cycle. Lifecycle addresses by UUID, so the
        rename is invisible to it. Stand-in for the
        ``LifecycleScheduler``-driven test — the scheduler binary just
        calls the same per-table RPCs."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid, table_uuid = _make_schema_and_table(
            client, catalog_uuid, table_name="lc_orig"
        )

        # Initial write + persist under the original name.
        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            rows={"name": ["pre"], "value": [1]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )

        # Rename mid-cycle.
        client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            new_table_name="lc_renamed",
            author="test",
            comment="rename mid-cycle",
        )

        # Write more rows post-rename, then complete the cycle.
        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            rows={"name": ["post"], "value": [2]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        snap = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        assert snap.snapshotted_at_micros > 0

        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_name="lc_renamed",
            branch_uuid=main_branch_uuid,
        )
        assert sorted(result.column("name").to_pylist()) == ["post", "pre"]


# Reads against ``__penca_system__.{schemas,tables}`` remain allowed —
# users discover their catalog through them. Mutating handlers reject
# targets that resolve to the three structural anchors with
# INVALID_ARGUMENT (plan §"Validation surface").


def _system_tables_table_uuid(client, catalog_uuid: str) -> str:
    """Discover ``__penca_system__.tables``'s ``table_uuid`` via the
    read API. The Python ``system_tables_table_uuid`` helper is
    deleted in CHA-236; clients now query the system schema directly."""
    info = client.get_table(
        catalog_uuid=catalog_uuid,
        schema_name=SYSTEM_SCHEMA_NAME,
        table_name=SYSTEM_TABLES_TABLE_NAME,
        branch_name=MAIN_BRANCH_NAME,
    )
    return info.table_uuid


def _system_schema_uuid(client, catalog_uuid: str) -> str:
    info = client.get_schema(
        catalog_uuid=catalog_uuid,
        schema_name=SYSTEM_SCHEMA_NAME,
        branch_name=MAIN_BRANCH_NAME,
    )
    return info.schema_uuid


class TestSystemTableLockdown:
    def test_create_table_in_system_schema_rejected(self):
        """CHA-236 #15: CreateTable targeting ``__penca_system__``
        rejected with INVALID_ARGUMENT — namespace metadata is managed
        exclusively through CRUD on schemas/tables."""
        client = make_client()
        _, catalog_uuid, _ = _new_catalog(client)

        with pytest.raises(InvalidRequestError) as exc_info:
            client.create_table(
                "user_attempt",
                USER_SCHEMA,
                primary_keys=["name"],
                catalog_uuid=catalog_uuid,
                schema_name=SYSTEM_SCHEMA_NAME,
                author="test",
                comment="create_table",
            )

        assert SYSTEM_SCHEMA_NAME in str(exc_info.value)

    def test_update_table_targeting_system_table_rejected(self):
        """CHA-236 #16: UpdateTable targeting
        ``__penca_system__.tables`` rejected with INVALID_ARGUMENT —
        users cannot rename a structural anchor."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        sys_tables_uuid = _system_tables_table_uuid(client, catalog_uuid)
        sys_schema_uuid = _system_schema_uuid(client, catalog_uuid)

        with pytest.raises(InvalidRequestError):
            client.update_table(
                USER_SCHEMA,
                catalog_uuid=catalog_uuid,
                schema_uuid=sys_schema_uuid,
                branch_uuid=main_branch_uuid,
                table_uuid=sys_tables_uuid,
                primary_keys=["name"],
                new_table_name="hijacked",
                author="test",
                comment="hijack attempt",
            )

    def test_delete_schema_targeting_system_schema_rejected(self):
        """CHA-236 #17: DeleteSchema targeting ``__penca_system__``
        rejected with INVALID_ARGUMENT."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        sys_schema_uuid = _system_schema_uuid(client, catalog_uuid)

        with pytest.raises(InvalidRequestError):
            client.delete_schema(
                catalog_uuid=catalog_uuid,
                schema_uuid=sys_schema_uuid,
                branch_uuid=main_branch_uuid,
                author="test",
                comment="delete_schema",
            )

    def test_write_data_targeting_system_table_rejected(self):
        """CHA-236 #18: WriteData with a ``Change.table_uuid`` pointing
        at ``__penca_system__.tables`` rejected with INVALID_ARGUMENT
        and the redirect-to-CRUD message. Validated inside WriteData's
        per-Change loop so a request bundling user-table mutations + a
        sneaky system-table mutation rejects cleanly."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        sys_tables_uuid = _system_tables_table_uuid(client, catalog_uuid)
        sys_schema_uuid = _system_schema_uuid(client, catalog_uuid)

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=sys_schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        # The Arrow schema of ``__penca_system__.tables`` is irrelevant
        # here — the validation check fires before any payload decode.
        sneaky = pa.table(
            {"name": ["x"], "value": [0]},
            schema=USER_SCHEMA,
        )

        with pytest.raises(InvalidRequestError) as exc_info:
            client.write_data(
                tx.tx_uuid,
                Mutation(table_uuid=sys_tables_uuid, upserts=sneaky),
                catalog_uuid=catalog_uuid,
                schema_uuid=sys_schema_uuid,
                branch_uuid=main_branch_uuid,
            )

        assert (
            "CRUD" in str(exc_info.value)
            or "managed exclusively" in str(exc_info.value)
            or SYSTEM_SCHEMAS_TABLE_NAME in str(exc_info.value)
            or SYSTEM_TABLES_TABLE_NAME in str(exc_info.value)
        )
