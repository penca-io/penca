"""Integration tests for catalog-scoped branch RPCs (CHA-184).

CreateBranch / DeleteBranch / MergeBranch span every schema in
their owning catalog — branches are catalog-scoped (git-like), so
materializing tables, cleaning cold-storage, and copying merge data
all walk every schema.

Run via ``just integration-test branch``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.errors import NotFoundError
from penca_client.naming import (
    MAIN_BRANCH_NAME,
    PUBLIC_SCHEMA_NAME,
    SYSTEM_SCHEMA_NAME,
    TABLE_PERSIST_SEGMENT_METADATA,
    branch_store_table,
    commit_tx_log_partition,
    delete_log_table,
    system_schemas_table_uuid,
    system_tables_table_uuid,
    tx_table_log_partition,
    upsert_log_table,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
)


def _qi(name: str) -> str:
    return Identifier(name).as_string(None)


def _setup_two_schema_catalog(client):
    """Create a catalog with two user schemas (s1, s2), each with one table.

    Returns ``(catalog_uuid, s1_uuid, s2_uuid, t1_uuid, t2_uuid,
    main_branch_uuid)``. All work happens on main; branches created from this
    fork from main's head (no explicit fork position).
    """
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"branch_cat_{uuid4().hex[:8]}", "owner"
    )

    s1_uuid = client.create_schema(
        "s1",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="create_schema",
    )
    s2_uuid = client.create_schema(
        "s2",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="create_schema",
    )

    t1_uuid = client.create_table(
        "t1",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=s1_uuid,
        author="test",
        comment="create_table",
    )
    t2_uuid = client.create_table(
        "t2",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=s2_uuid,
        author="test",
        comment="create_table",
    )

    return (
        catalog_uuid,
        s1_uuid,
        s2_uuid,
        t1_uuid,
        t2_uuid,
        main_branch_uuid,
    )


def _write_rows(
    client,
    *,
    catalog_uuid,
    schema_uuid,
    branch_uuid,
    table_uuid,
    rows,
):
    """Open + commit one tx that upserts ``rows`` into ``table_uuid``.

    Returns the committed tx.
    """
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
    return client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )


def _persist_seg_count(catalog_uuid, branch_uuid, table_uuid) -> int:
    """Count cold persist segments for a ``(branch, table)`` via direct PG."""
    tfsm = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl} WHERE branch_uuid = %s AND table_uuid = %s"
        ).format(tbl=Identifier(tfsm)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0]


class TestCreateBranchAllSchemas:
    def test_multi_schema_create_branch_materializes_every_schemas_tables(self):
        """CreateBranch on a catalog with 2 user schemas materializes
        tables in BOTH schemas on the child branch, not just one."""
        client = make_client()
        (
            catalog_uuid,
            s1_uuid,
            s2_uuid,
            t1_uuid,
            t2_uuid,
            _main_branch_uuid,
        ) = _setup_two_schema_catalog(client)

        # CreateBranch — catalog-wide.
        client.create_branch(
            "child",
            "test",
            "create_branch",
            catalog_uuid=catalog_uuid,
        )

        # All four schemas (s1, s2, public, __penca_system__) appear
        # via list_schemas on the child branch.
        schemas = list(
            client.list_schemas(
                catalog_uuid=catalog_uuid,
                branch_name="child",
            )
        )
        schema_names = {s.schema_name for s in schemas}
        assert {"s1", "s2", PUBLIC_SCHEMA_NAME, SYSTEM_SCHEMA_NAME}.issubset(
            schema_names
        ), f"expected s1/s2/public/system schemas on child, got {schema_names}"

        # list_tables on child for s1 returns t1 with the same arrow
        # schema as on main.
        tables_s1 = list(
            client.list_tables(
                catalog_uuid=catalog_uuid,
                schema_uuid=s1_uuid,
                branch_name="child",
            )
        )
        names_s1 = {t.table_name for t in tables_s1}
        uuids_s1 = {t.table_uuid for t in tables_s1}
        assert "t1" in names_s1, f"t1 missing from child.s1 tables; got {names_s1}"
        assert t1_uuid in uuids_s1
        # The materialized arrow schema matches the source.
        t1_on_child = next(t for t in tables_s1 if t.table_name == "t1")
        assert t1_on_child.arrow_schema.equals(USER_SCHEMA)

        # list_tables on child for s2 returns t2 with the same arrow
        # schema as on main. **This is the assertion that fails today**:
        # only one schema's tables get materialized into the child.
        tables_s2 = list(
            client.list_tables(
                catalog_uuid=catalog_uuid,
                schema_uuid=s2_uuid,
                branch_name="child",
            )
        )
        names_s2 = {t.table_name for t in tables_s2}
        uuids_s2 = {t.table_uuid for t in tables_s2}
        assert "t2" in names_s2, f"t2 missing from child.s2 tables; got {names_s2}"
        assert t2_uuid in uuids_s2
        t2_on_child = next(t for t in tables_s2 if t.table_name == "t2")
        assert t2_on_child.arrow_schema.equals(USER_SCHEMA)


class TestCreateBranchForkTxShape:
    def test_fork_tx_is_single_tx_writing_both_system_tables(self):
        """After CreateBranch on a multi-schema catalog the child branch
        carries exactly one tx (the fork_tx) and its tx_table_log rows
        cover BOTH __penca_system__.{schemas,tables}.
        """
        client = make_client()
        (
            catalog_uuid,
            _s1_uuid,
            _s2_uuid,
            _t1_uuid,
            _t2_uuid,
            _main_branch_uuid,
        ) = _setup_two_schema_catalog(client)

        child_branch = client.create_branch(
            "child",
            "fork_author",
            "fork_comment",
            catalog_uuid=catalog_uuid,
        )

        child_branch_uuid = child_branch.branch_uuid

        # Exactly one tx on the child branch (the fork_tx). Tx framing is
        # internal post-CHA-222 (no QueryService.ListTxs RPC) so read the
        # catalog's commit_tx_log partition directly. The fork tx's author /
        # comment surface per-row via AuditData on the child's system
        # tables — that contract is covered by
        # integration_tx_framing_test.py.
        tx_partition = commit_tx_log_partition(catalog_uuid, child_branch_uuid)
        tx_rows = get_pg_driver().execute(
            f"SELECT tx_uuid FROM {_qi(tx_partition)}",
        )
        assert len(tx_rows) == 1, (
            f"child should have exactly the fork_tx; got {len(tx_rows)} txs"
        )
        fork_tx_uuid = str(tx_rows[0][0])

        # tx_table_log on the child branch covers BOTH system tables
        # under fork_tx.
        sys_schemas_table_uuid = system_schemas_table_uuid(catalog_uuid)
        sys_tables_table_uuid = system_tables_table_uuid(catalog_uuid)

        partition = tx_table_log_partition(catalog_uuid, child_branch_uuid)
        rows = get_pg_driver().execute(
            f"SELECT table_uuid FROM {_qi(partition)} WHERE tx_uuid = %s::uuid",
            (fork_tx_uuid,),
        )
        observed = {str(r[0]) for r in rows}
        assert sys_tables_table_uuid in observed, (
            f"fork_tx tx_table_log missing system_tables_table_uuid; got {observed}"
        )
        assert sys_schemas_table_uuid in observed, (
            f"fork_tx tx_table_log missing system_schemas_table_uuid; got {observed}"
        )


class TestCreateBranchEnablesWritesPerSchema:
    def test_writes_succeed_on_every_schema_after_create_branch(self):
        """After multi-schema CreateBranch, write_data succeeds for
        BOTH s1.t1 and s2.t2 on the child branch, and reads on child
        return the new rows while reads on main do not."""
        client = make_client()
        (
            catalog_uuid,
            s1_uuid,
            s2_uuid,
            t1_uuid,
            t2_uuid,
            main_branch_uuid,
        ) = _setup_two_schema_catalog(client)

        child_branch = client.create_branch(
            "child",
            "test",
            "create_branch",
            catalog_uuid=catalog_uuid,
        )
        child_branch_uuid = child_branch.branch_uuid

        # Write into s1.t1 on child.
        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=s1_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=t1_uuid,
            rows={"name": ["alice"], "value": [10]},
        )

        # Write into s2.t2 on child.
        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=s2_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=t2_uuid,
            rows={"name": ["bob"], "value": [20]},
        )

        # Reads on child return the new rows.
        child_t1 = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=s1_uuid,
            table_uuid=t1_uuid,
            branch_uuid=child_branch_uuid,
        )
        assert child_t1.column("name").to_pylist() == ["alice"]
        child_t2 = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=s2_uuid,
            table_uuid=t2_uuid,
            branch_uuid=child_branch_uuid,
        )
        assert child_t2.column("name").to_pylist() == ["bob"]

        # Reads on main do NOT see the child writes.
        main_t1 = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=s1_uuid,
            table_uuid=t1_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert main_t1.num_rows == 0
        main_t2 = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=s2_uuid,
            table_uuid=t2_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert main_t2.num_rows == 0


class TestMergeBranchAllSchemas:
    def test_multi_schema_merge_lands_writes_from_every_schema_on_target(self):
        """MergeBranch from a multi-schema source copies writes for
        BOTH s1.t1 and s2.t2 onto target, and the merge_tx's
        tx_table_log rows cover both table_uuids."""
        client = make_client()
        (
            catalog_uuid,
            s1_uuid,
            s2_uuid,
            t1_uuid,
            t2_uuid,
            main_branch_uuid,
        ) = _setup_two_schema_catalog(client)

        # Create child branch — materializes both schemas.
        child_branch = client.create_branch(
            "child",
            "test",
            "create_branch",
            catalog_uuid=catalog_uuid,
        )
        child_branch_uuid = child_branch.branch_uuid

        # Write rows in BOTH schemas on child.
        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=s1_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=t1_uuid,
            rows={"name": ["alice"], "value": [10]},
        )
        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=s2_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=t2_uuid,
            rows={"name": ["bob"], "value": [20]},
        )

        # Merge child → main. Catalog-wide.
        merge = client.merge_branch(
            source_branch_name="child",
            target_branch_name=MAIN_BRANCH_NAME,
            comment="multi-schema merge",
            author="merger",
            catalog_uuid=catalog_uuid,
        )

        # Reads of s1.t1 on main return the row written on child.
        main_t1 = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=s1_uuid,
            table_uuid=t1_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert main_t1.column("name").to_pylist() == ["alice"]

        # Reads of s2.t2 on main return the row written on child.
        main_t2 = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=s2_uuid,
            table_uuid=t2_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert main_t2.column("name").to_pylist() == ["bob"]

        # The merge tx's tx_table_log rows on main cover BOTH
        # s1.t1 and s2.t2 table_uuids. Tx framing is internal
        # post-CHA-222; recover the merge tx_uuid by joining on the
        # response's commit_micros (single auto-commit ⇒ unique).
        main_tx_partition = commit_tx_log_partition(catalog_uuid, main_branch_uuid)
        merge_tx_rows = get_pg_driver().execute(
            f"SELECT tx_uuid FROM {_qi(main_tx_partition)} WHERE commit_micros = %s",
            (merge.commit_micros,),
        )
        assert len(merge_tx_rows) == 1, (
            f"expected exactly one tx at merge's commit_micros; got {len(merge_tx_rows)}"
        )
        merge_tx_uuid = str(merge_tx_rows[0][0])

        partition = tx_table_log_partition(catalog_uuid, main_branch_uuid)
        rows = get_pg_driver().execute(
            f"SELECT table_uuid FROM {_qi(partition)} WHERE tx_uuid = %s::uuid",
            (merge_tx_uuid,),
        )
        observed = {str(r[0]) for r in rows}
        assert t1_uuid in observed, (
            f"merge_tx tx_table_log missing t1_uuid; got {observed}"
        )
        assert t2_uuid in observed, (
            f"merge_tx tx_table_log missing t2_uuid; got {observed}"
        )


class TestDeleteBranchAllSchemas:
    def test_multi_schema_delete_branch_cleans_cold_storage_in_every_schema(self):
        """After DeleteBranch on a multi-schema catalog, persist_segment
        metadata for tables in BOTH schemas is removed and the branch
        no longer responds to any RPC."""
        client = make_client()
        (
            catalog_uuid,
            s1_uuid,
            s2_uuid,
            t1_uuid,
            t2_uuid,
            _main_branch_uuid,
        ) = _setup_two_schema_catalog(client)

        child_branch = client.create_branch(
            "child",
            "test",
            "create_branch",
            catalog_uuid=catalog_uuid,
        )
        child_branch_uuid = child_branch.branch_uuid

        # Write rows + persist in BOTH schemas on child so cold segments
        # exist for tables in both schemas.
        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=s1_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=t1_uuid,
            rows={"name": ["alice"], "value": [10]},
        )
        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=s2_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=t2_uuid,
            rows={"name": ["bob"], "value": [20]},
        )
        # CHA-220: persist is per-table — one call per (schema, table)
        # pair on the branch.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=s1_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=t1_uuid,
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=s2_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=t2_uuid,
        )

        # CHA-203: segments key on `(branch_uuid, table_uuid)`; the
        # old `hot_storage_table_name` text column is gone.
        tfsm_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"

        def _seg_count(table_uuid: str) -> int:
            rows = get_pg_driver().execute(
                SQL(
                    "SELECT count(*) FROM {tbl}"
                    " WHERE branch_uuid = %s AND table_uuid = %s"
                ).format(tbl=Identifier(tfsm_parent)),
                (child_branch_uuid, table_uuid),
            )
            return rows[0][0]

        # Sanity: both schemas have segment metadata pre-delete.
        assert _seg_count(t1_uuid) > 0
        assert _seg_count(t2_uuid) > 0

        # Delete child branch — catalog-wide.
        client.delete_branch(
            catalog_uuid=catalog_uuid,
            branch_uuid=child_branch_uuid,
        )

        # Cold segment metadata for s1.t1 and s2.t2 is gone.
        assert _seg_count(t1_uuid) == 0
        assert _seg_count(t2_uuid) == 0

        # Operations against the deleted branch raise NOT_FOUND.
        with pytest.raises(NotFoundError):
            client.get_branch(
                catalog_uuid=catalog_uuid,
                branch_uuid=child_branch_uuid,
            )


#
# The original CHA-184 plan included a test asserting that persisting
# `s1.t1` alone does not purge `commit_tx_log` rows still referenced by an
# unpersisted `s2.t2`. CHA-168 removed `LifecycleManager::try_purge_tx_log`
# outright and folded hot commit_tx_log purge into the per-branch persist
# commit — one watermark per branch, monotonically advancing. The
# per-schema list-walk this test was meant to catch no longer exists,
# so the test is dropped (not deferred) per CHA-184 plan rev 3.


class TestHotLogIndexPerBranch:
    """The `(tx_uuid, row_uuid)` hot-log index is created per branch with a
    branch-unique name.

    Guards the regression where keying the index name on `table_uuid` (shared
    across branches living in one PG schema) makes the second branch's
    `CREATE INDEX IF NOT EXISTS` a silent no-op, leaving that branch on the
    ~268ms merge-on-read seq-scan with no error surfaced.
    """

    def test_each_branch_hot_log_gets_a_distinctly_named_tx_index(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"idx_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "s1", catalog_uuid=catalog_uuid, author="test", comment="create_schema"
        )
        table_uuid = client.create_table(
            "t",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        # CreateBranch materializes the table's per-branch data log tables on
        # the child (write/mod.rs `create_data_tables`, CHA-177), so both
        # branches' logs exist — each with its own index — after this call.
        child = client.create_branch(
            "child", "test", "create_branch", catalog_uuid=catalog_uuid
        )

        pg = get_pg_driver()

        def tx_index_names(log_table: str) -> list[str]:
            rows = pg.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE tablename = %s AND indexdef LIKE %s",
                (log_table, "%(tx_uuid, row_uuid)%"),
            )
            return [r[0] for r in rows]

        index_names: list[str] = []
        for branch_uuid in (main_branch_uuid, child.branch_uuid):
            for log_table in (
                upsert_log_table(table_uuid, branch_uuid),
                delete_log_table(table_uuid, branch_uuid),
            ):
                names = tx_index_names(log_table)
                assert len(names) == 1, (
                    f"{log_table} should carry exactly one (tx_uuid, row_uuid) "
                    f"index, got {names}"
                )
                index_names.append(names[0])

        # 2 branches x {upsert, delete} = 4 logs, each with its own index name
        # — no `table_uuid`-keyed collision across branches.
        assert len(set(index_names)) == 4, (
            f"hot-log index names collided across branches: {index_names}"
        )


# CHA-515: main-only branching guard (interim, removed by CHA-509)


class TestCreateBranchMainOnlyGuard:
    """Forks are main-only until CHA-509 lands multi-level inheritance.

    The read planner enumerates a single immediate parent (CHA-178,
    single-level), so a fork off a non-main branch silently drops
    grandparent rows on read. ``create_branch`` rejects it up front with
    ``UNIMPLEMENTED``.
    """

    def test_fork_off_main_succeeds_but_fork_off_fork_is_rejected(self):
        client = make_client()
        catalog_uuid, _s1, _s2, _t1, _t2, _main = _setup_two_schema_catalog(client)

        # Fork off main → succeeds (unchanged).
        client.create_branch(
            "child", "test", "create_branch", catalog_uuid=catalog_uuid
        )

        # Fork off the non-main child → UNIMPLEMENTED. The client maps that
        # gRPC status to Python's built-in NotImplementedError.
        with pytest.raises(NotImplementedError, match="(?i)source branch must be main"):
            client.create_branch(
                "grandchild",
                "test",
                "create_branch",
                catalog_uuid=catalog_uuid,
                source_branch_name="child",
            )

        # Fail-fast: the rejected fork left no branch_store row behind.
        rows = get_pg_driver().execute(
            f"SELECT branch_name FROM {_qi(branch_store_table(catalog_uuid))} "
            "WHERE branch_name = %s",
            ("grandchild",),
        )
        assert rows == [], "rejected fork must not create a branch_store row"

    def test_rejected_fork_does_not_persist_the_source(self):
        """The guard runs before PersistBranch, so rejecting a non-main fork
        does no hot→cold flush on its source branch."""
        client = make_client()
        catalog_uuid, s1_uuid, _s2, t1_uuid, _t2, _main = _setup_two_schema_catalog(
            client
        )

        # A non-main source branch carrying hot rows not yet in cold: fork
        # child off main, then commit on child.
        child = client.create_branch(
            "child", "test", "create_branch", catalog_uuid=catalog_uuid
        )
        _write_rows(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=s1_uuid,
            branch_uuid=child.branch_uuid,
            table_uuid=t1_uuid,
            rows={"name": ["a"], "value": [1]},
        )
        assert _persist_seg_count(catalog_uuid, child.branch_uuid, t1_uuid) == 0

        # Fork off the non-main child → rejected. PersistBranch(child) must not
        # have run: child's hot rows stay unflushed (0 persist segments).
        with pytest.raises(NotImplementedError, match="(?i)source branch must be main"):
            client.create_branch(
                "grandchild",
                "test",
                "create_branch",
                catalog_uuid=catalog_uuid,
                source_branch_name="child",
            )

        assert _persist_seg_count(catalog_uuid, child.branch_uuid, t1_uuid) == 0, (
            "rejected fork must not flush the source branch hot→cold"
        )
