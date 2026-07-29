"""Integration tests for WriteService (table CUD, branching, transactions, mutations, merge).

Run via ``just integration-test``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client._time import micros_to_datetime
from penca_client.errors import (
    FailedPreconditionError,
    InvalidRequestError,
    NotFoundError,
)
from penca_client.naming import (
    commit_tx_log_partition,
    delete_log_table,
    system_tables_table_uuid,
    upsert_log_table,
)
from psycopg.sql import Identifier

from .integration_helpers import (
    USER_SCHEMA,
    count_stmts_referencing,
    create_table_on_branch,
    ensure_pg_stat_statements,
    get_pg_driver,
    make_client,
    reset_pg_stat,
    setup_schema,
)

_PK_SCHEMA_NAME = pa.schema([pa.field("name", pa.utf8())])


def _qi(name: str) -> str:
    return Identifier(name).as_string(None)


class TestWriteService:
    def test_create_table(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            "table_test_cat", "owner"
        )
        schema_uuid = client.create_schema(
            "table_test_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "test_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        assert isinstance(table_uuid, str)
        assert len(table_uuid) > 0

    def test_create_table_with_description(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            "table_desc_cat", "owner"
        )
        schema_uuid = client.create_schema(
            "table_desc_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "desc_table",
            USER_SCHEMA,
            description="A test table",
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        response = client.get_table(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_uuid=table_uuid
        )
        assert response.description == "A test table"

    def test_create_table_with_all_keys(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            "table_keys_cat", "owner"
        )
        schema_uuid = client.create_schema(
            "table_keys_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "keys_table",
            USER_SCHEMA,
            primary_keys=["name"],
            partition_keys=["name"],
            clustering_keys=["value"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        response = client.get_table(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_uuid=table_uuid
        )
        assert response.primary_keys == ["name"]
        assert response.partition_keys == ["name"]
        assert response.clustering_keys == ["value"]

    def test_update_table(self):
        """update_table modifies description / keys / arrow_schema; the name
        is a PK input (table_uuid = xxh3(schema_uuid:table_name)) and
        is immutable post-CHA-163, so any ``table_name`` passed is treated as
        an identifier."""
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog("table_upd_cat", "owner")
        schema_uuid = client.create_schema(
            "table_upd_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "the_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        new_schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("value", pa.int64()),
                pa.field("extra", pa.utf8()),
            ]
        )
        client.update_table(
            new_schema,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            description="updated",
            primary_keys=["name"],
            partition_keys=["name"],
            clustering_keys=["value"],
            author="test",
            comment="update_table",
        )
        response = client.get_table(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_uuid=table_uuid
        )
        assert response.table_name == "the_table"  # immutable
        assert response.description == "updated"
        assert response.primary_keys == ["name"]
        assert response.partition_keys == ["name"]
        assert response.clustering_keys == ["value"]

    def test_update_table_schema_evolution(self):
        """After adding a column, inserts with the new schema succeed and
        old rows read back with NULL for the new column."""
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            "schema_evo_cat", "owner"
        )
        schema_uuid = client.create_schema(
            "schema_evo_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "evo_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        branch = client.create_branch(
            "evo_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(
            client,
            catalog_uuid,
            schema_uuid,
            branch.branch_uuid,
            table_name="evo_table",
        )
        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch_old = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch_old,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        new_schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("value", pa.int64()),
                pa.field("extra", pa.utf8()),
            ]
        )
        client.update_table(
            new_schema,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            author="test",
            comment="update_table",
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch_new = pa.table(
            {"name": ["bob"], "value": [2], "extra": ["hello"]},
            schema=new_schema,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch_new,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert result.num_rows == 2
        rows = result.to_pydict()
        alice_idx = rows["name"].index("alice")
        bob_idx = rows["name"].index("bob")
        assert rows["extra"][alice_idx] is None
        assert rows["extra"][bob_idx] == "hello"

    def test_delete_table(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog("table_del_cat", "owner")
        schema_uuid = client.create_schema(
            "table_del_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "del_table",
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
            table_uuid=table_uuid,
            author="test",
            comment="delete_table",
        )

        with pytest.raises(NotFoundError):
            client.get_table(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
            )

    def test_create_branch(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "feature",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        assert branch.branch_name == "feature"
        assert branch.catalog_uuid == catalog_uuid
        # Forked from head (no fork position given) — records a real fork seq.
        assert branch.fork_commit_seq_num >= 0

    def test_create_branch_with_uuid(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        custom_uuid = str(uuid4())
        branch = client.create_branch(
            "custom",
            branch_uuid=custom_uuid,
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        assert branch.branch_uuid == custom_uuid

    def test_delete_branch(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "to_delete",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        client.delete_branch(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )
        branches = list(
            client.list_branches(catalog_uuid=catalog_uuid, schema_uuid=schema_uuid)
        )
        branch_uuids = [branch.branch_uuid for branch in branches]
        assert branch.branch_uuid not in branch_uuids

    def test_begin_tx(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "tx_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert tx.began_at_micros > 0

    def test_begin_tx_with_optional_params(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "tx_opts_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        custom_tx_uuid = str(uuid4())
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            comment="test comment",
            author="test_author",
            tx_uuid=custom_tx_uuid,
            timeout_seconds=3600,
        )
        assert tx.tx_uuid == custom_tx_uuid
        assert tx.expires_at_micros > tx.began_at_micros

    def test_commit_tx(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "commit_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )

        batch = pa.table(
            {"name": ["alice"], "value": [42]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )

        committed = client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert committed.commit_micros > tx.began_at_micros

    def test_abort_tx_unknown_raises_not_found(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        with pytest.raises(NotFoundError):
            client.abort_tx(str(uuid4()), catalog_uuid=catalog_uuid)

    def test_abort_tx_then_commit_fails_precondition(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "abort_then_commit_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.abort_tx(
            tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch.branch_uuid
        )
        with pytest.raises(FailedPreconditionError):
            client.commit_tx(
                tx.tx_uuid,
                catalog_uuid=catalog_uuid,
                branch_uuid=branch.branch_uuid,
            )

    def test_abort_tx_idempotent(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "abort_idempotent_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        # First abort succeeds; second is a no-op via ON CONFLICT.
        client.abort_tx(
            tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch.branch_uuid
        )
        client.abort_tx(
            tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch.branch_uuid
        )

    def test_commit_then_abort_fails_precondition(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "commit_then_abort_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )
        with pytest.raises(FailedPreconditionError):
            client.abort_tx(
                tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch.branch_uuid
            )

    def test_recommit_tx_fails_precondition(self):
        """A second CommitTx on an already-committed tx_uuid surfaces
        FailedPrecondition with a precise "already committed at X"
        message — not an INTERNAL/unique-violation. Exercises the
        ``TxStatus::Committed`` match arm in the commit_tx caller,
        which is reachable because ``get_tx_status`` now LEFT JOINs
        ``commit_tx_log`` and reports Committed in the window between commit
        and the lifecycle sweep purging the begin_tx_log row."""
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "recommit_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )
        with pytest.raises(FailedPreconditionError) as exc_info:
            client.commit_tx(
                tx.tx_uuid,
                catalog_uuid=catalog_uuid,
                branch_uuid=branch.branch_uuid,
            )

        # Precise error message: "already committed at <timestamp>"
        # — not the generic unique-violation message we'd see if the
        # status check missed Committed and the INSERT tripped the PK.
        assert "already committed" in str(exc_info.value)

    def test_write_data_upserts(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "upsert_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )

        batch = pa.table(
            {"name": ["alice", "bob"], "value": [1, 2]},
            schema=USER_SCHEMA,
        )
        response = client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert response is not None

    def test_write_data_upserts_overwrites_existing(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "overwrite_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )

        first = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=first,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )

        second = pa.table(
            {"name": ["alice"], "value": [99]},
            schema=USER_SCHEMA,
        )
        response = client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=second,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert response is not None

    def test_write_data_mixed_batch_upsert_parity(self):
        """Mixed new + existing row_uuids in one upserts payload.

        Before CHA-134, callers had to pre-classify rows by querying the
        table first. Post-CHA-134 they just send one batch and get
        upsert semantics automatically: newer tx wins per row_uuid.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "mixed_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)

        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        seed = pa.table(
            {"name": ["alice", "bob"], "value": [1, 2]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=seed,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        # Second tx sends a single batch containing:
        #   - alice (existing row_uuid, new value 99 — overwrites)
        #   - carol (new row_uuid — appears for the first time)
        # The client does NOT partition. Read-after-write returns the
        # latest value per row_uuid.
        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        mixed = pa.table(
            {"name": ["alice", "carol"], "value": [99, 3]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=mixed,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
        )
        by_name = dict(
            zip(
                result.column("name").to_pylist(),
                result.column("value").to_pylist(),
                strict=True,
            )
        )
        assert by_name == {"alice": 99, "bob": 2, "carol": 3}

    def test_write_data_deletes(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "delete_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )

        batch = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )

        response = client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert response is not None

    # Two upserts in one ``Change`` with the same ``row_uuid`` produce
    # the same ``version_uuid = hash(row_uuid, tx_uuid)``, and
    # ``insert_upserts``' ``ON CONFLICT (version_uuid) DO UPDATE``
    # silently last-wins → caller loses a row without learning. PG
    # would reject the analogous ``INSERT ... ON CONFLICT DO UPDATE``
    # with "ON CONFLICT DO UPDATE command cannot affect row a second
    # time". Reject at the write servicer with ``InvalidArgument``
    # before the ON CONFLICT branch collapses the duplicates.
    #
    # The ON CONFLICT branch itself stays load-bearing for legitimate
    # cross-statement same-row writes within one tx (case 2 in ADR
    # 0009 — `INSERT R` in one write_data RPC, `UPDATE R` in another).
    # Tightening would break that; the within-upserts intra-batch
    # check is the right granularity. Within-deletes duplicates are
    # idempotent (same tombstone twice → same merge outcome). Cross-set
    # upsert + delete of the same row resolves via CHA-243's
    # composite-tiebreaker upsert-wins on tie — value-preserving SETs
    # on PK columns and N-way swaps both rely on that resolution, so
    # rejecting cross-set would over-reach.

    def test_write_data_rejects_duplicate_upsert_row_uuid(self):
        """Two upserts in one Change with the same PK collide on
        ``row_uuid`` and must be rejected at the write servicer."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

        # Both rows resolve to ``row_uuid_for_pk(table, ["alice"])``.
        batch = pa.table(
            {"name": ["alice", "alice"], "value": [1, 2]},
            schema=USER_SCHEMA,
        )
        with pytest.raises(InvalidRequestError, match="(?i)duplicate.*row_uuid"):
            client.write_data(
                tx.tx_uuid,
                Mutation(table_uuid=table_uuid, upserts=batch),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
            )

    def test_write_data_allows_pk_rewrite_in_one_change(self):
        """CHA-237 regression guard: ``delete(old_pk) + upsert(new_pk)``
        in one Change must atomically vacate the old PK and populate
        the new one. CHA-242's within-upserts uniqueness check is
        scoped to repeats inside the upserts batch and doesn't reach
        across to deletes, so this shape stays unaffected."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        seed_tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            seed_tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [10]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            seed_tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        # PK rewrite: delete row at "alice", upsert at "alice2".
        rewrite_tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            rewrite_tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice2"], "value": [10]},
                    schema=USER_SCHEMA,
                ),
                deletes=pa.table(
                    {"name": ["alice"]},
                    schema=_PK_SCHEMA_NAME,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            rewrite_tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        rows = sorted(
            zip(
                result.column("name").to_pylist(),
                result.column("value").to_pylist(),
                strict=True,
            ),
            key=lambda r: r[0],
        )
        assert rows == [("alice2", 10)], (
            "PK rewrite in one Change must vacate the old PK and surface "
            f"the new one; got {rows}"
        )

    def test_write_data_allows_same_row_across_two_changes_in_one_tx(self):
        """Cross-statement same-row writes within one Penca tx must stay
        functional: case 2 in ADR 0009. ``insert_upserts``'
        ``ON CONFLICT (version_uuid) DO UPDATE`` is load-bearing for the
        last-write-wins semantics PG itself surfaces for sequential
        INSERT+UPDATE of the same row, and CHA-242's within-upserts
        intra-batch check must not regress it. Two distinct
        ``write_data`` calls on one ``tx_uuid`` carry the same
        ``row_uuid`` for ``alice`` but each call has its own upserts
        batch, so the uniqueness rule does not apply across them."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [1]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        # Second Change: overwrite ('alice', 2) — same row_uuid + same Penca
        # tx → same version_uuid → ``ON CONFLICT (version_uuid) DO UPDATE`` is
        # what makes the second write win.
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [2]},
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

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        rows = sorted(
            zip(
                result.column("name").to_pylist(),
                result.column("value").to_pylist(),
                strict=True,
            ),
            key=lambda r: r[0],
        )
        assert rows == [("alice", 2)], (
            f"cross-Change same-row last-write-wins regressed; got {rows}"
        )

    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_merge_upserts_compact_to_target_upsert_log(self):
        """Source upserts → target.upsert_log under merge_tx."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        target = client.create_branch(
            "target_ups",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        source = client.create_branch(
            "source_ups",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=target.branch_uuid,
            author="test",
            comment="create_branch",
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        batch = pa.table(
            {"name": ["alice", "bob"], "value": [1, 2]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        client.merge_branch(
            source_branch_uuid=source.branch_uuid,
            target_branch_uuid=target.branch_uuid,
            comment="merge upserts",
            author="tester",
            catalog_uuid=catalog_uuid,
        )

        ups, dels = _count_per_log(
            catalog_uuid, schema_uuid, target.branch_uuid, table_uuid
        )
        assert (ups, dels) == (2, 0)
        # Source tx_uuids don't bleed through onto target: every upsert
        # row carries the (single) merge tx's uuid. The merge's
        # `tx_uuid` is internal post-CHA-222, so we assert size instead
        # of equality. comment/author surface via audit_data on target
        # (covered by integration_tx_framing_test.py).
        assert (
            len(
                _all_tx_uuids(
                    catalog_uuid,
                    schema_uuid,
                    target.branch_uuid,
                    table_uuid,
                    "upsert",
                )
            )
            == 1
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=target.branch_uuid,
        )
        assert sorted(result.column("name").to_pylist()) == ["alice", "bob"]

    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_merge_deletes_compact_to_target_delete_log(self):
        """Source tombstone → target.delete_log."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        target = client.create_branch(
            "target_del",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        source = client.create_branch(
            "source_del",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=target.branch_uuid,
            author="test",
            comment="create_branch",
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        client.merge_branch(
            source_branch_uuid=source.branch_uuid,
            target_branch_uuid=target.branch_uuid,
            catalog_uuid=catalog_uuid,
        )

        ups, dels = _count_per_log(
            catalog_uuid, schema_uuid, target.branch_uuid, table_uuid
        )
        assert (ups, dels) == (0, 1)
        # Same invariant as the upsert-merge test: target rows carry a
        # single (merge) tx_uuid, not source's. Merge tx_uuid is
        # internal post-CHA-222 so we assert size, not equality.
        assert (
            len(
                _all_tx_uuids(
                    catalog_uuid,
                    schema_uuid,
                    target.branch_uuid,
                    table_uuid,
                    "delete",
                )
            )
            == 1
        )

    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_merge_create_then_delete_yields_dead_tombstone(self):
        """Source inserts X then tombstones X → under the unified upsert_log,
        the tombstone propagates to target even though the row never existed
        there pre-merge. Read correctness is preserved (the tombstone excludes
        a row_uuid that isn't present in any upsert_log or snapshot). See
        docs/decisions/0001-unified-upsert-log.md for the trade-off."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        target = client.create_branch(
            "target_ctd",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        source = client.create_branch(
            "source_ctd",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=target.branch_uuid,
            author="test",
            comment="create_branch",
        )

        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        batch = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        client.merge_branch(
            source_branch_uuid=source.branch_uuid,
            target_branch_uuid=target.branch_uuid,
            catalog_uuid=catalog_uuid,
        )

        ups, dels = _count_per_log(
            catalog_uuid, schema_uuid, target.branch_uuid, table_uuid
        )
        assert (ups, dels) == (0, 1)

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=target.branch_uuid,
        )
        assert result.num_rows == 0

    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_merge_within_rpc_delete_upsert_preserves_row_on_target(self):
        """CHA-243 → CHA-431 composite tiebreaker, end-to-end across
        `merge_branch`.

        A value-preserving UPDATE-on-PK (the [CHA-237](
        https://linear.app/chapala/issue/CHA-237) shape: ``SET id = id``,
        ``COALESCE(id, fallback)``, no-op ``CASE``) emits ``delete(R) +
        upsert(R)`` for the same ``row_uuid`` within a single
        ``write_data`` batch. Both writes share one ``commit_micros``;
        deletes-first gives the upsert the higher ``write_seq_num``, so on the
        composite ``(commit_micros, write_seq_num)`` the upsert wins. We
        emulate that shape directly via a ``Mutation`` carrying both
        ``upserts`` and ``deletes`` for the same PK in one ``write_data``
        call.

        Pre-CHA-243 branch-merge used a strict-``>`` predicate on
        ``commit_micros`` alone, so on tie BOTH sides failed their
        respective WHERE clauses (``T > T`` false on both), and the row
        vanished from target entirely. The composite-``>=`` predicate in
        ``penca_sql::build_composite_merge_resolution`` resolves it
        deterministically: upsert lands on ``target.upsert_log``, delete
        drops. Read on target sees the row.

        Pin both the row-level outcome (``read_data`` returns alice)
        and the log-level outcome (1 upsert row, 0 delete rows) so the
        helper-level lock-ins in ``penca-sql`` are joined by an
        end-to-end assertion across the branch-merge plumbing."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        target = client.create_branch(
            "target_within_rpc",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        source = client.create_branch(
            "source_within_rpc",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=target.branch_uuid,
            author="test",
            comment="create_branch",
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        # Single `write_data` carrying both upsert and delete for the
        # same PK — same PG tx → tied `now()` → composite tie.
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [1]},
                    schema=USER_SCHEMA,
                ),
                deletes=pa.table(
                    {"name": ["alice"]},
                    schema=_PK_SCHEMA_NAME,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        client.merge_branch(
            source_branch_uuid=source.branch_uuid,
            target_branch_uuid=target.branch_uuid,
            comment="merge tied delete+upsert",
            author="tester",
            catalog_uuid=catalog_uuid,
        )

        # Composite-`>=` upsert-visible predicate wins on tie → row
        # lands on `target.upsert_log`. Mirror composite-`>` delete-
        # visible predicate loses on tie → tombstone drops.
        ups, dels = _count_per_log(
            catalog_uuid, schema_uuid, target.branch_uuid, table_uuid
        )
        assert (ups, dels) == (1, 0), (
            "value-preserving within-RPC delete+upsert must resolve to "
            "upsert-wins under composite tiebreaker on branch merge — "
            f"got (upserts, deletes) = ({ups}, {dels})"
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=target.branch_uuid,
        )
        assert result.column("name").to_pylist() == ["alice"]
        assert result.column("value").to_pylist() == [1]

    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_merge_interleaved_upserts_final_alive_wins(self):
        """Multi-tx insert then update on source → single compacted row on target."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        target = client.create_branch(
            "target_mix",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        source = client.create_branch(
            "source_mix",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=target.branch_uuid,
            author="test",
            comment="create_branch",
        )

        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [99]}, schema=USER_SCHEMA
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        client.merge_branch(
            source_branch_uuid=source.branch_uuid,
            target_branch_uuid=target.branch_uuid,
            catalog_uuid=catalog_uuid,
        )

        # alice's two source upserts compact into one target.upsert_log row,
        # carrying the latest value (99).
        ups, dels = _count_per_log(
            catalog_uuid, schema_uuid, target.branch_uuid, table_uuid
        )
        assert (ups, dels) == (1, 0)

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=target.branch_uuid,
        )
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["alice"]
        assert result.column("value").to_pylist() == [99]

    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_merge_pit_coherence_update_over_prefork_row(self):
        """PIT read before merge_tx shows pre-merge target state, not source leak."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        target = client.create_branch(
            "target_pit",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )

        # Seed target with alice=1 before anyone forks.
        seed = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=target.branch_uuid,
        )
        client.write_data(
            seed.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=target.branch_uuid,
        )
        client.commit_tx(
            seed.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=target.branch_uuid,
        )

        # Source forks from target's committed state; updates alice=99.
        source = client.create_branch(
            "source_pit",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=target.branch_uuid,
            author="test",
            comment="create_branch",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [99]}, schema=USER_SCHEMA
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        merge_tx = client.merge_branch(
            source_branch_uuid=source.branch_uuid,
            target_branch_uuid=target.branch_uuid,
            catalog_uuid=catalog_uuid,
        )

        before = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=target.branch_uuid,
            as_of=micros_to_datetime(merge_tx.commit_micros - 1),
        )
        assert before.column("value").to_pylist() == [1]

        after = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=target.branch_uuid,
            as_of=micros_to_datetime(merge_tx.commit_micros),
        )
        assert after.column("value").to_pylist() == [99]

    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_merge_conflict_guard_rejects_non_ff(self):
        """Commit on target past source's fork point → merge raises."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        target = client.create_branch(
            "target_ff",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        source = client.create_branch(
            "source_ff",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=target.branch_uuid,
            author="test",
            comment="create_branch",
        )

        # Commit on target AFTER source forked from it — target now has a commit
        # past source's fork point, so the merge is no longer fast-forward.
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=target.branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=target.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=target.branch_uuid,
        )

        with pytest.raises(InvalidRequestError):
            client.merge_branch(
                source_branch_uuid=source.branch_uuid,
                target_branch_uuid=target.branch_uuid,
                catalog_uuid=catalog_uuid,
            )


def _count_per_log(
    catalog_uuid: str,
    schema_uuid: str,
    branch_uuid: str,
    table_uuid: str,
) -> tuple[int, int]:
    """(upsert_log, delete_log) row counts for a branch+table.

    Per-branch data tables are deterministic in
    ``(table_uuid, branch_uuid)`` (CHA-177 / CHA-203).
    """
    driver = get_pg_driver()
    ups_tbl = _qi(upsert_log_table(table_uuid, branch_uuid))
    del_tbl = _qi(delete_log_table(table_uuid, branch_uuid))
    (ups,) = driver.execute(f"SELECT COUNT(*) FROM {ups_tbl}")[0]
    (dels,) = driver.execute(f"SELECT COUNT(*) FROM {del_tbl}")[0]
    return int(ups), int(dels)


def _all_tx_uuids(
    catalog_uuid: str,
    schema_uuid: str,
    branch_uuid: str,
    table_uuid: str,
    log: str,
) -> set[str]:
    """Distinct tx_uuids present on one of {upsert,delete}_log."""
    table = {
        "upsert": upsert_log_table(table_uuid, branch_uuid),
        "delete": delete_log_table(table_uuid, branch_uuid),
    }[log]
    rows = get_pg_driver().execute(f"SELECT DISTINCT tx_uuid FROM {_qi(table)}")
    return {str(row[0]) for row in rows}


class TestMultiSchemaTransactions:
    """A single Penca transaction can span multiple schemas in one catalog.

    Pre-CHA-163, commit_tx_log was per-schema, so a tx written via s1 was invisible
    on s2 and vice versa — penca-sql-server rejected mid-tx DML against a
    different schema with FAILED_PRECONDITION. After the per-catalog lift,
    branches and commit_tx_log are catalog-scope, and a single tx_uuid lives in the
    catalog's commit_tx_log partition for that branch — both schemas see the same
    commit_micros at commit time.
    """

    def test_multi_schema_atomic_commit_visible(self):
        """Two WriteData calls on different schemas land in one commit_tx_log row."""
        client = make_client()

        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"multi_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_a_uuid = client.create_schema(
            "schema_a",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        schema_b_uuid = client.create_schema(
            "schema_b",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )

        table_a_uuid = client.create_table(
            "table_a",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a_uuid,
            author="test",
            comment="create_table",
        )
        table_b_uuid = client.create_table(
            "table_b",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_b_uuid,
            author="test",
            comment="create_table",
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a_uuid,
            branch_uuid=main_branch_uuid,
        )

        batch_a = pa.table({"name": ["alice"], "value": [10]}, schema=USER_SCHEMA)
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_a_uuid, upserts=batch_a),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a_uuid,
            branch_uuid=main_branch_uuid,
        )

        # Write to schema_b.table_b under the *same* tx — this is the new
        # capability gated by CHA-163. Pre-CHA-163 this would either route
        # to a different schema's commit_tx_log (impossible) or fail with
        # FAILED_PRECONDITION.
        batch_b = pa.table({"name": ["bob"], "value": [20]}, schema=USER_SCHEMA)
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_b_uuid, upserts=batch_b),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_b_uuid,
            branch_uuid=main_branch_uuid,
        )

        committed = client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert committed.commit_micros > 0

        # Both rows should be readable post-commit, both stamped with the
        # same tx_uuid (verified via white-box read of the catalog's
        # commit_tx_log partition).
        tx_part = commit_tx_log_partition(catalog_uuid, main_branch_uuid)
        rows = get_pg_driver().execute(
            f"SELECT tx_uuid, commit_micros "
            f"FROM {_qi(tx_part)} "
            f"WHERE tx_uuid = %s::uuid",
            (tx.tx_uuid,),
        )
        assert len(rows) == 1, (
            "commit_tx_log should have exactly one row for the multi-schema tx"
        )
        assert str(rows[0][0]) == tx.tx_uuid
        assert int(rows[0][1]) == committed.commit_micros

        # The catalog's commit_tx_log holds exactly one tx for our writes;
        # both schemas' physical upsert_log rows reference it.
        a_tx_uuids = _all_tx_uuids(
            catalog_uuid, schema_a_uuid, main_branch_uuid, table_a_uuid, "upsert"
        )
        b_tx_uuids = _all_tx_uuids(
            catalog_uuid, schema_b_uuid, main_branch_uuid, table_b_uuid, "upsert"
        )
        assert tx.tx_uuid in a_tx_uuids
        assert tx.tx_uuid in b_tx_uuids

    def test_multi_schema_atomic_rollback_discards_both(self):
        """Aborting a multi-schema tx leaves no committed rows in either schema."""
        client = make_client()

        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"multi_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_a_uuid = client.create_schema(
            "schema_a",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        schema_b_uuid = client.create_schema(
            "schema_b",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )

        table_a_uuid = client.create_table(
            "table_a",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a_uuid,
            author="test",
            comment="create_table",
        )
        table_b_uuid = client.create_table(
            "table_b",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_b_uuid,
            author="test",
            comment="create_table",
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a_uuid,
            branch_uuid=main_branch_uuid,
        )

        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_a_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [10]}, schema=USER_SCHEMA
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_b_uuid,
                upserts=pa.table({"name": ["bob"], "value": [20]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_b_uuid,
            branch_uuid=main_branch_uuid,
        )

        client.abort_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        # The aborted tx_uuid lives in abort_tx_log on the catalog, so
        # neither table's upsert_log rows are visible at read time even
        # though the rows are physically present until lifecycle sweep.
        # CommitTx after AbortTx fails.
        with pytest.raises(FailedPreconditionError):
            client.commit_tx(
                tx.tx_uuid,
                catalog_uuid=catalog_uuid,
                branch_uuid=main_branch_uuid,
            )


# CHA-164: schema/table CRUD takes an optional tx_uuid. Mode-switch
# mirrors WriteData — absent = auto-commit, present = join the open
# tx and become visible at CommitTx. AbortTx leaves no orphan rows.


class TestSchemaTxUuidMode:
    def _setup(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"sch_tx_cat_{uuid4().hex[:8]}", "owner"
        )
        return client, catalog_uuid, main_branch_uuid

    def test_create_schema_in_open_tx_invisible_until_commit(self):
        client, catalog_uuid, main_branch = self._setup()
        tx = client.begin_tx(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

        schema_uuid = client.create_schema(
            "tx_schema", catalog_uuid=catalog_uuid, tx_uuid=tx.tx_uuid
        )

        # Pre-commit: GetSchema without open_tx_uuid must NOT see the row.
        with pytest.raises(NotFoundError):
            client.get_schema(catalog_uuid=catalog_uuid, schema_uuid=schema_uuid)

        # RYOW: GetSchema WITH open_tx_uuid must see it.
        ryow = client.get_schema(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            open_tx_uuid=tx.tx_uuid,
        )
        assert ryow.schema_name == "tx_schema"

        client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch)
        # Post-commit: GetSchema resolves without open_tx_uuid.
        committed = client.get_schema(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid
        )
        assert committed.schema_name == "tx_schema"

    def test_create_schema_in_aborted_tx_never_visible(self):
        client, catalog_uuid, main_branch = self._setup()
        tx = client.begin_tx(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

        schema_uuid = client.create_schema(
            "aborted_schema", catalog_uuid=catalog_uuid, tx_uuid=tx.tx_uuid
        )
        client.abort_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch)

        with pytest.raises(NotFoundError):
            client.get_schema(catalog_uuid=catalog_uuid, schema_uuid=schema_uuid)


class TestTableTxUuidMode:
    def _setup(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"tab_tx_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "tx_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        return client, catalog_uuid, schema_uuid, main_branch_uuid

    def test_create_table_in_open_tx_invisible_until_commit(self):
        client, catalog_uuid, schema_uuid, main_branch = self._setup()
        tx = client.begin_tx(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

        table_uuid = client.create_table(
            "tx_table",
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            primary_keys=["name"],
            tx_uuid=tx.tx_uuid,
        )
        with pytest.raises(NotFoundError):
            client.get_table(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch,
            )

        # RYOW
        ryow = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch,
            open_tx_uuid=tx.tx_uuid,
        )
        assert ryow.table_name == "tx_table"

        client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch)
        committed = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch,
        )
        assert committed.table_name == "tx_table"

    def test_create_table_in_aborted_tx_never_visible(self):
        client, catalog_uuid, schema_uuid, main_branch = self._setup()
        tx = client.begin_tx(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

        table_uuid = client.create_table(
            "aborted_table",
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            primary_keys=["name"],
            tx_uuid=tx.tx_uuid,
        )
        client.abort_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch)

        with pytest.raises(NotFoundError):
            client.get_table(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch,
            )


class TestAgenticFlow:
    """End-to-end agentic DDL: BEGIN; CREATE SCHEMA; CREATE TABLE;
    INSERT; COMMIT — all atomic, visible together post-commit."""

    def test_begin_create_schema_create_table_insert_commit(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"agentic_{uuid4().hex[:8]}", "owner"
        )
        main_branch = main_branch_uuid

        tx = client.begin_tx(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

        schema_uuid = client.create_schema(
            "agentic_schema", catalog_uuid=catalog_uuid, tx_uuid=tx.tx_uuid
        )
        table_uuid = client.create_table(
            "agentic_table",
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            primary_keys=["name"],
            tx_uuid=tx.tx_uuid,
        )

        # Pre-commit: outside readers see nothing.
        with pytest.raises(NotFoundError):
            client.get_schema(catalog_uuid=catalog_uuid, schema_uuid=schema_uuid)

        with pytest.raises(NotFoundError):
            client.get_table(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch,
            )

        client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch)

        # Post-commit: both visible.
        assert (
            client.get_schema(
                catalog_uuid=catalog_uuid, schema_uuid=schema_uuid
            ).schema_name
            == "agentic_schema"
        )
        assert (
            client.get_table(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch,
            ).table_name
            == "agentic_table"
        )

    def test_abort_leaves_no_visible_state(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"agentic_abort_{uuid4().hex[:8]}", "owner"
        )
        main_branch = main_branch_uuid

        tx = client.begin_tx(catalog_uuid=catalog_uuid, branch_uuid=main_branch)
        schema_uuid = client.create_schema(
            "rollback_schema", catalog_uuid=catalog_uuid, tx_uuid=tx.tx_uuid
        )
        table_uuid = client.create_table(
            "rollback_table",
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            primary_keys=["name"],
            tx_uuid=tx.tx_uuid,
        )
        client.abort_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch)

        with pytest.raises(NotFoundError):
            client.get_schema(catalog_uuid=catalog_uuid, schema_uuid=schema_uuid)

        with pytest.raises(NotFoundError):
            client.get_table(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch,
            )


class TestClientCatalogFallback:
    """CHA-193: ``create_schema`` / ``list_schemas`` honour
    ``client.catalog`` like the rest of the within-catalog surface.

    Pre-CHA-193 these two methods required ``catalog_uuid`` as a
    required positional, breaking the canonical pattern (``create_table``
    et al.) and forcing every caller to keep re-passing it after one
    ``client.catalog = ...`` already established the connection
    catalog. Cover the fallback explicitly so a future regression
    surfaces here, not in user surprise.
    """

    def test_create_schema_uses_client_catalog_fallback(self):
        client = make_client()
        catalog_name = f"fallback_cat_{uuid4().hex[:8]}"
        catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
        client.catalog = catalog_name

        schema_uuid = client.create_schema(
            "fallback_schema",
            author="test",
            comment="create_schema fallback",
        )

        got = client.get_schema(schema_uuid=schema_uuid)
        assert got.schema_name == "fallback_schema"
        assert got.catalog_uuid == catalog_uuid

    def test_list_schemas_uses_client_catalog_fallback(self):
        client = make_client()
        catalog_name = f"fallback_cat_{uuid4().hex[:8]}"
        client.create_catalog(catalog_name, "owner")
        client.catalog = catalog_name
        client.create_schema(
            "listed_schema",
            author="test",
            comment="setup list_schemas fallback",
        )

        names = {s.schema_name for s in client.list_schemas()}
        assert "listed_schema" in names


class TestWriteDataByUuidSchemaAgnostic:
    """CHA-387: WriteData by ``table_uuid`` is schema-agnostic (catalog-wide).

    The by-``table_uuid`` write path resolves the table catalog-wide via
    ``resolve_table`` and reuses that resolved ``Table`` — it no longer issues
    a second, schema-scoped ``MetadataClient::get_table`` refetch. So a
    ``Change`` whose ``table_uuid`` lives in a schema other than the request
    ``schema_uuid`` resolves and writes (consistent with the read side,
    CHA-381), and the write path does ONE ``__penca_system__.tables`` merge
    per Change, not two.

    Pre-fix (current main): ``apply_one_change`` resolves the table
    catalog-wide, then refetches it schema-scoped via
    ``get_table(schema_b, table_uuid)`` — which misses when the table lives in
    schema_a -> NotFound, and is a second metadata read per Change.
    """

    @staticmethod
    def _catalog_with_two_schemas(client):
        """catalog + branch + (schema_a holding table_a) + an empty schema_b.

        Returns ``(catalog_uuid, branch_uuid, schema_a, schema_b, table_a)``.
        """
        catalog_uuid, branch_uuid = client.create_catalog(
            f"cha387_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_a = client.create_schema(
            "schema_a", catalog_uuid=catalog_uuid, author="test", comment="cha-387"
        )
        schema_b = client.create_schema(
            "schema_b", catalog_uuid=catalog_uuid, author="test", comment="cha-387"
        )
        table_a = client.create_table(
            "table_a",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a,
            author="test",
            comment="cha-387",
        )
        return catalog_uuid, branch_uuid, schema_a, schema_b, table_a

    def test_write_data_upsert_by_uuid_ignores_passed_schema(self):
        """An upsert by ``table_uuid`` with a MISMATCHED request ``schema_uuid``
        resolves catalog-wide and writes the row.

        Fail-first (current main): the schema-scoped refetch
        ``get_table(schema_b, table_a)`` misses -> NotFoundError raised by
        ``write_data``.
        """
        client = make_client()
        catalog_uuid, branch_uuid, _schema_a, schema_b, table_a = (
            self._catalog_with_two_schemas(client)
        )

        client.write_data(
            None,  # auto-commit
            Mutation(
                table_uuid=table_a,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_b,  # WRONG schema on purpose: uuid wins, catalog-wide
            branch_uuid=branch_uuid,
            author="test",
            comment="cha-387 upsert schema-agnostic",
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_a,
        )
        assert result.num_rows == 1

    def test_write_data_delete_by_uuid_ignores_passed_schema(self):
        """A delete by ``table_uuid`` with a MISMATCHED request ``schema_uuid``
        resolves catalog-wide and tombstones the row.

        Seeds the row via the CORRECT schema (works pre & post), then deletes
        via the wrong schema. Fail-first (current main): the delete's
        schema-scoped refetch misses -> NotFoundError.
        """
        client = make_client()
        catalog_uuid, branch_uuid, schema_a, schema_b, table_a = (
            self._catalog_with_two_schemas(client)
        )

        # Seed with the CORRECT schema so the row exists pre-fix.
        client.write_data(
            None,
            Mutation(
                table_uuid=table_a,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a,
            branch_uuid=branch_uuid,
            author="test",
            comment="cha-387 seed",
        )

        # Delete via the WRONG schema — the operation under test.
        client.write_data(
            None,
            Mutation(
                table_uuid=table_a,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_b,
            branch_uuid=branch_uuid,
            author="test",
            comment="cha-387 delete schema-agnostic",
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_a,
        )
        assert result.num_rows == 0

    def test_write_data_by_uuid_resolves_table_metadata_once(self):
        """A by-``table_uuid`` WriteData must issue the SAME number of
        ``__penca_system__.tables`` merge SELECTs as a by-``table_uuid``
        get_table (which resolves the row exactly once).

        Fail-first (current main): ``apply_one_change`` resolves the table
        catalog-wide AND refetches it schema-scoped -> 2x the merges of the
        single-resolve baseline. Green after dropping the refetch: 1x == 1x.
        K-agnostic equality (mirrors integration_query_test
        ::test_get_table_resolves_metadata_once).
        """
        client = make_client()
        catalog_uuid, branch_uuid = client.create_catalog(
            f"cha387_once_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "once_schema", catalog_uuid=catalog_uuid, author="test", comment="cha-387"
        )
        table_uuid = client.create_table(
            "once_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="cha-387",
        )

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        # The stream_merged over __penca_system__.tables reads this per-branch
        # upsert log; it's unique to this catalog, so background activity on
        # other catalogs doesn't pollute the count.
        tables_log = upsert_log_table(
            system_tables_table_uuid(catalog_uuid), branch_uuid
        )

        reset_pg_stat(pg)
        client.get_table(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_uuid=table_uuid
        )
        baseline = count_stmts_referencing(pg, tables_log)

        reset_pg_stat(pg)
        client.write_data(
            None,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            author="test",
            comment="cha-387 read-count",
        )
        under_test = count_stmts_referencing(pg, tables_log)

        assert baseline > 0, (
            "sanity: get_table by-uuid must touch __penca_system__.tables"
        )
        assert under_test == baseline, (
            f"write_data by-uuid issued {under_test} __penca_system__.tables "
            f"merge statements vs {baseline} for a single-resolve get_table "
            f"by-uuid — apply_one_change still double-reads the table row "
            f"(CHA-387: reuse the resolved Table, drop the schema-scoped "
            f"get_table refetch)"
        )

    def test_write_data_system_table_by_uuid_still_rejected(self):
        """Guard regression: mutating ``__penca_system__.tables`` by
        ``table_uuid`` (with a user schema in the request) stays rejected.

        Catalog-wide resolution CAN resolve the bootstrap system-table row,
        but ``assert_not_system_table`` fires on the resolved ``table_uuid``
        before any write. Green before AND after CHA-387 — it locks the guard
        audit: the dropped schema-scoped refetch added no system-table
        protection beyond this guard, which covers the complete
        ``__penca_system__`` registered-table set (``schemas`` + ``tables``).
        """
        client = make_client()
        catalog_uuid, branch_uuid = client.create_catalog(
            f"cha387_sys_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "user_schema", catalog_uuid=catalog_uuid, author="test", comment="cha-387"
        )
        sys_tables_uuid = system_tables_table_uuid(catalog_uuid)

        with pytest.raises(InvalidRequestError):
            client.write_data(
                None,
                Mutation(
                    table_uuid=sys_tables_uuid,
                    upserts=pa.table({"name": ["x"], "value": [1]}, schema=USER_SCHEMA),
                ),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                author="test",
                comment="cha-387 system-table guard",
            )
