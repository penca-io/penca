"""Acceptance tests for ``as_of_micros``-aware name resolution (CHA-236).

A renamed table found via time travel must resolve under the same
snapshot as the data read — otherwise
``ReadData(table_name="foo", as_of_micros=150)`` returns NOT_FOUND for
a table renamed ``foo → bar`` at T=200 even though it existed as
``foo`` at T=150.

Read RPCs grow ``optional int64 as_of_micros`` (or reuse an existing
window field for ``AuditData``); resolvers thread the resulting
``ReadSnapshot`` through ``__penca_system__.{schemas,tables}``
``stream_merged``. Resolution order in every handler is:

1. ``catalog_uuid`` ← ``catalog_store`` SELECT (non-MVCC)
2. ``branch_uuid`` ← ``branch_store`` SELECT (non-MVCC)
3. Derive ``ReadSnapshot`` from ``(as_of_micros, open_tx_uuid)``
4. ``schema_uuid`` ← ``__penca_system__.schemas`` ``stream_merged``
5. ``table_uuid`` ← ``__penca_system__.tables`` ``stream_merged``

Catalog + branch renames remain snapshot-blind (see plan §"as_of-aware
name resolution" + CHA-240); only schema + table resolution honors
``as_of_micros`` here.

Run via ``just integration-test as_of_naming``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client._time import micros_to_datetime
from penca_client.errors import NotFoundError
from penca_client.naming import MAIN_BRANCH_NAME

from .integration_helpers import USER_SCHEMA, make_client


def _new_catalog(client, prefix: str = "asof_cat") -> tuple[str, str, str]:
    """Future-shape ``CreateCatalog`` — returns
    ``(name, catalog_uuid, main_branch_uuid)``."""
    name = f"{prefix}_{uuid4().hex[:8]}"
    catalog_uuid, main_branch_uuid = client.create_catalog(name, "owner")
    return name, catalog_uuid, main_branch_uuid


def _write_rows_returning_commit(
    client,
    *,
    catalog_uuid: str,
    schema_uuid: str,
    branch_uuid: str,
    table_uuid: str,
    rows: dict[str, list],
) -> int:
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
    committed = client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )
    return committed.commit_micros


class TestAsOfNameResolution:
    def test_as_of_name_resolution_finds_table_at_historical_name(self):
        """CHA-236 #19: rename ``foo → bar`` at T_rename;
        ``read_data(table_name="foo", as_of=T_pre)`` returns data from
        when the table was named ``foo``. Name resolution uses the same
        snapshot as the data read."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "foo",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        write_at = _write_rows_returning_commit(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            rows={"name": ["alice"], "value": [1]},
        )

        # Rename strictly after the write.
        client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            new_table_name="bar",
            author="test",
            comment="rename after write",
        )

        # Read at the write's commit timestamp — name resolution must
        # see the table as ``foo`` at that snapshot.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_name="foo",
            branch_uuid=main_branch_uuid,
            as_of=micros_to_datetime(write_at),
        )
        assert result.column("name").to_pylist() == ["alice"]

    def test_as_of_name_resolution_misses_post_rename_name_before_rename(self):
        """CHA-236 #20: rename ``foo → bar`` at T_rename;
        ``read_data(table_name="bar", as_of=T_pre)`` returns NOT_FOUND
        because no table was named ``bar`` at the historical snapshot."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "foo",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        write_at = _write_rows_returning_commit(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            rows={"name": ["alice"], "value": [1]},
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
            comment="rename after write",
        )

        with pytest.raises(NotFoundError):
            client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_name="bar",
                branch_uuid=main_branch_uuid,
                as_of=micros_to_datetime(write_at),
            )

    def test_as_of_seq_name_resolution_finds_table_at_historical_name(self):
        """CHA-443 (RT-4): rename ``foo → bar``;
        ``read_data(table_name="foo", as_of_seq=N_pre)`` must resolve the table
        at its historical name on the SEQ axis — identifier resolution pins on
        the read's axis (the same rule the ``as_of_micros`` path above already
        follows).

        RED today: a seq read resolves names at ``pg_now``/latest
        (``ReadRequestIdents`` reads only the ``commit_micros`` arm, so a seq
        read yields ``None`` and the resolver falls back to ``pg_now``), where
        the table is now ``bar`` — so ``table_name="foo"`` raises NotFound.
        IMPL-6 makes identifier resolution seq-aware and greens this."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "foo",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        _write_rows_returning_commit(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            rows={"name": ["alice"], "value": [1]},
        )

        # The write's commit_seq_num — strictly below the rename (a later commit).
        upserts, _deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        write_seq = upserts.column("commit_seq_num").to_pylist()[0]

        # Rename strictly after the write.
        client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            new_table_name="bar",
            author="test",
            comment="rename after write",
        )

        # Read by the historical name on the SEQ axis at the write's seq — name
        # resolution must see the table as ``foo`` at that seq snapshot.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_name="foo",
            branch_uuid=main_branch_uuid,
            as_of_seq=write_seq,
        )
        assert result.column("name").to_pylist() == ["alice"]

    def test_get_table_as_of_seq_resolves_historical_name(self):
        """CHA-460 (RT1): ``GetTable`` honors ``as_of_seq`` for renames —
        identifier resolution on the SEQ axis, the metadata-RPC sibling of
        ``read_data``'s seq pin (CHA-443). Rename ``foo → bar``;
        ``get_table(table_name="foo", as_of_seq=write_seq)`` resolves the table
        at its historical name, ``table_name="bar"`` at the same seq is
        NOT_FOUND.

        RED today: ``GetTableRequest`` carries no ``as_of_seq`` field and the
        client ``get_table`` has no ``as_of_seq`` kwarg, so SQL identifier
        resolution cannot pin the seq axis. I1 adds the field + client param."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "foo",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        _write_rows_returning_commit(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            rows={"name": ["alice"], "value": [1]},
        )
        # The write's commit_seq_num — strictly below the rename (a later commit).
        upserts, _deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        write_seq = upserts.column("commit_seq_num").to_pylist()[0]

        client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            new_table_name="bar",
            author="test",
            comment="rename after write",
        )

        pre = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_name="foo",
            branch_uuid=main_branch_uuid,
            as_of_seq=write_seq,
        )
        assert pre.table_uuid == table_uuid
        assert pre.table_name == "foo"

        with pytest.raises(NotFoundError):
            client.get_table(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_name="bar",
                branch_uuid=main_branch_uuid,
                as_of_seq=write_seq,
            )

    def test_get_schema_as_of_seq_resolves_historical_name(self):
        """CHA-460 (RT1): ``GetSchema`` / ``ListSchemas`` honor ``as_of_seq``
        for schema renames — the seq sibling of the ``as_of_micros`` schema test
        above. Rename ``s_old → s_new`` after a write at ``pre_seq``;
        ``get_schema(schema_name="s_old", as_of_seq=pre_seq)`` finds it, the new
        name at that seq is NOT_FOUND, and ``list_schemas`` lists the old name.

        RED today: ``GetSchemaRequest`` / ``ListSchemasRequest`` carry no
        ``as_of_seq`` field (no client kwarg). I1 adds them."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s_old",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        anchor_table = client.create_table(
            "anchor",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        _write_rows_returning_commit(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=anchor_table,
            rows={"name": ["seed"], "value": [0]},
        )
        upserts, _deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=anchor_table,
        )
        pre_seq = upserts.column("commit_seq_num").to_pylist()[0]

        client.update_schema(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            new_schema_name="s_new",
            author="test",
            comment="rename schema",
        )

        pre = client.get_schema(
            catalog_uuid=catalog_uuid,
            schema_name="s_old",
            branch_uuid=main_branch_uuid,
            as_of_seq=pre_seq,
        )
        assert pre.schema_uuid == schema_uuid
        assert pre.schema_name == "s_old"

        with pytest.raises(NotFoundError):
            client.get_schema(
                catalog_uuid=catalog_uuid,
                schema_name="s_new",
                branch_uuid=main_branch_uuid,
                as_of_seq=pre_seq,
            )

        listed_pre = list(
            client.list_schemas(
                catalog_uuid=catalog_uuid,
                branch_uuid=main_branch_uuid,
                as_of_seq=pre_seq,
            )
        )
        names_pre = {s.schema_name for s in listed_pre}
        assert "s_old" in names_pre
        assert "s_new" not in names_pre

    def test_open_tx_name_resolution_sees_rename_ryow(self):
        """CHA-236 #21: rename inside an open tx; ``get_table`` with
        ``open_tx_uuid`` resolves the new name (read-your-own-writes).
        Same convention as data RYOW — the resolver layers the open
        tx's uncommitted writes onto the snapshot."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "foo",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        # Join the open tx — no author/comment (CHA-164 mode-switch).
        client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            primary_keys=["name"],
            new_table_name="bar",
            tx_uuid=tx.tx_uuid,
        )

        # RYOW lookup by the new name within the open tx.
        within_tx = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_name="bar",
            branch_uuid=main_branch_uuid,
            open_tx_uuid=tx.tx_uuid,
        )
        assert within_tx.table_uuid == table_uuid
        assert within_tx.table_name == "bar"

        # A concurrent reader without ``open_tx_uuid`` still sees the
        # pre-rename name.
        outside_tx = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert outside_tx.table_name == "foo"

        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

    def test_as_of_name_resolution_for_schema(self):
        """CHA-236 #22: ``GetSchema`` / ``ListSchemas`` honor
        ``as_of_micros`` for renames. Rename ``s_old → s_new`` after
        T_pre; ``get_schema(schema_name="s_old", as_of=T_pre)`` finds
        the schema; querying with the new name at the historical
        snapshot returns NOT_FOUND."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s_old",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )

        # Anchor T_pre via an unrelated tx commit on main (any committed
        # tx provides a snapshot timestamp before the rename).
        # CreateTable inside ``s_old`` works as a benign anchor.
        anchor_table = client.create_table(
            "anchor",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        t_pre = _write_rows_returning_commit(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=anchor_table,
            rows={"name": ["seed"], "value": [0]},
        )

        client.update_schema(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            new_schema_name="s_new",
            author="test",
            comment="rename schema",
        )

        # Historical snapshot finds the old name.
        pre = client.get_schema(
            catalog_uuid=catalog_uuid,
            schema_name="s_old",
            branch_uuid=main_branch_uuid,
            as_of_micros=t_pre,
        )
        assert pre.schema_uuid == schema_uuid
        assert pre.schema_name == "s_old"

        # New name at the historical snapshot ⇒ NOT_FOUND.
        with pytest.raises(NotFoundError):
            client.get_schema(
                catalog_uuid=catalog_uuid,
                schema_name="s_new",
                branch_uuid=main_branch_uuid,
                as_of_micros=t_pre,
            )

        # ListSchemas at the historical snapshot lists the old name.
        listed_pre = list(
            client.list_schemas(
                catalog_uuid=catalog_uuid,
                branch_uuid=main_branch_uuid,
                as_of_micros=t_pre,
            )
        )
        names_pre = {s.schema_name for s in listed_pre}
        assert "s_old" in names_pre
        assert "s_new" not in names_pre

    def test_audit_data_name_resolution_uses_committed_at_upper_bound(self):
        """CHA-236 #23: ``AuditData`` reuses its existing
        ``committed_at`` window for name resolution — when
        ``committed_at.max_micros`` is set, the resolver snapshots at
        that timestamp. Rename ``foo → bar`` at T_rename;
        ``audit_data(table_name="foo", before=T_pre)`` returns the
        historical rows."""
        client = make_client()
        _, catalog_uuid, main_branch_uuid = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "foo",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        write_at = _write_rows_returning_commit(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            rows={"name": ["alice"], "value": [1]},
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
            comment="rename after write",
        )

        # ``committed_at`` is half-open ``[from, to)`` (server-side
        # convention; see ``filter_batch_by_committed_at`` in
        # ``crates/penca-api/src/query.rs``). Step the upper bound past
        # the write's commit so its row is included, while staying
        # before the rename's commit so name resolution still sees
        # ``foo`` at the snapshot.
        upserts, _deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_name="foo",
            branch_uuid=main_branch_uuid,
            before=micros_to_datetime(write_at + 1),
        )
        assert upserts.column("name").to_pylist() == ["alice"]


# Reference: MAIN_BRANCH_NAME is the well-known parent branch this file
# operates on. Imported to keep parity with the rename-test fixtures and
# document that catalog + branch name resolution stays snapshot-blind
# (catalog_store / branch_store are non-MVCC — see plan §"as_of-aware
# name resolution"). CHA-240 tracks the design ticket to migrate them.
_ = MAIN_BRANCH_NAME
