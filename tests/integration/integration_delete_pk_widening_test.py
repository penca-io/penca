"""Integration tests for CHA-185: widen delete_log to carry PK columns,
ship Change.deletes as an Arrow IPC PK-batch on the wire.

Status (mid-impl, red): the proto wire shape has flipped to
``bytes deletes`` (Arrow IPC PK batch), but the storage schema, the
server-side row_uuid derivation, and the audit-shape changes are still
ahead of us. Each test fails for one of these reasons:

* Tests 1, 2, 4, 5, 6, 7 — server's delete branch silently drops the
  PK batch (commit 4 wires the decode + row_uuid_for_pk loop +
  widened delete_log insert); ``read_data`` / ``audit_data`` still see
  the row.
* Test 3 — schema-only assertion on ``audit_data`` deletes: today's
  shape exposes ``row_uuid``, not PK columns (commit 5).
* Test 8 — SQL ``DELETE`` translation ships an empty deletes payload
  pending commit 6, which collapses the per-row ``row_uuid_for_pk``
  loop in ``dml.rs::translate_delete`` into a PK-batch send.

See the plan comment on CHA-185 for the full commit sequence.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.errors import InvalidRequestError
from penca_client.naming import (
    delete_log_table,
    tx_table_log_partition,
)
from psycopg.sql import Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
    setup_schema,
    setup_with_data,
)

# PK schema for the default ``USER_SCHEMA`` (``primary_keys=["name"]``).
_PK_SCHEMA_NAME = pa.schema([pa.field("name", pa.utf8())])

# Composite-PK table fixture. Declared PK order is ``(region, name)``.
_COMPOSITE_USER_SCHEMA = pa.schema(
    [
        pa.field("region", pa.utf8()),
        pa.field("name", pa.utf8()),
        pa.field("value", pa.int64()),
    ]
)
_COMPOSITE_PK_SCHEMA = pa.schema(
    [
        pa.field("region", pa.utf8()),
        pa.field("name", pa.utf8()),
    ]
)


def _qi(name: str) -> str:
    return Identifier(name).as_string(None)


def _setup_composite_pk_table_with_data(client):
    """Seed a (region, name)-PK table with three rows on main.

    Returns ``{catalog_uuid, schema_uuid, table_uuid, main_branch_uuid}``.
    """
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"cpk_cat_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "cpk_schema",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="create_schema",
    )
    table_uuid = client.create_table(
        "cpk_table",
        _COMPOSITE_USER_SCHEMA,
        primary_keys=["region", "name"],
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
    batch = pa.table(
        {
            "region": ["us", "us", "eu"],
            "name": ["alice", "bob", "carol"],
            "value": [1, 2, 3],
        },
        schema=_COMPOSITE_USER_SCHEMA,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch_uuid,
    )

    return {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
        "main_branch_uuid": main_branch_uuid,
    }


class TestDeletePkWidening:
    """The wire / storage / audit changes for CHA-185, exercised end-to-end."""

    def test_delete_single_pk_round_trips_via_pk_batch(self):
        """Send a single-PK delete as an Arrow IPC batch; the row vanishes
        from ``read_data``, and ``audit_data`` surfaces the PK column +
        tx metadata on the deletes table — no ``row_uuid`` column."""
        client = make_client()
        context = setup_with_data(client)
        catalog_uuid = context["catalog_uuid"]
        schema_uuid = context["schema_uuid"]
        table_uuid = context["table_uuid"]
        branch_uuid = context["main_branch_uuid"]

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["bob"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        delete_tx = client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        # read_data: alice survives, bob is gone.
        rows = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert sorted(rows.column("name").to_pylist()) == ["alice"]

        # audit_data: deletes table has `name` (PK) + tx metadata,
        # no `row_uuid` column.
        _upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert deletes.num_rows == 1
        assert "name" in deletes.schema.names
        assert "row_uuid" not in deletes.schema.names
        assert deletes.column("name").to_pylist() == ["bob"]
        assert deletes.column("commit_micros").to_pylist() == [delete_tx.commit_micros]

    def test_delete_composite_pk_round_trips_via_pk_batch(self):
        """Composite-PK delete: PK columns in audit_data appear in the
        table's declared ``primary_keys`` order (region, name) — proves
        the server projects PKs by declared order, not batch order."""
        client = make_client()
        ctx = _setup_composite_pk_table_with_data(client)

        tx = client.begin_tx(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=ctx["table_uuid"],
                deletes=pa.table(
                    {"region": ["us"], "name": ["bob"]},
                    schema=_COMPOSITE_PK_SCHEMA,
                ),
            ),
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=ctx["catalog_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
        )

        rows = client.read_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
        )
        surviving = sorted(
            zip(
                rows.column("region").to_pylist(),
                rows.column("name").to_pylist(),
                strict=True,
            )
        )
        assert surviving == [("eu", "carol"), ("us", "alice")]

        _upserts, deletes = client.audit_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
        )
        assert deletes.num_rows == 1
        # PK columns appear in the table's declared primary_keys order.
        names = deletes.schema.names
        assert names.index("region") < names.index("name"), (
            f"PK columns must be in declared primary_keys order "
            f"(region, name); got {names}"
        )
        assert deletes.column("region").to_pylist() == ["us"]
        assert deletes.column("name").to_pylist() == ["bob"]

    def test_audit_data_delete_shape_has_pks_no_row_uuid(self):
        """Schema-only pin: post-widening, ``audit_data`` deletes is
        ``<pk_cols> + (began_at_micros, commit_micros, write_seq_num,
        commit_seq_num)``. CHA-507 moved ``comment``/``author`` off the default
        schema — they are opt-in via ``include_tx_metadata``. ``row_uuid`` is not
        in the projection (it remains a stored column in ``delete_log`` itself —
        merge join key)."""
        client = make_client()
        context = setup_with_data(client)
        catalog_uuid = context["catalog_uuid"]
        schema_uuid = context["schema_uuid"]
        table_uuid = context["table_uuid"]
        branch_uuid = context["main_branch_uuid"]

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        _upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )

        assert deletes.schema.names == [
            "name",
            "began_at_micros",
            "commit_micros",
            "write_seq_num",
            "commit_seq_num",
        ], (
            "audit_data deletes shape must be <pk_cols> + tx metadata "
            "(CHA-431 surfaces write_seq_num; CHA-507 moved comment/author off "
            "the default schema to the opt-in include_tx_metadata join), with "
            f"no row_uuid; got {deletes.schema.names}"
        )

    def test_delete_then_read_data_excludes_row_via_merge_on_read(self):
        """Merge-on-read invariant under widening: the server-derived
        ``row_uuid_for_pk(table_uuid, pk_values)`` still excludes the
        upserted row from ``read_data``. Pins that the join key in
        ``build_merge_resolved`` is unchanged by the PK-column widening."""
        client = make_client()
        context = setup_with_data(client)
        catalog_uuid = context["catalog_uuid"]
        schema_uuid = context["schema_uuid"]
        table_uuid = context["table_uuid"]
        branch_uuid = context["main_branch_uuid"]

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice", "bob"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        rows = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert rows.num_rows == 0, (
            f"merge-on-read should drop both deleted rows; "
            f"got {rows.column('name').to_pylist()}"
        )

    def test_delete_then_persist_carries_pks_into_cold_segments(self):
        """After ``persist`` purges hot, ``audit_data`` reads the cold
        delete segment directly. The PK column must be populated from
        cold — proves PKs were carried into the cold Lance segment, not
        just the hot Postgres ``delete_log``."""
        client = make_client()
        context = setup_with_data(client)
        catalog_uuid = context["catalog_uuid"]
        schema_uuid = context["schema_uuid"]
        table_uuid = context["table_uuid"]
        branch_uuid = context["main_branch_uuid"]

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            author="del-author",
            comment="del-bob",
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["bob"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        _upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            include_tx_metadata=True,
        )
        assert deletes.num_rows == 1
        assert "name" in deletes.schema.names
        assert deletes.column("name").to_pylist() == ["bob"]
        # Tx metadata round-trips through cold too (CHA-507: via the opt-in
        # include_tx_metadata join against the cold tx_log).
        assert deletes.column("author").to_pylist() == ["del-author"]
        assert deletes.column("comment").to_pylist() == ["del-bob"]

    def test_delete_pk_batch_with_zero_rows_is_a_noop(self):
        """Empty PK batch in ``Change.deletes`` succeeds; nothing lands
        in ``delete_log``; no ``tx_table_log`` row is emitted for the
        (tx, table) pair. The row-count check at ``apply_changes``
        treats both an empty IPC batch and an absent payload as no-op."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        driver = get_pg_driver()
        del_tbl = _qi(delete_log_table(table_uuid, main_branch_uuid))
        tx_table_part = _qi(tx_table_log_partition(catalog_uuid, main_branch_uuid))

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": []}, schema=_PK_SCHEMA_NAME),
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

        (delete_rows,) = driver.execute(f"SELECT COUNT(*) FROM {del_tbl}")[0]
        assert int(delete_rows) == 0

        (tx_table_rows_for_this_tx,) = driver.execute(
            f"SELECT COUNT(*) FROM {tx_table_part} "
            f"WHERE tx_uuid = %s AND table_uuid = %s",
            (tx.tx_uuid, table_uuid),
        )[0]
        assert int(tx_table_rows_for_this_tx) == 0, (
            "zero-row delete batch must not emit a tx_table_log row "
            "for (tx, table); apply_changes' empty-Change short-circuit "
            "should fire"
        )

    def test_delete_pk_batch_wrong_column_order_returns_invalid_argument(self):
        """Deletes batch with PK columns reordered relative to the
        table's declared ``primary_keys`` is rejected with
        INVALID_ARGUMENT. Server does not silently hash mismatched
        values — that would produce a ``row_uuid`` that disagrees with
        the upsert side and the delete would no-op invisibly."""
        client = make_client()
        ctx = _setup_composite_pk_table_with_data(client)

        # Table's declared PK order is (region, name); send (name, region).
        swapped_pk_schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("region", pa.utf8()),
            ]
        )

        tx = client.begin_tx(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
        )
        with pytest.raises(InvalidRequestError):
            client.write_data(
                tx.tx_uuid,
                Mutation(
                    table_uuid=ctx["table_uuid"],
                    deletes=pa.table(
                        {"name": ["bob"], "region": ["us"]},
                        schema=swapped_pk_schema,
                    ),
                ),
                catalog_uuid=ctx["catalog_uuid"],
                schema_uuid=ctx["schema_uuid"],
                branch_uuid=ctx["main_branch_uuid"],
            )

    def test_sql_delete_translates_to_pk_batch_not_hashed_row_uuids(self):
        """Flight SQL ``DELETE FROM t WHERE name = 'bob'`` writes the
        PK batch into ``Change.deletes``, not pre-hashed row_uuids. The
        post-CHA-185 audit shape carries the ``name`` PK column with
        ``"bob"`` — which can only be present if ``dml.rs::translate_delete``
        stopped hashing and shipped the raw PK batch."""
        client = make_client()
        catalog_name = f"sqldel_cat_{uuid4().hex[:8]}"
        catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
        schema_uuid = client.create_schema(
            "sqldel_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "sqldel_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        # CHA-169: pin the SQL connection to the freshly-created catalog
        # so subsequent execute_update calls don't get rejected as
        # cross-catalog. See `setup_with_data_named` in
        # ``integration_helpers.py``.
        client.catalog = catalog_name

        fqn = f"{catalog_name}.sqldel_schema.sqldel_table"
        client.execute_update(f"INSERT INTO {fqn} VALUES ('alice', 10)")
        client.execute_update(f"INSERT INTO {fqn} VALUES ('bob', 20)")

        affected = client.execute_update(f"DELETE FROM {fqn} WHERE name = 'bob'")
        assert affected == 1

        _upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert deletes.num_rows == 1
        assert "name" in deletes.schema.names, (
            "SQL DELETE must ship PK batch (post-CHA-185); deletes "
            "audit shape should include the `name` PK column instead "
            "of a hashed row_uuid"
        )
        assert deletes.column("name").to_pylist() == ["bob"]
