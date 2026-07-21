"""CHA-492 — explicit `ReadDataRequest.indexes` classification.

A caller may name a covering index on the read request via the structured
`indexes` field (an Arrow batch of the index's key columns + equality values,
the secondary-index sibling of `ids`). The server classifies each requested
index against the table's DEFINED index set BEFORE planning:

  Case A  index columns with NO defined index  → error, raised pre-plan
          (INVALID_ARGUMENT / FAILED_PRECONDITION — fail-fast at the boundary,
          the same discipline as the `ids` batch-shape validation)

  Case B  a DEFINED index whose sidecar is not yet materialized on the
          snapshot → NO error; the read falls back to a merge scan with the
          residual and returns the correct rows (visible-index lag, ADR 0026)

Fail-first: `ReadDataRequest.indexes` / the `read_data(indexes=...)` client
kwarg do not exist yet, so the request is unconstructable — Case A raises no
server error (the `read_data` call fails with `TypeError` before reaching the
server, which is NOT an `ApiError`, so the `pytest.raises((...))` does not
swallow it) and Case B cannot issue the read. Both fail.

Scoped run:  just integration-test cha492_indexes_classification
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from penca_client.errors import FailedPreconditionError, InvalidRequestError

from .integration_helpers import (
    SCALAR_BTREE,
    USER_SCHEMA,
    make_client,
    setup_schema,
)


def _seed(client, ctx, rows: dict[str, int]) -> None:
    from penca_client import Mutation

    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
    batch = pa.table(
        {"name": list(rows.keys()), "value": list(rows.values())},
        schema=USER_SCHEMA,
    )
    client.write_data(
        None,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        author="test",
        comment="cha-492 fixture",
    )


class TestIndexesClassification:
    def test_undefined_index_errors_pre_plan(self):
        """Case A: `indexes` naming columns with no defined index is rejected
        at the boundary, before the read plans."""
        client = make_client()
        ctx = setup_schema(client)
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
        _seed(client, ctx, {"alice": 10, "bob": 20})

        # No index is defined on `value`; a structured seek naming it must be
        # rejected rather than silently ignored or run as an unindexed scan.
        undefined = pa.table({"value": pa.array([10], pa.int64())})
        with pytest.raises((InvalidRequestError, FailedPreconditionError)):
            client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
                table_uuid=table_uuid,
                indexes=undefined,
            )

    def test_defined_but_unmaterialized_index_falls_back(self):
        """Case B: a defined index whose sidecar is not yet built on the
        snapshot serves the read via a scan fallback — correct rows, no error."""
        client = make_client()
        ctx = setup_schema(client)
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
        _seed(client, ctx, {"alice": 10, "bob": 20})

        kw = {
            "catalog_uuid": catalog_uuid,
            "schema_uuid": schema_uuid,
            "branch_uuid": main_branch_uuid,
            "table_uuid": table_uuid,
        }
        # Snapshot the baseline FIRST, then declare the index: the sidecar is
        # declared but not materialized on the existing snapshot (no rebuild).
        client.persist(**kw)
        client.snapshot(**kw)
        client.purge(**kw)
        client.create_index(
            table_uuid=table_uuid,
            index_name="idx_value",
            columns=["value"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author="test",
            comment="cha-492 fixture",
        )

        # Defined (surfaces in Table.indexes) but unmaterialized on the
        # snapshot → the classify pass emits no seek entry and the read falls
        # back to a merge scan with the residual `value = 10`. No error.
        result = client.read_data(
            indexes=pa.table({"value": pa.array([10], pa.int64())}),
            **kw,
        )
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["alice"]
        assert result.column("value").to_pylist() == [10]
