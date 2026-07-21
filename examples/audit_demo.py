#!/usr/bin/env python3
"""Demo script showing Penca's audit_data output.

Requires Docker services running: just penca-up
"""

from uuid import uuid4

import pyarrow as pa
from penca_client import PencaClient, Mutation
from penca_client._time import micros_to_datetime

SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8()),
        pa.field("value", pa.int64()),
    ]
)
PK_SCHEMA = pa.schema([pa.field("name", pa.utf8())])


def print_audit(upserts: pa.Table, deletes: pa.Table) -> None:
    if upserts.num_rows:
        print("Upserts:")
        print(upserts.to_pandas().to_markdown(index=False))
    else:
        print("Upserts: (none)")

    if deletes.num_rows:
        print("Deletes:")
        print(deletes.to_pandas().to_markdown(index=False))
    else:
        print("Deletes: (none)")


def main():
    client = PencaClient.from_settings()

    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"demo_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "demo_schema",
        catalog_uuid=catalog_uuid,
        author="demo",
        comment="create demo_schema",
    )
    table_uuid = client.create_table(
        "users",
        SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="demo",
        comment="create users table",
    )
    # Branches are catalog-scoped (CHA-163 / `feedback_branches_fork_from_main`).
    branch_uuid = main_branch_uuid

    # TX 1: insert alice and bob. With the unified upsert_log, there's no
    # longer a client-side distinction between "insert" and "update" —
    # every row write is an upsert.
    tx1 = client.begin_tx(
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
        author="demo",
        comment="insert alice and bob",
    )
    client.write_data(
        tx1.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            upserts=pa.table(
                {"name": ["alice", "bob"], "value": [10, 20]},
                schema=SCHEMA,
            ),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    committed1 = client.commit_tx(
        tx1.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
    )
    print(f"TX 1 committed at {micros_to_datetime(committed1.commit_micros)}")

    # TX 2: mixed batch — alice gets a new value (existing row_uuid),
    # charlie is brand new (new row_uuid). One upserts payload handles both.
    tx2 = client.begin_tx(
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
        author="demo",
        comment="update alice, insert charlie",
    )
    client.write_data(
        tx2.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            upserts=pa.table(
                {"name": ["alice", "charlie"], "value": [99, 30]},
                schema=SCHEMA,
            ),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    committed2 = client.commit_tx(
        tx2.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
    )
    print(f"TX 2 committed at {micros_to_datetime(committed2.commit_micros)}")

    # TX 3: delete bob.
    tx3 = client.begin_tx(
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
        author="demo",
        comment="delete bob",
    )
    client.write_data(
        tx3.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            deletes=pa.table({"name": ["bob"]}, schema=PK_SCHEMA),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    committed3 = client.commit_tx(
        tx3.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
    )
    print(f"TX 3 committed at {micros_to_datetime(committed3.commit_micros)}")

    print("\n--- Current state (read_data) ---")
    current = client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
    )
    print(current.to_pandas().to_markdown(index=False))

    # Full audit trail — the new shape returns two tables: one for row
    # upsert versions, one for tombstones. Deletes now appear in the
    # audit trail (previously they were invisible).
    print("\n--- Full audit trail (audit_data) ---")
    upserts, deletes = client.audit_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
    )
    print_audit(upserts, deletes)

    # Audit trail filtered: only changes after TX 1.
    print("\n--- Audit trail (after TX 1 only) ---")
    after = micros_to_datetime(committed1.commit_micros + 1)
    upserts, deletes = client.audit_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
        after=after,
    )
    print_audit(upserts, deletes)

    # Time-travel: state as of TX 1.
    print("\n--- Time-travel: state as of TX 1 ---")
    as_of = micros_to_datetime(committed1.commit_micros)
    snapshot = client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
        as_of=as_of,
    )
    print(snapshot.to_pandas().to_markdown(index=False))


if __name__ == "__main__":
    main()
