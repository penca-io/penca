"""CHA-92 — RPC input validation at the servicer boundary.

Acceptance tests proving the gRPC servicers reject malformed / absent /
non-open request shapes with the right structured error *before* the
request reaches Postgres — instead of surfacing a confusing ``INTERNAL``
from a downstream type/relation error.

Covers three validation groups: wire-format (UUID parseability, name
format, value bounds), existence (parseable-but-absent identifiers resolve
to ``NOT_FOUND``), and tx-open state (appending against a non-open tx fails
``FAILED_PRECONDITION``).

One case — ``WriteData`` with ``tx_uuid`` *and* ``author``/``comment`` —
is a regression guard rather than a new check: it was already rejected by
the lib helper ``resolve_or_auto_commit_tx`` (``penca-api``); the change
only moves *where* the check runs (lib → servicer ``validate_write_data``),
so the observable error is unchanged.

Scoped run: ``just integration-test rpc_validation``
"""

from __future__ import annotations

import time
from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.errors import (
    FailedPreconditionError,
    InvalidRequestError,
    NotFoundError,
)

from .integration_helpers import (
    USER_SCHEMA,
    make_client,
    setup_schema,
)

# ttl ceiling the server is configured with (docker/compose.yml
# WRITE_MAX_TX_TIMEOUT_SECONDS); a request above it must be rejected.
MAX_TX_TIMEOUT_SECONDS = 3600


def _one_row_mutation(table_uuid: str) -> Mutation:
    """A single well-formed upsert so the request is valid apart from the
    field under test — validation must fire on that field, not on payload."""
    batch = pa.table({"name": ["alice"], "value": [42]}, schema=USER_SCHEMA)
    return Mutation(table_uuid=table_uuid, upserts=batch)


class TestWritePathFormatValidation:
    """WriteService format validation → ``INVALID_ARGUMENT``."""

    def test_write_data_empty_tx_uuid_rejected(self):
        """Append with ``tx_uuid=""`` (present, empty) → INVALID_ARGUMENT.

        Today: routed to the append path with an empty uuid → Postgres
        ``invalid input syntax for type uuid: ""`` → surfaced as INTERNAL.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        with pytest.raises(InvalidRequestError):
            client.write_data(
                "",
                _one_row_mutation(table_uuid),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
            )

    def test_write_data_non_uuid_tx_uuid_rejected(self):
        """Append with a non-UUID ``tx_uuid`` → INVALID_ARGUMENT.

        Today: interpolated into ``format!("'{tx_uuid}'")`` in
        penca-storage-hot → Postgres syntax error → INTERNAL.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        with pytest.raises(InvalidRequestError):
            client.write_data(
                "not-a-uuid",
                _one_row_mutation(table_uuid),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
            )

    def test_begin_tx_ttl_exceeds_max_rejected(self):
        """``BeginTx`` with ``timeout_seconds`` above the server ceiling →
        INVALID_ARGUMENT. Today: unbounded, so the tx is opened normally."""
        client = make_client()
        _schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        with pytest.raises(InvalidRequestError):
            client.begin_tx(
                catalog_uuid=catalog_uuid,
                branch_uuid=main_branch_uuid,
                author="t",
                comment="ttl over max",
                timeout_seconds=MAX_TX_TIMEOUT_SECONDS + 1,
            )

    def test_create_catalog_empty_name_rejected(self):
        """``CreateCatalog`` with an empty name → INVALID_ARGUMENT.

        Today: no name-format check at the boundary; an empty name is
        accepted / fails downstream rather than as a clean argument error.
        """
        client = make_client()
        with pytest.raises(InvalidRequestError):
            client.create_catalog("", "owner")

    # -- regression guard (already enforced in lib) ------------------------

    def test_write_data_tx_uuid_with_author_comment_rejected(self):
        """Append (``tx_uuid`` set) with ``author``/``comment`` also set →
        INVALID_ARGUMENT.

        Already rejected by the lib helper ``resolve_or_auto_commit_tx``;
        CHA-92 moves the check to ``validate_write_data`` at the servicer
        with no behavior change. This is a regression guard, not a new check.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        with pytest.raises(InvalidRequestError):
            client.write_data(
                str(uuid4()),
                _one_row_mutation(table_uuid),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
                author="t",
                comment="append must not carry author/comment",
            )


def _other_catalog_with_schema(client) -> str:
    """Create a second catalog + schema; return its schema_uuid (resident in
    a *different* catalog than ``setup_schema``'s)."""
    other_catalog_uuid, _branch = client.create_catalog(
        f"other_cat_{uuid4().hex[:8]}", "owner"
    )
    return client.create_schema(
        "other_schema",
        catalog_uuid=other_catalog_uuid,
        author="test",
        comment="cross-catalog fixture",
    )


class TestExistenceResolution:
    """Parseable-but-absent identifiers resolve to ``NOT_FOUND``.

    Covers the cross-catalog schema case (``catalog_uuid=A`` with a
    ``schema_uuid`` that lives in catalog B) and ``DeleteCatalog`` against a
    missing catalog — both of which, without boundary validation, fail
    ``INTERNAL`` on a missing per-catalog ``branch_store`` relation rather
    than a clean ``NOT_FOUND``. The remaining cases guard existing
    already-correct ``NOT_FOUND`` behavior.
    """

    def test_create_table_cross_catalog_schema_rejected(self):
        """``catalog_uuid=A`` + ``schema_uuid=X`` where X lives in catalog B
        → NOT_FOUND (the fourth-motivating-case).

        Today: the uuid path resolves syntactically (no residency check),
        so the inconsistent tuple reaches a partition INSERT and fails as
        INTERNAL (``relation "<uuid>_meta_..._part" does not exist``) — or
        silently creates an orphan. Either way, not a clean NOT_FOUND.
        """
        client = make_client()
        _schema_a, _table_a, catalog_a, _branch_a = setup_schema(client)
        foreign_schema = _other_catalog_with_schema(client)
        with pytest.raises(NotFoundError):
            client.create_table(
                "cross_catalog_table",
                USER_SCHEMA,
                primary_keys=["name"],
                catalog_uuid=catalog_a,
                schema_uuid=foreign_schema,
                author="test",
                comment="cross-catalog schema",
            )

    def test_write_data_nonexistent_table_uuid_rejected(self):
        """Write path: ``WriteData`` against a well-formed but absent
        ``table_uuid`` → NOT_FOUND.

        Regression guard: the write path already resolves the table row and
        returns NOT_FOUND. Existence resolution on the uuid path must keep
        this NOT_FOUND.
        """
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        with pytest.raises(NotFoundError):
            client.write_data(
                None,
                _one_row_mutation(str(uuid4())),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
                author="t",
                comment="absent table_uuid",
            )

    def test_update_catalog_nonexistent_uuid_rejected(self):
        """``UpdateCatalog`` on an absent ``catalog_uuid`` → NOT_FOUND.

        Regression guard: UpdateCatalog already reads the catalog row before
        mutating. The genuinely-broken catalog-only path is DeleteCatalog
        (below) — bringing it to parity must not regress UpdateCatalog.
        """
        client = make_client()
        with pytest.raises(NotFoundError):
            client.update_catalog(catalog_uuid=str(uuid4()), owner="x")

    def test_delete_catalog_nonexistent_uuid_rejected(self):
        """``DeleteCatalog`` on an absent ``catalog_uuid`` → NOT_FOUND."""
        client = make_client()
        with pytest.raises(NotFoundError):
            client.delete_catalog(catalog_uuid=str(uuid4()))

    # -- regression guard: read path already surfaces NOT_FOUND ------------

    def test_read_data_nonexistent_table_uuid_not_found(self):
        """Read path with an absent ``table_uuid`` → NOT_FOUND.

        Post-CHA-381 the catalog-wide ``resolve_table_by_uuid`` existence
        read surfaces this (the old schema-scoped arrow-schema fallback was
        removed in the resolver fold-in); this guards the NOT_FOUND against
        regression.
        """
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        with pytest.raises(NotFoundError):
            client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
                table_uuid=str(uuid4()),
            )


class TestTxOpenState:
    """Appending to a non-open tx is rejected, not silently written."""

    def test_write_data_nonexistent_tx_rejected(self):
        """Append with a well-formed ``tx_uuid`` that was never begun →
        NOT_FOUND. Today the append writes rows referencing a tx with no
        ``begin_tx_log`` entry (invisible to readers, but written)."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        with pytest.raises(NotFoundError):
            client.write_data(
                str(uuid4()),
                _one_row_mutation(table_uuid),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
            )

    def test_write_data_aborted_tx_rejected(self):
        """``BeginTx`` → ``AbortTx`` → append → FAILED_PRECONDITION.
        Today the append against the aborted tx succeeds."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        tx = client.begin_tx(catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid)
        client.abort_tx(
            tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
        )
        with pytest.raises(FailedPreconditionError):
            client.write_data(
                tx.tx_uuid,
                _one_row_mutation(table_uuid),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
            )

    def test_write_data_expired_tx_rejected(self):
        """``BeginTx`` with a 2s ttl, wait past it, then append →
        FAILED_PRECONDITION. Today the append against the expired tx
        succeeds.

        Margin: a 2s ttl + 4s wait keeps comfortable headroom over coarse
        expiry granularity / clock skew, and stays well under the max ttl
        ceiling while remaining above any plausible min-ttl bound (>0).
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
            timeout_seconds=2,
        )
        time.sleep(4)
        with pytest.raises(FailedPreconditionError):
            client.write_data(
                tx.tx_uuid,
                _one_row_mutation(table_uuid),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
            )
