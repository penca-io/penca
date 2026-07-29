"""Shared helpers for integration tests.

These are plain functions (not pytest fixtures) used across all
integration test files. Each test creates its own catalog/schema/table
to avoid inter-test pollution.

The ``PsycopgDriver`` + ``DbDriver`` ABC also live here because the
runtime client never opens a direct PG connection — only the
integration suite needs raw PG access for white-box assertions on
state the gRPC API doesn't surface.

Prerequisites:
    - Docker daemon must be running (Docker Desktop or dockerd).
    - Run via 'just integration-test' which sets PENCA_DB_* and
      PENCA_OBJECT_STORAGE_* automatically.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache
from typing import Any
from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation
from penca_client.client import PencaClient
from penca_client.config import ClientSettings, DbSettings
from penca_client.naming import (
    row_uuid_for_pk,
    table_snapshot_uuid,
)
from psycopg import Connection
from psycopg.abc import Buffer
from psycopg.adapt import Loader
from psycopg.postgres import types as pg_types
from psycopg.pq import Format
from psycopg.sql import Composable
from psycopg_pool import ConnectionPool


class DbDriver(ABC):
    """Executes queries against a transactional database."""

    @abstractmethod
    def execute(
        self,
        query: str | Composable,
        params: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return all result rows."""
        ...

    @abstractmethod
    def execute_no_result(
        self,
        query: str | Composable,
        params: tuple[Any, ...] = (),
    ) -> None:
        """Execute a statement that returns no rows (INSERT/UPDATE/DELETE)."""
        ...

    @abstractmethod
    def execute_stream(
        self,
        query: str | Composable,
        params: tuple[Any, ...] = (),
        *,
        batch_size: int = 100,
    ) -> Iterator[tuple[Any, ...]]:
        """Execute a query and yield rows one at a time via server-side cursor.

        Uses a server-side cursor to avoid materializing the full result
        set in memory. Rows are fetched from the server in batches of
        ``batch_size`` for network efficiency, but yielded individually.
        """
        ...

    @abstractmethod
    def execute_many(
        self,
        query: str | Composable,
        params_seq: list[tuple[Any, ...]],
    ) -> None:
        """Execute a statement for each set of parameters.

        Runs all executions on a single connection for efficiency.
        """
        ...

    @abstractmethod
    @contextmanager
    def transaction(self) -> Iterator[DbDriver]:
        """Yield a driver that runs all operations in a single transaction."""
        ...

    @abstractmethod
    @contextmanager
    def advisory_lock(self, key: str) -> Iterator[None]:
        """Hold a cross-process advisory lock keyed by ``key`` for the block."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Release any resources held by this driver (e.g., connection pool)."""
        ...


class _UUIDStringLoader(Loader):
    """Load Postgres UUID values as plain strings.

    psycopg's default loader returns ``uuid.UUID`` objects, which
    pyarrow cannot convert to ``utf8()`` arrays. Registering this
    loader on a connection makes all UUID columns come back as strings,
    eliminating per-row isinstance checks.
    """

    format = Format.TEXT

    def load(self, data: Buffer) -> str:
        if isinstance(data, memoryview):
            data = bytes(data)

        return data.decode()


class PsycopgDriver(DbDriver):
    """PostgreSQL driver using psycopg3 with connection pooling."""

    def __init__(
        self,
        conninfo: str,
        min_size: int = 2,
        max_size: int = 10,
    ) -> None:
        self._pool = ConnectionPool(
            conninfo,
            min_size=min_size,
            max_size=max_size,
            open=True,
            configure=self._configure_connection,
        )

    @staticmethod
    def _configure_connection(connection: Connection[Any]) -> None:
        """Apply Penca-specific adapter settings to a psycopg connection."""
        connection.adapters.register_loader(
            pg_types["uuid"].oid,
            _UUIDStringLoader,
        )

    def execute(
        self,
        query: str | Composable,
        params: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        with self._pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    def execute_no_result(
        self,
        query: str | Composable,
        params: tuple[Any, ...] = (),
    ) -> None:
        with self._pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)

    def execute_many(
        self,
        query: str | Composable,
        params_seq: list[tuple[Any, ...]],
    ) -> None:
        if not params_seq:
            return

        with self._pool.connection() as connection:
            with connection.cursor() as cursor:
                # Use pipeline mode to batch round-trips.
                with connection.pipeline():
                    cursor.executemany(query, params_seq)

    def execute_stream(
        self,
        query: str | Composable,
        params: tuple[Any, ...] = (),
        *,
        batch_size: int = 100,
    ) -> Iterator[tuple[Any, ...]]:
        with self._pool.connection() as connection:
            with connection.cursor(name="stream") as cursor:
                cursor.arraysize = batch_size
                cursor.execute(query, params)
                yield from cursor

    @contextmanager
    def transaction(self) -> Iterator[DbDriver]:
        """Check out a connection and yield a driver sharing that transaction.

        All operations on the yielded driver run in one Postgres
        transaction. Row locks acquired via ``SELECT ... FOR UPDATE``
        are held until the block exits. Commits on normal exit, rolls
        back on exception.
        """
        with self._pool.connection() as connection:
            yield _PsycopgTxDriver(connection)

    @contextmanager
    def advisory_lock(self, key: str) -> Iterator[None]:
        """Hold a Postgres session-scoped advisory lock for the block.

        Uses ``pg_advisory_lock(1, hashtext(key))`` on a dedicated
        pooled connection held for the lock's lifetime. Connection is
        placed in autocommit so the lock/unlock statements are not
        wrapped in an implicit transaction. CHA-141: if any exception
        path prevents the unlock, the connection is closed before
        being returned so the pool discards it rather than handing a
        live session-scoped lock to the next consumer.
        """
        connection = self._pool.getconn()
        unlock_done = False
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_lock(1, hashtext(%s))",
                    (key,),
                )

            try:
                yield
            finally:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(1, hashtext(%s))",
                        (key,),
                    )

                unlock_done = True
        finally:
            if not unlock_done:
                # CHA-141: lock may still be held — kill the session so
                # the pool discards this connection on putconn.
                connection.close()

            self._pool.putconn(connection)

    def close(self) -> None:
        """Close the connection pool and release all connections."""
        self._pool.close()


class _PsycopgTxDriver(DbDriver):
    """Driver bound to an existing psycopg connection/transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def execute(
        self,
        query: str | Composable,
        params: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        with self._connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def execute_no_result(
        self,
        query: str | Composable,
        params: tuple[Any, ...] = (),
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(query, params)

    def execute_many(
        self,
        query: str | Composable,
        params_seq: list[tuple[Any, ...]],
    ) -> None:
        if not params_seq:
            return

        with self._connection.cursor() as cursor:
            with self._connection.pipeline():
                cursor.executemany(query, params_seq)

    def execute_stream(
        self,
        query: str | Composable,
        params: tuple[Any, ...] = (),
        *,
        batch_size: int = 100,
    ) -> Iterator[tuple[Any, ...]]:
        with self._connection.cursor(name="stream_tx") as cursor:
            cursor.arraysize = batch_size
            cursor.execute(query, params)
            yield from cursor

    @contextmanager
    def transaction(self) -> Iterator[DbDriver]:
        """Already in a transaction -- yield self."""
        yield self

    @contextmanager
    def advisory_lock(self, key: str) -> Iterator[None]:
        """Advisory locks require a pool connection — not valid here."""
        _ = key
        msg = "advisory_lock must be called on the pooled driver, not a tx driver"
        raise RuntimeError(msg)
        yield  # unreachable — keeps the type checker happy about Iterator

    def close(self) -> None:
        """No-op — transaction driver does not own connection resources."""
        pass


USER_SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8()),
        pa.field("value", pa.int64()),
    ]
)

# Live ``max_segment_bytes`` cap the lifecycle service runs with under
# integration tests. ``docker/test.env`` overrides the compose.yml default
# (64 MiB) to 1 MiB so CHA-215 chunking-acceptance tests can breach the cap
# with ~2 MiB of data instead of 128 MiB. ``just integration-test`` sources
# ``docker/test.env`` into the pytest shell so this var is in scope.
MAX_SEGMENT_BYTES = int(
    os.environ.get("LIFECYCLE_DEFAULT_MAX_SEGMENT_BYTES", str(64 * 1024 * 1024))
)

# Proto ``IndexType.INDEX_TYPE_SCALAR_BTREE`` value, passed as the raw enum
# int so test modules import cleanly without the generated proto enums.
SCALAR_BTREE = 1


def make_client(*, catalog: str | None = None):
    """Return a PencaClient talking to the configured backend.

    ``catalog`` is the catalog this client operates against. It defaults
    within-catalog gRPC methods (when the caller doesn't pass
    ``catalog_uuid`` / ``catalog_name``) and pins the Flight SQL
    connection via the ``x-penca-catalog`` gRPC metadata header at
    handshake (CHA-253), Postgres-shaped: catalog is bound at connect
    time and immutable for the connection's lifetime. ``None`` leaves
    both unset; the SQL server then falls back to its
    ``SQL_SERVER_DEFAULT_CATALOG``.
    """
    return PencaClient.from_settings(
        ClientSettings(),  # ty: ignore[missing-argument]
        catalog=catalog,
    )


@lru_cache(maxsize=1)
def get_pg_driver() -> PsycopgDriver:
    """Open a direct Postgres driver for white-box state verification.

    Integration tests that need to assert on internal storage state
    (hot-tier row counts, segment metadata, commit_tx_log contents) use this
    helper to bypass the gRPC API. Requires ``PENCA_DB_*`` env vars,
    which ``just integration-test`` sources from ``docker/.baseline.env``.

    Cached so every caller in the test session shares one pool — avoids
    paying connect cost per test and prevents psycopg's pool ``__del__``
    warning from firing when short-lived pools are garbage-collected.
    """
    settings = DbSettings()  # ty: ignore[missing-argument]
    conninfo = (
        f"host={settings.host} port={settings.port} dbname={settings.dbname} "
        f"user={settings.user} password={settings.password}"
    )
    return PsycopgDriver(conninfo, min_size=1, max_size=4)


def make_lock_driver() -> PsycopgDriver:
    """Fresh single-connection driver for an out-of-band advisory-lock holder.

    Distinct from ``get_pg_driver`` — that one is a cached pool (max 4
    conns) shared across a test session. The lock-holder pattern needs
    its own dedicated connection so the pool's per-connection lifecycle
    matches the advisory lock's session scope.
    """
    settings = DbSettings()  # ty: ignore[missing-argument]
    conninfo = (
        f"host={settings.host} port={settings.port} dbname={settings.dbname} "
        f"user={settings.user} password={settings.password}"
    )
    return PsycopgDriver(conninfo, min_size=1, max_size=1)


def setup_schema(client):
    """Create a catalog + schema + table on main; return ``(schema_uuid, table_uuid, catalog_uuid, main_branch_uuid)``.

    CHA-236: namespace UUIDs are random server-side, so
    ``main_branch_uuid`` must come from the ``CreateCatalogResponse``
    (no client-side hash). Returned alongside the rest of the fixture
    state so callers don't have to recompute it.
    """
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"write_cat_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "write_schema",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="setup_schema",
    )
    table_uuid = client.create_table(
        "write_table",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="setup_schema",
    )
    return schema_uuid, table_uuid, catalog_uuid, main_branch_uuid


def create_table_on_branch(
    client,
    catalog_uuid: str,
    schema_uuid: str,
    branch_uuid: str,
    table_name: str = "write_table",
    arrow_schema: pa.Schema = USER_SCHEMA,
    primary_keys: list[str] | None = None,
) -> str:
    """Create a table on a specific branch. Returns table_uuid.

    Idempotent against ``CreateBranch``'s per-schema fork (CHA-184):
    when the new branch already inherited the table from its source,
    surface the existing ``table_uuid`` rather than re-raising the
    server's ``ALREADY_EXISTS`` from the per-branch UNIQUE check
    (CHA-236). Pre-CHA-236 callers relied on the same name + parent
    UUIDs hashing back to the same ``table_uuid``, which masked the
    redundancy; the random-UUID world surfaces it.
    """
    from penca_client.errors import AlreadyExistsError

    try:
        return client.create_table(
            table_name,
            arrow_schema,
            primary_keys=primary_keys or ["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            author="test",
            comment="create_table_on_branch",
        )
    except AlreadyExistsError:
        existing = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_name=table_name,
        )
        return existing.table_uuid


def setup_with_data(client):
    """Create catalog/schema/table, insert data on main, commit. Returns context dict."""
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=main_branch_uuid
    )

    batch = pa.table(
        {"name": ["alice", "bob"], "value": [10, 20]},
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
        branch_uuid=main_branch_uuid,
    )
    committed_tx = client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch_uuid,
    )

    return {
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
        "catalog_uuid": catalog_uuid,
        "main_branch_uuid": main_branch_uuid,
        "tx": committed_tx,
    }


def setup_with_data_named(client: PencaClient) -> dict:
    """Create catalog/schema/table, insert data, commit — returning names too.

    [`setup_with_data`] returns UUIDs only, but SQL needs 3-part
    identifiers, so the SQL-facing suites (Flight SQL, span-breakdown)
    share this named variant.

    Pins the client's Flight SQL connection to the freshly-created
    catalog (CHA-169 connection-scoped catalog) so subsequent
    ``execute_query`` / ``execute_update`` calls on the same client
    target it without cross-catalog rejection.
    """
    catalog_name = f"sql_cat_{uuid4().hex[:8]}"
    schema_name = "sql_schema"
    table_name = "sql_table"

    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
    schema_uuid = client.create_schema(
        schema_name, catalog_uuid=catalog_uuid, author="test", comment="create_schema"
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

    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    batch = pa.table(
        {"name": ["alice", "bob"], "value": [10, 20]},
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
        branch_uuid=main_branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch_uuid,
    )

    # CHA-169: pin the client's Flight SQL connection to this fresh
    # catalog so the test's `execute_query` / `execute_update` calls
    # don't get rejected as cross-catalog (the connection would
    # otherwise default to `SQL_SERVER_DEFAULT_CATALOG`).
    client.catalog = catalog_name

    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
        "main_branch_uuid": main_branch_uuid,
    }


def setup_partitioned_table(prefix: str) -> tuple[PencaClient, str, str, str, str]:
    """Catalog + schema + partitioned table on ``main``.

    Returns ``(client, catalog_uuid, schema_uuid, table_uuid,
    main_branch_uuid)``. ``prefix`` names the catalog/schema/table so a
    failure points back at the test that created them.
    """
    client = make_client()
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"{prefix}_cat_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        f"{prefix}_schema", catalog_uuid=catalog_uuid, author="test", comment=prefix
    )
    table_uuid = client.create_table(
        f"{prefix}_table",
        USER_SCHEMA,
        primary_keys=["name"],
        partition_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment=prefix,
    )
    return client, catalog_uuid, schema_uuid, table_uuid, main_branch_uuid


def write_and_persist(
    client,
    *,
    catalog_uuid,
    schema_uuid,
    table_uuid,
    branch_uuid,
    upserts=None,
    deletes=None,
) -> None:
    """mutate -> commit -> persist, stopping short of a snapshot.

    Split out of :func:`write_cycle` so a test can leave a branch with a
    persist tail its last snapshot does not cover.
    """
    mutation_kwargs: dict[str, Any] = {}
    if upserts is not None:
        mutation_kwargs["upserts"] = upserts

    if deletes is not None:
        mutation_kwargs["deletes"] = deletes

    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, **mutation_kwargs),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
    client.persist(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
    )


def write_cycle(
    client,
    *,
    catalog_uuid,
    schema_uuid,
    table_uuid,
    branch_uuid,
    upserts=None,
    deletes=None,
) -> str:
    """One full write cycle on a branch: mutate -> commit -> persist ->
    snapshot. Returns the resulting ``table_snapshot_uuid``.
    """
    write_and_persist(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
        upserts=upserts,
        deletes=deletes,
    )
    response = client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
    )
    assert response.HasField("snapshotted_at_micros")
    return table_snapshot_uuid(
        catalog_uuid, branch_uuid, table_uuid, response.snapshotted_at_micros
    )


# The fmt subscriber colourises even into docker's non-TTY log; the SGR
# escapes land mid-token (e.g. between `outcome` and `=`), breaking naive
# regexes over the scraped log, so every scrape strips them first.
ANSI_SGR_RE = re.compile(r"\x1b\[[0-9;]*m")


def poll_log_for(
    service: str, since: int, needle: str, deadline_s: float = 5.0
) -> bool:
    """Poll ``service``'s stdout (from byte offset ``since``) for ``needle``,
    tolerating the container's json-log flush lag after the RPC returns.

    Shared by the marker-scraping suites (direct point read, snapshot-list
    cache, user index seek) — the polling contract lives here so a tweak
    lands once. Absence assertions must NOT use an unpolled window; use the
    flush-barrier + exact-count pattern (see
    ``integration_direct_point_read_test.py``).

    Callers MUST be marked ``@pytest.mark.serial`` — see ``container_log``.
    """
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        if needle in container_log(service)[since:]:
            return True

        time.sleep(0.1)

    return needle in container_log(service)[since:]


def container_log(service: str) -> str:
    """Return ``${COMPOSE_PROJECT_NAME}-<service>-1`` **stdout**, ANSI-stripped.

    The container name follows the compose convention
    ``${COMPOSE_PROJECT_NAME}-<service>-1``; ``COMPOSE_PROJECT_NAME`` is
    exported by the Justfile (``penca-<worktree-basename>``) and
    inherited by the pytest process under ``just integration-test``.

    Returns stdout ONLY, because that is where the servicers' `tracing`
    subscriber writes: ``penca_observability::init_tracing`` builds
    ``tracing_subscriber::fmt()`` with no ``.with_writer(...)``, whose
    default writer is **stdout**. A byte-offset taken on stdout is a
    stable window boundary: it is append-only, so ``stdout[offset:]`` is
    exactly the events emitted after the snapshot. (Reading one stream,
    not ``stdout + stderr``, keeps the offset meaningful — interleaving
    two streams would let bytes insert before the offset and fold in an
    earlier test's event.)

    The offset window is only sound because every caller is marked
    ``@pytest.mark.serial`` and so runs outside the ``-n auto`` phase. The
    suite itself is no longer serial: a concurrent worker driving the same
    service interleaves its lines into this container's stdout, and they land
    *after* the offset, inside the window. So calling this obligates the
    marker — CHA-519 removes both, replacing the scrape with a structured
    per-request seam.

    ``tests/static/static_serial_marker_test.py`` is what keeps that true, by
    walking the call graph. It resolves callees by name within one module, so
    a plain call reaching here is covered; a call through an alias or a
    dynamic attribute, or a helper that shells out to ``docker logs`` itself,
    is not — add those to that test's root set by hand.
    """
    project = os.environ["COMPOSE_PROJECT_NAME"]
    container = f"{project}-{service}-1"
    completed = subprocess.run(
        ["docker", "logs", container],
        capture_output=True,
        text=True,
        check=True,
    )
    return ANSI_SGR_RE.sub("", completed.stdout)


def lookup_data_log_prefix_uuid(branch_uuid: str, table_uuid: str) -> str:
    """Compute the data-log prefix for a (table, branch) pair.

    CHA-177 + CHA-203: per-branch data tables are deterministic in
    ``(table_uuid, branch_uuid)``; the prefix is
    ``row_uuid_for_pk(table_uuid, [branch_uuid])`` — no metadata lookup
    needed. The helper exists so tests don't have to repeat the import
    + naming convention at every white-box assertion site.
    """
    return row_uuid_for_pk(table_uuid, [branch_uuid])


# The metadata-resolution amplification CHA-365 fixes is invisible at the
# gRPC wire (responses are byte-identical) — the only observable is how
# many ``__penca_system__.{tables,schemas}`` merge SELECTs a single RPC
# issues. ``pg_stat_statements`` (preloaded on the test postgres via
# docker/compose.yml) is the white-box seam: reset it, issue one RPC,
# then count statements referencing the system table's per-branch upsert
# log. The extension is enabled on the shared test postgres, so any
# integration test can use these.


def ensure_pg_stat_statements(driver: DbDriver) -> None:
    """Idempotently create the ``pg_stat_statements`` extension.

    The shared-preload-libraries entry in docker/compose.yml is what makes
    the extension *available*; ``CREATE EXTENSION`` is what makes its view
    queryable. Both are safe to repeat across tests.
    """
    driver.execute_no_result("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")


def reset_pg_stat(driver: DbDriver) -> None:
    """Zero the ``pg_stat_statements`` counters before a measured RPC."""
    driver.execute("SELECT pg_stat_statements_reset()")


def count_stmts_referencing(driver: DbDriver, needle: str) -> int:
    """Sum ``calls`` across ``pg_stat_statements`` rows whose normalized
    query text contains ``needle`` (a table name — pg_stat_statements
    normalizes literals/params but preserves identifiers).

    Uses ``strpos`` (exact substring) rather than ``LIKE`` so an underscore
    in ``needle`` (Penca data-log names are ``<prefix>_data_upsert_log``)
    is matched literally, not as a single-character wildcard — otherwise a
    less-unique needle from a future caller could silently over-match.
    """
    rows = driver.execute(
        "SELECT COALESCE(SUM(calls), 0) FROM pg_stat_statements WHERE strpos(query, %s) > 0",
        (needle,),
    )
    return int(rows[0][0])


def demo_catalog_names(client) -> set[str]:
    return {
        catalog.catalog_name
        for catalog in client.list_catalogs()
        if catalog.catalog_name.startswith("demo_")
    }


@contextmanager
def reaped_demo_catalogs(client):
    """Reap any ``demo_``-prefixed catalog an ``examples/`` script leaves behind.

    Every demo smoke test runs its script as a subprocess against a stack the
    rest of the suite shares, and each script creates its catalog before it
    prints anything — so a red run strands one unless something takes it out.
    Yields the pre-existing set so a caller can additionally assert the script
    cleaned up after itself.

    Best-effort by construction: a failure to list or delete is printed, never
    raised, because this runs on the unwind path where the exception already in
    flight is the one worth propagating.
    """
    before = demo_catalog_names(client)
    try:
        yield before
    finally:
        try:
            leaked = demo_catalog_names(client) - before
        except Exception as exc:  # noqa: BLE001 - must not mask a real failure
            print(f"(could not list catalogs to reap: {exc})")
            leaked = set()

        for catalog_name in leaked:
            try:
                client.delete_catalog(catalog_name=catalog_name)
            except Exception as exc:  # noqa: BLE001 - must not mask a real failure
                print(f"(could not delete catalog {catalog_name}: {exc})")
