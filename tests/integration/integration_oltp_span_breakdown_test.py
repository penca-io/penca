"""CHA-417 acceptance: the stream/IPC-encode bucket emits timing spans.

docs/performance.md attributes ~60-70 ms of the OLTP point read to a
"streaming + IPC encode" bucket that is invisible to the span table —
the encode tails (`penca-server-grpc::ipc` for gRPC `read_data`,
`record_batch_response` for every Flight SQL DoGet arm) carry no spans.
These tests pin the two new spans:

- ``ipc_encode`` — wraps the gRPC ``ipc_response_stream`` encode loop
  (query container).
- ``flight_encode`` — wraps the ``record_batch_response`` encoded
  stream, the shared helper every ``do_get_fallback`` arm (statement,
  prepared, cache-miss re-plan) returns through, so one span covers
  both the JDBC ``CommandStatementQuery`` and ADBC
  ``CommandPreparedStatementQuery`` paths (penca-sql-server
  container).

Measurement seam: ``PENCA_SPAN_TIMING=1`` (docker/test.env) makes the
fmt subscriber emit one ``close time.busy=..`` line per enabled span;
``RUST_LOG=info,penca=debug`` (docker/compose.yml) enables the
debug-level spans. The tests scrape the container logs the same way the
CHA-367 planning-resolution-count tests do, windowed by a byte offset
captured before the workload runs.

Each test carries a sanity guard: some pre-existing span CLOSE line
must appear in the same window, so a misconfigured ``PENCA_SPAN_TIMING``
fails loudly as a harness error instead of masquerading as the red
assertion.
"""

from __future__ import annotations

import re
import time

from .integration_helpers import (
    container_log,
    make_client,
    setup_with_data,
    setup_with_data_named,
)

# FmtSpan::CLOSE renders the closing span's timing on a `close
# time.busy=..` event line; requiring the span name on the same line is
# enough to attribute the close (the names below are unique across the
# codebase). A bare `close time.busy` match is the sanity guard that the
# span-timing seam works at all.
_ANY_CLOSE_RE = re.compile(r"close time\.busy")


def _poll_for_span_close(
    service: str, since: int, span_name: str, deadline_seconds: float = 5.0
) -> tuple[int, int]:
    """Poll the ``service`` log window for ``span_name`` CLOSE lines.

    Returns ``(span_close_count, any_close_count)`` once the named span
    surfaces or the deadline lapses — the container's stdout may not be
    flushed to docker's json-log driver immediately after the RPC
    returns, hence the poll.
    """
    span_close_re = re.compile(rf"\b{re.escape(span_name)}\b.*close time\.busy")
    deadline = time.monotonic() + deadline_seconds
    span_closes = 0
    any_closes = 0
    while time.monotonic() < deadline:
        window = container_log(service)[since:]
        span_closes = sum(
            1 for line in window.splitlines() if span_close_re.search(line)
        )
        any_closes = sum(
            1 for line in window.splitlines() if _ANY_CLOSE_RE.search(line)
        )
        if span_closes >= 1:
            break

        time.sleep(0.2)

    return span_closes, any_closes


def test_grpc_read_emits_ipc_encode_span():
    """A gRPC ``read_data`` point read must close one ``ipc_encode`` span.

    RED today: the span does not exist (``rg ipc_encode crates/`` is
    empty), so the count is 0 while the sanity guard sees other CLOSE
    lines in the same window. GREEN once ``ipc_response_stream``
    (crates/penca-server-grpc/src/ipc.rs) is wrapped in the
    stream-level ``ipc_encode`` debug span.
    """
    client = make_client()
    try:
        ctx = setup_with_data(client)
        since = len(container_log("query"))
        table = client.read_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            table_uuid=ctx["table_uuid"],
            filter="name = 'alice'",
        )
        assert table.num_rows == 1, table
    finally:
        client.close()

    span_closes, any_closes = _poll_for_span_close("query", since, "ipc_encode")

    assert any_closes >= 1, (
        "no span CLOSE lines at all in the query-log window — either "
        "PENCA_SPAN_TIMING is unset on the query container "
        "(docker/test.env) or no debug-level spans are enabled on this "
        "path (RUST_LOG); harness/coverage issue, not a red result."
    )
    assert span_closes >= 1, (
        f"expected >= 1 `ipc_encode` span CLOSE on the query container after a "
        f"gRPC read_data point read; got {span_closes} "
        f"(window had {any_closes} other CLOSE lines, so span timing works — "
        "the encode bucket is uninstrumented)."
    )


def test_flight_sql_select_emits_flight_encode_span():
    """A Flight SQL SELECT (ADBC) must close one ``flight_encode`` span.

    The ADBC DB-API layer prepares unconditionally, so this exercises
    the ``CommandPreparedStatementQuery`` DoGet arm; the span sits on
    ``record_batch_response``, the encode helper shared by every arm,
    so the JDBC ``CommandStatementQuery`` arm is covered by the same
    wrap (driver parity by construction, per the helper's doc comment).

    RED today: the span does not exist, count is 0. GREEN once
    ``record_batch_response``
    (crates/penca-sql-server/src/flight_sql/service.rs) wraps its
    returned stream in the ``flight_encode`` debug span.
    """
    setup_client = make_client()
    try:
        ctx = setup_with_data_named(setup_client)
    finally:
        setup_client.close()

    fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
    since = len(container_log("penca-sql-server"))

    sql_client = make_client(catalog=ctx["catalog_name"])
    try:
        table = sql_client.execute_query(
            f"SELECT name, value FROM {fqn} WHERE name = 'alice'"
        )
        assert table.num_rows == 1, table
    finally:
        sql_client.close()

    span_closes, any_closes = _poll_for_span_close(
        "penca-sql-server", since, "flight_encode"
    )

    assert any_closes >= 1, (
        "no span CLOSE lines at all in the sql-server log window — either "
        "PENCA_SPAN_TIMING is unset on the penca-sql-server container "
        "(docker/test.env) or no debug-level spans are enabled on this "
        "path (RUST_LOG); harness/coverage issue, not a red result."
    )
    assert span_closes >= 1, (
        f"expected >= 1 `flight_encode` span CLOSE on the penca-sql-server "
        f"container after an ADBC SELECT; got {span_closes} "
        f"(window had {any_closes} other CLOSE lines, so span timing works — "
        "the DoGet encode tail is uninstrumented)."
    )
