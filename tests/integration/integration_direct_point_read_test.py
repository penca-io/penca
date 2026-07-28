"""CHA-476 red-tests — direct snapshot-only ``ids`` point-read arm (DataFusion bypass).

The 4th ``read_data`` dispatch arm serves a **default-current-time, snapshot-only**
(``is_all_cold`` with no persist band), ``ids`` point read with **no value filter**
WITHOUT DataFusion — emitting a ``direct_point_read=true`` debug marker distinct
from ``tier_shape``. The gate is purely query-shape; data residency is NOT a gate.

Fail-first: the marker does not exist today — a snapshot-only ``ids`` read is
served through the ``is_all_cold -> stream_all_cold`` DataFusion arm, so no
``direct_point_read=true`` event is ever emitted. Both tests below assert the
marker appears on the qualifying shape, which fails on current ``main``.

Scoped run::

    just integration-test --test-arg integration_direct_point_read_test
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from penca_client._time import micros_to_datetime

from .integration_helpers import (
    container_log,
    make_client,
    poll_log_for,
    setup_with_data,
)

# Serialized: asserts on process-global white-box state (container stdout log
# windows / pg_stat_statements counters) that a concurrent worker would
# pollute. Runs in the serial phase, not under -n auto.
# TODO(CHA-519): drop this mark once the structured per-request seam lands.
pytestmark = pytest.mark.serial

# ``ids`` carries only the table's primary-key column(s) (CHA-398); the server
# derives ``row_uuid`` from ``(table_uuid || pk_values)``. USER_SCHEMA's PK is
# ``name`` (see integration_helpers.setup_with_data, primary_keys=["name"]).
_PK_ONLY_SCHEMA = pa.schema([pa.field("name", pa.utf8())])

# The direct-arm marker the impl emits, rendered by the `tracing` fmt subscriber
# as an unquoted bool field (mirrors how ``tier_shape="snapshot_only"`` appears).
_DIRECT_MARKER = "direct_point_read=true"


def _snapshot_only(client):
    """Seed ``alice=10, bob=20`` then ``persist -> snapshot -> purge`` the user
    table so the read is snapshot-only (Pu <= W_snap, hot drained — the
    ``is_all_cold`` + no-persist-band shape the gate requires). Returns
    ``(kw, ctx)`` where ``kw`` is the catalog/schema/branch/table id kwargs."""
    ctx = setup_with_data(client)
    kw = {
        "catalog_uuid": ctx["catalog_uuid"],
        "schema_uuid": ctx["schema_uuid"],
        "branch_uuid": ctx["main_branch_uuid"],
        "table_uuid": ctx["table_uuid"],
    }
    # CHA-444: snapshot must precede purge so the baseline forms before Pu
    # advances (=> Pu <= W_snap => snapshot-only eligible).
    client.persist(**kw)
    client.snapshot(**kw)
    client.purge(**kw)
    return kw, ctx


class TestDirectPointReadArm:
    """The qualifying point read is served by the direct DataFusion-free arm."""

    def test_snapshot_only_ids_point_read_takes_direct_arm(self):
        """A default-current-time, snapshot-only, ``ids`` point read with no
        value filter returns exactly the probed row AND is served by the direct
        arm (``direct_point_read=true``).

        Fail-first: no ``direct_point_read=true`` event exists today (the read is
        served via ``stream_all_cold`` / DataFusion); the marker assertion fails.
        """
        client = make_client()
        kw, _ = _snapshot_only(client)
        ids = pa.table({"name": ["alice"]}, schema=_PK_ONLY_SCHEMA)

        since = len(container_log("query"))
        result = client.read_data(ids=ids, **kw)

        assert result.num_rows == 1, (
            f"ids=[alice] point read returns exactly the 1 probed row "
            f"(got {result.num_rows})"
        )
        assert result.column("name").to_pylist() == ["alice"]
        assert result.column("value").to_pylist() == [10]
        assert poll_log_for("query", since, _DIRECT_MARKER), (
            "a default-current-time snapshot-only ids point read with no value "
            "filter must be served by the direct DataFusion-free arm "
            f"({_DIRECT_MARKER}); not emitted today (served via stream_all_cold)"
        )


class TestDirectPointReadGate:
    """Each gate condition is purely query-shape: flipping exactly one keeps the
    read on ``stream_all_cold`` / ``stream_merged`` (no ``direct_point_read``)."""

    def test_gate_negative_controls(self):
        """The seek gate is `is_snapshot_only(plan) && exact_selection` — purely
        query-shape. A value filter or a full scan (both flip `exact_selection`)
        does NOT take the direct arm. CHA-501: the gate is axis-INDEPENDENT, so a
        time-travel `as_of` ids read of the snapshot-only base DOES take it (the
        planner picks the as_of-bounded snapshot, so the seek is exact) — it was
        a negative control before the widening.
        """
        client = make_client()
        kw, ctx = _snapshot_only(client)
        ids = pa.table({"name": ["alice"]}, schema=_PK_ONLY_SCHEMA)
        t_committed = ctx["tx"].commit_micros

        # The whole test's log window starts here (before any read), so the
        # count-based assertion below is scoped to this test's reads only.
        since = len(container_log("query"))

        # Positive: qualifying default-current-time shape → direct arm.
        client.read_data(ids=ids, **kw)
        assert poll_log_for("query", since, _DIRECT_MARKER), (
            "qualifying snapshot-only ids point read must take the direct arm"
        )

        # CHA-501: a time-travel `as_of` ids read of the snapshot-only base is
        # also exact, so it now takes the direct arm too (axis-independent gate).
        as_of_since = len(container_log("query"))
        historical = client.read_data(
            ids=ids, as_of=micros_to_datetime(t_committed), **kw
        )
        assert historical.num_rows == 1
        assert historical.column("value").to_pylist() == [10]
        assert poll_log_for("query", as_of_since, _DIRECT_MARKER), (
            "CHA-501: a snapshot-only time-travel as_of ids read must now take "
            "the direct arm — the axis restriction was dropped"
        )

        # Two controls that flip `exact_selection` — neither may take the arm.
        filtered = client.read_data(ids=ids, filter="value > 0", **kw)
        assert filtered.num_rows == 1  # filter.is_some() → not exact → stream_all_cold
        full = client.read_data(**kw)
        assert full.num_rows == 2  # row_uuids.is_none() (full scan) → not exact

        # Flush barrier: a final qualifying read whose marker we poll. The
        # container's json-log flush lags the RPC return, so per-read window
        # boundaries captured with unpolled offsets can misattribute a
        # not-yet-flushed marker to the wrong slice. Instead, once the barrier
        # marker has flushed the log is
        # append-only and every read above is fully present, so a COUNT over the
        # test's window is immune to boundary races: exactly the positive read,
        # the as_of read, and the barrier read may emit the marker — a
        # value-filtered or full-scan control taking the direct arm would push
        # the count past 3.
        barrier_since = len(container_log("query"))
        client.read_data(ids=ids, **kw)
        assert poll_log_for("query", barrier_since, _DIRECT_MARKER), (
            "flush-barrier qualifying read must take the direct arm"
        )
        assert container_log("query")[since:].count(_DIRECT_MARKER) == 3, (
            "exactly the positive read, the as_of read, and the flush-barrier "
            "read may emit direct_point_read=true; a value-filtered or full-scan "
            "control taking the direct arm would push the count past 3"
        )
