"""Integration tests for ``PsycopgDriver.advisory_lock``.

CHA-141 regression coverage: if the unlock statement fails (or the body
raises in the narrow window before the unlock runs), the driver must
close the connection before returning it to the pool so the
session-scoped advisory lock dies with the backend session instead of
riding back to the next pool consumer.

Run via ``just integration-test``.
"""

from __future__ import annotations

import threading
from uuid import uuid4

import pytest
from penca_client.config import DbSettings
from psycopg import Cursor

from .integration_helpers import PsycopgDriver


def _make_driver() -> PsycopgDriver:
    """Build a fresh single-connection driver — each call = a distinct pool."""
    settings = DbSettings()  # ty: ignore[missing-argument]
    conninfo = (
        f"host={settings.host} port={settings.port} dbname={settings.dbname} "
        f"user={settings.user} password={settings.password}"
    )
    return PsycopgDriver(conninfo, min_size=1, max_size=1)


class TestAdvisoryLock:
    def test_releases_lock_on_body_exception(self):
        """Ordinary Exception inside the body must still release the lock.

        Guards the ``unlock_done`` / inner-finally path: if someone refactors
        the guard and accidentally skips the unlock on the error path, a
        separate session would hang here.
        """
        driver_a = _make_driver()
        driver_b = _make_driver()
        key = f"cha-141-body-exc-{uuid4().hex}"

        class _TestError(Exception):
            pass

        with pytest.raises(_TestError):
            with driver_a.advisory_lock(key):
                raise _TestError()

        acquired = threading.Event()

        def acquire_in_b():
            with driver_b.advisory_lock(key):
                acquired.set()

        thread = threading.Thread(target=acquire_in_b, daemon=True)
        thread.start()
        assert acquired.wait(timeout=5.0), (
            "driver_b.advisory_lock hung — body-exception path did not release lock"
        )
        thread.join(timeout=1.0)

        driver_a.close()
        driver_b.close()

    def test_expels_connection_on_unlock_failure(self, monkeypatch):
        """If the unlock SQL raises with a healthy conn, the fix must close it.

        This is the CHA-141 scenario: the unlock is skipped (or raises) while
        the connection itself is still usable from psycopg_pool's POV. Without
        the fix, the pool would reclaim the connection with the session-scoped
        advisory lock still held; a fresh session would then block on
        ``pg_advisory_lock`` for the same key indefinitely.
        """
        driver_a = _make_driver()
        driver_b = _make_driver()
        key = f"cha-141-unlock-fail-{uuid4().hex}"

        original_execute = Cursor.execute

        def patched_execute(self, query, params=(), *args, **kwargs):
            # Match the unlock SQL only — keep everything else working so the
            # conn remains healthy from psycopg's POV (i.e. not a broken conn
            # that the pool would discard anyway).
            if "pg_advisory_unlock" in str(query):
                msg = "simulated unlock failure (CHA-141 test)"
                raise RuntimeError(msg)

            return original_execute(self, query, params, *args, **kwargs)

        monkeypatch.setattr(Cursor, "execute", patched_execute)

        with pytest.raises(RuntimeError, match="simulated unlock failure"):
            with driver_a.advisory_lock(key):
                pass

        # Restore real execute before driver_b tries to acquire.
        monkeypatch.setattr(Cursor, "execute", original_execute)

        acquired = threading.Event()

        def acquire_in_b():
            with driver_b.advisory_lock(key):
                acquired.set()

        thread = threading.Thread(target=acquire_in_b, daemon=True)
        thread.start()
        assert acquired.wait(timeout=5.0), (
            "driver_b.advisory_lock hung — CHA-141 regression "
            "(unlock-failure path did not close the contaminated conn)"
        )
        thread.join(timeout=1.0)

        driver_a.close()
        driver_b.close()
