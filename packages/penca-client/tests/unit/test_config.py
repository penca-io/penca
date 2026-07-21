"""Unit tests for ClientSettings env-var validation.

The Justfile leaves ``PENCA_SQL_URL`` empty (``localhost:`` with no
port) when the Flight SQL gateway is absent. ``_empty_port_is_unset``
turns those into ``None`` so the client can short-circuit Flight SQL
instead of opening an ADBC connection to an unbound port.
"""

from __future__ import annotations

import pytest
from penca_client.config import ClientSettings

_REQUIRED_BASE_ENV = {
    "PENCA_QUERY_URL": "localhost:50052",
    "PENCA_WRITE_URL": "localhost:50053",
    "PENCA_LIFECYCLE_URL": "localhost:50054",
}


def _set_env(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    for key, value in {**_REQUIRED_BASE_ENV, **overrides}.items():
        monkeypatch.setenv(key, value)


def test_explicit_flight_sql_url_passes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, PENCA_SQL_URL="localhost:50060")
    settings = ClientSettings()
    assert settings.flight_sql_url == "localhost:50060"


@pytest.mark.parametrize("blank", ["", "localhost:"])
def test_blank_or_unbound_port_becomes_none(
    monkeypatch: pytest.MonkeyPatch, blank: str
) -> None:
    _set_env(monkeypatch, PENCA_SQL_URL=blank)
    settings = ClientSettings()
    assert settings.flight_sql_url is None


def test_unset_env_var_defaults_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch)
    monkeypatch.delenv("PENCA_SQL_URL", raising=False)
    settings = ClientSettings()
    assert settings.flight_sql_url is None
