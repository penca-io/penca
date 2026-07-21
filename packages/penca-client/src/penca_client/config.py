"""Client-side configuration for Penca.

``ClientSettings`` holds the gRPC channel URLs for the 4 Penca
microservices plus an optional Flight SQL URL. The client opens a
channel per service and instantiates one stub per service — there is
no embedded execution path. The 4 gRPC URLs are required; Flight SQL
is only populated when the Rust backend is running.

``DbSettings`` is kept here so benchmarks and debug scripts that want to
talk to the same Postgres the servers use can read the standard
``PENCA_DB_*`` env vars. It is not used by the client.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class DbSettings(BaseSettings):
    """Postgres connection settings (used by perf baselines, not the client)."""

    host: str = Field(validation_alias="PENCA_DB_HOST")
    port: int = Field(validation_alias="PENCA_DB_PORT")
    dbname: str = Field(validation_alias="PENCA_DB_DBNAME")
    user: str = Field(validation_alias="PENCA_DB_USER")
    password: str = Field(validation_alias="PENCA_DB_PASSWORD")

    @property
    def conninfo(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )


class ClientSettings(BaseSettings):
    """gRPC channel URLs for the 4 Penca microservices + optional Flight SQL."""

    query_url: str = Field(validation_alias="PENCA_QUERY_URL")
    write_url: str = Field(validation_alias="PENCA_WRITE_URL")
    lifecycle_url: str = Field(validation_alias="PENCA_LIFECYCLE_URL")
    flight_sql_url: str | None = Field(default=None, validation_alias="PENCA_SQL_URL")

    @field_validator("flight_sql_url", mode="before")
    @classmethod
    def _empty_port_is_unset(cls, v: object) -> object:
        if isinstance(v, str) and (v == "" or v.endswith(":")):
            return None

        return v
