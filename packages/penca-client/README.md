# penca-client

Python client for Penca — an open-source lakebase for agent-first
applications.

This package ships:

- `penca_client.PencaClient` — pure-gRPC + Flight SQL client that
  talks to the three Penca microservices (query, write, lifecycle)
  plus the Flight SQL gateway.
- `penca_client.config` — `ClientSettings` (the four `PENCA_*_URL`
  channel URLs) for the client, plus `DbSettings` used by the
  top-level system test suite to open a direct Postgres connection
  for white-box assertions.
- `penca_client.{arrow, naming, errors, _time}` — small support
  modules backing the client (Arrow IPC helpers, deterministic UUID
  derivation that mirrors `crates/penca-core::naming`, typed errors,
  and microsecond-epoch conversion).
- `tests/unit/` — pure-Python tests of the client's own helpers (no
  infra required). System-level integration + performance tests of
  the Penca server live at the repo root under
  [`tests/integration/`](../../tests/integration/) and
  [`tests/performance/`](../../tests/performance/) — they use this
  client as the test driver but exercise the Rust server end-to-end.

See the [repo root README](../../README.md) for architecture, storage
tiers, data lifecycle, and deployment diagrams.
