<p align="center">
  <img src="docs/penca-logo.svg" alt="Penca" width="200">
</p>

[![CI](https://github.com/penca-io/penca/actions/workflows/ci.yml/badge.svg)](https://github.com/penca-io/penca/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94+-orange)](rust-toolchain.toml)
[![Python](https://img.shields.io/badge/python-3.10+-blue)](pyproject.toml)

# Branchable and versioned OLTP + OLAP on one open columnar copy of your data

**Open-source and self-hostable on object storage — no second system, no CDC, no ETL.**

## Introduction

Penca is a database you can fork like a git branch. A fork is a full read-write copy of your
data that copies no rows, and you can run transactions *and* analytical queries against it.
Fork production, let something loose on it, compare what it did, discard it.

Writes land in an internal Postgres hot tier under a real ACID transaction; a background
pipeline persists, snapshots and purges them out to columnar files on object storage (Lance
by default, Parquet supported). Reads merge both tiers on the fly, so a query sees committed
writes immediately whichever tier they sit in — that merge is what makes the fork →
transact → read-it-back loop *interactive* rather than a batch job.

Object storage is pluggable, and the **only permanent home** for your table data: the hot
tier buffers writes and purge reclaims rows once they are cold, leaving one set of open
columnar files that anything reading Lance or Parquet can open. Catalog and branch metadata
is served from Postgres and stays that way; checkpointing it to object storage so a cluster
can reload it on startup is on the [roadmap](#roadmap).

The honest trade: Penca is not competitive with bare-metal Postgres on transaction latency,
and its analytical side is young. What you get is one copy of your data instead of two
systems and a pipeline between them — [shortcomings](#current-shortcomings) has the edges.

## See it work

Three agents, three strategies, one live dataset. `examples/sandbox_demo.py` forks a branch
per agent off `main`, drives one shared deterministic visitor feed through all three, lets
each read back its own committed writes to steer its next move, then ranks them, deletes
every fork and shows `main` untouched. Its scoreboard:

| branch    | impressions | conversions | rate   |
|:----------|------------:|------------:|:-------|
| `greedy`  |        3000 |         417 | 13.90% |
| `epsilon` |        3000 |         383 | 12.77% |
| `even`    |        3000 |         307 | 10.23% |

`greedy` and `epsilon` beat `even` because they steer on what they just wrote; `even` splits
on the visitor index and never reads — that read-your-writes loop on a branch is the pitch.
The policies are toy; the mechanic is the point. Figures are one seeded run on 2026-07-27:
reproducible, not pinned. The forks copy no rows, which the demo asserts rather than prints
— see [`integration_sandbox_demo_test.py`](tests/integration/integration_sandbox_demo_test.py).

## Quick start

```bash
just penca-up                            # Postgres + object store + servicers + Flight SQL gateway
set -a && source docker/.client.env      # PENCA_*_URL for the client
uv run python examples/sandbox_demo.py
```

You need [Docker](https://docs.docker.com/engine/install/),
[`uv`](https://docs.astral.sh/uv/) and [`just`](https://github.com/casey/just). The first
`just penca-up` compiles the server image from source, which takes a while; a prebuilt
image is on the way. Ports are fixed (Postgres 5432, Flight SQL 50060) and bound to
loopback. Data is ephemeral unless you ask for a directory, which survives
`just penca-down`:

```bash
just penca-up --db ~/.penca/data
```

## How this differs

The defensible claim is the *conjunction*, on one copy. Each alternative has a leg of it:

- **Neon** — branchable Postgres. Branch plus OLTP, but no columnar analytics on the
  branch: queries run on the row store, at row-store cost.
- **Dolt** — branching, merge and audit, open source, but on a bespoke row-oriented
  format. Analytical queries pay row-store costs, and lakehouse tools cannot read it.
- **Iceberg / Nessie** — branching over open columnar files, but no interactive
  read-your-writes: you commit table snapshots, you do not transact.
- **Databricks Lakebase** — the closest peer. Databricks reached the same *diagnosis*
  independently, that the way out of the OLTP/OLAP split is keeping data once in open
  formats, and we read that as validation more than competition. Their lakehouse copy still
  arrives by managed sync, though, and their intermediate row versions serve MVCC and
  point-in-time recovery: invisible to lakehouse readers, collected in time. Penca's are a
  queryable row-level audit trail with `as_of` reads. Branching is metadata-only in both.

## Architecture

Penca runs as three gRPC services behind two entry points: SQL clients connect to a
Flight SQL gateway that translates SQL into those calls, programmatic clients call the
services directly.

```
   SQL clients (JDBC / ODBC / ADBC)          programmatic clients
                 │                                    │
            Flight SQL                        gRPC (3 channels)
                 ▼                                    ▼
    ┌────────────────────────┐          ┌────────────────────────────┐
    │ penca-sql-server :50060│─────────▶│ query · write · lifecycle  │
    │ (DataFusion)           │   gRPC   │ :50052   :50053   :50054   │
    └────────────────────────┘          └─────────────┬──────────────┘
                                                      ▼
                       Postgres (hot tier)  +  object storage (cold tier)
```

A fifth process, the lifecycle scheduler, drives `persist → snapshot → purge` on a tick
so the pipeline advances with no operator. Read planning is in-process in the query
service, not a service hop.

A fork copies no rows: it records its position in the parent and reads the parent's cold
files through it. Not free, though — creating a branch first flushes the parent's
unpersisted writes to cold, so fork latency tracks what the parent has buffered, not what it
holds. Detail in [docs/architecture.md](docs/architecture.md); algorithms and crash-safety
invariants in [docs/algorithms.md](docs/algorithms.md).

## Features

- [x] Fork, merge and discard a branch — gRPC only, no SQL branch DDL exists
- [x] Fork copies no rows off `main`
- [x] Read-your-writes on a branch, over SQL or gRPC
- [x] Time travel — read any table as of an earlier commit
- [x] Audit trail — full version history per row, including tombstones
- [x] ACID transactions spanning every schema in a catalog
- [x] SQL over Arrow Flight SQL (JDBC / ODBC / ADBC)
- [x] gRPC API for catalog, schema, table, branch, transaction and data operations
- [x] Primary-key point lookups and secondary-index seeks pushed into the scan
- [x] Lance and Parquet cold-tier formats on any S3-compatible store
- [x] Autonomous persist / snapshot / purge scheduler
- [ ] Branch `diff` and `revert`
- [ ] Forking off a fork
- [ ] Retention pruning
- [ ] Authentication and authorization
- [ ] Highly-available lifecycle scheduler
- [ ] Per-branch compute isolation

## Current shortcomings

- **No authentication or authorization, at all.** No auth interceptor, no TLS, and the Flight
  SQL handshake is unimplemented. Anything reaching the ports has full access — hence the
  loopback bind. Do not expose Penca to a network you do not control.
- **Branching is narrower than git.** You can fork `main`, not a fork, and merging back is
  fast-forward only — if the target took a commit past your fork point the merge is refused
  rather than reconciled. No conflict resolution, no `diff`, no `revert`.
- **No configurable isolation level.** What you get is fixed per operation — a snapshot at
  `BEGIN` for reads in a transaction, last-writer-wins for upserts, READ COMMITTED for
  `UPDATE`/`DELETE` — with no setting to choose, per catalog or at all.
- **OLTP is passable, not competitive.** The fixed per-statement pipeline dominates point
  operations: ~15 ms of SQL-layer overhead over the equivalent gRPC seek, ~40 ms for a
  single-statement read-modify-write. TPC-B tracks the gap, not parity.
- **OLAP is under-optimized.** Effort went into derisking transactions on a data lake first;
  at small scale Postgres still wins the analytical query — a crossover, not a wall
  ([docs/performance.md](docs/performance.md)).
- **Branches share compute.** Only storage is isolated; every branch runs on the same stack's
  CPU, so concurrent multi-branch load contends. Know it before you benchmark it.
- **No Iceberg export.** The cold tier is open Lance or Parquet and any engine can read the
  files, but nothing publishes them as an Iceberg table.
- **Arrow Flight SQL is the only SQL wire** — no pgwire gateway, so Postgres clients and
  drivers cannot connect unmodified.
- **No full-text search and no vector indexes.** Secondary indexes are equality seeks only.

## Roadmap

Everything above is the roadmap, in roughly that order — the shortcomings section is a plan
stated plainly rather than a list of regrets. Beyond it: bulk load that bypasses the hot
tier, so you can ingest existing data-lake files at full speed, and adopting an Iceberg
table in place with no migration. Retention gains the pruning half it is missing and the
scheduler gains leader election. A structured predicate on the read wire kills the
SQL-string double parse, with aggregate / limit / TopN pushed into the scan. Catalog
metadata gets checkpointed to object storage and reloaded into Postgres at startup — still
served from Postgres in steady state, but recoverable from the object store alone.

## Documentation

- [docs/usage.md](docs/usage.md) — connecting, a first table over SQL and gRPC, the demos, DataGrip
- [docs/architecture.md](docs/architecture.md) — services, storage tiers, concepts · [docs/development.md](docs/development.md) — build, run, test
- [docs/algorithms.md](docs/algorithms.md) — write path, read path, branch merge · [docs/performance.md](docs/performance.md) — benchmarks
- [docs/schema-reference.md](docs/schema-reference.md) — system tables · [docs/decisions/](docs/decisions/) — ADRs · [CONTRIBUTING.md](CONTRIBUTING.md) · [SECURITY.md](SECURITY.md)

## License

Apache-2.0. See [LICENSE](LICENSE).
