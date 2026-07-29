<p align="center">
  <img src="docs/penca-logo.svg" alt="Penca" width="200">
</p>

[![CI](https://github.com/penca-io/penca/actions/workflows/ci.yml/badge.svg)](https://github.com/penca-io/penca/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94+-orange)](rust-toolchain.toml)
[![Python](https://img.shields.io/badge/python-3.10+-blue)](pyproject.toml)

# Branchable OLTP + OLAP on one open columnar copy of your data

**Open-source and self-hostable on object storage — no second system, no ETL.**

## Introduction

Penca is a database you can fork like a git branch. A fork is a full read-write copy of
your data that copies no rows, and you can run transactions *and* analytical queries
against it. Fork production, let something loose on it, compare what it did, discard it.

Writes land in an internal Postgres hot tier under a real ACID transaction; a background
pipeline persists, snapshots and purges them out to columnar files on object storage
(Lance by default, Parquet supported). Reads merge both tiers on the fly, so a query
sees committed writes immediately whichever tier they sit in. That merge is what makes
the fork → transact → read-it-back loop *interactive* rather than a batch job. Object
storage is the only infrastructure you supply — Postgres is a component inside the Penca
stack, not a second database you operate.

The honest trade: Penca is not competitive with Postgres on transaction latency, and its
analytical side is young. What you get for it is one copy of your data instead of two
systems and a pipeline between them — [shortcomings](#current-shortcomings) has the edges.

## See it work

Three agents, three strategies, one live dataset. `examples/sandbox_demo.py` forks a
branch per agent off `main`, drives one shared deterministic visitor feed through all
three, lets each read back its own committed writes to steer its next move, then ranks
them, deletes every fork and shows `main` untouched.

```
branch      impressions   conversions   rate
greedy             3000           417   13.90%
epsilon            3000           383   12.77%
even               3000           307   10.23%

one copy: main holds its rows in 1 cold object, each branch owns 0; main unchanged
```

`greedy` and `epsilon` beat `even` because they steer on what they just wrote; `even`
splits on the visitor index and never reads. That read-your-writes loop on a branch is
the pitch, and the forks copied no rows. The policies are toy; the mechanic is the point.

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
loopback, so any Flight SQL driver on this machine can point at it. Data is ephemeral
unless you ask for a directory, which survives `just penca-down`:

```bash
just penca-up --db ~/.penca/data
```

## How this differs

The defensible claim is the *conjunction*, on one copy. Each alternative has a leg of it:

- **Neon** — branchable Postgres. Branch plus OLTP, but no OLAP on the branch.
- **Dolt** — branching, merge and audit on a row store in a closed format. Analytical
  queries pay row-store costs, and other tools cannot read the bytes.
- **Iceberg / Nessie** — branching over open columnar files, but no interactive
  read-your-writes: you commit table snapshots, you do not transact.
- **Databricks Lakebase** — managed, with OLTP in a Postgres store that syncs to the
  lakehouse. Penca is self-hostable and has no sync step: there is only one copy.

## Architecture

Penca runs as three gRPC services behind two entry points. SQL clients connect to a
Flight SQL gateway that translates SQL into those calls; programmatic clients call the
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

A fourth process, the lifecycle scheduler, drives `persist → snapshot → purge` on a tick
so the pipeline advances with no operator. Read planning is in-process inside the query
service, not a service hop. Branching is a metadata operation: a fork records its position
in the parent and reads the parent's cold files through it, which is why no rows are
copied. Detail in [docs/architecture.md](docs/architecture.md), algorithms and
crash-safety invariants in [docs/algorithms.md](docs/algorithms.md).

## Features

- [x] Fork a branch off `main` with no row copy
- [x] Read-your-writes on a branch, over SQL or gRPC
- [x] Branch merge, and branch delete/discard
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

A limitation you find yourself after a rosy README costs more than one you read up front.

- **No authentication or authorization, at all.** No auth interceptor, no TLS on any
  service, and the Flight SQL handshake is unimplemented. Anything that reaches the ports
  has full access — hence the loopback bind. Do not expose Penca to a network you do not
  control.
- **Branches share compute.** Only storage is isolated; every branch runs on the same
  stack's CPU, so concurrent multi-branch load contends. A deployment boundary rather
  than a branching one, but know it before you benchmark it.
- **Single-level branching.** You can fork `main`, not a fork — the attempt is rejected
  rather than silently returning incomplete data.
- **Retention is configured but never prunes.** Reads below the floor are refused so
  nothing serves partial history, but nothing reclaims it either, and the window is
  immutable once set.
- **The lifecycle scheduler is v0** — single replica, no leader election.
- **OLTP is passable, not competitive.** The fixed per-statement pipeline dominates
  point operations: ~15 ms of SQL-layer overhead over the equivalent gRPC seek, ~40 ms
  for a single-statement read-modify-write. The TPC-B numbers track that gap rather than
  claim parity.
- **OLAP is under-optimized.** Effort so far went into derisking transactions on a data
  lake; at small scale Postgres still wins the analytical query — a crossover, not a
  wall. See [docs/performance.md](docs/performance.md).

## Roadmap

Branching gets deeper: forking a fork, at arbitrary depth. Retention gets its other half
— policies you can update, and pruning that actually reclaims. Isolation gets stronger,
with true snapshot isolation and configurable levels per catalog. On the way in and out:
bulk load that bypasses the hot tier, so you can ingest existing data-lake files at full
speed, and Iceberg interop both directions — export committed snapshots, or adopt an
existing table in place with no migration.

On performance: a structured predicate on the read wire to kill the SQL-string double
parse, aggregate / limit / TopN pushed into the scan, a broad band of low-hanging fruit
on the Flight SQL path, and metadata itself moving to object storage. Further out:
full-text and vector indexes, a pgwire gateway so Postgres clients connect unmodified,
and the authentication story the shortcomings section is missing.

## Documentation

- [docs/usage.md](docs/usage.md) — connecting, a first table over SQL and gRPC, the demos, DataGrip
- [docs/architecture.md](docs/architecture.md) — services, storage tiers, concepts
- [docs/development.md](docs/development.md) — build, run, test, profile, configure
- [docs/algorithms.md](docs/algorithms.md) — write path, read path, branch merge
- [docs/performance.md](docs/performance.md) — benchmarks across hot, cold and mixed
- [docs/decisions/](docs/decisions/) — accepted ADRs · [CONTRIBUTING.md](CONTRIBUTING.md) · [SECURITY.md](SECURITY.md)

## License

Apache-2.0. See [LICENSE](LICENSE).
