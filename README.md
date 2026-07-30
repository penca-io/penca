<p align="center">
  <img src="docs/penca-logo.svg" alt="Penca" width="200">
</p>

[![CI](https://github.com/penca-io/penca/actions/workflows/ci.yml/badge.svg)](https://github.com/penca-io/penca/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94+-orange)](rust-toolchain.toml)
[![Python](https://img.shields.io/badge/python-3.10+-blue)](pyproject.toml)

# Branchable and versioned OLTP + OLAP on one open columnar copy of your data

**Open-source and self-hostable on object storage. No second system, no CDC, no ETL.**

## Introduction

> [!WARNING]
> **Penca is very early, closer to a proof of concept than a product.** Expect bugs and
> rough edges, and read [Current shortcomings](#current-shortcomings) before you plan
> anything around it: much of the [roadmap](#roadmap) is still ahead of us.

Penca is a database that serves transactional and analytical queries from one copy of your
data, stored as open columnar files on object storage. Unlike a split OLTP/OLAP stack there
is no second system and no CDC pipeline between them: the files an analytical query reads
are the files production wrote. The trade-off is that a single-row transaction costs more
than it would on a local-disk row store.

To keep transactional latency workable, writes land in an internal Postgres hot tier under a
real ACID transaction. A background pipeline persists, snapshots and purges them out to
columnar files (Lance by default, Parquet supported). Reads merge both tiers, so a query
sees committed writes immediately whichever tier they sit in.

Object storage is pluggable and the only permanent home for your table data. Catalog and
branch metadata is served from Postgres and stays that way, with object-storage checkpoints
for recovery on the [roadmap](#roadmap).

A branch is a full read-write copy that copies no rows, so forking production is cheap
enough to do per experiment. Every mutation appends to an immutable log carrying an author
and a timestamp, so any state is auditable and readable as of an earlier commit.

See [docs/architecture.md](docs/architecture.md) for the full design, and
[Current shortcomings](#current-shortcomings) for where the edges are.

## See it work

Three agents, three strategies, one live dataset. `examples/sandbox_demo.py` forks a branch
per agent off `main`, drives one shared deterministic visitor feed through all three, lets
each steer on its own committed writes, then ranks them, deletes every fork and shows `main`
untouched. Its scoreboard:

| branch    | impressions | conversions | rate   |
|:----------|------------:|------------:|:-------|
| `greedy`  |        3000 |         417 | 13.90% |
| `epsilon` |        3000 |         383 | 12.77% |
| `even`    |        3000 |         307 | 10.23% |

`greedy` and `epsilon` beat `even` because they steer on what they just wrote; `even` splits
on the visitor index and never reads. The policies are toy; the mechanic is the point.
Figures are one seeded run on 2026-07-27, reproducible but not pinned. The forks copy no rows,
which the demo asserts rather than prints: see
[`integration_sandbox_demo_test.py`](tests/integration/integration_sandbox_demo_test.py).

## Quick start

```bash
just penca-up                            # Postgres + object store + servicers + Flight SQL gateway
set -a && source docker/.client.env      # PENCA_*_URL for the client
uv run python examples/sandbox_demo.py
```

You need [Docker](https://docs.docker.com/engine/install/), [`uv`](https://docs.astral.sh/uv/)
and [`just`](https://github.com/casey/just). The first `just penca-up` compiles the server
image from source, which takes a while; a prebuilt image is on the way. Ports are fixed
(Postgres 5432, Flight SQL 50060) and bound to loopback. Data is ephemeral unless you ask
for a directory, which survives `just penca-down`:

```bash
just penca-up --db ~/.penca/data
```

## How this differs

The defensible claim is the *conjunction*, on one copy. Each alternative holds part of it:

- **Neon.** Branchable Postgres. Branch plus OLTP, but no columnar analytics on the
  branch: queries run on the row store, at row-store cost.
- **Dolt.** Branching, merge and audit, open source, but on a bespoke row-oriented
  format. Analytical queries pay row-store costs, and lakehouse tools cannot read it.
- **Iceberg / Nessie.** Branching over open columnar files, but no interactive
  read-your-writes: you commit table snapshots, you do not transact.
- **Databricks LTAP.** The closest peer by far.
  [LTAP](https://www.databricks.com/blog/lakebase-ltap-rethinking-database-storage), built
  on Lakebase and announced in June 2026, argues the same thing we do: the way out of the
  split is storing the data once in open formats instead of syncing a second copy to read
  it. We could not have asked for better validation. Where the two diverge is data
  versioning. LTAP keeps intermediate row versions to preserve Postgres MVCC and
  point-in-time recovery, but they stay invisible to Iceberg and Delta readers and are
  garbage-collected in time. Branching there is metadata-only, as it is here. Penca is
  Apache-2.0 and self-hostable rather than a managed service, and it makes that version
  history a first-class queryable surface: a row-level audit trail and `as_of` reads today,
  revert to come.

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
files through it. Not free, though: creating a branch first flushes the parent's
unpersisted writes to cold, so fork latency tracks what the parent has buffered, not what it
holds. Detail in [docs/architecture.md](docs/architecture.md); algorithms and crash-safety
invariants in [docs/algorithms.md](docs/algorithms.md).

## Features

- [x] Fork, merge and discard a branch (gRPC only, no SQL branch DDL exists)
- [x] Fork copies no rows off `main`; read-your-writes on the branch, over SQL or gRPC
- [x] Time travel: read any table as of an earlier commit
- [x] Audit trail: full version history per row, including tombstones
- [x] ACID transactions spanning every schema in a catalog
- [x] SQL over Arrow Flight SQL (JDBC / ODBC / ADBC), and a full gRPC API
- [x] Primary-key point lookups and secondary-index seeks pushed into the scan
- [x] Lance and Parquet on any S3-compatible store, with an autonomous lifecycle scheduler
- [ ] Branch `diff` and `revert`, and forking off a fork
- [ ] Retention pruning
- [ ] Authentication and authorization

## Current shortcomings

- **No authentication or authorization, at all.** No auth interceptor, no TLS, and the Flight
  SQL handshake is unimplemented. Anything reaching the ports has full access, hence the
  loopback bind. Do not expose Penca to a network you do not control.
- **Branching is narrower than git.** You can fork `main`, not a fork, and merging back is
  fast-forward only: if the target took a commit past your fork point the merge is refused
  rather than reconciled. No conflict resolution, no `diff`, no `revert`.
- **No configurable isolation level.** What you get is fixed per operation: a snapshot at
  `BEGIN` for reads in a transaction, last-writer-wins for upserts, READ COMMITTED for
  `UPDATE`/`DELETE`. There is no setting to choose, per catalog or at all.
- **OLTP is passable, not competitive.** A point read or write is dominated by the fixed
  per-statement pipeline rather than by the storage underneath it, and the SQL path pays
  more of that than the gRPC one. Good enough to build on, not a swap for bare-metal Postgres on
  single-client transactional work. Measure it yourself with `examples/oltp_demo.py`.
- **OLAP is under-optimized.** Effort went into derisking transactions on a data lake first;
  at small scale Postgres still wins the analytical query, a crossover rather than a wall.
- **No Iceberg export.** The cold tier is open Lance or Parquet and any engine can read the
  files, but nothing publishes them as an Iceberg table.
- **Arrow Flight SQL is the only SQL wire.** No pgwire gateway, so Postgres clients and
  drivers cannot connect unmodified.
- **No full-text search and no vector indexes.** Secondary indexes are currently equality seeks only.

## Roadmap

Everything above is the roadmap, in roughly that order. The shortcomings section is a plan
stated plainly rather than a list of regrets. Beyond it: bulk load that bypasses the hot
tier, so you can ingest existing data-lake files at full speed, and adopting an Iceberg
table in place with no migration. Retention gains the pruning half it is missing and the
scheduler gains leader election. A structured predicate on the read wire kills the
SQL-string double parse, with aggregate / limit / TopN pushed into the scan. Catalog
metadata gets checkpointed to object storage and reloaded into Postgres at startup, still
served from Postgres in steady state but recoverable from the object store alone.

## Documentation

- [docs/usage.md](docs/usage.md): connecting, a first table over SQL and gRPC, the demos, DataGrip
- [docs/architecture.md](docs/architecture.md): services, tiers, concepts · [docs/development.md](docs/development.md): build, run, test
- [docs/algorithms.md](docs/algorithms.md): read/write/merge · [docs/performance.md](docs/performance.md): benchmarks · [docs/schema-reference.md](docs/schema-reference.md): system tables
- [docs/decisions/](docs/decisions/) · [CONTRIBUTING.md](CONTRIBUTING.md) · [SECURITY.md](SECURITY.md)

## License

Apache-2.0. See [LICENSE](LICENSE).
