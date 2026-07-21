# 0014 — Fold AdminService into WriteService and QueryService

Status: Accepted
Ticket: [CHA-174](https://linear.app/chapala/issue/CHA-174)
Supersedes: pre-CHA-174 microservice topology section in [ADR 0005](0005-colocated-microservices-perf-boundary.md)

## Context

Pre-CHA-164, AdminService and WriteService had genuinely different
shapes. Admin operations (`CreateCatalog` / `CreateSchema` /
`CreateTable` and friends) wrote directly to metadata tables with no
transaction involvement. Data mutations went through
`BeginTx` → `MutateData` → `CommitTx` and resolved visibility via
`commit_tx_log`. The split mapped to a real boundary: "non-transactional
metadata DDL" vs "MVCC-tracked data writes."

[CHA-164](https://linear.app/chapala/issue/CHA-164) made schema/table
DDL transactional. `CreateSchema` / `CreateTable` / `UpdateTable` and
peers now write to `*_log` auditable stores tagged with `tx_uuid` and
gate on `commit_tx_log` for visibility — the same MVCC scheme as data writes.
Both kinds of mutation now resolve through the same transaction
machinery, with the same `tx_uuid`-mode-switch (`tx_uuid` set →
append to open tx; unset → auto-commit a fresh tx). The architectural
justification for a separate AdminService collapsed.

## Decision

Delete `AdminService` outright. Move its 9 mutation RPCs to
`WriteService` and its 6 read RPCs to `QueryService`. The deployment
topology shrinks from 5 microservices (admin, query, write, lifecycle,
storage-metadata) to 4 (query, write, lifecycle, storage-metadata).

**Mutations on WriteService:**
- `CreateCatalog`, `UpdateCatalog`, `DeleteCatalog`
- `CreateSchema`, `UpdateSchema`, `DeleteSchema`
- `CreateTable`, `UpdateTable`, `DeleteTable`

Schema and table mutations carry `author` / `comment` (mode-switched
by `tx_uuid` exactly like `MutateData`). Catalog mutations remain
non-transactional — they bootstrap the per-catalog `commit_tx_log` and
metadata stores, so they cannot themselves participate in a tx.

**Reads on QueryService:**
- `GetCatalog`, `ListCatalogs`
- `GetSchema`, `ListSchemas`
- `GetTable`, `ListTables`

`GetTable` / `ListTables` keep the effective-retention coalesce
(table → schema → catalog) on the read side.

## Consequences

- **One canonical tx-creation path on the write side.** Schema/table
  DDL auto-commit and `MutateData` auto-commit now share
  `WriteManager::resolve_or_auto_commit_tx`, which in turn calls a
  single storage helper `HotStorageClient::auto_commit_tx`. The old
  `MetadataClient::auto_commit_admin_tx` and
  `HotStorageClient::create_merge_tx` are deleted; both were variants
  of "atomically insert one row into `commit_tx_log`" under different names.
  The `commit_tx` storage helper (the back half of the explicit
  `BeginTx` / `CommitTx` flow) is renamed `commit_open_tx` for clarity.
- **Internal Rust clients (`penca-sql-server`, `penca-datafusion`)
  point at QueryService for all metadata discovery.** Their previous
  `AdminServiceClient` uses (`GetCatalog`, `GetTable`, `ListCatalogs`,
  `ListSchemas`, `ListTables`) were uniformly read-only — the
  `admin_channel` / `ADMIN_SERVICE_ADDR` plumbing is replaced by
  `query_channel` / `QUERY_SERVICE_ADDR` everywhere.
- **Python `PencaClient` drops the `admin` stub** and its
  corresponding `PENCA_ADMIN_URL` env var. DDL mutations route
  through `self._write`; DDL reads through `self._query`. Schema and
  table DDL methods gain `author` / `comment` parameters mirroring
  `mutate_data`.
- **`ADMIN_DEFAULT_PAGE_SIZE` collapses into existing
  `QUERY_DEFAULT_PAGE_SIZE`.** Mutations don't paginate, so the only
  caller of the page-size knob lives on QueryService now.
- **Bootstrap is unchanged.** `crates/penca-server-grpc/src/bin/bootstrap.rs`
  already seeds the default catalog via `MetadataClient` directly,
  not through gRPC; CHA-174 does not change that path.

## Why fold rather than keep three services

A standalone `AdminService` after CHA-164 would be a service whose
RPCs differ from WriteService's only by name. Both would write to
`commit_tx_log` under the same MVCC scheme, both would accept the same
`tx_uuid` mode-switch, both would be invoked by the same internal
clients in the same shape. The boundary between them would be purely
historical. Deployment cost (one more container per environment, one
more URL per client) doesn't earn its keep.

## Why keep AdminService's read RPCs as a separate move (to QueryService)

Reads have a different lifecycle from writes: separate scaling,
separate failure modes, and a separate cache layer in the SQL
gateway. Putting catalog/schema/table reads on QueryService keeps
"all reads in one place" coherent with `ReadData` / `AuditData` /
`GetBranch` etc. without inflating WriteService's responsibilities.

## Pointers

- Migration guide: clients setting `PENCA_ADMIN_URL` should remove
  it; mutation calls go to `PENCA_WRITE_URL`, read calls to
  `PENCA_QUERY_URL`. Internal Rust services replace
  `ADMIN_SERVICE_ADDR` with `QUERY_SERVICE_ADDR`.
- The original CHA-164 worktree commit `54342f1` (saved at
  `refs/backup/cha-164-pre-rebase`) did this fold pre-CHA-170 — useful
  as a reference for diff shape, though not directly cherry-pickable
  against current main.
