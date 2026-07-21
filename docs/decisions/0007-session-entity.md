# 0007 — Sessions are TCP-connection-local in `penca-sql-server`, not cookie-identified

- **Status:** Accepted
- **Date:** 2026-04-29 (initial); 2026-05-22 ([CHA-255](https://linear.app/chapala/issue/CHA-255) — per-TCP-conn rewrite)
- **Ticket:** [CHA-161](https://linear.app/chapala/issue/CHA-161) (initial); [CHA-253](https://linear.app/chapala/issue/CHA-253) (handshake-pinned catalog); [CHA-255](https://linear.app/chapala/issue/CHA-255) (per-TCP-conn rewrite)
- **Related:** [CHA-122](https://linear.app/chapala/issue/CHA-122) (SQL transaction control); [CHA-119](https://linear.app/chapala/issue/CHA-119) (per-conn branch); [CHA-159](https://linear.app/chapala/issue/CHA-159) (auth — per-conn); [CHA-86](https://linear.app/chapala/issue/CHA-86) (every read pins a bounded MVCC snapshot); [CHA-162](https://linear.app/chapala/issue/CHA-162) (`AbortTx` — needed for SQL `ROLLBACK`); [CHA-169](https://linear.app/chapala/issue/CHA-169) (catalog as connection-level invariant); [CHA-253](https://linear.app/chapala/issue/CHA-253) (catalog binding at handshake).

## Context

`penca-sql-server` needs connection-scoped state to bind a raw-SQL `BEGIN` to a later `INSERT` that doesn't echo `tx_uuid`. The naive option — keep that state in process memory keyed by a connection cookie — was initially rejected on three grounds: sticky load balancing required, in-flight transactions lost on server restart, and no horizontal scale.

The first proposal (CHA-161) responded by elevating `Session` to a first-class Penca entity persisted in Postgres. While implementing it we revisited the rejection arguments and found they don't hold against the actual requirement (1 session = 1 connection):

| Original argument | Holds for 1:1 session-connection? |
|---|---|
| Sticky LB required | **No.** The Flight SQL gRPC stream is itself the affinity. New connections route freely. |
| In-flight txs lost on restart | **Doesn't distinguish.** When the SQL server restarts, the gRPC stream drops and the client has to reconnect and restart their tx anyway. |
| No horizontal scale | **No.** Each connection lives on whichever server accepts it. Same model as Postgres / MySQL. |

The original CHA-161 design was solving Spanner-style sessions (sessions that span connections, decouple from the gRPC channel). What's actually needed is Postgres-style sessions (1 session = 1 connection, dies with the connection). Those are different primitives, and the simpler one fits the requirement.

The CHA-161 implementation kept a cookie (`penca-session-id`) as the session identifier, with the rationale that the gRPC stream itself was a near-equivalent affinity boundary. CHA-255 collapses the last gap: cookies are deleted entirely; sessions are scoped to the TCP connection itself, with no cross-conn identity.

## Decision

**A Penca SQL session is owned by one TCP connection.** When the TCP conn closes (cleanly or via network drop), the session is gone. This matches how PostgreSQL scopes databases to a TCP connection: `psql` to a database, run statements, close the connection, state is gone. The Penca core API stays unchanged: `Tx` is the only persisted user-facing primitive; `BeginTx` / `CommitTx` / `AbortTx` (CHA-162) / `MutateData` are the operations.

Concretely, in `penca-sql-server`:

- Each accepted TCP connection gets a fresh `ConnSession` instance. The catalog and branch are pinned at first request from the `x-penca-catalog` / `x-penca-branch` gRPC metadata headers (mirroring CHA-253 / CHA-119), falling back to `SQL_SERVER_DEFAULT_CATALOG` / `SQL_SERVER_DEFAULT_BRANCH`. Both are immutable for the connection's lifetime — mirroring Postgres's `Connection.setCatalog()`, which is a no-op for the same reason.
- The `ConnSession` holds: `catalog_uuid` / `catalog_name` (frozen at mint), `branch_uuid` / `branch_name` (frozen at mint; `branch_uuid` is resolved via `MetadataClient::get_branch_by_name` so the routing identity is rename-stable per CHA-255), `catalog_list: Vec<(name, uuid)>` (one-shot snapshot at mint), `ctx: Arc<SessionContext>` (per-conn DataFusion context), and `open_tx_uuid: Mutex<Option<String>>` (the only field that mutates post-mint, on BEGIN/COMMIT/ROLLBACK).
- `BEGIN` calls `WriteService.BeginTx` **eagerly** against the connection's pinned catalog and caches the returned `tx_uuid` on the `ConnSession`. A second `BEGIN` while the connection already has an open tx is rejected with `FAILED_PRECONDITION`. Per [CHA-163](https://linear.app/chapala/issue/CHA-163), Penca transactions are catalog-scoped — a single tx spans every schema in its catalog.
- DML statements (`INSERT` / `UPDATE` / `DELETE`) call `MutateData` with the cached `tx_uuid`. DML targeting a catalog other than the connection's pinned catalog is rejected with `FAILED_PRECONDITION`. Cross-schema DML *within the connection's catalog* is the entire point of CHA-163. Wire payloads (`MutateData`, `BeginTx`, `CommitTx`, `AbortTx`, `ReadData`, `GetSchema`, `ListSchemas`, `GetTable`) route by `branch_uuid` (CHA-255 — rename-stable).
- `COMMIT` calls `CommitTx`. `ROLLBACK` calls `AbortTx` (CHA-162). Both clear the `ConnSession`'s `open_tx_uuid`. A `COMMIT` / `ROLLBACK` with no open tx is `FAILED_PRECONDITION`.
- `SET branch = '<name>'` / `SET catalog = '<name>'` mid-session are rejected with `FAILED_PRECONDITION` ("fixed at handshake; reconnect to switch") on mismatch; no-op on match. `default_schema` is freely mutable mid-session via `SET search_path` / `SetSessionOptions(db_schema: …)` — it lives on `SessionConfig.options.catalog.default_schema` inside the per-conn `ctx`, not as a top-level `ConnSession` field.
- The authenticated principal will flow from the gRPC auth interceptor (CHA-159) into the `ConnSession`, then into `BeginTx.author`.
- MVCC reads pin to a bounded snapshot (CHA-86, implemented): `pg_now` for an autocommit read; once SQL transactions land (CHA-122) a read inside a transaction pins `as_of_micros` to the per-conn cached `Tx.began_at_micros`. There is no unbounded read path.

There is no `Session` proto, no `session_store` table, no `session_uuid` column on `begin_tx_log`, no `penca-session-id` cookie surface, no `SessionCache` DashMap, no idle-eviction sweeper.

### Catalog list is frozen at conn-mint

The first request on each accepted TCP connection issues a one-shot batched `QueryServiceClient::list_catalogs` and stores the `(name, uuid)` pairs on the `ConnSession`. `SHOW CATALOGS`, three-part SQL identifier resolution, and DataFusion's `CatalogProviderList` consult this frozen snapshot for the conn's lifetime. New catalogs created mid-session are not visible to the conn; reconnect to refresh. Renames keep the old name discoverable, new name not.

**Schema and table metadata stays live.** Every `get_schema` / `list_schemas` / `get_table` / `list_tables` issued by DataFusion hits the metadata service via gRPC — otherwise `CREATE SCHEMA foo; SELECT * FROM foo.t` in the same conn would break. The CHA-119-era `MetadataCaches` / `TtlLruCache` (a TTL cache over catalog + schema lookups) is deleted in CHA-255; the per-conn catalog snapshot replaces the catalog half, and the schema half goes through to gRPC live.

### Connection drop during an open transaction

When the TCP conn closes, `ConnSession::Drop` runs. If `open_tx_uuid.try_lock()` finds a `Some(tx_uuid)`, the Drop spawns a fire-and-forget `WriteServiceClient::abort_tx` on the tokio runtime so the orphan `tx_uuid` is bounded by the WriteService's response rather than the WriteService TTL (default 60s). Both branches — spawn-on-success and runtime-not-available — are best-effort: the WriteService TTL is the absolute backstop.

This eliminates the CHA-161 era "Eviction during an open transaction" failure mode (cookie pointing at an evicted session, `stale_cookie` flag, `SessionEvicted` error path). The conn close *is* the trigger; the `Drop` cleanup runs synchronously with the conn going away.

## Consequences

- **`penca-sql-server` is "stateful" only at the TCP-connection level.** Connection state is bounded by the lifetime of the TCP socket — the same kind of state Postgres `pg_stat_activity` rows or MySQL `SHOW PROCESSLIST` entries represent. Not the kind of state that breaks horizontal scale.
- **Programmatic clients use `Tx` directly.** They get `tx_uuid` from `BeginTx` and thread it through their own subsequent calls. No `Session` API to learn.
- **Connection drop = transaction abandonment.** The conn's `Drop` fires `AbortTx`; the orphan `tx_uuid`'s `begin_tx_log` row + uncommitted `upsert_log` rows expire promptly. Callers must reconnect and re-`BEGIN`.
- **CHA-119, CHA-159, CHA-86 are connection-state additions in the SQL server.** No schema or RPC changes against Penca.
- **CHA-162 (`AbortTx`) is the explicit transaction-abandonment mechanism.** Both `ROLLBACK` and `ConnSession::Drop` route through it.
- **CHA-253 / CHA-255 collapse the configuring window and the cookie surface.** The post-handshake `SetSessionOptions(catalog: …)` reseat path and the `Session.configured` bool are gone. Catalog and branch are handshake-pinned; the structural invariant (one conn = one catalog = one branch) is enforced by the type system rather than by post-handshake guards.

## Future considerations (would prompt revisiting)

These are explicitly out of scope. If they become real requirements, the decision in this ADR is revisited and a new design is drafted:

- **Cross-connection session affinity for BI tools.** Some BI clients keep many short-lived JDBC/ADBC connections and benefit from sharing authenticated session state (cached prepared statements, server-side aggregations). The right shape for this is a separate session-router service layered in front of `penca-sql-server` that holds affinity, not persisted sessions in Penca core. Adopt only when the workload demands it.
- **Resumable transactions across connection drops.** Postgres and MySQL don't support this; Spanner does (via its session model). If a workload genuinely needs "BEGIN on connection A, COMMIT on connection B," that's the same persisted-session shape CHA-161 originally proposed — revisit then.
- **Picking up mid-session catalog metadata changes.** Frozen at mint by design. If users need this for long-lived dashboard sessions, a per-conn admin RPC ("refresh catalog list") is cheaper than going back to TTL caching — the right way to introduce it is opt-in, not unconditional.

## Pointers

- **CHA-122** — initial implementation of the per-connection cache + SQL-to-Penca RPC routing.
- **CHA-162** — `AbortTx` + `abort_tx_log`. Required for SQL `ROLLBACK` and `ConnSession::Drop`.
- **CHA-119** — `x-penca-branch` header + connection-scoped branch.
- **CHA-159** — gRPC auth interceptor populates `author`.
- **CHA-86** — every read pins a bounded MVCC snapshot (`pg_now` by default); inside a SQL transaction (CHA-122) the pin is the per-conn cached `Tx.began_at_micros`.
- **CHA-169** — catalog as connection-level invariant.
- **CHA-253** — catalog binding at handshake (collapsed the configuring window).
- **CHA-255** — per-TCP-conn rewrite (this ADR's current shape).
