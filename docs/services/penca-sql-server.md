# penca-sql-server

**Port**: 50060
**Protocol**: [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
**Crate**: `crates/penca-sql-server/`

## Purpose

Exposes Penca tables as a DataFusion SQL endpoint over Arrow Flight SQL.
Clients (JDBC / ODBC / ADBC) connect and issue ordinary SQL; the server
resolves catalog / schema / table names against the Penca metadata plane
and streams Arrow batches back.

The server does not hold data. Every query plans through a globally-shared
`PencaCatalogProviderList` (built once at startup, `Arc::clone`-shared
across every session per [ADR 0010](../decisions/0010-flight-sql-tx-pin-routing.md))
that fans out to the `query` microservice via gRPC. The
`query` service continues to own the merge-on-read algorithm;
penca-sql-server just wraps it in a SQL front-end.

Per-connection state (catalog pin, branch pin, open `tx_uuid`,
`Arc<SessionContext>`) lives on a `ConnSession` instance owned by each
accepted TCP connection — see
[ADR 0007](../decisions/0007-session-entity.md). One `Arc<SessionContext>`
is built per conn at first-request mint and reused across every HTTP/2
stream on that TCP conn; the open `tx_uuid` flips on the `Arc`-shared
`ConnScope.open_tx_cell` (CHA-345) on `BEGIN` / `COMMIT` / `ROLLBACK` —
no `SessionState` mutation for tx control. When the TCP conn closes,
`ConnSession::Drop` fires `WriteService.AbortTx` for any in-flight
transaction and the conn's state is gone — there is no cross-conn
cookie surface and no idle eviction (CHA-255).

### Layered session caching

The per-connection `Arc<SessionContext>` is itself a microsecond clone of a
process-wide `SessionState` *template* built once at startup (the expensive
default function registry + analyzer/optimizer rules).
`ConnSessionFactory::build_ctx` derives each connection's context via
`SessionStateBuilder::new_from_existing(template.clone())` with a fresh
per-connection `PencaCatalogProviderList`. On top of that, each connection
holds a `statement_cache` of already-planned `LogicalPlan`s keyed by
`statement_uuid` (CHA-355) so `GetFlightInfo`'s plan is reused by `DoGet`.
Three nested scopes: process (template) → connection (ctx + catalog snapshot) →
statement (cached plan). The query service's cold-read path uses the same
template → per-unit-clone pattern (CHA-421); see
[Layered session scope and caching](../development-methodology-guide.md#layered-session-scope-and-caching).

## Endpoints (Flight SQL handlers)

- `GetFlightInfoStatement` / `DoGetStatement` — ad-hoc SQL reads.
- `DoPutStatementUpdate` — SQL DML (INSERT / UPDATE / DELETE). See
  [DML](#dml) below. The prepared-statement variant
  (`DoPutPreparedStatementUpdate`) is **not** wired; the Python
  client splits `execute_query` / `execute_update` so DML never goes
  through the prepare path.
- `GetCatalogs` / `GetDbSchemas` / `GetTables` / `GetTableTypes` —
  metadata introspection used by JDBC/ADBC auto-discovery.
- `GetSqlInfo` — Flight SQL server-capability handshake. Mandatory for
  JDBC `DatabaseMetaData` clients; the Dremio flight-sql-jdbc-driver
  used by every JetBrains DB tool + DBeaver calls it on first connect
  (via `getDatabaseProductName()`), and `UNIMPLEMENTED` here makes the
  connection unusable for any JDBC GUI. See [JDBC](#jdbc) below.
- `do_action_begin_transaction` / `do_action_end_transaction` — SQL
  `BEGIN` / `COMMIT` / `ROLLBACK` (see [Transactions](#transactions)).

Other Flight SQL calls (bulk ingest, savepoints, query cancellation,
`GetPrimaryKeys` / `GetExportedKeys` / `GetImportedKeys` /
`GetCrossReference` / `GetXdbcTypeInfo`) are unimplemented. The
structured mutation gRPC (`WriteService.WriteData` + `BeginTx`/`CommitTx`)
stays available as the programmatic primitive for callers that need
multi-table atomic writes; JDBC tooling tolerates `UNIMPLEMENTED` on
the metadata calls (the introspector just shows empty results).

`CREATE SCHEMA` and `CREATE TABLE` are supported via Flight SQL — both
auto-commit ([CHA-172](https://linear.app/chapala/issue/CHA-172)) and
inside a `BEGIN`/`COMMIT` block
([CHA-345](https://linear.app/chapala/issue/CHA-345)). They route through
the gateway to `WriteService.CreateSchema` / `WriteService.CreateTable`,
threading the open `tx_uuid` when one is set so the metadata row is
written under the transaction (visible to the tx's own reads, discarded
on `ROLLBACK`). Other DDL (`DROP …`, `ALTER …`, `CREATE INDEX`,
`CREATE VIEW`, …) still requires the gRPC `WriteService` directly — in
both auto-commit and transactional context; the rejection wording points
users there. See [ADR 0010](../decisions/0010-flight-sql-tx-pin-routing.md)
(and its CHA-345 addendum) for how transactional DDL was unblocked.

## JDBC

JDBC GUI tools (DataGrip, DBeaver, JetBrains DB, anything backed by
Apache's `flight-sql-jdbc-driver`) connect with:

```
jdbc:arrow-flight-sql://localhost:50060?useEncryption=false
```

Drop `flight-sql-jdbc-driver-<version>.jar` into the tool's JDBC driver
list, point it at the URL above, and "Test Connection" should succeed.
The driver's first call after handshake is `getDatabaseProductName()`,
which lazily loads the `SqlInfo` cache via `GetSqlInfo`; that returns
`"penca"` as the product name, the crate's `CARGO_PKG_VERSION` as the
server version, and the standard set of capability flags (DDL =
supported for the `CREATE SCHEMA` / `CREATE TABLE` auto-commit slice
per [CHA-172](https://linear.app/chapala/issue/CHA-172), transactions
via SQL statements supported, no read-only mode, identifier
case-folding follows Postgres' lowercase-unquoted rule, transactions
supported). A bare `SELECT 1` in the JDBC console confirms the full pipe
is up.

The driver pings `GetCatalogs` / `GetSchemas` / `GetTables` for
introspection; those work and populate the database explorer.
`GetPrimaryKeys` / `GetExportedKeys` / `GetImportedKeys` /
`GetCrossReference` / `GetXdbcTypeInfo` return `UNIMPLEMENTED` — the
introspector falls back to empty results without breaking the
connection.

DataGrip's "Error unmarshaling return; nested exception is:
java.io.NotSerializableException: ... CallStatus" is JetBrains' RMI
bridge failing to serialize a Flight `CallStatus`; the actual error is
masked. Run the driver directly out of DataGrip (Maven Central →
`flight-sql-jdbc-driver`, then `java -cp <jar> JdbcProbe.java`) if you
need to see what the server is actually saying.

### JDBC regression test

`TestFlightSqlJdbcProbe` in `tests/integration/integration_flight_sql_test.py`
drives the same Apache `flight-sql-jdbc-driver` JAR through the
ticket's acceptance queries (`SELECT 1`, `SELECT * FROM users`,
`SELECT * FROM public.public.users`) on every `just integration-test`.
A pyarrow-based test catches *spec* regressions in our `SqlInfo`
response; this catches *driver-compat* regressions — a dense-union
quirk pyarrow tolerates but the Dremio driver chokes on would fail
here, not there. The test skips with an actionable message if the
JVM or JAR aren't set up:

```
# Local dev — once per machine:
apt install openjdk-21-jdk-headless   # or `brew install --cask temurin`
just fetch-jdbc-driver                # downloads + SHA-256-verifies the JAR

# Then the usual:
just integration-test
```

The JAR is pinned at the version + checksum in the `fetch-jdbc-driver`
recipe; CI runs `actions/setup-java@v4` (Temurin 21) + the same
recipe so the JDBC smoke is always part of regression coverage.

## DML

`DoPutStatementUpdate` is the single entry point for INSERT, UPDATE,
and DELETE. The handler parses the SQL with sqlparser and orchestrates
the read-then-write itself — penca-sql-server is the SQL-aware layer,
the WriteService is a pure append-and-commit. There is exactly one
write RPC (`WriteData`, mode-switched on `tx_uuid`); the verb-specific
behaviour lives entirely in the gateway.

| SQL | Orchestration | Semantics |
|---|---|---|
| `INSERT INTO t VALUES (...)` / `INSERT INTO t SELECT ...` | derive `row_uuid` per row for the IN-list → collision-check `SELECT row_uuid IN (...)` via `QueryService` under a Pg advisory lock → `WriteData` | strict: `ALREADY_EXISTS` on PK collision |
| `INSERT ... ON CONFLICT DO UPDATE` | `WriteData` (no check, no lock) | last-writer-wins |
| `UPDATE t SET ... WHERE ...` | `SELECT *` (with SET expressions inlined) via `QueryService` → `WriteData` | READ COMMITTED-equivalent |
| `DELETE FROM t WHERE ...` | `SELECT pk_columns` via `QueryService` → derive `row_uuid` per row → `WriteData` | READ COMMITTED-equivalent |

`Change.upserts` carries the user-shape Arrow IPC; the WriteService
derives `row_uuid` (via `naming::row_uuid_for_pk`) and mints
`version_uuid` itself. penca-sql-server only derives `row_uuid` in
two narrow places: (a) building the strict-INSERT collision-check
IN-list against `QueryService`, and (b) populating `Change.deletes`
(which is wire-typed as `repeated string row_uuid`).

### Strict-INSERT advisory lock

Strict INSERT runs the collision-check `SELECT` and the `WriteData`
append as one critical section under a per-(branch, table) Postgres
advisory lock. Lock key:

```
dml:strict-insert:{branch_name}:{table_uuid}
```

Without the lock, two concurrent strict-INSERTs on the same primary
key could both pass the collision check (each sees an empty result)
and both append, defeating the strict semantics. The lock is acquired
on a tiny orchestrator-only Pg pool wired into penca-sql-server (no
data plane traffic flows through it). LWW upserts skip the lock by
design; UPDATE and DELETE do too — they inherit READ COMMITTED-
equivalent semantics from the gap between their `SELECT` and their
write.

See [ADR 0006](../decisions/0006-sql-dml-out-of-write-microservice.md)
for the rationale, and [CHA-141](https://linear.app/chapala/issue/CHA-141)
for the underlying `PgDriver::advisory_lock` machinery.

### Auto-commit vs explicit transactions

`CommandStatementUpdate.transaction_id` flows into `tx_uuid` resolution
in [`tx::resolve_tx_uuid_for_dml`](../../crates/penca-sql-server/src/tx.rs):

1. **Explicit `transaction_id` on the request** — used as-is (structured
   Flight SQL transaction path: ADBC `set_autocommit(False)` calls
   `do_action_begin_transaction`, threads the returned `tx_uuid` on
   every subsequent DML).
2. **Snapshot says session has an open tx** — return the cached
   `tx_uuid`. This is the raw-SQL `BEGIN ... INSERT ... COMMIT`
   path: `BEGIN` populated the cache but the `INSERT` didn't carry a
   `transaction_id` on the wire.
3. **No open tx** — `WriteData` auto-commits its own one-shot tx; the
   resulting `Tx` comes back on the response.

The WriteService handles both modes on the single `WriteData` RPC
(see [ADR 0004](../decisions/0004-sql-dml-via-flight-sql.md) for the
collapse rationale, [CHA-152](https://linear.app/chapala/issue/CHA-152)).

See [ADR 0006](../decisions/0006-sql-dml-out-of-write-microservice.md)
for the rationale (why DML orchestration lives in the gateway and not
in the WriteService, why row identity stays server-derived, why an
advisory lock instead of optimistic CC). Decisions 1 and 4 of the
original [ADR 0004](../decisions/0004-sql-dml-via-flight-sql.md)
(intercept at the Flight SQL boundary; Python client splits
`execute_query` / `execute_update`) still hold.

## Transactions

`BEGIN` (raw SQL or `do_action_begin_transaction`) calls
`WriteService.BeginTx` against the **connection's pinned catalog**,
records the returned `tx_uuid` on the per-conn `ConnSession`, and
flips the `Arc`-shared `ConnScope.open_tx_cell` (CHA-345). Subsequent
DML and SELECT pick up the open tx via
[`tx::resolve_tx_uuid_for_dml`](../../crates/penca-sql-server/src/tx.rs)
and `PencaTableProvider::scan` respectively; and, also via the cell,
`PencaSchemaProvider::{table,table_names,table_exist}` /
`PencaCatalogProvider::{schema,schema_names}` resolve tx-aware — which
is what makes a table or schema created earlier in the same transaction
visible to the tx's later statements (transactional DDL, CHA-345). Reads inside the open tx
are snapshot-isolated against the tx's `began_at_micros` and layered
with the tx's own uncommitted writes (RYOW), via the `open_tx_uuid`
parameter on `ReadData` (CHA-165). A `SELECT` repeated inside the same
open tx returns the same set across a concurrent commit on a separate
connection.

`COMMIT` / `ROLLBACK` atomically take the cached `(catalog_uuid, tx_uuid)`
pair via `ConnSession::take_open_tx`, dispatch to `WriteService.CommitTx`
/ `AbortTx`, and clear the pin. `COMMIT` / `ROLLBACK` outside an open
tx returns `FAILED_PRECONDITION` (matches Postgres's WARNING-and-no-op
semantics, surfaced as a clean error).

If the TCP conn drops with an open tx, `ConnSession::Drop` fires a
fire-and-forget `WriteService.AbortTx` for the orphan `tx_uuid` —
bounded by the WriteService response rather than the WriteService TTL
backstop (default 60s). Clients still need to reconnect and re-`BEGIN`;
the cleanup is just an optimisation that frees write-side state
promptly.

A second `BEGIN` while a tx is already open is rejected — Penca does
not support nested transactions or savepoints.

### Client autocommit settings

Penca advertises `FlightSqlServerTransaction = Transaction` in
`GetSqlInfo` (CHA-249). This is the canonical answer — Penca supports
transactions via the Flight SQL `BeginTransaction` action — but it has a
**knock-on effect on Python DB-API 2.0 clients**: the standard
`adbc_driver_flightsql.dbapi.connect(...)` defaults to
`autocommit=False`, which fires `BeginTransaction` *inside* `connect()`
against the connection's pinned catalog. The connection lands with an
autostarted tx already open, and any subsequent explicit SQL `BEGIN`
collides with our nested-tx rejection.

There are two correct usage patterns; pick one per connection:

1. **Recommended for code that uses explicit `BEGIN`/`COMMIT` SQL.**
   Pass `autocommit=True` to `flight_sql_connect(...)`:

   ```python
   from adbc_driver_flightsql.dbapi import connect

   conn = connect(
       "grpc://host:50060",
       db_kwargs={"adbc.flight.sql.rpc.call_header.x-penca-catalog": "mydb"},
       autocommit=True,
   )
   cursor = conn.cursor()
   cursor.execute("BEGIN")
   cursor.execute("INSERT INTO sales.customers VALUES ('alice', 10)")
   cursor.execute("COMMIT")
   ```

   This is Postgres-style: the connection auto-commits each statement
   unless it's wrapped in an explicit `BEGIN`/`COMMIT` block. Penca's
   first-party `PencaClient` (`packages/penca-client/`) opens its
   internal Flight SQL connection in this mode by default.

2. **For code that uses the DB-API 2.0 transaction surface
   (`conn.commit()` / `conn.rollback()`).** Leave the dbapi default of
   `autocommit=False`. `connect()` autostarts a tx; issue DML through
   the cursor as usual; close it with `conn.commit()` (which sends
   `ActionEndTransaction(Commit)` and starts a fresh tx on the next
   statement) or `conn.rollback()` (the `Abort` variant). Do **not**
   issue `BEGIN` SQL in this mode — it conflicts with the autostarted
   tx.

   ```python
   conn = connect(
       "grpc://host:50060",
       db_kwargs={"adbc.flight.sql.rpc.call_header.x-penca-catalog": "mydb"},
       # autocommit=False is the default
   )
   cursor = conn.cursor()
   cursor.execute("INSERT INTO sales.customers VALUES ('alice', 10)")
   conn.commit()
   ```

Java JDBC defaults to `autocommit=true` automatically (the standard
`Connection.setAutoCommit(true)` is the spec default), so JDBC GUI
clients are equivalent to pattern (1) without needing an explicit
override.

**Writes outside an open tx auto-commit server-side.** A DML statement
issued with no `transaction_id` (either because the client is in
autocommit-on mode, or it's the bare `INSERT`/`UPDATE`/`DELETE` between
two transactions) lands as a single-statement tx on `WriteService.WriteData`
— the row is durably committed and visible to other sessions immediately
upon return. There is no client-side "implicit BEGIN" buffering layer
in Penca; the server auto-commits per statement. See
[Auto-commit vs explicit transactions](#auto-commit-vs-explicit-transactions)
above for the wire-level resolution.

### Rejected `BEGIN` modifiers

Modifiers Penca doesn't honour are rejected with `UNIMPLEMENTED` at the
parse step (see [`tx::validate_start_transaction`](../../crates/penca-sql-server/src/tx.rs))
rather than silently coerced to plain `BEGIN`:

| Form | Status |
|---|---|
| `BEGIN`, `BEGIN TRANSACTION`, `BEGIN WORK`, `START TRANSACTION` | accepted |
| `BEGIN ISOLATION LEVEL { SERIALIZABLE \| REPEATABLE READ \| READ COMMITTED \| READ UNCOMMITTED }` | `UNIMPLEMENTED` — every Penca tx runs under the snapshot+RYOW visibility predicate from CHA-165 |
| `BEGIN READ ONLY` / `BEGIN READ WRITE` | `UNIMPLEMENTED` — Penca has no read-only tx mode |
| `BEGIN DEFERRED \| IMMEDIATE \| EXCLUSIVE \| TRY \| CATCH` | `UNIMPLEMENTED` — SQLite locking + T-SQL exception modifiers |
| `BEGIN ... END` (BigQuery procedural blocks) | `UNIMPLEMENTED` — penca-sql-server treats `BEGIN` purely as transaction control |

Loud rejection over silent coercion: a client that asks for SERIALIZABLE
and gets a Penca snapshot tx without warning would have a very wrong
mental model of what their transaction guarantees.

### Connection-scoped catalog pin

A Penca SQL connection is bound to one catalog the way a PostgreSQL
connection is bound to one database. The binding is sourced at
handshake from the `x-penca-catalog` gRPC metadata header (or the
JDBC `?catalog=…` URL param the driver translates from it), falling
back to `SQL_SERVER_DEFAULT_CATALOG` when absent. Switching catalogs
means reconnecting.

Mid-session catalog changes follow Postgres `Connection.setCatalog`
semantics:

- `Connection.setCatalog(X)` where `X` matches the connection's pin
  is a **no-op**.
- `Connection.setCatalog(Y)` where `Y` differs is rejected with
  `FAILED_PRECONDITION` — "catalog is fixed at handshake; reconnect
  to switch." Same wording for the `SetSessionOptions(catalog: Y)`
  wire action and for SQL `SET catalog = 'Y'`.

Every catalog-scoped action — `BEGIN`, DML, SELECT — validates its target
catalog against the session's pinned catalog. Cross-catalog access on a
single connection is `FAILED_PRECONDITION`. See
[ADR 0010](../decisions/0010-flight-sql-tx-pin-routing.md) for the
routing design and concurrency story.

### Identifier case-sensitivity

Identifier handling follows Postgres conventions: unquoted identifiers
fold to lowercase, double-quoted identifiers preserve case verbatim.
The gRPC `create_*` APIs (in `WriteService` / `LifecycleService`) store
names exactly as supplied — equivalent to writing `CREATE TABLE "Name"`
in psql. There is no canonicalization at the gRPC boundary, so a
catalog created as `MyCat` and later referenced from SQL as
`SELECT … FROM MyCat.…` resolves to `mycat` (the parser lowercased the
unquoted identifier) and misses. For SQL ergonomics, prefer lowercase
ASCII names when creating catalogs / schemas / tables, or quote
consistently on both the gRPC-create side and the SQL-reference side.

## Dependencies

- **query** (50052) — the merge-on-read reader that feeds every
  `TableProvider::scan`; the strict-INSERT collision check; and the
  table-metadata fetch DML uses for the `arrow_schema` (cast/coercion
  before encoding) and for the primary keys used in the strict-INSERT
  collision-check IN-list / DELETE row_uuid derivation.
- **write** (50053) — receives `WriteData` produced by
  `DoPutStatementUpdate` (auto-commit when `transaction_id` is empty,
  append when set).
- **postgres** — orchestrator-only pool used solely for the
  per-(branch, table) advisory lock around strict-INSERT (ADR 0006).
  No data reads or writes flow through it.

No object-storage access. No session state is persisted across
connections.

## Config

| Env var | Purpose |
|---|---|
| `BIND_ADDR` | Flight SQL listen address (e.g. `0.0.0.0:50060`) |
| `QUERY_SERVICE_ADDR` | gRPC URL of the query service (used for catalog / schema / table reads + the strict-INSERT collision check) |
| `WRITE_SERVICE_ADDR` | gRPC URL of the write service |
| `DATABASE_URL` | Postgres connection string for the orchestrator advisory-lock pool (ADR 0006) |
| `PG_POOL_MIN` / `PG_POOL_MAX` | Bounds on the orchestrator pool — sized to the maximum concurrent strict-INSERT lock holders |
| `SQL_SERVER_DEFAULT_CATALOG` | Catalog newly-minted sessions pin to when the `x-penca-catalog` header is absent |
| `SQL_SERVER_DEFAULT_SCHEMA` | Default schema for unqualified DML (`INSERT INTO foo` → `<catalog>.<default_schema>.foo`); typically `public` |
| `SQL_SERVER_DEFAULT_BRANCH` | Branch every session targets for `BEGIN` / DML; per-session overrides land with [CHA-119](https://linear.app/chapala/issue/CHA-119) |
| `SQL_SERVER_FLIGHT_STATEMENT_CACHE_CAPACITY` | Per-connection logical-plan cache size — `GetFlightInfo` stashes the statement-query plan so `DoGet` reuses it instead of re-planning ([CHA-355](https://linear.app/chapala/issue/CHA-355)); `0` disables the cache (every `DoGet` re-plans) |

No object-storage URL — the data plane stays gRPC-only. The Pg pool
exists purely for the strict-INSERT serialisation in `dml.rs`.

## Branch selection

Server-global, configured via `SQL_SERVER_DEFAULT_BRANCH`. Per-session
branch selection (`SET branch = ...`) is tracked in
[CHA-119](https://linear.app/chapala/issue/CHA-119); until then, reading
other branches means a second deployment with a different startup flag
or hitting the `query` gRPC API directly.

## Auth

Anonymous — the Flight SQL handshake does not validate credentials.
Deploy behind network-level auth (mTLS, service mesh) until auth lands
([CHA-159](https://linear.app/chapala/issue/CHA-159)). The Python ADBC
client notes this in the `PencaClient.execute_query` docstring.

## Failure modes

- **query or write unreachable.** `PencaCatalogProviderList`
  surfaces read-path gRPC errors as `DataFusionError::External`, which
  ADBC clients see as a Flight error. A write-service outage surfaces
  at `DoPutStatementUpdate` the same way — the call fails, no partial
  mutation lands.
- **Schema changes mid-query.** Schema and table metadata lookups are
  live (every `get_schema` / `get_table` hits gRPC); a table dropped
  between metadata lookup and query resolution surfaces as a Flight
  `Unknown` error on the `scan` call. Catalog *list* is frozen at
  conn-mint (CHA-255), so a catalog created mid-session is not visible
  to the conn until reconnect.
- **Cross-catalog access on a single connection.** A connection pinned
  to catalog A that issues SELECT or DML against catalog B gets
  `FAILED_PRECONDITION` at planning (SELECT) or DML-entry time. Reconnect
  with the desired catalog (`x-penca-catalog` gRPC metadata header).
- **Connection drop mid-transaction.** The conn's `Drop` fires
  `WriteService.AbortTx` for the in-flight `tx_uuid` (CHA-255); the
  client must reconnect and re-`BEGIN`. If the AbortTx itself fails
  (e.g. WriteService unreachable at conn close), the orphan tx times
  out via the WriteService TTL backstop
  (`WRITE_DEFAULT_TX_TIMEOUT_SECONDS`).
