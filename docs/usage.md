# Using Penca

How to talk to a running Penca stack: the two entry points, a minimal end-to-end
example over each, how to point a SQL IDE at it, and walkthroughs of the shipped demos.

Everything here assumes a stack is up and the client env is sourced:

```bash
just penca-up
set -a && source docker/.client.env
```

See [development.md](development.md) for what `just penca-up` actually starts.

## Connecting

Two entry points front the same three microservices:

- **Programmatic gRPC.** Direct channels to `WriteService`,
  `QueryService`, `LifecycleService` on
  ports 50052–50054. Full surface: catalog / schema / table CRUD
  (mutations on Write, reads on Query), branching, transactions,
  data mutations, lifecycle ops, streaming reads (`ReadData`,
  `AuditData`). The shipped Python `PencaClient` connects here; any
  third-party client built from the `protos/` files works the same
  way. `List*` RPCs are paginated with opaque base64 page tokens
  (currently wrapping an offset, but the type is opaque so we can
  switch to keyset pagination without breaking clients).
- **Arrow Flight SQL.** Port 50060, served by `penca-sql-server`.
  Reads (`SELECT`), DML (`INSERT` / `UPDATE` / `DELETE`), and
  transaction control (`BEGIN` / `COMMIT` / `ROLLBACK` via the Flight
  SQL action endpoints) for BI / ADBC / JDBC / ODBC clients. SQL DML
  translates to `WriteService.WriteData` under the hood; multi-table
  atomic writes still go through the gRPC `Insert` / `Update` /
  `Delete` primitives. See
  [services/penca-sql-server.md](services/penca-sql-server.md)
  for the session model, catalog pinning, and tx routing
  ([ADR 0007](decisions/0007-session-entity.md),
  [ADR 0010](decisions/0010-flight-sql-tx-pin-routing.md)).

The Python `PencaClient` wraps both surfaces:
`execute_query(sql)` / `execute_stream(sql)` / `execute_update(sql)`
for SQL; `read_data` / `audit_data` / `write_data` / branch + tx
methods for the gRPC surface.

### You do not need the client

`PencaClient` is a convenience, not a requirement. The SQL side is plain Arrow Flight
SQL, so ADBC directly, SQLAlchemy, JDBC and ODBC all connect to port 50060 without it;
the gRPC side is plain gRPC, so any client generated from `protos/` works. The client
itself is just an ADBC consumer:
[`_flight_sql_cursor`](../packages/penca-client/src/penca_client/client.py#L2077) opens
`adbc_driver_flightsql.dbapi.connect` against `grpc://<host>:50060` and nothing more
exotic.

```python
from adbc_driver_flightsql.dbapi import connect

with connect("grpc://localhost:50060") as conn, conn.cursor() as cur:
    cur.executescript("CREATE TABLE greetings (id BIGINT PRIMARY KEY, note VARCHAR)")
    cur.executescript("INSERT INTO greetings (id, note) VALUES (1, 'hello'), (2, 'world')")
    cur.execute("SELECT * FROM greetings ORDER BY id")
    print(cur.fetch_arrow_table())
```

**Use `executescript` for DDL and DML, not `execute`.** Flight SQL splits statements
across two server verbs: `GetFlightInfo` for anything returning rows, and
`DoPutStatementUpdate` for anything returning a row count. DB-API assumes one verb, so
`Cursor.execute` is hardwired to the query path and a `CREATE TABLE` sent through it is
rejected with `update statement routed to GetFlightInfo`. `Cursor.executescript` calls
`execute_update`, which is the right verb. One statement per call: multi-statement
requests are rejected. `PencaClient.execute_update` does the same thing through the
low-level statement handle.

The client also sets three connection options you would otherwise have to set yourself,
so it is worth reading that function before hand-rolling a session:

- `adbc.flight.sql.rpc.with_cookie_middleware=true`, so the server's `penca-session-id`
  `Set-Cookie` / `Cookie` round-trip binds successive statements to one session. Without
  it a multi-statement `BEGIN` / `INSERT` / `COMMIT` will not hold together
  ([ADR 0007](decisions/0007-session-entity.md)).
- `adbc.flight.sql.rpc.call_header.x-penca-branch` and `…x-penca-catalog`, the headers
  the server reads at session-mint time to pin the connection's branch and catalog. Both
  are immutable for the session's lifetime.
- `autocommit=True`. The DB-API default of `False` sends a `BeginTransaction` on connect,
  which then collides with an explicit SQL `BEGIN`; Penca's transaction surface is the
  explicit Postgres-style `BEGIN` / `COMMIT` / `ROLLBACK`.

## Your first table

One table, written over both surfaces. The gRPC arm below appends to the *same* table
the SQL arm created, and the final read returns every row; they are the same engine
addressing the same data, not two parallel worlds.

### Over SQL

`PencaClient.execute_update` runs DDL and DML; `execute_query` returns a
`pyarrow.Table`.

```python
from penca_client import PencaClient

client = PencaClient.from_settings()

client.execute_update("CREATE SCHEMA shop")
client.execute_update(
    "CREATE TABLE shop.orders (order_id BIGINT PRIMARY KEY, customer VARCHAR, total BIGINT)"
)
client.execute_update(
    "INSERT INTO shop.orders (order_id, customer, total) VALUES (1, 'ada', 4200), (2, 'grace', 1300)"
)

print(client.execute_query("SELECT * FROM shop.orders ORDER BY order_id"))
```

The catalog is pinned per connection at handshake, so unqualified names resolve against
the session's default catalog (`public` in the shipped compose stack) and there is no
`USE` statement to issue. Note that `IF NOT EXISTS` is not supported on either
`CREATE SCHEMA` or `CREATE TABLE`; re-running this block against a live stack fails on
the already-existing schema rather than silently no-opping.

### Over gRPC

The gRPC surface is explicit about the hierarchy and hands identity back to you, which
is why the demos use it for setup: forking a branch pins to a `commit_seq_num` that SQL
does not return. It accepts human-readable names anywhere a UUID is expected, so it can
address the table the SQL arm just made:

```python
import pyarrow as pa
from penca_client import Mutation, PencaClient

client = PencaClient.from_settings()

SCHEMA = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("customer", pa.utf8()),
    pa.field("total", pa.int64()),
])

tx = client.begin_tx(catalog_name="public", branch_name="main", author="demo")
client.write_data(
    tx.tx_uuid,
    Mutation(
        table_name="orders",
        upserts=pa.table(
            {"order_id": [3], "customer": ["hopper"], "total": [900]}, schema=SCHEMA
        ),
    ),
    catalog_name="public",
    schema_name="shop",
    branch_name="main",
)
client.commit_tx(tx.tx_uuid, catalog_name="public", branch_name="main")

# All three rows: ada and grace from SQL, hopper from gRPC.
print(client.read_data(
    catalog_name="public", schema_name="shop", branch_name="main", table_name="orders"
))
```

Every write is an upsert; the unified upsert log means there is no client-side
insert-versus-update distinction. `read_data` resolves the latest committed version per
row and applies tombstones; `audit_data` returns the version history instead. To build
the whole hierarchy over gRPC instead of SQL (`create_catalog` → `create_schema` →
`create_table`), see `examples/audit_demo.py`, which suffixes its catalog name with a
random hex string so repeat runs do not collide.

## Connecting DataGrip

DataGrip ships no Arrow Flight SQL driver, so register Apache's as a custom driver.

1. **Get the driver jar.** Download `flight-sql-jdbc-driver` from Maven Central
   (`org.apache.arrow:flight-sql-jdbc-driver`). It is a shaded, self-contained jar with
   no extra dependencies to add.
2. **Register it.** DataGrip → **Database Explorer** → **+** → **Driver**. Under
   *Driver Files*, add the jar. Set **Class** to
   `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver` (the class registered in the
   driver's `META-INF/services/java.sql.Driver`). Set the **URL template** to
   `jdbc:arrow-flight-sql://{host}:{port}/?<comma-free params>`.
3. **Create the data source** from that driver and point it at the local stack:

   ```
   jdbc:arrow-flight-sql://localhost:50060/?useEncryption=false
   ```

   `useEncryption` defaults to **true**, and the shipped stack serves plaintext, so
   this parameter is required; without it the connection fails at the TLS handshake.
   Parameter names are case-sensitive. Leave user/password empty: Penca does not
   authenticate today; no auth interceptor, no TLS, and the Flight SQL handshake is
   unimplemented (see
   [Current shortcomings](../README.md#current-shortcomings)).
4. **On JDK 9+**, add `--add-opens=java.base/java.nio=ALL-UNNAMED` to the driver's VM
   options, which the Arrow driver requires for off-heap buffer access.

The full parameter list (`user`, `password`, `token`, `threadPoolSize`, `trustStore`,
`tlsRootCerts`, mTLS options, …) is in the
[Arrow Flight SQL JDBC driver docs](https://arrow.apache.org/docs/16.0/java/flight_sql_jdbc_driver.html).
Unrecognized parameters are forwarded to the server as gRPC headers.

> These steps are written against the driver's documented URL scheme and its registered
> driver class, but have not been executed against a DataGrip install. Corrections
> welcome.

## Examples

Everything under `examples/` runs against a `just penca-up` stack with the
client env sourced, and nothing else. One composite story, then a family of
single-feature scripts you can read end to end in a minute:

| Script | Shows |
|---|---|
| `examples/sandbox_demo.py` | The flagship. Fork a branch per agent, transact on each in place, compare them, throw them away: prod untouched. |
| `examples/oltp_demo.py` | Fetching one row out of a large table on cold columnar storage, timed over the gRPC client and over Flight SQL: both of which resolve to the same keyed read. |
| `examples/audit_demo.py` | Version history and time travel on one table: `read_data`, `audit_data`, and reading the table as it was at an earlier commit. |

Each is standalone and copy-pasteable; they deliberately repeat their setup
rather than sharing a helper module, so you can lift one file and run it.

### `examples/sandbox_demo.py`: a disposable sandbox per agent

**Give each agent its own copy of production, then throw it away.**

Three agents need to try three different strategies against the same live data.
You do not want three copies of the database, you do not want them touching prod,
and you do want to compare what they actually did. So fork a branch per agent:
each one reads and writes real committed state, in place, isolated from the
others, and none of them copied any data to get it.

The demo seeds a `prod` catalog with ad creatives and a running conversion tally,
forks three branches off `main`, and drives **one** shared, deterministic visitor
feed through all three. Each visitor's response to each creative is fixed up
front, so the branches see identical traffic and can only diverge on what they
*do* with it, which is what makes the final scoreboard a fair comparison of
strategies rather than of luck.

Each agent's loop is the shape agentic work actually takes: read the current
state, decide, write, repeat, with each round reading back the writes it *committed*
a moment ago, on the same copy it is transacting against. (Committed, not
uncommitted: the read is taken before the transaction opens. Nothing here relies
on reading your own dirty writes.) That feedback loop is the thing you cannot get
from a read replica or a nightly extract.

The round loop is **ordinary SQL over Flight SQL**. Each branch is one
connection, and branch selection binds at handshake and is immutable for the
connection's lifetime, the way a Postgres connection is to one database. So a
branch is reachable as a plain SQL endpoint, and these are the statements any
Flight SQL driver would send:

```sql
-- 1. read this branch's own committed tallies (read-your-writes,
--    on the same copy it is about to transact against)
SELECT creative_id, impressions, conversions FROM prod_a1b2c3d4.ads.creatives;

-- 2. the allocation policy picks creatives from what it just read, then:
BEGIN;
INSERT INTO prod_a1b2c3d4.ads.creatives (creative_id, headline, impressions, conversions)
VALUES ('carousel', 'One copy of your data. Both workloads.', 425, 94)
ON CONFLICT (creative_id) DO UPDATE
  SET impressions = EXCLUDED.impressions, conversions = EXCLUDED.conversions;
INSERT INTO prod_a1b2c3d4.ads.impressions (visitor_id, creative_id, converted)
VALUES ('v000401', 'carousel', 1), ('v000402', 'carousel', 0);
COMMIT;
```

The tally upsert and the log append share one transaction, so the two can never
disagree.

Setup; creating the catalog, the tables and the three forks, uses the gRPC
client, because forking pins to the seed's `commit_seq_num` and SQL does not
hand that back. Everything in the loop above is SQL.

Every branch reads its tallies each round; the tally is cumulative, so writing it
is a read-modify-write. `even` is the foil because it ignores *what the read said*,
splitting on the visitor index alone; `greedy` and `epsilon` reallocate from their
own running results. Then a
cross-branch scoreboard ranks all three, `delete_branch` throws every fork away,
and `main` is shown untouched. One run, measured 2026-07-27 at the shipped
defaults (3000 impressions, 25 per transaction, epsilon 0.15, seed
20260727); the run
reproduces, but nothing pins these particular figures, so treat them as a dated
transcript rather than a contract:

| branch    | impressions | conversions | rate   |
|:----------|------------:|------------:|:-------|
| `greedy`  |        3000 |         417 | 13.90% |
| `epsilon` |        3000 |         383 | 12.77% |
| `even`    |        3000 |         307 | 10.23% |

Both reading policies beat the fixed split, because both steer on what they wrote,
and neither finds the genuinely best creative. `greedy` and `epsilon` each
converge on `story` (true rate 0.14) rather than `carousel` (0.22); `epsilon` spends
158 of its 3000 impressions on `carousel` and still does not switch, and its extra
exploration costs it slightly against pure `greedy`. That is what toy policies look
like, and it is the honest version of the claim: the read-your-writes loop is what
separates these branches from the foil, not the quality of the allocator. How fast
greedy commits is also partly an artifact of `--round-size`, which sets decision
granularity as well as write granularity; a round's picks are all evaluated
against the read taken at its start.

**Forking does not copy your row data.** Measured on the seeded `creatives`
table: after the three forks, `main` holds its rows in **exactly one** cold object,
unchanged by the second and third fork, and each branch owns **zero** cold objects
of its own, while all three read the full seeded set. (`create_branch` does copy
per-branch *metadata* (schema and table entries) by design; what it never copies
is the rows.)
Those are the assertions in
`../tests/integration/integration_sandbox_demo_test.py::test_forks_share_one_copy_of_the_seeded_data`,
and they are the "one copy" half of the headline. (Penca records an in-memory Arrow footprint per segment rather than the
object's size on disk, so there is no stored-byte figure to quote here; the
load-bearing claim is the object count.)

Two honest caveats. The allocation policies are deliberately toy; the database
mechanic is the point, not the bandit. And at this scale the fork itself is the
hook: reading your transactional writes back *analytically* only outruns a
row-store at real volume or on a query shape a row-store chokes on, which is not
what a 3000-impression demo shows.

### `examples/oltp_demo.py`: a point lookup that stays a point lookup

**One row out of a hundred thousand, straight out of columnar files.**

```bash
uv run python examples/oltp_demo.py    # same sourced env, no extra setup
```

A columnar layout is built for scans, so the fair question to ask of a lakehouse
is what happens to a single-row primary-key lookup once the data has left the
hot tier. The script seeds a table and drives it all the way cold (persist,
snapshot, **and purge**, because persist leaves the rows physically in hot and
the plan attaches a hot arm while any remain; purge is the delete that makes the
read all-cold), and only then times the lookup.

It fetches the row two ways, and they converge. The gRPC arm sends `ids=`, a
primary-key restriction the engine resolves to a row identity. The SQL arm sends
`WHERE account_id = …` over Flight SQL, and the gateway extracts that
primary-key equality into the **same** `ids` restriction; the `WHERE` fragment
is then not pushed with the read, so nothing evaluates it over the columnar
files, so both arms land on the same keyed read. Neither scans. What the SQL
arm pays on top is parsing and planning on each execution, plus the driver's
extra round trips.

**No figures are printed here on purpose.** Every number the demo shows is
measured on your machine when you run it; hardware, container limits and object
store all move it. Run it and read your own.

### `examples/audit_demo.py`: time travel and the audit trail

```bash
uv run python examples/audit_demo.py    # same sourced env, no extra setup
```

`audit_demo.py` walks through Penca's auditable-store semantics on a
fresh `users(name PK, value)` table:

1. Three transactions on `main`: `insert(alice, bob)` →
   `upsert(alice=99, charlie)` → `delete(bob)`.
2. **`read_data`**: current state (alice=99, charlie=30; bob gone).
3. **`audit_data`**: full version history including the
   tombstone for bob; `audit_data(after=tx1)` shows only the post-tx1
   diff.
4. **`read_data(as_of=tx1)`**: time-travel back to alice=10, bob=20
   before the upsert + delete landed.

The same flow expressed as SQL through Flight SQL is the same wire
calls under the hood. DML translates to `WriteService.WriteData`,
SELECT goes through the merge-on-read planner. See
[Connecting](#connecting).
