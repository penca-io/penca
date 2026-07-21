# 0004 — SQL DML via Flight SQL (INSERT / UPDATE / DELETE)

- **Status:** Partially superseded by [ADR 0006](0006-sql-dml-out-of-write-microservice.md). Decision 1 (intercept at Flight SQL) and decision 4 (Python client surface) still hold. Decisions 2 (single-op WriteService RPCs) and 3 (server-side WHERE resolution) are reversed there in light of the colocated-microservices boundary (ADR 0005) and CHA-121 implementation experience. As of [CHA-152](https://linear.app/chapala/issue/CHA-152) the two-RPC dispatch on `CommandStatementUpdate.transaction_id` is also gone — `MutateData` and `MutateDataAndCommitTx` were collapsed into one `MutateData` RPC with `optional tx_uuid`, so penca-sql-server now passes `transaction_id` straight through.
- **Date:** 2026-04-21
- **Ticket:** [CHA-121](https://linear.app/chapala/issue/CHA-121)
- **Related:** [CHA-122](https://linear.app/chapala/issue/CHA-122) (SQL transaction control consumes the `transaction_id` seam this decision establishes); [CHA-134](https://linear.app/chapala/issue/CHA-134) (unified `upsert_log`, the storage primitive this builds on); [ADR 0006](0006-sql-dml-out-of-write-microservice.md) (the reversal of decisions 2 and 3); [CHA-152](https://linear.app/chapala/issue/CHA-152) (collapse of the `MutateData`/`MutateDataAndCommitTx` pair)

## Context

Today OLTP writes go through the structured mutation gRPC API (`BeginTx` →
`MutateData` → `CommitTx`, or the one-shot `MutateDataAndCommitTx`). Reads go
through Flight SQL — a SQL query lands in `FlightSqlService::do_get_fallback`,
which drives `QueryService::read_data` via `PencaTableProvider`. A client
connecting to the Flight SQL endpoint can read but cannot write; to issue an
INSERT, UPDATE, or DELETE it has to switch protocols to the mutation gRPC.

CHA-121 adds SQL DML over Flight SQL so one SQL client (DBeaver, ADBC, notebook,
ORM) can do both reads and writes against the same endpoint. The mutation gRPC
stays as the programmatic primitive; SQL DML is a convenience layer over it.

Three design forks arose while scoping the ticket. This ADR records the
decisions taken for each so the implementation has a single reference and
future contributors know why the shape looks the way it does.

## Decision

### 1. All SQL DML intercepts at the Flight SQL boundary

The `do_put_statement_update` handler in `crates/penca-sql-server/src/flight_sql/service.rs`
is the single entry point for all four DML verbs. It parses the SQL via
DataFusion's planner (`ctx.state().create_logical_plan(sql)`), matches on
the resulting `LogicalPlan::Dml`, and dispatches to one of the single-operation
WriteService RPCs defined in decision 2 below.

`PencaTableProvider` stays a read-only abstraction. We do **not** implement
`TableProvider::insert_into` on it, and we do **not** plumb a write-service
`Channel` through the `PencaCatalogProviderList → CatalogProvider →
SchemaProvider → TableProvider` chain.

`INSERT ... SELECT` is handled by lifting the SELECT subtree out of the
DataFusion `LogicalPlan::Dml` node and streaming its batches through
`ctx.execute_logical_plan(input)` — the same machinery that DataFusion
would drive internally for `insert_into`, just invoked from our handler
instead of from a physical-plan node. Each batch feeds one
`InsertData`/`UpsertData` RPC.

### 2. Add single-operation DML RPCs on WriteService

Introduce four new RPC pairs on `WriteService`, each with a "tx-attached"
variant (requires an open `tx_uuid`) and a one-shot "`AndCommitTx`" variant
(creates + commits a tx in one round trip):

| RPC pair | SQL verb | Semantics |
| -- | -- | -- |
| `InsertData` / `InsertDataAndCommitTx` | `INSERT INTO t VALUES (...)` | strict: fail with `ALREADY_EXISTS` on PK collision |
| `UpsertData` / `UpsertDataAndCommitTx` | `INSERT ... ON CONFLICT DO UPDATE` | last-writer-wins, no existence check |
| `UpdateData` / `UpdateDataAndCommitTx` | `UPDATE t SET ... WHERE ...` | server-side WHERE resolution; atomic |
| `DeleteData` / `DeleteDataAndCommitTx` | `DELETE FROM t WHERE ...` | server-side WHERE resolution; atomic |

These are **internal RPCs** reserved for penca-sql-server. The Python and
Rust programmatic clients continue to expose only `MutateData` /
`MutateDataAndCommitTx` — the one-RPC bulk/multi-table atomic endpoint.
External clients that want strict-INSERT semantics with the programmatic
API will file a follow-up to promote one of the single-op RPCs to the
public surface; until then, programmatic callers get LWW via
`MutateData`.

`Change` is **unchanged** from today (ADR 0001). No `oneof`, no
`Inserts`/`Updates`/`Upserts` wrappers. `MutateData` remains the
programmatic multi-table atomic endpoint with its existing
"`upserts` (LWW bytes) + `deletes` (UUIDs)" shape.

`UpdateData` carries a `where_sql` fragment (a SQL WHERE predicate as a
string) and a `set: map<string, string>` whose values are SQL expressions
evaluable against each matched row. The write service resolves the
predicate via merge-on-read and applies the SET expressions inside one
transaction — no client-side row materialization, no round trip.
`DeleteData` has the same `where_sql` shape, no SET.

### 3. WHERE/SET resolution and strict-INSERT validation both run through merge-on-read inside the WriteService

All three verbs that need a pre-write read (`InsertData`'s PK
existence check, `UpdateData`'s WHERE, `DeleteData`'s WHERE) funnel
through the same `MetadataClient::plan` + `penca_merge::merge_read`
pipeline the query service uses. Hot-tier-only shortcuts would be
incorrect: a row persisted to cold must be as visible to strict-INSERT
as it is to `SELECT`, and a cold row matching a WHERE must be as
reachable to UPDATE/DELETE as to a read.

Correctness implications are handled by the existing merge machinery:
hot + cold union, tombstone shadowing, committed-tx visibility (the
current tx's pending writes are filtered out by the `committed_tx`
CTE, so strict-INSERT doesn't self-collide on the rows it just
appended). Performance comes from `merge_read`'s all-hot fast path —
if `plan.cold_storage` is absent, the cold resolve and snapshot
scans are no-ops, and the check collapses to one Postgres query.
OLTP workloads that haven't persisted pay the cost of a WHERE scan on
the unified `upsert_log`.

The verbs do three things inside one Postgres transaction:

1. Resolve the scope of the write via merge-on-read (`row_uuid IN (...)`
   filter for `InsertData`; `where_sql` for `UpdateData` / `DeleteData`).
2. For `UpdateData`, evaluate SET expressions against the matched batch
   to produce the patched rows.
3. Append the result to `upsert_log` (INSERT / UPDATE) or `delete_log`
   (DELETE), bound to the current tx's `tx_uuid`. On a collision for
   `InsertData`, return `ALREADY_EXISTS` and let the outer transaction
   roll back the uncommitted `upsert_log` rows.

This gives strict serializability for every verb against other
writers on the same branch (the merge-read snapshot and the log append
share a Postgres transaction; no window between them).
penca-sql-server is a pass-through: it translates the DML into the
appropriate single-op RPC and ships it in one round trip.

### 4. Python client exposes two methods: `execute_query` and `execute_update`

`PencaClient` surfaces Flight SQL through three thin wrappers rather than
a single polymorphic `execute`:

- `execute_query(sql) -> pyarrow.Table` — routes through
  `cursor.execute(sql)` → ADBC `execute_query` → `GetFlightInfo` + `DoGet`.
- `execute_stream(sql) -> Iterator[RecordBatch]` — same RPC path as
  `execute_query`, streams batches instead of materializing.
- `execute_update(sql) -> int` — drops to the low-level
  `cursor.adbc_statement.execute_update()` → `DoPutStatementUpdate`.

Callers pick the method that matches the SQL verb. A convenience
polymorphic entry point is deliberately not offered; see rationale below.

### Transaction-boundary hook for CHA-122

`do_put_statement_update` routes on `CommandStatementUpdate.transaction_id`:

- empty → `*DataAndCommitTx` (auto-commit)
- non-empty → `*Data { tx_uuid: transaction_id, ... }`

CHA-121 implements both code paths. CHA-122 will wire SQL `BEGIN` /
`COMMIT` / `ROLLBACK` through `do_action_begin_transaction` /
`do_action_end_transaction` to populate `transaction_id`. Penca's
`tx_uuid` (16-byte UUID) is the Flight SQL `transaction_id` — no
mapping table needed.

## Rationale

### Why intercept at the Flight SQL boundary instead of `TableProvider::insert_into`

DataFusion's `TableProvider::insert_into` is the idiomatic extension point
for INSERT, but it only covers INSERT. DataFusion has no matching
`update_into` / `delete_from` hook; UPDATE and DELETE either require a
custom extension planner or have to be intercepted at the SQL level
anyway. Routing INSERT through `insert_into` while routing UPDATE and
DELETE through the Flight SQL handler leaves the codebase with two
mental models for four verbs — one inside a DataFusion physical plan
node, the other at the Flight SQL handler.

Keeping all four verbs at one entry point means:

- One mental model, one handler, one dispatch table.
- `PencaTableProvider` stays purely read-only. No write-service `Channel`
  plumbed through four layers of catalog/schema/table providers.
- Per-verb code lives in one module (`flight_sql/dml.rs`) where it can be
  read and tested together.

### Why single-operation RPCs instead of reshaping `Change`

Two alternatives were considered and rejected.

**A — Reshape `Change` with a `oneof` for mutation semantics**
(`Inserts` / `Updates` / `Upserts` variants). This encodes strict-insert
vs strict-update vs LWW on the wire, but couples two separable concerns
into one proto: how to classify the rows *and* what to do with them
(particularly for UPDATE, where the client still has to materialize
patched rows, which means a read round-trip to resolve WHERE first).
The oneof version still leaves penca-sql-server doing a SELECT →
patch-in-memory → write round-trip for every UPDATE and DELETE — it
doesn't actually eliminate the read-then-write race that motivates
single-op RPCs in the first place.

**B — Ship CHA-121 with LWW-only semantics via today's `MutateData`.**
Rejected because the "new SQL write surface" (likely the highest-volume
write path once DBeaver, notebooks, and ORMs hit it) would inherit
silently-upserting semantics, baking the regression into tests, docs,
and client expectations before being unwound. ORM authors rely on
`IntegrityError` for duplicate-PK detection — silently turning that
into an upsert is a data-correctness issue users find in production.

Single-operation RPCs:

- **Eliminate the read-then-write race** for UPDATE and DELETE. The
  WHERE resolution and the log append share one Postgres transaction at
  the write service, so no concurrent writer can land between the two.
  This is meaningfully stronger than the READ COMMITTED-equivalent
  semantics the `Change`-oneof shape can give.
- **Make penca-sql-server a pure translator.** No SELECT round-trip,
  no batch-level patching, no coordination. Each SQL verb is one RPC.
- **Map SQL verbs 1:1 to RPCs.** The translator is a match statement,
  not a payload-construction pipeline.
- **Carry their own error surface.** `InsertData` returns
  `ALREADY_EXISTS` on PK collision; `UpdateData`/`DeleteData` return
  a count of affected rows (SQL-expected semantics, including zero).
  `MutateData` stays LWW-only and can't grow these distinct error modes
  without an enum + flag shape that's easy to misuse.

The cost is four additional RPC pairs on the WriteService. That cost
is bounded — they share implementation internals with `MutateData`
(the `insert_rows` helper, the delete-log append, the tx metadata
dispatch) and add no new validation path beyond the strict PK check,
which is a natural extension of existing merge-on-read machinery.

### Why these RPCs are internal-only

The four new RPC pairs exist to serve penca-sql-server. They encode
SQL-ish semantics that the programmatic gRPC API does not and should
not need. Exposing them on the public `WriteService` surface would
confuse the API:

- Eight write RPCs for five semantic operations (INSERT, UPDATE, UPSERT,
  DELETE, MULTI-TABLE-ATOMIC) is more surface than the programmatic
  client wants to navigate.
- The `WHERE` predicate is a SQL string. Forcing programmatic clients to
  build SQL fragments for what they could do with row-UUID lists
  (via `MutateData`) is a worse ergonomics trade for the programmatic
  case.
- Keeping the strict semantics internal means we can evolve the RPC
  shape (adding preconditions, extending the SET grammar) without a
  breaking-change tax on external clients.

Concretely: the Python and Rust programmatic clients (`client.py`,
`penca_client`) expose only `mutate_data` and
`mutate_data_and_commit_tx`. The new RPCs are not wrapped. A client that
genuinely needs strict-insert semantics programmatically files a
follow-up to promote `InsertData` to the public surface; the gRPC is
there, we just don't ship a wrapper until the use-case is validated.

### Why `UpdateData.set` is `map<string, string>` of SQL expressions

The SET grammar must support both literal-valued updates
(`SET status = 'active'`) and expression-valued updates
(`SET count = count + 1`). The simplest wire format that handles both
is a map of column-name → SQL expression string. Values are parsed by
the write service and either passed through to Postgres (for hot-tier
rows, where Postgres evaluates the expression against each matched
row in a single INSERT ... SELECT) or evaluated via DataFusion (for
cold-tier rows, which are already in-memory Arrow batches).

A structured-AST alternative was considered and rejected. DataFusion's
logical expression tree is rich, but every consumer on both sides of
the RPC has to translate to and from it. SQL strings round-trip
trivially through `exprs_to_where_fragment` on the client side and
through the write service's existing dialect layer on the server
side.

### Why the Python client splits `execute_query` / `execute_update`

The Arrow Flight SQL spec has separate RPCs for queries
(`GetFlightInfo` + `DoGet`) and updates (`DoPutStatementUpdate` /
`DoPutPreparedStatementUpdate`). A Flight SQL server must honour this
split — the update RPCs are the only ones that return a rowcount, and
the query RPCs are the only ones that return a result stream.

Drivers differ on who dispatches:

- **JDBC (Dremio's driver and others).** The driver parses the SQL
  client-side, sees `INSERT` / `UPDATE` / `DELETE`, and calls the
  update RPC. Callers invoke a single `executeUpdate` /
  `executeQuery` JDBC entry point and the driver picks the RPC.
  DBeaver on JDBC works transparently.
- **ADBC Go, Rust, C++.** Expose two explicit statement methods
  (`ExecuteQuery` / `ExecuteUpdate`). The caller picks the RPC.
- **ADBC Python.** The DB-API `cursor.execute(sql)` unconditionally
  calls the low-level `execute_query` → `GetFlightInfo` path, even
  for DML. To reach `DoPutStatementUpdate` you have to drop to the
  `cursor.adbc_statement.execute_update()` handle. There is no
  client-side SQL sniffer that picks the RPC for you.
- **InfluxDB 3.** Doesn't support DML over Flight SQL at all; tracked
  as a feature request upstream.

We could add a SQL sniffer to the Python `PencaClient` (the
JDBC-driver approach) so callers can write `client.execute(sql)` for
both verbs. Rejected: it's load-bearing magic that embeds a SQL parser
in the client wrapper to paper over an ADBC Python limitation, and it
fails opaquely on edge cases (leading whitespace, comments, vendor
extensions). Two named methods make the transport split legible at the
call site, match the spec's own two-RPC model, and let each method
call the right ADBC entry point with zero parsing. Callers that want
the JDBC experience (one method, client dispatches) can layer a
sniffer on top of these two methods locally.

## Trigger conditions to revisit

Re-evaluate **any** of these decisions if:

### Dispatch location (decision 1)

1. **DataFusion grows first-class UPDATE/DELETE TableProvider hooks.** If
   a future DataFusion release adds `update_into` / `delete_from` (or
   similar) methods on `TableProvider`, revisit decision 1 — the mental-
   model cost of splitting dispatch disappears, and going through
   DataFusion's physical plan would let UPDATE/DELETE participate in
   any cross-node optimizations the planner grows.
2. **A feature meaningfully benefits from sitting inside a physical plan
   node.** E.g. `INSERT INTO t_penca SELECT ... FROM t_penca ...` with
   write-path planner fusion (pushing the write down to the same node
   running the scan). Unlikely in the near term, but worth re-examining
   if such a feature comes up.

### Single-operation RPCs (decision 2)

3. **PK-existence scan for `InsertData` becomes a write-latency
   bottleneck at production volumes.** The validation step runs the
   full merge-on-read pipeline with a `row_uuid IN (...)` filter,
   relying on `merge_read`'s all-hot fast path (no cold segments → no
   DataFusion work) to keep pure-OLTP writes fast. Mitigations if the
   scan dominates write latency are listed under trigger #4 in
   [ADR 0001](0001-unified-upsert-log.md) — segment-level `row_uuid`
   statistics, a btree on `upsert_log.row_uuid`, or re-splitting into a
   physical `insert_log` with `UNIQUE(row_uuid)`. A UNIQUE constraint on
   the unified `upsert_log` itself is not viable (it would reject every
   UPDATE, since updates reuse `row_uuid`).
4. **A SQL variant shows up that needs a fifth mutation mode** (e.g.
   `MERGE`, `REPLACE`, conditional-by-column upserts). Add a new RPC
   pair; the dispatch table in `flight_sql/dml.rs` grows one branch.
5. **External programmatic clients start needing strict-INSERT
   semantics.** The RPCs are internal today; promoting `InsertData` to
   the public surface is a pure documentation + client-wrapper change.

### WHERE/SET resolution (decision 3)

6. **SET-expression grammar grows beyond per-row scalar expressions.**
   Window functions, subqueries, or cross-table references in SET would
   break the pass-through-to-Postgres-or-DataFusion model. Likely
   response: restrict the grammar at parse time and refuse the subset
   we don't support, rather than growing a structured AST.
7. **Cold-tier SET evaluation becomes a throughput bottleneck.** Today
   the write service materializes cold-tier matched rows, evaluates SET
   via DataFusion, and serializes. If that pipeline's memory cost or
   CPU cost dominates UPDATE throughput on cold-heavy tables, consider
   pushing evaluation into the cold reader so the patched batches
   stream through instead of materializing.

### Client dispatch split (decision 4)

8. **ADBC Python adds a client-side SQL-verb dispatcher** (i.e.
   `cursor.execute(sql)` starts routing DML through
   `DoPutStatementUpdate` without the caller dropping to
   `adbc_statement`). At that point `execute_update` collapses into
   `execute_query`, and the Python client can expose a single
   polymorphic `execute` to match the JDBC experience.
9. **A vendored SQL parser appears elsewhere in the client.** If the
   client grows a parser for unrelated reasons (e.g. client-side
   statement pre-processing, macro expansion), the cost of also using
   it to sniff DML verbs drops to near zero — at which point the
   JDBC-driver pattern becomes the right trade.

## Related tickets

- [CHA-121](https://linear.app/chapala/issue/CHA-121) — this decision.
- [CHA-122](https://linear.app/chapala/issue/CHA-122) — SQL transaction
  control; consumes the `CommandStatementUpdate.transaction_id` routing
  seam established here.
- [CHA-134](https://linear.app/chapala/issue/CHA-134) — unified
  `upsert_log`. The storage primitive every mutation RPC here appends
  to.
- [CHA-119](https://linear.app/chapala/issue/CHA-119) — per-session
  branch binding. DML currently inherits the hardcoded `"main"` from
  `penca-sql-server`'s `main.rs`; CHA-119 replaces that with a client-
  controlled session property.
- [ADR 0001](0001-unified-upsert-log.md) — the storage decision this
  decision builds on. Trigger #4 there is the cross-link for when
  strict-INSERT validation cost becomes the reason to revisit the
  unified-log choice.
