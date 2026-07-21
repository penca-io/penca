# 0006 — SQL DML stays out of the write microservice

- **Status:** Accepted
- **Date:** 2026-04-21
- **Ticket:** [CHA-121](https://linear.app/chapala/issue/CHA-121)
- **Supersedes:** Decisions 2 and 3 of [ADR 0004](0004-sql-dml-via-flight-sql.md). Decisions 1 (intercept at Flight SQL) and 4 (Python client surface) of ADR 0004 still hold.
- **Related:** [ADR 0001](0001-unified-upsert-log.md) (the storage primitive every mutation here appends to); [ADR 0005](0005-colocated-microservices-perf-boundary.md) (the colocation assumption that makes the orchestration cost of this design tolerable); [CHA-122](https://linear.app/chapala/issue/CHA-122) (SQL transaction control consumes the `transaction_id` seam unchanged from ADR 0004); [CHA-141](https://linear.app/chapala/issue/CHA-141) (the advisory-lock pattern used here).

## Context

ADR 0004 paired the new SQL DML surface with eight new "single-op" RPCs on
the WriteService: `InsertData` / `UpsertData` / `UpdateData` / `DeleteData`,
each in tx-attached and one-shot `AndCommitTx` flavours. The motivation was
strict serializability — running the strict-INSERT collision check, or the
WHERE/SET resolution for UPDATE/DELETE, inside the same Postgres transaction
that appends to `upsert_log`. penca-sql-server became a thin verb-to-RPC
translator; the WriteService grew SQL-ish responsibilities (a `where_sql`
predicate field on `UpdateData`/`DeleteData`, server-side merge-on-read for
collision checks).

Two things changed during implementation that turned the trade against
that shape.

1. **The colocated-microservices boundary became explicit.** [ADR 0005](0005-colocated-microservices-perf-boundary.md)
   recorded that cross-service gRPC hops are negligible — the perf
   boundary worth optimizing is between any microservice and Postgres /
   object storage. Once that's stated, the strongest argument for the
   single-op RPCs ("eliminate one round trip from SQL server to write
   service for the read-then-write") loses force. penca-sql-server can
   issue three internal RPCs (admin → query → write) to satisfy a single
   SQL statement and the cost is dominated by the Postgres work, not by
   the gRPC plumbing.

2. **The "thin write microservice" property turned out to be the more
   important invariant.** Pulling SQL parsing, WHERE-fragment dialect
   translation, and merge-on-read into the WriteService meant it had to
   carry a DataFusion session context, a `MetadataClient`, and the full
   read pipeline (`penca_merge::merge_read`, the cold reader, the
   exclusion-set planner). It was no longer a write microservice — it
   was a second read+write microservice that happened to expose write
   verbs. The WriteService dependency graph started looking exactly like
   the QueryService dependency graph, modulo a few RPC names.

The reversal here moves all DML orchestration up into penca-sql-server,
keeps the WriteService as a pure "append + commit" layer with no PK
metadata or read-path code, and uses a small Postgres advisory lock in
penca-sql-server to recover the serializability that decision 3 of ADR
0004 was buying through transactional colocation.

## Decision

### 1. One write RPC: `MutateData` with optional `tx_uuid`

The WriteService surface collapses to a single mutation RPC,
mode-switched on `tx_uuid`:

| `tx_uuid` | Semantics |
| -- | -- |
| unset    | **Auto-commit.** Server opens a tx, appends to `upsert_log` / `delete_log`, commits — all inside one Pg transaction, skipping `begin_tx_log`. Returns the new `Tx`. `author` and `comment` are tx metadata. |
| set      | **Append** to that already-open `tx_uuid`. Pure append; no PK validation, no WHERE resolution, no read pipeline. Caller still calls `CommitTx` to finalize. |

`author` / `comment` must be unset when `tx_uuid` is set; the handler
returns `INVALID_ARGUMENT` otherwise.

The eight single-op RPCs introduced by ADR 0004 (`InsertData` /
`UpsertData` / `UpdateData` / `DeleteData` × {tx-attached, AndCommitTx})
were deleted in CHA-121. penca-sql-server is the only consumer that
ever needed them, and it now constructs `Change` payloads directly.

The original two-RPC pair (`MutateData` + `MutateDataAndCommitTx`)
documented in earlier revisions of this ADR was further collapsed in
[CHA-152](https://linear.app/chapala/issue/CHA-152): Flight SQL's
`CommandStatementUpdate.transaction_id` already mode-switches on
presence (empty = auto-commit, set = within-tx), and the SQL server's
two-arm dispatch on it became dead weight once both modes lived behind
the same RPC.

### 2. The WriteService derives `row_uuid` and `version_uuid` server-side

The Arrow IPC bytes in `Change.upserts` carry only user-shape columns.
The WriteService looks up the table's primary keys via `MetadataClient`
(on the same Postgres driver it already uses for the `upsert_log`
append), derives `row_uuid` deterministically via
`naming::row_uuid_for_pk(table_uuid, pk_values)` (xxh3_128), and mints
a fresh `version_uuid` (UUIDv4) per row before INSERT.

An earlier version of this ADR flipped derivation to the caller on the
grounds that it would drop the WriteService's only remaining metadata
dependency. That was walked back before ship: `version_uuid` carries a
`UNIQUE` constraint in Postgres, and mint-at-caller means any at-least-
once retry (gRPC retry interceptor, client reconnect, user-level retry
after an ambiguous error) collides the constraint and surfaces as a
hard write failure instead of a silent dedupe or a clean idempotent
replay. Server-side minting keeps each attempted write getting a fresh
`version_uuid`, which is the behaviour every caller actually wants.
The metadata lookup cost is one extra `MetadataClient::get_table` call
per `Change` against the WriteService's own pool — small under
[ADR 0005](0005-colocated-microservices-perf-boundary.md).

### 3. penca-sql-server orchestrates the read-then-write itself

For each verb:

- **Strict INSERT.** Decode the source rows, derive `row_uuid` per row,
  call `QueryService::read_data` with `filter = "l.row_uuid IN (...)"`
  to check for collisions. If any row comes back, return
  `ALREADY_EXISTS`. Otherwise call `MutateData` with the
  `Change.upserts` payload.
- **`INSERT ... ON CONFLICT DO UPDATE` (LWW upsert).** Same payload
  construction as strict INSERT, but skip the collision check. Append
  directly.
- **UPDATE.** Run a `SELECT *` against `QueryService::read_data` with
  the WHERE predicate, applying SET expressions in-line via DataFusion
  (`SELECT col1, (set_expr) AS col2, col3, ... FROM t WHERE pred`).
  Materialize the patched batch, attach `row_uuid` / `version_uuid`,
  ship via `MutateData`.
- **DELETE.** `SELECT pk_columns FROM t WHERE pred` via
  `QueryService::read_data`, derive `row_uuid` per row, ship as
  `Change.deletes`.

`CommandStatementUpdate.transaction_id` is passed straight through
into `MutateDataRequest.tx_uuid` (empty = auto-commit, set = append to
that open tx); no two-arm dispatch in the SQL server.

### 4. Strict-INSERT runs under a per-(branch, table) Postgres advisory lock in penca-sql-server

Decision 3 of ADR 0004 got serializability for free by colocating the
collision-check `SELECT` and the `upsert_log` append inside one Postgres
transaction at the WriteService. Splitting them across two gRPC calls
(QueryService then WriteService) opens a window where two concurrent
strict-INSERTs against the same PK can both run their `SELECT`, both
see no collision, and both append — defeating the strict semantics.

To recover serializability without re-coupling the services, penca-sql-server
acquires a Postgres advisory lock for the strict-INSERT critical
section (collision check + write call) using the existing
`PgDriver::advisory_lock(key, body)` pattern from
`penca-api/src/lifecycle.rs` (CHA-141). Lock key:

```
dml:strict-insert:{branch_name}:{table_uuid}
```

The lock is held only across the QueryService + WriteService calls;
LWW upserts, UPDATEs, and DELETEs run unlocked (LWW has no
serialisation requirement, and UPDATE/DELETE inherit Postgres's
READ COMMITTED-equivalent semantics — a concurrent writer can land
between our SELECT and our write, but the result is consistent with
*some* serial order).

The cost is one extra Postgres round-trip per strict INSERT (the
`pg_advisory_lock(1, hashtext(key))` call on the orchestrator pool).
Acceptable under [ADR 0005](0005-colocated-microservices-perf-boundary.md) —
the orchestrator pool sits next to penca-sql-server, and the perf
boundary worth optimizing is the WriteService's own `upsert_log` append,
not the orchestrator-side coordination.

### 5. penca-sql-server gains a small Postgres pool used only for orchestration

A new `DATABASE_URL` / `PG_POOL_MIN` / `PG_POOL_MAX` triple wires a
`PgDriver` into `FlightSqlService` purely to host the advisory lock.
No data plane traffic flows through this pool — catalog discovery,
query execution, and write dispatch all stay on gRPC. The pool is
sized to the maximum number of concurrent strict-INSERT lock holders
the gateway expects to serve (today: small; the limiting factor is
the strict-INSERT call itself, which holds the lock for the duration
of a QueryService call + a WriteService call).

The config struct documents this narrow purpose explicitly so
future contributors don't try to use the pool as a general-purpose
data-access channel from the gateway.

## Rationale

### Why move DML orchestration out of the WriteService

The colocated-microservices ADR (0005) reframed what "cheap" means for
us. ADR 0004's decision 3 paid a complexity tax — DataFusion +
merge-on-read + SQL fragment translation in the write path — to save a
single internal gRPC hop. With ADR 0005 in place, that gRPC hop is no longer
a target for optimization. The complexity tax has no offsetting win.

The downstream effect is bigger than the LOC count suggests. With DML
out of the WriteService, the write microservice's dependency graph
shrinks dramatically: no `penca-merge`, no `penca-dl`, no read-path
config (no `OBJECT_STORAGE_*`, no `ColdStorageClient`, no segment-scan
parameters). It becomes the only microservice in the system whose job
description fits in one sentence: "open transactions, append to
`upsert_log` / `delete_log`, commit transactions". That property is
worth a Postgres roundtrip per strict-INSERT.

### Why row_uuid and version_uuid stay server-derived

The alternative considered (and briefly implemented before being walked
back) was to ship `row_uuid` / `version_uuid` as caller-populated
columns inside the upsert IPC. Two problems killed it:

1. **`version_uuid` has a `UNIQUE` constraint on `upsert_log`.** Any
   client-side mint that survives a retry — and gRPC interceptor
   retries, transient reconnects, and ambiguous-error replay are all
   normal — will collide the constraint on the second attempt. The
   server has no clean way to disambiguate "duplicate because retried"
   from "duplicate because client sent the same UUID twice on purpose,"
   so the safe response is to surface the constraint violation as a
   hard error. Server-side mint sidesteps the entire class: each
   physical INSERT attempt gets a fresh UUID and idempotency lives
   above this layer (`tx_uuid` semantics).
2. **The "remove the WriteService's last metadata dependency" win
   doesn't survive the ADR 0005 framing.** With cross-service hops
   priced at near-zero, paying one `MetadataClient::get_table` call per
   `Change` (against the WriteService's own pool, where the
   `MetadataClient` already lives for other reasons) is a wash on
   latency. The WriteService keeps its narrow surface — open tx,
   append, commit — and adds back exactly one read of static catalog
   data per write batch. That's a much smaller dependency than
   "DataFusion + merge-on-read + cold reader" that decision 1
   (collapsing back to two RPCs) was specifically designed to remove.

The IPC stays user-shape only: one less coupling between caller and
storage layout, and no risk of the IPC encoding drifting from the
table's PK definition (e.g. after a future PK change).

### Why a Postgres advisory lock instead of an optimistic-CC precondition

Two alternatives were considered and rejected for CHA-121's scope:

**A — Optimistic concurrency control on `MutateData`.** Add a
precondition field that asserts "no row with these `row_uuid`s exists
at commit time." The WriteService re-runs the collision check inside
the commit transaction, fails the commit if it loses. This is
strictly more parallel than the lock (no contention on the
non-conflicting case), but it adds a new RPC field, a new error code,
and re-introduces a piece of read-path machinery into the WriteService.
Tracked as a follow-up under [CHA-86](https://linear.app/chapala/issue/CHA-86)
(consistent read snapshot) territory.

**B — A Postgres `UNIQUE(row_uuid)` index on `upsert_log`.** Cleanest
correctness story (the database enforces it), but it doesn't work for
the unified `upsert_log` — every UPDATE appends another row with the
same `row_uuid`, so the constraint would reject legal updates. Noted
as trigger #4 in [ADR 0001](0001-unified-upsert-log.md); requires
re-splitting the log to revisit.

The advisory lock is the smallest pattern that recovers serializability
without changing the storage layout or the RPC surface. It also reuses
the existing `PgDriver::advisory_lock` machinery (CHA-141) the
lifecycle path already runs against — same `pg_advisory_lock(1,
hashtext(key))` on a dedicated pooled connection, same
`AdvisoryLockGuard` for panic safety. Zero new infrastructure.

### Why per-(branch, table) lock granularity

The lock guards the "no other strict-INSERT against this PK lands
between my SELECT and my append" invariant. The PK lives in a single
table on a single branch — no cross-table or cross-branch contention is
possible. Per-(branch, table) is therefore the tightest key that's
still correct.

A coarser key (per-branch, per-database) would unnecessarily
serialise unrelated strict-INSERTs. A finer key (per-(branch, table,
row_uuid) bucket) would be correct but requires hashing the input
batch and acquiring N locks per call — extra round-trips and lock-
ordering complexity for no observed contention.

### Why LWW upserts and UPDATE/DELETE skip the lock

- **LWW upserts** have no serialisation requirement by definition.
  Concurrent upserts on the same `row_uuid` produce two `upsert_log`
  appends; the merge-on-read pipeline picks the later `committed_at`
  on the read side, which is exactly the contract.
- **UPDATE and DELETE** inherit READ COMMITTED-equivalent semantics:
  a concurrent writer can land a change between the SELECT (that
  resolves the WHERE) and the MutateData call, and the resulting
  `upsert_log` content reflects an interleaving consistent with *some*
  serial order. This is the same guarantee Postgres's READ COMMITTED
  isolation gives, and is sufficient for the common SQL UPDATE / DELETE
  workload. Strict serializability on UPDATE/DELETE is out of scope —
  a follow-up would either combine with [CHA-86](https://linear.app/chapala/issue/CHA-86)
  (consistent read snapshot for the SELECT side) or extend the
  advisory-lock approach to cover the same critical section.

## Trigger conditions to revisit

Re-evaluate **any** of these decisions if:

1. **The advisory-lock round-trip dominates strict-INSERT latency at
   production volumes.** Today profiling shows the QueryService and
   WriteService calls dwarf the lock acquisition. If that flips —
   e.g. lots of zero-row strict-INSERTs whose collision check returns
   instantly — switch to the optimistic-CC approach (Alternative A
   above) and reclaim the lock round-trip.
2. **A new internal consumer needs strict-INSERT semantics
   programmatically (not via SQL).** Today the eight single-op RPCs are
   gone because the only consumer was penca-sql-server. If a Python or
   Rust programmatic client grows the same need, the right answer is
   not to bring back the eight RPCs — it's to expose a thin
   strict-INSERT wrapper at the *client* layer that does the same
   collision-check + advisory-lock + `MutateData` dance penca-sql-server
   does. The lock must be acquired on the same Postgres instance as the
   write, so the wrapper would either need its own pool or call back
   through penca-sql-server.
3. **DataFusion's planner gains pushdown into the WriteService that
   would benefit from physical-plan-integrated DML.** Same trigger #1
   from ADR 0004 — if the planner can fuse a `SELECT` + INSERT
   round-trip into one node, the read-then-write split penca-sql-server
   does today becomes the bottleneck and pushes back toward the ADR
   0004 shape (DML inside the planner, write service grows
   read-pipeline knowledge).
4. **`UPDATE`/`DELETE` workloads hit anomalies from READ COMMITTED-
   equivalent semantics.** The current behaviour is "concurrent writer
   may interleave between our SELECT and our write, like Postgres
   READ COMMITTED." If users start seeing lost updates or non-
   repeatable read symptoms, extend the advisory-lock pattern to cover
   the UPDATE/DELETE critical section, or escalate to a CHA-86-style
   consistent read snapshot.

## Related tickets

- [CHA-121](https://linear.app/chapala/issue/CHA-121) — this decision
  and the implementation that landed it.
- [ADR 0004](0004-sql-dml-via-flight-sql.md) — the prior decision that
  this one partially supersedes. Decisions 1 and 4 of 0004 still hold.
- [ADR 0005](0005-colocated-microservices-perf-boundary.md) — the
  colocation assumption that makes paying a Postgres round-trip per
  strict INSERT a sensible trade.
- [ADR 0001](0001-unified-upsert-log.md) — the unified `upsert_log`
  this decision appends to. Trigger #4 there is the cross-link for
  when strict-INSERT validation cost becomes the reason to revisit
  the unified-log choice.
- [CHA-141](https://linear.app/chapala/issue/CHA-141) — the
  `PgDriver::advisory_lock` pattern this decision reuses.
- [CHA-86](https://linear.app/chapala/issue/CHA-86) — consistent read
  snapshot; potential follow-up for tightening UPDATE/DELETE
  semantics beyond READ COMMITTED-equivalent.
- [CHA-122](https://linear.app/chapala/issue/CHA-122) — SQL transaction
  control; consumes the unchanged `transaction_id` routing seam.
