# 0010 — Route `open_tx_uuid` through `SessionConfig` extensions; gate transactional DDL to gRPC

- **Status:** Accepted
- **Date:** 2026-05-02
- **Ticket:** [CHA-170](https://linear.app/chapala/issue/CHA-170)
- **Related:** [CHA-169](https://linear.app/chapala/issue/CHA-169) (connection-scoped catalog pin); [CHA-164](https://linear.app/chapala/issue/CHA-164) (transactional table DDL); [CHA-112](https://linear.app/chapala/issue/CHA-112) (RYOW reads); [ADR 0007](0007-session-entity.md) (sessions are connection-local).

## Context

CHA-169 shipped per-session catalog pinning by baking `(session_catalog_uuid, open_tx_uuid)` into `PencaCatalogProviderList` as constructor fields. Because `open_tx_uuid` toggles within a session (BEGIN / COMMIT / ROLLBACK), the catalog tree had to be rebuilt per request, which forced a per-request `SessionState` rebuild via `SessionStateBuilder::new_from_existing(template.clone()).with_catalog_list(...).build()` — ~30–100µs of HashMap-shuffling per request.

This ADR replaces that routing. Two options were on the table; both are recorded here so a future revisit doesn't have to rederive them.

## Options considered

### Option A — per-session `SessionState` + shared `Arc<TxPinCell>` on schema/table providers

Cache one `SessionState` per `session_uuid` in `SessionCache`. Catalog tree is per-session, structurally stable for the session's lifetime. `open_tx_uuid` lives in a typed cell shared by `Arc::clone` between the `Session` row and the schema/table providers under the catalog tree:

```
Session in SessionCache:
  catalog_name, catalog_uuid (immutable for session lifetime)
  tx_pin: Arc<TxPinCell>          // RwLock<Option<String>> or ArcSwap; shared with providers below
  state: Arc<SessionState>         // built once at session mint, reused per request

The catalog tree inside that SessionState:
  PencaCatalogProviderList { session_catalog_uuid }
    PencaCatalogProvider
      PencaSchemaProvider { tx_pin: Arc<TxPinCell> }   // CHA-164: tx-aware metadata reads
        PencaTableProvider  { tx_pin: Arc<TxPinCell> } // RYOW data reads + CHA-164
```

**Lifecycle:** session mint allocates the cell + builds the tree; BEGIN writes `Some(tx_uuid)` to the cell; mid-tx requests read the cell from `PencaSchemaProvider::table()` (for tx-aware admin RPCs) and `PencaTableProvider::scan()` (for the RYOW pin in `ReadDataRequest`); COMMIT/ROLLBACK clears the cell; idle eviction drops everything.

**Why the pin must live on the providers, not just `scan()`:** once CHA-164 ships transactional table DDL, table metadata reads need to see the tx's own DDL. `SchemaProvider::table(&self, name: &str)` has no `Session` parameter in its trait signature, so the pin must be reachable from `&self`. `Arc<TxPinCell>` is the smallest mechanism that satisfies that constraint.

**Supports transactional DDL through Flight SQL.**

**Tradeoffs:** interior mutability shared between handlers and providers; catalog-tree provider constructors gain a field (touches every test fixture); each session owns its own catalog tree (memory footprint scales with active sessions × tree size); `RwLock` vs `ArcSwap` becomes a dep choice.

### Option B — `SessionConfig` extension; transactional DDL gated to gRPC (chosen)

Catalog tree is built **once at server startup**, shared by `Arc::clone` across every session, structurally immutable. Per-session state (`session_catalog_uuid`, `open_tx_uuid`) flows via DataFusion's typed extensions on `SessionConfig`:

```rust
#[derive(Debug, Clone)]
pub struct SessionPin {
    pub catalog_uuid: String,         // session-pinned at mint, immutable
    pub open_tx_uuid: Option<String>, // toggles on BEGIN / COMMIT / ROLLBACK
}
```

Catalog/schema/table providers carry no session-derived state. `PencaTableProvider::scan(state: &dyn Session, ...)` reads the pin from `state.config_options().extensions().get::<SessionPin>()` for both cross-catalog rejection and the RYOW `ReadDataRequest.open_tx_uuid`.

**Doesn't support transactional DDL through Flight SQL.** `SchemaProvider::table()` has no access to `&dyn Session` (per its trait signature), so it can't read the extension. Option B is therefore only viable if `PencaSchemaProvider::table()` never needs to know about in-flight tx state — i.e., **Flight SQL clients only see committed metadata**.

**Tradeoffs:** catalog tree fully immutable + globally shareable, smallest reasoning surface, uses DataFusion's idiomatic extension mechanism, no provider-tree constructor changes. Cross-catalog SELECT rejection moves from catalog-list resolution to `scan()` time (still during planning, just later).

## Decision

**Adopt option B with per-session `SessionContext` caching.**

1. **Catalog tree built once at server startup**, stored on `FlightSqlService`. `Arc::clone`-shared across every session and every request. Catalog/schema/table providers lose all session-derived constructor fields.

2. **`SessionState` built once per session at session-mint time.** A template `SessionState` is built once at server startup with all default features registered; `SessionCache::get_or_create` clones the template, inserts a `SessionPin { catalog_uuid, open_tx_uuid: None }` extension, wraps in a `SessionContext`, and stores `Arc<SessionContext>` on the `Session` row.

3. **Handlers borrow the cached `Arc<SessionContext>` directly per request.** No clone, no per-request rebuild. All concurrency safety is **server-side** (see "Concurrency" below); we do not rely on any driver-side per-connection statement serialization.

4. **`SessionPin.open_tx_uuid` mutates on transaction control.** BEGIN takes the write lock on the cached state and sets `Some(tx_uuid)`; COMMIT/ROLLBACK takes the write lock and clears to `None`. `SessionPin.catalog_uuid` is set once at mint and never changes.

5. **`PencaTableProvider::scan(state: &dyn Session, ...)` reads `SessionPin`** to (a) reject cross-catalog SELECTs (`pin.catalog_uuid != table.catalog_uuid` → planning error) and (b) populate `ReadDataRequest.open_tx_uuid`. The `dml::execute` path keeps its existing `tx::validate_session_catalog` snapshot precheck (already in place from CHA-169).

6. **Transactional schema/table DDL via Flight SQL is intentionally unsupported.** `dml::execute` rejects DDL statements with a `FAILED_PRECONDITION` pointing at the gRPC `WriteService` API and citing this ADR. **(Superseded by [CHA-345](https://linear.app/chapala/issue/CHA-345) — `CREATE SCHEMA` / `CREATE TABLE` inside a `BEGIN`/`COMMIT` block are now supported; see the CHA-345 addendum. The decision below is preserved as the original record.)**

## Concurrency: how DataFusion's `SessionContext` makes this safe

ADBC formally serializes statements per connection. JDBC and ODBC do not — connections are not thread-safe per spec, but the spec doesn't enforce single-statement-at-a-time on the wire. **The cached `SessionState`'s mutation safety must therefore live on our side, not depend on driver behavior.** It does, because of how DataFusion's `SessionContext` is built. From `datafusion-52.5.0/src/execution/context/mod.rs`:

```rust
pub struct SessionContext {
    // ...
    state: Arc<RwLock<SessionState>>,                    // line 297
}

impl SessionContext {
    pub fn new_with_state(state: SessionState) -> Self {
        // ...
        state: Arc::new(RwLock::new(state)),             // line 369
    }
}
```

Every read path (`ctx.sql(...)`, `ctx.state()`) goes through the read lock; every mutation (`register_udf`, `add_analyzer_rule`, etc.) uses `self.state.write().method()` (lines 477, 488, 1468, 1481, ...). DataFusion's internal `Arc<RwLock<SessionState>>` *is* our concurrency primitive; we don't need an additional `Mutex` on the Session row.

**Per request (auto-commit or in-tx)** — nothing on our side:
```rust
let stream = ctx.sql(sql).await?.execute_stream().await?;
// ctx acquires the read lock internally for planning + execution.
```

**On BEGIN / COMMIT / ROLLBACK** — single brief write-lock acquisition to flip the extension:
```rust
let mut state = ctx.state_ref().write();
state.config_mut().options_mut().extensions.insert(SessionPin {
    catalog_uuid: ...,
    open_tx_uuid: Some(tx_uuid),  // or None on COMMIT/ROLLBACK
});
// guard drops here; concurrent ctx.sql() callers (if any) resume.
```

`state_ref()` returns `Arc<RwLock<SessionState>>` (line 1854) — the same `Arc` the context uses internally, exposed publicly. Concurrent in-flight `ctx.sql(...)` calls block on the write lock for microseconds while the extension flips, then resume reading the new pin. A misbehaving multi-threaded client sees serialized requests rather than undefined behavior — matching what every Flight SQL driver implicitly assumes a server provides.

## Why DDL is gated to gRPC

What forces the option A vs B trade: `SchemaProvider::table(&self, name: &str)` has no `Session` parameter in its trait signature. Option B can't thread `open_tx_uuid` into `PencaSchemaProvider::table()`'s read RPC, so it's only viable if Flight SQL clients can't observe mid-tx pending DDL.

That restriction is defensible on its own merits:

- Penca's primary user is the agentic/programmatic write path, which uses the gRPC `WriteService` directly. DDL via SQL is a developer-tool affordance (DBeaver, JDBC, ADBC notebooks), not the load-bearing interface.
- Flight SQL clients today don't issue DDL anyway. `dml::execute` matches only `INSERT`/`UPDATE`/`DELETE`; everything else errors. The status quo is "no DDL via Flight SQL"; this ADR formalizes it with a clear rejection message rather than expanding capability.
- Postgres-style ADBC/JDBC users typically issue DDL outside transactions (most drivers auto-commit DDL anyway). Transactional DDL inside `BEGIN`/`COMMIT` is real but rare in our workflows.

## Consequences

### Positive

- **Catalog tree fully immutable + globally shared.** Smallest reasoning surface; uses DataFusion's idiomatic extension mechanism; no provider-tree constructor churn.
- **Zero per-request `SessionState` work.** CHA-169's 30–100µs/request rebuild cost is gone. Requests just borrow the cached `Arc<SessionContext>`.
- **Prepared-plan caching becomes natural.** `SessionState.prepared_plans` lives inside the cached per-session state, so it persists across requests by construction.
- **`flight_sql/state_provider.rs` is deleted entirely.** That file held the vendored upstream `SessionStateProvider` trait + `StaticSessionStateProvider` impl + Penca's `PencaSessionStateProvider`. The vendored bits were kept across CHA-169 for vendor-sync alignment with `datafusion-flight-sql-server` v0.4.16; with no remaining consumer of the trait (Penca now owns the `SessionState` template directly on `SessionCache`), the alignment doesn't help anyone, and the `async-trait` dep goes with it. If we ever want the upstream trait back for a re-vendor, it's a straightforward re-add — but we shouldn't carry dead vendored code in the meantime.

### Negative

- **Asymmetry between data DML and schema DDL inside Flight SQL transactions.** A user who knows `BEGIN; INSERT; COMMIT;` works might assume `BEGIN; CREATE TABLE; COMMIT;` does too. The rejection wording emitted by `gateway::classify` is loud and distinguishes two distinct cases so users know which gap is which:

  - **Transactional DDL** (inside a Flight SQL `BEGIN`/`COMMIT` block) was, *at the time of this ADR*, gated to the gRPC WriteService API as an architectural restriction. **[CHA-345](https://linear.app/chapala/issue/CHA-345) lifted this** for the `CREATE SCHEMA` / `CREATE TABLE` pair (see addendum); the original rejection cited this ADR and pointed at `BeginTx + Create… + CommitTx`.
  - **Auto-commit DDL** (outside any tx) is supported for the `CREATE SCHEMA` + `CREATE TABLE` slice ([CHA-172](https://linear.app/chapala/issue/CHA-172)); other auto-commit DDL (`DROP …`, `ALTER …`, `CREATE INDEX`, `CREATE VIEW`, …) still requires the gRPC WriteService and the rejection wording points users there.

> **Note (CHA-174):** This ADR was originally written when DDL lived on a separate `AdminService`. CHA-174 folded that service into WriteService (mutations) and QueryService (reads). The architectural conclusion is unchanged: transactional DDL via Flight SQL still goes through the gRPC `WriteService` (`BeginTx + CreateSchema/CreateTable + CommitTx`), not the SQL surface. References to "AdminService" in this ADR have been retargeted to WriteService for the current topology.

  Conflating the two — pre-PR error said "only INSERT/UPDATE/DELETE supported" for both; an early draft of this ADR said "DDL is not supported through Flight SQL" period — would mislead about which knobs are even available.

- **Per-session memory grows.** Each active session holds a `SessionState` (~100–500KB depending on registered UDFs/optimizers). Idle TTL eviction bounds total footprint.

- **Future migration cost** if transactional DDL via Flight SQL becomes load-bearing: refactor toward option A (catalog tree per-session, `Arc<TxPinCell>` on schema/table providers). Bounded — local to `penca-datafusion` (provider constructors) and `flight_sql/service.rs` (handler extension-insert ↔ cell-write swap). No wire change, no client change. Option A's full design is recorded above so the migration doesn't need to rederive it. **(Realized by [CHA-345](https://linear.app/chapala/issue/CHA-345), and cheaper than estimated: [CHA-255](https://linear.app/chapala/issue/CHA-255) had already moved the catalog tree per-conn — paying Option A's structural cost — so CHA-345 only had to add one `Arc`-shared cell to `ConnScope`. See addendum.)**

- **Cross-catalog metadata visibility widens.** Pre-CHA-170, `PencaCatalogProviderList::catalog(name)` returned a `RejectedCatalogProvider` for non-session catalogs, which made `RejectedSchema::schema_names()` and `table_names()` return empty Vecs. After CHA-170 the catalog list is global and unaware of session pins, so `SHOW SCHEMAS IN other_catalog`, `SHOW TABLES IN other_catalog.schema`, and filtered `information_schema.tables` queries enumerate other catalogs' children rather than appearing empty. This is consistent with the principle that "the connection-scope pin only restricts which catalog can be *operated on*, not what's visible in metadata listings" (Postgres `\l`-style) — and arguably more honest than the prior `RejectedSchema::table_exist == true` workaround that forced every nonexistent-table reference into the cross-catalog rejection — but it's a user-facing behavior change worth flagging. The actionable rejection still fires at `scan()` time, so any *operation* on a non-session-catalog table still surfaces the cross-catalog error; only inert metadata listings widen.

## Future considerations (would prompt revisiting)

- Real demand for transactional DDL through Flight SQL — would prompt the option A migration.
- Schema scoping that needs to be tx-aware on the read side via Flight SQL — symmetric concern.
- Multi-instance session pooling (Spanner-style sessions across connections). Independent of this ADR; revisits ADR 0007 first.

## Pointers

- **CHA-170** — the audit + prepared-plan caching ticket this ADR ships with.
- **CHA-164** — transactional table DDL; Flight SQL exposure explicitly deferred by this ADR.
- **CHA-169** — introduced the per-session catalog pin; this ADR refactors the routing without changing pin semantics.
- **CHA-120** — metadata cache TTL + version-check invalidation; prerequisite for the prepared-plan caching follow-up.
- **ADR 0007** — sessions are connection-local in `penca-sql-server`. Unchanged.

## Addendum (CHA-253, 2026-05): SessionPin.catalog_uuid is write-once

This ADR's routing design treats `SessionPin.catalog_uuid` as an
extension on the cached `SessionContext`'s `SessionConfig`, written
once by `SessionCache::mint_ctx` at session-mint time. Before CHA-253
there was a second writer — `SessionCache::reseat_catalog` (via the
helper `write_catalog_pin`) — which fired during the CHA-212
configuring window when a `SetSessionOptions(catalog: …)` arrived
before the first non-`SetSessionOptions` request. CHA-253 removes
that reseat path entirely (catalog is now bound at handshake from the
`x-penca-catalog` gRPC header; see ADR 0007's CHA-253 addendum), so
`SessionPin.catalog_uuid` is genuinely write-once for the session's
lifetime. `write_catalog_pin` is gone; only `write_open_tx_uuid`
remains as a runtime mutator on the cached config — and that only
toggles `open_tx_uuid` between `Some(uuid)` and `None`, never touches
`catalog_uuid`.

This tightens the routing invariant: `PencaTableProvider::scan` (and
every cross-catalog check) reads `SessionPin.catalog_uuid` under
DataFusion's `Arc<RwLock<SessionState>>`, with a guarantee that the
read is stable for the connection's lifetime. No reseat race window
to reason about.

## Addendum (CHA-255, 2026-05): SessionPin.catalog_uuid is structurally write-once

CHA-255 deletes the `SessionCache` DashMap entirely — sessions are now
owned by the TCP connection (`ConnSession`), minted at first request by
`ConnSessionFactory::mint`, dropped when the conn closes. The
write-once-ness of `SessionPin.catalog_uuid` was already a CHA-253-era
behavioural invariant (one writer, at mint, in `mint_ctx`); under
CHA-255 it is now a **structural** invariant: there is no per-session
cache to re-key, no second writer code path can exist by construction,
and the `Arc<SessionContext>` that holds the pin is owned by exactly
one `Arc<ConnSession>` with the same lifetime as the TCP conn.

The runtime mutator `write_open_tx_uuid` still toggles
`open_tx_uuid` between `Some(uuid)` and `None` on
BEGIN/COMMIT/ROLLBACK; that's unchanged. The two-place atomic write
that keeps `ConnSession.open_tx_uuid` (the field) and
`SessionPin.open_tx_uuid` (the extension) in sync is now serialised
under the conn's `tokio::sync::Mutex<Option<String>>` rather than the
old `DashMap` shard lock — same semantics, simpler primitive.

## Addendum (CHA-345, 2026-05): transactional DDL via Flight SQL unblocked

This ADR chose Option B and recorded that transactional DDL via Flight
SQL was "permanently gated to gRPC" — the architectural blocker being
that `SchemaProvider::table(&self, name: &str)` has no `&Session`
parameter, so `PencaSchemaProvider::table` could not read the open
`tx_uuid` and therefore could not see a table created earlier in the
same transaction. CHA-345 re-evaluated that framing and unblocked it.

**Why the cost-benefit shifted.** The ADR estimated the migration as
"refactor toward Option A — catalog tree per-session, `Arc<TxPinCell>`
on schema/table providers." [CHA-255](https://linear.app/chapala/issue/CHA-255)
had since rebuilt the catalog tree **per connection**
(`ConnSessionFactory::build_ctx` constructs a fresh
`PencaCatalogProviderList` per accepted TCP connection, carrying a
`ConnScope` down every provider level) for rename-stable branch routing
and structural conn ownership. That inadvertently paid Option A's
structural cost — per-session provider tree, providers carrying
session-derived state, constructor changes, per-session memory — without
delivering its capability. So the remaining work was not the full
Option A migration; it was "add one `Arc`-shared cell to `ConnScope`."

**What CHA-345 changed (no wire change, no client change):**

1. Added `open_tx_cell: Arc<RwLock<Option<String>>>` to `ConnScope`
   (the single source of the open tx for the provider tree's read
   paths). `ConnSession` holds an `Arc`-clone and flips it on
   `BEGIN`/`COMMIT`/`ROLLBACK` in the same critical section as the
   authoritative `Mutex<Option<String>>`.
2. `PencaSchemaProvider::{table,table_names,table_exist}` read the cell
   so mid-tx metadata reads/lists/existence checks see the tx's own DDL;
   `PencaTableProvider::scan` reads the same cell for the RYOW data pin.
3. Retired `SessionPin.open_tx_uuid` (and `write_open_tx_uuid`) — the
   extension now pins only `catalog_uuid` for the cross-catalog `scan`
   check. The cell flip replaces the former extension write, so there is
   no longer any runtime mutation of the cached `SessionState` for tx
   control (only `SET search_path` mutates it now).
4. `gateway::classify` routes in-tx `CREATE SCHEMA` / `CREATE TABLE` to
   the DDL translator; `ddl::execute` threads the open `tx_uuid` into the
   `WriteService` request. The server side was already transaction-aware
   on both legs — `get_table` honours `open_tx_uuid` via
   `ReadRequestScope::resolve_read_snapshot`, and
   `WriteService::Create{Schema,Table}` honour `tx_uuid` via
   `resolve_or_auto_commit_tx` ([CHA-164](https://linear.app/chapala/issue/CHA-164)).

**Still out of scope:** other in-tx DDL (`DROP …`, `ALTER …`,
`CREATE INDEX`, `CREATE VIEW`) — not an architectural restriction, just
not yet implemented on the Flight SQL surface (same status as their
auto-commit forms); and dropping `SessionPin`'s now-vestigial
`catalog_uuid` (the per-conn `ConnScope.catalog_uuid` makes it
redundant). The Option A design recorded above remains the reference for
any future per-session-tree concern.

## Addendum (CHA-346, 2026-05): SessionPin deleted

The follow-up foreshadowed by the CHA-345 addendum. `SessionPin` (which
held only `catalog_uuid` after CHA-345 retired `open_tx_uuid`) is
deleted entirely; the connection's catalog now lives solely on the
per-conn provider tree's `ConnScope.catalog_uuid` (CHA-255). The
`SessionConfig` extension and its `ConfigExtension` / `ExtensionOptions`
impls are gone.

1. **The scan-time cross-catalog check is removed as structurally
   unreachable.** `PencaTableProvider::resolve_session_pin`'s
   `pin.catalog_uuid != scope.catalog_uuid` comparison cannot fire
   post-CHA-255: `build_ctx` builds the whole provider tree with the
   conn's catalog *and* inserted the pin with the same value (equal by
   construction), and a foreign-catalog `PencaTableProvider` is never
   built because `PencaCatalogProviderList::catalog(name)` returns
   `Some` only for the pinned catalog — DataFusion errors at planning
   first. The check was pure defence-in-depth on top of the catalog-list
   short-circuit.

2. **Sole intended cross-catalog gates** (do not re-add a scan-time
   check): the catalog-list short-circuit in `penca_datafusion::catalog_list`
   (SELECT path — yields DataFusion's `table … not found` at planning)
   and `penca_sql_server::tx::validate_session_catalog_name` (DML/BEGIN
   path — carries the more actionable "cross-catalog" wording).

3. **Surface delta:** `SET penca_session_pin.X = Y` was hard-rejected by
   the extension's `set()` (a deliberate "not user-configurable" error);
   without the extension registered it falls to DataFusion's generic
   unknown-config handling instead. Non-issue in practice — the
   `penca_session_pin` prefix is internal and no user sets it — but
   recorded here so the wording change isn't mistaken for a regression.
