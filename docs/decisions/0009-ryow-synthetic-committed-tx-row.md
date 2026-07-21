# 0009 — Inject the open tx into `committed_tx` as a synthetic literal

- **Status:** Accepted
- **Date:** 2026-04-30
- **Ticket:** [CHA-165](https://linear.app/chapala/issue/CHA-165)

## Context

CHA-165 adds read-your-own-writes to `ReadData`: when a request carries `open_tx_uuid`, the read sees the open tx's uncommitted upserts/deletes layered onto a snapshot-isolation view at `began_at_micros`. The visibility predicate is:

```text
(commit_micros < began_at_micros) OR (tx_uuid = open_tx_uuid)
```

The merge-on-read SQL builder (`crates/penca-merge/src/sql.rs`, mirrored in Python) emits one logical query, dialect-specialized to Postgres (hot tier) and DataFusion (cold tier). The query starts with a `committed_tx` CTE that selects committed `(tx_uuid, commit_micros)` from `commit_tx_log`, then JOINs `upsert_log` / `delete_log` against it via `USING (tx_uuid)`.

The naive RYOW emission — adding `OR tx_uuid = :open_tx_uuid` to the `committed_tx` WHERE clause — is wrong. The open tx's row isn't in `commit_tx_log` (it's in `begin_tx_log`), so the OR clause matches nothing and the open tx's `upsert_log` rows get filtered out by the JOIN.

The fix is to inject a synthetic committed-tx entry for the open tx so the JOIN includes its rows. Two design questions:

1. **Where does the synthetic row come from?**
2. **What value does its synthetic `commit_micros` carry?**

## Decision

### 1. Synthetic literal, not a JOIN to `begin_tx_log`

Emit the open tx's synthetic row as a `UNION ALL SELECT '<open_tx_uuid>' AS tx_uuid, <began_at_micros> AS commit_micros` literal — **not** as a `UNION ALL FROM begin_tx_log WHERE tx_uuid = ...` table reference.

The cold tier's DataFusion context has only the cold log tables (`upsert_log`, `delete_log`, `commit_tx_log`) registered. `begin_tx_log` lives only on the hot Postgres tier. Referencing `begin_tx_log` in the shared SQL would force per-tier emission divergence (hot reads from `begin_tx_log`; cold can't). The shared `build_merge_resolved` is generic over `Dialect` precisely so the same logical SQL runs in both tiers — that property is load-bearing for keeping the two implementations in sync.

The servicer already resolves `(tx_uuid, began_at_micros)` via `read_begin_tx` (to validate the tx exists, lives on the requested branch, and isn't aborted). Threading those two values into the SQL builder is essentially free — no extra round-trip, same SQL emission across hot and cold.

### 2. `began_at_micros` as the synthetic timestamp

Use the tx's actual `began_at_micros` for the synthetic row's `commit_micros`. The strict-`<` filter on the committed-tx side guarantees every real committed row has `commit_micros < began_at_micros`, so the synthetic row wins the latest-version-per-row_uuid race in `latest_per_partition` and dominates any committed delete tombstone in the deletes JOIN.

Earlier drafts used `i64::MAX` as the synthetic timestamp — bigger than any possible commit ts, so own writes "obviously win." That's pure caution: under the strict-`<` filter, `began_at_micros` already wins by construction. `began_at_micros` carries semantic meaning ("the open tx is visible as of when it began") and the value is already in hand, so prefer it over the magic constant.

### 3. Cold tier: dead UNION row, accepted

By the CHA-103 invariant, only committed txs are persisted to cold. The open tx's `tx_uuid` therefore never appears in cold's `upsert_log` or `delete_log`, and the synthetic UNION row in cold's `committed_tx` joins to zero rows. Pure dead weight in the cold-tier SQL.

We accept this. The cost is one row in a CTE materialization and one zero-match hash probe in the downstream join — invisible at any practical scale. The alternative — branching on dialect to suppress the UNION row in cold — leaks tier-specific behavior into the shared SQL builder for no measurable gain.

If the inconsistency becomes a readability problem when reading SQL traces, a follow-up can downgrade `OpenTx` → `AsOfMicros(began_at_micros - 1)` inside `resolve_cold` / `cold_exclusion_row_uuids` before calling the SQL builder. Strict `< began` and `<= began - 1` are equivalent on integer micros, so cold's emitted SQL would lose the UNION cleanly.

**CHA-168 update:** branch-coordinated persist clamps `effective_target = min(target_micros ?? now(), oldest_open_began_at - 1)`, so cold's `max(commit_micros) < every open tx's began_at_micros` by construction. Under that invariant, the cold-tier strict-`<` filter — and equivalently the synthetic UNION row — is provably never load-bearing on cold: no real cold row can pass the filter and produce a different answer than the (committed_at < began OR tx_uuid = open) clause. The deferred cold-side downgrade therefore moves from "harmless cleanup" to "correctness-preserving simplification." Still deferred (not load-bearing for any user-facing test), but the rationale strengthens.

## Consequences

### Same SQL, both tiers

`build_merge_resolved` and `build_exclusion_set` keep their `<D: Dialect>` shape. The only dialect-aware divergence is `Dialect::uuid_literal`: PgDialect emits `'<uuid>'::uuid` (Postgres needs the explicit cast for UNION/JOIN type-checking against uuid columns), DfDialect emits a bare `'<uuid>'` (DataFusion treats tx_uuid as Utf8).

### Multiple writes of the same `row_uuid` within an open tx (resolved by [CHA-243](https://linear.app/chapala/issue/CHA-243))

`upsert_log` and `delete_log` carry a per-row `written_at_micros BIGINT NOT NULL DEFAULT now()`, sourced from the writing PG transaction's `now()` clock. The merge-on-read ordering key is the **composite** `(commit_micros, written_at_micros)`, and the tombstone-shadow predicate is the lexicographic `>=` on that pair:

```sql
l.commit_micros > d.commit_micros
  OR (l.commit_micros = d.commit_micros
      AND l.written_at_micros >= d.written_at_micros)
```

The composite-`>=` form rather than the SQL standard `(a, b) >= (c, d)` row-value comparison: DataFusion 52's executor schema for the row-value form doesn't match its planner schema (manifests as an Arrow `column types must match schema types` error at execute time); PG supports both forms identically. The Rust merge SQL builder spells the predicate out for both dialects.

**The uniform rule:** *for any tied `(commit_micros, written_at_micros)` on the same `row_uuid`, the upsert wins.*

This one rule covers every observable case:

1. **Within-RPC same-Change `delete(R) + upsert(R)`** — the [CHA-237](https://linear.app/chapala/issue/CHA-237) value-preserving-SET shape (`SET id = id`, `COALESCE`, no-op `CASE`). Both writes hit the same PG tx → same `now()` → tied `written_at_micros`. Composite `>=` → upsert wins → row visible. The pre-CHA-243 strict-`>` predicate silently tombstoned the row; the composite tiebreaker eliminates that regression at the root.
2. **Cross-RPC same-tx submit order** — `BEGIN; INSERT R; DELETE R; COMMIT;` and `BEGIN; DELETE R; INSERT R; COMMIT;` each emit two `MutateData` RPCs. Each RPC is its own PG transaction, so the two writes get *distinct, monotonically-increasing* `now()` values. `INSERT R; DELETE R`: insert at T1, delete at T2, T1 < T2, so the predicate `(T_commit, T1) >= (T_commit, T2)` is false → DELETE wins → R hidden. `DELETE R; INSERT R`: T2 > T1, predicate `(T_commit, T2) >= (T_commit, T1)` true → INSERT wins → R visible. Submit order preserved across RPCs.
3. **Cross-RPC concurrent same-tx writes whose `now()` values coincide at microsecond resolution** — only reachable when a client multiplexes connections or async-submits over one connection without waiting (programmer error; sequential RPCs over network/IPC essentially never collide at microsecond resolution). Composite `>=` → upsert wins → row visible. A defensible default: without client-side synchronization the ordering is ill-defined, so the uniform upsert-wins-on-tie rule matches the within-RPC case and avoids a separate explanation.

The Rust SQL emitter has a unit test (`merge_resolved_within_rpc_tie_upsert_wins` in `crates/penca-merge/src/sql.rs`) that pins both the new composite-`>=` shape and the absence of the pre-CHA-243 strict-`>` form so anyone touching the comparison fails loudly.

### Mutual exclusion with `as_of_micros`

`OpenTx` and `AsOfMicros` are mutually exclusive on `ReadDataRequest` (servicer rejects with `INVALID_ARGUMENT` if both are set). RYOW into a different point-in-time view is incoherent — the schema or table may not exist at the time-travel target. The synthetic-row design assumes this invariant: only one snapshot variant emits a UNION clause.

### Branch and abort validation

The servicer's `read_begin_tx` call returns the tx's `branch_uuid`. We reject branch-mismatch (`FAILED_PRECONDITION`) and consult `abort_tx_log` to reject post-abort RYOW reads (`begin_tx_log` survives abort until the lifecycle sweep purges it). These checks live next to the `ReadSnapshot::OpenTx` construction so the SQL builder never sees an aborted or wrong-branch tx.

## Out of scope (deferred)

- **Cold-side dead UNION cleanup.** See "Cold tier" above. Optional; revisit if SQL trace readability becomes a problem.
- **Within-tx ordering of multiple writes to the same row.** ~~Out of CHA-165 acceptance; pick a monotonic tiebreaker if/when needed.~~ **Resolved by [CHA-243](https://linear.app/chapala/issue/CHA-243)** — see "Multiple writes of the same `row_uuid` within an open tx" above.
- **Expired-but-not-swept open tx.** Today the resolver only checks `abort_tx_log`. Folding an `expires_at_micros < now` check into the resolver to treat expiry as abort requires deciding which clock (PG vs. server) is canonical — left as a TODO in `query.rs` and `query.py`.
