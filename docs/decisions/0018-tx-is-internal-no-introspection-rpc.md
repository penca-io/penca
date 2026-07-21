# ADR 0018 — Tx is an internal mechanism; no public introspection RPC

## Status

Accepted (CHA-222).

## Context

Pre-CHA-222, the public proto exposed transactions as a first-class
domain object:

- `message Tx { tx_uuid, branch_uuid, began_at_micros, expires_at_micros,
  commit_micros, comment, author }` in `common.proto`.
- `QueryService.GetTx(tx_uuid)` and `QueryService.ListTxs(branch)` to
  read transactions back out.
- `optional Tx tx` fields on `BeginTxResponse`, `CommitTxResponse`,
  `MergeBranchResponse`, and the auto-commit path of `MutateDataResponse`.

That surface area was load-bearing in only one direction: callers needed
the commit watermark (`commit_micros`) as `as_of_micros` for
subsequent time-travel reads. Everything else on `Tx` — `began_at_micros`,
`expires_at_micros`, `comment`, `author` — was either set-once-and-asserted
in tests or never read.

Two other changes made the rest of the framing redundant for external
callers:

1. **[ADR 0017](0017-cold-data-segments-pre-joined-tx-metadata.md)**
   pre-joins `(commit_micros, began_at_micros, comment, author)`
   onto every cold data row at persist time. `AuditData` surfaces those
   four columns per-version, so per-row historical context is reachable
   without any tx-level lookup.
2. **CHA-218** made `commit_tx_log` hot-only. The post-persist state has no
   external observability story for `tx_uuid` anyway: once a tx's rows
   land in cold, the cold layout has no `tx_uuid` column at all (cold
   reads are a pure scan, no JOIN). A public `GetTx` would have had to
   define behavior for tx_uuids whose `commit_tx_log` rows have been
   garbage-collected post-persist — work that buys nothing because
   nothing consumes it.

A `grep` across `tests/` and `packages/` confirmed the actual usage:
the load-bearing pattern is `committed.commit_micros` for
time-travel pins; `tx.comment` / `tx.author` are asserted-once in write
tests, never consumed downstream; `client.get_tx` and `client.list_txs`
exist primarily as tests of themselves.

## Decision

**Tx framing is an internal mechanism. The public proto exposes only
the load-bearing scalar.**

Concretely:

- Delete `message Tx` from `common.proto`. Pre-1.0 — no `reserved`
  slot.
- Delete `QueryService.GetTx` and `QueryService.ListTxs` (and their
  request/response messages). No public RPC for introspecting tx state.
- Flatten the timestamps onto the response messages:
  - `BeginTxResponse { tx_uuid, began_at_micros, expires_at_micros }`
  - `CommitTxResponse { commit_micros }`
  - `AbortTxResponse { aborted_at_micros }`
  - `MergeBranchResponse { commit_micros }`
  - `MutateDataResponse { optional commit_micros }` (auto-commit
    populates; append leaves unset, preserving `FIXME(CHA-157)`
    semantics).
- `BeginTxRequest.tx_uuid` stays `optional`. Server allocates a UUID
  when omitted; clients that want retry-idempotency under transport
  failure can supply their own.

Internals are unchanged: `commit_tx_log`, `begin_tx_log`, `abort_tx_log`
schemas keep their full column set; the `version_uuid =
deterministic_uuid(row_uuid, tx_uuid)` invariant
([ADR 0013](0013-auditable-store-invariant-deterministic-version-uuid.md))
still holds; the `get_tx_status` internal lookup (used by `CommitTx` /
`AbortTx` to classify state) still walks all three hot partitions.

## Why no `tx_uuid` in audit rows

CHA-218 settled this already: the cold layout is
`(<user_cols>, began_at_micros, commit_micros, comment, author)`
per row. No `tx_uuid`. The four denormalized metadata columns answer
every question a historical reader can ask about a committed mutation —
who wrote it, when, with what comment, in what tx-frame window. Adding
`tx_uuid` back to audit rows would re-create the pre-CHA-218 JOIN
contract without any reader actually needing it.

## Why no narrow introspection RPC even now

A future need could plausibly resurface — e.g., "which tx_uuids touched
table X between T1 and T2." If that need ever surfaces, reintroduce a
narrow RPC scoped to the actual question. Reintroducing the deleted
shape today is the wrong default: the previous `GetTx(tx_uuid)` was
unconstrained by use case and ended up paying for a Postgres parent-table
scan (when the caller didn't supply a branch) to answer "what was the
`commit_micros` of this tx_uuid I already have" — which is a
question a caller almost never has, because the only reason to know a
`tx_uuid` in the first place is to act on it (commit, abort, append
within an open one), and those response shapes already carry the
timestamp inline.

## Consequences

**Wire-incompatible.** Pre-1.0, drop-and-recreate (CHA-203, CHA-218
precedent).

**Migration toll for callers:** existing `tx = client.begin_tx(...);
client.mutate_data(tx.tx_uuid, ...)` call sites keep compiling
(`tx_uuid` stays a top-level field on `BeginTxResponse`). Sites that
previously read `committed.tx.commit_micros` flip to
`committed.commit_micros`. Sites reading `merge_tx.comment` or
`merge_tx.author` migrate to `AuditData` against the target branch,
windowed on the merge's `commit_micros`.

**Test-tier impact:** integration tests that previously round-tripped
through `GetTx` to validate cross-branch tx-uuid invariants either
delete (the public observable is gone) or fall through to a SQL-level
white-box check on the catalog's `commit_tx_log` partition (the internal
invariant is still meaningful and worth asserting at the test tier).

**Doc rewrites:** the previous design-decisions motivation for branch
partitioning leaned on `get_tx(tx_uuid)` cross-branch lookup; that
rationale now points at the internal `get_tx_status` cross-branch
classification (which still walks the parent partition under the same
mechanic). Algorithms.md merge-provenance discussion reframes onto
`AuditData` windowed by `commit_micros`.

## Alternatives considered

- **Keep `Tx` and `GetTx`, just rename / re-document.** Rejected: the
  shape isn't a documentation problem; it's load-bearing for nothing
  except one scalar, and CHA-218 already moved the per-row historical
  metadata path to `AuditData`. Keeping it would be paying caching
  cost (cache invalidation on RPC-shape audits) for an unused surface.
- **Require `tx_uuid` on `BeginTxRequest` and drop the server fallback.**
  Considered as part of this ticket; reversed during the plan-review
  step. Most callers don't care about retry idempotency, and forcing
  every caller to mint a UUID just to make a single-shot BeginTx call
  is a worse default than the current "server allocates if you don't
  care; supply your own if you do." Field stays `optional`.
- **`reserved` slots on the dropped proto fields and messages.**
  Convention for this repo is pre-release-drop-no-reserve (commit
  `9605e17`, the CHA-218 cleanup). Revisit if we 1.0 before this ADR
  ships.

## Related

- [ADR 0013 — Auditable store invariant: `version_uuid = deterministic_uuid(row_uuid, tx_uuid)`](0013-auditable-store-invariant-deterministic-version-uuid.md)
- [ADR 0017 — Cold data segments pre-join tx metadata](0017-cold-data-segments-pre-joined-tx-metadata.md)
- CHA-218 — `commit_tx_log` becomes hot-only (the load-bearing precondition for "tx is internal")
- CHA-157 — pending: `MutateDataResponse.commit_micros` becomes
  required on both auto-commit and append paths once the append-time
  tx-fetch is folded into the apply SQL.
