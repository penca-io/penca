# ADR 0027 — Decoupled Purge: the purge watermark `Pu` is the read fence; Persist becomes pure CDC; Purge owns aborts on an independent seq axis

## Status

Accepted (CHA-444). Supersedes the hot↔cold read-fence decision in
[ADR 0019](0019-plan-time-pinning-and-universal-grace-window.md) /
[CHA-443](https://linear.app/chapala/issue/CHA-443) (fence was the persist
watermark `W_persist`), reinstates — on the `commit_seq_num` axis — the
"purge is the cutoff" decision from
[ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md), and **reverses**
[ADR 0021](0021-persist-owns-aborted-hot-cleanup.md) (aborted-hot cleanup
moves from Persist back to Purge).

> This replaces an earlier draft of ADR 0027 (the "split hot/cold grace +
> two-watermark fence-advance-vs-grace-deferred-delete" model). That model
> was rejected in design review; the decisions below are the corrected
> design. History is in git.

## Context

After [CHA-443](https://linear.app/chapala/issue/CHA-443) the hot↔cold read
fence is the **persist** seq watermark `W_persist` (`MAX(commit_seq_num)` over
committed `table_persist_metadata`), read at plan time in
`crates/penca-storage-meta/src/plan.rs`. Cold serves `commit_seq_num <=
W_persist`; hot serves `> W_persist`. Purge (ADR 0019) lags persist by
`query_timeout` and only deletes hot rows already past that fence.

That coupling is the load-shedding gap CHA-444 closes. Persist is the CDC —
it runs as fast as possible to shrink the eventual-consistency window — so
under a write spike persist races ahead and the hot tier (`upsert_log` /
`delete_log`) keeps growing until Snapshot drains it. There is no faster
valve, and the read fence being `W_persist` means a freshly-persisted row
is *immediately* removed from hot-read service even though it is identical
to the hot copy. The failure mode is hot-memory exhaustion (OOM,
container exit 137), not graceful degradation.

Two foundations from the `commit_seq_num` epic make the redesign possible:

- **A gapless, monotonic commit serial** ([CHA-428](https://linear.app/chapala/issue/CHA-428)):
  commits get a strictly-increasing `commit_seq_num` *at commit*, allocated
  under a row lock held to transaction end (allocation order = commit
  visibility order). This obsoletes Persist's open-tx clamp (below).
- **`plan()` is an in-process function** ([CHA-445](https://linear.app/chapala/issue/CHA-445))
  and the hot read **materializes early under one MVCC snapshot**
  (`penca_merge::resolve_hot` issues one-shot SELECTs, read first / in
  parallel with cold). Postgres MVCC keeps those rows visible to the read
  for its duration even if a concurrent Purge deletes them, as long as the
  delete commits after the read's snapshot. So the hot-side purge needs no
  `query_timeout` grace — plan→hot-read latency + MVCC is enough.

## Decision

### 1. Persist = pure CDC (committed-only, no open-tx clamp)

Persist persists **committed rows only** and stamps the persist seq
watermark `P` (`MAX(commit_seq_num)` over the rows it moved). It no longer:

- **clamps to the oldest open tx.** The old clamp
  (`effective_target < oldest_open_began_at`) existed only to keep the
  shared *micros* watermark below open-tx begins — it absorbed
  out-of-order wall-clock commits. On the seq axis it is unnecessary: an
  open (uncommitted) tx has **no `commit_seq_num`**, so its hot rows are
  invisible (the visibility join is an INNER JOIN to the committed
  `commit_tx_log`) and never persisted; on commit it gets a *fresh max* seq and
  lands above every fence. It cannot be stranded below `P`/`Pu`.
- **touches aborts.** Aborted-hot cleanup moves entirely to Purge (§3).

Persist races ahead of Snapshot and Purge, so `P > W_snap` essentially
always. Its cadence is never tied to snapshots.

### 2. The read fence is `Pu` (the purge watermark), not `W_persist`

`plan()` reads `Pu = MAX(last_purged_commit_seq_num)` over committed
`table_purge_metadata` and partitions the read into three tiers:

```
snapshot baseline : commit_seq_num <= W_snap
cold persist-log  : W_snap < commit_seq_num <= Pu     ← persisted AND purged from hot
hot               : Pu      < commit_seq_num <= as_of ← still in hot
```

- The cold persist plan is fenced **`(W_snap, Pu]`** (lower bound excludes
  rows already folded into the snapshot baseline; upper bound is `Pu`).
- The hot plan is floored at **`max(Pu, W_snap)`** — load-bearing because
  Snapshot and Purge are independently scheduled, so `Pu` may briefly lag
  `W_snap`; the floor keeps hot from overlapping the snapshot baseline.
- `audit_data` (no merge dedup) relies on this partition being symmetric:
  cold capped `<= Pu`, hot floored `> Pu`.

**Happy path — `Pu = W_snap`.** This ticket parks the purge target at the
snapshot watermark (§3). The cold persist-log tier `(W_snap, Pu]` is then
**empty**, and a read is **snapshot + hot** with no persist-log scan. The
persisted-but-unpurged band `(W_snap, P]` is served **from hot** — i.e.
**persisting a row does not evict it from hot-read service; only Purge
does.** That is the steady state.

The cold persist-log is the **overflow valve**, dormant here. The
memory-pressure follow-up ([CHA-466](https://linear.app/chapala/issue/CHA-466))
slides `Pu` *up* from `W_snap` toward `P − grace` when hot is under
pressure — then `(W_snap, Pu]` fills and those rows are served from the
cold persist-log (slower reads, bounded hot). "Decouple Purge from
Snapshot" is that **capability**; this ticket builds the machinery and
parks at the floor.

### 3. Purge clears hot atomically; owns aborts on an independent axis

`Purge(T)` does two independent things in one pass, both **atomic** (no
deferred delete):

**(a) Committed cleanup.** Advance `Pu(T)` to the happy-path target
**`W_snap`** (those rows are in the durable, read-served snapshot baseline,
so dropping them from hot needs no grace), and delete committed hot rows
with `commit_seq_num <= Pu`. Stamp `last_purged_commit_seq_num = Pu`. A read pinned
at `Pu(T_q)` keeps its hot rows: it materialized them early under an MVCC
snapshot at `~T_q`, and a concurrent Purge's delete commits later, so MVCC
keeps them visible (the §Context argument). The `P − grace` ceiling is the
[CHA-466](https://linear.app/chapala/issue/CHA-466) hook — inert while
`Pu = W_snap`, since `W_snap <= P` always.

**(b) Aborted cleanup (reverses ADR 0021).** Aborted hot rows are
**invisible to every read** (no committed `commit_tx_log` row to join), so they
are pure garbage needing **no grace** and **no snapshot/persist gating** —
they must not be bounded by the committed fence `Pu`, or aborts on a
commit-idle or pure-aborts-only table would never be cleaned. Purge instead
treats aborts as a **fully independent axis**:

- A dedicated per-branch **`abort_seq_num` counter** — implemented
  *identically* to the `commit_tx_log_seq_num` commit counter (a locked counter
  row, `UPDATE … SET seq_num = seq_num + 1 RETURNING seq_num - 1`,
  incremented in the same statement as the `abort_tx_log` INSERT) — stamps
  each abort a unique, strictly-monotone `aborted_at_seq_num`. **It is not
  a sample of the commit counter.** (CHA-429 stamped `aborted_at_seq_num`
  by *reading* the commit-counter frontier; that value stalls between
  commits, so two aborts can share `S`. A watermark advancing past `S`
  would then falsely cover a later abort also stamped `S` → premature
  ledger GC + orphaned hot rows. A dedicated monotone counter — gapless and
  in-allocation-order, like commit — removes that hazard.)
- Purge samples the abort-counter frontier `F` at the start of the pass,
  deletes aborted hot rows in `T` whose tx is in `abort_tx_log` with
  `aborted_at_seq_num <= F`, and stamps the **abort purge watermark
  `Pa(T) = F`** (`last_purged_aborted_seq_num`). Aborts that land during
  the pass get `> F` and are caught next pass.

**Expired-begin txs (the union).** A tx that timed out without ever
committing or explicitly aborting (a `begin_tx_log` row, expired, no
`commit_tx_log` / `abort_tx_log` row) also has invisible hot rows that must be
reclaimed. Purge cleans the **union** — `abort_tx_log ∪ (begin_tx_log
WHERE expired AND not committed AND not aborted)` — enumerating both via
`tx_table_log`. Expired-begins are *not* on the abort axis (they have no
`aborted_at_seq_num`), so they do **not** advance `Pa`; their hot-row
delete is uncertified by any seq watermark, which is fine because the
delete needs none. Their *ledger* GC (§5) is the only part that needs a
bound, and expiry is intrinsically wall-clock, so it uses a wall-clock
grace — there is no serialization point at which to allocate a monotone
expiry seq (`began_at_seq_num` is a non-monotone commit-counter sample that
could even name a still-open tx, so it cannot back a watermark). This
closes the expired-begin leak in-ticket; no tx-expiry reaper is needed.

Purge enumerates abort-bearing tables via `tx_table_log ⋈ abort_tx_log`
(the abort half of `list_modified_tables`, which already unions
`abort_tx_log`) plus expired `begin_tx_log` rows; that enumeration migrates
from feeding Persist's old aborted cleanup to feeding Purge (§5).

`began_at_seq_num` is **unchanged** — it stays a *sample* of the commit
counter, because it is the OpenTx snapshot-isolation bound compared
directly against commit `commit_seq_num` (`commit_seq_num < began_at_seq_num`); it
is not a monotone cleanup watermark, so the stall hazard does not apply.

### 4. `table_purge_metadata` is seq-only

- **Add** `last_purged_commit_seq_num` (`Pu`) and `last_purged_aborted_seq_num`
  (`Pa`).
- **Drop** `purged_at_micros` — it was the watermark *value*, and every
  consumer moves to seq: the read fence (§2), commit_tx_log GC (§5), and the
  deterministic two-phase row identity. The `table_purge_uuid` seed moves
  to the `(Pu, Pa)` composite (always present and distinct per advancing
  wave, so phase-1 replay stays idempotent even for pure-abort purges where
  `Pu` does not advance). The row's own `commit_micros` (two-phase
  commit timestamp, used by commit_tx_log GC's as-of isolation) is a separate
  column and is **kept**.

Pre-1.0 drop-and-recreate
([CHA-203](https://linear.app/chapala/issue/CHA-203) precedent); no
metadata migration.

### 5. commit_tx_log GC trails the PURGE watermark, on the seq axis

A persisted-but-unpurged tx is still live in hot (hot rows join `commit_tx_log`
for visibility), so GC at `min(persist)` would yank `commit_tx_log` out from under
live `(W_snap, P]` hot reads. GC trails **purge**: once purged, a row
serves only from snapshot/cold, which carry pre-joined tx metadata
(ADR 0017). `PurgeTxLog` re-axises from micros to seq:

- committed `X` eligible when `X.commit_seq_num <= MIN(Pu(T))` over the tables
  `X` wrote to;
- aborted `X` eligible when `X.aborted_at_seq_num <= MIN(Pa(T))` over the
  tables `X` wrote to.

`MIN(Pu)` / `MIN(Pa)` are the branch-min watermarks (same shape as today's
`MIN(purged_at_micros)`, two axes instead of one). The pure-begin+abort
fast-path (no `tx_table_log` entries) re-expresses on the abort-counter
frontier. The as-of isolation against concurrent Purges keeps reading the
purge row's `commit_micros`.

**Expired-begin ledger GC** is the wall-clock branch. An expired-begin tx
(no `commit_tx_log` / `abort_tx_log` row) is eligible to drop its `begin_tx_log` /
`tx_table_log` rows when `expires_at_micros < cleanup_started_at − grace`.
`begin_tx_log` is the *only* handle Purge has to enumerate that tx's
(invisible) hot rows (`purge::delete_expired_begin_hot_rows`), so dropping it
before Purge has cleared them would strand those rows forever — an unbounded,
invisible hot leak. The grace must therefore be **at least one Purge sweep
interval** so ≥1 Purge pass has re-swept every table the tx wrote to (it stays
enumerated via `tx_table_log` until its ledger drops).

Concretely the grace is **`max(purge_sweep_interval, hot_grace_window)`** —
the **hot-purge grace window** (`HOT_PURGE_GRACE_SECONDS`, the same knob CHA-466
will use for the `Pu ≤ P − hot_grace` ceiling; default 60s), floored at the
Purge sweep cadence. Purge rides the snapshot loop, so that is
`SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS`; the floor is taken over both
scheduler cadences as a conservative bound, shared with the lifecycle service
like `QUERY_TIMEOUT_SECONDS`. It is **not** `query_timeout`:
that is the query-service cap, with no relation to the Purge cadence, so wiring
the ledger GC to it left a leak when the tick interval exceeded the timeout (a
slow/lagging table while the branch-wide `PurgeTxLog` proceeded). This is the
same shape as the pure-begin+abort fast-path, keyed on `expires_at_micros`.

**Persist-watermark gates durability; purge-watermark gates commit_tx_log GC.**

## Correctness

- **Committed visibility.** A read pins `Pu(T_q)` and reads hot for
  `commit_seq_num > Pu(T_q)`, materializing those rows within milliseconds
  under an MVCC snapshot at `~T_q`. A concurrent Purge advancing `Pu` past
  `Pu(T_q)` commits its delete at `T_adv > T_q`; MVCC keeps the rows
  visible to the earlier snapshot. Resolved + exclusion hot reads are both
  at `~T_q`, so a delete cannot land between them. ∎
- **Abort GC safety.** `Pa(T) = F` certifies *all* aborts with
  `aborted_at_seq_num <= F` are deleted from `T`'s hot logs. commit_tx_log GC
  drops `X`'s ledger only when `MIN(Pa(T)) >= X.aborted_at_seq_num` over
  every `T` that `X` wrote to ⇒ `X`'s aborted hot rows are gone everywhere.
  The dedicated monotone counter guarantees no *new* abort is ever `<= F`
  after `Pa` reaches `F`. ∎
- **No abort stranding.** Purge cleans aborts independent of `W_snap`/`P`,
  so aborts on commit-idle and pure-aborts-only tables are reclaimed at the
  abort-frontier (matching today's prompt, clamp-bounded behavior — just
  relocated Persist→Purge and re-axised micros→seq).

## Preserved invariants

- The per-table advisory keys (`persist:` / `purge:` / `purge_tx_log:`) and
  the strict tier partition are unchanged. Cross-operation pairs stay
  lock-free (the MVCC argument is lock-free).
- The cold-segment GC grace (`sweep_segments`) keeps `query_timeout` — a
  cold scan streams whole-query, so cold-file GC still waits. Only the
  **hot** side drops the grace.
- Time-travel / `audit_data` results are unchanged; only which tier serves
  the `(Pu, P]` tail changes (hot instead of cold).

## Out of scope

- The proactive memory-pressure trigger that slides `Pu` above `W_snap`
  ([CHA-466](https://linear.app/chapala/issue/CHA-466)).
- Live-`Pu`-per-request + snapshot-segment-list caching
  ([CHA-441](https://linear.app/chapala/issue/CHA-441)).

## Relationship to other ADRs

- **[ADR 0018](0018-purge-as-hot-cold-visibility-cutoff.md):** purge-as-cutoff
  reinstated, now on the gapless `commit_seq_num` axis.
- **[ADR 0019](0019-plan-time-pinning-and-universal-grace-window.md):** the
  fence-source decision is superseded (fence = `Pu`, not `W_persist`); the
  per-operation locks and the cold-GC `query_timeout` survive; the hot-side
  grace is removed.
- **[ADR 0021](0021-persist-owns-aborted-hot-cleanup.md):** reversed —
  Purge owns aborted-hot cleanup again (forced by Persist→committed-only),
  now on the independent `Pa` axis.
- **[ADR 0017](0017-cold-data-segments-pre-joined-tx-metadata.md):** purged rows serve
  from snapshot/cold with pre-joined tx metadata — why commit_tx_log GC at the
  purge watermark is safe.
