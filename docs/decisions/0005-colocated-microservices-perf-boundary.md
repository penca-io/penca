# 0005 — Colocated microservices: the perf boundary is at Postgres / object storage

- **Status:** Accepted
- **Date:** 2026-04-21
- **Ticket:** [CHA-121](https://linear.app/chapala/issue/CHA-121) (the design discussion that surfaced the need to record this assumption)
- **Related:** [CHA-120](https://linear.app/chapala/issue/CHA-120) (sql-server metadata caching — sized against the assumption recorded here); [CHA-86](https://linear.app/chapala/issue/CHA-86) (consistent read snapshot — also assumes colocation when reasoning about per-tx overhead)

## Context

Penca is deployed as five gRPC microservices (admin, query, write,
lifecycle, storage-metadata) plus the Flight SQL gateway. The gateway
and the five microservices each live in a separate container, but the
deployment topology — captured in `docker/compose.yml` and the
production Helm charts that mirror it — places every service on the
same host (or, in clustered deployments, the same Kubernetes node /
availability zone, behind the same service mesh).

This is a load-bearing assumption that has been quietly informing
trade-off calls across recent design discussions:

- The CHA-121 refactor moves SQL DML orchestration into penca-sql-server,
  which means a single SQL `INSERT` now triggers up to three internal
  gRPC calls (admin → PK metadata, query → collision check, write →
  append + commit). Acceptable only if those hops are cheap.
- The merge-on-read parallelism story (CHA-144) parallelizes per-tier
  resolves and the snapshot segment scan; the orchestration cost of
  fanning work back through the query service is treated as free.
- The lifecycle service issues admin/query RPCs during compaction; the
  per-cycle gRPC overhead is treated as negligible vs. the actual
  segment IO.

We have not been writing this assumption down. New contributors —
human or LLM — landing in the codebase will see N microservices and
reasonably ask "why are we OK with all this fan-out?" Without the
assumption recorded, the natural next move is to optimize away
cross-service hops (in-process composition, RPC batching, fat
multi-purpose RPCs), which would re-introduce the very coupling the
microservice split was designed to break.

## Decision

**Cross-microservice gRPC hops are negligible. The perf boundary
worth optimizing is the boundary between any microservice and its
external dependencies — Postgres and object storage.**

Concretely:

1. **Cross-service round-trip count is not a design constraint.** It is
   fine for penca-sql-server to issue 3+ internal RPCs to satisfy a
   single SQL statement. It is fine for the lifecycle service to fan
   out admin and query RPCs during compaction. Adding an internal RPC
   to enforce a clean responsibility split (e.g., moving collision
   check from write into sql-server via query) is the right call even
   though it adds hops.

2. **Postgres round trips count.** Combining `upsert_log` INSERT +
   `commit_tx_log` INSERT into one data-modifying CTE so a `*AndCommitTx`
   write is one PG round trip instead of two: worth it. Caching
   metadata in penca-sql-server (CHA-120) so steady-state reads don't
   hit admin → Postgres on every query: worth it. Avoiding an extra
   `SELECT` to satisfy a constraint that could be encoded structurally:
   worth it.

3. **Object-storage round trips count more.** Object storage may not
   be colocated (S3, R2, GCS — separate VPC, separate region). The
   merge-on-read snapshot segment fan-out, the lifecycle persist, and
   the cold-tier scan plans are all sized against this — minimize
   reads, batch writes, prefer one large request over many small ones.

4. **In-process composition is not the right answer to "too many
   internal hops."** If a service boundary feels chatty, the question
   is whether the boundary is in the right place — not whether to
   collapse two services into one. The microservice split exists so
   each service has one responsibility; chattiness inside the cluster
   is the price.

## Rationale

The colocation assumption is what makes the microservice topology
worth its overhead. If services were geographically distributed, every
SQL `INSERT` paying ~3 cross-AZ RTTs would be untenable, and we'd
have to collapse the orchestration logic back into a fat write
service — which would reintroduce the read-path config and
`ColdStorageClient` on the write service that CHA-121 explicitly
removed.

Recording the assumption explicitly does three things:

- **Frees future design calls from re-litigating it.** "Should we add
  an internal RPC here?" stops being an open question.
- **Anchors the perf-optimization vocabulary.** When we write
  `// TODO(CHA-XXX): cache this` or `perf(api): combine into one CTE`,
  the reader knows what budget the optimization is being measured
  against.
- **Names the assumption that, if violated, would force a major
  rearchitecture.** A future deployment that violates colocation
  doesn't just slow down — it invalidates a layering decision. The
  trigger conditions below name the situations where we'd revisit.

This is a shape-of-the-system decision, not a particular feature
trade-off. It belongs in the ADR set rather than scattered across
service docs and code comments.

## Trigger conditions to revisit

Re-evaluate **all** internal-RPC-fan-out trade-offs if any of the
following becomes true:

1. **Cross-region or cross-AZ deployment.** If we ever deploy
   microservices to separate availability zones, separate regions, or
   different cloud providers — or expose any of the internal RPCs
   across a public network boundary — the cost model flips. Several
   places in the codebase that currently issue 2–5 internal RPCs per
   request would need to be collapsed or batched.

2. **External consumers of the internal microservice RPCs.** Today
   the eight internal-only RPCs (and any future internal-only RPCs)
   are network-reachable but documented as reserved for in-cluster
   use. If we add authenticated access for external callers (e.g., a
   third-party tool that wants to talk to QueryService directly), the
   per-RPC overhead becomes user-visible and matters again. ADR 0004
   already names this as a follow-up auth concern.

3. **Per-RPC overhead measurably dominating end-to-end latency.** If
   profiling shows that the gRPC request/response framing,
   serialization, or scheduling across in-cluster services is a
   bigger contributor to p50/p99 latency than the underlying Postgres
   / object-storage work, the assumption may have decayed (e.g., due
   to a runtime regression, a service-mesh sidecar adding tens of ms,
   or a request shape that makes the framing cost non-trivial). This
   is unlikely in steady-state but worth checking before re-doing the
   perf-tuning vocabulary.

4. **Collapsing two services to one ever looks like the only way to
   meet a latency target.** Per the decision, this should never be
   the right answer — if it appears to be, the assumption above has
   broken, or the service boundary is in the wrong place. Either way,
   revisit before collapsing.

## Related tickets

- [CHA-121](https://linear.app/chapala/issue/CHA-121) — the design
  conversation that surfaced this assumption and motivated capturing
  it. Decisions in CHA-121 (sql-server orchestrates DML through
  Query + Write rather than the write service doing both) only make
  sense under this assumption.
- [CHA-120](https://linear.app/chapala/issue/CHA-120) — sql-server
  metadata caching. The cache exists because admin → Postgres is
  expensive (assumption #2 in the decision); per-query admin RPCs
  to penca-sql-server are not.
- [CHA-86](https://linear.app/chapala/issue/CHA-86) — consistent
  read snapshot. The "as-of cap" route assumes per-tx coordination
  RPCs are cheap.
- [CHA-122](https://linear.app/chapala/issue/CHA-122) — SQL
  transaction control. The Flight SQL transaction handlers will
  issue extra internal RPCs to coordinate begin/commit; this ADR
  is the basis for not worrying about that overhead.
