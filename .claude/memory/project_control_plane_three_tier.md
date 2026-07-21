---
name: project-control-plane-three-tier
description: "Control plane is penca-catalog (global) → penca-branch (per catalog) → per-branch stack. Each service's state lives in MVCC Penca tables on small PG + S3, dogfooding the auditable-store model. Control-plane PG (catalog/branch_store) is a SEPARATE GLOBAL instance, so state that must be transactionally atomic with per-branch data (e.g. commit counters atomic with tx_log) must live in the per-branch stack, not branch_store."
metadata: 
  node_type: memory
  type: project
  originSessionId: a2944ddb-3a7e-4250-a08a-734547bc31ba
---

The Penca control plane is three services that mirror the data hierarchy:

```
penca-catalog  (one global, per deployment)
├── owns: catalog records; CreateCatalog RPC
├── creates: a penca-branch service per catalog
└── exposes: Resolve(catalog, branch) → stack endpoint   (convenience method that internally hops catalog → branch → stack and returns an opaque endpoint — does NOT proxy traffic)
        │
        ▼
penca-branch  (one per catalog)
├── owns: branch records for its catalog; CreateBranch RPC
├── creates: full per-branch stack per CreateBranch
└── exposes: Resolve(branch) → stack endpoint
        │
        ▼
per-branch stack  (Flight SQL + Write + Query + Meta + PG + Lifecycle + Cache)
```

**Client flow:** connect to penca-catalog → resolve `(catalog, branch)` → receive opaque stack endpoint → connect direct to the stack for all data-plane traffic. Clients cache the resolved endpoint with an invalidation hook (branch delete / re-route).

**Why three tiers mirroring the data:** RBAC, blast radius, and tenancy boundaries all fall out one-for-one with the data model — catalog is the tenant boundary, penca-branch failures are scoped to one catalog, existing branch stacks keep serving even when control-plane services are down.

**State storage (the [[CHA-240]] throughline):**

Each control-plane service stores its own records as MVCC Penca tables on a small PG + S3 — i.e., catalog_store and branch_store become auditable Penca tables, same model as schema_store / table_store after [[CHA-177]]. The control plane *uses the database it is part of*. This buys:

- Per-row audit on CreateCatalog / CreateBranch (author, comment, tx_uuid)
- `as_of` time-travel on catalog and branch name resolution (closes the asymmetry [[CHA-236]] / ADR 0020 documents)
- Standard soft-delete via tombstone, subsuming most of [[CHA-239]]
- One mental model across all namespace tiers

This promotes [[CHA-240]] from a "Low" design exploration to a structural prerequisite for the three-tier control plane. penca-catalog needs catalog_store to be a Penca table to ship in its final form; penca-branch needs branch_store to be a Penca table for the same reason. Bootstrap recursion (catalog_store living before any catalog exists) is the open problem CHA-240 has to solve — meta-catalog with a deterministic UUID is the leading candidate.

**k8s RBAC default (reversible):** penca-catalog gets cluster-scoped RBAC to create catalog namespaces + the initial penca-branch Deployment per catalog; penca-branch gets namespace-scoped RBAC for everything inside its catalog. Simpler to start; lift to a CRD-driven meta-operator pattern if blast radius from penca-catalog's broad permissions becomes a concern. Document this as the default; don't hard-code it.

**DNS surface:** one public DNS for penca-catalog (`catalog.penca.io` shape). penca-branch and per-branch-stack endpoints are opaque connection strings handed back by Resolve — no public DNS per branch/stack. Removes the per-tenant DNS-management problem.

**How to apply when planning [[CHA-129]] / [[CHA-207]] / [[CHA-240]] / control-plane tickets:**

- [[CHA-129]] needs reframing: the split is **three** services along the data hierarchy (penca-catalog / penca-branch / per-branch stack), not two along data/control. The old name "penca-api-server" is ambiguous; drop it in favor of penca-catalog + penca-branch.
- The unfiled "penca-api-server skeleton" ticket splits into three:
  1. penca-catalog skeleton (catalog records as Penca table + CreateCatalog + Resolve)
  2. penca-branch skeleton (branch records as Penca table + CreateBranch + per-branch-stack provisioning)
  3. Per-branch-stack Kustomize template (what penca-branch instantiates)
- [[CHA-240]] graduates from Low to effectively a prereq — promote and schedule alongside the control-plane work.
- The unfiled "k8s RBAC scaffold" ticket splits into two: cluster-scoped role for penca-catalog, namespace-scoped role for penca-branch.
- Multi-tenancy primitives ticket: catalog = tenant boundary, enforced at penca-catalog. No additional tenancy machinery needed.
- The convenience `Resolve(catalog, branch)` method is **resolution only** — it must return an endpoint, never proxy traffic. Otherwise penca-catalog becomes a hot path for every Flight SQL connection.

**PG instance topology — load-bearing placement rule (CHA-428):** the control-plane stores (`catalog_store`, `branch_store`) live in a **separate, global PG instance**, while each per-branch stack has its **own isolated PG** (per [[project-per-branch-isolated-stack]]). Consequence: **anything that must be transactionally atomic with per-branch data must live in the per-branch stack's PG, never in `branch_store`/the global control plane** — a cross-instance write can't share a transaction or hold a row lock across the per-branch INSERT.

Worked example: CHA-428's `tx_seq_num` commit-order counter is allocated by an `UPDATE … RETURNING` in the *same statement* as the `tx_log` INSERT, lock held to tx-end (allocation order == commit-visibility order). It therefore gets its **own dedicated `tx_log_seq_num` table co-located with the tx_log family in the per-branch stack** — explicitly NOT a column on the `branch_store` row, which would be (a) the wrong PG instance under the managed topology and (b) hot-counter contention against branch-metadata writes. General rule when siting new state: if it's written in the same tx as per-branch data, it belongs in the per-branch stack; only globally-shared, branch-independent records belong in catalog/branch_store.

Related: [[project-per-branch-isolated-stack]], [[project-pg-no-wal-archive]].
