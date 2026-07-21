---
name: project-pg-no-wal-archive
description: "Per-branch PG needs cross-AZ replicas for durability, NOT WAL archiving to S3 or base backups. Cold tier's row-level audit log replaces PITR."
metadata: 
  node_type: memory
  type: project
  originSessionId: a2944ddb-3a7e-4250-a08a-734547bc31ba
---

Per-branch Postgres in Penca does **not** archive WAL to S3 and does **not** take base backups. Durability for un-flushed writes comes from CloudNativePG cross-AZ replicas; PITR is replaced by the cold tier's row-level audit log.

**Why:** Under the per-branch isolated-stack model (see [[project-per-branch-isolated-stack]]), S3 is the durable substrate and PG is a write-through buffer between flushes. WAL → S3 + base backups + PITR are RDS-style features that solve two problems:

1. **Pod / node / AZ failure with un-flushed writes** — already covered by CloudNativePG's 1-primary-+-N-replicas-across-AZs.
2. **Logical-error recovery (replay to an LSN)** — strictly subsumed by Penca's cold-tier `tx_log` / row-level audit log, which preserves every row version indefinitely with branch-aware lineage and is queryable via `as_of`. PG WAL PITR replays operations to an LSN; the cold tier preserves the data itself. The cold tier is the better PITR.

The flush cadence becomes the explicit data-loss-on-disaster SLA (e.g., "total-cluster loss can lose up to N seconds of acked writes").

**How to apply when planning [[CHA-207]] / CloudNativePG manifests / disaster-recovery work:**
- Drop `barmanObjectStore` / `backup.barmanObjectStore` from CloudNativePG `Cluster` manifests.
- Drop the `pg-wal` / `pg-backups` S3 bucket from [[CHA-207]]'s S3 layout. Two buckets, not three (cold-tier and logs/snapshots).
- The DR drill is "kill the stack, watch it reconstitute from S3 cold tier" — not "restore base backup + replay WAL."
- Cross-AZ replica spread stays load-bearing; don't conflate "dropping WAL archive" with "dropping replicas."
- KEDA-scaled-to-zero stacks have no WAL-archive ambiguity because there is no WAL archive.
- For workloads that need a tighter data-loss-on-disaster SLA, the lever is **flush cadence**, not WAL retention.

[[CHA-207]] description still mentions "WAL → S3" and "daily base backups" as of 2026-05-23 — flag for refresh.
