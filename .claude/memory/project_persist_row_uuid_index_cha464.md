---
name: project_persist_row_uuid_index_cha464
description: "CHA-464 persist-tier row_uuid index — built end-to-end then CANCELLED unmerged as off-strategy; why, and the revisit gate"
metadata:
  node_type: memory
  type: project
  originSessionId: cbac9bae-6b0c-484c-a6fa-e8c55826f565
---

**CANCELLED unmerged 2026-07-05.** CHA-464 (persist-tier internal `row_uuid` index, mirror of snapshot [[project_cold_row_uuid_index_format_agnostic]]/CHA-412) was implemented end-to-end (build+cache+seek, PR #291, all gates green) then **closed without merging** and the ticket set to Canceled. **Nothing landed on `main`** — the snapshot index (CHA-412/454/482) is unaffected. Do NOT resurrect without the gate below.

**Why off-strategy** (per the cold-oltp cross-epic tie-breaker doc — the design authority that wins over ticket prose):
- The doc's optimized cold-read path is **snapshot-only**: internal index auto-built per *snapshot* segment (CHA-412), seek in `SnapshotTableProvider` (CHA-454), DataFusion-free fast path snapshot-only (CHA-476). The **persist-log is the deliberately-slow history / time-travel substrate**, not an optimized read tier. Persist data folds into snapshot baselines (CHA-425) that already carry the index → CHA-464 indexed the transient intermediate.
- The index **build sits on the persist (memory-relief) critical path** (`phase1_durable_writes`): adds CPU + an S3 PUT to the one path whose job is to shed memory pressure faster — self-defeating under the write load that triggers persist.
- The read payoff is **gated on CHA-469** (selective row-group decode), which is NOT built: a cold miss still full-decodes the whole segment, so the index only saves post-decode row selection; on a one-shot cold read the extra sidecar GET makes it marginally net-negative. Persist point reads are also rare (degraded-mode spill), and we don't even index the hot PG logs.

**Revisit gate:** only reconsider once CHA-469 makes a cold index save real I/O, AND build the sidecar off the persist path (at CHA-425 baseline-fold), never in `phase1` persist writes.

**Process lesson:** see [[feedback_evaluate_ticket_necessity_first_principles]] — the ticket existed (Low priority, ADR-0026 "persist gets a row_uuid index later, out of scope") and we built it to spec instead of evaluating whether to build it at all (cost on the memory-relief path, win gated on unbuilt CHA-469, rare degraded path) at the plan gate.

If revisited, the original design detail (settled with user 2026-07-04): metadata = **parent+child two-table** (user kept it for future persist secondary indexes); no new cache TTL surface; compaction is repoint-based so sidecars survive with no rebuild (single build site = persist op, none in `compact.rs`). Kernel `penca_format::index::{build_segment_index, seek_row_offsets}`; `PersistSegment.index_sidecar` field in `penca-core/src/plan.rs`.
