---
name: reference_cold_datafusion_session_and_penca_api_dep_boundary
description: "Cold DataFusion reads must derive their session from the driver template (derive_cold_session), never SessionContext::new(); penca-api reaches datafusion types only via penca-dl re-exports"
metadata: 
  node_type: memory
  type: reference
  originSessionId: a5550c71-2323-4b0a-829d-65d378434a04
---

Two coupled Penca invariants for any cold-tier DataFusion read (proven by CHA-507 review findings):

**1. Session derivation (CHA-421).** A cold read that builds a `SessionContext` must derive it from the driver's `session_template` (`Arc<SessionState>`) via `penca_dl::derive_cold_session(&template)` — NOT a fresh `SessionContext::new()`. The template carries the shared function registry + analyzer/optimizer rules; a fresh context silently diverges. `QueryManager` holds `self.session_template`; derive once per read call and pass `&SessionContext` down into the penca-merge helper (mirrors `output.rs`'s `session: &SessionContext` param). This is exactly what a review will flag (CHA-507 `zdfk`): `penca_merge::cold_audit_batches` originally did `SessionContext::new()` on the `audit_data` path.

**2. Dependency boundary.** penca-api does NOT depend on the `datafusion` crate directly (penca-merge owns DataFusion) — see [[project_metadata_reads_to_querymanager]]. So penca-api can only name datafusion types through **penca-dl re-exports**: `penca_dl::{SessionState, SessionContext}` and `penca_dl::derive_cold_session` (`pub use` in penca-dl/src/lib.rs). Writing `use datafusion::prelude::SessionContext` in penca-api is an `E0433 unlinked crate` error; add the re-export to penca-dl instead.

Related: the cold fork-point seek (CHA-507) is a read → it lives on `QueryManager::resolve_fork_from_cold`, and `WriteManager::resolve_fork_watermark` delegates via `self.query_manager` (the write path threads FormatReaders through but holds no read logic). See [[feedback_validation_at_grpc_api_layer]] for the analogous "which layer owns this" instinct.
