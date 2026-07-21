---
name: project_oss_license_apache2_open_core
description: "OSS license = Apache-2.0 and EVERYTHING in the penca-io/penca repo is open core (moat = the closed control plane, not in-repo); decided 2026-07-21, resolves CHA-357/CHA-356; CHA-151 visibility flip is human-only"
metadata: 
  node_type: memory
  type: project
  originSessionId: 5eb2b7dd-8f01-4e0f-8623-c88a865d0251
  modified: 2026-07-21T22:06:04.902Z
---

2026-07-21 (nhobin219, during CHA-151): the OSS license is **Apache-2.0** and **everything in the `penca-io/penca` repo is open core** — no source-available/BSL, no feature-crippling. The commercial moat is the **closed control plane** (per-branch compute isolation, [[project_control_plane_three_tier]]), which is NOT in the public repo, so permissive licensing of the engine is acceptable.

This resolves the two items the CHA-151 checklist flagged as unresolved: **CHA-357** (license choice — previously "assumes Apache v2") and **CHA-356** (open-core boundary). Both can be marked Done. The README already carried an Apache-v2 badge; CHA-151's PR #2 (penca-io/penca) added the actual `LICENSE` (Apache-2.0), `CONTRIBUTING.md` (**not accepting external contributions yet** — no review bandwidth, so CODE_OF_CONDUCT / issue+PR templates / DCO were deliberately dropped), and `SECURITY.md`. Contacts = `info@penca.io`.

CHA-151's remaining deliverable — the private→public **visibility flip** + announce + topics + GH secrets + ruleset activation — is **human-only**; do NOT flip repo visibility as the agent. PR #2 uses `Refs CHA-151` (not `Closes`) so its merge does not auto-close the ticket.
