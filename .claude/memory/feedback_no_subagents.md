---
name: No sub-agents
description: User does not want sub-agents (Agent tool) — apply fixes directly
type: feedback
originSessionId: 0a5e08cc-4890-4d7a-b79a-f9f86754ba43
---
Do not delegate work via the Agent tool — apply edits / run commands directly in the main session.

**Why:** User explicitly said "no sub-agents" while resuming the CHA-203 /do-issue workflow (2026-05-11), interrupting an in-flight `feature-engineer` invocation. They have a strong preference for the main session doing the work end-to-end on this project rather than fanning out.

**How to apply:** When the workflow or a memory suggests delegating (e.g. /do-issue Step 7's `feature-engineer`/`code-quality-reviewer`/`perf-engineer` agents), inline the work instead. Read files, run edits, and run gates from the main session. Reviews can still be done — just do them yourself instead of spawning a reviewer agent. If you genuinely need parallelism for independent long-running searches, ask before launching agents.

**Confirmed exception — /do-issue `orch:spawn-review` (2026-07-05 CHA-484; RE-CONFIRMED 2026-07-11 CHA-432):** the fresh-context `/review-pr` subagent at the end of the drain IS accepted — **spawn it, don't self-review in-session.** For that ONE step, spawn the Opus `/review-pr` subagent exactly as the do-issue skill documents; do not substitute an in-session review (on CHA-432 the in-session substitution was corrected: "spawn the review subagent as the skill says"). Everything else stays main-session. When any OTHER subagent tension arises, still ask.
