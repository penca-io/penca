---
name: Keep CLAUDE.md stable — instructions go in skills, not the router
description: CLAUDE.md is loaded at session start and is part of the prompt-cache prefix; every edit invalidates that prefix and forces a cold cache_create on the next session. Treat CLAUDE.md as a thin router to skills, not the place where project conventions live.
type: feedback
originSessionId: 1e2a19a2-2bb2-44f0-b8a5-624c302e21f7
---

CLAUDE.md is a session-startup prompt-cache prefix. Heavy/specific content there is paid as `cache_creation_input_tokens` on every session that follows an edit — directly visible in the user's weekly plan usage.

CHA-208 slimmed CLAUDE.md from a multi-section dossier to a 6-line router. The router shape:
1. Names the project + one-line description.
2. Lists the canonical workflows by skill name (e.g. "Implement a Linear ticket → `/do-issue`").
3. Notes that reference docs are read on-demand within the relevant skill, not preemptively.

**Why:** The 5-minute prompt-cache TTL combined with multiple sessions/day means every CLAUDE.md change pays many cold-start `cache_create` hits before settling back into warm-hit equilibrium. Specific conventions live longer (and are read more cheaply) inside skills that load on-demand.

**How to apply:**
- When the user adds a project convention or workflow preference, ask whether it should live in a **skill** (loaded on demand) rather than CLAUDE.md.
- Treat any proposed CLAUDE.md edit as a tax on every future session. Reach for it only when the content needs to be present *before* any skill is invoked — typically just routing.
- Memory entries (this file and siblings) are also part of the per-session context, but the MEMORY.md index is read lazily and individual entries are read on relevance — the per-session cost is dominated by CLAUDE.md and the system prompt, not the memory store.
- The cache-key stability hierarchy (most → least stable, by docs): system prompt → CLAUDE.md → memory → conversation → tool results. CLAUDE.md is the most user-controllable stable layer, so changes there carry the largest amortized cost.
