---
name: Intermediate breakage OK during large refactors
description: For multi-commit/multi-file refactors, breaking tests in intermediate states is acceptable as long as the final state passes — don't bend the design or split commits to keep tests green at every step.
type: feedback
originSessionId: 74462f36-da0e-4bfc-a388-c929a3430113
---
For large structural refactors (CHA-164/CHA-177-scale), it's fine to leave the codebase in a broken intermediate state between commits as long as the final commit makes everything pass. Don't bend the design or fragment the work into awkward pieces just to keep CI green at every commit.

**Why:** Fragmenting a coherent refactor into "always-green" intermediate commits can produce contortions that aren't actually shippable on their own (temporary shims, dead code, half-renamed APIs). The reviewer ends up reading commit-history-as-narrative rather than reading the final diff. For these cases, prefer one big commit (or a handful of logical commits where each describes a phase, even if the system isn't runnable mid-phase) over many "make it compile/pass" commits.

**How to apply:** During multi-step storage refactors, API renames, or other invasive structural changes, work in the natural unit of the change. Commit at meaningful checkpoints (logical phases) but don't insert shim layers or revert/re-add code purely to keep tests green between commits. Verify the green-bar at the end.

**Doesn't apply to:** small/contained changes, bug fixes, single-feature work — those should still be commit-at-a-time green.
