---
name: Read Linear comments before drafting plans
description: When using /do-issue or otherwise drafting a plan on a Linear ticket, list_comments before plan-drafting; the user often drops canonical references and context in comments separately from the description
type: feedback
originSessionId: 5568463c-6cb1-4d73-8dae-a6e403ec73da
---
When drafting a plan on any Linear ticket, **list comments before drafting**. The /do-issue skill explicitly calls this out in Step 1 ("Comments — prior design discussion lives there") but it's easy to skip after a description-heavy back-and-forth that feels complete.

**Why:** On CHA-195, the user dropped four canonical-reference comments (Martin Fowler for architect, Rust Performance Book for perf-engineer, refactoring.guru + GeeksforGeeks for feature-engineer, SRP wiki shared) on the ticket *between* description-iteration turns. I drafted the plan without listing comments, missed all four, and had to revise. The user's correction was to instruct me to read comments — exactly what /do-issue Step 1 already says to do.

**How to apply:** On any /do-issue invocation, run `mcp__linear-server__list_comments` for the ticket as part of Step 1, before drafting. Treat comments as load-bearing context equal to description, not optional supplementary material — references and constraints land in comments often, especially during active design back-and-forth.
