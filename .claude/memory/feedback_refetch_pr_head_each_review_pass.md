---
name: feedback-refetch-pr-head-each-review-pass
description: "For /review-pr re-runs, refetch pull/N/head before reading code — the local ref is stale and the user may have pushed fixes between passes"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: ae721342-ae10-4ea2-a25f-e3e6921d4c40
---

For repeat /review-pr passes on the same PR (the user types "Review once more please", "give it another pass", etc.), the first step is always `git fetch origin pull/N/head:pr<N>-head --force`. The local ref does not auto-update between passes — assuming it still points at the prior head produces stale reviews that flag findings the user has already fixed.

**Why:** Caught on [[feedback-purged-at-micros-cold-coverage-invariant]] review of CHA-220 / PR #84. After pass 4 posted a BLOCKER on `cffb0019`, the user pushed `ce017f4` (a fix commit titled "fix(api): address CHA-220 review feedback") that resolved the BLOCKER plus 5 Suggestions. Pass 5 didn't refetch, reviewed against stale `cffb0019`, and posted an "ADR also needs updating" finding for changes that were already in the pushed commit. User had to correct: "Wait the ADR was updated in ce017f4. That should already be pushed."

**How to apply:** First step of any /review-pr re-run after the initial pass:

1. `git fetch origin pull/N/head:pr<N>-head --force` — note the `cffb001..ce017f4` style range output that signals new commits.
2. If the head advanced, diff prior→new (`git log <prior-head>..pr<N>-head --oneline` + commit bodies) to understand what landed.
3. Read the new HEAD's view of any files referenced by prior review findings — line numbers shift; comments may already be resolved.
4. Frame the new pass against the actual current head, not against findings frozen at the prior head.

Bonus: also `git log origin/<branch>..HEAD` locally to spot unpushed work. Reviews are anchored to the pushed state, but the user may have local progress that informs scope.
