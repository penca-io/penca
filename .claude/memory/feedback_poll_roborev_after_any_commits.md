---
name: feedback_poll_roborev_after_any_commits
description: roborev fires on EVERY commit (drain or ad-hoc); poll it to quiet + drain findings before declaring work done — not only inside the /do-issue loop
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fa802ea1-b627-42d1-8f02-ea3596b0f8f7
---

Every `git commit` triggers the roborev post-commit hook → an async review → any findings enqueued into the `cha-NNN` kata queue. The `/do-issue` drain (Step 5) polls `roborev status` and drains those findings *while its loop runs* — but commits made OUTSIDE that loop escape it: late review-fix commits, and especially later ad-hoc work (merge-conflict resolution, doc consolidation, follow-up edits) done in a session that isn't running `/do-issue`.

**Why:** twice on CHA-415 we left roborev findings unaddressed (a weakened test assertion, a stale recipe comment) because they landed on post-drain commits — the CHA-411 merge re-point and the CHA-423 doc consolidation, both done later as direct user requests, not under the drain. The drain's exit is gated on "PR MERGED", which the agent never does (the human merges — [[feedback_never_merge_pr]]), so the loop is always stopped early by hand and there is no automatic final sweep.

**How to apply:** after ANY batch of commits — a `/do-issue` drain, a merge-conflict resolution, a follow-up fix, a doc edit — and before declaring the work done / ready-for-merge, run `roborev status` until it shows 0 queued/running, then drain `kata ready --unowned --label cha-NNN --json | jq '.issues[] | select(.labels | index("approved"))'` AND `kata list --label cha-NNN --status open` (findings can land after a cleanup pass). Use the single-`--label` + jq form, not `--label cha-NNN --label approved`: the dual-`--label` AND-intersect is broken ([[feedback_kata_list_label_intersect_broken]]). roborev reviews lag the commit by seconds-to-minutes, so poll — don't assume quiet. Verify directly per [[feedback_bg_task_signal_reliability]]. Pairs with [[feedback_autonomous_drain_no_checkins]].
