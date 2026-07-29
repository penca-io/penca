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

**How to apply:** after ANY batch of commits — a `/do-issue` drain, a merge-conflict resolution, a follow-up fix, a doc edit — and before declaring the work done / ready-for-merge, poll `roborev status` until its **Jobs line reports 0 queued and 0 running** (parse the counts — a `grep` for the words matches even on an idle daemon, whose header reads "Daemon: running" and whose jobs line spells out both words at zero), then drain the ready∩approved intersection AND `kata list --label cha-NNN --status open` (findings can land after a cleanup pass). Both queries need care: repeated `--label` flags don't AND-intersect, and `kata ready --json` carries no `labels` field at all, so a `.labels` post-filter on *its* output silently matches nothing and the sweep looks clean when it isn't. The `/do-issue` skill's kata cheat sheet has the correct `comm -12` intersection form. roborev reviews lag the commit by seconds-to-minutes, so poll — don't assume quiet. Verify directly per [[feedback_slow_commands_capture_and_wait]]. Pairs with [[feedback_autonomous_drain_no_checkins]].
