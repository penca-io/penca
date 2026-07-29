---
name: reference_roborev_severity_threshold_met_is_clean
description: "roborev's SEVERITY_THRESHOLD_MET output means CLEAN, not \"findings found\"; trust .verdict_bool, not the string."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 05a4b981-1ec9-4115-91f7-2cdcd554c7ee
  modified: 2026-07-29T17:33:37.873Z
---

When `roborev show <job>` prints only `SEVERITY_THRESHOLD_MET` as the review body, the commit **passed**. The string reads like "the severity threshold was met, so there are findings" — it means the opposite: the reviewer produced findings that all fell *below* `review_min_severity`, which `.roborev.toml` sets to `'medium'`, so they were suppressed and never surfaced.

The authoritative field is `roborev show <job> --json | jq .verdict_bool` — **1 = clean, 0 = issues found**. That contract is documented in `scripts/roborev-kata-hook.sh`'s `verdict_bool` case block, which is what decides whether a kata task gets enqueued; `.job.verdict` is `'P'`/`'F'` and agrees. Plain `roborev show` prints only the review body, so the sentinel is all you see — the verdict is not in that output at all.

So: an empty `kata list --label cha-NNN` alongside a `SEVERITY_THRESHOLD_MET` job is consistent, not a dropped finding. Don't go hunting for the suppressed text — check `verdict_bool` and move on.

Related: [[feedback_poll_roborev_after_any_commits]], [[feedback_kata_list_label_intersect_broken]].
