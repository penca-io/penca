---
name: reference_roborev_severity_threshold_met_is_clean
description: "roborev's SEVERITY_THRESHOLD_MET output means CLEAN, not \"findings found\"; trust .verdict_bool, not the string."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 05a4b981-1ec9-4115-91f7-2cdcd554c7ee
  modified: 2026-07-31T00:37:19.351Z
---

When `roborev show <job>` prints only `SEVERITY_THRESHOLD_MET` as the review body, the commit **passed**. The string reads like "the severity threshold was met, so there are findings" — it means the opposite: the reviewer produced findings that all fell *below* `review_min_severity`, which `.roborev.toml` sets to `'medium'`, so they were suppressed and never surfaced.

The authoritative field is `roborev show <job> --json | jq .verdict_bool` — **1 = clean, 0 = issues found**. That contract is documented in `scripts/roborev-kata-hook.sh`'s `verdict_bool` case block, which is what decides whether a kata task gets enqueued; `.job.verdict` is `'P'`/`'F'` and agrees. Plain `roborev show` prints only the review body, so the sentinel is all you see — the verdict is not in that output at all.

So: an empty `kata list --label cha-NNN` alongside a `SEVERITY_THRESHOLD_MET` job is consistent, not a dropped finding. Don't go hunting for the suppressed text — check `verdict_bool` and move on.

**Timing gotcha when polling for the verdict.** For a window after a job finishes, `roborev show <job> --json` exits non-zero with `Error: no review found for job <job>` while the plain `roborev show <job>` already renders the body — the review row lands after the job row. So a wait loop shaped like `until [ "$(roborev show N --json | jq -r '.status // "running"')" != "running" ]` **terminates immediately on that error**: jq gets no input, prints the empty string, and `"" != "running"` is true, which reads as "the job finished" when nothing was actually observed. Poll plain `roborev show` / `roborev list` for completion, then read `--json .verdict_bool` once. See [[feedback_absence_is_not_evidence]].

Related: [[feedback_poll_roborev_after_any_commits]], and [[reference_roborev_kata_bridge_needs_cha_branch]] for the other direction — an empty queue is *not* evidence of a clean review when the bridge never ran at all.
