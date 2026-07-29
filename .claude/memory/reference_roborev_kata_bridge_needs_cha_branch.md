---
name: reference_roborev_kata_bridge_needs_cha_branch
description: "The roborev→kata bridge only enqueues on branches whose name contains cha-NNN; on any other branch reviews still run but findings reach nothing — an empty kata queue is NOT evidence of a clean review."
metadata:
  node_type: memory
  type: reference
---

`scripts/roborev-kata-hook.sh` extracts `cha-[0-9]+` from the reviewed branch
name and **skips the job when there is no match** (a deliberate quiet no-op):

```bash
CHA=$(printf '%s' "$BRANCH" | grep -oE 'cha-[0-9]+' | head -1)
[ -z "$CHA" ] && continue
```

So on a branch like `nhobin219/dream-curate-memory-2026-07-29`, `docs/…`, or any
ad-hoc fix branch, roborev **still reviews every commit** and still finds real
defects — but nothing is enqueued, `kata tui` shows "no issues match", and the
hook log reads `done: processed 0 non-clean review(s), created 0 task(s)`.

**Why this matters:** the standing rule
([[feedback_poll_roborev_after_any_commits]]) is "poll roborev to quiet, then
drain the kata queue before declaring done." On a non-`cha-NNN` branch the
second half is vacuous — the queue is empty *by construction*, not because the
review was clean. Treating empty-queue as all-clear silently discards every
finding. Observed 2026-07-29 on the `/dream` curation branch: four commits, four
completed reviews, several genuine Medium findings, zero kata tasks.

**How to apply — on any branch without `cha-NNN` in its name:**

1. Poll to quiet by parsing the counts off the `Jobs:` line (a `grep` for
   "queued"/"running" matches even when idle; no `Jobs:` line at all means the
   daemon is down, which is not idle).
2. Then read the findings **by hand** — `roborev list --status done` to get the
   job ids for your commits, `roborev show <id>` for the body. There is no
   queue to drain; the review output is the only artifact.
3. Judge each finding on its merits rather than accepting it. In the observed
   case two of four were real (a busy-check that failed open when the daemon is
   unreachable; an unguarded `jq` iteration over a null array) and one was
   confidently wrong — it read kata's *tagged* source to claim repeated
   `--label` AND-intersects, which the installed `dev` binary contradicts when
   you actually run it.

Naming a working branch `…/cha-NNN-…` is the cheap fix when the work does have
a ticket. When it genuinely doesn't, step 2 is the whole mechanism.
