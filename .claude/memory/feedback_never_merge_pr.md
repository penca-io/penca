---
name: never-merge-prs
description: "NEVER merge a PR — merging is strictly the user's action, even when they say 'get it merged'"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 156cf9b3-02aa-4acd-854c-0928ebfb20e0
---

**NEVER merge a PR.** Do not run `gh pr merge` (or any merge action, UI or CLI) under any circumstances — merging to the default branch is **strictly reserved for the user**, every single time.

**Why:** Merging is a significant, hard-to-reverse, owner-reserved decision; the user wants to be the one who pulls the trigger. (I merged PR #200 after the user said "let's get the PR merged" — they meant *prepare* it for merge, not click it. They corrected: "NEVER EVER merge a PR (that is strictly reserved for me).")

**How to apply:** "get the PR merged" / "let's merge it" / "ready to merge" means → finalize the branch (commits in, `just check`/gates green, roborev + review findings drained, `gh pr view` shows `mergeable=CLEAN`), then **report it as ready-to-merge and STOP. The user merges.** The `/do-issue` flow already assumes this — Step 6 post-merge cleanup runs only *after the human has merged*; never pre-empt the merge to get there. Related: [[feedback_review_role_no_implementation]].
