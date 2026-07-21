---
name: Single self-sufficient resume comment, not a multi-comment thread
description: When checkpointing mid-workflow on a Linear issue for a fresh-session resume, write ONE comment containing everything — no cross-references like "see comment immediately above"
type: feedback
originSessionId: 27255d73-7452-4a06-887c-cb9972b02cd5
---
When the user is about to `/clear` mid-ticket and wants the next session to resume cleanly:

- Write exactly **one** checkpoint comment containing the full resume state.
- It must be self-sufficient: a fresh Claude session given only that comment URI must be able to pick up the workflow without reading any other comment.
- **Why:** cross-references like "see the checkpoint comment immediately above this one" are brittle — comments get reordered, deleted, or new ones land between, and the cross-ref breaks. The next session may also miss the older comment entirely if it only loads the cited URI.
- **How to apply:** at the checkpoint moment, if a prior partial-state comment exists, *edit it in place* (via `save_comment` with the existing `id`) to absorb new state — don't add a second comment. If two state comments already exist, consolidate them: edit one to be comprehensive, delete the other.

Shape the resume comment to cover (at minimum):
- Workflow position (which skill, which step, what's left)
- Worktree path + branch
- Commits landed so far (`git log --oneline` style table)
- Gate results to date
- Outstanding decisions / plan deviations
- Remaining work as an ordered checklist
- Pointers to durable artifacts (other Linear comments by URI, ADRs, `/tmp` files that may not survive a clear)

User confirmed this preference 2026-05-11 during CHA-203 implementation walk.
