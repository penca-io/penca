---
name: Review role does not include implementation
description: During /review-pr, only review and update comments — do not make code changes, even when the user describes the desired fix
type: feedback
originSessionId: 0ae6b595-3697-427a-a970-a5eefcb7c15b
---
When invoked via `/review-pr` (or any code review context), the role is review and feedback only — posting/updating GitHub comments and Linear tickets. Do not edit source files in the PR branch even when the user describes how they want the fix done.

**Why:** During CHA-152 review, the user said "I think the right fix is to bring python in line with Rust" and "I don't think the SQL gateway should filter…". I interpreted that as instructions to make the changes, switched to the PR branch in the main checkout, edited Python + Rust, and started running integration tests. The user stopped me with "what are you doing? You're role is to code review, not make changes." Their direction-setting was meant to inform updated review comments so a future editing Claude could implement, not for me to implement directly.

**How to apply:**
- During review, the user describing "the right fix" or "the desired direction" → update the inline review comment + ticket so the next editor (human or Claude) sees the intent. Do not modify code in the PR.
- Linear ticket creation/updates that the user explicitly asks for ("create a ticket", "update CHA-X") → fine, do them.
- If unsure whether the user wants code changes, ask. The cheap interruption beats reverting a half-applied edit.
- Even if you do end up making code changes during review, never do it directly on the PR branch — always start a separate branch (see [[feedback_worktrees]] for VM-vs-laptop branching mechanics).
