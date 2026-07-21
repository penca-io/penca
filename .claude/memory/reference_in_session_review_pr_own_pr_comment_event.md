---
name: reference_in_session_review_pr_own_pr_comment_event
description: In-session /review-pr reviews your OWN PR (same gh account) → GitHub blocks APPROVE/REQUEST_CHANGES; post the review with event=COMMENT.
metadata: 
  node_type: memory
  type: reference
  originSessionId: fbeefa98-2dba-4e47-816e-7f224eb7b6ca
---

Because [[feedback_no_subagents]] means /do-issue's `orch:spawn-review` runs
`/review-pr` **in-session** (not via a fresh subagent), the reviewer and the PR
author are the same GitHub account. GitHub rejects `event="APPROVE"` and
`event="REQUEST_CHANGES"` on your own PR ("Can not approve/request-changes your
own pull request"), so the `gh api .../reviews` POST must use **`event="COMMENT"`**
regardless of severity — convey "no blocking issues" / "Important finding" in the
review body + inline comments, not the review event.

Findings still flow normally: post inline comments on the diff lines, enqueue
each as a `cha-NNN review-pr approved` kata task, drain (fix → commit), then a
round-2 in-session re-review resolves the thread via the GraphQL
`resolveReviewThread` mutation. The `event=COMMENT` constraint only affects the
review verdict, not the finding mechanics. (First hit: CHA-468 / PR #269.)
