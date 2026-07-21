---
name: feedback_fold_trivial_review_fixes
description: "Fold trivial in-scope review fixes (one-liners) into the PR; don't file them as follow-up tickets"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 0aa01aee-6a64-42c7-958d-cbf95e971cbc
---

Trivial, in-scope fixes surfaced during review — a one-line guard addition, adding a
string to a check's list, a wording fix — get **folded into the open PR**, not filed as
their own follow-up ticket. During CHA-475 a reviewer flagged that `assert_not_system_table`
omitted `__penca_system__.indexes`; I filed it as CHA-478, and the user pushed back hard:
"literally adding a single string to a list during a check… you should not have made that
its own ticket. We're creating follow-ups faster than we're closing tickets."

**Why:** ticket proliferation outpaces closure and buries the real backlog; a fix smaller
than the ticket describing it belongs in the diff that surfaced it.

**How to apply:** before filing a follow-up, ask "is the fix smaller than the ticket?"
If yes, just do it in the current PR (with a test if warranted) and close the finding.
Reserve follow-up tickets for genuinely out-of-scope, substantial work — a new mechanism,
a multi-handler refactor (e.g. [[feedback_review_role_no_implementation]] keeps review
itself non-implementing, but /do-issue fixes land in-PR). When in doubt, fold in; if the
fix grows beyond a few lines or crosses into separate behavior, then file. Relates to
[[feedback_tickets_are_spirit_not_spec]].
