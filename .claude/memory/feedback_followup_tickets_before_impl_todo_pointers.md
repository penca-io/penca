---
name: feedback_followup_tickets_before_impl_todo_pointers
description: "Deferred-scope follow-up tickets are minted BEFORE implementation starts; TODO(CHA-NNN) code comments and their Linear tickets must point at each other, and no TODO may reference a ticket that closes without doing it"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 073ad8c4-e9ae-44e1-b027-60a781cb6136
---

Two linked rules from the CHA-485 gate (2026-07-04):

1. **Mint follow-up tickets at plan approval, before any implementation.** When the plan defers scope ("range lookups → follow-up"), create the Linear ticket right then — not at PR-open time — so the first commit can reference a real CHA-NNN id.
2. **TODO(CHA-NNN) anchoring is bidirectional.** A `TODO(CHA-NNN)` code comment requires the referenced ticket to carry a pointer to that code site (in its description at create time, or a pointer comment on an existing ticket). And a TODO must never be left pointing at a ticket that is about to close without doing the work — retarget it to the follow-up in the same PR (e.g. `snapshot_op.rs` `TODO(CHA-485)` → `TODO(CHA-490)` shipped inside CHA-485's PR).

**Why:** grep gives code→ticket discovery, but a ticket reader has no way to know a code anchor exists without the reverse pointer; and a TODO naming a Done ticket is archaeology that actively misleads.

**How to apply:** in /do-issue, when the gate summary lists "deferred to follow-up" items, create those tickets immediately after (or during) plan approval with the code-anchor path in the description, and add a task-body line retargeting any existing TODO in whichever kata task already touches that file. Related: [[feedback_fold_trivial_review_fixes]], [[tickets-are-spirit-not-spec]].

Not a licence to file freely: [[feedback_ask_before_filing_tickets]] is the default (never file unprompted). This rule is one of its named exceptions, because the plan the user approved enumerated the deferrals — so the approval already covers them.
