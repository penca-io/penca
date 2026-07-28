---
name: feedback_ask_before_filing_tickets
description: Don't file Linear tickets on your own initiative — propose in a sentence and wait for approval.
metadata:
  node_type: memory
  type: feedback
---

**Never create a Linear ticket unprompted.** Propose it in one or two sentences and wait for the user to say yes. This holds even when the finding is real, well-characterised and clearly worth tracking — discovering a problem and *filing* a problem are separate acts, and only the first is mine to make unilaterally.

**Why:** filing is cheap for me and expensive for the user. Every ticket is backlog they have to triage, prioritise and eventually close, and volume dilutes the queue. Corrected 2026-07-28 after I filed CHA-529 and CHA-530 on my own initiative during CHA-517's cleanup.

**Where filing IS expected — no need to re-ask:**

- **Deferred scope at an approved plan gate.** [[feedback_followup_tickets_before_impl_todo_pointers]] says to mint follow-up tickets at plan approval, before implementation, so the first commit can cite a real CHA-NNN. That is not in tension with this rule: the plan the user approved *enumerated* the deferrals, so the approval already covers them.
- **A ticket or skill whose own text mandates it** — either by naming the split ("stage 2 → its own ticket when stage 1 ships") or by setting a standing rule ("engine bugs found by this work go to separate tickets, never folded into this PR"). Both are CHA-517; both are the ticket instructing you, so both are covered. Say so when reporting, rather than filing silently.

**How to apply:** outside those cases, report the finding and end with "want me to file it?". If the answer is no, the finding still belongs in the report — the user can act on it without a ticket existing.

Related: [[feedback_evaluate_ticket_necessity_first_principles]] (whether the work should exist at all), [[feedback_no_harness_for_local_dev_tooling]] (the proportionality correction from the same review).
