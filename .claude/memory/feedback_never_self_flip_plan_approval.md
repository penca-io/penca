---
name: feedback_never_self_flip_plan_approval
description: "\"Keep going until approved\" = iterate /plan-reviewer to APPROVED, then STOP at the human gate; never flip plan-draft to approved myself"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 620ebdd9-f18f-4b8e-973d-5a5738b93f6a
  modified: 2026-07-30T19:45:53.101Z
---

In `/do-issue`, the label flip `plan-draft` → `approved` is the **user's** gate and the
only human checkpoint in the whole workflow. Never perform that flip myself, and never
run the Step-5 drain on tasks I approved.

When the user says "keep going until approved" / "drive this autonomously", that means:
iterate the `/plan-reviewer` audit — patching task bodies and re-reviewing — until the
*reviewer* returns `APPROVED`, then **stop and present the plan**. It does not mean
carry the plan through to implementation.

**Why:** the user reads the emitted kata task set before any code is written; that read is
where scope, mechanism, and necessity get challenged (see
[[feedback_evaluate_ticket_necessity_first_principles]] and
[[feedback_tickets_are_spirit_not_spec]]). Self-approving skips the only place that
judgment enters. On CHA-542 I flipped all six tasks and drained through two of them
before the user caught it — their words: "WTF? I meant get the plan approved not skip the
human approval gate."

**How to apply:** the ONE sanctioned exception is `/plan-reviewer`'s documented
`cleanup-pass` auto-flip, where every plan-draft task also carries `cleanup-pass` — those
sit under an umbrella the user already approved. Everywhere else, after `APPROVED`, post
the plan visualization and wait. Autonomy resumes *after* the gate — see
[[feedback_autonomous_drain_no_checkins]], which governs Steps 4–6 only.
