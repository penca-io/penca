---
name: feedback-ask-before-filing-tickets
description: "Don't file Linear tickets unprompted — propose and wait for approval; and don't propose test harnesses for local dev tooling."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: f4540695-2957-4a5b-9c63-ff237adce167
  modified: 2026-07-28T19:18:57.547Z
---

**Never create a Linear ticket without the user's explicit approval.** Propose it in one or two sentences and wait. This holds even when a finding is real and well-characterised.

Exception: when a ticket's own text mandates a split (e.g. CHA-517 said "stage 2 → its own ticket when stage 1 ships") or a skill instructs it, filing is expected — but say so when reporting.

**Why:** filing is cheap for me and expensive for the user — every ticket is backlog they have to triage, prioritise and eventually close. Volume dilutes the queue. Discovering a problem and *filing* a problem are different acts, and only the first is mine to make unilaterally.

**Second, related correction (2026-07-28):** I proposed extracting `just clean-agent-tools`' drain loop into a script so it could carry a regression test, after getting its loop-termination guard wrong twice. The user pushed back — regression tests for **local dev tooling** are disproportionate. Reasoning worth keeping:

- Blast radius is tiny: the recipe runs once per ticket on a disposable VM, and failure leaves some kata rows behind.
- A **hang is self-announcing** — you Ctrl-C it and know instantly. Only *silent* failures (the original under-clean, which exited 0 having skipped items past a list cap) justify machinery.
- Weigh the cost of the guard against the cost of the failure it prevents. Production/engine paths earn tests; a convenience recipe usually does not.

**How to apply:** when a dev-tooling bug is found, fix it and verify by hand. Report the verification in the commit message. Do not propose extraction-plus-harness unless the failure would be silent AND costly. See [[feedback_evaluate_ticket_necessity_first_principles]] — same instinct, applied to whether the work should exist at all.
