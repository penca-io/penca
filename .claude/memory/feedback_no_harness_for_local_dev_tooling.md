---
name: feedback_no_harness_for_local_dev_tooling
description: Don't build test harnesses for local dev tooling — weigh the guard against the failure it prevents, not against how embarrassing the bug was.
metadata:
  node_type: memory
  type: feedback
---

**Don't restructure local dev tooling to make it testable.** Fix the bug, verify by hand, record the verification in the commit message, and move on.

This is about *behavioural* harnesses — extracting logic out of a Justfile recipe into a script so stubs can drive it. Cheap **structural** checks are a different thing and are already the repo norm: `static_perf_framework_wiring_test.py` greps recipe bodies for the symbols they must invoke, at near-zero cost and no restructuring. Those are fine to add. Just don't mistake them for behavioural coverage — a grep confirms a guard is *present*, not that it is *correct*, and the two `clean-agent-tools` liveness bugs below would both have passed one.

**Why:** correcting 2026-07-28. After getting `just clean-agent-tools`' loop-termination guard wrong twice, I proposed extracting its drain loop into a script so it could carry a stub-driven test. The user pushed back, and the reasoning is the part worth keeping:

- **Blast radius.** The recipe runs once per ticket on a disposable VM; failure leaves a few kata rows behind. That is the entire cost.
- **A hang is self-announcing.** You Ctrl-C it and know instantly. Only *silent* failures justify machinery — the one that actually mattered there was exiting 0 having skipped every item past a list cap, and once that is fixed the remaining risk is loud.
- **Weigh the guard against the failure it prevents**, not against how embarrassing the bug felt. "I got this wrong twice" is an argument for care, not for infrastructure.

**How to apply:** production and engine paths earn tests. A convenience recipe, a one-off script, or anything whose failure is immediately visible to the person running it generally does not. If tempted, ask whether the failure would be both silent *and* costly — if not, skip it. Consistent with [[feedback_dont_test_upstream_libs]] (don't test what isn't yours to own) and [[feedback_evaluate_ticket_necessity_first_principles]] (evaluate necessity from first principles rather than treating a well-described problem as self-justifying).

Sibling correction from the same review: [[feedback_ask_before_filing_tickets]].
