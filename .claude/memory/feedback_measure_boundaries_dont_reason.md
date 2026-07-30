---
name: feedback_measure_boundaries_dont_reason
description: "On this codebase's boundary conditions my reasoning is unreliable and measurement is cheap — dump the actual state before committing a fix, and red-verify."
metadata:
  node_type: memory
  type: feedback
---

Fixes that are **mechanically right but wrong at the boundary** are my dominant
failure mode here. Across CHA-539 roughly ten landed this way. Each compiled
clean, passed clippy, and passed a targeted test run:

- which **side of a join** a predicate lands on (data side vs `tx_log` side)
- which **axis** bounds a window (seq vs micros; segment selection vs per-row)
- which **tier** carries the data (persist vs snapshot — a fixture where the
  snapshot covered the whole persist)
- **inclusive vs exclusive** (`watermark + 1` vs the copied segments' actual floor)
- lock **order** vs lock **strength** vs lock **completeness** vs lock **scope**
  (`LOCK TABLE` vs `LOCK TABLE ONLY`)
- **row identity vs slice identity** for a cache key, then a fingerprint that
  wasn't **injective**
- an error path whose `map_err` **erased the variant** the match keyed on, so the
  mechanism was dead on its main path

Not one was caught by re-reading my own code. They were caught by reviewers, by
dumping real state, and by the user asking a question.

**Why:** these are all "I reasoned about which side/axis/tier applies" rather than
"I looked". The reasoning is plausible enough to survive review-by-author, and the
compiler and a scoped test run can't see it — a wrong boundary is still
well-typed and still passes the tests that never exercised the other side.

**How to apply:** before committing a fix that turns on a boundary, name the
single observation that would distinguish right from wrong, and go get it.
Concretely, what worked:

- **Dump the actual state.** A 3-minute query against `table_persist_segment_metadata`
  settled what two rounds of source-reading got wrong — it showed the parent
  holding *two* segments carrying the same row, which no amount of reading the
  copy logic would have revealed.
- **Red-verify.** Run the new test against the pre-fix binary. The one time I did
  (by accident — the image predated the fix), it produced the exact predicted
  error. Every other test I added was only ever observed green, which proves
  nothing about whether it would fail.
- **Re-run what you just changed.** Twice I added a guard or an assertion and
  never re-ran it; once that guard then failed the full-suite gate.

If a fix can't be measured or red-verified, say so explicitly rather than
reporting it as verified — "compiles and the suite is green" is weak evidence for
a boundary change.

Related: [[feedback_absence_is_not_evidence]] (a check whose output you didn't
read proves nothing) — this is its sibling: a check you never ran because the
reasoning felt sound.
