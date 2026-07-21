---
name: tickets-are-spirit-not-spec
description: "Linear tickets convey the spirit of a change, not precise instructions — the /do-issue planning phase must derive the best mechanism, probing design alternatives with the user, not mechanism-bind to the ticket's literal text"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 279d2837-6736-45c1-9d10-0841cb37fccb
---

Linear ticket descriptions (including their "Mechanism" sections) give the *spirit* of the desired change, not precise instructions. The planning phase exists to figure out the actual best implementation.

**Why:** On CHA-405 the ticket said "keep-last-N retention" and the plan bound to it literally (config knob, ranked LIMIT N query). The user's actual intent: snapshots are a read cache — retire everything except the latest; bounded history/time-travel retention belongs to persist compaction (baseline + pruned log), a separate design. Taking the ticket text as spec produced a wrong-shaped plan that survived /plan-reviewer (which checks mechanism-binding, not design fit).

**How to apply:** During /do-issue Step 1–2, treat ticket mechanism text as one candidate design. Ask "is this mechanism the right one given the architecture?" and surface alternatives/design questions to the user at or before the Step-3 gate, especially when the ticket's mechanism implies a new user-facing knob or policy. Related: [[feedback_discuss_before_implementing]].
