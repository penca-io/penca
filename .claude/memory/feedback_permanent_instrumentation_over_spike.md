---
name: feedback_permanent_instrumentation_over_spike
description: "Make useful diagnostic instrumentation permanent + off-by-default, not throwaway spike code"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: c32e62f2-d35e-4604-82fa-6d04922200a9
---

When instrumentation built for a one-off investigation is reusable for future debugging (e.g. per-phase span timing that decomposes a read), make it a **permanent, off-by-default affordance** rather than spike code to revert.

**Why:** the CHA-352/353 read-latency traces were written as spike code and stripped; CHA-353 then had to rebuild the same decomposition from scratch. The user asked "if these logs are useful, should we leave them as trace level so we don't do this every time?" — yes.

**How to apply:**
- Gate output behind an env toggle (e.g. `PENCA_SPAN_TIMING` → `FmtSpan::CLOSE` in `penca-observability`) so it's silent + zero-overhead by default.
- Put fine-grained spans at `level = "trace"` so they're dormant under the default `penca=debug` filter and opt-in via `…=trace`. A `#[instrument]` span with no events inside produces no output unless span-timing is on AND the level passes — so cost-when-off is ~a cheap enabled-check.
- Write the comment as rationale, not "SPIKE (revert)".
- It becomes a real deliverable — decide whether it rides in the feature PR or its own small observability change.

Related: [[feedback_capture_test_output_once]].
