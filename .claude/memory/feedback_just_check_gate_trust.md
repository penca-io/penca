---
name: feedback_just_check_gate_trust
description: "Trust the `just check` gate only when it truly passed — run it locally before pushing any CI-green fix, and when streaming it via Monitor, only the stream-end event is the pass signal (mid-pipeline 'All checks passed!' lies)."
metadata:
  type: feedback
---

Two failure modes around believing `just check` passed when it didn't:

**1. Run it locally before pushing any CI-green fix.** Before pushing a fix targeted at unblocking a red CI run (clippy, fmt, test, etc.), run `just check` locally first and confirm exit 0 — not just the targeted recipe (e.g. `just cargo-clippy` alone). The user has had to point out repeated CI-only failures across consecutive pushes (clippy `too_many_arguments`, then pre-existing `cargo fmt` drift) that a single local `just check` would have caught. Pushing a narrow fix without the full gate trades one red CI run for another. Applies to direct-to-main fixes especially; PR branches get CI as a safety net but you still owe the user the faster signal.

**2. When streaming `just check` via Monitor, only the stream-end event is the gate result.** Do NOT treat an `"All checks passed!"` event as the gate result — that line is pytest's per-suite finish, which fires when the Python tests complete, and pytest typically runs *early* in the pipeline, BEFORE the trailing cargo-fmt-check / clippy / cargo-check phases. The only definitive pass signal is the Monitor's **stream-end event** (`status=completed` with the exit-code summary, or the absence of any error events before stream-end when the filter is scoped to failures). Burned on CHA-308: streamed `just check`, got `"All checks passed!"` early, declared green, pushed, opened PR #138 — late events showed `cargo-fmt-check` actually FAILED with exit 1. Order-of-arrival in the buffered pipe inverted the conclusion.

**How to apply:**
- Any commit whose purpose is to make CI green runs through `just check` (exit 0) before commit/push. Capture output to a logfile per [[feedback_capture_test_output_once]] so follow-up greps don't re-run the suite.
- When streaming via Monitor, wait for the **stream-end notification** before treating the gate as green; ignore any mid-stream `"All checks passed!"`. For one-shot "did the gate pass" verification, prefer `Bash` with `run_in_background` and grep for `` recipe `.*` failed `` after exit — single notification, unambiguous result (see [[feedback_bg_task_signal_reliability]]).
