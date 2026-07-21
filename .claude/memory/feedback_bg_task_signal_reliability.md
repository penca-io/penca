---
name: feedback_bg_task_signal_reliability
description: "Background-task completion signals are unreliable — verify output directly. Never combine shell `&` with the Bash tool's run_in_background, and don't write watcher loops with complex grep conditions."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 98982079-62d6-4537-a3fc-f905ed5e04b3
---

Background-task completion notifications from the Bash tool cannot be the *only* completion signal — re-check the output file directly before trusting them. Two root causes, both observed on Penca work:

1. **`&` + `run_in_background: true` double-backgrounding.** When you launch a long command (`just check`, `cargo build --workspace`) via the Bash tool's `run_in_background: true`, pass the bare command — do NOT also append shell `&`. `cmd & ; echo …` makes the wrapper shell return immediately, so the harness records the *wrapper's* exit code (the trailing echo = 0) and fires the completion notification while the real command is still running — or the real command gets SIGHUP'd when the wrapper exits. On CHA-135 this produced a false "`just check` exit 0" with a log truncated mid-clippy; on CHA-259 the same shape fired "task done" while the work ran unsupervised.
2. **Buggy watcher condition never exits the loop.** A `until [cond]; do sleep 5; done` poller against `roborev status` used `grep -E 'running|queued'`, which matched the "Daemon: running" header in addition to the jobs line, so the count never dropped and the loop ran until killed manually. Don't write watcher loops with complex grep conditions.
   - **`pgrep -f '<pat>'` self-match (CHA-479, cost ~32h of stalled session).** A `while pgrep -f 'just penca-up' >/dev/null; do sleep 15; done` waiter never exited because the **waiter's own command line contains the literal `just penca-up`**, so `pgrep -f` matched its own process forever. The build had finished minutes in; I watched a self-referential loop for a day. Rule: never `pgrep -f`/`grep` a wait-loop on a string that appears in the loop's own command. Wait on a concrete signal instead — a healthcheck (`docker ps` shows the container `(healthy)`), a sentinel line in the log, or `tail --pid=<known-pid>`. And always put a wall-clock deadline on any poll loop (`deadline=$(( $(date +%s) + N )); while [ "$(date +%s)" -lt "$deadline" ] && cond; do …`) so a stuck condition self-terminates instead of hanging the session.

**Why:** the user flagged this directly — "Why didn't you catch that when it finished? Your bg tasks alert unreliably." Same root cause every time: relying on a notification instead of verifying state directly.

**How to apply:**
- For background work, use `run_in_background: true` with the command alone (optionally `2>&1 | tee logfile` for grepping). Capture the real inner exit via `${PIPESTATUS[0]}` if you pipe. Never also append `&`.
- Prefer foreground `Bash` calls for a wait — they block but give unambiguous exit codes. If foreground is too slow, use `tail --pid=<pid> -f /dev/null` against a *known* PID: it exits exactly when the pid dies, no condition logic to get wrong.
- Don't write polling loops with `until grep … do sleep`. Either run unpiped in the foreground (full output, blocking) or use `run_in_background: true` and wait for the system notification — no `until` loop in between.
- When a background task "completes," still re-check the output file (`cat`/`grep` for the expected end marker) before trusting the notification.

Related: [[feedback_capture_test_output_once]], [[feedback_just_check_gate_trust]].
