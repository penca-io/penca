---
name: feedback_slow_commands_capture_and_wait
description: "Capture every slow run to a logfile on the FIRST invocation and grep the file afterwards; never poll with an until-grep loop, and verify a background task's output directly rather than trusting its completion signal."
metadata:
  node_type: memory
  type: feedback
---

Two halves of one discipline: capture the output once, and don't trust
anything but the output.

## Capture once, grep many times

Any `just integration-test`, `just check`, `cargo test`, `cargo build`, or slow
`pytest` run MUST be piped to a logfile on the **first** invocation —
`just X 2>&1 | tee /tmp/X.log` or `just X > /tmp/X.log 2>&1`. Every follow-up
question is then a `grep` / `rg` / `tail` against the file. Never re-run a
command to ask a different question of the same output.

The user flagged this twice. Integration suites take ~17 min and rebuild
containers; `just check` takes ~30s+. Re-running to extract a different slice
(summary, one failure, a line count) is a multi-minute round-trip for zero new
information — and this applies to **passing** runs exactly as much as failing
ones. The trigger is command *duration*, not "am I debugging." Decide the
logfile name **before** running; don't run naked and capture later. Re-run only
when the source changed since the captured run, or the prior run was killed.

**Anti-pattern — destructive filtering with no `tee`.** `just X 2>&1 | tail -5`
or `... | grep PATTERN` writes only the *filtered* output to the harness capture.
If the command hangs or fails late, there's no record of what happened. Always
`tee` upstream, or redirect the whole thing with `> /tmp/X.log 2>&1`.

**Anti-pattern — chained slow commands with separate pipes.** `just penca-down
2>&1 | tail -3; just penca-up 2>&1 | tail -5` in one call loses the second
half's output. One slow command per call, or `{ cmd1; cmd2; } > /tmp/log 2>&1`.

## Waiting: verify state, never a notification

Background-task completion notifications cannot be the *only* completion
signal — re-check the output file before trusting them.

- **Never combine shell `&` with `run_in_background: true`.** Pass the bare
  command. `cmd & ; echo …` makes the wrapper shell return immediately, so the
  harness records the *wrapper's* exit code and fires "done" while the real
  command still runs — or the command gets SIGHUP'd when the wrapper exits.
  Produced a false "`just check` exit 0" on a log truncated mid-clippy.
  Capture the real inner exit with `${PIPESTATUS[0]}` if you pipe.
- **Don't write `until <grep>; do sleep; done` watchers.** Every instance of
  this has failed on the condition, not the wait. A poller on `roborev status`
  used `grep -E 'running|queued'`, which matches the "Daemon: running" header
  and the "0 queued, 0 running" jobs line no matter what — so it never exited.
  Parse the counts instead. Worse, a `while pgrep -f 'just penca-up'` waiter
  never exited because **the waiter's own command line contains the pattern**,
  so `pgrep -f` matched itself forever; the build had finished minutes in and I
  watched a self-referential loop for ~32 hours of stalled session.
- **Rule:** never `pgrep -f` / `grep` a wait-loop on a string that appears in
  the loop's own command. Wait on a concrete signal — a healthcheck (`docker ps`
  showing `(healthy)`), a sentinel line in the log, or `tail --pid=<known-pid>
  -f /dev/null`, which exits exactly when the pid dies with no condition logic
  to get wrong. Prefer a foreground `Bash` call when you can afford to block:
  unambiguous exit code, nothing to misparse.
- Put a wall-clock deadline on any poll loop that survives all of the above
  (`deadline=$(( $(date +%s) + N ))`) so a stuck condition self-terminates
  instead of hanging the session.

The user's framing: *"Why didn't you catch that when it finished? Your bg tasks
alert unreliably."* Same root cause every time — trusting a notification
instead of verifying state.

Related: [[feedback_just_check_gate_trust]],
[[feedback_integration_suite_full_fresh_before_pr]],
[[feedback_no_background_git_commits]].
