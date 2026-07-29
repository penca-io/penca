---
name: feedback_absence_is_not_evidence
description: A negative or passing result from a check whose output you did not read is not evidence — three distinct failures of this in one session
metadata: 
  node_type: memory
  type: feedback
  originSessionId: cf719e67-ea5c-402e-9fd1-f1fc03c24af6
  modified: 2026-07-29T17:32:26.011Z
---

**A check whose raw output you did not read proves nothing** — not when it
returns "no matches", not when it exits 0. Re-run it so the output is visible
before asserting anything on it, especially before writing the conclusion into a
plan, a doc, or a commit message.

**Why:** CHA-513 hit this three separate ways in one session, each costing real
work:

1. **Glob that never matched.** `grep -r 'ORDER BY' crates/penca-storage-meta/src/**/*.rs`
   returned nothing, so I recorded "neither dirty-set enumeration is ordered" in
   the plan and used it to justify an approved non-goal. Without `shopt -s
   globstar`, `src/**/*.rs` does **not** match `src/lifecycle.rs` — both
   `ORDER BY MAX(...) ASC` clauses were sitting there. That false negative was
   one review round away from shipping a permanent-starvation bug.
2. **Suppressed stderr + `&&`.** `kata comment <ref> "text" >/dev/null 2>&1 && echo works`
   printed `works`, so I wrote that invocation into a skill. Run visibly it is
   `kata: accepts 1 arg(s), received 2`, exit 2 — the body needs `--body`.
3. **Predicate that could never be true.** `roborev status | grep -cE 'queued|running'`
   compared against `"0"` never terminates: the `Daemon: running` and
   `Jobs: 0 queued, 0 running` lines always match. Two hours idle on a loop
   waiting for work that had already finished. Parse the numbers
   (`sed -n 's/^Jobs:[[:space:]]*\([0-9]*\) queued, \([0-9]*\) running.*/\1\2/p'`),
   don't `grep -c` the whole output.

**How to apply:** before claiming something is absent, broken, or verified —
print the command's real output and read it. Prefer `-r <dir>` over `**` globs;
never pipe a verification's stderr to `/dev/null`; make wait-predicates parse
values rather than count matching lines. If a search is what justifies a *plan
decision*, quote the file:line you actually read, not the empty result.

Related: [[feedback_slow_commands_capture_and_wait]],
[[feedback_just_check_gate_trust]].
