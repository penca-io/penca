---
name: Capture test output once, grep many times
description: Any slow command (>~10s) goes to `/tmp/<name>.log` on the first run — grep the log for every follow-up, never re-run to ask a different question.
type: feedback
originSessionId: afda47ed-4a41-4d8b-b430-dc7dd648fdc4
---
**Rule (no exceptions):** any `just integration-test`, `just check`, `cargo test`, `cargo build`, `pytest` (slow suite), or other multi-second run MUST be piped to `/tmp/<name>.log` on the **first** invocation — `just X 2>&1 | tee /tmp/X.log` or `just X > /tmp/X.log 2>&1`. After that, every follow-up question is a `grep` / `rg` / `tail` against the file. Never run the same command twice in a row to ask a different `grep` question, regardless of whether the first run passed or failed.

**Why:** User flagged this directly, twice. Integration suites take ~60s and rebuild Docker containers; `just check` takes ~30s. Re-running just to extract a different slice of the same output (summary, specific failure, line count) is a multi-minute round-trip for zero new information. This applies to passing runs as much as failing ones — the output is the same either way, and the log already has the answer.

**How to apply:**
- **Before** running any slow command, decide the logfile name and include `> /tmp/<name>.log 2>&1` (or `| tee`) in the command line. Don't run "naked" first and capture later.
- The trigger is **command duration**, not "am I debugging." A passing test whose summary I want to see is the same case as a failing test whose error I want to dig into.
- Follow-ups: `grep -E "passed|failed|FAILED" /tmp/X.log`, `tail -50 /tmp/X.log`, `rg "thread .* panicked" /tmp/X.log`. Pipe through `less` or `head` if huge.
- Re-run only when source has changed since the captured run, OR when the prior run was killed/incomplete.
- If I forgot to capture and need a different grep, the cost is real — but **don't compound it** by re-running without `tee` this time.

**Anti-pattern — destructive filtering before the harness capture:** `just X 2>&1 | tail -5` (without `tee`) or `... | grep PATTERN` writes only the *filtered* output to the harness's capture file. If the command hangs, fails late, or never reaches the lines that match, I have no record of what actually happened — and `Read`-ing the harness output file shows only the filtered tail. The whole point of capturing once is undermined. Always `tee /tmp/X.log` (writes both to stdout for the live view AND the file in full) or redirect with `> /tmp/X.log 2>&1` (writes the full output to the file, no harness view). Never pipe to `tail`/`grep` as the terminal stage unless I also `tee` upstream.

**Anti-pattern — chained commands with separate pipes:** `just penca-down 2>&1 | tail -3; just penca-up 2>&1 | tail -5` chained in a single Bash call (especially `run_in_background: true`) loses the second half's output entirely — the harness captures only what each pipe yields, and a chained-command-with-tails leaves no record of whether the second half ran. Always one command per Bash call when each is slow, OR use `{ cmd1; cmd2; } > /tmp/log 2>&1`.

**Anti-pattern — polling loops with `until grep ... do sleep`:** wastes wall-clock on "is it done yet?" checks. The harness already notifies on background-task completion. Either run unpiped in the foreground (full output, blocking, no polling needed) or use `run_in_background: true` and wait for the system notification — no `until` loop in between.
