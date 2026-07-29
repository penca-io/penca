---
name: feedback_commit_before_kata_close_sha
description: "Never chain `kata close --commit $(git rev-parse HEAD)` after `git commit` — pre-commit reformat aborts the commit, leaving a stale SHA"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 0cb0fe8c-b1e3-4176-a680-a0ea91c3cb62
---

When closing a kata task with commit evidence, NEVER chain `git commit` and
`kata close --commit <sha>` in one `;`/`&&` shell line that reads HEAD inline.

**Why:** the repo's pre-commit hooks include `ruff-format` (and
`check_blank_lines --fix`), which **reformat files and then abort the commit**
(pre-commit fails when a hook modifies files). So after a "Format... Failed -
files were modified" line, the commit did NOT happen, HEAD is unchanged, and a
`;`-chained `SHA=$(git rev-parse --short HEAD); kata close --commit $SHA`
records the *previous* commit's SHA against the task. Hit this twice on CHA-423
(closed RT4/zx89/8d3g against wrong SHAs; had to `kata reopen` + re-close).

**How to apply:** sequence it as three separate steps —
1. `git add -A && git commit ...` (let ruff-format reformat + abort if it will),
2. if it aborted, `git add -A && git commit ...` again (now files are already
   formatted, so it passes), and verify `git rev-parse --short HEAD` actually
   moved,
3. THEN `kata close <ref> --done --commit <verified-sha>` as its own command.

Relates to [[feedback_just_check_gate_trust]] (only trust a gate when it truly
passed) and the commit-message rules in the `/do-issue` skill's Step-4 commit guidance (types, and scopes sourced from `linear/labels.toml`).
