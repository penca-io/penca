---
name: review-pr
description: Review a Penca PR against project conventions and architecture
argument-hint: "<PR number or URL>"
allowed-tools: Bash Glob Grep Read
---

# Penca PR Review

Review a pull request against Penca's architecture, conventions, and coding standards.

**PR:** "$ARGUMENTS"

## Scope: review-only, no code changes

Read the PR, post structured review comments on GitHub, and enqueue follow-up work as kata tasks. Do not edit source files, even when the user describes the desired fix during the review conversation. User direction like "I think the right fix is X" or "we shouldn't do Y" is meant to inform updated review comments and queued follow-ups — not to authorize you to apply the fix yourself.

The deliverable of a review is two things in lockstep:
- **Inline GitHub PR comments** on specific diff lines (small / local findings).
- **Kata tasks** under the PR's `cha-NNN` label (the same small / local findings *and* any larger behavior-level findings that warrant a follow-up Linear ticket).

Findings never become Linear comments. Large / behavior-level findings become a kata task whose body says "file follow-up Linear ticket for X"; the human actually files the Linear ticket at drain time. The skill does not call `save_issue` to create follow-ups autonomously.

If the user explicitly asks for code changes mid-review, pause and confirm before touching files; edits go on a separate branch as follow-up work, not as commits to the PR being reviewed.

## Read the conventions up front

Before reviewing, read `docs/style-guide.md` and `docs/development-methodology-guide.md` in full — they are the rubric this review applies, loaded **preemptively**, not on demand. Other references (`docs/algorithms.md`, `docs/services/`, `README.md`) stay on demand.

## What NOT to flag

These look like findings but are settled non-issues. Don't raise them at any severity — and if a previous round flagged one, mark the thread resolved.

- **Proto wire-compat — pre-release.** Penca isn't shipped to clients yet, so there's no installed-base of generated stubs that could mis-decode a re-typed or renumbered field. Tag reuse with a different wire type, field removal without `reserved` markers, and renumbering after a removal are all fine. Don't gate the change, don't add `reserved`, don't suggest renumbering as a follow-up. (This rule retires once Penca ships; the right place to revisit is in this skill body, not via a one-off review finding.)
- **"Phantom delete cleanup at compact"** as a correctness fix. A tombstone in `delete_log` for a `row_uuid` with no upsert anywhere in the row's history is *not* a separate class of bug — the cross-branch propagation case is the same write-write conflict that CHA-5's serializable-write isolation owns. `MutateData`'s contract is unconditional-tombstone-on-row_uuid; whether the source ever held the row is irrelevant to storage. Compact-time phantom cleanup is timing-dependent (only catches phantoms whose compact races merge) and uneven (doesn't address non-phantom WW conflicts). If a PR proposes this, push back and point at CHA-5.
- **"Committed rows + missing files = corruption"** without checking reader-visibility. Before writing "this creates zombie committed rows / dangling references / corruption," verify the read path. The Penca pattern across persist / snapshot / compact is: **parent-or-collective commit is the atomic boundary.** Per-segment commits inside the write loop are operationally visible (internal queries can see them) but user-invisible (reader queries INNER JOIN the parent's committed flag, so partial state is unreachable). Retry / idempotency via deterministic UUIDs + `ON CONFLICT DO NOTHING` re-creates missing files before the final flip, closing the loop. If the design is atomic via last-step commit, the "orphan" rows aren't actually visible to readers — that's not corruption.
- **Deterministic-UUID tie-breaks** on `ORDER BY ... DESC LIMIT 1` over Penca metadata tables. Before suggesting a tie-break column, trace the table's deterministic-UUID derivation (ADR 0016, `crates/penca-core/src/naming.rs` / the Python `naming.py`). If the `ORDER BY` column is one of the `row_uuid_for_pk` inputs for that table's PK *and* the `WHERE` filters by the other inputs, two rows with the same sort value would derive the same UUID and collide on PK (`ON CONFLICT DO UPDATE` collapses them) — the tie is structurally impossible and the `LIMIT 1` is already deterministic. Only suggest a tie-break when the ordering column is NOT load-bearing in the PK derivation. (Caught on PR #79: `table_snapshot_uuid` already derives from `snapshotted_at_micros`, so a `, table_snapshot_uuid DESC` tie-break would never fire.)

## Step 0: Check CI status

```bash
gh pr checks $ARGUMENTS
```

If any checks are failing, note which ones and whether the failures are related to the PR changes. If Rust compilation fails, flag it — there's no point reviewing code that doesn't compile.

## Step 1: Gather PR context

`gh pr view` without `--json` hits the deprecated Projects Classic API and errors. Always use `--json` to select specific fields.

```bash
gh pr view $ARGUMENTS --json title,body,headRefName,baseRefName,files,additions,deletions,changedFiles,commits
gh pr diff $ARGUMENTS
```

If the diff is very large (>5000 lines), pipe through `head -5000` and note that the review is partial. Read the changed files in full (not just the diff) to understand context.

## Step 2: Check conventional commit compliance

Verify all commits follow `<type>(<scope>): <description>`:
- Valid types: feat, fix, refactor, perf, test, docs, build, chore
- Scopes must match `linear/labels.toml`
- Description: lowercase, imperative mood, no period, under 72 chars
- `CHA-XX` footer when a ticket exists

```bash
gh pr view $ARGUMENTS --json commits --jq '.commits[].messageHeadline'
```

## Step 3: Rust-specific checks (if Rust files changed)

For any changes under `crates/`, audit against these checklists:

### 3a: Python reference implementation
Python is the correctness reference; Rust is the production implementation for perf. When reviewing Rust changes to core algorithms (merge-on-read, branch merge, persist, compact, snapshot, storage clients, metadata client):
- Read the corresponding Python source file to verify the Rust port mirrors the same logic, data flow, and correctness guarantees.
- Flag any divergence that changes *behavior* (dropping a query, different merge semantics, altered conflict resolution).
- Do NOT flag divergences that are deliberate Rust improvements (streaming where Python materializes, zero-copy where Python copies, projection pushdown, concurrent execution).
- Rust-only code (Flight SQL, DataFusion integration, gRPC microservice plumbing, config, deployment) has no Python counterpart — skip this check for those areas.

### 3b: Query efficiency audit
- Count SQL queries per method in both Python and Rust versions.
- Flag if the Rust version issues MORE queries than Python (never acceptable).
- Note FEWER queries as an improvement.

### 3c: Streaming vs materialization
- Verify unbounded reads use `fetch_stream` pattern, not `fetch_all`.
- Verify bounded reads (system metadata, small result sets) use materialized fetch.
- Check cold storage reads are per-segment (each fits in memory).

### 3d: DbDriver abstraction
- Verify methods use `&impl DbDriver` not `&dyn DbDriver`.
- Verify optional driver pattern matches Python (`driver = driver or self._driver`).
- Verify no sqlx types leak through the DbDriver trait boundary (except via associated types).

### 3e: Error handling
- Custom error types use `thiserror`.
- No `.unwrap()` or `.expect()` in library code (OK in tests and main.rs).
- Errors propagate with `?`, not manual match blocks.

For a focused second-pair-of-eyes pass on a Rust diff, you can invoke `/code-quality-reviewer` — it captures the same checklist plus type-idiom audit.

## Step 4: General code quality

Audit checklist (language-agnostic):
- No unnecessary `.clone()` calls (check if a borrow would work).
- No heap allocation where stack works (Vec where array suffices, Box where value works).
- SQL identifiers use dialect quoting functions, never string formatting.
- No bind parameters contain user-generated data in system table queries.
- System-generated UUIDs formatted with `format_sql_text_array` or typed UUID params.
- Intermediate structs feeding proto responses follow the hot-path rule: in Rust, owned-and-moved into the proto (no `.clone()` on payload fields when ownership transfer would do); in Python, no materialized list of intermediate dataclasses just to build a parallel list of protos (go row → proto, or yield protos via a generator for streaming RPCs).

## Step 5: Architecture checks

Lightweight architecture checks (greppable patterns):
- Validation logic lives in server/servicer layer, not in library/storage clients.
- No intermediate variable aliasing (inline `settings.x.y` at call sites).
- No method aliasing (inline the full method call, don't assign to short names).
- Imports at top of file, not inside functions.
- **Purge-state consumers read the stored watermark, not a derivation.** Code that needs "the Purge watermark for table T" (tx_log GC, segment GC, cleanup-pass cutoffs) reads `table_purge_metadata.purged_at_micros(T)` directly — flag any `MAX(persisted_at_micros) ... WHERE now - committed_at > grace` expression standing in for it, even when functionally close. "Use `purged_at`, not `persisted_at`" is categorical.
- **`stream_merged` row filters qualify columns with `l.`.** When a `stream_merged` / `resolve_table_metadata` caller passes a `row_filter`, columns must be `l.`-qualified (e.g. `l.row_uuid = '...'`). All three SQL paths (hot, cold, snapshot) expose the same `l` alias; an unqualified column is ambiguous on the join and any other prefix breaks exactly one path. Canonical pattern: `crates/penca-storage-meta/src/table.rs::resolve_table_metadata`.

For cross-bounded-context architecture concerns — bounded-context boundaries, microservice ownership, ADR alignment, CQRS-shaped splits — invoke `/software-architect`. It applies a Fowler-principled checklist (single bounded context per service, CQS vs CQRS, TolerantReader, StranglerFig, DesignStaminaHypothesis) against the diff and returns structured findings.

### Commit-message-vs-diff mechanism audit

Commit message's mechanism claim matches the diff's actual code path. If the message names a canonical mechanism (e.g. `stream_merged`, `apply_changes`, `compact_persist_segments`), verify the new code path actually invokes it — not a parallel resolver, helper, or workaround that merely mentions the symbol. This is the canonical responsibility of `/review-pr`.

## Step 6: Report

Organize findings into these sections. For each finding, include the file path and line number.

### Critical (must fix before merge)
Issues that affect correctness, security, or violate architecture boundaries.

### Important (should fix)
Convention violations, missing patterns, query efficiency regressions.

### Suggestions (nice to have)
Style improvements, performance micro-optimizations, documentation.

### Strengths
What the PR does well. Acknowledge good decisions.

## Step 7: Post review to GitHub

Post the review using the GitHub API with inline line-level comments on specific diff lines.

### 7a: Get the latest commit SHA

```bash
gh pr view $ARGUMENTS --json commits --jq '.commits[-1].oid'
```

### 7b: Build and submit the review

The `event` field controls the review type:
- `"REQUEST_CHANGES"` — critical or important issues found (comments must be resolved before merge)
- `"COMMENT"` — suggestions only, no blocking
- `"APPROVE"` — no critical or important issues

```bash
gh api repos/{owner}/{repo}/pulls/{number}/reviews \
  --method POST \
  --field commit_id="<SHA>" \
  --field event="REQUEST_CHANGES" \
  --field body="## Penca PR Review Summary

<high-level summary of findings>" \
  --field comments='[
    {
      "path": "crates/penca-storage-cold/src/lib.rs",
      "line": 42,
      "body": "**Important**: This helper function may be unnecessary indirection..."
    }
  ]'
```

For multi-line comments, use `start_line` and `line` to highlight a range.

### 7c: Comment placement rules

- **Critical and Important** findings: always post as inline review comments on the specific lines. These create resolve-before-merge threads.
- **Suggestions**: post as inline comments if they target a specific line, otherwise include in the review body summary.
- **Strengths**: include in the review body summary only.

### 7d: Determine the correct line numbers

The `line` field refers to the line number in the **new version** of the file (the right side of the diff). Use the diff output from Step 1 to map findings to the correct line numbers. Lines prefixed with `+` in the diff are new lines — use the line number shown in the `@@` hunk header plus the offset within the hunk.

### 7e: Fallback

If the review has no findings that target specific lines, use `gh pr review` instead:

```bash
gh pr review $ARGUMENTS --comment --body "$(cat <<'EOF'
## Penca PR Review

<full review here>
EOF
)"
```

## Step 8: Enqueue findings as kata tasks

After the GitHub review is posted, mirror each finding into the PR's `cha-NNN` kata queue so the drain loop in `/do-issue` Step 7 picks them up.

### 8a: Extract `cha-NNN` from the PR

The PR's `cha-NNN` is the slug used for both the inline-GitHub-comment finding and any follow-up kata tasks. Sources, in order of preference:

```bash
# Head ref shape: nhobin219/cha-NN-...
gh pr view $ARGUMENTS --json headRefName --jq '.headRefName' | grep -oE 'cha-[0-9]+'

# Fallback: PR body's "Closes CHA-NN" footer
gh pr view $ARGUMENTS --json body --jq '.body' | grep -oE 'CHA-[0-9]+' | head -1 | tr 'A-Z' 'a-z'
```

If neither shape yields a `cha-NNN`, halt and ask the user — kata tasks must be scoped to a ticket for the drain loop to pick them up.

### 8b: Per-finding kata task

For each Critical / Important / Suggestion finding, emit one kata task. The task body is the same prose as the inline GitHub comment (so the human reading the kata task has all the context); the `idempotency-key` is content-stable so re-reviews on the same PR don't duplicate. **Findings are auto-approved** (`--label approved`) so the `/do-issue` drain picks them up without a human gate — the initial development plan is the only thing that still passes through `plan-draft`:

```bash
key="pr${PR_NUMBER}:${FINDING_FILE}:${FINDING_LINE}:$(printf '%s' "$FINDING_TITLE" | sha256sum | cut -c1-12)"
case "$SEVERITY" in
  critical|important) prio=1 ;;
  *)                  prio=2 ;;
esac
out=$(kata create \
  --label "$CHA" \
  --label review-pr \
  --label approved \
  --priority "$prio" \
  --idempotency-key "$key" \
  --body "$FINDING_BODY" \
  --json \
  -- "$FINDING_TITLE")
new_ref=$(printf '%s' "$out" | jq -r '.qualified_id // empty')

# Dynamic blocker extension: every still-open /do-issue orchestration task
# under this CHA gains the new finding as a blocker, so PR open / post-open
# review wait on the finding before unblocking. `kata edit --blocked-by` is
# additive + idempotent. No-op when /review-pr is invoked outside /do-issue
# (no orch:* tasks exist).
if [ -n "$new_ref" ]; then
  for orch in orch:run-cleanup orch:open-pr orch:spawn-review; do
    orch_ref=$(kata list --label "$CHA" --label "$orch" --status open --json 2>/dev/null \
      | jq -r '.issues[0].qualified_id // empty')
    [ -n "$orch_ref" ] && kata edit "$orch_ref" --blocked-by "$new_ref" >/dev/null
  done
fi
```

Idempotency note: `kata create --idempotency-key` errors on fingerprint mismatch (same key, different content). If a re-review adjusts the wording of a finding, the kata create call will fail; that is intentional — the re-reviewer should either re-key (`pr<N>:<file>:<line>:<new-hash>`) or update the existing kata task in place via `kata edit`. Don't paper over the conflict.

### 8c: Large / behavior-level findings

If a finding is too large for inline-comment-resolution (e.g. "the stream_merged path needs a structural rewrite, not a code-level fix in this PR"), the kata task body becomes:

> **File follow-up Linear ticket for:** `<one-sentence summary of the problem>`
>
> Context: `<paragraph or bullets pulled from the inline review comment>`

The skill does **not** call `save_issue` to create the follow-up Linear ticket. That's the human's call at drain time — they read the kata task, decide if it's worth a Linear ticket, and file it themselves. Mechanism non-goal: no autonomous follow-up ticket creation from the review skill.

### 8d: Strengths

`Strengths` findings (positive callouts in the review body summary) do NOT get kata tasks — they're informational and have no action item.

## Re-reviews

When asked to re-review a PR, only review what changed since the last review.

### R1: Get the interdiff

```bash
gh pr view $ARGUMENTS --json commits --jq '.commits[-1].oid'
git diff <last_reviewed_sha>..<latest_sha> -- crates/
```

Save to a temp file if large: `git diff ... > /tmp/prN_interdiff.txt`.

### R1.5: Fetch author replies before drafting findings

A previous-round finding may have been *addressed by argument* in the comment thread rather than fixed in code. Re-flagging it without reading the reply wastes the author's time and undermines the review.

```bash
gh api repos/{owner}/{repo}/pulls/{number}/comments
gh api repos/{owner}/{repo}/issues/{number}/comments
```

For each unresolved thread carrying forward, read the author's reply. Treat a substantive reply as either:
- **Resolution-by-argument** — drop the finding (and resolve the thread per R3 once the argument holds up).
- **Guidance on what counter-argument to make** — sharpen the next round's finding so it engages with the reply rather than repeating the original.

Never carry a round-1 finding into round 2 without acknowledging the round-1 reply.

### R2: Review only new changes

Focus on:
- Whether previously flagged issues were addressed.
- Any new issues introduced by the fix commits.
- Don't re-flag things that haven't changed.

### R3: Resolve addressed review threads

After confirming a previously flagged issue has been fixed, resolve its review thread. Both `Bash(gh api repos/*)` and `Bash(gh api graphql:*)` need to be on the project's `Bash` permission allow list — `.claude/settings.local.json` carries them.

```bash
gh api graphql -f query='
query {
  repository(owner: "{owner}", name: "{repo}") {
    pullRequest(number: {number}) {
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          comments(first: 1) { nodes { body } }
        }
      }
    }
  }
}' --jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false) | {id, comment: .comments.nodes[0].body[:100]}'
```

```bash
gh api graphql -f query='
mutation {
  resolveReviewThread(input: {threadId: "<THREAD_ID>"}) {
    thread { id isResolved }
  }
}'
```

Resolve threads one at a time after verifying the fix in the interdiff. Do NOT resolve threads whose issues are still present.

### R4: Post the re-review

Use the same Step 7 process. Title the review body `## Penca PR Review — Re-review` (or `Re-review #N` for subsequent rounds). The body should:
- List each previously flagged issue with ✓ (addressed) or ✗ (still open).
- Report any new findings from the interdiff.
- Include strengths if the fix commits are well done.
