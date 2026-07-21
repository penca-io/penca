---
name: feedback-gh-pr-edit-broken
description: gh pr edit silently fails on PR description edits (GraphQL deprecation error). Use gh api PATCH for body updates.
metadata:
  type: feedback
---

`gh pr edit <n> --body ...` returns exit code 1 and **does not apply
the edit** in this env. The only stdout is:

```
GraphQL: Projects (classic) is being deprecated in favor of the new
Projects experience, see: …  (repository.pullRequest.projectCards)
```

Looks like a warning but is fatal: the underlying GraphQL response
carries that as an `errors` field, gh treats any errors field as a
failure, and the mutation rolls back. The PR body is unchanged even
though gh ran to completion-ish.

**Why:** GitHub announced sunsetting of classic Projects, and gh's
GraphQL query for `pullRequest` still requests `projectCards` which
emits the deprecation error. Until gh stops querying that field
(GitHub CLI release post-sunset), `gh pr edit` is broken for body /
title edits.

**How to apply:** Use the REST API directly:

```bash
gh api -X PATCH repos/<owner>/<repo>/pulls/<n> \
  -f body="$body" \
  --jq '.html_url'
```

(Or `-f title="..."` for the title.) `gh api` doesn't fetch the
classic-projects field, so the deprecation doesn't fire and the
mutation lands.

**Confirm the edit landed by viewing the body back** — `gh pr view
<n> --json body --jq '.body'`. Don't trust gh's own exit code on
this path.

Tripped on CHA-259 (PR #151) when adding the CHA-333 follow-up
pointer to the PR description.
