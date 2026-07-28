---
name: project_repo_moved_merge_queue_gate
description: "Repo is now penca-io/penca (PUBLIC) — `fabricdb/fabric` is the prior private home; main is gated by the same merge-queue ruleset (require `CI success`), merges go through \"Merge when ready\"; Linear GitHub auto-close VERIFIED working 2026-06-23 (CHA-444/PR #266 → Done at merge)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 50239920-f2d3-4ec0-9a3f-97916bfac46e
---

**Current home is `penca-io/penca`, and it is PUBLIC** (verified 2026-07-27:
`git remote -v` → `origin git@github.com:penca-io/penca.git`, the only remote;
`gh api repos/penca-io/penca` → `private=false`). That is where work lands now —
PRs #2 and #3 merged there. `fabricdb/fabric` still exists but was last pushed
2026-07-21, before penca-io.

The lineage is `nhobin219/fabric` → `fabricdb/fabric` (2026-06-16, CHA-458) →
`penca-io/penca` (the CHA-150 squash + CHA-151 public flip). **The
fabricdb-specific push/PR recipe at the bottom of this entry does not apply to a
penca-io checkout** — there is no `fabricdb` remote here, so plain
`git push -u origin <branch>` + `gh pr create` is correct.

The gating is replicated on penca-io, so the flow is unchanged:

- **`main` is protected by an active repository ruleset "main merge gate":** requires a PR, requires the **`CI success`** status check (the `ci.yml` aggregator job — the single required context), enables the **merge queue** (MERGE / ALLGREEN), and blocks deletion + force-push. Direct pushes to `main` are rejected.
- **Merging:** a green PR is landed via **"Merge when ready"** → it enters the merge queue, the `merge_group` event runs the **full** CI incl. the Docker integration suite on the prospective merge commit, and the queue auto-merges on green. Integration is **queue-only** (skipped on plain `pull_request` runs; runs in `merge_group` + post-merge `push`). Never `gh pr merge` — the queue (user-initiated) does it. See [[feedback_never_merge_pr]].
- **`penca-io/penca` carries the same ruleset** (verified 2026-07-27): name `main merge gate`, target `branch`, rules `deletion, non_fast_forward, pull_request, required_status_checks, merge_queue`, required context **`CI success`**. Note `gh api repos/.../branches/main/protection` returns 404 "Branch not protected" — the gating is a *ruleset*, not classic branch protection, so that endpoint is the wrong place to look.
- **Merge queue needs Enterprise for private repos** (Team returns 422 on the `merge_queue` ruleset rule; branch protection itself only needs Team). Now moot for penca-io, which is public. Configure rulesets via `gh api -X POST repos/<owner>/<repo>/rulesets`.
- **GitHub→Linear auto-close VERIFIED working (2026-06-23).** First merge through the queue (CHA-444 / PR #266) transitioned the ticket to **Done at merge time** automatically (`completedAt` == merge timestamp) — the linkage is the PR↔issue attachment, not a `Closes CHA-NNN` footer (this PR had none). No manual `save_issue state="Done"` needed anymore. (Prior concern: the repo webhook id 606013406 was subscribed to `push` only; whatever the path, the transition fired correctly.)
- **VM provisioning** (historical, for fabricdb-era VMs) cloned `git@github.com:fabricdb/fabric.git`; old-URL clones still work via GitHub's redirect until someone recreates `nhobin219/fabric`. Update the exe.dev VM image once rather than `git remote set-url` per VM.
- **Historical, fabricdb-era only — check `git remote -v` before applying.** Some VMs had `origin` = the live `nhobin219/fabric` fork (not fabricdb). Verified 2026-07-10 (CHA-504): `git remote -v` → `origin ssh://git@github.com/nhobin219/fabric.git`. On such a VM the naive `git push -u origin <branch>` + `gh pr create` **fails** — it attempts a cross-fork PR (`fabricdb:main ← nhobin219:branch`) and errors `No commits between … / not all refs are readable / Head repository can't be blank`. **Fix that worked:** `git remote add fabricdb ssh://git@github.com/fabricdb/fabric.git`, `git push fabricdb <branch>`, then `gh pr create --repo fabricdb/fabric --base main --head <branch> --body-file …`. Push every follow-up commit (review fixes) to `fabricdb` too, not just `origin`, or the PR won't update. (I have ADMIN on fabricdb/fabric, so pushing feature branches there is allowed; only `main` is gated.)
