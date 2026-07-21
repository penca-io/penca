#!/usr/bin/env bash
# Bridge: roborev `review.completed` hook → one kata task per non-clean review.
#
# Wired in .roborev.toml under [[hooks]]. Roborev's daemon invokes this after a
# per-commit review finishes. Roborev v0.56.0 passes the hook NO arguments, NO
# `ROBOREV_*` env, and NO stdin — there is no event payload to parse. So instead
# of waiting to be told which review completed, we resolve finished review jobs
# ourselves:
#
#   roborev list --status done --json   → finished jobs on the current branch
#   roborev show --json <job-id>        → .verdict_bool, .output, .job.*
#
# We process ALL finished jobs on the branch, not just the latest: the daemon
# gives us no way to know which review just completed, and under the autonomous
# /do-issue drain commits land back-to-back, so two reviews can both reach
# `done` before either hook fires — a latest-only resolver would enqueue the
# newer twice and drop the older silently. The idempotency key makes
# reprocessing already-enqueued jobs a no-op, so iterating the whole (small,
# branch-scoped) set is safe. Two deliberate trade-offs of the sweep:
#   - It is O(branch history) per fire, bounded by `roborev list`'s 50-job
#     window (jobs older than the most-recent 50 fall off, but were enqueued on
#     earlier fires). Cheap enough for a per-commit hook on a feature branch; a
#     persisted last-seen marker would trade this for added state.
#   - The orch `--blocked-by` extension runs ONLY for newly-created findings
#     (kata `changed:true`), never on reprocess — otherwise an idempotent
#     re-fire would re-attach a blocker a human/drain had deliberately removed
#     to let orchestration proceed, silently re-stalling the PR.
#
# We read the SHA/branch from each job record (not git HEAD), so a job in the
# returned set is never misattributed to whatever HEAD currently points at.
# Note the residual assumption: the job SET comes from `roborev list`, which
# defaults to the current-branch + current-repo (50-job) scope, so this relies
# on HEAD still being on the reviewed branch when the hook fires — which holds
# for the normal post-commit → async-review → completion flow. If something
# checked out a different branch between commit and review completion, that
# review's job would fall outside the list entirely; `roborev list` exposes no
# all-branches sweep to close that gap cleanly, and it is not worth the
# complexity for a per-commit hook.
#
# roborev exposes no structured findings array — a review is free-text markdown
# in `.output` plus an integer `.verdict_bool` (1 = clean, 0 = issues found). So
# we enqueue ONE kata task per non-clean review, carrying `.output` as the body,
# scoped by the `cha-NNN` label from the reviewed branch. Per-finding fan-out is
# not achievable against this roborev.
#
# Each task is enqueued with `--label approved` so it joins the /do-issue drain
# without a human gate. The bridge then walks any still-open orchestration tasks
# (`orch:run-cleanup`, `orch:open-pr`, `orch:spawn-review`) under the same
# `cha-NNN` and extends their `--blocked-by` to include the new task — so PR open
# / post-open review wait on inbound roborev findings instead of racing past
# them. See `.claude/skills/do-issue/SKILL.md`.
#
# Idempotency: --idempotency-key "<short-sha>:<job-id>". Reprocessing the same
# review job is a no-op (kata returns changed:false); a genuine re-review
# produces a new job id, hence a fresh task.
#
# Logging: normal no-ops (clean review, non-cha branch, no jobs) log quietly to
# /tmp/roborev-kata-hook.log. Structural surprises — a non-clean review missing
# an expected field, a create returning no ref — go through warn(), which also
# writes to stderr so the daemon surfaces them: a silent revert to "enqueues
# nothing" is the exact failure this bridge exists to prevent. We still exit 0
# unconditionally so a broken adapter never blocks roborev's review pipeline.

set -uo pipefail

LOG=/tmp/roborev-kata-hook.log
exec 3>>"$LOG"
log()  { printf '[%s] %s\n' "$(date -Iseconds)" "$*" >&3; }
warn() { printf '[%s] ANOMALY: %s\n' "$(date -Iseconds)" "$*" | tee -a /dev/fd/3 >&2; }

REPO_PATH=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$REPO_PATH" 2>/dev/null || { warn "cannot cd $REPO_PATH"; exit 0; }

# Finished review jobs on the current branch (roborev list is branch-scoped),
# oldest-first so back-to-back reviews enqueue in commit order.
JOB_IDS=$(roborev list --status done --json 2>>"$LOG" \
  | jq -r 'if type=="array" then (sort_by(.finished_at)[] | .id) else empty end' 2>/dev/null)
[ -z "$JOB_IDS" ] && { log "no finished review jobs on this branch; exit"; exit 0; }

processed=0
created=0
while IFS= read -r JOB_ID; do
  [ -z "$JOB_ID" ] && continue

  REVIEW_JSON=$(roborev show --json "$JOB_ID" 2>>"$LOG" || echo "")
  [ -z "$REVIEW_JSON" ] && { warn "roborev show --json $JOB_ID returned nothing"; continue; }

  # verdict_bool: 1/true = clean (no task), 0/false = issues found. Anything else
  # (null / missing / errored review) is skipped rather than enqueued as noise.
  # Read it WITHOUT `// empty`: jq's `//` treats both null AND boolean false as
  # absent, so `.verdict_bool // empty` would collapse a non-clean `false` to ""
  # and silently drop the review — the very failure this hook fixes. Plain
  # `.verdict_bool` yields "false"/"true"/"0"/"1"/"null", all distinguishable.
  VERDICT_BOOL=$(printf '%s' "$REVIEW_JSON" | jq -r '.verdict_bool' 2>/dev/null)
  case "$VERDICT_BOOL" in
    1|true)  continue ;;   # clean → normal no-op (quiet)
    0|false) ;;            # non-clean → enqueue
    *)       warn "job $JOB_ID: verdict_bool='$VERDICT_BOOL' missing/unexpected (schema drift?); skipping"; continue ;;
  esac

  # .job.git_ref is the commit SHA. (.job.commit_id is a numeric row id, NOT the
  # hash — do not use it.)
  SHA=$(printf '%s' "$REVIEW_JSON" | jq -r '.job.git_ref // empty' 2>/dev/null)
  [ -z "$SHA" ] && { warn "job $JOB_ID: non-clean review but no .job.git_ref (schema drift?); skipping"; continue; }
  SHORT=${SHA:0:12}
  SHORT8=${SHA:0:8}

  BRANCH=$(printf '%s' "$REVIEW_JSON" | jq -r '.job.branch // empty' 2>/dev/null)
  CHA=$(printf '%s' "$BRANCH" | grep -oE 'cha-[0-9]+' | head -1)
  [ -z "$CHA" ] && continue   # not a cha-NNN branch → normal no-op (quiet)

  OUTPUT=$(printf '%s' "$REVIEW_JSON" | jq -r '.output // empty' 2>/dev/null)
  [ -z "$OUTPUT" ] && { warn "job $JOB_ID: non-clean review but empty .output (schema drift?); skipping"; continue; }

  # Severity → priority: scan the review markdown for `**Severity**: <level>`
  # lines and take the highest. Default to medium (2) when none parse.
  SEVERITIES=$(printf '%s' "$OUTPUT" | grep -oiE '\*\*severity\*\*[*: ]+(critical|high|medium|low)' 2>/dev/null \
    | grep -oiE '(critical|high|medium|low)' | tr '[:upper:]' '[:lower:]')
  if printf '%s' "$SEVERITIES" | grep -qE 'critical|high'; then
    prio=1
  elif printf '%s' "$SEVERITIES" | grep -q 'medium'; then
    prio=2
  elif printf '%s' "$SEVERITIES" | grep -q 'low'; then
    prio=3
  else
    prio=2
  fi

  SUBJECT=$(printf '%s' "$REVIEW_JSON" | jq -r '.job.commit_subject // empty' 2>/dev/null)
  [ -z "$SUBJECT" ] && SUBJECT="$SHORT8"
  TITLE="roborev: ${SUBJECT} (${SHORT8})"
  TITLE=${TITLE:0:72}

  # Auto-approved (--label approved): the task joins the /do-issue drain without
  # a human gate. Body is the full review markdown via --body-stdin so multi-line
  # content survives. Capture stdout only — folding stderr in (2>&1) would make
  # the blob invalid JSON and silently empty the ref below; stderr → log instead.
  out=$(printf '%s' "$OUTPUT" | kata create \
    --label "$CHA" \
    --label roborev \
    --label approved \
    --priority "$prio" \
    --idempotency-key "${SHORT}:${JOB_ID}" \
    --body-stdin \
    --as roborev \
    --json \
    -- "$TITLE" 2>>"$LOG")
  processed=$((processed + 1))

  # kata create --json shape: {changed, event, issue:{id, short_id, uid, ...}}.
  # There is NO qualified_id field — build the ref from .issue.short_id.
  SHORT_ID=$(printf '%s' "$out" | jq -r '.issue.short_id // empty' 2>/dev/null)
  if [ -z "$SHORT_ID" ]; then
    warn "job $JOB_ID: kata create returned no .issue.short_id; orch extension skipped → $(printf '%s' "$out" | head -c 200)"
    continue
  fi
  NEW_REF="penca#${SHORT_ID}"

  # Only newly-created findings extend orch blockers. A reprocessed (idempotent)
  # finding must NOT re-touch --blocked-by: that would resurrect a blocker a
  # human/drain deliberately removed to unblock orchestration. See header.
  if [ "$(printf '%s' "$out" | jq -r '.changed // empty' 2>/dev/null)" != "true" ]; then
    log "kata create key=${SHORT}:${JOB_ID} → $NEW_REF (idempotent; already enqueued, no re-extend)"
    continue
  fi
  created=$((created + 1))
  log "kata create key=${SHORT}:${JOB_ID} prio=$prio title=\"$TITLE\" → $NEW_REF (new)"

  # Dynamic blocker extension: extend every still-open /do-issue orchestration
  # task under this CHA with the new finding as a blocker, so PR open / post-open
  # review wait on it. Resolve each orch task with a jq filter on the labels
  # array rather than `kata list --label $CHA --label $orch`: observed kata
  # behavior does not AND-intersect repeated --label flags (it returned every
  # cha task here, which self-blocked the finding), and kata's own --help claims
  # AND-logic — the jq filter is correct and robust either way. We exclude the
  # finding's own ref so it can never block itself.
  for orch in orch:run-cleanup orch:open-pr orch:spawn-review; do
    orch_ref=$(kata list --label "$CHA" --status open --json 2>>"$LOG" \
      | jq -r --arg o "$orch" --arg self "$NEW_REF" \
          '.issues[] | select(.labels | index($o)) | select(.qualified_id != $self) | .qualified_id' 2>/dev/null \
      | head -1)
    [ -z "$orch_ref" ] && continue
    kata edit "$orch_ref" --blocked-by "$NEW_REF" >/dev/null 2>>"$LOG" \
      && log "extended $orch_ref --blocked-by $NEW_REF" \
      || warn "failed to extend $orch_ref --blocked-by $NEW_REF"
  done
done <<< "$JOB_IDS"

log "done: processed $processed non-clean review(s), created $created task(s)"
exit 0
