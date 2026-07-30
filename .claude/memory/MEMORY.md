# Memory Index

## Project state — read first
- [project_branching_main_only_mvp_single_base.md](project_branching_main_only_mvp_single_base.md) — MVP branching is MAIN-ONLY (fork only from `main`, CHA-515 guard); fork-off-a-fork DEFERRED to CHA-509. Single-base invariant holds everywhere incl. CHA-507 — don't generalize to a chain.
- [project_branch_create_flush_to_cold.md](project_branch_create_flush_to_cold.md) — TARGET per-branch = fully isolated stack, only S3 shared. Isolated stack UNSHIPPED; the read-fix half LANDED — `create_branch` calls `persist_branch`, forks read the parent's cold (2026-07-27). Fork flush is Persist-ONLY, never Snapshot. Obsoletes CHA-67 v1/v2.
- [project_cha433_plan_time_retention_floor.md](project_cha433_plan_time_retention_floor.md) — CHA-433 plan-time retention floor (`as_of<floor` → FAILED_PRECONDITION) IN FLIGHT; retention is schema-broadest. Enforce in read_data/plan_audit, not plan().
- [project_cha432_durable_substrate.md](project_cha432_durable_substrate.md) — CHA-432 snapshot durability substrate MERGED (PR #308): `durable` flag, sticky `decide_durable`, retention helpers. 432 ships the helper only — enforcement is CHA-433's.
- [project_repo_moved_merge_queue_gate.md](project_repo_moved_merge_queue_gate.md) — Repo is `penca-io/penca` (PUBLIC); `fabricdb/fabric` recipes do NOT apply (plain `origin` push + `gh pr create`). `main merge gate` ruleset, queue-only integration, no direct pushes.
- [reference_penca_io_git_identity_gotcha.md](reference_penca_io_git_identity_gotcha.md) — Repo-local git identity lives in `.git/config` and no bootstrap sets it; an unset checkout auto-authors as `exedev@<dev-vm>`. Set + verify on every fresh clone / new VM.
- [project_oss_license_apache2_open_core.md](project_oss_license_apache2_open_core.md) — Apache-2.0, everything in-repo is open core (moat = closed control plane). Visibility flip + announce are HUMAN-ONLY.

## Behavioral — how to collaborate with this user
- [feedback_never_merge_pr.md](feedback_never_merge_pr.md) — NEVER merge a PR; "get it merged" = finalize, report ready-to-merge, STOP.
- [feedback_no_subagents.md](feedback_no_subagents.md) — Don't delegate via the Agent tool. EXCEPTION: /do-issue `orch:spawn-review` — DO spawn the Opus `/review-pr` subagent.
- [feedback_autonomous_drain_no_checkins.md](feedback_autonomous_drain_no_checkins.md) — During an autonomous drain, bank task after task; halt only for course-changing issues.
- [feedback_never_self_flip_plan_approval.md](feedback_never_self_flip_plan_approval.md) — "keep going until approved" = drive /plan-reviewer to APPROVED then STOP; never self-flip plan-draft→approved.
- [feedback_poll_roborev_after_any_commits.md](feedback_poll_roborev_after_any_commits.md) — roborev fires on EVERY commit; poll to quiet + drain kata findings before declaring done.
- [reference_roborev_kata_bridge_needs_cha_branch.md](reference_roborev_kata_bridge_needs_cha_branch.md) — The kata bridge only enqueues on `cha-NNN` branches; elsewhere an empty queue is NOT a clean review — read `roborev show` by hand.
- [reference_roborev_severity_threshold_met_is_clean.md](reference_roborev_severity_threshold_met_is_clean.md) — `SEVERITY_THRESHOLD_MET` means CLEAN; trust `.verdict_bool` (1=clean), not the string.
- [feedback_discuss_before_implementing.md](feedback_discuss_before_implementing.md) — "should we", "I feel like", "is this right" = discuss; don't start editing.
- [feedback_simplest_correct_mechanism_no_hedging.md](feedback_simplest_correct_mechanism_no_hedging.md) — Lead with the simplest correct mechanism; don't over-engineer or hedge.
- [feedback_tickets_are_spirit_not_spec.md](feedback_tickets_are_spirit_not_spec.md) — Tickets = spirit, not spec; derive the best mechanism, surface alternatives at the gate.
- [feedback_evaluate_ticket_necessity_first_principles.md](feedback_evaluate_ticket_necessity_first_principles.md) — A well-specified ticket isn't self-justifying; evaluate necessity at the plan gate (CHA-464 was built then cancelled for this miss).
- [feedback_ask_before_filing_tickets.md](feedback_ask_before_filing_tickets.md) — NEVER file a Linear ticket unprompted; propose and wait (exceptions inside)
- [feedback_fold_trivial_review_fixes.md](feedback_fold_trivial_review_fixes.md) — Fold trivial in-scope review fixes into the PR; don't file follow-up tickets for one-liners.
- [feedback_review_role_no_implementation.md](feedback_review_role_no_implementation.md) — During /review-pr, only update comments/tickets — never edit source.
- [feedback_intermediate_breakage_ok.md](feedback_intermediate_breakage_ok.md) — Large refactors: don't fragment commits to keep CI green every step.
- [feedback_claude_md_stability.md](feedback_claude_md_stability.md) — CLAUDE.md is cache-prefix; keep it a thin router, push instructions into skills.
- [feedback_no_kata_refs_in_source.md](feedback_no_kata_refs_in_source.md) — Source + commits cite Linear CHA-XXX; never kata `penca#xxxx`.
- [feedback_permanent_instrumentation_over_spike.md](feedback_permanent_instrumentation_over_spike.md) — Diagnostic instrumentation → permanent + off-by-default, not throwaway spike code.
- [feedback_send_user_message_mid_loop.md](feedback_send_user_message_mid_loop.md) — Mid-loop replies go via SendUserMessage; plain text between tool calls doesn't render.
- [feedback_followup_tickets_before_impl_todo_pointers.md](feedback_followup_tickets_before_impl_todo_pointers.md) — Mint deferred-scope follow-ups at plan approval; TODO(CHA-NNN)↔ticket pointers are bidirectional.

## Workflow / tool habits
- [feedback_absence_is_not_evidence.md](feedback_absence_is_not_evidence.md) — A check whose output you didn't READ proves nothing: `src/**/*.rs` misses `src/file.rs` without globstar, `2>/dev/null && echo ok` hides exit 2, `grep -c 'queued|running'` never reaches 0.
- [feedback_slow_commands_capture_and_wait.md](feedback_slow_commands_capture_and_wait.md) — Capture slow runs to a logfile on the FIRST run and grep it; no `until grep` poll loops; verify bg output directly.
- [feedback_integration_suite_full_fresh_before_pr.md](feedback_integration_suite_full_fresh_before_pr.md) — Pre-PR gate = FULL suite on a FRESH stack (branch CI skips integration entirely); subsets are inner-loop only.
- [feedback_just_check_gate_trust.md](feedback_just_check_gate_trust.md) — Trust `just check` only when it truly passed; mid-pipeline "All checks passed!" lies.
- [reference_proto_comment_edits_regen_py_stubs.md](reference_proto_comment_edits_regen_py_stubs.md) — Editing a .proto COMMENT requires `just compile-protos-py`; the tracked `*_pb2_grpc.py` stubs embed proto comments as docstrings and no CI job checks their freshness.
- [feedback_commit_before_kata_close_sha.md](feedback_commit_before_kata_close_sha.md) — Never chain `kata close --commit $(git rev-parse HEAD)` after `git commit` (pre-commit reformats → stale SHA). Commit, verify, then close.
- [feedback_use_just_commands.md](feedback_use_just_commands.md) — Tests/lint/format via `just`, never bare `uv run pytest` / `uv run ruff`.
- [feedback_clippy_not_in_cargo_check.md](feedback_clippy_not_in_cargo_check.md) — `cargo check` skips clippy; run `just cargo-clippy` before pushing after signature changes.
- [reference_buffer_unordered_send_hrtb.md](reference_buffer_unordered_send_hrtb.md) — Send-for-all-lifetimes cold reads: chunked `try_join_all`, NOT `buffer_unordered`; catch with `cargo check -p penca-server-grpc`.
- [feedback_worktrees.md](feedback_worktrees.md) — VM-per-ticket uses plain `git checkout -b`; worktrees are the laptop-only fallback.
- [reference_memory_dir_symlinks_into_repo.md](reference_memory_dir_symlinks_into_repo.md) — the auto-memory dir symlinks into the repo's TRACKED `.claude/memory/`; saving a memory dirties the tree and can leak into a feature PR.
- [feedback_self_sufficient_resume_comment.md](feedback_self_sufficient_resume_comment.md) — Mid-workflow Linear checkpoints go in ONE self-sufficient comment; edit in place.
- [feedback_linear_workflow.md](feedback_linear_workflow.md) — Repo TOML + just commands for Linear projects/labels; MCP for ad-hoc issue work.
- [feedback_read_linear_comments_first.md](feedback_read_linear_comments_first.md) — list_comments before drafting plans; constraints land in comments, not the description.
- [feedback_linear_cross_refs_markdown.md](feedback_linear_cross_refs_markdown.md) — Cross-link tickets via markdown links, not bare CHA-NNN (auto-resolver binds wrong UUIDs).
- [feedback_linear_parallel_save_drop.md](feedback_linear_parallel_save_drop.md) — Linear MCP save_issue can silently drop a write under parallel calls; verify load-bearing edits, prefer sequential.
- [feedback_refetch_pr_head_each_review_pass.md](feedback_refetch_pr_head_each_review_pass.md) — Re-running /review-pr: `git fetch origin pull/N/head` first.
- [feedback_gh_pr_edit_broken.md](feedback_gh_pr_edit_broken.md) — `gh pr edit` silently fails; use `gh api -X PATCH repos/.../pulls/<n> -f body=...`.
- [feedback_review_subagent_head_hazard.md](feedback_review_subagent_head_hazard.md) — /review-pr subagents leave HEAD on main even with isolation:worktree; always re-checkout after.
- [feedback_no_background_git_commits.md](feedback_no_background_git_commits.md) — Never background a chain ending in git add/commit while working in the foreground (pre-commit stash race).

## Tool choices
- [reference_vm_resource_limits_docker_disk_memory.md](reference_vm_resource_limits_docker_disk_memory.md) — This VM's ceilings: ~10min bg-task kill, ~7GB RAM (OOM reassigns host ports), 50GB disk. Which docker prune is safe when, the kept-up-stack test recipe, and the detached `just integration-test` survivor that fails every test with NO error text.
- [feedback_rust_rename_symbol.md](feedback_rust_rename_symbol.md) — Typed Rust renames: prefer rename_symbol, verify sites in session-edited files, kill the server after (~3GB).
- [reference_linear_attachment_visibility.md](reference_linear_attachment_visibility.md) — Linear file-attachments don't render in the UI; surface assets via a markdown link in a comment.
- [reference_sccache_s3_build_cache.md](reference_sccache_s3_build_cache.md) — sccache→S3 (`fabric-sccache` — genuinely still that name, don't "fix" it); persistent config error → `sccache --stop-server`.

## Style / code-as-written
(Project-wide conventions live in `docs/style-guide.md`, including where wire-shape validation belongs. Testing discipline — what earns a test, and the helper cross-product rule — lives in `docs/development-methodology-guide.md`.)
- [feedback_consistent_window_arg_shape.md](feedback_consistent_window_arg_shape.md) — Don't mix IntegerRange-struct and split scalar args for one window; keep IntegerRange at the proto boundary only.

## Architecture — current direction
- [project_persist_cdc_purge_governs_reads.md](project_persist_cdc_purge_governs_reads.md) — CHA-444 (MERGED PR #266): Persist = committed-only CDC; reads fenced at max(Pu,W_snap), not persist; Purge owns aborts on an independent gapless axis + cleans expired begins; tx_log GC trails purge. Follow-ups: CHA-468, CHA-466, CHA-441.
- [project_control_plane_three_tier.md](project_control_plane_three_tier.md) — penca-catalog (global) → penca-branch (per catalog) → per-branch stack; state as MVCC Penca tables; Resolve returns endpoints, never proxies. Per-branch-transactional state lives in the branch stack, not branch_store.
- [project_metadata_reads_to_querymanager.md](project_metadata_reads_to_querymanager.md) — DONE (CHA-472, PR #274): MetadataClient reads rehomed onto QueryManager; write/lifecycle remainder → LifecycleManager. Gap: OpenTx still misses the seek fast-path → CHA-501.
- [project_mut_seq_num_sequence.md](project_mut_seq_num_sequence.md) — mut_seq_num = lock-free PG SEQUENCE, within-tx ordering only. Contrast tx_seq_num, which MUST stay a locked counter row.
- [project_cold_row_uuid_index_format_agnostic.md](project_cold_row_uuid_index_format_agnostic.md) — Cold row_uuid index BUILD is format-agnostic; never gate it on storage_format. CHA-339 is predicate pushdown, NOT the identity seek.
- [project_pg_no_wal_archive.md](project_pg_no_wal_archive.md) — Per-branch PG durability = cross-AZ replicas, NOT WAL archiving. Cold-tier audit log replaces PITR.

## Review judgment / cross-cutting
(Several review invariants live in `.claude/skills/review-pr/SKILL.md` — including the cold-DataFusion session rule, the penca-api dependency boundary, and the own-PR `event=COMMENT` constraint. The kata `--json` shape table lives in `.claude/skills/do-issue/SKILL.md`.)
- [feedback_flight_sql_driver_parity.md](feedback_flight_sql_driver_parity.md) — Read the ADBC/JDBC/ODBC driver source BEFORE planning a Flight SQL ticket; same SQL → different wire actions → different entry-points.
