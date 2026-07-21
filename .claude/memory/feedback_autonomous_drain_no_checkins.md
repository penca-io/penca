---
name: feedback_autonomous_drain_no_checkins
description: "During an autonomous drain (/do-issue Step 5 loop), keep going across many tasks without stopping to ask after each — the user isn't watching"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fa802ea1-b627-42d1-8f02-ea3596b0f8f7
---

Once the user says to run the drain / "keep going", DRIVE IT AUTONOMOUSLY: claim → implement → commit → close → claim next, across many tasks and tool-call batches, without stopping to report or ask after every turn. The user is often not watching the terminal during a drain.

**Why:** Stopping to ask/checkpoint after literally every task (or every turn) is infuriating to this user and defeats the point of the autonomous /loop drain, which is designed to run unattended and is session-resumable.

**How to apply:** Only stop and surface when there is a *truly course-changing* issue that genuinely needs the user's judgment (a structural blocker, a scope fork, a destructive/irreversible action, a decision that changes the whole approach) — the kind of thing [[feedback_discuss_before_implementing]] is about. Routine per-task progress, minor mechanism refinements discovered mid-task, and "should I keep going" are NOT stop conditions — just keep banking tasks and report at the end or at a real milestone. Context exhaustion is not a reason to stop-and-ask; commit what's done and continue/hand off cleanly. Pairs with [[feedback_no_subagents]] (do the work directly) and [[feedback_intermediate_breakage_ok]].
