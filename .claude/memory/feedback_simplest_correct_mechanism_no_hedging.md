---
name: feedback-simplest-correct-mechanism-no-hedging
description: "In design discussion, reach for the simplest correct mechanism and state it plainly; don't over-engineer or hedge — the user will cut through both"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 49581542-700a-405f-a971-61b8cf9ab7ca
---

During a design discussion the user twice course-corrected toward simplicity: (1) I proposed a structural "lineage-rank" tiebreak for cross-branch read ordering; they asked why not just seed the child's `commit_seq_num` from the base — the simpler, unifying mechanism (which was correct). (2) I hedged with "let me confirm the historical-fork edge case"; they cut through: just seed from `commit_seq_num(T)` off the commit log — "clean, zero cost, correct." Both times my added complexity/caution was unnecessary.

**Why:** This user thinks fast and precisely and optimizes for the simplest correct design. Extra abstraction layers, defensive tiebreaks, and "let me verify this might-be-an-issue" hedging read as noise, not rigor. They will spot and remove unnecessary complexity themselves.

**How to apply:** In design back-and-forth, lead with the simplest mechanism that is actually correct, stated as a claim not a hedge. Prefer deriving from an existing authoritative source (e.g. the commit log) over adding a new fast-path/branch. If something genuinely needs checking, name it in one line as an implementation-time verify, not as a blocking question. Save the caveats for real correctness forks, not routine edge cases the mechanism already covers. Related: [[feedback_discuss_before_implementing]].
