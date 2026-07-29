This is a per-commit review of one commit on a feature branch. The commit is an
intermediate state, not the shipped state, and a holistic branch-level review
runs separately at PR time (/review-pr).

Do not raise:
- Work the commit message explicitly defers to a named follow-up commit or
  ticket. Deferral is a plan, not a defect. Flag it only if the deferral leaves
  the tree broken: data loss, a failing build, or a wrong result that no later
  commit in the stated plan reaches.
- Comment, docstring, or commit-message wording, unless the text states
  something factually false about the code it documents.
- Test naming, test file organization, or extract-a-shared-helper suggestions,
  unless the duplication is load-bearing for correctness.
- Anything in pre-existing code the diff did not touch.

Prefer few, high-confidence findings over exhaustive coverage. A commit with no
correctness, safety, or contract problem should review clean.
