---
name: feedback-no-kata-refs-in-source
description: "Source code + commit messages must cite Linear CHA-XXX, not kata issue refs (penca#xxxx). Kata IDs are ephemeral local-instance."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7df128b4-cba6-482f-8e19-ebf115d3fe59
---

Never reference kata issue IDs (`penca#5f64`, `penca#m898`, etc.) in
committed source, comments, doc text, or commit messages. Use the
Linear `CHA-XXX` number — and when intra-ticket sequencing matters,
refer to "the follow-up commit" / "the prior commit" instead of any
ID.

**Why:** kata is a local-first per-VM task queue; its `<project>#<short_id>`
refs are ephemeral (different VM, different IDs; kata reinit
re-mints them). The repo is read by PR reviewers on GitHub, future
contributors, and people working from clones who have no kata
state — those refs mean nothing to them. CHA-XXX resolves uniformly
to the canonical Linear ticket from any context.

**How to apply:**
- In Rust doc comments / module docs / function docs: write
  "follow-up CHA-NNN commit" or "the CHA-NNN wire-up commit",
  never "see `penca#xxxx`".
- In commit message bodies: cite `CHA-NNN` in the footer; describe
  prior/next commits by what they do, not by kata ID.
- In Python test docstrings: same — `CHA-259 will when it ships`,
  not `penca#m898 will`.
- In kata task bodies themselves: cross-referencing kata IDs is
  fine — that text is local to the kata tracker. Source code is
  different.
