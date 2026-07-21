---
name: feedback_evaluate_ticket_necessity_first_principles
description: "A well-specified ticket isn't self-justifying — evaluate whether it should be built at all from first principles at the plan gate, before mechanism-binding it"
metadata:
  node_type: memory
  type: feedback
---

Being well-specified is not the same as being worth building. Before mechanism-binding a ticket at the `/do-issue` plan gate, evaluate its **necessity from first principles** — independently of how cleanly it's written. A ticket existing (even user-created, even Low-priority-and-tidy) is not its own justification.

**Why:** CHA-464 (persist-tier `row_uuid` index) was a clean, well-scoped ticket. We built it end-to-end (PR #291, all gates green) — then the user challenged its *merit* and it didn't survive: the build cost lands on the persist (memory-relief) critical path, the read win is gated on unbuilt CHA-469, and it optimizes a rare degraded-mode path we don't even index in the hotter tier. All of that was knowable at the plan gate from first principles. We built a tidy ticket to spec instead of asking whether to build it at all. See [[project_persist_row_uuid_index_cha464]].

**How to apply — first-principles necessity questions (ask at the gate, for every ticket):**
1. **Cost placement:** what does it cost, and *where does that cost land*? Work added to a live/critical path (write, memory-relief, commit) to benefit a read/rare path is a red flag.
2. **Benefit reality:** what's the actual win *as it would ship* — not as aspirationally framed? Is it gated on other unbuilt work? If so the win is ~zero until that lands.
3. **Path frequency:** how often is the optimized path actually exercised? Optimizing a rare / degraded-mode path (e.g. cold spill under OOM) is low-ROI by construction.
4. **Consistency:** do we optimize comparable-or-hotter paths this way? If the hotter, more-common path is left unoptimized, accelerating the colder one is suspect.
5. **Strategic fit:** does the broader design actually call for this? A cross-epic design doc, *when one exists*, is an authoritative input (where it disagrees with ticket prose, the doc wins) — but **most tickets have no doc**, so the discipline is the reasoning in 1–4, not a doc lookup.

Any of these firing → raise a **Challenge** at the Step-3 gate (a "should we build this now?" objection or a cheaper alternative), don't just plan it. Ties into [[feedback_tickets_are_spirit_not_spec]] and [[feedback_discuss_before_implementing]].
