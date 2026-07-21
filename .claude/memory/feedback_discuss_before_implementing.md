---
name: Discuss before implementing when user is weighing options
description: When the user invites discussion ("I feel like…", "Is this the right thing to do?", "should we…"), respond with analysis only — do not start editing files
type: feedback
originSessionId: d13112f0-2772-4812-89ba-37245fa3fa22
---
When the user phrases a message as a question, a "should we", an "I feel like", or otherwise invites a discussion of trade-offs, respond with the analysis only. Do not begin editing files, running commands, or otherwise executing the change.

**Why:** The user explicitly told me to stop after I jumped into refactoring `bootstrap.rs` while they were still weighing whether to bake bootstrap into the Dockerfile vs run it as a one-shot. Quoting them: "can you not jump right into making the change? I'm not even sure this is the right thing to do. I want to discuss." Auto mode is not a license to skip the discussion phase when the user is signalling uncertainty.

**How to apply:**
- Watch for "should we", "is this right", "I feel like X is best" — those are discussion invites, not implementation orders.
- Reply with the trade-off analysis, name the options, give an honest recommendation, and ask which direction they want.
- Only start editing files after the user picks a direction or says "go".
- Even under Auto mode, "minimize interruptions" doesn't override an explicit pause signal.
