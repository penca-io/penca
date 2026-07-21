---
name: just integration-test takes multiple prefixes
description: Pass all needed prefixes to `just integration-test` in one call so Docker startup is paid once
type: feedback
originSessionId: dbcdedd4-8ff5-434d-9e7f-6f31e7774e2d
---
`just integration-test` accepts variadic prefixes — `just integration-test lifecycle query branch_persist` runs all three test files in a single Docker compose up/down cycle.

**Why:** Docker startup (postgres + seaweedfs + 5 penca services) is ~60–90 seconds — by far the dominant cost. Running the recipe N times pays it N times.

**How to apply:** When testing across multiple integration files, pass every prefix to one `just integration-test` call instead of looping over single prefixes. Default to multi-prefix even when investigating one area, since adjacent files often catch regressions and cost ~10 seconds more vs. starting Docker again.
