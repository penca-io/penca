---
name: feedback-dont-test-upstream-libs
description: Don't write integration tests whose assertions are mostly upstream library behavior. Test the Penca-owned logic; cover the rest with grep/structural checks.
metadata:
  type: feedback
---

Before adding a test, decompose what it actually asserts. If most of the chain is "upstream library X does what its docs say," that's not a test — that's an expensive boilerplate. Pin only the Penca-owned logic; cover one-time structural guarantees with grep, not runtime tests.

**Why:** During CHA-329, an initial plan added a Python integration test that read `docker logs <container>` for every servicer and grepped for a startup INFO line. On audit, the test was asserting (1) compose env present — covered by `rg`; (2) every binary calls `init_tracing()` — covered by `rg`; (3) helper resolves filter correctly — covered by Rust unit tests; (4) `tracing-subscriber` writes to stdout — *that's tracing-subscriber's job, not ours*; (5) docker captures stdout — *that's docker's job, not ours*. Only (3) was Penca-owned. The user prompted the reconsideration with "what is this test for? Do we really need an integration test for this?" — and the honest answer was no. Integration-test cost (compose stack startup, brittleness to log-message phrasing) outweighed the marginal value over grep + unit tests.

**How to apply:** Mid-plan, when sketching an integration test, list the chain of things it implicitly asserts and label each as Penca-owned vs upstream. If the Penca-owned subset is empty or covered by a cheaper test, use the cheaper test. Default to grep for one-time structural guarantees ("every binary calls X", "compose has N occurrences of Y"); default to unit tests for pure-function logic; reserve integration tests for behaviors that genuinely require the live stack to observe (cross-service RPC ordering, real Postgres / S3 interaction, etc.). Related to the skill's existing "doc-only tickets get grep-based acceptance" carve-out, but applies more broadly than doc-only.
