---
name: flight-sql-driver-parity
description: Every Flight SQL behavior must be exercised through ADBC + JDBC (+ ODBC); pull SQL-entry behavior into one helper so per-driver divergence is structurally impossible.
metadata:
  type: feedback
---

Rule: every new Flight SQL server-side behavior — rejections, DML routing, SET handling, DDL surface, error wording — must be (a) implemented in **one** helper called from every Flight SQL SQL-entry handler, and (b) tested through **every** driver (ADBC + JDBC today; ODBC when wired). Default to parametrized pytest fixtures (`pytest.param(..., id="adbc"/"jdbc")`); the test body stays driver-agnostic and the fixture supplies the `execute_query` / `execute_update` adapter.

**Why:** Different Flight SQL drivers route the same user-level SQL through different wire actions, and our server implements each entry point from scratch, so the paths drift:

- ADBC **updates** via the low-level statement (`stmt.set_sql_query` + `stmt.execute_update`, as `PencaClient.execute_update` does) → `DoPutStatementUpdate`, no prepare.
- ADBC **queries** via the DB-API `cursor.execute()` (as `PencaClient.execute_query` does) → `prepare()` is called **unconditionally** by `adbc_driver_manager`'s `_prepare_execute` (it only skips on `NotSupportedError`, which the Flight SQL driver does not raise) → `ActionCreatePreparedStatement` → `get_flight_info_prepared_statement` → `DoGet(CommandPreparedStatementQuery)`. **NOT** `CommandStatementQuery`. A bare `SELECT` with no user-level `prepare()` and no params still takes the prepared path.
- JDBC Apache `flight-sql-jdbc-driver`'s plain `Statement.execute(SELECT)` → `CommandStatementQuery` → `get_flight_info_statement` → `DoGet(CommandStatementQuery)`. (Its DML/`PreparedStatement` path uses `ActionCreatePreparedStatement` → `DoPutPreparedStatementUpdate`, because the driver needs the result schema upfront.)
- ODBC depends on the bridge.

So for the same `SELECT`, **ADBC lands on the `CommandPreparedStatementQuery` DoGet arm while JDBC lands on the `CommandStatementQuery` arm** — the two query paths do not converge on one server handler.

The Flight SQL spec pins the actions but does not pin the client-side decision. There is no unified cross-driver spec; each driver's choice is findable only by reading that driver's source. So a feature added to one server entry point silently doesn't exist for users of a different driver.

Surface CHA-355 (2026-05-31): the GetFlightInfo-plan-reuse cache was first wired only on the `CommandStatementQuery` DoGet arm. JDBC (statement arm) hit the cache; ADBC (prepared arm) got zero benefit and the RT1 hit-event test stayed red — for two full sessions the failure was misdiagnosed as a ticket-rewrap routing problem before the real cause (ADBC's unconditional `prepare()`) was found by reading `adbc_driver_manager/dbapi.py::_prepare_execute`. The fix wired the cache on the prepared arm too. Had the wire-action audit been done at plan time, the prepared arm would have been in scope from commit one.

Earlier surface: CHA-257 (2026-05-23). The CHA-172 auto-commit-DDL rejection lives in `penca-sql-server/src/dml.rs::execute`, only reachable via `do_put_statement_update`. ADBC users got the actionable CHA-172 message; JDBC users got DataFusion's internal `"schema provider does not support registering tables"` because their CREATE TABLE bailed inside `do_action_create_prepared_statement` (where `ctx.statement_to_logical_plan` invokes the default `SchemaProvider::register_table`) long before `dml::execute` ran. Discovered during the CHA-257 PR review, not at design time. User: *"I'm sick of having JDBC and ODBC completely broken."*

The architectural follow-up is tracked separately (a "SQL-entry gateway" upstream of DataFusion's planner that every Flight SQL entry point calls, plus driver-parametrized integration tests).

**How to apply:**

- **Don't guess driver routing — look it up at design time.** Each driver's choice of Flight SQL action is documented *in that driver's source*: `adbc-driver-flightsql` (Go/C), Apache `flight-sql-jdbc-driver` (Java), and whichever ODBC bridge is in play. Before writing a test that asserts "JDBC `CREATE TABLE` should surface message X" or "ADBC `cursor.execute()` should reach server entry-point Y," read the driver source to confirm the exact action path. Empirical post-implementation discovery ("oh, JDBC routes through `ActionCreatePreparedStatement` not `DoPutStatementUpdate`") is the failure mode that triggered this memory — don't repeat it. The Flight SQL server-protocol spec does not pin client-side action choice; only the driver source does.
- **Designing a Flight SQL ticket** — list every SQL-entry handler the change touches (`do_put_statement_update`, `do_action_create_prepared_statement`, `do_put_prepared_statement_update`, `do_put_substrait_plan`, `get_flight_info_statement`, `get_flight_info_prepared_statement`). If the change conceptually applies to "any SQL coming in," it has to live in the shared helper, not in one entry-point. Resolve which entry-points each target driver invokes by reading the driver source, not by guessing.
- **Writing a Flight SQL test** — parametrize over `(adbc, jdbc)` (and `odbc` once wired) via a fixture rather than writing one test per driver. The same SQL must produce the same wire-level error / result through every driver. If it doesn't, the parametrized failure is the signal — that's the test doing its job, don't silence it with skip-marks.
- **Reviewing a Flight SQL diff** — if it edits only one entry-point handler and adds behavior (parse, reject, route), flag it. The behavior probably belongs in the shared helper, and the test probably needs to fail through every driver.
- **Hitting a JDBC-only or ADBC-only bug** — fix it in the shared helper, not by special-casing one entry-point. If no shared helper exists yet for that surface, extracting one is part of the fix.
- **Related**: see the "Compose small single-responsibility functions" section of `docs/style-guide.md` (the helper extraction is the same composition discipline) and [[feedback-exhaustive-helper-cross-product-tests]] (driver parametrization is one axis of the cross-product).
