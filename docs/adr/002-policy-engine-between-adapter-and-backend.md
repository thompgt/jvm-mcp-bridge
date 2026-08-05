# ADR 002 — A single policy engine sits between every adapter and its backend

**Status:** accepted · 2026-08-04

## Context

Existing database MCP servers enforce safety inside the tool implementation: the query tool
checks the SQL, the schema tool checks the table name. This has failed in practice. Datadog
published a SQL-injection case study against the reference PostgreSQL MCP server, and
reviewers have noted it ships with no query timeout, no row limit and no plan preflight.

The failure mode is structural, not a coding slip. When each tool owns its own checks:

- a new tool starts with zero guardrails and has to re-derive them,
- the checks drift apart, so `sql.query` and `schema.describe_table` disagree about what
  "visible" means, and
- there is no single place to answer "what is this client actually allowed to do?"

This project has four backends. Repeating that mistake four times is not an option.

## Decision

One `PolicyEngine` in `mcp-policy`, backend-agnostic, sits between every adapter and every
backend. It reasons about abstract concepts — a *resource name*, a *mode* (read/write), a
*row cap*, a *timeout* — not about SQL or topics. Adapters translate their backend's
concepts into those terms and ask for a `Decision`.

The engine's API is shaped so that the backend handle is only reachable from inside an
allowed decision. An adapter cannot open a connection and "remember to check first"; there
is no code path that yields a connection without a decision having been made.

Every decision, allow or deny, goes to the `AuditSink` with the rule that fired.

## Consequences

- Adding a backend means implementing translation, not re-implementing safety. The row cap,
  timeout, dry-run and audit behaviour come for free and behave identically.
- Guardrail tests live in `mcp-policy` and need no Docker and no live backend, so the rules
  that matter most are the cheapest to test.
- A deny carries a reason string that names the rule. This is not only for operators: the
  reason is returned to the model, and a model that is told *why* it was refused stops
  retrying blindly. See CLAUDE.md on tool descriptions as prompt surface.
- The engine cannot catch backend-specific escapes on its own. `adapter-jdbc` still has to
  resolve table names from a parsed AST before handing them over (ADR 003, WORKPLAN 1.3) —
  the engine can only be as correct as the names it is given. The split is deliberate:
  *parsing* is backend-specific, *deciding* is not.
