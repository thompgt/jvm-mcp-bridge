# ADR 003 — Read-only by default; writes need two independent opt-ins

**Status:** accepted · 2026-08-04

## Context

An MCP server hands a language model a live connection to production infrastructure. The
model is not adversarial, but its input is: a database row, a Kafka message header, or an
HTTP response body can all contain instruction-shaped text, and the model reads tool output
as context. A single misread row should not be able to become a `DELETE`.

Meanwhile, "read-only" is a claim that is easy to make and hard to keep. `SET TRANSACTION
READ ONLY` does not stop a stacked statement if the driver allows multiple statements.
A `SELECT` can contain a data-modifying CTE. A regex over the SQL string is defeated by a
comment.

## Decision

**Default is read-only, and read-only is enforced at more than one layer.**

For `adapter-jdbc`, "read-only" means all of:

1. The statement is parsed with JSqlParser and rejected unless it is exactly one `SELECT`
   or `WITH` — checked on the AST, so comments and whitespace tricks are irrelevant.
2. Every table the AST references is resolved and matched against the allowlist by *name*,
   never by matching the raw query string.
3. The JDBC connection is `setReadOnly(true)` and the transaction carries a statement
   timeout, so a permitted query still cannot run forever.
4. `maxRows` and a result byte cap bound what comes back.

Any one of these could be circumvented in isolation. That is the point of having four.

**Writes require two independent opt-ins**, and neither one alone is sufficient:

- write mode is enabled in configuration for that backend, **and**
- the specific target (table, topic) is on the *write* allowlist, which is a separate list
  from the read allowlist.

There is no wildcard write allowlist. `*` is rejected at config load.

## Consequences

- The out-of-the-box configuration cannot mutate anything. A user who deploys this without
  reading the docs is safe by default, which is the only default worth having.
- Enabling writes is a deliberate, auditable, two-part act. It cannot happen by flipping one
  boolean, and it cannot happen at all from inside a tool call.
- Some legitimate uses are inconvenient. That is accepted: the cost of a refused write is a
  config change, and the cost of an unintended one is an incident.
- `dry-run` mode exists as the bridge between the two — it returns the *validated plan* (the
  parsed SQL, the topic that would be written) without touching the backend, so an operator
  can see exactly what would happen before granting the opt-in.
