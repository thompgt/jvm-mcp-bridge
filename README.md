# jvm-mcp-bridge — expose a running JVM system to LLM clients

[![CI](https://github.com/thompgt/jvm-mcp-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/thompgt/jvm-mcp-bridge/actions/workflows/ci.yml)

An [MCP](https://modelcontextprotocol.io) server, written in Java, that lets an LLM client
ask questions about a live JVM system — its database, its broker, its runtime, its internal
APIs — and get bounded, audited answers.

The MCP server ecosystem is almost entirely Python, TypeScript and Go. The systems that most
need this are JVM systems: Spring Boot services behind Postgres and Kafka, instrumented with
JMX, that nobody wants to hand an LLM a raw connection to. Writing this in Java is the point,
not an accident — the bridge can hold a JDBC pool, a Kafka `AdminClient`, and a JMX
connection to the application it is describing, in one process.

**The tools are the easy part. The policy engine is the project.** Handing a model a
database connection is ten lines; making it safe enough that a team will actually deploy it
is the rest of this repository.

```
   Claude Code ──stdio──┐
                        ├─→ ToolRegistry ─→ PolicyEngine ─┬─→ adapter-jdbc  ─→ RDBMS
   any MCP client ─HTTP─┘      (mcp-core)    (mcp-policy)  ├─→ adapter-kafka ─→ broker
        + OAuth2 / API key                        │        ├─→ adapter-jvm   ─→ JMX / JFR
                                                  ▼        └─→ adapter-http  ─→ internal API
                                             AuditSink
```

| Path | Component | Role |
|---|---|---|
| `mcp-core/` | protocol layer | Tool registry, result and error mapping. MCP SDK only — no Spring, no drivers. |
| `mcp-policy/` | guardrail engine | Allowlists, read/write mode, row and byte caps, timeouts, dry-run, audit. Backend-agnostic. |
| `adapter-jdbc/` | database | AST-validated read-only SQL, schema introspection |
| `adapter-kafka/` | broker | Topics, consumer lag, bounded peek, DLQ triage |
| `adapter-jvm/` | runtime | JMX MBeans, memory and threads, Actuator, JFR summaries |
| `adapter-http/` | internal APIs | OpenAPI 3 spec → generated MCP tools |
| `server-app/` | executable | Spring Boot: config, transports, auth, health |

## What "safe" means here

Read-only is the default, and it is enforced at four independent layers rather than one — a
lesson taken from the published SQL-injection case study against the reference Postgres MCP
server, which enforced its checks in one place and had them bypassed:

1. **AST validation.** Statements are parsed with JSqlParser and rejected unless they are
   exactly one `SELECT` or `WITH`. Comments, whitespace and casing tricks are irrelevant to
   a parse tree.
2. **Resolved-name allowlists.** Every table the AST references is resolved and matched by
   name. The allowlist is never matched against the raw query string — that is the hole.
3. **Connection-level enforcement.** `setReadOnly(true)` plus a statement timeout, so a
   permitted query still cannot run forever.
4. **Result bounds.** Row cap, byte cap, and column redaction by pattern.

Writes need *two* independent opt-ins — write mode enabled for that backend **and** the
specific table or topic on a separate write allowlist. There is no wildcard write allowlist;
`*` is rejected at config load. See [ADR 003](docs/adr/003-read-only-by-default-write-mode-opt-in.md).

Every call, allowed or denied, is written to a structured audit log with the rule that fired.
Denials are phrased so the *model* can recover from them — `table "orders" is not in the
allowlist; visible tables are: customers, order_items` beats a stack trace, and stops the
model retrying blindly.

## Quickstart

Requires JDK 21+ and Docker. No `gradle` install needed — use the wrapper.

```bash
git clone https://github.com/thompgt/jvm-mcp-bridge.git
cd jvm-mcp-bridge

docker compose up -d postgres          # sample database on :5432
cp bridge.example.yaml bridge.yaml     # gitignored; edit to point at your system
./gradlew :server-app:bootJar
```

Register it with Claude Code:

```bash
claude mcp add jvm-bridge -- java -jar "$PWD/server-app/build/libs/jvm-mcp-bridge.jar" \
  --bridge.config=$PWD/bridge.yaml --bridge.transport=stdio
```

Or in `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "jvm-bridge": {
      "command": "java",
      "args": [
        "-jar", "/abs/path/server-app/build/libs/jvm-mcp-bridge.jar",
        "--bridge.config=/abs/path/bridge.yaml",
        "--bridge.transport=stdio"
      ]
    }
  }
}
```

Then ask it something that needs the schema *and* the data — "which customers have orders
with no shipment?" — and watch `audit.log` show both the `schema.describe_table` and the
`sql.query` call with their decisions.

## Configuration

One file describes every backend and its policy. Nothing else needs changing to onboard a
system.

```yaml
bridge:
  mode: read-only          # read-only | dry-run | read-write
  datasources:
    - name: orders-db
      url: jdbc:postgresql://localhost:5432/orders
      username: ${DB_USER}
      password: ${DB_PASSWORD}
      policy:
        allow-tables: [customers, orders, order_items]
        max-rows: 200
        statement-timeout: 5s
        redact-columns: ["*.email", "*.ssn", "*_token"]
```

`dry-run` returns the validated plan — the parsed SQL, the table it resolved to — without
touching the backend. It is the honest way to audit the guardrails before granting access.

## Build

```bash
./gradlew build                        # compile + unit tests, no Docker required
./gradlew build -PincludeIntegration   # adds Testcontainers integration tests
```

Integration tests are `@Tag("integration")` and excluded by default so a clone builds with
no Docker daemon running. CI runs them as a separate job gated on the fast one.

## Status

| Phase | State |
|---|---|
| 0 — Scaffolding | ⬜ in progress |
| 1 — JDBC vertical slice | ⬜ |
| 2 — Streamable HTTP + auth | ⬜ |
| 3 — Kafka adapter | ⬜ |
| 4 — JVM runtime adapter | ⬜ |
| 5 — Internal HTTP API bridge | ⬜ |
| 6 — Hardening and release | ⬜ |

Detail and acceptance criteria in [WORKPLAN.md](WORKPLAN.md). Design decisions in
[docs/adr/](docs/adr/).

## License

MIT — see [LICENSE](LICENSE).
