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
| `adapter-kafka/` | broker | Topics, consumer lag, bounded peek, DLQ triage, gated replay and offset reset |
| `adapter-jvm/` | runtime | JMX MBeans, memory and GC deltas, grouped thread stacks, Actuator, JFR summaries |
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
   name. The allowlist is never matched against the raw query string — that is the hole. A
   schema qualifier is part of the name: `public.`/`dbo.` fold away, anything else must be
   allowlisted as `schema.table`, so `secrets.orders` is not `orders`.
3. **Connection-level enforcement.** `setReadOnly(true)` plus a statement timeout, so a
   permitted query still cannot run forever.
4. **Result bounds.** Row cap, byte cap, and column redaction by pattern. Redaction is decided
   from the underlying column, not the label, so `SELECT email AS x` is still withheld; a
   computed column over a redacted one is withheld too.

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
                                       # port taken? BRIDGE_PG_PORT=55432 docker compose up -d postgres
cp bridge.example.yaml bridge.yaml     # gitignored; edit to point at your system
./gradlew :server-app:bootJar
```

Register it with Claude Code:

```bash
claude mcp add jvm-bridge -- java -jar "$PWD/server-app/build/libs/jvm-mcp-bridge.jar" \
  --spring.config.additional-location=file:$PWD/bridge.yaml --bridge.transport=stdio
```

Or in `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "jvm-bridge": {
      "command": "java",
      "args": [
        "-jar", "/abs/path/server-app/build/libs/jvm-mcp-bridge.jar",
        "--spring.config.additional-location=file:/abs/path/bridge.yaml",
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

### Shared deployment

`transport: http` serves the Streamable HTTP transport on `/mcp` for a team rather than one
laptop. Callers authenticate with a static API key or an OAuth2 bearer token, and each
credential names a **policy profile**:

```yaml
bridge:
  transport: http
  http:
    auth:
      mode: api-key                    # api-key | oauth2 | none
      keys:
        - key: ${ANALYST_KEY}
          principal: analytics-team    # the identity in the audit log
          profile: analyst             # the policy below
  datasources:
    - name: orders-db
      policy:
        allow-tables: [customers, orders, order_items]
        max-rows: 200
      profiles:
        analyst:                       # states only what it changes
          allow-tables: [customers]
          max-rows: 50
```

A profile may only **narrow** the datasource policy. One that reads a table the datasource
does not, runs longer, returns more rows, or drops a redaction is rejected at startup, naming
the dimension that widened. That keeps the `policy` block a true ceiling — reviewing what a
database exposes means reading one list — and it is what makes tool descriptions safe to
generate from it: a caller may be permitted less than the description promises, never more.

Under `mode: oauth2`, an `audience` is mandatory. It is the RFC 8707 resource indicator
identifying this bridge, and without it a token the same issuer minted for any other service
would be accepted here — the confused-deputy problem the 2025-06-18 security revision exists
to close.

`/actuator/health/backends` reports each backend separately. An unreachable one is `DEGRADED`
at HTTP 200 and is excluded from liveness and readiness: a dead broker should not restart the
process or stop the database questions that still work.

Under `transport: http` Actuator shares the port with `/mcp`, so it sits behind the same
authentication filter: `/actuator/health` names every backend and its version, which is a map
of the estate. `/actuator/health/liveness` and `/actuator/health/readiness` are the only
exceptions — a kubelet has no credential to present, and both answer with a status word and
nothing else. Where the deployment can carry it, `management.server.port` on a port the
outside cannot reach is still worth doing on top.

## Build

```bash
./gradlew build                        # compile + unit tests, no Docker required
./gradlew build -PincludeIntegration   # adds Testcontainers integration tests
```

Integration tests are `@Tag("integration")` and excluded by default so a clone builds with
no Docker daemon running. CI runs them as a separate job gated on the fast one.

## Skills

What this repository exercises, and where to look if you want to read the code rather than
take the claim:

| Skill | Applied here | Read |
|---|---|---|
| **Java 21** | Records as the domain vocabulary (`Decision`, `EffectiveLimits`, `ToolOutcome`), sealed intent through package-private constructors, pattern matching in the SQL visitor, text blocks in config fixtures. Compiled `--release 21` under `-Xlint:all -Werror`. | `mcp-policy/` |
| **MCP protocol** | The SDK used directly, not through a framework starter: tool descriptors with declared **output schemas** and structured content, tool annotations (`readOnlyHint`, `destructiveHint`) so clients can decide what to auto-approve, server `instructions` delivered at initialize, and failures returned as `isError` results the model can recover from rather than protocol errors it cannot. | `mcp-core/`, [ADR 001](docs/adr/001-mcp-sdk-direct-not-spring-ai.md) |
| **Guardrail engineering** | Defence in depth across four independent layers, an API shaped so a backend connection is unreachable outside a policy decision, deny-by-default with model-actionable reasons, and an audit record per call. Adversarial tests cover stacked statements, data-modifying CTEs, `SELECT INTO`, comment-obfuscated writes and allowlist evasion by join. | `mcp-policy/`, `SqlGuard.java`, [ADR 002](docs/adr/002-policy-engine-between-adapter-and-backend.md) |
| **Spring Boot 3.5** | Confined to one module on purpose. Type-safe `@ConfigurationProperties` binding for the whole `bridge.yaml` tree, a transport chosen before the context starts (stdio runs `WebApplicationType.NONE` with the banner off), Actuator health, and a logging config where **every appender targets stderr** — one stdout line is a protocol parse error. | `server-app/` |
| **JDBC & connection pooling** | HikariCP with a deliberately small pool, read-only transactions layered with dialect-aware `SET LOCAL statement_timeout`, `DatabaseMetaData` introspection surfaced as MCP resources, and a `maxRows + 1` fetch so truncation is *detectable* rather than silently indistinguishable from a complete result. | `adapter-jdbc/` |
| **Gradle multi-module & testing** | Kotlin DSL, version catalog, `java-library` with `api`/`implementation` policed so `mcp-core` cannot leak a driver onto a consumer's classpath, and a resolved Jackson 2/3 coexistence conflict. Tests run in tiers: fast JVM units by default, Testcontainers behind `@Tag("integration")`, and a round-trip suite that launches the packaged jar as a subprocess and speaks JSON-RPC to it over pipes. | `build.gradle.kts`, `McpRoundTripTest.java` |

## Status

| Phase | State |
|---|---|
| 0 — Scaffolding | ✅ |
| 1 — JDBC vertical slice | ✅ |
| 2 — Streamable HTTP + auth | ✅ |
| 3 — Kafka adapter | ✅ |
| 4 — JVM runtime adapter | ✅ |
| 5 — Internal HTTP API bridge | ⬜ |
| 6 — Hardening and release | ⬜ |

Detail and acceptance criteria in [WORKPLAN.md](WORKPLAN.md). Design decisions in
[docs/adr/](docs/adr/).

## License

MIT — see [LICENSE](LICENSE).
