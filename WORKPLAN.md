# Workplan

`[ ]` pending · `[x]` done · each subphase ends with a commit.

---

## Phase 0 — Scaffolding

- [x] **0.1 Gradle multi-module skeleton** — seven modules, pinned wrapper 8.14.3, version
      catalog, `-Werror` and release 21 applied from the root, MIT licence, `.gitattributes`.
- [x] **0.2 Context docs** — CLAUDE.md, WORKPLAN.md, README, ADRs 001–003.
- [x] **0.3 CI + compose** — GitHub Actions with a fast pure-JVM job gating a Docker
      integration job; `docker-compose.yml` with Postgres and Redpanda.

## Phase 1 — JDBC vertical slice

The proof that the harness works: a client can ask a real question and get a real answer,
and every call is bounded and audited.

- [x] **1.1 `mcp-core` registry** — `BridgeTool`, `ToolRegistry`, JSON-schema helpers,
      `ToolOutcome`, and the error mapping that turns backend failures into model-actionable
      MCP errors.
- [x] **1.2 `mcp-policy` engine** — `PolicyEngine`, `PolicyProfile`, `Decision` (allow/deny
      with the rule that fired), resource allowlists, row/byte caps, timeouts, dry-run,
      and the structured `AuditSink`.
- [x] **1.3 SQL validation at the AST** — JSqlParser; reject anything that is not a single
      `SELECT`/`WITH`; resolve every referenced table and match the allowlist on resolved
      names, never on the raw string.
- [x] **1.4 Read-only execution** — `setReadOnly(true)`, dialect statement timeout,
      `maxRows`, byte cap, cursor-bounded fetch, column redaction by pattern.
- [x] **1.5 Schema introspection** — `schema.list_tables` / `schema.describe_table` from
      `DatabaseMetaData`, filtered by the same allowlist, plus `jdbc://schema/{table}`
      resources.
- [x] **1.6 stdio server wiring** — `server-app` binds `bridge.yaml`, builds the registry,
      serves over stdio.
- [x] **1.7 Guardrail test suite** — the known attacks (stacked statements, writing CTE,
      comment-obfuscated DML, allowlist bypass via join/view, row-cap blowout) each assert a
      *deny with reason*.
- [x] **1.8 Round-trip test** — in-process MCP client drives `tools/list` and a real
      `sql.query` against Testcontainers Postgres.

Done when: an MCP client registered against this server answers a question that requires
both a schema lookup and a query, and the audit log shows both calls with their decisions.

## Phase 2 — Streamable HTTP + auth

- [x] **2.1 Streamable HTTP transport** — `HttpServletStreamableServerTransportProvider` on
      `/mcp`, registered as a `ServletRegistrationBean`; same registry as stdio.
- [x] **2.2 API-key auth** — static keys mapped to policy profiles, for internal networks.
- [x] **2.3 OAuth2 resource server** — JWT validation with RFC 8707 resource indicators, per
      the 2025-06-18 security revision; audience binding so a token for another service is
      rejected.
- [x] **2.4 Per-principal profiles** — allowlists and caps resolved from the authenticated
      principal, not from global config. Named profiles may only *narrow* the backend
      default, checked at startup, so tool descriptions stay an upper bound.
- [x] **2.5 Health + readiness** — Actuator endpoints that report each configured backend
      separately, so a dead broker doesn't mark the whole bridge down. An unreachable
      backend is `DEGRADED` at HTTP 200; liveness and readiness exclude backends entirely.

Done when: two API keys with different profiles see different tool results against the same
database, and an unauthenticated request to `/mcp` is refused.

## Phase 3 — Kafka adapter

- [x] **3.1 AdminClient lifecycle** — connection config, timeouts, clean shutdown.
- [x] **3.2 Topic and group inspection** — `kafka.list_topics`, `kafka.describe_topic`,
      `kafka.describe_group`. A group is only visible if it consumes something the caller
      may read, and an invisible group is refused with the same words as an absent one.
- [x] **3.3 Consumer lag** — `kafka.consumer_lag` with per-partition breakdown and the
      end-offset snapshot time, so the model doesn't misread a stale number as live.
- [x] **3.4 Bounded peek** — `kafka.peek` reads from an explicit offset with a message and
      byte cap, using a unique group id and **never committing offsets**. Assigns rather than
      subscribes, so it never joins a group or triggers a rebalance, and reports `nextOffsets`
      so paging is an explicit read rather than a consumer held open between calls.
- [x] **3.5 DLQ triage** — `kafka.dlq_sample` groups a dead-letter topic by error header and
      returns representative messages per class rather than a raw dump. Detects the header
      convention (Spring Kafka, Spring Cloud Stream, Kafka Connect) and reports which one it
      grouped on plus every header seen, so a bad grouping is recoverable with `error_header`
      rather than fatal. Per-occurrence values in the label are normalised, or a free-text
      error header produces a thousand classes of one — the dump it exists to replace.
- [x] **3.6 Write path, gated** — `kafka.produce` and `kafka.reset_offsets` are registered
      unconditionally and refused unless write-mode is on *and* the topic is on the write
      allowlist. Registered even where they can never run, because a model that cannot see a
      tool routes around it — usually by asking a human to do the same thing with no allowlist
      and no audit record — while a model told *why* it was refused stops. `reset_offsets`
      additionally refuses a group with live members, clamps an out-of-range offset into the
      retained log, and defaults to `dry_run: true`: it reports how many messages the move
      would skip unrecoverably before it will make it. Denials are tested per gate.

Done when: against a Redpanda container with a lagging consumer, the model can identify which
partition is behind and sample the messages stuck there.

## Phase 4 — JVM runtime adapter

- [x] **4.1 MBean access** — `jvm.mbeans` (pattern query) and `jvm.attribute`, over the local
      `MBeanServer` when embedded or a remote JMX URL when running as a sidecar. Reads only:
      `invoke`, `setAttribute` and bean registration are unreachable at every access mode,
      because the beans carrying the attributes worth reading are the same ones that can
      trigger a full GC, dump the heap to a caller-chosen path, or relevel every logger —
      there is no allowlist granular enough to make that a considered grant. Composite and
      tabular values are rendered as nested objects under a shared budget, and truncation is
      reported: a list of 100 thread ids from a JVM with 5000 is a partial answer, and one
      that does not say so is a wrong one. Redaction matches keys *inside* those values, since
      a JVM's credentials are never an attribute called Password — they are a key in
      `SystemProperties`. Alone among the backends its redaction default is non-empty.
- [x] **4.2 Memory and GC** — `jvm.memory` from the platform MXBeans: heap/non-heap, pool
      breakdown, GC counts and times, with deltas rather than raw counters. A cumulative
      counter read once says how old the JVM is, not how it is: 41 seconds of GC is
      unremarkable over a fortnight and an outage over four minutes, and the raw value does
      not distinguish them. So every counter carries a delta and the interval it covers,
      either since this bridge last looked or across an in-call `sample_seconds` window
      clamped to the call timeout. The tool also corrects which "used" to believe — pool
      usage counts uncollected garbage, so a healthy young generation reads as nearly full;
      the reported figure is usage after the pool's last collection, which is the only number
      here whose growth is the shape of a leak.
- [x] **4.3 Threads** — `jvm.threads` with state histogram, top stacks, and explicit
      deadlock detection via `findDeadlockedThreads`. Threads are *grouped* by their stack
      rather than dumped, the same move `kafka.dlq_sample` makes: the finding in a thread dump
      is almost never one stack, it is that three hundred threads are stopped in the same
      place, and reading that out of a dump means comparing every stack against every other.
      Grouping drops line numbers, or one pool waiting on one queue splits into four groups
      across four lines of the same park loop; samples keep their full frames. Deadlocks are
      reported separately because they are the only verdict here — a large WAITING group is an
      idle pool or a stuck one and the snapshot cannot tell, whereas a cycle is broken on its
      own terms. Nothing is inferred: a wrong deadlock report sends someone after a bug that
      is not there while the real one continues.
- [x] **4.4 Actuator client** — `jvm.actuator` reads health, env and metrics from a Spring
      Boot app's Actuator, with the sensitive-key redaction the policy engine already has.
      Endpoints are allowlisted one at a time, not as a group: `health` is a status word,
      `env` is the whole resolved configuration and `heapdump` is every string that has ever
      held a password, so a group grant means whoever wanted the first gave away the third.
      GET only. It is also an HTTP client aimed at an internal address with a model-chosen
      path — the SSRF shape — so the host comes only from configuration, every path segment
      is validated against a strict charset before a request is made, and redirects are
      refused rather than followed. Actuator's own masking is the *target's* setting and can
      be switched off there, so the policy redactor is applied again over every key at every
      depth: what leaves this process does not depend on how the application it describes
      happens to be configured.
- [x] **4.5 JFR snapshot** — `jvm.jfr_snapshot` runs a time-boxed recording and returns a
      *summary* (hot methods, allocation sites, longest pauses, monitor contention). Never a
      raw dump — a .jfr file is useless to a model and enormous. Driven through
      `FlightRecorderMXBean`, so it works over a JMX connection to a sidecar exactly as it
      does in-process, which is the case that matters: the JVM worth profiling is rarely the
      one running the bridge. Bounded four ways — duration capped by configuration rather
      than by the caller, one recording at a time (a concurrent call is refused, not queued,
      since two recordings double an overhead that was accepted once), a size ceiling on the
      transferred stream, and unconditional cleanup of both the recording and the temporary
      file. The recording also carries a dead-man's duration longer than the requested
      window, so a bridge that dies mid-call leaves a recording that stops itself rather than
      one that profiles the target until it is restarted.

Done when: pointed at a JVM under synthetic load, the model reports which pool is filling
and which threads are contended, without a human reading a dump.

## Phase 5 — Internal HTTP API bridge

- [ ] **5.1 OpenAPI ingestion** — parse an OpenAPI 3 document at startup.
- [ ] **5.2 Operation → tool** — emit one MCP tool per allowlisted operation with its input
      schema derived from the spec's parameters and request body.
- [ ] **5.3 Invocation** — JDK `HttpClient` with per-route timeout, response size cap, and
      auth passthrough or a configured service credential.
- [ ] **5.4 Safety defaults** — only `GET`/`HEAD` unless write-mode; route allowlist is
      opt-in, never "everything in the spec".

Done when: a sample OpenAPI document produces working tools with no code changes.

## Phase 6 — Hardening and release

- [ ] **6.1 MCP conformance** — pass `npx @modelcontextprotocol/conformance server --suite
      active` against the HTTP transport. The SDK itself passes 40/40; so should this.
- [ ] **6.2 Prompt-injection resistance** — tests where database rows and Kafka messages
      contain instruction-shaped text; assert it reaches the client as content and never
      changes a policy decision.
- [ ] **6.3 Container image** — distroless, non-root, healthcheck, published to GHCR.
- [ ] **6.4 Publish `mcp-core` + `mcp-policy`** — to Maven Central, so others can build
      adapters against the policy engine without forking.
- [ ] **6.5 Registry + docs** — MCP registry submission, CHANGELOG, adapter-authoring guide.

Done when: a stranger can `docker run` the image against their own database with only a
`bridge.yaml`, and a Java developer can add an adapter without touching this repo.

---

## Risks

- **The policy engine is the product.** If a guardrail can be bypassed, nothing else here
  matters. Every rule needs a test that asserts the *denial reason*, not just that an
  exception was thrown — a rule that denies for the wrong reason will deny the wrong things.
- **Tool descriptions drift from behaviour.** A description that promises a bound the code
  no longer enforces misleads the model into unsafe calls. Phase 6 should add a test that
  asserts declared caps match configured caps.
- **Jackson 2/3 split** across the SDK and Spring Boot is contained today because `mcp-core`
  imports neither. If a module ever imports Jackson directly, that containment is gone.
- **JFR and JMX can be expensive** on a loaded production JVM. Phase 4 defaults must be
  conservative and the cost documented in the tool description.
