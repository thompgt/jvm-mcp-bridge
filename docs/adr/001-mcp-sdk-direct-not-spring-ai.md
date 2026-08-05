# ADR 001 — Use the MCP Java SDK directly, not Spring AI's MCP starters

**Status:** accepted · 2026-08-04

## Context

There are two ways to build an MCP server on the JVM:

1. `io.modelcontextprotocol.sdk:mcp` — the official SDK, maintained in collaboration with
   Spring AI. Version 2.0.0 (2026-06-11) tracks the 2025-11-25 protocol revision, requires
   Java 17+, and bundles stdio, servlet-SSE and Streamable HTTP transports with no web
   framework required.
2. Spring AI's `spring-ai-starter-mcp-server-*` starters, which wrap the same SDK with Boot
   auto-configuration and an annotation programming model.

The starters are more convenient. They are also, as of writing, documented against the
Spring AI 2.0 **snapshot** line.

## Decision

Depend on the SDK directly. Use Spring Boot only for what Boot is genuinely good at here —
configuration binding, the servlet container, OAuth2 resource-server support, and Actuator —
and keep all protocol handling in `mcp-core` with no Spring types in sight.

## Consequences

- The MCP protocol version this server speaks is a function of one pinned dependency we
  control, not of Spring AI's release cadence. That matters because the protocol is still
  moving: three revisions shipped between mid-2025 and mid-2026.
- `mcp-core` and `mcp-policy` compile with no Spring on the classpath, so they are
  publishable as standalone artifacts (Phase 6.4) and unit-testable without a context.
- We wire transports by hand. This is genuinely small: the SDK's
  `HttpServletStreamableServerTransportProvider` implements `Servlet`, so exposing it is one
  `ServletRegistrationBean`, and `StdioServerTransportProvider` takes a `McpJsonMapper` and
  nothing else.
- We give up the annotation model (`@McpTool` and friends). Since every tool in this project
  has to be routed through the policy engine anyway, tools are constructed by a registry
  rather than discovered by scanning — an annotation could too easily register a tool that
  bypasses `PolicyEngine`, which ADR 002 exists to prevent.

## Note for implementers

The SDK's published documentation site shows packages as `io.modelcontextprotocol.sdk.*`.
The actual jar uses `io.modelcontextprotocol.*` (`io.modelcontextprotocol.server`,
`io.modelcontextprotocol.spec`, `io.modelcontextprotocol.server.transport`). Verify APIs
against the jar, not the docs.
