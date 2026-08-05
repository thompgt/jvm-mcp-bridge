package io.github.thompgt.jvmmcp.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.spec.McpSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Launches the built jar as a subprocess and drives it over stdio, exactly as Claude Code or
 * Claude Desktop would.
 *
 * <p>Every other test calls Java objects. This one goes through {@code initialize},
 * {@code tools/list} and {@code tools/call} as JSON-RPC over pipes, against the real artifact.
 * That is the only way to catch the failures that matter most in practice and are invisible
 * from inside the JVM: a malformed tool schema, a capability the server forgot to advertise,
 * or — the classic — a log line on stdout corrupting the protocol stream.
 */
@Tag("integration")
@Testcontainers
class McpRoundTripTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("orders")
            .withUsername("bridge")
            .withPassword("bridge")
            .withInitScript("seed.sql");

    @TempDir
    static Path tempDir;

    private static McpSyncClient client;
    private static Path auditLog;

    @BeforeAll
    static void launchServer() throws IOException {
        // Set by the Gradle test task; see server-app/build.gradle.kts.
        String jar = System.getProperty("bridge.jar");
        assertThat(jar).as("bridge.jar system property (run via Gradle, not the IDE)").isNotNull();
        assertThat(Path.of(jar)).exists();

        auditLog = tempDir.resolve("audit.log");
        Path config = writeConfig(tempDir.resolve("bridge.yaml"));

        ServerParameters parameters = ServerParameters.builder("java")
                .args(
                        "-jar",
                        jar,
                        "--spring.config.additional-location=file:" + config.toAbsolutePath(),
                        "--bridge.transport=stdio")
                .build();

        StdioClientTransport transport = new StdioClientTransport(parameters, McpJsonDefaults.getMapper());
        client = McpClient.sync(transport)
                // Generous: the subprocess has to boot a Spring context and a connection pool.
                .requestTimeout(Duration.ofSeconds(30))
                .initializationTimeout(Duration.ofSeconds(60))
                .build();
        client.initialize();
    }

    @AfterAll
    static void stop() {
        if (client != null) {
            client.closeGracefully();
        }
    }

    private static Path writeConfig(Path path) throws IOException {
        String yaml = """
            bridge:
              mode: read-only
              transport: stdio
              audit:
                file: %s
              datasources:
                - name: orders-db
                  url: %s
                  username: %s
                  password: %s
                  policy:
                    allow-tables: [customers, orders, order_items]
                    max-rows: 50
                    statement-timeout: 5s
                    redact-columns: ["*.email"]
            """
                .formatted(
                        auditLog.toAbsolutePath().toString().replace('\\', '/'),
                        POSTGRES.getJdbcUrl(),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword());
        Files.writeString(path, yaml, StandardCharsets.UTF_8);
        return path;
    }

    @Test
    void listsEveryToolWithADescriptionAndAnInputSchema() {
        McpSchema.ListToolsResult tools = client.listTools();

        assertThat(tools.tools())
                .extracting(McpSchema.Tool::name)
                .containsExactlyInAnyOrder("sql.query", "sql.explain", "schema.list_tables", "schema.describe_table");

        // A tool with no description is a tool the model has to guess at.
        assertThat(tools.tools()).allSatisfy(tool -> {
            assertThat(tool.description()).isNotBlank();
            assertThat(tool.inputSchema()).isNotNull();
        });
    }

    @Test
    @DisplayName("the query tool's description names the tables the model may read")
    void toolDescriptionCarriesThePolicyTheModelNeedsToKnow() {
        McpSchema.Tool query = client.listTools().tools().stream()
                .filter(t -> t.name().equals("sql.query"))
                .findFirst()
                .orElseThrow();

        // Prompt surface: without the allowlist in the description, the model discovers the
        // boundary one refusal at a time.
        assertThat(query.description()).contains("customers", "orders", "order_items");
        assertThat(query.description()).contains("read-only");
        assertThat(query.outputSchema()).isNotNull();
        assertThat(query.annotations().readOnlyHint()).isTrue();
    }

    @Test
    void callsTheQueryToolAndGetsStructuredRowsBack() {
        McpSchema.CallToolResult result = client.callTool(McpSchema.CallToolRequest.builder("sql.query")
                .arguments(Map.of("sql", "SELECT id, name FROM customers ORDER BY id"))
                .build());

        assertThat(result.isError()).isFalse();
        assertThat(result.structuredContent()).isNotNull();
        assertThat(textOf(result)).contains("4 row(s)").contains("id, name");
    }

    @Test
    void answersAQuestionThatNeedsBothASchemaLookupAndAQuery() throws IOException {
        // The workplan's acceptance criterion, over the wire: discover the columns, then use
        // them. This is the shape of every real interaction with this server.
        McpSchema.CallToolResult schema = client.callTool(McpSchema.CallToolRequest.builder("schema.describe_table")
                .arguments(Map.of("table", "orders"))
                .build());
        assertThat(schema.isError()).isFalse();
        assertThat(textOf(schema)).contains("status").contains("total_cents");

        McpSchema.CallToolResult rows = client.callTool(McpSchema.CallToolRequest.builder("sql.query")
                .arguments(Map.of(
                        "sql", "SELECT status, count(*) AS n FROM orders GROUP BY status ORDER BY status"))
                .build());
        assertThat(rows.isError()).isFalse();
        assertThat(textOf(rows)).contains("status, n");

        // And both calls are on disk, which is the other half of the acceptance criterion.
        String audit = Files.readString(auditLog, StandardCharsets.UTF_8);
        assertThat(audit).contains("\"tool\":\"schema.describe_table\"").contains("\"tool\":\"sql.query\"");
        assertThat(audit).contains("\"backend\":\"orders-db\"").contains("\"allowed\":true");
    }

    @Test
    void aDeniedCallComesBackAsAnErrorResultNotAProtocolFailure() throws IOException {
        McpSchema.CallToolResult result = client.callTool(McpSchema.CallToolRequest.builder("sql.query")
                .arguments(Map.of("sql", "SELECT * FROM internal_audit"))
                .build());

        // isError=true rather than a transport exception: the model has to be able to read
        // the reason and try something else.
        assertThat(result.isError()).isTrue();
        assertThat(textOf(result)).contains("internal_audit").contains("customers");
        assertThat(textOf(result)).doesNotContain("this-row-must-never-reach-a-model");

        assertThat(Files.readString(auditLog, StandardCharsets.UTF_8)).contains("\"allowed\":false");
    }

    @Test
    void redactionSurvivesTheRoundTrip() {
        McpSchema.CallToolResult result = client.callTool(McpSchema.CallToolRequest.builder("sql.query")
                .arguments(Map.of("sql", "SELECT name, email FROM customers ORDER BY id"))
                .build());

        assertThat(result.isError()).isFalse();
        assertThat(String.valueOf(result.structuredContent()))
                .contains("redacted by policy")
                .doesNotContain("ada@example.com");
    }

    @Test
    void serverAdvertisesInstructionsThatFrameToolOutputAsUntrusted() {
        assertThat(client.getServerCapabilities().tools()).isNotNull();
        // Delivered at initialise, so it is in context before any row is read.
        assertThat(client.getServerInstructions()).contains("untrusted").contains("never direction");
    }

    private static String textOf(McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().get(0)).text();
    }
}
