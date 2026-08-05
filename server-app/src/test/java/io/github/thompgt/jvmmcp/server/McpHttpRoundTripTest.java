package io.github.thompgt.jvmmcp.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.spec.McpSchema;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Drives the Streamable HTTP transport over a real socket.
 *
 * <p>The stdio round-trip test proves the tools work; this one proves the <em>second</em>
 * transport serves the identical registry. The risk being tested is drift — a guardrail or a
 * tool that exists on one transport and not the other, which is how a shared deployment ends
 * up less safe than the local one it was modelled on.
 */
@Tag("integration")
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "bridge.transport=http")
class McpHttpRoundTripTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("orders")
            .withUsername("bridge")
            .withPassword("bridge")
            .withInitScript("seed.sql");

    /**
     * Every {@code datasources[0]} key is set here, including the static ones. Spring Boot binds
     * a list from the single highest-precedence property source that contains it rather than
     * merging across sources, so splitting these between {@code @SpringBootTest(properties)} and
     * this method silently drops whichever half loses.
     */
    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("bridge.datasources[0].name", () -> "orders-db");
        registry.add("bridge.datasources[0].url", POSTGRES::getJdbcUrl);
        registry.add("bridge.datasources[0].username", POSTGRES::getUsername);
        registry.add("bridge.datasources[0].password", POSTGRES::getPassword);
        registry.add("bridge.datasources[0].policy.allow-tables", () -> "customers,orders,order_items");
        registry.add("bridge.datasources[0].policy.max-rows", () -> 50);
        registry.add("bridge.datasources[0].policy.redact-columns", () -> "*.email");
    }

    @LocalServerPort
    private int port;

    private McpSyncClient client;

    @BeforeEach
    void connect() {
        HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport.builder(
                        "http://localhost:" + port)
                .endpoint("/mcp")
                .jsonMapper(McpJsonDefaults.getMapper())
                .build();
        client = McpClient.sync(transport)
                .requestTimeout(Duration.ofSeconds(30))
                .initializationTimeout(Duration.ofSeconds(30))
                .build();
        client.initialize();
    }

    @AfterEach
    void disconnect() {
        if (client != null) {
            client.closeGracefully();
        }
    }

    @Test
    void servesTheSameToolsOverHttpAsOverStdio() {
        assertThat(client.listTools().tools())
                .extracting(McpSchema.Tool::name)
                .containsExactlyInAnyOrder("sql.query", "sql.explain", "schema.list_tables", "schema.describe_table");

        assertThat(client.getServerInstructions()).contains("untrusted");
    }

    @Test
    void runsAQueryAndGetsStructuredRowsBack() {
        McpSchema.CallToolResult result = client.callTool(McpSchema.CallToolRequest.builder("sql.query")
                .arguments(Map.of("sql", "SELECT id, name FROM customers ORDER BY id"))
                .build());

        assertThat(result.isError()).isFalse();
        assertThat(result.structuredContent()).isNotNull();
        assertThat(textOf(result)).contains("4 row(s)");
    }

    @Test
    @DisplayName("the allowlist denies over HTTP exactly as it does over stdio")
    void guardrailsApplyOnThisTransportToo() {
        McpSchema.CallToolResult result = client.callTool(McpSchema.CallToolRequest.builder("sql.query")
                .arguments(Map.of("sql", "SELECT * FROM internal_audit"))
                .build());

        assertThat(result.isError()).isTrue();
        assertThat(textOf(result)).contains("internal_audit").contains("customers");
        assertThat(textOf(result)).doesNotContain("this-row-must-never-reach-a-model");
    }

    @Test
    void redactionAppliesOverHttpToo() {
        McpSchema.CallToolResult result = client.callTool(McpSchema.CallToolRequest.builder("sql.query")
                .arguments(Map.of("sql", "SELECT name, email FROM customers ORDER BY id"))
                .build());

        assertThat(String.valueOf(result.structuredContent()))
                .contains("redacted by policy")
                .doesNotContain("ada@example.com");
    }

    private static String textOf(McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().get(0)).text();
    }
}
