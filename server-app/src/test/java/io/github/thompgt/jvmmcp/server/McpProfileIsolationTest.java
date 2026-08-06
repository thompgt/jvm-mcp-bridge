package io.github.thompgt.jvmmcp.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.Map;
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
 * Phase 2's acceptance criterion: two API keys with different profiles, one database, one
 * server, different answers.
 *
 * <p>Three connections run against the same registry and the same connection pool, and the only
 * thing that differs is the credential. Whatever a profile restricts has to hold across every
 * tool — a bridge where {@code sql.query} respects the caller's allowlist but
 * {@code schema.list_tables} still reads the backend's would leak the shape of the data it is
 * refusing to return, and would tell the model to go and ask for it.
 */
@Tag("integration")
@Testcontainers
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "bridge.transport=http",
            "bridge.http.auth.keys[0].key=" + McpProfileIsolationTest.ANALYST_KEY,
            "bridge.http.auth.keys[0].principal=analyst",
            "bridge.http.auth.keys[0].profile=analyst",
            "bridge.http.auth.keys[1].key=" + McpProfileIsolationTest.SUPPORT_KEY,
            "bridge.http.auth.keys[1].principal=support",
            "bridge.http.auth.keys[1].profile=support",
            // No profile at all: this key gets the backend default, and is here to show the
            // default really is the ceiling the other two narrow.
            "bridge.http.auth.keys[2].key=" + McpProfileIsolationTest.OPERATOR_KEY,
            "bridge.http.auth.keys[2].principal=operator"
        })
class McpProfileIsolationTest {

    static final String ANALYST_KEY = "analyst-profile-key-0123456789";
    static final String SUPPORT_KEY = "support-profile-key-0123456789";
    static final String OPERATOR_KEY = "operator-profile-key-0123456789";

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("orders")
            .withUsername("bridge")
            .withPassword("bridge")
            .withInitScript("seed.sql");

    /** All of {@code datasources[0]} in one source; see the note in {@code McpHttpRoundTripTest}. */
    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("bridge.datasources[0].name", () -> "orders-db");
        registry.add("bridge.datasources[0].url", POSTGRES::getJdbcUrl);
        registry.add("bridge.datasources[0].username", POSTGRES::getUsername);
        registry.add("bridge.datasources[0].password", POSTGRES::getPassword);
        registry.add("bridge.datasources[0].policy.allow-tables", () -> "customers,orders,order_items");
        registry.add("bridge.datasources[0].policy.max-rows", () -> 50);

        // Each profile states only its deltas from the block above.
        registry.add("bridge.datasources[0].profiles.analyst.allow-tables", () -> "customers");
        registry.add("bridge.datasources[0].profiles.analyst.max-rows", () -> 2);
        registry.add("bridge.datasources[0].profiles.analyst.redact-columns", () -> "customers.email");
        registry.add("bridge.datasources[0].profiles.support.allow-tables", () -> "customers,orders");
    }

    @LocalServerPort
    private int port;

    @Test
    @DisplayName("each key is shown only the tables its own profile permits")
    void schemaListingIsPerProfile() {
        assertThat(listedTables(ANALYST_KEY)).contains("customers").doesNotContain("orders", "order_items");
        assertThat(listedTables(SUPPORT_KEY)).contains("customers", "orders").doesNotContain("order_items");
        assertThat(listedTables(OPERATOR_KEY)).contains("customers", "orders", "order_items");
    }

    @Test
    @DisplayName("a table one profile may read is refused to another")
    void theAllowlistIsResolvedFromTheCaller() {
        try (McpSyncClient analyst = McpTestClients.connect(port, ANALYST_KEY);
                McpSyncClient support = McpTestClients.connect(port, SUPPORT_KEY)) {

            McpSchema.CallToolResult refused = query(analyst, "SELECT id FROM orders");
            assertThat(refused.isError()).isTrue();
            // Model-actionable: it names what the *caller* may read, not what the backend holds.
            assertThat(textOf(refused))
                    .contains("orders")
                    .contains("customers")
                    .doesNotContain("order_items");

            assertThat(query(support, "SELECT id FROM orders").isError()).isFalse();
        }
    }

    @Test
    @DisplayName("redaction added by a profile applies only to that profile")
    void redactionIsPerProfile() {
        try (McpSyncClient analyst = McpTestClients.connect(port, ANALYST_KEY);
                McpSyncClient support = McpTestClients.connect(port, SUPPORT_KEY)) {

            String sql = "SELECT name, email FROM customers ORDER BY id";

            assertThat(String.valueOf(query(analyst, sql).structuredContent()))
                    .contains("redacted by policy")
                    .doesNotContain("ada@example.com");
            assertThat(String.valueOf(query(support, sql).structuredContent()))
                    .contains("ada@example.com");
        }
    }

    @Test
    @DisplayName("the row cap is the caller's, and it is enforced not merely advertised")
    void rowCapIsPerProfile() {
        try (McpSyncClient analyst = McpTestClients.connect(port, ANALYST_KEY);
                McpSyncClient support = McpTestClients.connect(port, SUPPORT_KEY)) {

            String sql = "SELECT id FROM customers ORDER BY id";

            // Asking for more than the profile allows returns the cap, and says so — the four
            // rows exist, and the analyst is told the answer is partial rather than shown three.
            McpSchema.CallToolResult capped = query(analyst, sql, 10);
            assertThat(rowCountOf(capped)).isEqualTo(2);
            assertThat(textOf(capped)).contains("TRUNCATED");

            assertThat(rowCountOf(query(support, sql))).isEqualTo(4);
        }
    }

    @Test
    @DisplayName("tool descriptions state the backend ceiling, which every profile stays under")
    void descriptionsAreAnUpperBound() {
        try (McpSyncClient analyst = McpTestClients.connect(port, ANALYST_KEY)) {
            String description = analyst.listTools().tools().stream()
                    .filter(tool -> tool.name().equals("sql.query"))
                    .findFirst()
                    .orElseThrow()
                    .description();

            // Descriptors are built once at startup, before anyone connects, so they cannot be
            // per-caller. PolicyProfiles enforces that this overstates rather than understates:
            // the analyst is promised 50 rows and gets 2, which costs a refusal at worst. The
            // reverse — promising 2 and permitting 50 — is what invites an unsafe call.
            assertThat(description).contains("capped at 50 rows");
        }
    }

    private java.util.List<String> listedTables(String key) {
        try (McpSyncClient client = McpTestClients.connect(port, key)) {
            McpSchema.CallToolResult result = client.callTool(
                    McpSchema.CallToolRequest.builder("schema.list_tables").arguments(Map.of()).build());
            assertThat(result.isError()).isFalse();
            return java.util.List.of(textOf(result).split("[^a-z_]+"));
        }
    }

    private static McpSchema.CallToolResult query(McpSyncClient client, String sql) {
        return client.callTool(McpSchema.CallToolRequest.builder("sql.query")
                .arguments(Map.of("sql", sql))
                .build());
    }

    private static McpSchema.CallToolResult query(McpSyncClient client, String sql, int maxRows) {
        return client.callTool(McpSchema.CallToolRequest.builder("sql.query")
                .arguments(Map.of("sql", sql, "max_rows", maxRows))
                .build());
    }

    private static int rowCountOf(McpSchema.CallToolResult result) {
        assertThat(result.isError()).isFalse();
        assertThat(result.structuredContent()).isInstanceOf(Map.class);
        Object rowCount = ((Map<?, ?>) result.structuredContent()).get("rowCount");
        return ((Number) rowCount).intValue();
    }

    private static String textOf(McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().get(0)).text();
    }
}
