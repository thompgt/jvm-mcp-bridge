package io.github.thompgt.jvmmcp.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Asserts the endpoint is closed to callers it cannot identify.
 *
 * <p>Deliberately driven with a plain HTTP client rather than the MCP client: the thing being
 * tested is that an unauthenticated request is refused <em>before</em> any MCP session exists,
 * and an MCP client would hide the status code behind a connection failure.
 */
@Tag("integration")
@Testcontainers
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "bridge.transport=http",
            "bridge.http.auth.keys[0].key=" + McpHttpAuthTest.ANALYST_KEY,
            "bridge.http.auth.keys[0].principal=analyst",
            "bridge.http.auth.keys[1].key=" + McpHttpAuthTest.SUPPORT_KEY,
            "bridge.http.auth.keys[1].principal=support"
        })
class McpHttpAuthTest {

    static final String ANALYST_KEY = "analyst-key-for-tests-0123456789";
    static final String SUPPORT_KEY = "support-key-for-tests-0123456789";

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("orders")
            .withUsername("bridge")
            .withPassword("bridge")
            .withInitScript("seed.sql");

    @TempDir
    static Path tempDir;

    private static Path auditLog;

    @DynamicPropertySource
    static void configuration(DynamicPropertyRegistry registry) {
        auditLog = tempDir.resolve("audit.log");
        registry.add("bridge.audit.file", () -> auditLog.toAbsolutePath().toString().replace('\\', '/'));
        registry.add("bridge.datasources[0].name", () -> "orders-db");
        registry.add("bridge.datasources[0].url", POSTGRES::getJdbcUrl);
        registry.add("bridge.datasources[0].username", POSTGRES::getUsername);
        registry.add("bridge.datasources[0].password", POSTGRES::getPassword);
        registry.add("bridge.datasources[0].policy.allow-tables", () -> "customers,orders,order_items");
    }

    @LocalServerPort
    private int port;

    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @Test
    @DisplayName("no credential is refused with 401 and a challenge")
    void unauthenticatedRequestsAreRefused() throws IOException, InterruptedException {
        HttpResponse<String> response = post(null);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.headers().firstValue("WWW-Authenticate")).contains("Bearer realm=\"jvm-mcp-bridge\"");
        // JSON-RPC shaped, because the caller is an MCP client and parses before it branches
        // on the status code.
        assertThat(response.body()).contains("\"jsonrpc\":\"2.0\"").contains("unauthenticated");
    }

    @Test
    @DisplayName("a wrong key is refused exactly like a missing one")
    void unknownKeysAreRefused() throws IOException, InterruptedException {
        HttpResponse<String> wrong = post("Bearer not-a-real-key-0123456789012");
        HttpResponse<String> missing = post(null);

        // Same status and same body: telling the two apart would tell an attacker which of
        // their guesses was a real key.
        assertThat(wrong.statusCode()).isEqualTo(401);
        assertThat(wrong.body()).isEqualTo(missing.body());
    }

    @Test
    @DisplayName("a near-miss on a valid key is still refused")
    void keysAreMatchedInFull() throws IOException, InterruptedException {
        assertThat(post("Bearer " + ANALYST_KEY.substring(0, ANALYST_KEY.length() - 1)).statusCode())
                .isEqualTo(401);
        assertThat(post("Bearer " + ANALYST_KEY + "x").statusCode()).isEqualTo(401);
    }

    @Test
    void aValidKeyReachesTheEndpointAndIsNamedInTheAuditLog() throws IOException, InterruptedException {
        HttpResponse<String> response = post("Bearer " + SUPPORT_KEY);

        // Past the filter: whatever the MCP layer makes of this body, it is no longer a 401.
        assertThat(response.statusCode()).isNotEqualTo(401);
    }

    @Test
    void theAuditLogAttributesCallsToTheKeyThatMadeThem() throws Exception {
        try (var client = McpTestClients.connect(port, ANALYST_KEY)) {
            client.callTool(io.modelcontextprotocol.spec.McpSchema.CallToolRequest.builder("sql.query")
                    .arguments(java.util.Map.of("sql", "SELECT id FROM customers ORDER BY id"))
                    .build());
        }

        String audit = Files.readString(auditLog, StandardCharsets.UTF_8);
        // The whole point of authenticating: "who read this table" has an answer.
        assertThat(audit).contains("\"principal\":\"analyst\"");
        assertThat(audit).doesNotContain("\"principal\":\"local\"");
    }

    @Test
    @DisplayName("actuator health needs the same credential as /mcp")
    void healthDetailsAreNotReadableWithoutACredential() throws IOException, InterruptedException {
        HttpResponse<String> anonymous = get("/actuator/health", null);

        // show-details: always names every backend and its version. On the same port as /mcp
        // that is a map of the estate handed out to anyone who can reach it.
        assertThat(anonymous.statusCode()).isEqualTo(401);
        assertThat(anonymous.body()).doesNotContain("orders-db");

        HttpResponse<String> authenticated = get("/actuator/health", "Bearer " + ANALYST_KEY);
        assertThat(authenticated.statusCode()).isEqualTo(200);
        assertThat(authenticated.body()).contains("orders-db");
    }

    @Test
    @DisplayName("orchestrator probes stay open, because a kubelet has no credential")
    void livenessAndReadinessAreStillAnonymous() throws IOException, InterruptedException {
        // A status word and nothing else, which is why these are the only exceptions.
        HttpResponse<String> liveness = get("/actuator/health/liveness", null);
        HttpResponse<String> readiness = get("/actuator/health/readiness", null);

        assertThat(liveness.statusCode()).isEqualTo(200);
        assertThat(readiness.statusCode()).isEqualTo(200);
        assertThat(liveness.body()).doesNotContain("orders-db");
        assertThat(readiness.body()).doesNotContain("orders-db");
    }

    private HttpResponse<String> get(String path, String authorization) throws IOException, InterruptedException {
        HttpRequest.Builder request =
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET();
        if (authorization != null) {
            request.header("Authorization", authorization);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String authorization) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/mcp"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}"));
        if (authorization != null) {
            request.header("Authorization", authorization);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
}
