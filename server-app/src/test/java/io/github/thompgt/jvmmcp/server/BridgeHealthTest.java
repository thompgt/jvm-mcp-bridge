package io.github.thompgt.jvmmcp.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Health reporting, including what happens when a backend actually goes away.
 *
 * <p>The container is stopped part-way through, which is why the methods are ordered — the
 * degraded case has to run last and there is no way to bring a Testcontainers Postgres back on
 * the same port. Everything before it runs against a live database.
 */
@Tag("integration")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "bridge.transport=http",
            "bridge.http.auth.mode=none",
            "bridge.http.auth.i-understand-this-is-unauthenticated=true"
        })
class BridgeHealthTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("orders")
            .withUsername("bridge")
            .withPassword("bridge")
            .withInitScript("seed.sql");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("bridge.datasources[0].name", () -> "orders-db");
        registry.add("bridge.datasources[0].url", POSTGRES::getJdbcUrl);
        registry.add("bridge.datasources[0].username", POSTGRES::getUsername);
        registry.add("bridge.datasources[0].password", POSTGRES::getPassword);
        registry.add("bridge.datasources[0].policy.allow-tables", () -> "customers,orders");
        // Short, so the degraded case fails fast instead of holding the scrape open.
        registry.add("bridge.datasources[0].policy.statement-timeout", () -> "2s");
    }

    @LocalServerPort
    private int port;

    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @Test
    @Order(1)
    @DisplayName("each backend is reported under its own configured name")
    void backendsAreReportedIndividually() throws Exception {
        HttpResponse<String> health = get("/actuator/health");

        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(health.body()).contains("\"status\":\"UP\"").contains("orders-db").contains("PostgreSQL");
    }

    @Test
    @Order(2)
    @DisplayName("health never discloses the connection string or the credentials")
    void detailsCarryNothingSensitive() throws Exception {
        // Scoped to what this repo contributes; Boot's own diskSpace indicator reports a path,
        // which is its choice to make and not what is being asserted here.
        String body = get("/actuator/health/backends").body();

        // The endpoint is not behind the MCP auth filter, because a probe cannot authenticate.
        assertThat(body)
                .contains("orders-db")
                .doesNotContain("jdbc:")
                .doesNotContain(POSTGRES.getUsername())
                .doesNotContain(POSTGRES.getPassword())
                .doesNotContain(String.valueOf(POSTGRES.getFirstMappedPort()));
    }

    @Test
    @Order(3)
    @DisplayName("readiness reflects the bridge itself, not the backends behind it")
    void readinessReportsTheMcpLayer() throws Exception {
        HttpResponse<String> readiness = get("/actuator/health/readiness");

        assertThat(readiness.statusCode()).isEqualTo(200);
        assertThat(readiness.body()).contains("sql.query").contains("\"tools\":4");
        // The datasource is deliberately absent: see the group configuration in application.yaml.
        assertThat(readiness.body()).doesNotContain("orders-db");
    }

    @Test
    @Order(4)
    @DisplayName("a dead backend degrades the bridge, and the endpoint still answers 200")
    void aDeadBackendDoesNotTakeTheBridgeDown() throws Exception {
        POSTGRES.stop();

        HttpResponse<String> health = get("/actuator/health");

        // 200 with DEGRADED, not 503 with DOWN. A load balancer keeps this instance in
        // rotation, which is right — every tool that does not need this database still works,
        // and the ones that do fail with a reason rather than a connection reset.
        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(health.body()).contains("DEGRADED");

        // Liveness is untouched: nothing about a remote database says restart this JVM.
        assertThat(get("/actuator/health/liveness").statusCode()).isEqualTo(200);
        assertThat(get("/actuator/health/readiness").statusCode()).isEqualTo(200);
    }

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                        .timeout(Duration.ofSeconds(30))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
