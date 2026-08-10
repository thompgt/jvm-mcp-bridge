package io.github.thompgt.jvmmcp.jvm;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.github.thompgt.jvmmcp.policy.PolicyProfiles;
import io.github.thompgt.jvmmcp.policy.Redactor;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Against a real HTTP server rather than a mocked client.
 *
 * <p>Half of what this tool has to get right is on the wire — the path it actually requests after
 * validation, the credentials header, the query string for tag filters, what it does with a
 * redirect or an oversized body. A stubbed client would let every one of those be wrong while the
 * assertions passed. {@code com.sun.net.httpserver} is in the JDK, so this costs no dependency.
 */
class ActuatorToolsTest {

    private static HttpServer server;
    private static String baseUrl;

    /** Paths the fake Actuator was asked for, in order — the assertion for path handling. */
    private static final List<String> requested = new ArrayList<>();

    private static volatile String lastAuthorization;

    private JvmAdapter adapter;

    @BeforeAll
    static void startFakeActuator() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

        server.createContext("/actuator/health", exchange -> respond(exchange, 200, "application/json", """
                {"status":"UP","components":{"db":{"status":"UP","details":{"database":"PostgreSQL"}},
                "kafka":{"status":"DOWN","details":{"error":"connection refused"}}}}"""));

        server.createContext("/actuator/env", exchange -> respond(exchange, 200, "application/json", """
                {"activeProfiles":["prod"],
                 "propertySources":{"applicationConfig":{
                   "spring.datasource.url":"jdbc:postgresql://db:5432/orders",
                   "spring.datasource.password":"hunter2",
                   "app.api-key":"sk-live-1234"}}}"""));

        server.createContext("/actuator/metrics", exchange -> respond(exchange, 200, "application/json", """
                {"name":"jvm.memory.used","measurements":[{"statistic":"VALUE","value":1234.5}]}"""));

        server.createContext("/actuator/plain", exchange -> respond(exchange, 200, "text/plain", "not json at all"));

        server.createContext("/actuator/huge", exchange -> respond(exchange, 200, "application/json",
                "{\"padding\":\"" + "x".repeat(20_000) + "\"}"));

        server.createContext("/actuator/secured", exchange -> respond(exchange, 401, "application/json", "{}"));

        server.createContext("/actuator/moved", exchange -> {
            record(exchange);
            exchange.getResponseHeaders().add("Location", "http://127.0.0.1:1/elsewhere");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });

        // Anything not matched above, including a would-be traversal that escaped validation.
        server.createContext("/", exchange -> respond(exchange, 404, "application/json", "{\"status\":404}"));

        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/actuator";
    }

    private static void respond(HttpExchange exchange, int status, String contentType, String body)
            throws IOException {
        record(exchange);
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static void record(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();
        requested.add(exchange.getRequestURI().getPath() + (query == null ? "" : "?" + query));
        lastAuthorization = exchange.getRequestHeaders().getFirst("Authorization");
    }

    @AfterAll
    static void stopFakeActuator() {
        server.stop(0);
    }

    @BeforeEach
    void resetRecording() {
        requested.clear();
        lastAuthorization = null;
    }

    @AfterEach
    void tearDown() {
        if (adapter != null) {
            adapter.close();
            adapter = null;
        }
    }

    private static PolicyProfile.Builder policy() {
        return PolicyProfile.builder("orders-service")
                .allowRead(
                        "java.lang:*",
                        ActuatorTools.RESOURCE_PREFIX + "health",
                        ActuatorTools.RESOURCE_PREFIX + "env",
                        ActuatorTools.RESOURCE_PREFIX + "metrics",
                        ActuatorTools.RESOURCE_PREFIX + "plain",
                        ActuatorTools.RESOURCE_PREFIX + "huge",
                        ActuatorTools.RESOURCE_PREFIX + "secured",
                        ActuatorTools.RESOURCE_PREFIX + "moved",
                        ActuatorTools.RESOURCE_PREFIX + "missing")
                .maxRows(100)
                .maxResultBytes(8_000L)
                .timeout(Duration.ofSeconds(10));
    }

    private ToolOutcome call(PolicyProfile profile, ActuatorHandle actuator, Map<String, Object> arguments) {
        adapter = new JvmAdapter(
                JvmTargetHandle.embedded("orders-service"), actuator, PolicyProfiles.of(profile), AuditSink.noop());
        BridgeTool tool = adapter.tools().stream()
                .filter(t -> t.descriptor().name().equals("jvm.actuator"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("jvm.actuator was not registered"));
        return tool.call(arguments);
    }

    private ToolOutcome call(Map<String, Object> arguments) {
        return call(policy().build(), handle(null, null), arguments);
    }

    private static ActuatorHandle handle(String username, String password) {
        return new ActuatorHandle(baseUrl, username, password, null, Duration.ofSeconds(5));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> structured(ToolOutcome outcome) {
        return (Map<String, Object>) outcome.structured();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    @Test
    void readsAndParsesAnEndpoint() {
        ToolOutcome outcome = call(Map.of("endpoint", "health"));

        assertThat(outcome.error()).isFalse();
        Map<String, Object> result = structured(outcome);
        assertThat(result.get("status")).isEqualTo(200);
        Map<String, Object> body = asMap(result.get("body"));
        assertThat(body.get("status")).isEqualTo("UP");
        assertThat(asMap(asMap(asMap(body.get("components")).get("kafka")).get("details")).get("error"))
                .isEqualTo("connection refused");
        assertThat(requested).containsExactly("/actuator/health");
    }

    @Test
    void aSubResourceIsASecondSegment() {
        call(Map.of("endpoint", "metrics/jvm.memory.used"));

        assertThat(requested).containsExactly("/actuator/metrics/jvm.memory.used");
    }

    @Test
    void tagsBecomeRepeatedQueryParameters() {
        call(Map.of("endpoint", "metrics/jvm.memory.used", "tags", List.of("area:heap", "id:G1 Eden".replace(" ", "-"))));

        assertThat(requested).containsExactly("/actuator/metrics/jvm.memory.used?tag=area:heap&tag=id:G1-Eden");
    }

    /**
     * The host is this server's, never the caller's. Each of these is a way of asking for a
     * different one, and each is refused before any request is made.
     */
    @Test
    void pathsThatWouldLeaveTheConfiguredBaseAreRefused() {
        for (String attempt : List.of(
                "../../etc/passwd",
                "..",
                "health/../../admin",
                "http://evil.example.com/",
                "//evil.example.com/x",
                "health?x=1",
                "health%2F..%2Fadmin")) {
            ToolOutcome outcome = call(Map.of("endpoint", attempt));

            assertThat(outcome.error()).as("endpoint '%s' must be refused", attempt).isTrue();
            assertThat(outcome.summary()).as("endpoint '%s'", attempt).contains("This server chooses the host");
        }
        assertThat(requested).as("nothing should have reached the network").isEmpty();
    }

    @Test
    void tooManySegmentsIsRefusedWithTheLimit() {
        ToolOutcome outcome = call(Map.of("endpoint", "metrics/a/b"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("at most 2");
        assertThat(requested).isEmpty();
    }

    @Test
    void aMalformedTagIsRefusedBeforeTheRequest() {
        ToolOutcome outcome = call(Map.of("endpoint", "metrics", "tags", List.of("area heap")));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("key:value");
        assertThat(requested).isEmpty();
    }

    @Test
    void anEndpointOutsideTheAllowlistIsDeniedWithTheAlternatives() {
        ToolOutcome outcome = call(
                PolicyProfile.builder("orders-service")
                        .allowRead(ActuatorTools.RESOURCE_PREFIX + "health")
                        .build(),
                handle(null, null),
                Map.of("endpoint", "env"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("actuator:health");
        assertThat(requested).isEmpty();
    }

    /**
     * Actuator masks some keys itself, by a setting belonging to the target application. The
     * policy redactor is applied again here so what leaves this process does not depend on that.
     */
    @Test
    void nestedConfigurationValuesAreRedactedByPolicy() {
        ToolOutcome outcome = call(
                policy().redact("*password*", "*api-key*").build(), handle(null, null), Map.of("endpoint", "env"));

        Map<String, Object> sources = asMap(asMap(asMap(structured(outcome).get("body")).get("propertySources"))
                .get("applicationConfig"));
        assertThat(sources.get("spring.datasource.password")).isEqualTo(Redactor.MARKER);
        assertThat(sources.get("app.api-key")).isEqualTo(Redactor.MARKER);
        assertThat(sources.get("spring.datasource.url")).isEqualTo("jdbc:postgresql://db:5432/orders");
        assertThat(structured(outcome).get("redacted")).isEqualTo(true);
        assertThat(outcome.summary()).contains("not evidence it is safe");
    }

    @Test
    void basicCredentialsAreSentWhenConfigured() {
        call(policy().build(), handle("probe", "s3cret"), Map.of("endpoint", "health"));

        assertThat(lastAuthorization).isEqualTo("Basic "
                + java.util.Base64.getEncoder()
                        .encodeToString("probe:s3cret".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void noCredentialHeaderIsSentWhenNoneIsConfigured() {
        call(Map.of("endpoint", "health"));

        assertThat(lastAuthorization).isNull();
    }

    @Test
    void aRedirectIsNotFollowed() {
        ToolOutcome outcome = call(Map.of("endpoint", "moved"));

        assertThat(outcome.error()).isTrue();
        assertThat(requested).containsExactly("/actuator/moved");
    }

    @Test
    void a404SaysTheApplicationDoesNotExposeItRatherThanBlamingPolicy() {
        ToolOutcome outcome = call(Map.of("endpoint", "missing"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary())
                .contains("This server permits the endpoint")
                .contains("management.endpoints.web.exposure.include");
    }

    @Test
    void a401NamesTheConfigurationRatherThanInvitingARetry() {
        ToolOutcome outcome = call(Map.of("endpoint", "secured"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("not something to retry");
    }

    @Test
    void aNonJsonResponseIsReturnedAsText() {
        ToolOutcome outcome = call(Map.of("endpoint", "plain"));

        assertThat(outcome.error()).isFalse();
        assertThat(asMap(structured(outcome).get("body")).get("text")).isEqualTo("not json at all");
    }

    /** A body cut mid-object is no longer JSON; reporting that as a parse error would blame the app. */
    @Test
    void anOversizedBodyIsTruncatedAndExplained() {
        ToolOutcome outcome = call(Map.of("endpoint", "huge"));

        assertThat(outcome.error()).isFalse();
        assertThat(structured(outcome).get("truncated")).isEqualTo(true);
        assertThat(asMap(structured(outcome).get("body"))).containsKey("text");
        assertThat(outcome.summary()).contains("cut at this server's size cap").contains("sub-resource");
    }

    @Test
    void noToolIsRegisteredWhenTheTargetHasNoActuator() {
        adapter = new JvmAdapter(
                JvmTargetHandle.embedded("orders-service"), policy().build(), AuditSink.noop());

        assertThat(adapter.tools()).extracting(t -> t.descriptor().name()).doesNotContain("jvm.actuator");
    }

    @Test
    void theDescriptionNamesThePermittedEndpoints() {
        adapter = new JvmAdapter(
                JvmTargetHandle.embedded("orders-service"),
                handle(null, null),
                PolicyProfiles.of(PolicyProfile.builder("orders-service")
                        .allowRead(ActuatorTools.RESOURCE_PREFIX + "health")
                        .build()),
                AuditSink.noop());

        String description = adapter.tools().stream()
                .filter(t -> t.descriptor().name().equals("jvm.actuator"))
                .findFirst()
                .orElseThrow()
                .descriptor()
                .description();

        assertThat(description).contains("this server permits: health").contains("Any other endpoint is refused");
    }
}
