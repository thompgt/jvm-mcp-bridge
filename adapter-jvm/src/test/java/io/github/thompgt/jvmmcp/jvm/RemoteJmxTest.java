package io.github.thompgt.jvmmcp.jvm;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.thompgt.jvmmcp.core.BackendProbe;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The sidecar path: the same tools, reached over a real RMI connector rather than a field access.
 *
 * <p>Worth a fixture of its own even though the assertions look like the embedded ones. The
 * embedded case cannot fail in any of the ways the remote case can, so it proves nothing about
 * the code that matters most here — connecting, reconnecting, and returning an answer to a target
 * that has stopped answering. The connector server runs in this JVM and exports the same platform
 * MBeans, so the loop is genuine RMI (serialisation, a registry lookup, a socket) without needing
 * a second process.
 *
 * <p>Not tagged {@code integration}: it needs no Docker daemon, and the transport it covers is the
 * one every real deployment of this adapter will use.
 */
class RemoteJmxTest {

    private static Registry registry;
    private static JMXConnectorServer connectorServer;
    private static String url;

    @BeforeAll
    static void startConnectorServer() throws Exception {
        // A port claimed and released rather than a fixed one: 1099 is the JMX default and is
        // exactly the port a developer running this suite is most likely to have something on.
        // The window between releasing and rebinding is a race in principle and has no better
        // answer — LocateRegistry.createRegistry(0) binds a free port but does not report which.
        int port;
        try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        registry = LocateRegistry.createRegistry(port);
        url = "service:jmx:rmi:///jndi/rmi://127.0.0.1:" + port + "/jmxrmi";
        connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(
                new JMXServiceURL(url), null, ManagementFactory.getPlatformMBeanServer());
        connectorServer.start();
    }

    @AfterAll
    static void stopConnectorServer() throws Exception {
        if (connectorServer != null) {
            connectorServer.stop();
        }
        if (registry != null) {
            java.rmi.server.UnicastRemoteObject.unexportObject(registry, true);
        }
    }

    private JvmAdapter adapter(Duration timeout) {
        return new JvmAdapter(
                new JvmTargetHandle("orders-service", url, timeout, null, null),
                PolicyProfile.builder("orders-service")
                        .allowRead("java.lang:*")
                        .maxRows(100)
                        .timeout(timeout)
                        .build(),
                AuditSink.noop());
    }

    @Test
    void readsAnAttributeOverTheConnector() throws Exception {
        try (JvmAdapter adapter = adapter(Duration.ofSeconds(10))) {
            assertThat(adapter.backendName()).isEqualTo("orders-service");

            ToolOutcome outcome = adapter.tools().stream()
                    .filter(t -> t.descriptor().name().equals("jvm.attribute"))
                    .findFirst()
                    .orElseThrow()
                    .call(Map.of("mbean", "java.lang:type=Memory", "attributes", List.of("HeapMemoryUsage")));

            assertThat(outcome.error()).isFalse();
            @SuppressWarnings("unchecked")
            Map<String, Object> structured = (Map<String, Object>) outcome.structured();
            assertThat(structured.get("mbean")).isEqualTo("java.lang:type=Memory");
        }
    }

    @Test
    void probeReportsTheTargetReachable() throws Exception {
        try (JvmAdapter adapter = adapter(Duration.ofSeconds(10))) {
            BackendProbe probe = adapter.probe();

            assertThat(probe.reachable()).isTrue();
            assertThat(probe.details()).containsEntry("embedded", false);
            assertThat((Integer) probe.details().get("mbeans")).isPositive();
            // The URL carries a hostname and a port and must not reach an unauthenticated
            // health endpoint; see BackendProbe.
            assertThat(probe.details().values()).noneMatch(v -> String.valueOf(v).contains("rmi://"));
        }
    }

    /**
     * A target that cannot be reached is a refusal, not a hang. The address below is in the
     * documentation-only range from RFC 5737, so nothing routes to it and the connect attempt has
     * to be stopped by the bound rather than by a refused connection.
     */
    @Test
    void anUnreachableTargetFailsTheProbeWithinTheTimeout() throws Exception {
        JvmTargetHandle unreachable = new JvmTargetHandle(
                "gone",
                "service:jmx:rmi:///jndi/rmi://192.0.2.1:9010/jmxrmi",
                Duration.ofMillis(750),
                null,
                null);
        try (JvmAdapter adapter = new JvmAdapter(
                unreachable, PolicyProfile.builder("gone").allowRead("java.lang:*").build(), AuditSink.noop())) {

            long startedAt = System.nanoTime();
            BackendProbe probe = adapter.probe();
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(probe.reachable()).isFalse();
            assertThat(probe.error()).isNotBlank();
            assertThat(elapsed).isLessThan(Duration.ofSeconds(10));
        }
    }
}
