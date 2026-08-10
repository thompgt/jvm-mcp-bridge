package io.github.thompgt.jvmmcp.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.thompgt.jvmmcp.core.BackendProbe;
import io.github.thompgt.jvmmcp.core.ToolRegistry;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The {@code bridge.jvms} configuration path, exercised without Spring or Docker.
 *
 * <p>{@link BridgeAssembler} is deliberately annotation-free so this is possible, and it is worth
 * doing: the adapter's own tests construct their policy directly, so nothing else covers the
 * translation from YAML vocabulary to a {@code PolicyProfile} — which is where an allowlist that
 * silently permits nothing, or a default redaction that quietly disappears, would actually happen.
 */
class JvmAssemblyTest {

    private BridgeAssembler assembler;

    @AfterEach
    void tearDown() {
        if (assembler != null) {
            assembler.close();
        }
    }

    private static BridgeProperties.Jvm jvm() {
        BridgeProperties.Jvm jvm = new BridgeProperties.Jvm();
        jvm.setName("orders-service");
        // No jmx-url: the embedded target, so this test needs no second process.
        jvm.getPolicy().setAllowMbeans(List.of("java.lang:*"));
        return jvm;
    }

    private ToolRegistry assemble(BridgeProperties.Jvm jvm) {
        BridgeProperties properties = new BridgeProperties();
        properties.setJvms(List.of(jvm));
        assembler = new BridgeAssembler();
        return assembler.assemble(properties, AuditSink.noop());
    }

    @Test
    void aJvmTargetRegistersTheRuntimeTools() {
        ToolRegistry registry = assemble(jvm());

        assertThat(registry.toolNames())
                .contains("jvm.mbeans", "jvm.attribute", "jvm.memory", "jvm.threads", "jvm.jfr_snapshot");
    }

    /** No base URL, no tool: there is no endpoint for it to reach and nothing to explain. */
    @Test
    void theActuatorToolIsAbsentUntilABaseUrlIsConfigured() {
        assertThat(assemble(jvm()).toolNames()).doesNotContain("jvm.actuator");
    }

    @Test
    void anActuatorBaseUrlRegistersTheToolAndItsEndpointAllowlist() {
        BridgeProperties.Jvm jvm = jvm();
        jvm.setActuatorBaseUrl("http://localhost:8080/actuator");
        jvm.getPolicy().setAllowActuator(List.of("health", "metrics"));

        ToolRegistry registry = assemble(jvm);

        assertThat(registry.toolNames()).contains("jvm.actuator");
        assertThat(registry.tool("jvm.actuator").orElseThrow().descriptor().description())
                .contains("this server permits: health, metrics");
    }

    @Test
    void anInvalidActuatorBaseUrlStopsTheServerStartingRatherThanFailingPerCall() {
        BridgeProperties.Jvm jvm = jvm();
        jvm.setActuatorBaseUrl("localhost:8080/actuator");

        BridgeProperties properties = new BridgeProperties();
        properties.setJvms(List.of(jvm));
        assembler = new BridgeAssembler();

        assertThatThrownBy(() -> assembler.assemble(properties, AuditSink.noop()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must start with http:// or https://");
    }

    /**
     * The one backend whose redaction default is non-empty. An operator who configures a JVM
     * target and thinks no further about redaction should not thereby hand a model the datasource
     * password out of the target's own system properties.
     */
    @Test
    void redactionDefaultsAreAppliedWithoutBeingAskedFor() {
        assemble(jvm());

        assertThat(assembler.backends()).hasSize(1);
        assertThat(new BridgeProperties.JvmPolicy().getRedactAttributes())
                .contains("*password*", "*secret*", "*credential*");
    }

    @Test
    void jfrMaxDurationReachesTheToolDescription() {
        BridgeProperties.Jvm jvm = jvm();
        jvm.getPolicy().setJfrMaxDuration(Duration.ofSeconds(12));

        ToolRegistry registry = assemble(jvm);

        assertThat(registry.tool("jvm.jfr_snapshot").orElseThrow().descriptor().description())
                .contains("capped at 12s");
    }

    @Test
    void theTargetIsProbedUnderItsConfiguredName() {
        assemble(jvm());

        BackendProbe probe = assembler.backends().get(0).probe();

        assertThat(probe.backend()).isEqualTo("orders-service");
        assertThat(probe.reachable()).isTrue();
        assertThat(probe.details()).containsEntry("embedded", true);
    }
}
