package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.BackendProbe;
import io.github.thompgt.jvmmcp.core.ProbeableBackend;
import io.github.thompgt.jvmmcp.core.ToolRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Health reporting, one entry per configured backend.
 *
 * <p>An unreachable backend reports {@link #DEGRADED} rather than {@code DOWN}, and
 * {@code application.yaml} maps that status to HTTP 200. This is the whole point of reporting
 * backends separately: a bridge fronting a database and a broker is still doing its job when
 * the broker is unreachable — the database tools work, and the broker tools fail with a clear
 * reason. Reporting {@code DOWN} would take the pod out of rotation and stop the calls that
 * would have succeeded, turning one backend's outage into an outage of everything.
 *
 * <p>A backend that is genuinely required is expressed by the operator, in a health group, not
 * by this class deciding on their behalf.
 */
@Configuration(proxyBeanMethods = false)
public class BridgeHealthConfiguration {

    /**
     * Serving, but not everything it was configured to front is reachable.
     *
     * <p>Ordered between {@code UP} and {@code OUT_OF_SERVICE} in {@code application.yaml}, so
     * one degraded backend degrades the aggregate — visible on a dashboard — without failing it.
     */
    public static final Status DEGRADED = new Status("DEGRADED", "at least one backend is unreachable");

    /**
     * {@code /actuator/health/backends/<name>}, and a rolled-up {@code backends} entry.
     *
     * <p>Depends on {@link ToolRegistry} rather than on the assembler alone because the
     * registry is what forces assembly to have happened; asking an unassembled bridge about
     * its backends would report a healthy bridge with none.
     */
    @Bean
    public HealthContributor backends(BridgeAssembler assembler, ToolRegistry registry) {
        Map<String, HealthContributor> contributors = new LinkedHashMap<>();
        for (ProbeableBackend backend : assembler.backends()) {
            contributors.put(backend.backendName(), (HealthIndicator) () -> toHealth(backend.probe()));
        }
        if (contributors.isEmpty()) {
            // assemble() already refuses to start with no backends; this keeps the endpoint
            // well-formed rather than relying on that from two places.
            return (HealthIndicator) () -> Health.unknown()
                    .withDetail("reason", "no backends are configured")
                    .build();
        }
        return CompositeHealthContributor.fromMap(contributors);
    }

    private static Health toHealth(BackendProbe probe) {
        Health.Builder health = probe.reachable() ? Health.up() : Health.status(DEGRADED);
        probe.asMap().forEach(health::withDetail);
        return health.build();
    }

    /**
     * The bridge's own readiness, which is not the same question as any backend's.
     *
     * <p>Reports what the MCP layer is actually offering. A server that starts, connects to
     * everything, and exposes zero tools looks healthy from every other angle and is useless —
     * the client connects, sees nothing, and the user concludes the model cannot do it.
     */
    @Bean
    public HealthIndicator mcp(ToolRegistry registry, BridgeProperties properties) {
        return () -> Health.up()
                .withDetail("transport", properties.getTransport().name().toLowerCase(java.util.Locale.ROOT))
                .withDetail("mode", properties.getMode().name())
                .withDetail("tools", registry.toolCount())
                .withDetail("toolNames", registry.toolNames())
                .build();
    }
}
