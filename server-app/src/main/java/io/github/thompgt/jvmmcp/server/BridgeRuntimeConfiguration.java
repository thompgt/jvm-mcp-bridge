package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.ToolRegistry;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Publishes the assembled registry as beans so both transports share one assembly.
 *
 * <p>The assembly itself is still done by the Spring-free {@link BridgeAssembler}; this class
 * only decides when it runs and who holds the result. Under stdio the context has no web
 * server and these are the only beans that matter; under HTTP the servlet configuration picks
 * the same registry up. Neither transport gets its own copy of the wiring, which is the point —
 * a guardrail that only applies on one transport is not a guardrail.
 */
@Configuration(proxyBeanMethods = false)
public class BridgeRuntimeConfiguration {

    /**
     * Registered as a bean rather than closed by hand so its {@code close()} runs on context
     * shutdown. It owns the connection pools; leaking those keeps the JVM alive after the
     * client has gone.
     */
    @Bean
    public BridgeAssembler bridgeAssembler() {
        return new BridgeAssembler();
    }

    @Bean
    public AuditSink auditSink(BridgeAssembler assembler, BridgeProperties properties) {
        return assembler.auditSink(properties);
    }

    @Bean
    public ToolRegistry toolRegistry(
            BridgeAssembler assembler, BridgeProperties properties, AuditSink audit) {
        return assembler.assemble(properties, audit);
    }
}
