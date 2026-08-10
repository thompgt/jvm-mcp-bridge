package io.github.thompgt.jvmmcp.jvm;

import io.github.thompgt.jvmmcp.core.BackendProbe;
import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ProbeableBackend;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyEngine;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.github.thompgt.jvmmcp.policy.PolicyProfiles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Assembles the tools for one JVM target.
 *
 * <p>Same shape as the JDBC and Kafka adapters, for the same reason: the engine is built here and
 * handed to every tool with the handle, so a tool holding a JMX connection and no policy cannot be
 * constructed.
 */
public final class JvmAdapter implements ProbeableBackend, AutoCloseable {

    private final JvmTargetHandle handle;
    private final PolicyEngine policy;

    public JvmAdapter(JvmTargetHandle handle, PolicyProfile profile, AuditSink audit) {
        this(handle, PolicyProfiles.of(profile), audit);
    }

    public JvmAdapter(JvmTargetHandle handle, PolicyProfiles profiles, AuditSink audit) {
        this.handle = handle;
        this.policy = new PolicyEngine(profiles, audit);
    }

    public List<BridgeTool> tools() {
        List<BridgeTool> tools = new ArrayList<>(MBeanTools.create(handle, policy));
        return List.copyOf(tools);
    }

    public PolicyEngine policy() {
        return policy;
    }

    @Override
    public String backendName() {
        return handle.name();
    }

    @Override
    public BackendProbe probe() {
        long startedAt = System.nanoTime();
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("mode", policy.profile().mode().name());
        details.put("embedded", handle.isEmbedded());
        details.put("allowedMBeanPatterns", policy.profile().readableResources().size());

        try {
            // getMBeanCount is the cheapest call that proves the connection works end to end:
            // it crosses the wire, needs no MBean to exist, and cannot be slow in a way that
            // says anything other than "the target is not answering".
            Integer count = handle.call(handle.connectTimeout(), connection -> connection.getMBeanCount());
            details.put("mbeans", count);
            return BackendProbe.up(handle.name(), millisSince(startedAt), details);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return BackendProbe.down(handle.name(), millisSince(startedAt), details, "interrupted");
        } catch (Exception e) {
            return BackendProbe.down(handle.name(), millisSince(startedAt), details, BackendProbe.describe(e));
        }
    }

    private static long millisSince(long startedAtNanos) {
        return Duration.ofNanos(System.nanoTime() - startedAtNanos).toMillis();
    }

    @Override
    public void close() {
        handle.close();
    }
}
