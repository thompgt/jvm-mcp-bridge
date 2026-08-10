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
    private final List<BridgeTool> tools;

    public JvmAdapter(JvmTargetHandle handle, PolicyProfile profile, AuditSink audit) {
        this(handle, PolicyProfiles.of(profile), audit);
    }

    public JvmAdapter(JvmTargetHandle handle, PolicyProfiles profiles, AuditSink audit) {
        this(handle, null, profiles, audit);
    }

    /**
     * @param actuator the target's Actuator, or {@code null} when it has none. Absent rather than
     *     refusing, unlike the Kafka write tools: those are registered even where they cannot run
     *     because the model needs to be told <em>why</em> a thing it can see is refused. Here
     *     there is nothing to explain — an application with no Actuator has no endpoint that any
     *     configuration change to this server would reach.
     */
    public JvmAdapter(
            JvmTargetHandle handle, ActuatorHandle actuator, PolicyProfiles profiles, AuditSink audit) {
        this.handle = handle;
        this.policy = new PolicyEngine(profiles, audit);

        List<BridgeTool> built = new ArrayList<>(MBeanTools.create(handle, policy));
        built.addAll(MemoryTools.create(handle, policy));
        built.addAll(ThreadTools.create(handle, policy));
        if (actuator != null) {
            built.addAll(ActuatorTools.create(handle, actuator, policy));
        }
        this.tools = List.copyOf(built);
    }

    /**
     * The tools, built once.
     *
     * <p>Built in the constructor rather than per call because {@code jvm.memory} keeps the
     * previous reading in order to report a delta against it. A {@code tools()} that returned
     * fresh instances would hand a second caller a tool that had never seen the first, so every
     * call would look like a first call and no delta would ever be reported — a bug that is
     * invisible in a server that assembles its registry once at startup and appears the moment
     * anything else asks twice.
     */
    public List<BridgeTool> tools() {
        return tools;
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
