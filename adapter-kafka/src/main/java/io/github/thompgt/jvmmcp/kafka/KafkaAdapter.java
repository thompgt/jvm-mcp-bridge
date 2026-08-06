package io.github.thompgt.jvmmcp.kafka;

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
import org.apache.kafka.clients.admin.DescribeClusterResult;

/**
 * Assembles the tools for one configured broker.
 *
 * <p>Same shape as the JDBC adapter, for the same reason: the engine is built here and every
 * tool is handed it along with the handle, so a tool with a broker connection and no policy
 * cannot be constructed.
 */
public final class KafkaAdapter implements ProbeableBackend, AutoCloseable {

    private final KafkaBrokerHandle handle;
    private final PolicyEngine policy;

    public KafkaAdapter(KafkaBrokerHandle handle, PolicyProfile profile, AuditSink audit) {
        this(handle, PolicyProfiles.of(profile), audit);
    }

    public KafkaAdapter(KafkaBrokerHandle handle, PolicyProfiles profiles, AuditSink audit) {
        this.handle = handle;
        this.policy = new PolicyEngine(profiles, audit);
    }

    public List<BridgeTool> tools() {
        List<BridgeTool> tools = new ArrayList<>();
        tools.addAll(TopicTools.create(handle, policy));
        tools.addAll(GroupTools.create(handle, policy));
        tools.add(new PeekTool(handle, policy));
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
        details.put("allowedTopics", policy.profile().readableResources().size());

        try {
            DescribeClusterResult cluster = handle.admin().describeCluster();
            details.put("clusterId", handle.await(cluster.clusterId()));
            details.put("brokers", handle.await(cluster.nodes()).size());
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
