package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.ProbeableBackend;
import io.github.thompgt.jvmmcp.core.ToolRegistry;
import io.github.thompgt.jvmmcp.jdbc.JdbcAdapter;
import io.github.thompgt.jvmmcp.jdbc.JdbcDataSourceHandle;
import io.github.thompgt.jvmmcp.jvm.ActuatorHandle;
import io.github.thompgt.jvmmcp.jvm.ActuatorTools;
import io.github.thompgt.jvmmcp.jvm.JvmAdapter;
import io.github.thompgt.jvmmcp.jvm.JvmTargetHandle;
import io.github.thompgt.jvmmcp.kafka.KafkaAdapter;
import io.github.thompgt.jvmmcp.kafka.KafkaBrokerHandle;
import io.github.thompgt.jvmmcp.policy.AccessMode;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.github.thompgt.jvmmcp.policy.PolicyProfiles;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns bound configuration into a populated {@link ToolRegistry}.
 *
 * <p>Kept free of Spring annotations so the whole assembly can be exercised from a test with a
 * hand-built {@link BridgeProperties} and no application context.
 */
public final class BridgeAssembler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BridgeAssembler.class);

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final List<ProbeableBackend> backends = new ArrayList<>();

    public AuditSink auditSink(BridgeProperties properties) {
        String file = properties.getAudit().getFile();
        if (file == null || file.isBlank()) {
            return AuditSink.logging();
        }
        AuditSink sink = AuditSink.file(Path.of(file));
        if (sink instanceof AutoCloseable closeable) {
            closeables.add(closeable);
        }
        return sink;
    }

    public ToolRegistry assemble(BridgeProperties properties, AuditSink audit) {
        ToolRegistry registry = new ToolRegistry();

        for (BridgeProperties.Datasource ds : properties.getDatasources()) {
            requireText(ds.getName(), "bridge.datasources[].name");
            requireText(ds.getUrl(), "bridge.datasources[].url for datasource '" + ds.getName() + "'");

            PolicyProfiles profiles = toProfiles(
                    properties.getMode(), PolicySpec.of(ds.getName(), ds.getPolicy()), ds.getProfiles());
            PolicyProfile profile = profiles.defaultProfile();
            JdbcDataSourceHandle handle = new JdbcDataSourceHandle(
                    ds.getName(),
                    ds.getUrl(),
                    ds.getUsername(),
                    ds.getPassword(),
                    ds.getPolicy().getStatementTimeout());

            JdbcAdapter adapter = new JdbcAdapter(handle, profiles, audit);
            closeables.add(adapter);
            backends.add(adapter);
            registry.registerAll(adapter.tools());

            log.info(
                    "datasource '{}' registered in {} mode with {} readable table(s) and profile(s) {}",
                    ds.getName(),
                    profile.mode(),
                    profile.readableResources().size(),
                    profiles.names().isEmpty() ? "[default only]" : profiles.names());
        }

        for (BridgeProperties.Broker broker : properties.getBrokers()) {
            requireText(broker.getName(), "bridge.brokers[].name");
            requireText(
                    broker.getBootstrapServers(),
                    "bridge.brokers[].bootstrap-servers for broker '" + broker.getName() + "'");

            PolicyProfiles profiles = toProfiles(
                    properties.getMode(),
                    PolicySpec.of(broker.getName(), broker.getPolicy()),
                    broker.getProfiles());
            PolicyProfile profile = profiles.defaultProfile();
            KafkaBrokerHandle handle = new KafkaBrokerHandle(
                    broker.getName(),
                    broker.getBootstrapServers(),
                    broker.getPolicy().getRequestTimeout(),
                    broker.getClient());

            KafkaAdapter adapter = new KafkaAdapter(handle, profiles, audit);
            closeables.add(adapter);
            backends.add(adapter);
            registry.registerAll(adapter.tools());

            log.info(
                    "broker '{}' registered in {} mode with {} readable topic pattern(s) and profile(s) {}",
                    broker.getName(),
                    profile.mode(),
                    profile.readableResources().size(),
                    profiles.names().isEmpty() ? "[default only]" : profiles.names());
        }

        for (BridgeProperties.Jvm jvm : properties.getJvms()) {
            requireText(jvm.getName(), "bridge.jvms[].name");

            PolicyProfiles profiles = toProfiles(
                    properties.getMode(), PolicySpec.of(jvm.getName(), jvm.getPolicy()), jvm.getProfiles());
            PolicyProfile profile = profiles.defaultProfile();
            JvmTargetHandle handle = new JvmTargetHandle(
                    jvm.getName(),
                    jvm.getJmxUrl(),
                    jvm.getPolicy().getRequestTimeout(),
                    jvm.getUsername(),
                    jvm.getPassword(),
                    jvm.getPolicy().getJfrMaxDuration());

            // Null when no base URL is configured, which registers no jvm.actuator tool at all.
            ActuatorHandle actuator = jvm.getActuatorBaseUrl() == null
                            || jvm.getActuatorBaseUrl().isBlank()
                    ? null
                    : new ActuatorHandle(
                            jvm.getActuatorBaseUrl(),
                            jvm.getActuatorUsername(),
                            jvm.getActuatorPassword(),
                            jvm.getActuatorToken(),
                            jvm.getPolicy().getRequestTimeout());

            JvmAdapter adapter = new JvmAdapter(handle, actuator, profiles, audit);
            closeables.add(adapter);
            backends.add(adapter);
            registry.registerAll(adapter.tools());

            log.info(
                    "jvm '{}' registered in {} mode against {} with {} readable resource(s){} and profile(s) {}",
                    jvm.getName(),
                    profile.mode(),
                    handle.isEmbedded() ? "this process" : "a remote JMX connector",
                    profile.readableResources().size(),
                    actuator == null ? "" : ", plus Actuator at " + actuator.baseUrl(),
                    profiles.names().isEmpty() ? "[default only]" : profiles.names());
        }

        if (registry.toolCount() == 0) {
            // An MCP server with no tools looks healthy and is useless — the client connects,
            // sees nothing, and the user concludes the model "can't do it". Fail loudly.
            throw new IllegalStateException(
                    "no backends are configured, so this server would expose no tools."
                            + " Add at least one entry under bridge.datasources, bridge.brokers or"
                            + " bridge.jvms in your config file.");
        }
        log.info("registered {} tool(s): {}", registry.toolCount(), registry.toolNames());
        return registry;
    }

    /** Every configured backend, in declaration order. Populated by {@link #assemble}. */
    public List<ProbeableBackend> backends() {
        return List.copyOf(backends);
    }

    /**
     * One backend's policy in backend-neutral terms.
     *
     * <p>Datasources speak tables and rows, brokers speak topics and messages, and the operator
     * should write the word that fits the backend. This is where the two vocabularies meet, so
     * that the profile overlay and the narrowing check exist once rather than once per adapter —
     * a second copy of that logic is a second place for the two to disagree about what
     * "narrower" means.
     */
    private record PolicySpec(
            String backendName,
            List<String> allowRead,
            List<String> allowWrite,
            int maxRecords,
            long maxResultBytes,
            Duration timeout,
            List<String> redact) {

        static PolicySpec of(String name, BridgeProperties.Policy p) {
            return new PolicySpec(
                    name,
                    p.getAllowTables(),
                    p.getAllowWriteTables(),
                    p.getMaxRows(),
                    p.getMaxResultBytes().toBytes(),
                    p.getStatementTimeout(),
                    p.getRedactColumns());
        }

        /**
         * A JVM target has no write allowlist because the adapter has no write path: reading an
         * attribute is the only thing it can do, at any access mode. See {@code MBeanTools} for
         * why {@code invoke} and {@code setAttribute} are not offered even behind two gates.
         */
        static PolicySpec of(String name, BridgeProperties.JvmPolicy p) {
            // MBean patterns and Actuator endpoints share one read allowlist, the endpoints
            // prefixed so the two cannot collide. One list rather than two because the engine
            // has one, and a second dimension would put the narrowing check — what stops a
            // named profile widening a backend default — in two places that have to agree.
            List<String> readable = new ArrayList<>(p.getAllowMbeans());
            p.getAllowActuator().forEach(endpoint -> readable.add(
                    ActuatorTools.RESOURCE_PREFIX + endpoint.trim()));
            return new PolicySpec(
                    name,
                    readable,
                    List.of(),
                    p.getMaxResults(),
                    p.getMaxResultBytes().toBytes(),
                    p.getRequestTimeout(),
                    p.getRedactAttributes());
        }

        static PolicySpec of(String name, BridgeProperties.BrokerPolicy p) {
            return new PolicySpec(
                    name,
                    p.getAllowTopics(),
                    p.getAllowWriteTopics(),
                    p.getMaxMessages(),
                    p.getMaxResultBytes().toBytes(),
                    p.getRequestTimeout(),
                    p.getRedactHeaders());
        }

        /** Applies a profile's deltas; every unset field inherits from this spec. */
        PolicySpec with(BridgeProperties.ProfileOverride override) {
            return new PolicySpec(
                    backendName,
                    override.getAllowTables() == null ? allowRead : override.getAllowTables(),
                    override.getAllowWriteTables() == null ? allowWrite : override.getAllowWriteTables(),
                    override.getMaxRows() == null ? maxRecords : override.getMaxRows(),
                    override.getMaxResultBytes() == null
                            ? maxResultBytes
                            : override.getMaxResultBytes().toBytes(),
                    override.getStatementTimeout() == null ? timeout : override.getStatementTimeout(),
                    // The backend's patterns always apply; a profile can only add to them.
                    concat(redact, override.getRedactColumns()));
        }

        PolicyProfile toProfile(AccessMode mode, AccessMode overrideMode) {
            PolicyProfile.Builder builder = PolicyProfile.builder(backendName)
                    .mode(overrideMode == null ? mode : overrideMode)
                    .allowRead(allowRead)
                    .maxRows(maxRecords)
                    .maxResultBytes(maxResultBytes)
                    .timeout(timeout)
                    .redact(redact);
            if (!allowWrite.isEmpty()) {
                // Rejects wildcards, and build() rejects the list entirely unless mode is
                // READ_WRITE — the two independent opt-ins from ADR 003.
                builder.allowWrite(allowWrite);
            }
            return builder.build();
        }

        private static List<String> concat(List<String> a, List<String> b) {
            List<String> merged = new ArrayList<>(a);
            b.stream().filter(entry -> !merged.contains(entry)).forEach(merged::add);
            return merged;
        }
    }

    private static PolicyProfiles toProfiles(
            AccessMode globalMode, PolicySpec spec, Map<String, BridgeProperties.ProfileOverride> overrides) {
        PolicyProfile backendDefault = spec.toProfile(globalMode, null);
        if (overrides.isEmpty()) {
            return PolicyProfiles.of(backendDefault);
        }

        Map<String, PolicyProfile> named = new LinkedHashMap<>();
        for (Map.Entry<String, BridgeProperties.ProfileOverride> entry : overrides.entrySet()) {
            named.put(
                    entry.getKey(),
                    spec.with(entry.getValue()).toProfile(globalMode, entry.getValue().getMode()));
        }
        // Rejects any profile broader than the default, naming the dimension that widened.
        return PolicyProfiles.of(backendDefault, named);
    }

    private static void requireText(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(what + " is required");
        }
    }

    @Override
    public void close() {
        for (AutoCloseable closeable : closeables.reversed()) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.warn("failed to close {}", closeable.getClass().getSimpleName(), e);
            }
        }
        closeables.clear();
        backends.clear();
    }
}
