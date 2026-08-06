package io.github.thompgt.jvmmcp.jdbc;

import io.github.thompgt.jvmmcp.core.BackendProbe;
import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ProbeableBackend;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyEngine;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.github.thompgt.jvmmcp.policy.PolicyProfiles;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Assembles the tools for one configured datasource.
 *
 * <p>The engine is constructed here and handed to every tool. No tool is given the
 * {@link JdbcDataSourceHandle} without also being given the engine, which is the practical
 * form the first invariant takes: there is no way to build a JDBC tool that has a connection
 * and no policy.
 */
public final class JdbcAdapter implements ProbeableBackend, AutoCloseable {

    private final JdbcDataSourceHandle handle;
    private final PolicyEngine policy;

    /** Single-profile backend: every caller gets the same policy. */
    public JdbcAdapter(JdbcDataSourceHandle handle, PolicyProfile profile, AuditSink audit) {
        this(handle, PolicyProfiles.of(profile), audit);
    }

    public JdbcAdapter(JdbcDataSourceHandle handle, PolicyProfiles profiles, AuditSink audit) {
        this.handle = handle;
        this.policy = new PolicyEngine(profiles, audit);
    }

    /**
     * Tools are named without a datasource prefix when there is only one datasource, which is
     * the common case and keeps the names the model sees short.
     */
    public List<BridgeTool> tools() {
        List<BridgeTool> tools = new ArrayList<>();
        tools.add(new SqlQueryTool(handle, policy));
        tools.add(new SqlExplainTool(handle, policy));
        tools.addAll(SchemaTools.create(handle, policy));
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
        // The timeout is the configured statement timeout: a database that cannot answer
        // "are you there" inside the budget a real query gets is not usable for queries,
        // whatever it would eventually have said.
        Duration timeout = policy.timeout();
        long startedAt = System.nanoTime();
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("dialect", handle.dialect().name().toLowerCase(Locale.ROOT));
        details.put("mode", policy.profile().mode().name());
        details.put("readableResources", policy.profile().readableResources().size());

        try {
            JdbcDataSourceHandle.ProductInfo info = handle.validate(timeout);
            details.put("product", info.product());
            details.put("version", info.version());
            return BackendProbe.up(handle.name(), millisSince(startedAt), details);
        } catch (SQLException | RuntimeException e) {
            return BackendProbe.down(
                    handle.name(), millisSince(startedAt), details, BackendProbe.describe(e));
        }
    }

    private static long millisSince(long startedAtNanos) {
        return Duration.ofNanos(System.nanoTime() - startedAtNanos).toMillis();
    }

    public JdbcDataSourceHandle handle() {
        return handle;
    }

    @Override
    public void close() {
        handle.close();
    }
}
