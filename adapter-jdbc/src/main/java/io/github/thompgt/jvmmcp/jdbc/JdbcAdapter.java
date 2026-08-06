package io.github.thompgt.jvmmcp.jdbc;

import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyEngine;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.github.thompgt.jvmmcp.policy.PolicyProfiles;
import java.util.ArrayList;
import java.util.List;

/**
 * Assembles the tools for one configured datasource.
 *
 * <p>The engine is constructed here and handed to every tool. No tool is given the
 * {@link JdbcDataSourceHandle} without also being given the engine, which is the practical
 * form the first invariant takes: there is no way to build a JDBC tool that has a connection
 * and no policy.
 */
public final class JdbcAdapter implements AutoCloseable {

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

    public JdbcDataSourceHandle handle() {
        return handle;
    }

    @Override
    public void close() {
        handle.close();
    }
}
