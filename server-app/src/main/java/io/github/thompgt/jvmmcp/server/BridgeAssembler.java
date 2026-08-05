package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.ToolRegistry;
import io.github.thompgt.jvmmcp.jdbc.JdbcAdapter;
import io.github.thompgt.jvmmcp.jdbc.JdbcDataSourceHandle;
import io.github.thompgt.jvmmcp.policy.AccessMode;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

            PolicyProfile profile = toProfile(properties.getMode(), ds);
            JdbcDataSourceHandle handle = new JdbcDataSourceHandle(
                    ds.getName(),
                    ds.getUrl(),
                    ds.getUsername(),
                    ds.getPassword(),
                    ds.getPolicy().getStatementTimeout());

            JdbcAdapter adapter = new JdbcAdapter(handle, profile, audit);
            closeables.add(adapter);
            registry.registerAll(adapter.tools());

            log.info(
                    "datasource '{}' registered in {} mode with {} readable table(s)",
                    ds.getName(),
                    profile.mode(),
                    profile.readableResources().size());
        }

        if (registry.toolCount() == 0) {
            // An MCP server with no tools looks healthy and is useless — the client connects,
            // sees nothing, and the user concludes the model "can't do it". Fail loudly.
            throw new IllegalStateException(
                    "no backends are configured, so this server would expose no tools."
                            + " Add at least one entry under bridge.datasources in your config file.");
        }
        log.info("registered {} tool(s): {}", registry.toolCount(), registry.toolNames());
        return registry;
    }

    private static PolicyProfile toProfile(AccessMode globalMode, BridgeProperties.Datasource ds) {
        BridgeProperties.Policy p = ds.getPolicy();
        PolicyProfile.Builder builder = PolicyProfile.builder(ds.getName())
                .mode(globalMode)
                .allowRead(p.getAllowTables())
                .maxRows(p.getMaxRows())
                .maxResultBytes(p.getMaxResultBytes().toBytes())
                .timeout(p.getStatementTimeout())
                .redact(p.getRedactColumns());

        if (!p.getAllowWriteTables().isEmpty()) {
            // Rejects wildcards, and build() rejects the list entirely unless mode is
            // READ_WRITE — the two independent opt-ins from ADR 003.
            builder.allowWrite(p.getAllowWriteTables());
        }
        return builder.build();
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
    }
}
