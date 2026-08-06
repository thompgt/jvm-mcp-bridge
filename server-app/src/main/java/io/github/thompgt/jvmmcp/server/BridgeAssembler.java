package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.ToolRegistry;
import io.github.thompgt.jvmmcp.jdbc.JdbcAdapter;
import io.github.thompgt.jvmmcp.jdbc.JdbcDataSourceHandle;
import io.github.thompgt.jvmmcp.policy.AccessMode;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.github.thompgt.jvmmcp.policy.PolicyProfiles;
import java.nio.file.Path;
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

            PolicyProfiles profiles = toProfiles(properties.getMode(), ds);
            PolicyProfile profile = profiles.defaultProfile();
            JdbcDataSourceHandle handle = new JdbcDataSourceHandle(
                    ds.getName(),
                    ds.getUrl(),
                    ds.getUsername(),
                    ds.getPassword(),
                    ds.getPolicy().getStatementTimeout());

            JdbcAdapter adapter = new JdbcAdapter(handle, profiles, audit);
            closeables.add(adapter);
            registry.registerAll(adapter.tools());

            log.info(
                    "datasource '{}' registered in {} mode with {} readable table(s) and profile(s) {}",
                    ds.getName(),
                    profile.mode(),
                    profile.readableResources().size(),
                    profiles.names().isEmpty() ? "[default only]" : profiles.names());
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

    private static PolicyProfiles toProfiles(AccessMode globalMode, BridgeProperties.Datasource ds) {
        PolicyProfile backendDefault = toProfile(globalMode, ds);
        if (ds.getProfiles().isEmpty()) {
            return PolicyProfiles.of(backendDefault);
        }

        Map<String, PolicyProfile> named = new LinkedHashMap<>();
        for (Map.Entry<String, BridgeProperties.ProfileOverride> entry : ds.getProfiles().entrySet()) {
            named.put(entry.getKey(), overlay(backendDefault, globalMode, ds, entry.getValue()));
        }
        // Rejects any profile broader than the default, naming the dimension that widened.
        return PolicyProfiles.of(backendDefault, named);
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

    /** Applies a profile's deltas on top of the datasource policy; unset fields inherit. */
    private static PolicyProfile overlay(
            PolicyProfile backendDefault,
            AccessMode globalMode,
            BridgeProperties.Datasource ds,
            BridgeProperties.ProfileOverride override) {
        BridgeProperties.Policy p = ds.getPolicy();
        List<String> writable =
                override.getAllowWriteTables() == null ? p.getAllowWriteTables() : override.getAllowWriteTables();

        PolicyProfile.Builder builder = PolicyProfile.builder(ds.getName())
                .mode(override.getMode() == null ? globalMode : override.getMode())
                .allowRead(override.getAllowTables() == null ? p.getAllowTables() : override.getAllowTables())
                .maxRows(override.getMaxRows() == null ? p.getMaxRows() : override.getMaxRows())
                .maxResultBytes(
                        override.getMaxResultBytes() == null
                                ? p.getMaxResultBytes().toBytes()
                                : override.getMaxResultBytes().toBytes())
                .timeout(
                        override.getStatementTimeout() == null
                                ? p.getStatementTimeout()
                                : override.getStatementTimeout())
                // The default's patterns always apply; a profile can only add to them.
                .redact(backendDefault.redactionPatterns())
                .redact(override.getRedactColumns());

        if (!writable.isEmpty()) {
            builder.allowWrite(writable);
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
