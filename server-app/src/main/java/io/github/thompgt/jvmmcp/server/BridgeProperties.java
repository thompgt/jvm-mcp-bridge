package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.policy.AccessMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

/**
 * Binds {@code bridge.yaml}.
 *
 * <p>Deliberately plain: no validation logic here beyond what Boot does for types. The real
 * checks live in {@code PolicyProfile.Builder}, so that a configuration mistake is caught by
 * the same code whether it arrives from YAML, a test, or a future admin API.
 */
@ConfigurationProperties(prefix = "bridge")
public class BridgeProperties {

    /** Global default; a datasource may not exceed it, only narrow it. */
    private AccessMode mode = AccessMode.READ_ONLY;

    private Transport transport = Transport.STDIO;

    private Audit audit = new Audit();

    private List<Datasource> datasources = new ArrayList<>();

    public enum Transport {
        /** Launched as a subprocess by an MCP client. */
        STDIO,
        /** Streamable HTTP on /mcp, for a shared deployment. Phase 2. */
        HTTP
    }

    public static class Audit {
        /** When set, records go to this file as JSON lines. Otherwise to the application log. */
        private String file;

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }
    }

    public static class Datasource {
        private String name;
        private String url;
        private String username;
        private String password;
        private Policy policy = new Policy();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Policy getPolicy() {
            return policy;
        }

        public void setPolicy(Policy policy) {
            this.policy = policy;
        }
    }

    public static class Policy {
        private List<String> allowTables = new ArrayList<>();
        private List<String> allowWriteTables = new ArrayList<>();
        private int maxRows = 100;
        private DataSize maxResultBytes = DataSize.ofMegabytes(1);
        private Duration statementTimeout = Duration.ofSeconds(5);
        private List<String> redactColumns = new ArrayList<>();

        public List<String> getAllowTables() {
            return allowTables;
        }

        public void setAllowTables(List<String> allowTables) {
            this.allowTables = allowTables;
        }

        public List<String> getAllowWriteTables() {
            return allowWriteTables;
        }

        public void setAllowWriteTables(List<String> allowWriteTables) {
            this.allowWriteTables = allowWriteTables;
        }

        public int getMaxRows() {
            return maxRows;
        }

        public void setMaxRows(int maxRows) {
            this.maxRows = maxRows;
        }

        public DataSize getMaxResultBytes() {
            return maxResultBytes;
        }

        public void setMaxResultBytes(DataSize maxResultBytes) {
            this.maxResultBytes = maxResultBytes;
        }

        public Duration getStatementTimeout() {
            return statementTimeout;
        }

        public void setStatementTimeout(Duration statementTimeout) {
            this.statementTimeout = statementTimeout;
        }

        public List<String> getRedactColumns() {
            return redactColumns;
        }

        public void setRedactColumns(List<String> redactColumns) {
            this.redactColumns = redactColumns;
        }
    }

    public AccessMode getMode() {
        return mode;
    }

    public void setMode(AccessMode mode) {
        this.mode = mode;
    }

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public Audit getAudit() {
        return audit;
    }

    public void setAudit(Audit audit) {
        this.audit = audit;
    }

    public List<Datasource> getDatasources() {
        return datasources;
    }

    public void setDatasources(List<Datasource> datasources) {
        this.datasources = datasources;
    }
}
