package io.github.thompgt.jvmmcp.jdbc;

import java.util.Locale;

/**
 * The handful of places where SQL databases differ in ways this adapter cares about.
 *
 * <p>Only three things are dialect-specific here: how a statement timeout is set, how a query
 * plan is requested, and whether the server supports a read-only transaction. Everything else
 * goes through {@code DatabaseMetaData} or standard SQL.
 */
public enum SqlDialect {
    POSTGRESQL,
    MYSQL,
    ORACLE,
    SQLSERVER,
    H2,
    GENERIC;

    public static SqlDialect forJdbcUrl(String jdbcUrl) {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(Locale.ROOT);
        if (url.startsWith("jdbc:postgresql")) {
            return POSTGRESQL;
        }
        if (url.startsWith("jdbc:mysql") || url.startsWith("jdbc:mariadb")) {
            return MYSQL;
        }
        if (url.startsWith("jdbc:oracle")) {
            return ORACLE;
        }
        if (url.startsWith("jdbc:sqlserver")) {
            return SQLSERVER;
        }
        if (url.startsWith("jdbc:h2")) {
            return H2;
        }
        return GENERIC;
    }

    /**
     * A statement that binds the timeout to the current transaction, or {@code null} where the
     * dialect has no equivalent.
     *
     * <p>{@code Statement.setQueryTimeout} is set as well and is the portable mechanism, but
     * it is enforced by the driver: it stops the client waiting without necessarily stopping
     * the server working. A server-side timeout is what actually protects the database.
     */
    public String statementTimeoutSql(long millis) {
        return switch (this) {
            case POSTGRESQL -> "SET LOCAL statement_timeout = " + millis;
            case MYSQL -> "SET SESSION MAX_EXECUTION_TIME = " + millis;
            case H2 -> "SET QUERY_TIMEOUT " + millis;
            // Oracle and SQL Server express this through resource governor / profile settings
            // rather than a session statement, so the portable query timeout is all we have.
            case ORACLE, SQLSERVER, GENERIC -> null;
        };
    }

    /** Whether {@code SET TRANSACTION READ ONLY} is understood. */
    public boolean supportsReadOnlyTransaction() {
        return this == POSTGRESQL || this == MYSQL;
    }

    /** Prefix that turns a query into a plan request without executing it. */
    public String explainPrefix() {
        return switch (this) {
            // Deliberately not ANALYZE: that would run the query, which is the opposite of
            // what a plan preflight is for.
            case POSTGRESQL -> "EXPLAIN ";
            case MYSQL -> "EXPLAIN ";
            case H2 -> "EXPLAIN ";
            case ORACLE -> "EXPLAIN PLAN FOR ";
            case SQLSERVER, GENERIC -> null;
        };
    }
}
