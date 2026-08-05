package io.github.thompgt.jvmmcp.jdbc;

import io.github.thompgt.jvmmcp.policy.EffectiveLimits;
import io.github.thompgt.jvmmcp.policy.Redactor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes a statement that {@link SqlGuard} has already accepted and the policy engine has
 * already allowed.
 *
 * <p>This is the last of the four independent read-only layers described in ADR 003. By the
 * time control reaches here the statement has been parsed and its tables checked, but the
 * connection is still configured as if none of that had happened — read-only, time-boxed,
 * row-capped. Each layer assumes the others might have been circumvented.
 */
final class JdbcQueryRunner {

    private JdbcQueryRunner() {}

    static ResultSetReader.Page runQuery(
            JdbcDataSourceHandle handle,
            String sql,
            String primaryTable,
            EffectiveLimits limits,
            Redactor redactor)
            throws SQLException {
        try (Connection connection = handle.connection()) {
            prepareReadOnly(connection, handle.dialect(), limits.timeout());
            try (Statement statement = connection.createStatement()) {
                applyStatementBounds(statement, handle.dialect(), limits);
                try (ResultSet rs = statement.executeQuery(sql)) {
                    return ResultSetReader.read(
                            rs, primaryTable, limits.maxRows(), limits.maxResultBytes(), redactor);
                }
            }
        }
    }

    /** Runs the dialect's EXPLAIN and returns the plan lines. */
    static List<String> explain(JdbcDataSourceHandle handle, String sql, EffectiveLimits limits)
            throws SQLException {
        String prefix = handle.dialect().explainPrefix();
        if (prefix == null) {
            return List.of();
        }
        try (Connection connection = handle.connection()) {
            prepareReadOnly(connection, handle.dialect(), limits.timeout());
            try (Statement statement = connection.createStatement()) {
                applyStatementBounds(statement, handle.dialect(), limits);
                try (ResultSet rs = statement.executeQuery(prefix + sql)) {
                    List<String> plan = new ArrayList<>();
                    int columns = rs.getMetaData().getColumnCount();
                    while (rs.next() && plan.size() < 200) {
                        StringBuilder line = new StringBuilder();
                        for (int i = 1; i <= columns; i++) {
                            if (i > 1) {
                                line.append(' ');
                            }
                            line.append(rs.getString(i));
                        }
                        plan.add(line.toString());
                    }
                    return List.copyOf(plan);
                }
            }
        }
    }

    private static void prepareReadOnly(Connection connection, SqlDialect dialect, Duration timeout)
            throws SQLException {
        connection.setReadOnly(true);

        if (dialect.supportsReadOnlyTransaction()) {
            // An explicit transaction is what makes SET LOCAL statement_timeout stick, and it
            // is also what makes the server enforce read-only rather than only the driver.
            connection.setAutoCommit(false);
            try (Statement s = connection.createStatement()) {
                s.execute("SET TRANSACTION READ ONLY");
            }
        }

        String timeoutSql = dialect.statementTimeoutSql(timeout.toMillis());
        if (timeoutSql != null) {
            try (Statement s = connection.createStatement()) {
                s.execute(timeoutSql);
            }
        }
    }

    private static void applyStatementBounds(Statement statement, SqlDialect dialect, EffectiveLimits limits)
            throws SQLException {
        // maxRows tells the driver to stop fetching; ResultSetReader also stops at the cap.
        // Both, because a driver that ignores maxRows would otherwise stream the whole table.
        statement.setMaxRows(limits.maxRows());
        statement.setFetchSize(Math.min(limits.maxRows(), 500));

        long seconds = Math.max(1, limits.timeout().toSeconds());
        // Portable, driver-enforced. On dialects with no server-side timeout it is the only
        // protection there is, which is why it is set even when statementTimeoutSql applied.
        statement.setQueryTimeout((int) Math.min(seconds, Integer.MAX_VALUE));
        if (dialect == SqlDialect.GENERIC) {
            statement.setEscapeProcessing(false);
        }
    }
}
