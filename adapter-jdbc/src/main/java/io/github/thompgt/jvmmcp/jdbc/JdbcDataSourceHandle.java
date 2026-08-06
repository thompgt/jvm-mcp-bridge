package io.github.thompgt.jvmmcp.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;

/**
 * Owns the connection pool for one configured datasource.
 *
 * <p>The pool is small on purpose. This bridge serves interactive questions from a model, not
 * application traffic; a large pool here would let a burst of generated queries become the
 * thing that exhausts a production database's connection limit.
 */
public final class JdbcDataSourceHandle implements AutoCloseable {

    private final String name;
    private final HikariDataSource dataSource;
    private final SqlDialect dialect;

    public JdbcDataSourceHandle(String name, String url, String username, String password, Duration timeout) {
        this.name = name;
        this.dialect = SqlDialect.forJdbcUrl(url);

        HikariConfig config = new HikariConfig();
        config.setPoolName("jvm-mcp-bridge-" + name);
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(4);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(Math.max(timeout.toMillis(), 1_000L));
        // Defence in depth: every connection from this pool starts read-only, so a code path
        // that somehow skipped SqlGuard would still be refused by the driver.
        config.setReadOnly(true);
        config.setAutoCommit(true);
        this.dataSource = new HikariDataSource(config);
    }

    /** Package-private: only {@link JdbcQueryRunner} gets a connection, and only under policy. */
    Connection connection() throws SQLException {
        return dataSource.getConnection();
    }

    public String name() {
        return name;
    }

    public SqlDialect dialect() {
        return dialect;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    /**
     * Takes a connection from the pool and asks the driver whether it is still good.
     *
     * <p>Not a {@code SELECT 1}: {@link Connection#isValid} is what the driver itself considers
     * a live connection, it runs no user SQL, and it honours the timeout even when the server
     * has stopped answering rather than gone away — which is the failure mode a query-based
     * check hangs on.
     *
     * @return the product name and version, for a caller that wants to report them
     */
    ProductInfo validate(Duration timeout) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            int seconds = Math.max(1, (int) timeout.toSeconds());
            if (!connection.isValid(seconds)) {
                throw new SQLException("the driver reports the connection is not valid");
            }
            var meta = connection.getMetaData();
            return new ProductInfo(meta.getDatabaseProductName(), meta.getDatabaseProductVersion());
        }
    }

    /** What the database says it is. Neither field contains a host or a credential. */
    record ProductInfo(String product, String version) {}

    @Override
    public void close() {
        dataSource.close();
    }
}
