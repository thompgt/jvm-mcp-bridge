package io.github.thompgt.jvmmcp.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class SqlDialectTest {

    @Test
    @DisplayName("no dialect offers a plan that writes to get one")
    void oracleHasNoExplainBecauseItsExplainWrites() {
        // `EXPLAIN PLAN FOR` inserts rows into PLAN_TABLE and returns no result set. Running it
        // from sql.explain would make a tool annotated readOnlyHint(true) write to the database.
        assertThat(SqlDialect.ORACLE.explainPrefix()).isNull();
        assertThat(SqlDialect.SQLSERVER.explainPrefix()).isNull();
    }

    @ParameterizedTest
    @EnumSource(SqlDialect.class)
    void anyExplainPrefixThatExistsIsAPlainReadingOne(SqlDialect dialect) {
        String prefix = dialect.explainPrefix();
        if (prefix == null) {
            return;
        }
        // ANALYZE would execute the query, which is the opposite of a preflight.
        assertThat(prefix).isEqualTo("EXPLAIN ");
    }

    @Test
    void dialectsAreRecognisedFromTheJdbcUrl() {
        assertThat(SqlDialect.forJdbcUrl("jdbc:oracle:thin:@//host:1521/orders")).isEqualTo(SqlDialect.ORACLE);
        assertThat(SqlDialect.forJdbcUrl("jdbc:postgresql://host/orders")).isEqualTo(SqlDialect.POSTGRESQL);
        assertThat(SqlDialect.forJdbcUrl("jdbc:acme://host/orders")).isEqualTo(SqlDialect.GENERIC);
    }
}
