package io.github.thompgt.jvmmcp.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The attack suite.
 *
 * <p>Each case here is a documented way a "read-only" database MCP server has been or could
 * be bypassed. They assert the <em>rule</em> that refused as well as the refusal, because a
 * statement rejected for the wrong reason is a rule that will let the right attack through.
 */
class SqlGuardTest {

    @Test
    void acceptsAPlainSelectAndResolvesItsTable() {
        SqlGuard.ReadStatement read = SqlGuard.requireReadOnly("SELECT id, name FROM customers");

        assertThat(read.tables()).containsExactly("customers");
    }

    @Test
    void resolvesEveryTableInAJoinNotJustTheFirst() {
        SqlGuard.ReadStatement read = SqlGuard.requireReadOnly(
                "SELECT c.name, o.total_cents FROM customers c JOIN orders o ON o.customer_id = c.id");

        // If only the leading table were resolved, a join onto a forbidden table would be
        // invisible to the policy engine. This is the allowlist-bypass-via-join case.
        assertThat(read.tables()).containsExactlyInAnyOrder("customers", "orders");
    }

    @Test
    void resolvesTablesReachedThroughASubquery() {
        SqlGuard.ReadStatement read = SqlGuard.requireReadOnly(
                "SELECT * FROM customers WHERE id IN (SELECT actor_id FROM internal_audit)");

        assertThat(read.tables()).contains("internal_audit");
    }

    @Test
    @DisplayName("stacked statements are refused outright, not silently truncated")
    void refusesStackedStatements() {
        // The classic bypass: the first statement validates, and the driver runs both.
        var e = catchThrowableOfType(
                SqlGuard.RejectedStatementException.class,
                () -> SqlGuard.requireReadOnly("SELECT 1 FROM customers; DROP TABLE customers"));

        assertThat(e.rule()).isEqualTo("single-statement");
        assertThat(e).hasMessageContaining("only the first would be checked");
    }

    @Test
    void refusesADataModifyingCommonTableExpression() {
        // Top-level node is a Select, and it deletes rows.
        var e = catchThrowableOfType(
                SqlGuard.RejectedStatementException.class,
                () -> SqlGuard.requireReadOnly(
                        "WITH gone AS (DELETE FROM orders RETURNING *) SELECT * FROM gone"));

        assertThat(e.rule()).isEqualTo("read-only");
        assertThat(e).hasMessageContaining("CTE that writes is still a write");
    }

    @Test
    void refusesSelectIntoWhichCreatesATable() {
        var e = catchThrowableOfType(
                SqlGuard.RejectedStatementException.class,
                () -> SqlGuard.requireReadOnly("SELECT * INTO orders_copy FROM orders"));

        assertThat(e.rule()).isEqualTo("read-only");
        assertThat(e).hasMessageContaining("creates a table");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "UPDATE orders SET status = 'shipped'",
                "DELETE FROM orders",
                "INSERT INTO orders (status) VALUES ('x')",
                "DROP TABLE customers",
                "TRUNCATE TABLE orders",
                "ALTER TABLE orders ADD COLUMN x INT",
                "CREATE TABLE evil (id INT)",
            })
    void refusesEveryWritingStatementKind(String sql) {
        assertThatThrownBy(() -> SqlGuard.requireReadOnly(sql))
                .isInstanceOf(SqlGuard.RejectedStatementException.class)
                .hasMessageContaining("only SELECT and WITH statements are permitted");
    }

    @ParameterizedTest
    @DisplayName("statements the parser cannot represent fail closed")
    @ValueSource(strings = {"GRANT ALL ON orders TO PUBLIC", "VACUUM FULL orders", "COPY orders TO '/tmp/x'"})
    void refusesStatementsTheParserCannotRepresent(String sql) {
        // JSqlParser has no node for some vendor DDL. "We could not understand it" must mean
        // refused, never passed through — an analyser that fails open is not an analyser.
        var e = catchThrowableOfType(SqlGuard.RejectedStatementException.class, () -> SqlGuard.requireReadOnly(sql));

        assertThat(e.rule()).isEqualTo("parse");
        assertThat(e).hasMessageContaining("will not be run");
    }

    @ParameterizedTest
    @DisplayName("comment and whitespace obfuscation does not change the parse tree")
    @ValueSource(
            strings = {
                "/* SELECT */ UPDATE orders SET status = 'x'",
                "--SELECT * FROM customers\nDELETE FROM orders",
                "\n\t  DELETE   FROM\n orders",
                "UpDaTe orders SeT status = 'x'",
            })
    void refusesObfuscatedWrites(String sql) {
        // A regex over the raw string can be talked out of any of these. A parser cannot.
        assertThatThrownBy(() -> SqlGuard.requireReadOnly(sql))
                .isInstanceOf(SqlGuard.RejectedStatementException.class);
    }

    @Test
    void unparseableSqlIsRefusedRatherThanPassedThrough() {
        var e = catchThrowableOfType(
                SqlGuard.RejectedStatementException.class,
                () -> SqlGuard.requireReadOnly("SELECT FROM WHERE ((("));

        assertThat(e.rule()).isEqualTo("parse");
        // Fail closed: something we cannot analyse is something we do not run.
        assertThat(e).hasMessageContaining("cannot be checked and will not be run");
    }

    @Test
    void cteAliasesAreNotTreatedAsTablesToBeAllowlisted() {
        SqlGuard.ReadStatement read = SqlGuard.requireReadOnly(
                "WITH recent AS (SELECT * FROM orders) SELECT * FROM recent");

        // `recent` is this statement's own name for a result set, not a table an operator
        // could ever have allowlisted.
        assertThat(read.tables()).containsExactly("orders");
    }

    @Test
    void quotingAndSchemaQualificationCannotHideATableName() {
        SqlGuard.ReadStatement read =
                SqlGuard.requireReadOnly("SELECT * FROM \"public\".\"Internal_Audit\"");

        // Quoted, qualified and mixed-case must all resolve to the same name the allowlist
        // is written in, or the allowlist is trivially sidestepped.
        assertThat(read.tables()).containsExactly("internal_audit");
    }

    @Test
    void setOperationsResolveTablesFromEveryBranch() {
        SqlGuard.ReadStatement read = SqlGuard.requireReadOnly(
                "SELECT id FROM customers UNION ALL SELECT actor_id FROM internal_audit");

        assertThat(read.tables()).containsExactlyInAnyOrder("customers", "internal_audit");
    }
}
