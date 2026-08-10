package io.github.thompgt.jvmmcp.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.thompgt.jvmmcp.policy.Redactor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The redaction bypasses.
 *
 * <p>Redaction is decided from result-set metadata, and result-set metadata is under the
 * caller's influence: the caller chooses the alias, and the caller chooses whether the column
 * arrives as itself or wrapped in a function. Each case here is a way a rule written as
 * {@code *.email} stopped applying to the email column. They run against a real driver, because
 * what the driver reports is the whole question.
 */
class ResultSetReaderRedactionTest {

    private static Connection connection;

    @BeforeAll
    static void seed() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:redaction;DB_CLOSE_DELAY=-1");
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(64), email VARCHAR(64))");
            s.execute("INSERT INTO customers VALUES (1, 'Ada Lovelace', 'ada@example.com')");
            s.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total_cents INT)");
            s.execute("INSERT INTO orders VALUES (10, 1, 500)");
        }
    }

    @AfterAll
    static void close() throws SQLException {
        connection.close();
    }

    private static final Redactor EMAIL = new Redactor(List.of("*.email"));

    /** Reads one row through the same path {@code sql.query} uses. */
    private static Map<String, Object> firstRow(String sql, Redactor redactor) throws SQLException {
        SqlGuard.ReadStatement read = SqlGuard.requireReadOnly(sql);
        boolean unnamed = SqlQueryTool.computesOverRedactedColumn(read, redactor);
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            ResultSetReader.Page page = ResultSetReader.read(rs, read.tables(), unnamed, 10, 1_000_000L, redactor);
            assertThat(page.rows()).isNotEmpty();
            // Keys folded because H2 reports labels upper-cased; the assertions are about the
            // values, not about which case a particular driver echoes back.
            Map<String, Object> row = new java.util.LinkedHashMap<>();
            page.rows().get(0).forEach((k, v) -> row.put(k.toLowerCase(java.util.Locale.ROOT), v));
            return row;
        }
    }

    @Test
    void redactsTheColumnWhenItIsSelectedPlainly() throws SQLException {
        assertThat(firstRow("SELECT name, email FROM customers", EMAIL))
                .containsEntry("name", "Ada Lovelace")
                .containsEntry("email", Redactor.MARKER);
    }

    @Test
    @DisplayName("an alias does not lift the redaction")
    void aliasingTheColumnDoesNotDefeatRedaction() throws SQLException {
        // The one-word bypass: deciding on getColumnLabel means `AS x` is a rule you can
        // rename your way out of. The value must be withheld whatever it is called.
        Map<String, Object> row = firstRow("SELECT email AS x FROM customers", EMAIL);

        assertThat(row).containsEntry("x", Redactor.MARKER);
        assertThat(row.values()).doesNotContain("ada@example.com");
    }

    @Test
    @DisplayName("wrapping the column in a function does not lift the redaction")
    void computingOverTheColumnDoesNotDefeatRedaction() throws SQLException {
        // A computed item has no underlying column name at all, so metadata cannot decide it.
        // The parse tree still contains the word `email`, and that is what fails it closed.
        Map<String, Object> row = firstRow("SELECT lower(email) AS x FROM customers", EMAIL);

        assertThat(row).containsEntry("x", Redactor.MARKER);
        assertThat(row.values()).doesNotContain("ada@example.com");
    }

    @Test
    void concatenatingTheColumnIntoAnotherDoesNotDefeatRedaction() throws SQLException {
        Map<String, Object> row = firstRow("SELECT name || '/' || email AS blended FROM customers", EMAIL);

        assertThat(row).containsEntry("blended", Redactor.MARKER);
        assertThat(row.values()).doesNotContain("Ada Lovelace/ada@example.com");
    }

    @Test
    @DisplayName("a computed column unrelated to a redacted one still comes back")
    void unrelatedComputedColumnsAreNotRedacted() throws SQLException {
        // Failing closed on every unnamed column would redact count(*), which withholds
        // nothing and teaches the model the tool is broken.
        assertThat(firstRow("SELECT count(*) AS n FROM customers", EMAIL))
                .containsEntry("n", 1L);
    }

    @Test
    @DisplayName("a table-scoped rule fires on a join even when it is not the leading table")
    void tableScopedRedactionAppliesToTheJoinedTableNotJustTheFirst() throws SQLException {
        // The fallback used to be the statement's *first* resolved table, so a rule written for
        // the joined-in table was evaluated against `customers` and quietly matched nothing.
        Redactor totals = new Redactor(List.of("orders.total_cents"));
        Map<String, Object> row = firstRow(
                "SELECT c.name, o.total_cents FROM customers c JOIN orders o ON o.customer_id = c.id", totals);

        assertThat(row).containsEntry("name", "Ada Lovelace");
        assertThat(row).containsEntry("total_cents", Redactor.MARKER);
    }

    @Test
    @DisplayName("a table-scoped rule fires through a derived table, where drivers name no table")
    void tableScopedRedactionSurvivesADerivedTable() throws SQLException {
        // A subselect is where metadata gives up: there is no base table to report for the
        // column. Every table the statement reads is then a candidate, because the alternative
        // is picking one and being wrong.
        Redactor totals = new Redactor(List.of("orders.total_cents"));
        Map<String, Object> row = firstRow(
                "SELECT c.name, o.total_cents FROM customers c"
                        + " JOIN (SELECT customer_id, total_cents FROM orders) o ON o.customer_id = c.id",
                totals);

        assertThat(row).containsEntry("total_cents", Redactor.MARKER);
        assertThat(row.values()).doesNotContain(500);
    }

    @Test
    void withNothingConfiguredNothingIsRedacted() throws SQLException {
        assertThat(firstRow("SELECT email AS x FROM customers", new Redactor(List.of())))
                .containsEntry("x", "ada@example.com");
    }

    @Test
    @DisplayName("the byte cap counts a non-String value by what it will serialise to")
    void theByteCapMeasuresNonStringValuesRatherThanAssumingTheyAreSmall() throws SQLException {
        // A driver-specific object — pgjdbc's PGobject for json/jsonb is the real case — used to
        // be charged a flat 16 bytes, so a megabyte of JSON per row sailed past a 1 MB cap. The
        // JSON column here is read back as a driver object, not a String.
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS docs (id INT PRIMARY KEY, body JSON)");
            s.execute("DELETE FROM docs");
            for (int i = 1; i <= 5; i++) {
                s.execute("INSERT INTO docs VALUES (" + i + ", '{\"pad\":\"" + "x".repeat(4000) + "\"}' FORMAT JSON)");
            }
        }

        Redactor none = new Redactor(List.of());
        SqlGuard.ReadStatement read = SqlGuard.requireReadOnly("SELECT id, body FROM docs ORDER BY id");
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT id, body FROM docs ORDER BY id")) {
            // 5 rows of ~4 KB each against a 6 KB cap: honest accounting stops early.
            ResultSetReader.Page page = ResultSetReader.read(rs, read.tables(), false, 100, 6_000L, none);

            assertThat(page.truncated()).isTrue();
            assertThat(page.truncationReason()).contains("result size limit");
            assertThat(page.rows()).hasSizeLessThan(5);
        }
    }
}
