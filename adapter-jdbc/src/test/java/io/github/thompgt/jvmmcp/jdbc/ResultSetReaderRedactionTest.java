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
        String primaryTable = read.tables().isEmpty() ? "" : read.tables().get(0);
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            ResultSetReader.Page page = ResultSetReader.read(rs, primaryTable, unnamed, 10, 1_000_000L, redactor);
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
    void withNothingConfiguredNothingIsRedacted() throws SQLException {
        assertThat(firstRow("SELECT email AS x FROM customers", new Redactor(List.of())))
                .containsEntry("x", "ada@example.com");
    }
}
