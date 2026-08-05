package io.github.thompgt.jvmmcp.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.AccessMode;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.github.thompgt.jvmmcp.policy.Redactor;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The guardrails against a real database.
 *
 * <p>{@link SqlGuardTest} proves the parser rejects what it should. This proves the whole
 * stack does — that a statement the parser accepts is still stopped by the allowlist, that the
 * caps are actually applied to a real result set, and that a denied call leaves no trace in
 * the database. Unit tests of a security boundary are necessary and not sufficient.
 */
@Tag("integration")
@Testcontainers
class JdbcGuardrailIntegrationTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("orders")
            .withUsername("bridge")
            .withPassword("bridge")
            .withInitScript("seed.sql");

    private static JdbcDataSourceHandle handle;
    private AuditSink.RecordingAuditSink audit;
    private JdbcAdapter adapter;

    @BeforeAll
    static void openPool() {
        handle = new JdbcDataSourceHandle(
                "orders-db",
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword(),
                Duration.ofSeconds(5));
    }

    @AfterAll
    static void closePool() {
        handle.close();
    }

    @BeforeEach
    void freshAdapter() {
        audit = AuditSink.recording();
        adapter = new JdbcAdapter(handle, profile(AccessMode.READ_ONLY), audit);
    }

    private static PolicyProfile profile(AccessMode mode) {
        return PolicyProfile.builder("orders-db")
                .mode(mode)
                // internal_audit is deliberately absent.
                .allowRead("customers", "orders", "order_items")
                .maxRows(3)
                .maxResultBytes(1_048_576L)
                .timeout(Duration.ofSeconds(5))
                .redact("*.email")
                .build();
    }

    private BridgeTool tool(String name) {
        return adapter.tools().stream()
                .filter(t -> t.descriptor().name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no tool named " + name));
    }

    @Test
    void runsAPermittedQueryAndReturnsRows() {
        // 4 customers, cap is 3.
        ToolOutcome outcome = tool("sql.query").call(Map.of("sql", "SELECT id, name FROM customers ORDER BY id"));

        assertThat(outcome.error()).isFalse();
        @SuppressWarnings("unchecked")
        Map<String, Object> structured = (Map<String, Object>) outcome.structured();
        assertThat((List<?>) structured.get("rows")).hasSize(3);
        assertThat(structured).containsEntry("truncated", true);
        assertThat(outcome.summary()).contains("TRUNCATED").contains("incomplete");
    }

    @Test
    @DisplayName("a result that exactly fills the cap is not reported as truncated")
    void resultAtExactlyTheCapIsNotFlaggedTruncated() {
        // The other side of the off-by-one. Claiming truncation here would push the model to
        // narrow a query that already returned everything.
        ToolOutcome outcome =
                tool("sql.query").call(Map.of("sql", "SELECT id FROM customers ORDER BY id LIMIT 3"));

        @SuppressWarnings("unchecked")
        Map<String, Object> structured = (Map<String, Object>) outcome.structured();
        assertThat((List<?>) structured.get("rows")).hasSize(3);
        assertThat(structured).containsEntry("truncated", false);
        assertThat(outcome.summary()).doesNotContain("TRUNCATED");
    }

    @Test
    @DisplayName("a join onto a non-allowlisted table is denied even though the SQL is valid")
    void deniesJoinOntoForbiddenTable() {
        // The statement parses, is a SELECT, and would run happily. Only resolved-name
        // allowlisting stops it — this is the case a raw-string check misses.
        ToolOutcome outcome = tool("sql.query")
                .call(Map.of(
                        "sql",
                        "SELECT c.name, a.secret FROM customers c"
                                + " JOIN internal_audit a ON a.actor = c.name"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("internal_audit").contains("customers");
        assertThat(outcome.summary()).doesNotContain("this-row-must-never-reach-a-model");
        assertThat(audit.denials()).hasSize(1);
        assertThat(audit.denials().get(0).rule()).isEqualTo("allow-read");
    }

    @Test
    void deniesForbiddenTableReachedThroughASubquery() {
        ToolOutcome outcome = tool("sql.query")
                .call(Map.of("sql", "SELECT * FROM customers WHERE name IN (SELECT actor FROM internal_audit)"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("internal_audit");
    }

    @Test
    void redactedColumnsComeBackWithheldButTheRowStillArrives() {
        ToolOutcome outcome =
                tool("sql.query").call(Map.of("sql", "SELECT name, email FROM customers ORDER BY id", "max_rows", 1));

        @SuppressWarnings("unchecked")
        Map<String, Object> structured = (Map<String, Object>) outcome.structured();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) structured.get("rows");

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)).containsEntry("name", "Ada Lovelace");
        assertThat(rows.get(0)).containsEntry("email", Redactor.MARKER);
    }

    @Test
    void writesAreRefusedAndTheDatabaseIsUnchanged() {
        ToolOutcome outcome = tool("sql.query").call(Map.of("sql", "DELETE FROM orders"));
        assertThat(outcome.error()).isTrue();

        ToolOutcome after = tool("sql.query").call(Map.of("sql", "SELECT count(*) AS n FROM orders"));
        @SuppressWarnings("unchecked")
        Map<String, Object> structured = (Map<String, Object>) after.structured();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) structured.get("rows");
        assertThat(String.valueOf(rows.get(0).get("n"))).isEqualTo("5");
    }

    @Test
    @DisplayName("stacked statements do not execute their second half")
    void stackedStatementDoesNotDropTheTable() {
        ToolOutcome outcome =
                tool("sql.query").call(Map.of("sql", "SELECT 1 FROM customers; DROP TABLE order_items"));
        assertThat(outcome.error()).isTrue();

        // The proof is not the refusal — it is that the table is still there afterwards.
        ToolOutcome after = tool("sql.query").call(Map.of("sql", "SELECT count(*) AS n FROM order_items"));
        assertThat(after.error()).isFalse();
    }

    @Test
    void schemaListingShowsOnlyAllowlistedTables() {
        ToolOutcome outcome = tool("schema.list_tables").call(Map.of());

        assertThat(outcome.error()).isFalse();
        // A listing that reveals internal_audit exists would invite the model to try it, and
        // the table name is itself information.
        assertThat(outcome.summary())
                .contains("customers", "orders", "order_items")
                .doesNotContain("internal_audit");
    }

    @Test
    void describingAForbiddenTableIsDeniedNotAnsweredFromMetadata() {
        ToolOutcome outcome = tool("schema.describe_table").call(Map.of("table", "internal_audit"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("not in the allowlist");
        assertThat(outcome.summary()).doesNotContain("secret");
    }

    @Test
    void describeTableMarksRedactedColumns() {
        ToolOutcome outcome = tool("schema.describe_table").call(Map.of("table", "customers"));

        assertThat(outcome.error()).isFalse();
        assertThat(outcome.summary()).contains("email").contains("values redacted by policy");
        assertThat(outcome.summary()).contains("id").contains("PK");
    }

    @Test
    void explainReturnsAPlanWithoutRunningTheQuery() {
        ToolOutcome outcome = tool("sql.explain").call(Map.of("sql", "SELECT * FROM orders WHERE status = 'shipped'"));

        assertThat(outcome.error()).isFalse();
        assertThat(outcome.summary()).containsIgnoringCase("scan");
    }

    @Test
    void dryRunModeExecutesNothingAgainstARealDatabase() {
        JdbcAdapter dryRun = new JdbcAdapter(handle, profile(AccessMode.DRY_RUN), audit);
        BridgeTool query = dryRun.tools().stream()
                .filter(t -> t.descriptor().name().equals("sql.query"))
                .findFirst()
                .orElseThrow();

        ToolOutcome outcome = query.call(Map.of("sql", "SELECT * FROM customers"));

        assertThat(outcome.error()).isFalse();
        assertThat(outcome.summary()).contains("dry-run").contains("Nothing was executed");
    }

    @Test
    void everyCallIsAuditedWithTheResolvedTablesNotTheRawSql() {
        tool("sql.query").call(Map.of("sql", "SELECT c.id FROM customers c JOIN orders o ON o.customer_id = c.id"));

        assertThat(audit.records()).hasSize(1);
        assertThat(audit.records().get(0).resources()).containsExactlyInAnyOrder("customers", "orders");
        assertThat(audit.records().get(0).allowed()).isTrue();
        assertThat(audit.records().get(0).toJsonLine()).contains("\"backend\":\"orders-db\"");
    }
}
