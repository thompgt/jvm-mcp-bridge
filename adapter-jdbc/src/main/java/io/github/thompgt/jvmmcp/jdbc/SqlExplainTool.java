package io.github.thompgt.jvmmcp.jdbc;

import io.github.thompgt.jvmmcp.core.Arguments;
import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.Schemas;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.PolicyEngine;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.List;
import java.util.Map;

/**
 * Returns the query plan without running the query.
 *
 * <p>Exists so an expensive query can be recognised before it is run rather than after. The
 * plan is also the honest way for a model to check whether its query will use an index, which
 * is otherwise invisible to it.
 */
public final class SqlExplainTool implements BridgeTool {

    private final JdbcDataSourceHandle handle;
    private final PolicyEngine policy;

    public SqlExplainTool(JdbcDataSourceHandle handle, PolicyEngine policy) {
        this.handle = handle;
        this.policy = policy;
    }

    @Override
    public McpSchema.Tool descriptor() {
        Map<String, Object> input = Schemas.object()
                .requiredString("sql", "The SELECT statement to plan. Validated exactly as sql.query validates it.")
                .build();
        return McpSchema.Tool.builder("sql.explain", input)
                .title("Explain a query plan")
                .description(
                        "Returns the database's execution plan for a SELECT against '"
                                + handle.name()
                                + "' without running it.\n\n"
                                + "Use this when a query might be expensive — a plan showing a"
                                + " sequential scan over a large table is a reason to add a WHERE"
                                + " clause before calling sql.query, not after it times out.\n\n"
                                + "The statement is validated identically to sql.query: read-only,"
                                + " allowlisted tables. The plan is estimated, not measured; the"
                                + " query is never executed.")
                .annotations(McpSchema.ToolAnnotations.builder()
                        .title("Explain a query plan")
                        .readOnlyHint(true)
                        .destructiveHint(false)
                        .idempotentHint(true)
                        .openWorldHint(false)
                        .build())
                .build();
    }

    @Override
    public ToolOutcome call(Map<String, Object> arguments) {
        String sql = new Arguments(arguments).requireString("sql");

        SqlGuard.ReadStatement read;
        try {
            read = SqlGuard.requireReadOnly(sql);
        } catch (SqlGuard.RejectedStatementException e) {
            return ToolOutcome.denied(e.getMessage(), Map.of("rule", e.rule(), "tool", "sql.explain"));
        }

        if (handle.dialect().explainPrefix() == null) {
            return ToolOutcome.failure(
                    "this database ("
                            + handle.dialect().name().toLowerCase(java.util.Locale.ROOT)
                            + ") has no portable EXPLAIN through this server. Judge cost from the"
                            + " schema and row counts instead; sql.query is still available.");
        }

        return policy.guardRead("sql.explain", read.tables(), 0, limits -> {
            List<String> plan = JdbcQueryRunner.explain(handle, sql, limits);
            Map<String, Object> structured = Map.of("plan", plan, "tables", read.tables());
            return ToolOutcome.success(
                    structured,
                    plan.isEmpty() ? "The database returned no plan." : String.join("\n", plan),
                    plan.size());
        });
    }

    @Override
    public String backend() {
        return "jdbc:" + handle.name();
    }
}
