package io.github.thompgt.jvmmcp.jdbc;

import io.github.thompgt.jvmmcp.core.Arguments;
import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.Schemas;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.PolicyEngine;
import io.github.thompgt.jvmmcp.policy.Redactor;
import io.modelcontextprotocol.spec.McpSchema;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema introspection, filtered through the same allowlist as the query tool.
 *
 * <p>Filtering matters as much here as in {@code sql.query}. A schema listing that shows every
 * table in the database tells the model that forbidden tables exist and invites it to try
 * them; worse, table and column names are themselves information. What is not readable is not
 * listed.
 */
public final class SchemaTools {

    private SchemaTools() {}

    public static List<BridgeTool> create(JdbcDataSourceHandle handle, PolicyEngine policy) {
        return List.of(new ListTablesTool(handle, policy), new DescribeTableTool(handle, policy));
    }

    /** {@code schema.list_tables} — what the model is allowed to see at all. */
    static final class ListTablesTool implements BridgeTool {
        private final JdbcDataSourceHandle handle;
        private final PolicyEngine policy;

        ListTablesTool(JdbcDataSourceHandle handle, PolicyEngine policy) {
            this.handle = handle;
            this.policy = policy;
        }

        @Override
        public McpSchema.Tool descriptor() {
            return McpSchema.Tool.builder("schema.list_tables", Schemas.object().build())
                    .title("List readable tables")
                    .description(
                            "Lists the tables in the '"
                                    + handle.name()
                                    + "' database that this server will let you read, with their row"
                                    + " counts where the database reports them cheaply.\n\n"
                                    + "This is the complete set. Tables not listed here are not"
                                    + " hidden by accident — they are excluded by policy, and"
                                    + " querying them will be refused. Start here, then use"
                                    + " schema.describe_table for the columns of one table.")
                    .annotations(McpSchema.ToolAnnotations.builder()
                            .title("List readable tables")
                            .readOnlyHint(true)
                            .destructiveHint(false)
                            .openWorldHint(false)
                            .build())
                    .build();
        }

        @Override
        public ToolOutcome call(Map<String, Object> arguments) {
            // No named resource: this reads catalog metadata, and the allowlist is applied to
            // the results rather than to the request.
            return policy.guardRead("schema.list_tables", List.of(), 0, limits -> {
                List<Map<String, Object>> tables = new ArrayList<>();
                try (Connection connection = handle.connection()) {
                    DatabaseMetaData meta = connection.getMetaData();
                    try (ResultSet rs = meta.getTables(null, null, "%", new String[] {"TABLE", "VIEW"})) {
                        while (rs.next()) {
                            String name = rs.getString("TABLE_NAME");
                            if (name == null || !policy.profile().isReadable(name)) {
                                continue;
                            }
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("table", name.toLowerCase(java.util.Locale.ROOT));
                            row.put("type", rs.getString("TABLE_TYPE"));
                            row.put("schema", rs.getString("TABLE_SCHEM"));
                            String remarks = rs.getString("REMARKS");
                            if (remarks != null && !remarks.isBlank()) {
                                row.put("comment", remarks);
                            }
                            tables.add(row);
                        }
                    }
                }
                Map<String, Object> structured = Map.of("tables", tables, "count", tables.size());
                String summary = tables.isEmpty()
                        ? "No readable tables are configured for '" + handle.name() + "'."
                        : tables.size()
                                + " readable table(s): "
                                + tables.stream().map(t -> String.valueOf(t.get("table"))).sorted().toList();
                return ToolOutcome.success(structured, summary, tables.size());
            });
        }

        @Override
        public String backend() {
            return "jdbc:" + handle.name();
        }
    }

    /** {@code schema.describe_table} — columns, types, keys. */
    static final class DescribeTableTool implements BridgeTool {
        private final JdbcDataSourceHandle handle;
        private final PolicyEngine policy;
        private final Redactor redactor;

        DescribeTableTool(JdbcDataSourceHandle handle, PolicyEngine policy) {
            this.handle = handle;
            this.policy = policy;
            this.redactor = new Redactor(policy.profile().redactionPatterns());
        }

        @Override
        public McpSchema.Tool descriptor() {
            Map<String, Object> input = Schemas.object()
                    .requiredString(
                            "table",
                            "Table name, unqualified and case-insensitive. Must be one of the tables"
                                    + " returned by schema.list_tables.")
                    .build();
            return McpSchema.Tool.builder("schema.describe_table", input)
                    .title("Describe a table")
                    .description(
                            "Returns the columns of one table in '"
                                    + handle.name()
                                    + "': name, SQL type, nullability, default, and whether the column"
                                    + " is part of the primary key.\n\n"
                                    + "Columns marked redacted exist and can be selected, but their"
                                    + " values come back withheld — do not build a query that depends"
                                    + " on reading them.\n\n"
                                    + "Use this before writing a query. Column names are not reliably"
                                    + " guessable and a wrong guess is a wasted round trip.")
                    .annotations(McpSchema.ToolAnnotations.builder()
                            .title("Describe a table")
                            .readOnlyHint(true)
                            .destructiveHint(false)
                            .openWorldHint(false)
                            .build())
                    .build();
        }

        @Override
        public ToolOutcome call(Map<String, Object> arguments) {
            String requested = new Arguments(arguments).requireString("table");
            String table = SqlGuard.normalise(requested);

            // Asking about the table by name is itself a read of that table's metadata, so it
            // goes through the same allowlist — otherwise the schema tool becomes the way to
            // enumerate what the query tool refuses to show.
            return policy.guardRead("schema.describe_table", List.of(table), 0, limits -> {
                List<Map<String, Object>> columns = new ArrayList<>();
                try (Connection connection = handle.connection()) {
                    DatabaseMetaData meta = connection.getMetaData();
                    List<String> primaryKey = primaryKeyColumns(meta, table);
                    try (ResultSet rs = meta.getColumns(null, null, matchPattern(meta, table), "%")) {
                        while (rs.next()) {
                            String name = rs.getString("COLUMN_NAME");
                            Map<String, Object> column = new LinkedHashMap<>();
                            column.put("column", name);
                            column.put("type", rs.getString("TYPE_NAME"));
                            column.put("nullable", "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
                            column.put("primaryKey", primaryKey.contains(name.toLowerCase(java.util.Locale.ROOT)));
                            String def = rs.getString("COLUMN_DEF");
                            if (def != null) {
                                column.put("default", def);
                            }
                            column.put("redacted", redactor.isRedacted(table, name));
                            columns.add(column);
                        }
                    }
                }
                if (columns.isEmpty()) {
                    return ToolOutcome.failure(
                            "table '"
                                    + table
                                    + "' is allowed by policy but does not exist in the '"
                                    + handle.name()
                                    + "' database. Call schema.list_tables to see what is actually"
                                    + " there.");
                }
                Map<String, Object> structured = Map.of("table", table, "columns", columns);
                return ToolOutcome.success(structured, summarise(table, columns), columns.size());
            });
        }

        private String summarise(String table, List<Map<String, Object>> columns) {
            StringBuilder sb = new StringBuilder(table).append(" (").append(columns.size()).append(" columns)\n");
            for (Map<String, Object> c : columns) {
                sb.append("  ").append(c.get("column")).append(' ').append(c.get("type"));
                if (Boolean.TRUE.equals(c.get("primaryKey"))) {
                    sb.append(" PK");
                }
                if (Boolean.FALSE.equals(c.get("nullable"))) {
                    sb.append(" NOT NULL");
                }
                if (Boolean.TRUE.equals(c.get("redacted"))) {
                    sb.append("  [values redacted by policy]");
                }
                sb.append('\n');
            }
            return sb.toString();
        }

        private static List<String> primaryKeyColumns(DatabaseMetaData meta, String table) {
            List<String> keys = new ArrayList<>();
            try (ResultSet rs = meta.getPrimaryKeys(null, null, matchPattern(meta, table))) {
                while (rs.next()) {
                    keys.add(rs.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT));
                }
            } catch (SQLException e) {
                // Not every driver reports keys for every object type (views, in particular).
                // Missing key information degrades the answer; it should not fail the call.
                return List.of();
            }
            return keys;
        }

        /**
         * Metadata lookups match on the identifier exactly as the database stores it, so the
         * lower-cased name has to be folded the way the driver reports it — Postgres stores
         * unquoted names lower-case, Oracle and H2 upper-case.
         */
        private static String matchPattern(DatabaseMetaData meta, String table) throws SQLException {
            return meta.storesUpperCaseIdentifiers()
                    ? table.toUpperCase(java.util.Locale.ROOT)
                    : table.toLowerCase(java.util.Locale.ROOT);
        }

        @Override
        public String backend() {
            return "jdbc:" + handle.name();
        }
    }
}
