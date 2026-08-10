package io.github.thompgt.jvmmcp.jdbc;

import io.github.thompgt.jvmmcp.policy.Redactor;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a {@link ResultSet} into rows that are safe to hand a model, under a row and byte cap.
 *
 * <p>Two caps, because they fail differently. A row cap stops a query with many small rows;
 * a byte cap stops one row carrying a megabyte of JSON in a TEXT column. Only having the
 * first is how a "limit 100" server still fills a context window.
 */
final class ResultSetReader {

    /**
     * @param columns column names in order
     * @param rows row values keyed by column name
     * @param truncated whether reading stopped at a cap rather than at the end of the results
     * @param truncationReason which cap stopped it, or empty
     */
    record Page(List<String> columns, List<Map<String, Object>> rows, boolean truncated, String truncationReason) {}

    private ResultSetReader() {}

    /**
     * @param tables every table the statement reads, as {@link SqlGuard} resolved them. Used
     *     when the driver reports no table for a column: the answer then is <em>all</em> of
     *     them, not the first of them.
     * @param redactUnnamedColumns what to do with a column the driver gives no underlying name
     *     for — a computed expression. True means redact it. {@code SqlQueryTool} sets this when
     *     the statement computes over a column that policy withholds, because at that point the
     *     value on the wire is derived from a secret and the label is whatever the caller chose.
     */
    static Page read(
            ResultSet rs,
            List<String> tables,
            boolean redactUnnamedColumns,
            int maxRows,
            long maxBytes,
            Redactor redactor)
            throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        List<String> columns = new ArrayList<>(columnCount);
        boolean[] redactOf = new boolean[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            String label = meta.getColumnLabel(i);
            columns.add(label);
            // The table the driver reports for the column, when it reports one. Several drivers
            // leave it blank for computed columns and for joins, and the fallback used to be
            // the statement's *first* table — so on `customers JOIN accounts` a rule written
            // `accounts.secret` was evaluated against `customers` and matched nothing. When the
            // driver will not say which table a column came from, every table the statement
            // touches is a candidate. Redaction has to err towards redacting, not towards
            // leaking.
            String driverTable = meta.getTableName(i);
            boolean fromNamedTable = driverTable != null && !driverTable.isBlank();
            // Only believe the driver's table when it is a table this statement actually reads.
            // Through a derived table H2 reports the subquery's *alias* — `o`, not `orders` —
            // and an alias matches no rule an operator could have written, so trusting it would
            // silently switch redaction off for exactly the queries that hide their source.
            List<String> candidates = fromNamedTable && isResolvedTable(tables, driverTable)
                    ? List.of(driverTable)
                    : tables;
            redactOf[i - 1] = shouldRedact(
                    redactor, candidates, fromNamedTable, meta.getColumnName(i), label, redactUnnamedColumns);
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        long bytes = 0;
        boolean truncated = false;
        String reason = "";

        while (rs.next()) {
            if (rows.size() >= maxRows) {
                truncated = true;
                reason = "row limit of " + maxRows + " reached";
                break;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            long rowBytes = 0;
            for (int i = 1; i <= columnCount; i++) {
                String column = columns.get(i - 1);
                Object value = redactOf[i - 1] ? Redactor.MARKER : readValue(rs, meta, i);
                row.put(column, value);
                rowBytes += estimateBytes(value);
            }
            if (bytes + rowBytes > maxBytes && !rows.isEmpty()) {
                // Stop before adding the row that would breach the cap, so the result is
                // always under it. The first row is admitted regardless — returning zero rows
                // and "too big" tells the model nothing about the shape of the data.
                truncated = true;
                reason = "result size limit of " + maxBytes + " bytes reached";
                break;
            }
            bytes += rowBytes;
            rows.add(row);
        }

        return new Page(List.copyOf(columns), List.copyOf(rows), truncated, reason);
    }

    /**
     * Decides redaction for one result column, before any row is read.
     *
     * <p>The label is the alias, not the column: for {@code SELECT email AS x} a driver reports
     * label {@code x} and name {@code email}. Deciding on the label is a one-word bypass of
     * every redaction rule, so the underlying name decides, and the label is only consulted as
     * an additional reason to redact — never as a reason not to.
     *
     * <p>A column only counts as a real table column when the driver names <em>both</em> the
     * table and the column for it. Anything else is a computed item, and drivers disagree about
     * what to report for one — H2 echoes the alias back as the column name, so "the name is
     * missing" is not a test that can be relied on. Computed items fail closed on
     * {@code redactUnnamedColumns}, which the caller derives from the parse tree.
     */
    private static boolean shouldRedact(
            Redactor redactor,
            List<String> candidateTables,
            boolean fromNamedTable,
            String columnName,
            String label,
            boolean redactUnnamedColumns) {
        if (redactor.isEmpty()) {
            return false;
        }
        boolean named = columnName != null && !columnName.isBlank();
        if (named && matchesAnyTable(redactor, candidateTables, columnName)) {
            return true;
        }
        if (redactUnnamedColumns && !(named && fromNamedTable)) {
            return true;
        }
        return matchesAnyTable(redactor, candidateTables, label);
    }

    /** Whether the driver's table name is one of the names {@link SqlGuard} resolved. */
    private static boolean isResolvedTable(List<String> tables, String driverTable) {
        String name = driverTable.toLowerCase(java.util.Locale.ROOT);
        for (String table : tables) {
            // Qualified either side: `orders` answers for `public.orders` and the reverse.
            if (table.equals(name) || table.endsWith("." + name) || name.endsWith("." + table)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesAnyTable(Redactor redactor, List<String> candidateTables, String column) {
        if (candidateTables.isEmpty()) {
            // No table resolved at all — `SELECT now()`. A rule written for any table
            // (`*.email`) still has to be able to fire, so ask with an empty table name.
            return redactor.isRedacted("", column);
        }
        for (String table : candidateTables) {
            if (redactor.isRedacted(table, column)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads one column as a JSON-friendly value.
     *
     * <p>Temporal and binary types are converted deliberately rather than left to
     * {@code getObject}: a driver-specific {@code Timestamp} subclass serialises unpredictably,
     * and a byte array would otherwise arrive at the model as a meaningless array of numbers.
     */
    private static Object readValue(ResultSet rs, ResultSetMetaData meta, int index) throws SQLException {
        int type = meta.getColumnType(index);
        Object value =
                switch (type) {
                    case Types.DATE -> {
                        java.sql.Date d = rs.getDate(index);
                        yield d == null ? null : d.toLocalDate().toString();
                    }
                    case Types.TIME, Types.TIME_WITH_TIMEZONE -> {
                        java.sql.Time t = rs.getTime(index);
                        yield t == null ? null : t.toLocalTime().toString();
                    }
                    case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
                        java.sql.Timestamp ts = rs.getTimestamp(index);
                        yield ts == null ? null : ts.toInstant().toString();
                    }
                    case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> {
                        byte[] raw = rs.getBytes(index);
                        yield raw == null ? null : "<" + raw.length + " bytes of binary data>";
                    }
                    case Types.DECIMAL, Types.NUMERIC -> {
                        java.math.BigDecimal bd = rs.getBigDecimal(index);
                        // toString, not doubleValue: a monetary amount must not acquire a
                        // floating-point rounding error on its way to the model.
                        yield bd == null ? null : bd.toString();
                    }
                    default -> rs.getObject(index);
                };
        return rs.wasNull() ? null : value;
    }

    /**
     * Cheap size estimate — exact serialised length is not worth a second pass.
     *
     * <p>Charging a flat 16 bytes for everything that is not a {@code String} is what made the
     * byte cap ignorable: a pgjdbc {@code PGobject} holding a megabyte of jsonb is not a String,
     * and neither is a {@code java.sql.Array}. A 1 MB cap was reached after 65,536 rows of it.
     * Anything not obviously fixed-width is measured through its own {@code toString()}, which
     * is what the JSON encoder will render anyway.
     */
    private static long estimateBytes(Object value) {
        if (value == null) {
            return 4;
        }
        if (value instanceof String s) {
            return s.length() + 2L;
        }
        if (value instanceof byte[] b) {
            // Not every binary-ish type is declared BINARY — H2 hands back a byte[] for JSON —
            // and `String.valueOf` on an array measures the identity hash, not the payload.
            return b.length + 2L;
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof Character) {
            // Fixed-width and small however they serialise; not worth the toString.
            return 16;
        }
        return String.valueOf(value).length() + 2L;
    }
}
