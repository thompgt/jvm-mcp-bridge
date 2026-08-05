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

    static Page read(ResultSet rs, String primaryTable, int maxRows, long maxBytes, Redactor redactor)
            throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        List<String> columns = new ArrayList<>(columnCount);
        String[] tableOf = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            columns.add(meta.getColumnLabel(i));
            // Prefer the table the driver reports for the column; several drivers leave it
            // blank for computed columns and joins, so fall back to the statement's primary
            // table. Redaction has to err towards redacting, not towards leaking.
            String table = meta.getTableName(i);
            tableOf[i - 1] = (table == null || table.isBlank()) ? primaryTable : table;
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
                Object value = redactor.apply(tableOf[i - 1], column, readValue(rs, meta, i));
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

    /** Cheap size estimate — exact serialised length is not worth a second pass. */
    private static long estimateBytes(Object value) {
        if (value == null) {
            return 4;
        }
        if (value instanceof String s) {
            return s.length() + 2L;
        }
        return 16;
    }
}
