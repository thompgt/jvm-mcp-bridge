package io.github.thompgt.jvmmcp.policy;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Replaces values in configured columns before they leave the process.
 *
 * <p>Patterns are written as {@code table.column}, where either side may be {@code *}, and a
 * bare {@code *} in a segment may also appear as a prefix or suffix — {@code *_token} matches
 * any column whose name ends in {@code _token}.
 *
 * <p>Redaction is not a substitute for a column allowlist; it is a safety net for the case a
 * permitted table happens to carry a sensitive column. The redacted value is replaced with a
 * marker rather than dropped, so the model can see that a column exists and is withheld
 * instead of concluding the data is absent and querying around it.
 */
public final class Redactor {

    /** What a redacted value is replaced with. Deliberately obvious to a reader and a model. */
    public static final String MARKER = "[redacted by policy]";

    private final List<Pattern> tablePatterns;
    private final List<Pattern> columnPatterns;
    private final boolean empty;

    public Redactor(List<String> patterns) {
        this.empty = patterns.isEmpty();
        this.tablePatterns = new java.util.ArrayList<>(patterns.size());
        this.columnPatterns = new java.util.ArrayList<>(patterns.size());
        for (String raw : patterns) {
            String pattern = raw.trim().toLowerCase(Locale.ROOT);
            int dot = pattern.lastIndexOf('.');
            // A pattern with no dot applies to any table: `*_token` means the column, anywhere.
            String table = dot < 0 ? "*" : pattern.substring(0, dot);
            String column = dot < 0 ? pattern : pattern.substring(dot + 1);
            tablePatterns.add(globToRegex(table));
            columnPatterns.add(globToRegex(column));
        }
    }

    /** True when nothing is configured, so a caller can skip explaining redaction that cannot happen. */
    public boolean isEmpty() {
        return empty;
    }

    public boolean isRedacted(String table, String column) {
        if (empty) {
            return false;
        }
        String t = table == null ? "" : table.toLowerCase(Locale.ROOT);
        String c = column == null ? "" : column.toLowerCase(Locale.ROOT);
        for (int i = 0; i < columnPatterns.size(); i++) {
            if (columnPatterns.get(i).matcher(c).matches() && tablePatterns.get(i).matcher(t).matches()) {
                return true;
            }
        }
        return false;
    }

    /** Returns the marker when the column is redacted, otherwise the value unchanged. */
    public Object apply(String table, String column, Object value) {
        return isRedacted(table, column) ? MARKER : value;
    }

    private static Pattern globToRegex(String glob) {
        StringBuilder regex = new StringBuilder();
        int from = 0;
        int star;
        while ((star = glob.indexOf('*', from)) >= 0) {
            if (star > from) {
                regex.append(Pattern.quote(glob.substring(from, star)));
            }
            regex.append(".*");
            from = star + 1;
        }
        if (from < glob.length()) {
            regex.append(Pattern.quote(glob.substring(from)));
        }
        return Pattern.compile(regex.toString());
    }
}
