package io.github.thompgt.jvmmcp.jdbc;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.ParenthesedStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 * Decides whether a statement is a read, and which tables it actually touches.
 *
 * <p>Everything here works on the parse tree. String inspection — regexes for {@code DROP},
 * prefix checks for {@code select} — is what the reference PostgreSQL MCP server did, and it
 * is defeated by a comment, an unusual whitespace run, or a second statement after a
 * semicolon. A parse tree has none of those ambiguities: a statement either is a {@code
 * SELECT} node or it is not.
 *
 * <p>This class does not consult the allowlist. It resolves names; {@code PolicyEngine}
 * decides. Keeping the split means the parsing can be backend-specific while the decision
 * stays uniform across every adapter.
 */
public final class SqlGuard {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SqlGuard.class);

    private SqlGuard() {}

    /** A statement that is not a permitted read. The message is written for the model. */
    public static final class RejectedStatementException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final String rule;

        RejectedStatementException(String rule, String message) {
            super(message);
            this.rule = rule;
        }

        /** Which check refused, for the audit record. */
        public String rule() {
            return rule;
        }
    }

    /**
     * The result of validating a statement.
     *
     * @param tables lower-cased table names the statement reads, with CTE aliases removed —
     *     these are what the policy engine is asked about
     * @param expressionIdentifiers lower-cased identifiers appearing inside select items that
     *     are <em>not</em> a bare column reference — {@code lower(email)} contributes
     *     {@code lower} and {@code email}. A driver reports no column name for such an item, so
     *     redaction cannot be decided from result-set metadata alone; these are what it falls
     *     back to. Deliberately over-inclusive: over-redacting a computed column is a nuisance,
     *     under-redacting one is the leak.
     */
    public record ReadStatement(Statement parsed, List<String> tables, List<String> expressionIdentifiers) {}

    /**
     * Parses and validates {@code sql}, returning the tables it reads.
     *
     * @throws RejectedStatementException if it is not exactly one read-only statement
     */
    public static ReadStatement requireReadOnly(String sql) {
        try {
            return validate(sql);
        } catch (RejectedStatementException e) {
            throw e;
        } catch (RuntimeException e) {
            // The parser is third-party code walking attacker-influenced input, and it does
            // throw things other than JSQLParserException — getSelect() on a writing CTE
            // raised ClassCastException, which would have escaped this class entirely and
            // skipped every check below it. Anything unexpected here fails closed.
            LOG.warn("SQL analysis failed unexpectedly; refusing the statement", e);
            throw new RejectedStatementException(
                    "parse",
                    "the SQL could not be analysed, so it will not be run. Send a single,"
                            + " straightforward SELECT statement.");
        }
    }

    private static ReadStatement validate(String sql) {
        Statement statement = parseExactlyOne(sql);

        if (!(statement instanceof Select select)) {
            throw new RejectedStatementException(
                    "read-only",
                    "only SELECT and WITH statements are permitted; this is a "
                            + statementKind(statement)
                            + " statement. This server is read-only and no argument or"
                            + " phrasing will change that.");
        }

        rejectWritingCommonTableExpressions(select);
        rejectSelectIntoTargets(select);

        Set<String> tables = new LinkedHashSet<>();
        for (String table : new TablesNamesFinder<>().getTables(statement)) {
            tables.add(normalise(table));
        }
        // A CTE name looks like a table to the finder in some shapes. Removing the names
        // declared by this statement's own WITH clause avoids denying a legitimate query
        // because its CTE happens not to be in the allowlist.
        cteNames(select).forEach(tables::remove);

        return new ReadStatement(statement, List.copyOf(tables), List.copyOf(expressionIdentifiers(select)));
    }

    /** Identifier-shaped tokens, for pulling column names out of an expression's text. */
    private static final java.util.regex.Pattern IDENTIFIER =
            java.util.regex.Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");

    /**
     * Collects the identifiers used inside computed select items.
     *
     * <p>{@code SELECT lower(email) AS x FROM customers} defeats metadata-based redaction
     * completely: the driver reports no underlying column for a computed item, and the label is
     * whatever the caller chose to call it. Recovering the identifiers from the parse tree is
     * the only place the word {@code email} still exists.
     *
     * <p>Read off {@code toString()} rather than through a typed visitor on purpose — a visitor
     * that misses an expression node fails <em>open</em>, and new node types arrive with every
     * parser release. Text tokens cannot miss one.
     */
    private static Set<String> expressionIdentifiers(Select select) {
        Set<String> identifiers = new LinkedHashSet<>();
        collectExpressionIdentifiers(select, identifiers);
        return identifiers;
    }

    private static void collectExpressionIdentifiers(Select select, Set<String> into) {
        if (select instanceof PlainSelect plain) {
            List<net.sf.jsqlparser.statement.select.SelectItem<?>> items = plain.getSelectItems();
            if (items == null) {
                return;
            }
            for (net.sf.jsqlparser.statement.select.SelectItem<?> item : items) {
                net.sf.jsqlparser.expression.Expression expression = item.getExpression();
                if (expression == null || expression instanceof net.sf.jsqlparser.schema.Column) {
                    // A bare column: the driver will report its real name, which is a better
                    // answer than anything guessed from the text.
                    continue;
                }
                java.util.regex.Matcher m = IDENTIFIER.matcher(expression.toString());
                while (m.find()) {
                    into.add(m.group().toLowerCase(Locale.ROOT));
                }
            }
        } else if (select instanceof ParenthesedSelect parenthesed) {
            collectExpressionIdentifiers(parenthesed.getSelect(), into);
        } else if (select instanceof SetOperationList setOperations) {
            setOperations.getSelects().forEach(s -> collectExpressionIdentifiers(s, into));
        }
    }

    private static Statement parseExactlyOne(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new RejectedStatementException("parse", "no SQL was supplied");
        }

        Statements statements;
        try {
            // parseStatements, not parse: `SELECT 1; DROP TABLE customers` parses cleanly as
            // *two* statements. Parsing only the first would validate the SELECT and hand the
            // whole string to the driver, which is precisely the stacked-statement bypass.
            statements = CCJSqlParserUtil.parseStatements(sql);
        } catch (JSQLParserException e) {
            throw new RejectedStatementException(
                    "parse",
                    "the SQL could not be parsed, so it cannot be checked and will not be run."
                            + " Send a single, complete SELECT statement.");
        }

        // Statements extends ArrayList<Statement>; getStatements() is deprecated in 5.x.
        if (statements.size() != 1) {
            throw new RejectedStatementException(
                    "single-statement",
                    "send exactly one statement; this contained "
                            + statements.size()
                            + ". Multiple statements separated by ';' are refused outright,"
                            + " because only the first would be checked.");
        }
        return statements.get(0);
    }

    /**
     * Rejects a data-modifying CTE.
     *
     * <p>{@code WITH gone AS (DELETE FROM orders RETURNING *) SELECT * FROM gone} is a
     * {@code Select} at the top level and deletes rows. The top-level node type is not
     * sufficient; the {@code WITH} items have to be checked individually.
     */
    private static void rejectWritingCommonTableExpressions(Select select) {
        List<WithItem<?>> withItems = select.getWithItemsList();
        if (withItems == null) {
            return;
        }
        for (WithItem<?> item : withItems) {
            // getParenthesedStatement(), not getSelect(): the latter casts to
            // ParenthesedSelect unconditionally and throws ClassCastException on exactly the
            // writing CTE this method exists to catch — the check would never have run.
            ParenthesedStatement body = item.getParenthesedStatement();
            if (!(body instanceof ParenthesedSelect)) {
                throw new RejectedStatementException(
                        "read-only",
                        "the WITH clause '"
                                + item.getUnquotedAliasName()
                                + "' contains a data-modifying statement. A CTE that writes is"
                                + " still a write, and is refused.");
            }
        }
    }

    /**
     * Rejects {@code SELECT ... INTO newtable}, which creates a table despite being a select.
     *
     * <p>Checked recursively because the offending select can be nested inside a set operation
     * or parentheses.
     */
    private static void rejectSelectIntoTargets(Select select) {
        if (select instanceof PlainSelect plain) {
            if (plain.getIntoTables() != null && !plain.getIntoTables().isEmpty()) {
                throw new RejectedStatementException(
                        "read-only",
                        "SELECT ... INTO creates a table, so it is a write and is refused."
                                + " Remove the INTO clause to run the query as a read.");
            }
        } else if (select instanceof ParenthesedSelect parenthesed) {
            rejectSelectIntoTargets(parenthesed.getSelect());
        } else if (select instanceof SetOperationList setOperations) {
            setOperations.getSelects().forEach(SqlGuard::rejectSelectIntoTargets);
        }
    }

    private static Set<String> cteNames(Select select) {
        Set<String> names = new LinkedHashSet<>();
        List<WithItem<?>> withItems = select.getWithItemsList();
        if (withItems != null) {
            for (WithItem<?> item : withItems) {
                if (item.getAlias() != null) {
                    names.add(normalise(item.getAlias().getName()));
                }
            }
        }
        return names;
    }

    /** Human-readable statement kind for the refusal message. */
    private static String statementKind(Statement statement) {
        String simple = statement.getClass().getSimpleName();
        return simple.toUpperCase(Locale.ROOT);
    }

    /**
     * Folds a possibly qualified, possibly quoted identifier to a bare lower-case name.
     *
     * <p>{@code "Public"."Orders"} and {@code public.orders} and {@code ORDERS} must all
     * resolve to {@code orders}, or the allowlist can be sidestepped by quoting.
     */
    static String normalise(String rawName) {
        String name = rawName.trim();
        int lastDot = name.lastIndexOf('.');
        if (lastDot >= 0) {
            name = name.substring(lastDot + 1);
        }
        name = name.replace("\"", "").replace("`", "").replace("[", "").replace("]", "");
        return name.toLowerCase(Locale.ROOT);
    }
}
