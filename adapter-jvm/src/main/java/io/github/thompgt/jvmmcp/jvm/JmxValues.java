package io.github.thompgt.jvmmcp.jvm;

import io.github.thompgt.jvmmcp.policy.EffectiveLimits;
import io.github.thompgt.jvmmcp.policy.Redactor;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

/**
 * Turns whatever an MBean attribute returns into something that can be serialised, bounded, and
 * partly withheld.
 *
 * <p>An attribute value is not a scalar. {@code HeapMemoryUsage} is a {@link CompositeData} of
 * four longs; {@code SystemProperties} is a {@link TabularData} of several hundred rows;
 * {@code AllThreadIds} is a {@code long[]} that is short on a laptop and five thousand entries on
 * a thread-leaking application server. All three arrive through the same {@code getAttribute}
 * call, so the conversion has to be recursive and it has to be bounded, or the first genuinely
 * interesting JVM anyone points this at returns a result nothing can read.
 *
 * <p>Three properties, each of which exists because the alternative was worse:
 *
 * <ul>
 *   <li><b>Bounded by element count and by size.</b> Every collection is cut at the row cap and
 *       the whole rendering shares one character budget, so a wide value cannot spend the budget
 *       that a later, smaller attribute in the same call needed.
 *   <li><b>Truncation is reported, never silent.</b> A list cut at 100 of 5000 that says so is a
 *       partial answer; the same list without the marker is a wrong one, and a model has no way
 *       to tell them apart.
 *   <li><b>Redaction reaches inside.</b> The pattern is matched against nested keys as well as
 *       the attribute name, because the credentials on a JVM are essentially never an attribute
 *       called {@code Password} — they are a key inside {@code SystemProperties} or a word inside
 *       the {@code InputArguments} array. Redacting only the outer name would be a guardrail that
 *       protects the one case that does not happen.
 * </ul>
 *
 * <p>Unknown types fall back to {@code toString()} rather than being dropped. A model shown
 * {@code com.example.Thing@4b2ba2} learns that the attribute exists and is opaque, which is a
 * fact about the system; shown nothing, it concludes the attribute is absent and looks elsewhere.
 */
final class JmxValues {

    /** Guards against a self-referencing composite and against a model-unreadable nesting depth. */
    private static final int MAX_DEPTH = 6;

    /** Rendered string values longer than this are cut individually, before the shared budget. */
    private static final int MAX_STRING_CHARS = 4_000;

    static final String TRUNCATED = "[truncated: result size cap]";

    private final int maxElements;
    private final Redactor redactor;
    private long budget;
    private boolean truncated;

    private JmxValues(EffectiveLimits limits, Redactor redactor) {
        this.maxElements = limits.maxRows();
        this.redactor = redactor;
        this.budget = limits.maxResultBytes();
    }

    /**
     * A converted value and whether anything was left out of it.
     *
     * @param value JSON-safe: maps, lists, strings, numbers, booleans and null
     * @param truncated true when an element cap, the size budget or the depth limit removed
     *     something. The caller must surface this; see the class comment.
     */
    record Rendered(Object value, boolean truncated) {}

    /**
     * Renders one attribute value.
     *
     * @param owner the MBean's canonical name, matched as the "table" half of a redaction pattern
     * @param attribute the attribute name, matched as the "column" half
     */
    static Rendered render(
            Object raw, String owner, String attribute, EffectiveLimits limits, Redactor redactor) {
        JmxValues values = new JmxValues(limits, redactor);
        Object converted = values.redactor.isRedacted(owner, attribute)
                ? Redactor.MARKER
                : values.convert(raw, owner, 0);
        return new Rendered(converted, values.truncated);
    }

    /**
     * Renders several attributes of one MBean under a <em>shared</em> budget.
     *
     * <p>Shared rather than per attribute, because the caller's cap is on the result they receive.
     * Ten attributes each allowed the full cap is a result ten times the size of the limit the
     * operator configured, which is not a cap.
     */
    static Rendered renderAll(
            Map<String, Object> attributes, String owner, EffectiveLimits limits, Redactor redactor) {
        JmxValues values = new JmxValues(limits, redactor);
        Map<String, Object> rendered = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            rendered.put(
                    entry.getKey(),
                    values.redactor.isRedacted(owner, entry.getKey())
                            ? Redactor.MARKER
                            : values.convert(entry.getValue(), owner, 0));
        }
        return new Rendered(rendered, values.truncated);
    }

    private Object convert(Object raw, String owner, int depth) {
        if (raw == null) {
            return null;
        }
        if (budget <= 0) {
            truncated = true;
            return TRUNCATED;
        }
        if (depth > MAX_DEPTH) {
            truncated = true;
            return "[truncated: nested more than " + MAX_DEPTH + " deep]";
        }

        return switch (raw) {
            case String s -> spend(s);
            // Boxed primitives pass through as themselves so a client sees a number, not "42".
            case Integer i -> spend(i, 12);
            case Long l -> spend(l, 20);
            case Short s -> spend(s, 8);
            case Byte b -> spend(b, 6);
            case Double d -> spend(d, 24);
            case Float f -> spend(f, 16);
            case Boolean b -> spend(b, 5);
            // A Date is the one common attribute type whose toString is locale-dependent; an
            // instant is comparable across two calls and a "Tue Aug 09" is not.
            case Date date -> spend(date.toInstant().toString());
            case ObjectName objectName -> spend(objectName.getCanonicalName());
            case CompositeData composite -> convertComposite(composite, owner, depth);
            case TabularData tabular -> convertTabular(tabular, owner, depth);
            case Map<?, ?> map -> convertMap(map, owner, depth);
            case Collection<?> collection -> convertList(collection.toArray(), owner, depth);
            default -> {
                if (raw.getClass().isArray()) {
                    yield convertArray(raw, owner, depth);
                }
                // Deliberately last: an attribute of an application's own type would otherwise
                // need that type on this classpath, and toString is what is actually available.
                yield spend(String.valueOf(raw));
            }
        };
    }

    private Object convertComposite(CompositeData composite, String owner, int depth) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (String key : composite.getCompositeType().keySet()) {
            if (map.size() >= maxElements) {
                truncated = true;
                map.put(TRUNCATED, composite.getCompositeType().keySet().size() - map.size() + " more key(s)");
                break;
            }
            map.put(key, entry(owner, key, composite.get(key), depth));
        }
        return map;
    }

    /**
     * A tabular attribute rendered as a flat map when its rows are single-keyed name/value pairs,
     * and as a list of rows otherwise.
     *
     * <p>The special case is not cosmetic. {@code SystemProperties} is tabular with rows of
     * {@code {key, value}}, and rendered literally it is 400 two-element objects — the shape that
     * makes a model read every row to answer "what is the value of java.version". Flattened, it is
     * an object it can index, and the redaction below is applied to the property name rather than
     * to the literal column called "value", which is what makes it work at all.
     */
    private Object convertTabular(TabularData tabular, String owner, int depth) {
        List<String> indexNames = tabular.getTabularType().getIndexNames();
        Collection<?> rows = tabular.values();

        if (indexNames.size() == 1 && tabular.getTabularType().getRowType().keySet().size() == 2) {
            String keyColumn = indexNames.get(0);
            String valueColumn = tabular.getTabularType().getRowType().keySet().stream()
                    .filter(k -> !k.equals(keyColumn))
                    .findFirst()
                    .orElse(keyColumn);
            Map<String, Object> flat = new LinkedHashMap<>();
            for (Object row : rows) {
                if (!(row instanceof CompositeData composite)) {
                    continue;
                }
                if (flat.size() >= maxElements) {
                    truncated = true;
                    flat.put(TRUNCATED, rows.size() - flat.size() + " more entry(s)");
                    break;
                }
                String key = String.valueOf(composite.get(keyColumn));
                flat.put(key, entry(owner, key, composite.get(valueColumn), depth));
            }
            return flat;
        }
        return convertList(rows.toArray(), owner, depth);
    }

    private Object convertMap(Map<?, ?> map, String owner, int depth) {
        Map<String, Object> converted = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : map.entrySet()) {
            if (converted.size() >= maxElements) {
                truncated = true;
                converted.put(TRUNCATED, map.size() - converted.size() + " more entry(s)");
                break;
            }
            String key = String.valueOf(e.getKey());
            converted.put(key, entry(owner, key, e.getValue(), depth));
        }
        return converted;
    }

    private Object convertArray(Object array, String owner, int depth) {
        int length = Array.getLength(array);
        Object[] boxed = new Object[length];
        for (int i = 0; i < length; i++) {
            boxed[i] = Array.get(array, i);
        }
        return convertList(boxed, owner, depth);
    }

    private Object convertList(Object[] elements, String owner, int depth) {
        List<Object> list = new java.util.ArrayList<>();
        for (Object element : elements) {
            if (list.size() >= maxElements) {
                truncated = true;
                list.add(TRUNCATED + ": " + (elements.length - list.size()) + " more element(s)");
                break;
            }
            list.add(convert(element, owner, depth + 1));
        }
        return list;
    }

    /** A keyed value: redaction is decided by the key, then the value is converted normally. */
    private Object entry(String owner, String key, Object value, int depth) {
        if (redactor.isRedacted(owner, key)) {
            return Redactor.MARKER;
        }
        return convert(value, owner, depth + 1);
    }

    private Object spend(String value) {
        String text = value;
        if (text.length() > MAX_STRING_CHARS) {
            text = text.substring(0, MAX_STRING_CHARS) + "… " + TRUNCATED;
            truncated = true;
        }
        budget -= text.length();
        if (budget < 0) {
            truncated = true;
        }
        return text;
    }

    private Object spend(Object value, int approximateChars) {
        budget -= approximateChars;
        if (budget < 0) {
            truncated = true;
        }
        return value;
    }
}
