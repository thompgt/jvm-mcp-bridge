package io.github.thompgt.jvmmcp.core;

import java.util.Map;

/**
 * Typed reads over the raw argument map an MCP client sends.
 *
 * <p>Arguments arrive as {@code Map<String, Object>} after JSON decoding, so a number may be
 * an {@code Integer}, a {@code Long} or a {@code Double} depending on how the client encoded
 * it. Casting directly works until the day a client sends {@code 100.0}; these accessors
 * normalise instead, and throw {@link BadArgumentException} with a message the model can act
 * on rather than a {@code ClassCastException} it cannot.
 */
public final class Arguments {

    private final Map<String, Object> raw;

    public Arguments(Map<String, Object> raw) {
        this.raw = raw == null ? Map.of() : raw;
    }

    /** Thrown when an argument is missing or the wrong shape. Carries a model-readable message. */
    public static final class BadArgumentException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public BadArgumentException(String message) {
            super(message);
        }
    }

    public String requireString(String name) {
        Object value = raw.get(name);
        if (value == null) {
            throw new BadArgumentException("missing required argument: " + name);
        }
        if (!(value instanceof String s) || s.isBlank()) {
            throw new BadArgumentException("argument " + name + " must be a non-empty string");
        }
        return s;
    }

    public String optionalString(String name, String fallback) {
        Object value = raw.get(name);
        if (value == null) {
            return fallback;
        }
        if (!(value instanceof String s)) {
            throw new BadArgumentException("argument " + name + " must be a string");
        }
        return s;
    }

    /**
     * Reads an integer, accepting any JSON number that has no fractional part. A client that
     * sends {@code 100.0} for a row limit means 100, and refusing it would be pedantry the
     * model has to work around.
     */
    public int optionalInt(String name, int fallback) {
        Object value = raw.get(name);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number n) {
            double d = n.doubleValue();
            if (d != Math.floor(d) || Double.isInfinite(d)) {
                throw new BadArgumentException("argument " + name + " must be a whole number");
            }
            return (int) d;
        }
        throw new BadArgumentException("argument " + name + " must be a number");
    }

    /**
     * As {@link #optionalInt}, for values that genuinely outgrow an {@code int} — a Kafka
     * offset is the case this exists for. Reading one as an {@code int} works on every test
     * fixture and truncates on the one busy topic anybody asks about.
     */
    public long optionalLong(String name, long fallback) {
        Object value = raw.get(name);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number n) {
            if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
                return n.longValue();
            }
            double d = n.doubleValue();
            // A double carries offsets exactly only to 2^53; beyond that "whole number" is a
            // property of the decoded double, not of what the client wrote.
            if (d != Math.floor(d) || Double.isInfinite(d) || Math.abs(d) > (double) (1L << 53)) {
                throw new BadArgumentException(
                        "argument " + name + " must be a whole number that survives JSON encoding");
            }
            return (long) d;
        }
        throw new BadArgumentException("argument " + name + " must be a number");
    }

    /**
     * Reads an array of strings, rejecting a bare string in its place.
     *
     * <p>Accepting {@code "a"} as {@code ["a"]} would be a kindness that costs more than it
     * saves: the model that sent it learns the wrong shape and sends it again on a tool where
     * the two mean different things. An empty list and an absent argument are both returned as
     * the fallback, because a client that omits a filter and one that sends an empty filter mean
     * the same thing by it.
     */
    public java.util.List<String> optionalStringList(String name, java.util.List<String> fallback) {
        Object value = raw.get(name);
        if (value == null) {
            return fallback;
        }
        if (!(value instanceof java.util.List<?> list)) {
            throw new BadArgumentException("argument " + name + " must be an array of strings");
        }
        if (list.isEmpty()) {
            return fallback;
        }
        java.util.List<String> strings = new java.util.ArrayList<>(list.size());
        for (Object element : list) {
            if (!(element instanceof String s) || s.isBlank()) {
                throw new BadArgumentException("argument " + name + " must contain only non-empty strings");
            }
            strings.add(s);
        }
        return java.util.List.copyOf(strings);
    }

    public boolean optionalBoolean(String name, boolean fallback) {
        Object value = raw.get(name);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        throw new BadArgumentException("argument " + name + " must be true or false");
    }

    public Map<String, Object> raw() {
        return raw;
    }
}
