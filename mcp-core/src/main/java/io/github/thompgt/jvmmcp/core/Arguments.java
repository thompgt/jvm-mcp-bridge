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
