package io.github.thompgt.jvmmcp.policy;

import java.time.Instant;
import java.util.Map;

/**
 * One line in the audit log: what was asked, what was decided, and what came back.
 *
 * <p>Allowed calls are recorded as well as denied ones. An audit log that only shows refusals
 * answers "what did we stop" but not "what did it read", and the second question is the one
 * asked after an incident.
 *
 * @param at when the call completed
 * @param principal authenticated caller, or {@code local} for the stdio transport where the
 *     operating-system user launching the process is the only identity
 * @param backend which backend, e.g. {@code orders-db}
 * @param tool tool name as the client called it
 * @param resources resources the call actually resolved to — for SQL, the tables from the
 *     parsed AST rather than anything the client claimed
 * @param allowed the decision
 * @param rule the rule that decided
 * @param reason why, when denied
 * @param rowsReturned how much data left the process; {@code -1} when not applicable
 * @param durationMillis wall-clock time of the backend call
 */
public record AuditRecord(
        Instant at,
        String principal,
        String backend,
        String tool,
        java.util.List<String> resources,
        boolean allowed,
        String rule,
        String reason,
        int rowsReturned,
        long durationMillis) {

    /**
     * Renders as a single JSON object.
     *
     * <p>Hand-rolled rather than delegating to a mapper: this module deliberately depends on
     * no JSON library (the SDK is on Jackson 3, Spring on Jackson 2), and the shape here is
     * fixed and flat enough that a mapper would buy nothing.
     */
    public String toJsonLine() {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        field(sb, "at", at.toString()).append(',');
        field(sb, "principal", principal).append(',');
        field(sb, "backend", backend).append(',');
        field(sb, "tool", tool).append(',');
        sb.append("\"resources\":[");
        for (int i = 0; i < resources.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(escape(resources.get(i))).append('"');
        }
        sb.append("],");
        sb.append("\"allowed\":").append(allowed).append(',');
        field(sb, "rule", rule).append(',');
        field(sb, "reason", reason).append(',');
        sb.append("\"rowsReturned\":").append(rowsReturned).append(',');
        sb.append("\"durationMillis\":").append(durationMillis);
        sb.append('}');
        return sb.toString();
    }

    private static StringBuilder field(StringBuilder sb, String key, String value) {
        return sb.append('"').append(key).append("\":\"").append(escape(value)).append('"');
    }

    private static String escape(String raw) {
        if (raw == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(raw.length() + 8);
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    // Control characters would break a line-oriented log parser, and a
                    // denied resource name is attacker-influenced text.
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    /** Convenience view for callers that want the record as a map. */
    public Map<String, Object> asMap() {
        return Map.of(
                "at", at.toString(),
                "principal", principal,
                "backend", backend,
                "tool", tool,
                "resources", resources,
                "allowed", allowed,
                "rule", rule,
                "reason", reason,
                "rowsReturned", rowsReturned,
                "durationMillis", durationMillis);
    }
}
