package io.github.thompgt.jvmmcp.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The result of asking one backend whether it is reachable.
 *
 * <p>Deliberately not a Spring {@code Health}: the adapters are Spring-free, and the health
 * endpoint is one consumer of this rather than its definition. A future admin tool or a
 * startup self-check reads the same record.
 *
 * @param backend the backend's configured name, as it appears in the audit log
 * @param reachable whether the check succeeded
 * @param latencyMillis how long the check took, which is the number that moves before an
 *     outage does
 * @param details non-sensitive facts about the backend. Never a URL, a username or a
 *     credential — this ends up on an endpoint a probe reads without authenticating.
 * @param error a short description when {@code reachable} is false, otherwise null
 */
public record BackendProbe(
        String backend, boolean reachable, long latencyMillis, Map<String, Object> details, String error) {

    public BackendProbe {
        details = Map.copyOf(details);
    }

    public static BackendProbe up(String backend, long latencyMillis, Map<String, Object> details) {
        return new BackendProbe(backend, true, latencyMillis, details, null);
    }

    public static BackendProbe down(String backend, long latencyMillis, Map<String, Object> details, String error) {
        return new BackendProbe(backend, false, latencyMillis, details, error);
    }

    /**
     * Renders the cause of a failure without the stack trace or the connection string.
     *
     * <p>A driver's exception message is the most useful thing an operator can be shown here,
     * and it is also the most likely place for a host, a port or a database name to appear.
     * The class name plus the message is the compromise: enough to tell a refused connection
     * from a bad password, and no more than the operator already knows.
     */
    public static String describe(Throwable cause) {
        Throwable root = cause;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    /** The probe as a flat map, for a caller that serialises rather than inspects. */
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>(details);
        map.put("backend", backend);
        map.put("latencyMillis", latencyMillis);
        if (error != null) {
            map.put("error", error);
        }
        return map;
    }
}
