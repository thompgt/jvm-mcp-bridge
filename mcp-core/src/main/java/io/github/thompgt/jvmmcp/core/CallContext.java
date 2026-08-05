package io.github.thompgt.jvmmcp.core;

import java.util.function.Supplier;

/**
 * Carries the calling {@link Principal} from the transport down to the policy engine.
 *
 * <p>A thread-local rather than a parameter, because the alternative is threading a principal
 * through every adapter method and every tool signature — and the moment one of them forgets,
 * the call runs as somebody else. The binding happens in exactly one place ({@link
 * ToolRegistry}), it always unbinds, and adapters never touch this class at all.
 *
 * <p>Safe with the SDK's synchronous server: each tool call is handled on one thread for its
 * whole duration. An adapter that hands work to another thread must not expect the principal
 * to follow it there — none currently do, and a new one should pass the limits it was given
 * instead.
 */
public final class CallContext {

    /**
     * Key under which the transport publishes the principal. HTTP puts it there after
     * authenticating; stdio has no transport context and falls back to {@link Principal#LOCAL}.
     */
    public static final String TRANSPORT_KEY = "jvm-mcp-bridge.principal";

    private static final ThreadLocal<Principal> CURRENT = new ThreadLocal<>();

    private CallContext() {}

    /** The principal for the call on this thread, or {@link Principal#LOCAL} outside one. */
    public static Principal principal() {
        Principal principal = CURRENT.get();
        return principal == null ? Principal.LOCAL : principal;
    }

    /** Runs {@code body} with {@code principal} bound, restoring the previous binding after. */
    public static <T> T with(Principal principal, Supplier<T> body) {
        Principal previous = CURRENT.get();
        CURRENT.set(principal);
        try {
            return body.get();
        } finally {
            // Restored rather than cleared: the SDK's threads are pooled and reused, and a
            // stale principal left behind would be attributed to the next call on that thread.
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }
}
