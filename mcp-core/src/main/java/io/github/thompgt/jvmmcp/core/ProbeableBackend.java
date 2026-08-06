package io.github.thompgt.jvmmcp.core;

/**
 * A backend that can say whether it is currently usable.
 *
 * <p>Implemented by adapters so the health endpoint can report each backend separately. That
 * separation is the requirement, not a nicety: this process fronts several systems, and a
 * broker that is down does not stop the database questions being answerable. Collapsing them
 * into one status would take the whole bridge out of rotation over a backend most callers were
 * not using.
 */
public interface ProbeableBackend {

    /** Name used in configuration and in the audit log, e.g. {@code orders-db}. */
    String backendName();

    /**
     * Checks reachability. Must not throw — an unreachable backend is a result, not an error —
     * and must be cheap enough to run on every scrape.
     */
    BackendProbe probe();
}
