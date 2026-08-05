package io.github.thompgt.jvmmcp.policy;

import java.time.Duration;

/**
 * The bounds that apply to one allowed call, after the client's request has been reconciled
 * with the configured policy.
 *
 * <p>A client may ask for <em>fewer</em> rows than the cap; it may never ask for more. The
 * reconciliation is a floor-and-ceiling, not a negotiation, and it happens once here rather
 * than being re-derived in each adapter.
 *
 * @param maxRows hard row cap
 * @param maxResultBytes hard cap on serialised result size
 * @param timeout how long the backend call may run
 * @param dryRun when true the adapter must return its resolved plan and touch nothing
 */
public record EffectiveLimits(int maxRows, long maxResultBytes, Duration timeout, boolean dryRun) {

    /** Limits attached to a denial. Nothing may run, so the values are deliberately zero. */
    public static EffectiveLimits none() {
        return new EffectiveLimits(0, 0L, Duration.ZERO, false);
    }

    /**
     * Applies a client-requested row count, clamped to the configured cap.
     *
     * @param requested the client's {@code max_rows}, or a non-positive value for "no preference"
     */
    public EffectiveLimits withRequestedRows(int requested) {
        if (requested <= 0) {
            return this;
        }
        return new EffectiveLimits(Math.min(requested, maxRows), maxResultBytes, timeout, dryRun);
    }

    public EffectiveLimits asDryRun() {
        return new EffectiveLimits(maxRows, maxResultBytes, timeout, true);
    }
}
