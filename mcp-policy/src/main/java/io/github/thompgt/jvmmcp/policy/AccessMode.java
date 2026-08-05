package io.github.thompgt.jvmmcp.policy;

/**
 * How much the bridge is permitted to do to a backend.
 *
 * <p>The default is {@link #READ_ONLY} and nothing escalates it at runtime — mode comes from
 * configuration only. A tool result can never widen it, which matters because tool results
 * are untrusted: a database row can contain text shaped like an instruction.
 */
public enum AccessMode {

    /** Reads only. The out-of-the-box mode; a deployment that ignores the docs is still safe. */
    READ_ONLY,

    /**
     * Validates the request and returns the resolved plan without touching the backend.
     *
     * <p>The honest way to audit guardrails before granting access: an operator sees exactly
     * which tables a query resolved to and which rule would have allowed it.
     */
    DRY_RUN,

    /**
     * Writes are <em>possible</em>, not permitted. A write still needs its target on the
     * separate write allowlist. Enabling this mode alone grants nothing. See docs/adr/003.
     */
    READ_WRITE;

    public boolean allowsBackendAccess() {
        return this != DRY_RUN;
    }
}
