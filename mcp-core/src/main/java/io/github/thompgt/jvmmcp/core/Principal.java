package io.github.thompgt.jvmmcp.core;

import java.util.Objects;

/**
 * Who is making a call, and which policy profile applies to them.
 *
 * <p>Two separate things on purpose. {@code name} is what lands in the audit log and answers
 * "who read this table"; {@code profile} is what the policy engine looks up and answers "what
 * were they allowed to read". Collapsing them would mean every new user needed a new profile,
 * which is how allowlists end up copy-pasted and drift apart.
 *
 * @param name identity for the audit trail — an API key's label, a JWT subject, or {@code local}
 * @param profile name of the policy profile to apply; {@link #DEFAULT_PROFILE} for the
 *     backend's configured default
 */
public record Principal(String name, String profile) {

    /** The profile a backend's own configuration defines, used when nothing narrower applies. */
    public static final String DEFAULT_PROFILE = "default";

    /**
     * The caller on the stdio transport. There is no authentication there and none is wanted:
     * the client launched this process, so it already has whatever access the process has.
     */
    public static final Principal LOCAL = new Principal("local", DEFAULT_PROFILE);

    public Principal {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(profile, "profile");
        if (name.isBlank()) {
            throw new IllegalArgumentException("principal name must not be blank");
        }
        if (profile.isBlank()) {
            throw new IllegalArgumentException("principal profile must not be blank");
        }
    }

    /** A principal that carries no profile of its own and takes the backend default. */
    public static Principal named(String name) {
        return new Principal(name, DEFAULT_PROFILE);
    }
}
