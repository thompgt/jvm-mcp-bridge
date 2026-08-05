package io.github.thompgt.jvmmcp.policy;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The configured policy for one backend.
 *
 * <p>Built through {@link Builder}, which validates at construction rather than at first use.
 * A misconfiguration should stop the server starting, not surface as a surprising allow on a
 * production query at 3am.
 */
public final class PolicyProfile {

    private final String backendName;
    private final AccessMode mode;
    private final Set<String> readAllowlist;
    private final Set<String> writeAllowlist;
    private final int maxRows;
    private final long maxResultBytes;
    private final Duration timeout;
    private final List<String> redactionPatterns;

    private PolicyProfile(Builder b) {
        this.backendName = b.backendName;
        this.mode = b.mode;
        this.readAllowlist = Set.copyOf(b.readAllowlist);
        this.writeAllowlist = Set.copyOf(b.writeAllowlist);
        this.maxRows = b.maxRows;
        this.maxResultBytes = b.maxResultBytes;
        this.timeout = b.timeout;
        this.redactionPatterns = List.copyOf(b.redactionPatterns);
    }

    public static Builder builder(String backendName) {
        return new Builder(backendName);
    }

    public String backendName() {
        return backendName;
    }

    public AccessMode mode() {
        return mode;
    }

    /** Sorted for stable denial messages — the model should see the same list every time. */
    public List<String> readableResources() {
        return readAllowlist.stream().sorted().toList();
    }

    public List<String> writableResources() {
        return writeAllowlist.stream().sorted().toList();
    }

    /**
     * Case-insensitive: SQL identifiers and topic names are compared folded to lower case.
     *
     * <p>Entries may use {@code *} as a wildcard, which is how Kafka topic families
     * ({@code orders.*}) are expressed. The wildcard is matched against the resource name the
     * caller resolved — for SQL that is a table name taken from the parsed AST, never the raw
     * query text.
     */
    public boolean isReadable(String resource) {
        return matchesAny(readAllowlist, normalise(resource));
    }

    /** Writable resources are always literal; {@link Builder#allowWrite} rejects wildcards. */
    public boolean isWritable(String resource) {
        return writeAllowlist.contains(normalise(resource));
    }

    private static boolean matchesAny(Set<String> patterns, String resource) {
        if (resource.isEmpty()) {
            return false;
        }
        for (String pattern : patterns) {
            if (matches(pattern, resource)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matches(String pattern, String resource) {
        if (pattern.indexOf('*') < 0) {
            return pattern.equals(resource);
        }
        // Quote every literal segment so a '.' in a table or topic name cannot act as a
        // regex metacharacter and widen the allowlist beyond what the operator wrote.
        StringBuilder regex = new StringBuilder();
        int from = 0;
        int star;
        while ((star = pattern.indexOf('*', from)) >= 0) {
            if (star > from) {
                regex.append(java.util.regex.Pattern.quote(pattern.substring(from, star)));
            }
            regex.append(".*");
            from = star + 1;
        }
        if (from < pattern.length()) {
            regex.append(java.util.regex.Pattern.quote(pattern.substring(from)));
        }
        return resource.matches(regex.toString());
    }

    public int maxRows() {
        return maxRows;
    }

    public long maxResultBytes() {
        return maxResultBytes;
    }

    public Duration timeout() {
        return timeout;
    }

    public List<String> redactionPatterns() {
        return redactionPatterns;
    }

    EffectiveLimits baseLimits() {
        return new EffectiveLimits(maxRows, maxResultBytes, timeout, mode == AccessMode.DRY_RUN);
    }

    static String normalise(String resource) {
        return resource == null ? "" : resource.trim().toLowerCase(Locale.ROOT);
    }

    /** Validating builder. Every rejection here is a server that refuses to start. */
    public static final class Builder {
        private final String backendName;
        private AccessMode mode = AccessMode.READ_ONLY;
        private final Set<String> readAllowlist = new LinkedHashSet<>();
        private final Set<String> writeAllowlist = new LinkedHashSet<>();
        private int maxRows = 100;
        private long maxResultBytes = 1_048_576L;
        private Duration timeout = Duration.ofSeconds(5);
        private final List<String> redactionPatterns = new java.util.ArrayList<>();

        private Builder(String backendName) {
            if (backendName == null || backendName.isBlank()) {
                throw new IllegalArgumentException("backend name is required");
            }
            this.backendName = backendName;
        }

        public Builder mode(AccessMode mode) {
            this.mode = java.util.Objects.requireNonNull(mode, "mode");
            return this;
        }

        /**
         * Resources readable through this backend.
         *
         * <p>{@code *} is accepted here — exposing everything readable is a legitimate, if
         * broad, choice, and the operator has to type it. It is <em>not</em> accepted for
         * writes; see {@link #allowWrite}.
         */
        public Builder allowRead(String... resources) {
            for (String r : resources) {
                readAllowlist.add(normalise(r));
            }
            return this;
        }

        public Builder allowRead(java.util.Collection<String> resources) {
            resources.forEach(r -> readAllowlist.add(normalise(r)));
            return this;
        }

        /**
         * Resources writable through this backend.
         *
         * @throws IllegalArgumentException on a wildcard. A blanket write grant is never a
         *     considered decision — it is what someone types to make an error go away, and
         *     the blast radius is the whole database. Writes must be enumerated. See ADR 003.
         */
        public Builder allowWrite(String... resources) {
            for (String r : resources) {
                String n = normalise(r);
                if (n.contains("*")) {
                    throw new IllegalArgumentException(
                            "wildcard write allowlist is not permitted (backend '"
                                    + backendName
                                    + "'): every writable resource must be named explicitly");
                }
                writeAllowlist.add(n);
            }
            return this;
        }

        public Builder allowWrite(java.util.Collection<String> resources) {
            return allowWrite(resources.toArray(String[]::new));
        }

        public Builder maxRows(int maxRows) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("max-rows must be positive, got " + maxRows);
            }
            this.maxRows = maxRows;
            return this;
        }

        public Builder maxResultBytes(long maxResultBytes) {
            if (maxResultBytes <= 0) {
                throw new IllegalArgumentException(
                        "max-result-bytes must be positive, got " + maxResultBytes);
            }
            this.maxResultBytes = maxResultBytes;
            return this;
        }

        public Builder timeout(Duration timeout) {
            if (timeout == null || timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("statement timeout must be positive");
            }
            this.timeout = timeout;
            return this;
        }

        public Builder redact(String... patterns) {
            redactionPatterns.addAll(List.of(patterns));
            return this;
        }

        public Builder redact(java.util.Collection<String> patterns) {
            redactionPatterns.addAll(patterns);
            return this;
        }

        public PolicyProfile build() {
            // A write allowlist under READ_ONLY is not harmless — it means someone believed
            // writes were enabled. Failing loudly beats silently ignoring their intent.
            if (mode != AccessMode.READ_WRITE && !writeAllowlist.isEmpty()) {
                throw new IllegalArgumentException(
                        "backend '"
                                + backendName
                                + "' lists writable resources but mode is "
                                + mode
                                + "; set mode to READ_WRITE or remove the write allowlist");
            }
            return new PolicyProfile(this);
        }
    }
}
