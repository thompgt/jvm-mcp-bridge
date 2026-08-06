package io.github.thompgt.jvmmcp.policy;

import io.github.thompgt.jvmmcp.core.Principal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The set of policy profiles a backend offers, and the rule for picking one per caller.
 *
 * <p>Named profiles may only <em>narrow</em> the backend default, never widen it, and that is
 * checked when this is built rather than when a query runs. Two things follow. A profile can
 * never become a way to reach past the backend's own configuration — the default is a ceiling,
 * so reviewing what a backend exposes means reading one list. And because tool descriptions are
 * generated from the default, what a model is told about the limits is always an upper bound:
 * it may be permitted less than the description promises, never more. A description that
 * understated the limits would be the drift that makes a model attempt unsafe calls.
 */
public final class PolicyProfiles {

    private static final Logger log = LoggerFactory.getLogger(PolicyProfiles.class);

    private final PolicyProfile defaultProfile;
    private final Map<String, PolicyProfile> named;

    private PolicyProfiles(PolicyProfile defaultProfile, Map<String, PolicyProfile> named) {
        this.defaultProfile = defaultProfile;
        this.named = Map.copyOf(named);
    }

    /** A backend with no per-principal profiles: everyone gets the default. */
    public static PolicyProfiles of(PolicyProfile defaultProfile) {
        return new PolicyProfiles(defaultProfile, Map.of());
    }

    /**
     * @throws IllegalArgumentException if any named profile is broader than {@code defaultProfile}
     *     in any dimension. The message names the dimension, because "profile is too broad" sends
     *     the operator looking through four lists.
     */
    public static PolicyProfiles of(PolicyProfile defaultProfile, Map<String, PolicyProfile> named) {
        Map<String, PolicyProfile> checked = new LinkedHashMap<>();
        for (Map.Entry<String, PolicyProfile> entry : named.entrySet()) {
            String name = entry.getKey().trim().toLowerCase(Locale.ROOT);
            if (name.equals(Principal.DEFAULT_PROFILE)) {
                throw new IllegalArgumentException(
                        "a profile may not be called '"
                                + Principal.DEFAULT_PROFILE
                                + "' — that name always refers to the backend's own policy block");
            }
            requireNarrower(defaultProfile, entry.getValue(), name);
            checked.put(name, entry.getValue());
        }
        return new PolicyProfiles(defaultProfile, checked);
    }

    private static void requireNarrower(PolicyProfile ceiling, PolicyProfile profile, String name) {
        for (String resource : profile.readableResources()) {
            if (!ceiling.isReadable(resource)) {
                throw new IllegalArgumentException(problem(
                        name,
                        ceiling,
                        "it allows reading '"
                                + resource
                                + "', which the backend default does not. Readable: "
                                + String.join(", ", ceiling.readableResources())));
            }
        }
        for (String resource : profile.writableResources()) {
            if (!ceiling.isWritable(resource)) {
                throw new IllegalArgumentException(problem(
                        name, ceiling, "it allows writing '" + resource + "', which the backend default does not"));
            }
        }
        if (permissiveness(profile.mode()) > permissiveness(ceiling.mode())) {
            throw new IllegalArgumentException(problem(
                    name, ceiling, "its mode " + profile.mode() + " is broader than the backend's " + ceiling.mode()));
        }
        if (profile.maxRows() > ceiling.maxRows()) {
            throw new IllegalArgumentException(problem(
                    name, ceiling, "its max-rows " + profile.maxRows() + " exceeds the backend's " + ceiling.maxRows()));
        }
        if (profile.maxResultBytes() > ceiling.maxResultBytes()) {
            throw new IllegalArgumentException(problem(
                    name,
                    ceiling,
                    "its max-result-bytes " + profile.maxResultBytes() + " exceeds the backend's "
                            + ceiling.maxResultBytes()));
        }
        if (profile.timeout().compareTo(ceiling.timeout()) > 0) {
            throw new IllegalArgumentException(problem(
                    name,
                    ceiling,
                    "its statement timeout " + profile.timeout() + " exceeds the backend's " + ceiling.timeout()));
        }
        // Redaction narrows by covering more, so the default's patterns are a floor.
        for (String pattern : ceiling.redactionPatterns()) {
            if (!profile.redactionPatterns().contains(pattern)) {
                throw new IllegalArgumentException(problem(
                        name,
                        ceiling,
                        "it drops the redaction pattern '" + pattern + "' that the backend default applies"));
            }
        }
    }

    private static String problem(String name, PolicyProfile ceiling, String detail) {
        return "profile '" + name + "' on backend '" + ceiling.backendName()
                + "' is broader than the backend default: " + detail
                + ". Profiles may only narrow what the backend already permits.";
    }

    /** DRY_RUN touches nothing, so it is the narrowest of the three. */
    private static int permissiveness(AccessMode mode) {
        return switch (mode) {
            case DRY_RUN -> 0;
            case READ_ONLY -> 1;
            case READ_WRITE -> 2;
        };
    }

    /**
     * The profile that applies to {@code principal}.
     *
     * <p>An unknown profile name falls back to the default rather than failing the call. The
     * default is the <em>narrowest</em> thing that is certainly configured, and a token carrying
     * a profile claim this backend has never heard of is ordinary in a deployment where one
     * issuer serves several bridges.
     */
    public PolicyProfile forPrincipal(Principal principal) {
        if (principal == null || Principal.DEFAULT_PROFILE.equals(principal.profile())) {
            return defaultProfile;
        }
        PolicyProfile profile = named.get(principal.profile().trim().toLowerCase(Locale.ROOT));
        if (profile == null) {
            log.debug(
                    "principal '{}' asked for profile '{}' which backend '{}' does not define; using the default",
                    principal.name(),
                    principal.profile(),
                    defaultProfile.backendName());
            return defaultProfile;
        }
        return profile;
    }

    /** The backend's own policy block. Tool descriptions are generated from this. */
    public PolicyProfile defaultProfile() {
        return defaultProfile;
    }

    public java.util.Set<String> names() {
        return named.keySet();
    }
}
