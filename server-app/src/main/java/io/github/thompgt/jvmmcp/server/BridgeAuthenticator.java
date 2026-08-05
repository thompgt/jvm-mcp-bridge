package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.Principal;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Optional;

/**
 * Turns an HTTP request into the {@link Principal} that made it.
 *
 * <p>Deliberately narrow. An authenticator says <em>who</em> — it never decides what they may
 * do, because authorisation belongs to the policy engine and having two places that grant
 * access is how the two disagree. All it produces is a name and a profile to look up.
 */
public interface BridgeAuthenticator {

    /**
     * @return the caller, or empty if the credential is missing, malformed or unrecognised.
     *     Those three are one answer on purpose: distinguishing them tells an attacker which
     *     of their guesses was a real key.
     */
    Optional<Principal> authenticate(HttpServletRequest request);

    /** Value for the {@code WWW-Authenticate} header sent with a 401. */
    String challenge();

    /**
     * Accepts every request as an anonymous principal on the default profile.
     *
     * <p>Only reachable when the operator has explicitly acknowledged it in configuration; see
     * {@link BridgeProperties.Auth.Mode#NONE}.
     */
    static BridgeAuthenticator anonymous() {
        return new BridgeAuthenticator() {
            private final Optional<Principal> anonymous = Optional.of(Principal.named("anonymous"));

            @Override
            public Optional<Principal> authenticate(HttpServletRequest request) {
                return anonymous;
            }

            @Override
            public String challenge() {
                return "Bearer realm=\"jvm-mcp-bridge\"";
            }
        };
    }
}
