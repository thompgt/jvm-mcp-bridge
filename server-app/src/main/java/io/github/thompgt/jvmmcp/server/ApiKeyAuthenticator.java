package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.Principal;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a static API key to the {@link Principal} it belongs to.
 *
 * <p>For internal networks where running an identity provider to let three services query a
 * database is disproportionate. OAuth2 (2.3) is the option for anything reachable more widely.
 */
public final class ApiKeyAuthenticator implements BridgeAuthenticator {

    private static final Logger log = LoggerFactory.getLogger(ApiKeyAuthenticator.class);

    private static final String BEARER = "Bearer ";

    private final String header;

    /** Keyed by SHA-256 of the secret, so the configured keys are not held in memory verbatim. */
    private final Map<String, Principal> byDigest;

    public ApiKeyAuthenticator(BridgeProperties.Auth auth) {
        this.header = auth.getHeader();
        this.byDigest = index(auth);
    }

    private static Map<String, Principal> index(BridgeProperties.Auth auth) {
        if (auth.getKeys().isEmpty()) {
            throw new IllegalStateException(
                    "bridge.http.auth.mode is api-key but no keys are configured, so every request"
                            + " would be refused. Add at least one entry under bridge.http.auth.keys.");
        }
        Map<String, Principal> index = new LinkedHashMap<>();
        for (BridgeProperties.ApiKey configured : auth.getKeys()) {
            String secret = configured.getKey();
            if (secret == null || secret.isBlank()) {
                throw new IllegalStateException("bridge.http.auth.keys[].key is required");
            }
            if (secret.startsWith("${") && secret.endsWith("}")) {
                // Boot's binder leaves an unresolvable placeholder as its own text rather than
                // failing, so `key: ${ANALYST_KEY}` with no ANALYST_KEY set arrives here as the
                // literal string. Without this the operator is told their key is too short,
                // which is true and useless: the actual fault is an unset variable.
                throw new IllegalStateException(
                        "the API key for principal '"
                                + configured.getPrincipal()
                                + "' is the unresolved placeholder "
                                + secret
                                + ". Export "
                                + secret.substring(2, secret.length() - 1)
                                + " before starting the bridge, or write the key into the config"
                                + " file. It is not being used as a literal key.");
            }
            if (secret.length() < 16) {
                // Short keys in a config file get reused and guessed. Refuse at startup rather
                // than let a deployment discover this after the fact.
                throw new IllegalStateException(
                        "API key for principal '"
                                + configured.getPrincipal()
                                + "' is shorter than 16 characters. Generate one with"
                                + " `openssl rand -base64 32`.");
            }
            String name = configured.getPrincipal();
            if (name == null || name.isBlank()) {
                throw new IllegalStateException(
                        "bridge.http.auth.keys[].principal is required — it is what the audit log"
                                + " records, and 'unknown' is not an answer after an incident.");
            }
            String profile = configured.getProfile() == null || configured.getProfile().isBlank()
                    ? Principal.DEFAULT_PROFILE
                    : configured.getProfile();

            Principal previous = index.put(digest(secret), new Principal(name, profile));
            if (previous != null) {
                throw new IllegalStateException(
                        "the same API key is configured for principals '"
                                + previous.name()
                                + "' and '"
                                + name
                                + "'; the audit log could not tell them apart");
            }
        }
        log.info("api-key auth enabled for {} principal(s) on header '{}'", index.size(), auth.getHeader());
        return Map.copyOf(index);
    }

    @Override
    public Optional<Principal> authenticate(HttpServletRequest request) {
        String presented = request.getHeader(header);
        if (presented == null || presented.isBlank()) {
            return Optional.empty();
        }
        if (presented.startsWith(BEARER)) {
            presented = presented.substring(BEARER.length());
        }
        // Comparison is on the digest, which is fixed length, so this does not leak the key's
        // length or a prefix through timing the way String.equals on the raw secret would.
        return Optional.ofNullable(byDigest.get(digest(presented.trim())));
    }

    @Override
    public String challenge() {
        return "Bearer realm=\"jvm-mcp-bridge\"";
    }

    private static String digest(String value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(hash);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the JLS and is missing", e);
        }
    }
}
