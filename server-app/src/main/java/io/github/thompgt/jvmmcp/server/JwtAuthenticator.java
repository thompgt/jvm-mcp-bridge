package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.Principal;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/**
 * Validates a JWT bearer token and maps it to a {@link Principal}.
 *
 * <p>The audience check is the point of this class. MCP's 2025-06-18 security revision requires
 * a server to verify that a token was issued <em>for it</em> (RFC 8707 resource indicators),
 * because the alternative is a confused deputy: a client holding a token for some other service
 * behind the same issuer presents it here, and a server that only checks the signature and the
 * issuer accepts it. Signature and issuer prove the token is genuine — not that it is for us.
 */
public final class JwtAuthenticator implements BridgeAuthenticator {

    private static final Logger log = LoggerFactory.getLogger(JwtAuthenticator.class);

    private static final String BEARER = "Bearer ";

    private final String header;
    private final JwtDecoder decoder;
    private final BridgeProperties.OAuth2 config;

    public JwtAuthenticator(BridgeProperties.Auth auth) {
        this(auth, decoderFor(auth.getOauth2()));
    }

    /** Test seam: lets a test supply a decoder rather than stand up an issuer. */
    JwtAuthenticator(BridgeProperties.Auth auth, JwtDecoder decoder) {
        this.header = auth.getHeader();
        this.config = auth.getOauth2();
        this.decoder = decoder;
        requireText(config.getPrincipalClaim(), "bridge.http.auth.oauth2.principal-claim");
    }

    private static JwtDecoder decoderFor(BridgeProperties.OAuth2 config) {
        requireText(config.getIssuerUri(), "bridge.http.auth.oauth2.issuer-uri");
        requireText(
                config.getAudience(),
                "bridge.http.auth.oauth2.audience — set it to the URI that identifies this bridge."
                        + " Without it, any token the issuer minted for any other service would be"
                        + " accepted here");

        NimbusJwtDecoder decoder = NimbusJwtDecoder.withIssuerLocation(config.getIssuerUri()).build();
        decoder.setJwtValidator(validators(config));
        log.info(
                "oauth2 auth enabled: issuer {} audience {}", config.getIssuerUri(), config.getAudience());
        return decoder;
    }

    /**
     * The full validation chain. Package-private so a test can attach it to a decoder holding a
     * locally generated key — testing against a decoder without these would prove only that a
     * signature verifies, which is the part that was never in doubt.
     */
    static OAuth2TokenValidator<Jwt> validators(BridgeProperties.OAuth2 config) {
        return new DelegatingOAuth2TokenValidator<>(
                // Signature, expiry, not-before and issuer.
                JwtValidators.createDefaultWithIssuer(config.getIssuerUri()),
                audienceValidator(config.getAudience()),
                scopeValidator(config.getRequiredScope()));
    }

    private static OAuth2TokenValidator<Jwt> audienceValidator(String audience) {
        return jwt -> {
            List<String> presented = jwt.getAudience() == null ? List.of() : jwt.getAudience();
            return presented.contains(audience)
                    ? OAuth2TokenValidatorResult.success()
                    : OAuth2TokenValidatorResult.failure(new org.springframework.security.oauth2.core.OAuth2Error(
                            "invalid_token",
                            "the token's audience does not include this bridge (" + audience + ")",
                            null));
        };
    }

    private static OAuth2TokenValidator<Jwt> scopeValidator(String required) {
        if (required == null || required.isBlank()) {
            return jwt -> OAuth2TokenValidatorResult.success();
        }
        return jwt -> scopesOf(jwt).contains(required)
                ? OAuth2TokenValidatorResult.success()
                : OAuth2TokenValidatorResult.failure(new org.springframework.security.oauth2.core.OAuth2Error(
                        "insufficient_scope", "the token is missing the '" + required + "' scope", null));
    }

    /** Both spellings are in the wild: {@code scope} is RFC 8693, {@code scp} is Entra ID. */
    private static List<String> scopesOf(Jwt jwt) {
        Object claim = jwt.getClaim("scope");
        if (claim == null) {
            claim = jwt.getClaim("scp");
        }
        if (claim instanceof String text) {
            return Arrays.asList(text.split(" "));
        }
        if (claim instanceof List<?> list) {
            return list.stream().map(String::valueOf).toList();
        }
        return List.of();
    }

    @Override
    public Optional<Principal> authenticate(HttpServletRequest request) {
        String presented = request.getHeader(header);
        if (presented == null || !presented.startsWith(BEARER)) {
            return Optional.empty();
        }
        try {
            Jwt jwt = decoder.decode(presented.substring(BEARER.length()).trim());
            String name = jwt.getClaimAsString(config.getPrincipalClaim());
            if (name == null || name.isBlank()) {
                log.warn("token validated but claim '{}' is absent, so it names nobody", config.getPrincipalClaim());
                return Optional.empty();
            }
            return Optional.of(new Principal(name, profileOf(jwt)));
        } catch (JwtException e) {
            // Message only: a rejected token is routine, and its contents are a credential.
            log.warn("rejected a bearer token: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private String profileOf(Jwt jwt) {
        if (config.getProfileClaim() == null || config.getProfileClaim().isBlank()) {
            return Principal.DEFAULT_PROFILE;
        }
        String profile = jwt.getClaimAsString(config.getProfileClaim());
        return profile == null || profile.isBlank() ? Principal.DEFAULT_PROFILE : profile;
    }

    @Override
    public String challenge() {
        // Points the client at the issuer so it can go and get a token, rather than only
        // telling it that the one it had was refused.
        return "Bearer realm=\"jvm-mcp-bridge\", resource_metadata=\""
                + config.getIssuerUri()
                + "/.well-known/oauth-protected-resource\"";
    }

    private static void requireText(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(what + " is required when auth mode is "
                    + BridgeProperties.Auth.Mode.OAUTH2.name().toLowerCase(Locale.ROOT));
        }
    }
}
