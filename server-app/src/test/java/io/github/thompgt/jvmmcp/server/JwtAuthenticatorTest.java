package io.github.thompgt.jvmmcp.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import io.github.thompgt.jvmmcp.core.Principal;
import jakarta.servlet.http.HttpServletRequest;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/**
 * Signs tokens with a locally generated key and pushes them through the real validation chain.
 *
 * <p>The case that matters is {@link #rejectsATokenMintedForAnotherService()}: a correctly
 * signed, unexpired token from the right issuer, for somebody else. Everything about it is
 * genuine except who it was for.
 */
class JwtAuthenticatorTest {

    private static final String ISSUER = "https://issuer.example.com";
    private static final String AUDIENCE = "https://bridge.internal/mcp";

    private static RSAPrivateKey privateKey;
    private static JwtDecoder decoder;

    @BeforeAll
    static void keys() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair pair = generator.generateKeyPair();
        privateKey = (RSAPrivateKey) pair.getPrivate();

        NimbusJwtDecoder nimbus =
                NimbusJwtDecoder.withPublicKey((RSAPublicKey) pair.getPublic()).build();
        nimbus.setJwtValidator(JwtAuthenticator.validators(config()));
        decoder = nimbus;
    }

    private static BridgeProperties.OAuth2 config() {
        BridgeProperties.OAuth2 oauth2 = new BridgeProperties.OAuth2();
        oauth2.setIssuerUri(ISSUER);
        oauth2.setAudience(AUDIENCE);
        oauth2.setPrincipalClaim("sub");
        oauth2.setProfileClaim("bridge_profile");
        return oauth2;
    }

    private static JwtAuthenticator authenticator() {
        BridgeProperties.Auth auth = new BridgeProperties.Auth();
        auth.setMode(BridgeProperties.Auth.Mode.OAUTH2);
        auth.setOauth2(config());
        return new JwtAuthenticator(auth, decoder);
    }

    @Test
    void acceptsATokenIssuedForThisBridge() {
        Optional<Principal> principal = authenticator().authenticate(bearer(token(claims -> claims)));

        assertThat(principal).isPresent();
        assertThat(principal.get().name()).isEqualTo("analyst@example.com");
        assertThat(principal.get().profile()).isEqualTo("read-only-analyst");
    }

    @Test
    @DisplayName("a valid token minted for another service is refused")
    void rejectsATokenMintedForAnotherService() {
        // Right issuer, right signature, not expired — and audience 'some-other-api'. Accepting
        // this is the confused-deputy problem RFC 8707 resource indicators exist to close.
        String other = token(claims -> claims.audience(List.of("https://some-other-api.internal")));

        assertThat(authenticator().authenticate(bearer(other))).isEmpty();
    }

    @Test
    void rejectsATokenFromAnotherIssuer() {
        assertThat(authenticator().authenticate(bearer(token(claims -> claims.issuer("https://evil.example.com")))))
                .isEmpty();
    }

    @Test
    void rejectsAnExpiredToken() {
        String expired = token(claims -> claims.expirationTime(
                Date.from(Instant.now().minus(1, ChronoUnit.HOURS))));

        assertThat(authenticator().authenticate(bearer(expired))).isEmpty();
    }

    @Test
    void rejectsATokenSignedByTheWrongKey() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        RSAPrivateKey attacker = (RSAPrivateKey) generator.generateKeyPair().getPrivate();

        assertThat(authenticator().authenticate(bearer(sign(baseClaims().build(), attacker))))
                .isEmpty();
    }

    @Test
    @DisplayName("a token with no profile claim falls back to the backend default")
    void missingProfileClaimIsTheDefaultProfile() {
        String noProfile = token(claims -> claims.claim("bridge_profile", null));

        assertThat(authenticator().authenticate(bearer(noProfile)).orElseThrow().profile())
                .isEqualTo(Principal.DEFAULT_PROFILE);
    }

    @Test
    void rejectsATokenMissingTheRequiredScope() throws Exception {
        BridgeProperties.OAuth2 scoped = config();
        scoped.setRequiredScope("mcp.read");
        BridgeProperties.Auth auth = new BridgeProperties.Auth();
        auth.setOauth2(scoped);

        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair pair = generator.generateKeyPair();
        NimbusJwtDecoder scopedDecoder =
                NimbusJwtDecoder.withPublicKey((RSAPublicKey) pair.getPublic()).build();
        scopedDecoder.setJwtValidator(JwtAuthenticator.validators(scoped));
        JwtAuthenticator scopedAuthenticator = new JwtAuthenticator(auth, scopedDecoder);
        RSAPrivateKey key = (RSAPrivateKey) pair.getPrivate();

        assertThat(scopedAuthenticator.authenticate(bearer(sign(baseClaims().build(), key))))
                .isEmpty();
        assertThat(scopedAuthenticator.authenticate(
                        bearer(sign(baseClaims().claim("scope", "mcp.read mcp.write").build(), key))))
                .isPresent();
    }

    @Test
    @DisplayName("configuring oauth2 without an audience is refused at startup")
    void audienceIsMandatory() {
        BridgeProperties.Auth auth = new BridgeProperties.Auth();
        auth.setMode(BridgeProperties.Auth.Mode.OAUTH2);
        BridgeProperties.OAuth2 oauth2 = config();
        oauth2.setAudience(null);
        auth.setOauth2(oauth2);

        // Failing to start beats starting with the check that makes the others meaningful off.
        assertThatThrownBy(() -> new JwtAuthenticator(auth))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("audience")
                .hasMessageContaining("any other service");
    }

    @Test
    void nonBearerCredentialsAreNotEvenDecoded() {
        assertThat(authenticator().authenticate(header("Basic dXNlcjpwYXNz"))).isEmpty();
        assertThat(authenticator().authenticate(header(null))).isEmpty();
    }

    // --- helpers ---

    private static JWTClaimsSet.Builder baseClaims() {
        return new JWTClaimsSet.Builder()
                .subject("analyst@example.com")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .claim("bridge_profile", "read-only-analyst")
                .issueTime(Date.from(Instant.now().minusSeconds(30)))
                .expirationTime(Date.from(Instant.now().plus(10, ChronoUnit.MINUTES)));
    }

    private static String token(java.util.function.UnaryOperator<JWTClaimsSet.Builder> customise) {
        return sign(customise.apply(baseClaims()).build(), privateKey);
    }

    private static String sign(JWTClaimsSet claims, RSAPrivateKey key) {
        try {
            SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claims);
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (Exception e) {
            throw new IllegalStateException("could not sign the test token", e);
        }
    }

    private static HttpServletRequest bearer(String token) {
        return header("Bearer " + token);
    }

    /** Minimal stub: the authenticator reads exactly one header and nothing else. */
    private static HttpServletRequest header(String value) {
        return (HttpServletRequest) java.lang.reflect.Proxy.newProxyInstance(
                JwtAuthenticatorTest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                (proxy, method, args) ->
                        "getHeader".equals(method.getName()) ? value : defaultValue(method.getReturnType()));
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        return type == boolean.class ? Boolean.FALSE : 0;
    }
}
