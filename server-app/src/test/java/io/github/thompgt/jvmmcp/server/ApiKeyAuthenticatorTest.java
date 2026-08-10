package io.github.thompgt.jvmmcp.server;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Startup checks on the configured keys.
 *
 * <p>The case worth a test is the unresolved placeholder. Boot's binder leaves
 * {@code ${ANALYST_KEY}} as its own text when the variable is unset rather than failing, so a
 * quickstart that copies {@code bridge.example.yaml} and switches to HTTP used to be told its
 * key was too short — true, and no help at all in finding the unset variable.
 */
class ApiKeyAuthenticatorTest {

    @Test
    @DisplayName("an unresolved ${VAR} names the variable rather than complaining about length")
    void unresolvedPlaceholdersAreNamed() {
        assertThatThrownBy(() -> new ApiKeyAuthenticator(auth("${ANALYST_KEY}", "analytics-team")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ANALYST_KEY")
                .hasMessageContaining("analytics-team")
                // The misleading diagnosis this replaces.
                .hasMessageNotContaining("shorter than 16 characters");
    }

    @Test
    @DisplayName("a placeholder long enough to pass the length check is still refused")
    void aLongPlaceholderIsNotUsedAsALiteralKey() {
        // 16+ characters, so nothing but the placeholder check stops this becoming a real,
        // shared, source-controlled credential.
        assertThatThrownBy(() -> new ApiKeyAuthenticator(auth("${BRIDGE_ONCALL_API_KEY}", "oncall")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("BRIDGE_ONCALL_API_KEY");
    }

    @Test
    void shortKeysAreStillRefused() {
        assertThatThrownBy(() -> new ApiKeyAuthenticator(auth("too-short", "oncall")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shorter than 16 characters");
    }

    private static BridgeProperties.Auth auth(String key, String principal) {
        BridgeProperties.ApiKey configured = new BridgeProperties.ApiKey();
        configured.setKey(key);
        configured.setPrincipal(principal);

        BridgeProperties.Auth auth = new BridgeProperties.Auth();
        auth.setKeys(List.of(configured));
        return auth;
    }
}
