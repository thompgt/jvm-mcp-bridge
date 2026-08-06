package io.github.thompgt.jvmmcp.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.thompgt.jvmmcp.core.Principal;
import java.time.Duration;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The narrowing invariant, one test per dimension.
 *
 * <p>Every one of these is a way a profile could end up granting more than the backend it sits
 * on, and each is rejected at construction rather than at query time — a widening profile that
 * only fails when someone uses it is a hole that exists in production until it is exploited.
 */
class PolicyProfilesTest {

    private static PolicyProfile backend() {
        return PolicyProfile.builder("orders-db")
                .mode(AccessMode.READ_ONLY)
                .allowRead("orders", "customers")
                .maxRows(500)
                .maxResultBytes(1_000_000L)
                .timeout(Duration.ofSeconds(10))
                .redact("customers.email")
                .build();
    }

    /** The backend default with one thing changed, so each test names only what it widens. */
    private static PolicyProfile variant(UnaryOperator<PolicyProfile.Builder> customise) {
        return customise
                .apply(PolicyProfile.builder("orders-db")
                        .mode(AccessMode.READ_ONLY)
                        .allowRead("orders", "customers")
                        .maxRows(500)
                        .maxResultBytes(1_000_000L)
                        .timeout(Duration.ofSeconds(10))
                        .redact("customers.email"))
                .build();
    }

    private static void assertRejected(PolicyProfile profile, String because) {
        assertThatThrownBy(() -> PolicyProfiles.of(backend(), Map.of("analyst", profile)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("analyst")
                .hasMessageContaining("only narrow")
                .hasMessageContaining(because);
    }

    @Test
    void acceptsAProfileThatNarrowsEveryDimension() {
        PolicyProfile narrower = PolicyProfile.builder("orders-db")
                .mode(AccessMode.DRY_RUN)
                .allowRead("orders")
                .maxRows(10)
                .maxResultBytes(1_000L)
                .timeout(Duration.ofSeconds(1))
                .redact("customers.email", "orders.total")
                .build();

        PolicyProfiles profiles = PolicyProfiles.of(backend(), Map.of("analyst", narrower));

        assertThat(profiles.forPrincipal(new Principal("a", "analyst")).maxRows()).isEqualTo(10);
    }

    @Test
    void anIdenticalProfileIsNarrowEnough() {
        assertThat(PolicyProfiles.of(backend(), Map.of("analyst", backend())).names())
                .containsExactly("analyst");
    }

    @Test
    void rejectsAProfileThatReadsATableTheBackendDoesNot() {
        assertRejected(variant(b -> b.allowRead("internal_audit")), "internal_audit");
    }

    @Test
    @DisplayName("a wildcard read is a widening if the backend enumerates tables")
    void rejectsAWildcardReadAgainstAnEnumeratedBackend() {
        assertRejected(variant(b -> b.allowRead("*")), "'*'");
    }

    @Test
    void rejectsAProfileThatWritesWhenTheBackendDoesNot() {
        PolicyProfile writable = PolicyProfile.builder("orders-db")
                .mode(AccessMode.READ_WRITE)
                .allowRead("orders")
                .allowWrite("orders")
                .maxRows(10)
                .maxResultBytes(1_000L)
                .timeout(Duration.ofSeconds(1))
                .redact("customers.email")
                .build();

        // Mode is checked too, but the write allowlist is the concrete grant and is reported first.
        assertRejected(writable, "orders");
    }

    @Test
    void rejectsAProfileInABroaderMode() {
        PolicyProfile readWrite = PolicyProfile.builder("orders-db")
                .mode(AccessMode.READ_WRITE)
                .allowRead("orders")
                .maxRows(10)
                .maxResultBytes(1_000L)
                .timeout(Duration.ofSeconds(1))
                .redact("customers.email")
                .build();

        assertRejected(readWrite, "READ_WRITE");
    }

    @Test
    void rejectsAProfileWithAHigherRowCap() {
        assertRejected(variant(b -> b.maxRows(5_000)), "max-rows 5000");
    }

    @Test
    void rejectsAProfileWithAHigherByteCap() {
        assertRejected(variant(b -> b.maxResultBytes(50_000_000L)), "max-result-bytes 50000000");
    }

    @Test
    void rejectsAProfileWithALongerTimeout() {
        assertRejected(variant(b -> b.timeout(Duration.ofMinutes(5))), "statement timeout");
    }

    @Test
    @DisplayName("dropping a redaction the backend applies is a widening")
    void rejectsAProfileThatUnredacts() {
        PolicyProfile unredacted = PolicyProfile.builder("orders-db")
                .mode(AccessMode.READ_ONLY)
                .allowRead("orders")
                .maxRows(10)
                .maxResultBytes(1_000L)
                .timeout(Duration.ofSeconds(1))
                .build();

        assertRejected(unredacted, "customers.email");
    }

    @Test
    @DisplayName("'default' is not available as a profile name")
    void rejectsAProfileNamedDefault() {
        assertThatThrownBy(() -> PolicyProfiles.of(backend(), Map.of(Principal.DEFAULT_PROFILE, backend())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("may not be called");
    }

    @Test
    void profileNamesAreMatchedCaseInsensitively() {
        PolicyProfiles profiles = PolicyProfiles.of(backend(), Map.of("Analyst", variant(b -> b.maxRows(7))));

        assertThat(profiles.forPrincipal(new Principal("a", "ANALYST")).maxRows()).isEqualTo(7);
    }

    @Test
    @DisplayName("an unrecognised profile name falls back to the default, which is the narrowest")
    void unknownProfileNamesFallBackToTheDefault() {
        PolicyProfiles profiles = PolicyProfiles.of(backend(), Map.of("analyst", variant(b -> b.maxRows(7))));

        // One issuer serving several bridges will mint tokens carrying profiles this one has
        // never heard of. Falling back to the ceiling is safe; falling back to 'analyst' or to
        // an error would be surprising in opposite directions.
        assertThat(profiles.forPrincipal(new Principal("a", "warehouse")).maxRows()).isEqualTo(500);
        assertThat(profiles.forPrincipal(Principal.LOCAL).maxRows()).isEqualTo(500);
        assertThat(profiles.forPrincipal(null).maxRows()).isEqualTo(500);
    }

    @Test
    void aBackendWithNoNamedProfilesGivesEveryoneTheDefault() {
        PolicyProfiles profiles = PolicyProfiles.of(backend());

        assertThat(profiles.names()).isEmpty();
        assertThat(profiles.forPrincipal(new Principal("a", "analyst"))).isSameAs(profiles.defaultProfile());
    }
}
