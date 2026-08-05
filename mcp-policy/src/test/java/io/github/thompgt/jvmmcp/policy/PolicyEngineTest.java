package io.github.thompgt.jvmmcp.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.thompgt.jvmmcp.core.ToolOutcome;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The rules here are the product. Each test asserts the <em>reason</em> a call was refused,
 * not merely that it was: a rule that denies for the wrong reason will eventually deny the
 * wrong things, and the reason is also what the model reads in order to recover.
 */
class PolicyEngineTest {

    private static PolicyProfile readOnlyProfile() {
        return PolicyProfile.builder("orders-db")
                .mode(AccessMode.READ_ONLY)
                .allowRead("customers", "orders", "order_items")
                .maxRows(200)
                .timeout(Duration.ofSeconds(5))
                .build();
    }

    @Test
    void allowsReadsOfAllowlistedResources() {
        PolicyEngine engine = new PolicyEngine(readOnlyProfile(), AuditSink.noop());

        Decision decision = engine.evaluate(PolicyRequest.read("sql.query", List.of("customers", "orders"), 0));

        assertThat(decision.allowed()).isTrue();
        assertThat(decision.rule()).isEqualTo("allow-read");
        assertThat(decision.effective().maxRows()).isEqualTo(200);
    }

    @Test
    void deniesUnlistedResourceAndTellsTheModelWhatIsVisible() {
        PolicyEngine engine = new PolicyEngine(readOnlyProfile(), AuditSink.noop());

        Decision decision =
                engine.evaluate(PolicyRequest.read("sql.query", List.of("orders", "internal_audit"), 0));

        assertThat(decision.allowed()).isFalse();
        assertThat(decision.rule()).isEqualTo("allow-read");
        // The recovery path has to be in the message, not just in the server log.
        assertThat(decision.reason())
                .contains("internal_audit")
                .contains("customers", "order_items", "orders");
        assertThat(decision.detail()).containsEntry("denied", "internal_audit");
    }

    @Test
    void deniesEveryWriteWhenModeIsReadOnly() {
        PolicyEngine engine = new PolicyEngine(readOnlyProfile(), AuditSink.noop());

        Decision decision = engine.evaluate(PolicyRequest.write("sql.execute", List.of("orders")));

        assertThat(decision.allowed()).isFalse();
        assertThat(decision.rule()).isEqualTo("mode");
        // The model must understand this is not something it can talk its way past.
        assertThat(decision.reason()).contains("cannot be changed from a tool call");
    }

    @Test
    void writeModeAloneStillDeniesAWriteToAnUnlistedTable() {
        PolicyProfile profile = PolicyProfile.builder("orders-db")
                .mode(AccessMode.READ_WRITE)
                .allowRead("customers", "orders")
                .allowWrite("order_items")
                .build();
        PolicyEngine engine = new PolicyEngine(profile, AuditSink.noop());

        Decision decision = engine.evaluate(PolicyRequest.write("sql.execute", List.of("orders")));

        assertThat(decision.allowed()).isFalse();
        assertThat(decision.rule()).isEqualTo("allow-write");
        assertThat(decision.reason()).contains("order_items");
    }

    @Test
    void readAllowlistDoesNotGrantWriteToTheSameTable() {
        PolicyProfile profile = PolicyProfile.builder("orders-db")
                .mode(AccessMode.READ_WRITE)
                .allowRead("orders")
                .allowWrite("order_items")
                .build();
        PolicyEngine engine = new PolicyEngine(profile, AuditSink.noop());

        assertThat(engine.evaluate(PolicyRequest.read("sql.query", List.of("orders"), 0)).allowed())
                .isTrue();
        assertThat(engine.evaluate(PolicyRequest.write("sql.execute", List.of("orders"))).allowed())
                .isFalse();
    }

    @Test
    void wildcardWriteAllowlistIsRejectedAtConfigLoad() {
        // A blanket write grant is never a considered decision — see ADR 003.
        assertThatThrownBy(() -> PolicyProfile.builder("orders-db").mode(AccessMode.READ_WRITE).allowWrite("*"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wildcard write allowlist is not permitted");
    }

    @Test
    void writeAllowlistWithoutWriteModeFailsFastRatherThanBeingIgnored() {
        assertThatThrownBy(() -> PolicyProfile.builder("orders-db")
                        .mode(AccessMode.READ_ONLY)
                        .allowRead("orders")
                        .allowWrite("order_items")
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("set mode to READ_WRITE");
    }

    @Test
    void clientMayRequestFewerRowsButNeverMore() {
        PolicyEngine engine = new PolicyEngine(readOnlyProfile(), AuditSink.noop());

        assertThat(engine.evaluate(PolicyRequest.read("sql.query", List.of("orders"), 50))
                        .effective()
                        .maxRows())
                .isEqualTo(50);
        assertThat(engine.evaluate(PolicyRequest.read("sql.query", List.of("orders"), 10_000))
                        .effective()
                        .maxRows())
                .isEqualTo(200);
    }

    @Test
    void wildcardReadPatternsMatchFamiliesButNotAcrossASeparator() {
        PolicyProfile profile = PolicyProfile.builder("orders-kafka")
                .allowRead("orders.*")
                .build();

        assertThat(profile.isReadable("orders.created")).isTrue();
        assertThat(profile.isReadable("orders.shipped")).isTrue();
        // The '.' in the pattern is a literal, not a regex any-char — otherwise "ordersXfoo"
        // would match and the allowlist would be wider than what the operator wrote.
        assertThat(profile.isReadable("ordersXcreated")).isFalse();
        assertThat(profile.isReadable("payments.created")).isFalse();
    }

    @Test
    void dryRunReturnsThePlanAndNeverRunsTheCall() {
        PolicyProfile profile = PolicyProfile.builder("orders-db")
                .mode(AccessMode.DRY_RUN)
                .allowRead("orders")
                .build();
        AuditSink.RecordingAuditSink audit = AuditSink.recording();
        PolicyEngine engine = new PolicyEngine(profile, audit);
        boolean[] executed = {false};

        ToolOutcome outcome = engine.guardRead("sql.query", List.of("orders"), 0, limits -> {
            executed[0] = true;
            return ToolOutcome.success(Map.of(), "should never happen");
        });

        assertThat(executed[0]).isFalse();
        assertThat(outcome.error()).isFalse();
        assertThat(outcome.summary()).contains("dry-run").contains("Nothing was executed");
        // Dry-run calls are still audited — "what did we plan" is an operator question too.
        assertThat(audit.records()).hasSize(1);
    }

    @Test
    void everyCallIsAuditedWithTheRuleThatDecided() {
        AuditSink.RecordingAuditSink audit = AuditSink.recording();
        PolicyEngine engine = new PolicyEngine(readOnlyProfile(), audit);

        engine.guardRead("sql.query", List.of("orders"), 0, limits -> ToolOutcome.success(Map.of(), "ok", 3));
        engine.guardRead("sql.query", List.of("internal_audit"), 0, limits -> ToolOutcome.success(Map.of(), "no"));

        assertThat(audit.records()).hasSize(2);
        AuditRecord allowed = audit.records().get(0);
        assertThat(allowed.allowed()).isTrue();
        assertThat(allowed.rowsReturned()).isEqualTo(3);
        assertThat(allowed.resources()).containsExactly("orders");

        AuditRecord denied = audit.denials().get(0);
        assertThat(denied.rule()).isEqualTo("allow-read");
        assertThat(denied.reason()).contains("internal_audit");
    }

    @Test
    void aDeniedCallNeverReachesTheGuardedBody() {
        PolicyEngine engine = new PolicyEngine(readOnlyProfile(), AuditSink.noop());
        boolean[] executed = {false};

        ToolOutcome outcome = engine.guardRead("sql.query", List.of("internal_audit"), 0, limits -> {
            executed[0] = true;
            return ToolOutcome.success(Map.of(), "leaked");
        });

        assertThat(executed[0]).isFalse();
        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).doesNotContain("leaked");
    }

    @Test
    void backendFailureIsAuditedAsAllowedAndReportedWithoutInternalDetail() {
        AuditSink.RecordingAuditSink audit = AuditSink.recording();
        PolicyEngine engine = new PolicyEngine(readOnlyProfile(), audit);

        assertThatThrownBy(() -> engine.guardRead("sql.query", List.of("orders"), 0, limits -> {
                    throw new IllegalStateException("jdbc:postgresql://secret-host/db password=hunter2");
                }))
                .isInstanceOf(PolicyEngine.BackendCallException.class)
                .hasMessageNotContaining("hunter2")
                .hasMessageContaining("orders-db");

        // The call did reach the backend, so the audit must not record it as blocked.
        assertThat(audit.records()).hasSize(1);
        assertThat(audit.records().get(0).allowed()).isTrue();
    }
}
