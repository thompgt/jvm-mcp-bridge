package io.github.thompgt.jvmmcp.jvm;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Against this JVM's real collectors, which is the only way to exercise the part that matters:
 * the platform reports counters that only mean something as a difference, and a fixture that
 * returned steady numbers would let a broken delta pass.
 */
class MemoryToolsTest {

    private JvmAdapter adapter;

    @BeforeAll
    static void ensureSomethingHasBeenCollected() {
        // Without at least one collection, every pool's collection usage is null and the field
        // this tool is built around is absent everywhere — a state real JVMs leave within
        // milliseconds of starting and a fresh test JVM can genuinely still be in.
        System.gc();
    }

    @AfterEach
    void tearDown() {
        if (adapter != null) {
            adapter.close();
        }
    }

    private static PolicyProfile.Builder policy() {
        return PolicyProfile.builder("self")
                .allowRead("java.lang:*")
                .maxRows(100)
                .timeout(Duration.ofSeconds(15));
    }

    private ToolOutcome call(PolicyProfile profile, Map<String, Object> arguments) {
        if (adapter == null) {
            adapter = new JvmAdapter(JvmTargetHandle.embedded("self"), profile, AuditSink.noop());
        }
        return tool().call(arguments);
    }

    private BridgeTool tool() {
        return adapter.tools().stream()
                .filter(t -> t.descriptor().name().equals("jvm.memory"))
                .findFirst()
                .orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> structured(ToolOutcome outcome) {
        return (Map<String, Object>) outcome.structured();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rows(Object value) {
        return (List<Map<String, Object>>) value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    @Test
    void reportsHeapPoolsAndCollectors() {
        Map<String, Object> result = structured(call(policy().build(), Map.of()));

        assertThat(map(result.get("heap"))).containsKeys("usedBytes", "committedBytes", "maxBytes", "percentOfMax");
        assertThat((Long) map(result.get("heap")).get("usedBytes")).isPositive();
        assertThat(rows(result.get("pools"))).isNotEmpty();
        assertThat(rows(result.get("collectors"))).isNotEmpty();
        assertThat((Long) result.get("uptimeMillis")).isPositive();
    }

    /** The pool the tool exists to point at reports live data, not used-including-garbage. */
    @Test
    void poolsReportUsageAfterTheLastCollection() {
        Map<String, Object> result = structured(call(policy().build(), Map.of()));

        assertThat(rows(result.get("pools")))
                .anyMatch(pool -> (Long) pool.get("afterLastGcBytes") >= 0)
                .allSatisfy(pool -> assertThat(pool).containsKeys("afterLastGcBytes", "afterLastGcPercentOfMax"));
    }

    /**
     * A first reading has nothing to subtract, and says so rather than presenting totals since
     * JVM start as if they described now.
     */
    @Test
    void aFirstReadingHasNoIntervalAndSaysWhy() {
        ToolOutcome outcome = call(policy().build(), Map.of());

        assertThat(structured(outcome)).doesNotContainKey("interval");
        assertThat(outcome.summary())
                .contains("No interval")
                .contains("totals since the JVM started")
                .contains("sample_seconds");
    }

    @Test
    void aSecondCallReportsTheChangeSinceTheFirst() throws Exception {
        call(policy().build(), Map.of());
        Thread.sleep(50);
        ToolOutcome outcome = tool().call(Map.of());

        Map<String, Object> interval = map(structured(outcome).get("interval"));
        assertThat(interval.get("source")).isEqualTo("previous call");
        assertThat((Long) interval.get("millis")).isPositive();
        assertThat(rows(structured(outcome).get("collectors")))
                .allSatisfy(c -> assertThat((Long) c.get("collectionsInInterval")).isNotNegative());
        assertThat(outcome.summary()).contains("Change measured over");
    }

    @Test
    void sampleSecondsTakesTwoReadingsWithinTheCall() {
        ToolOutcome outcome = call(policy().build(), Map.of("sample_seconds", 1));

        Map<String, Object> interval = map(structured(outcome).get("interval"));
        assertThat(interval.get("source")).isEqualTo("sampled in call");
        assertThat((Long) interval.get("millis")).isGreaterThanOrEqualTo(900L);
        assertThat((Integer) interval.get("gcPercentOfInterval")).isNotNegative();
    }

    /** Sampling inside a call it cannot finish is refused with the alternative, not attempted. */
    @Test
    void sampleSecondsIsRefusedWhenTheCallTimeoutLeavesNoRoom() {
        ToolOutcome outcome = call(policy().timeout(Duration.ofSeconds(2)).build(), Map.of("sample_seconds", 10));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("call timeout").contains("Call this tool twice instead");
    }

    @Test
    void sampleSecondsIsClampedToWhatTheTimeoutAllows() {
        ToolOutcome outcome =
                call(policy().timeout(Duration.ofSeconds(3)).build(), Map.of("sample_seconds", 25));

        assertThat(outcome.error()).isFalse();
        assertThat(outcome.summary()).contains("reduced to 1s");
    }

    @Test
    void theAssessmentNamesTheFullestPool() {
        ToolOutcome outcome = call(policy().build(), Map.of());

        String assessment = (String) structured(outcome).get("assessment");
        assertThat(assessment).contains("Fullest pool after its last collection");
        assertThat(rows(structured(outcome).get("pools")))
                .anyMatch(pool -> assessment.contains((String) pool.get("name")));
    }

    /** A profile narrowed to specific beans narrows this output too, and the gap is counted. */
    @Test
    void poolsOutsideTheAllowlistAreExcludedAndCounted() {
        ToolOutcome outcome = call(
                PolicyProfile.builder("self")
                        .allowRead("java.lang:type=Memory", "java.lang:type=GarbageCollector,*")
                        .build(),
                Map.of());

        Map<String, Object> result = structured(outcome);
        assertThat(rows(result.get("pools"))).isEmpty();
        assertThat((Integer) result.get("hiddenByPolicy")).isPositive();
        assertThat(outcome.summary()).contains("excluded by the MBean allowlist");
    }

    @Test
    void theToolIsDeniedWhenTheMemoryBeanIsNotAllowlisted() {
        ToolOutcome outcome = call(
                PolicyProfile.builder("self").allowRead("java.nio:*").build(), Map.of());

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("java.nio:*");
    }
}
