package io.github.thompgt.jvmmcp.jvm;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Real threads, in the JVM running the test.
 *
 * <p>The fixture parks a known number of threads at a known frame and, separately, deadlocks a
 * pair on two monitors. Both are things a mock cannot produce honestly: the grouping is only
 * worth anything if it collapses threads the JVM genuinely reports as being in the same place,
 * and the deadlock detector is the JVM's, so the only way to test the reporting is to give it a
 * deadlock to find.
 *
 * <p>Every fixture thread is a daemon. The deadlocked pair never becomes runnable again by
 * construction, and a non-daemon one would keep the test JVM alive after the suite finished.
 */
class ThreadToolsTest {

    private static final int PARKED_THREADS = 5;
    private static final String PARK_MARKER = "jvm-mcp-test-parked";

    private static final CountDownLatch release = new CountDownLatch(1);
    private static final Object lockA = new Object();
    private static final Object lockB = new Object();

    private JvmAdapter adapter;

    @BeforeAll
    static void startFixtureThreads() throws Exception {
        for (int i = 0; i < PARKED_THREADS; i++) {
            Thread thread = new Thread(ThreadToolsTest::parkUntilReleased, PARK_MARKER + "-" + i);
            thread.setDaemon(true);
            thread.start();
        }

        CountDownLatch bothHoldOne = new CountDownLatch(2);
        startDeadlockHalf("jvm-mcp-test-deadlock-a", lockA, lockB, bothHoldOne);
        startDeadlockHalf("jvm-mcp-test-deadlock-b", lockB, lockA, bothHoldOne);
        bothHoldOne.await(5, TimeUnit.SECONDS);

        // The deadlock exists only once both threads have moved on to the second monitor, which
        // is after the latch above rather than at it. Poll for the JVM's own verdict instead of
        // sleeping a guessed interval.
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline
                && java.lang.management.ManagementFactory.getThreadMXBean().findDeadlockedThreads() == null) {
            Thread.sleep(25);
        }
    }

    /** Shared frame for the parked group: every one of them stops here. */
    private static void parkUntilReleased() {
        try {
            release.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void startDeadlockHalf(String name, Object first, Object second, CountDownLatch holdingOne) {
        Thread thread = new Thread(
                () -> {
                    synchronized (first) {
                        holdingOne.countDown();
                        try {
                            holdingOne.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        synchronized (second) {
                            throw new AssertionError("unreachable: this pair is meant to deadlock");
                        }
                    }
                },
                name);
        thread.setDaemon(true);
        thread.start();
    }

    @AfterAll
    static void releaseParkedThreads() {
        release.countDown();
    }

    @AfterEach
    void tearDown() {
        if (adapter != null) {
            adapter.close();
            adapter = null;
        }
    }

    private ToolOutcome call(PolicyProfile profile, Map<String, Object> arguments) {
        adapter = new JvmAdapter(JvmTargetHandle.embedded("self"), profile, AuditSink.noop());
        BridgeTool tool = adapter.tools().stream()
                .filter(t -> t.descriptor().name().equals("jvm.threads"))
                .findFirst()
                .orElseThrow();
        return tool.call(arguments);
    }

    private static PolicyProfile.Builder policy() {
        return PolicyProfile.builder("self")
                .allowRead("java.lang:*")
                .maxRows(100)
                .timeout(Duration.ofSeconds(20));
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
    private static Map<String, Integer> counts(Object value) {
        return (Map<String, Integer>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<String> strings(Object value) {
        return (List<String>) value;
    }

    @Test
    void reportsCountsAndAStateHistogram() {
        Map<String, Object> result = structured(call(policy().build(), Map.of()));

        assertThat((Integer) result.get("threadCount")).isPositive();
        assertThat((Integer) result.get("peakCount")).isGreaterThanOrEqualTo((Integer) result.get("threadCount"));
        assertThat(counts(result.get("states"))).containsKey("RUNNABLE");
        assertThat(counts(result.get("states")).values().stream().mapToInt(Integer::intValue).sum())
                .isGreaterThanOrEqualTo(PARKED_THREADS);
    }

    /**
     * The property the tool exists for: threads stopped in one place are one row with a count,
     * not N rows to be compared against each other.
     */
    @Test
    void threadsAtTheSameFrameCollapseIntoOneGroup() {
        Map<String, Object> result = structured(call(policy().build(), Map.of("name_contains", PARK_MARKER)));

        List<Map<String, Object>> groups = rows(result.get("groups"));
        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).get("threads")).isEqualTo(PARKED_THREADS);
        assertThat(strings(groups.get(0).get("signature")))
                .anyMatch(frame -> frame.contains("ThreadToolsTest.parkUntilReleased"));
        assertThat(result.get("matched")).isEqualTo(PARKED_THREADS);
    }

    @Test
    void samplesCarryFullFramesWithLineNumbers() {
        List<Map<String, Object>> groups =
                rows(structured(call(policy().build(), Map.of("name_contains", PARK_MARKER))).get("groups"));

        List<Map<String, Object>> samples = rows(groups.get(0).get("samples"));
        assertThat(samples).hasSize(3);
        assertThat(samples).allSatisfy(sample -> {
            assertThat((String) sample.get("name")).startsWith(PARK_MARKER);
            assertThat(strings(sample.get("stack"))).anyMatch(frame -> frame.contains("ThreadToolsTest.java:"));
        });
    }

    @Test
    void nameFilterExcludesEverythingElse() {
        Map<String, Object> result = structured(call(policy().build(), Map.of("name_contains", PARK_MARKER)));

        assertThat(rows(result.get("groups")))
                .flatExtracting(group -> rows(group.get("samples")))
                .allSatisfy(sample -> assertThat((String) sample.get("name")).contains(PARK_MARKER));
    }

    @Test
    void stateFilterNarrowsToOneState() {
        Map<String, Object> result = structured(call(policy().build(), Map.of("state", "waiting")));

        assertThat(rows(result.get("groups")))
                .isNotEmpty()
                .allSatisfy(group -> assertThat(counts(group.get("states"))).containsOnlyKeys("WAITING"));
    }

    @Test
    void anUnknownStateNamesTheRealOnes() {
        ToolOutcome outcome = call(policy().build(), Map.of("state", "STUCK"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("TIMED_WAITING").contains("BLOCKED");
    }

    /**
     * The one judgement this tool makes. It is the JVM's own detector, so a test that finds the
     * deadlock proves the reporting rather than the detection.
     */
    @Test
    void aRealDeadlockIsReportedAndCalledOne() {
        ToolOutcome outcome = call(policy().build(), Map.of());
        Map<String, Object> result = structured(outcome);

        List<Map<String, Object>> deadlocks = rows(result.get("deadlocks"));
        assertThat(deadlocks).hasSize(1);
        assertThat(strings(deadlocks.get(0).get("threads")))
                .anyMatch(name -> name.contains("jvm-mcp-test-deadlock-a"))
                .anyMatch(name -> name.contains("jvm-mcp-test-deadlock-b"));
        assertThat((String) deadlocks.get(0).get("description")).contains("held by");
        assertThat((String) result.get("assessment")).startsWith("DEADLOCK");
        assertThat(outcome.summary()).contains("Deadlock cycle");
    }

    @Test
    void blockedThreadsAreCalledOutInTheAssessment() {
        // The deadlocked pair is BLOCKED on a monitor, so this holds for the same fixture.
        String assessment = (String) structured(call(policy().build(), Map.of())).get("assessment");

        assertThat(assessment).contains("BLOCKED").contains("waiting for a monitor");
    }

    @Test
    void tooManyGroupsIsTruncatedWithAdviceToFilter() {
        ToolOutcome outcome = call(policy().maxRows(1).build(), Map.of());

        Map<String, Object> result = structured(outcome);
        assertThat(rows(result.get("groups"))).hasSize(1);
        assertThat(result.get("truncated")).isEqualTo(true);
        assertThat((Integer) result.get("grouped")).isLessThan((Integer) result.get("matched"));
        assertThat(outcome.summary()).contains("Filter by state or name");
    }

    @Test
    void aFilterThatMatchesNothingSaysSoRatherThanLookingEmpty() {
        ToolOutcome outcome = call(policy().build(), Map.of("name_contains", "no-such-thread-anywhere"));

        assertThat(outcome.error()).isFalse();
        assertThat(rows(structured(outcome).get("groups"))).isEmpty();
        assertThat(outcome.summary()).contains("No thread matched the filters");
    }

    @Test
    void isDeniedWhenTheThreadingBeanIsNotAllowlisted() {
        ToolOutcome outcome =
                call(PolicyProfile.builder("self").allowRead("java.lang:type=Memory").build(), Map.of());

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("java.lang:type=memory");
    }
}
