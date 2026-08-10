package io.github.thompgt.jvmmcp.jvm;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A real Flight Recorder run against the JVM running the test.
 *
 * <p>Slow by nature — the call blocks for the recording — so the durations here are the shortest
 * that reliably produce samples. There is no way to test this without recording something: the
 * summary is derived from a parsed {@code .jfr}, and a fixture that supplied pre-baked events
 * would test the arithmetic while leaving the part that actually breaks — starting, streaming and
 * cleaning up a recording on a live JVM — uncovered.
 */
class JfrToolsTest {

    private JvmAdapter adapter;

    @AfterEach
    void tearDown() {
        if (adapter != null) {
            adapter.close();
            adapter = null;
        }
    }

    private static PolicyProfile.Builder policy() {
        return PolicyProfile.builder("self")
                .allowRead("java.lang:*", JfrTools.JFR_MBEAN)
                .maxRows(20)
                .timeout(Duration.ofSeconds(10));
    }

    private BridgeTool tool(PolicyProfile profile, Duration jfrMax) {
        adapter = new JvmAdapter(
                new JvmTargetHandle("self", null, Duration.ofSeconds(5), null, null, jfrMax),
                profile,
                AuditSink.noop());
        return adapter.tools().stream()
                .filter(t -> t.descriptor().name().equals("jvm.jfr_snapshot"))
                .findFirst()
                .orElseThrow();
    }

    private BridgeTool tool() {
        return tool(policy().build(), Duration.ofSeconds(30));
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
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    /** Something for the profiler to find, named distinctively enough to assert on. */
    private static AutoCloseable burnCpu() {
        AtomicBoolean running = new AtomicBoolean(true);
        Thread thread = new Thread(() -> spinUntilStopped(running), "jvm-mcp-test-burn");
        thread.setDaemon(true);
        thread.start();
        return () -> {
            running.set(false);
            thread.join(2_000);
        };
    }

    private static void spinUntilStopped(AtomicBoolean running) {
        long accumulator = 0;
        while (running.get()) {
            for (int i = 0; i < 100_000; i++) {
                accumulator += i * 31L;
            }
            if (accumulator == Long.MIN_VALUE) {
                // Never true; present so the loop above cannot be optimised away entirely.
                throw new IllegalStateException();
            }
        }
    }

    @Test
    void recordsAndSummarisesRatherThanReturningTheFile() throws Exception {
        try (AutoCloseable burn = burnCpu()) {
            ToolOutcome outcome = tool().call(Map.of("duration_seconds", 3));

            assertThat(outcome.error()).isFalse();
            Map<String, Object> result = structured(outcome);
            assertThat(result.get("durationSeconds")).isEqualTo(3);
            assertThat(result.get("configuration")).isEqualTo("default");
            assertThat((Integer) result.get("eventsParsed")).isPositive();
            assertThat((Integer) result.get("executionSamples")).isPositive();
            assertThat(rows(result.get("hotMethods"))).isNotEmpty();
            assertThat(asMap(result.get("gc"))).containsKeys("pauses", "totalPauseMillis", "longestPauseMillis");

            // The summary is text and structure, never the recording itself.
            assertThat(result).doesNotContainKeys("recording", "jfr", "bytes");
            assertThat(outcome.summary()).contains("Hottest methods").contains("statistical samples");
            assertThat(burn).isNotNull();
        }
    }

    @Test
    void findsTheMethodThatIsActuallyBurningCpu() throws Exception {
        try (AutoCloseable burn = burnCpu()) {
            ToolOutcome outcome = tool().call(Map.of("duration_seconds", 3, "configuration", "profile"));

            assertThat(rows(structured(outcome).get("hotMethods")))
                    .as("the spinning fixture thread should dominate a 3s profile")
                    .anyMatch(row -> String.valueOf(row.get("method")).contains("spinUntilStopped"));
            assertThat(burn).isNotNull();
        }
    }

    @Test
    void durationIsClampedToTheConfiguredMaximum() {
        ToolOutcome outcome = tool(policy().build(), Duration.ofSeconds(2)).call(Map.of("duration_seconds", 600));

        assertThat(structured(outcome).get("durationSeconds")).isEqualTo(2);
    }

    @Test
    void theDeclaredMaximumMatchesTheConfiguredOne() {
        String description = tool(policy().build(), Duration.ofSeconds(7)).descriptor().description();

        assertThat(description).contains("capped at 7");
    }

    /** Two overlapping recordings double an overhead that was accepted once. */
    @Test
    void aSecondConcurrentRecordingIsRefusedRatherThanQueued() throws Exception {
        BridgeTool tool = tool();

        CompletableFuture<ToolOutcome> first =
                CompletableFuture.supplyAsync(() -> tool.call(Map.of("duration_seconds", 4)));
        // Long enough that the first call is certainly inside its recording, short enough to be
        // well within it.
        Thread.sleep(1_000);
        ToolOutcome second = tool.call(Map.of("duration_seconds", 1));

        assertThat(second.error()).isTrue();
        assertThat(second.summary()).contains("already running").contains("refused rather than queued");
        assertThat(first.get().error()).isFalse();
    }

    @Test
    void theTemporaryRecordingIsDeletedAfterwards() throws Exception {
        Path tempDir = Path.of(System.getProperty("java.io.tmpdir"));
        long before = countBridgeRecordings(tempDir);

        tool().call(Map.of("duration_seconds", 2));

        assertThat(countBridgeRecordings(tempDir))
                .as("a profile of a production system must not be left on disk")
                .isEqualTo(before);
    }

    private static long countBridgeRecordings(Path dir) throws Exception {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().startsWith("jvm-mcp-bridge-"))
                    .filter(p -> p.getFileName().toString().endsWith(".jfr"))
                    .count();
        }
    }

    @Test
    void anUnknownConfigurationIsRefusedWithTheTwoThatExist() {
        ToolOutcome outcome = tool().call(Map.of("configuration", "everything"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("'default'").contains("'profile'");
    }

    @Test
    void isDeniedWhenTheFlightRecorderBeanIsNotAllowlisted() {
        ToolOutcome outcome = tool(
                        PolicyProfile.builder("self").allowRead("java.lang:*").build(), Duration.ofSeconds(30))
                .call(Map.of("duration_seconds", 1));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("java.lang:*");
    }

    @Test
    void everyCallIsAudited() {
        AuditSink.RecordingAuditSink audit = AuditSink.recording();
        adapter = new JvmAdapter(
                new JvmTargetHandle("self", null, Duration.ofSeconds(5), null, null, Duration.ofSeconds(30)),
                policy().build(),
                audit);

        adapter.tools().stream()
                .filter(t -> t.descriptor().name().equals("jvm.jfr_snapshot"))
                .findFirst()
                .orElseThrow()
                .call(Map.of("duration_seconds", 1));

        assertThat(audit.records()).hasSize(1);
        assertThat(audit.records().get(0).tool()).isEqualTo("jvm.jfr_snapshot");
        assertThat(audit.records().get(0).allowed()).isTrue();
    }
}
