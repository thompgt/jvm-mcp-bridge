package io.github.thompgt.jvmmcp.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The audit file has to have a ceiling.
 *
 * <p>It cannot be rotated from outside — the sink holds one appending handle for the life of
 * the process, which on Linux keeps writing to a renamed inode and on Windows blocks the rename
 * — so a sink that never rotates is a file that grows until the disk the bridge runs on is
 * full.
 */
class FileAuditSinkTest {

    @TempDir
    Path dir;

    @Test
    @DisplayName("the live file is rolled aside once it passes the bound")
    void rotatesAtTheConfiguredSize() throws IOException {
        Path log = dir.resolve("audit.log");

        // Ten records against a 400-byte bound is about five rotations, comfortably inside the
        // nine generations kept — so nothing should have been dropped yet.
        try (AuditSink.FileAuditSink sink =
                (AuditSink.FileAuditSink) AuditSink.file(log, 400, 9)) {
            for (int i = 0; i < 10; i++) {
                sink.record(call("tool-" + i));
            }
        }

        // Every record is present across the generations: rotation moves records, never drops
        // the one that crossed the line.
        String everything = concatenated(log, 9);
        assertThat(everything).contains("tool-0").contains("tool-9");
        assertThat(Files.exists(dir.resolve("audit.log.1"))).isTrue();
    }

    @Test
    @DisplayName("the oldest generation is dropped, so the total is bounded")
    void keepsOnlyTheConfiguredHistory() throws IOException {
        Path log = dir.resolve("audit.log");

        try (AuditSink.FileAuditSink sink =
                (AuditSink.FileAuditSink) AuditSink.file(log, 300, 2)) {
            for (int i = 0; i < 200; i++) {
                sink.record(call("tool-" + i));
            }
        }

        assertThat(Files.exists(dir.resolve("audit.log.3"))).isFalse();
        assertThat(Files.exists(dir.resolve("audit.log.4"))).isFalse();

        long total = Files.size(log) + Files.size(dir.resolve("audit.log.1")) + Files.size(dir.resolve("audit.log.2"));
        // The record that crosses the bound is written whole, so each file may overshoot by one
        // line — but the total is a multiple of the bound, not a function of uptime.
        assertThat(total).isLessThan(300L * 3 + 1024);

        // The newest records survived; the oldest are what was dropped.
        assertThat(Files.readString(log, StandardCharsets.UTF_8)
                        + Files.readString(dir.resolve("audit.log.1"), StandardCharsets.UTF_8)
                        + Files.readString(dir.resolve("audit.log.2"), StandardCharsets.UTF_8))
                .contains("tool-199")
                .doesNotContain("\"tool\":\"tool-0\"");
    }

    @Test
    @DisplayName("a restart counts what is already on disk")
    void appendingToAnExistingLogDoesNotResetTheBudget() throws IOException {
        Path log = dir.resolve("audit.log");
        Files.writeString(log, "x".repeat(500) + System.lineSeparator(), StandardCharsets.UTF_8);

        try (AuditSink.FileAuditSink sink =
                (AuditSink.FileAuditSink) AuditSink.file(log, 400, 2)) {
            sink.record(call("after-restart"));
        }

        // Without counting the existing 500 bytes, a process that restarts often would never
        // rotate at all. One record was enough to cross a bound that was already crossed.
        assertThat(Files.readString(dir.resolve("audit.log.1"), StandardCharsets.UTF_8))
                .contains("after-restart");
        assertThat(Files.size(log)).isZero();
    }

    private static String concatenated(Path log, int keep) throws IOException {
        StringBuilder sb = new StringBuilder(Files.readString(log, StandardCharsets.UTF_8));
        for (int i = 1; i <= keep; i++) {
            Path generation = log.resolveSibling(log.getFileName() + "." + i);
            if (Files.exists(generation)) {
                sb.append(Files.readString(generation, StandardCharsets.UTF_8));
            }
        }
        return sb.toString();
    }

    private static AuditRecord call(String tool) {
        return new AuditRecord(
                Instant.parse("2026-01-01T00:00:00Z"),
                "analyst",
                "orders-db",
                tool,
                List.of("customers"),
                true,
                "allow-tables",
                "",
                1,
                7);
    }
}
