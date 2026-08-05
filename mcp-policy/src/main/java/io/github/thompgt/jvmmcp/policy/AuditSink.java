package io.github.thompgt.jvmmcp.policy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Where audit records go. */
@FunctionalInterface
public interface AuditSink {

    void record(AuditRecord record);

    /** Discards records. For unit tests only — never wire this into a running server. */
    static AuditSink noop() {
        return record -> {};
    }

    /** Writes to the application log at INFO. The default when no audit file is configured. */
    static AuditSink logging() {
        Logger log = LoggerFactory.getLogger("jvm-mcp-bridge.audit");
        return record -> log.info("{}", record.toJsonLine());
    }

    /**
     * Appends JSON lines to a file.
     *
     * <p>Writes are synchronised and flushed per record. That is slower than buffering, and
     * deliberately so: an audit log that loses its last entries when the process dies fails
     * at exactly the moment it is needed. The cost is bounded by the tool call it accompanies,
     * which is already a network round trip to a database.
     */
    static AuditSink file(Path path) {
        return new FileAuditSink(path);
    }

    /** Records in memory. For tests that assert on what was audited. */
    static RecordingAuditSink recording() {
        return new RecordingAuditSink();
    }

    /** An {@link AuditSink} that keeps everything it is given. */
    final class RecordingAuditSink implements AuditSink {
        private final List<AuditRecord> records = new ArrayList<>();

        @Override
        public synchronized void record(AuditRecord record) {
            records.add(record);
        }

        public synchronized List<AuditRecord> records() {
            return List.copyOf(records);
        }

        public synchronized List<AuditRecord> denials() {
            return records.stream().filter(r -> !r.allowed()).toList();
        }
    }

    /** File-backed sink; see {@link AuditSink#file(Path)}. */
    final class FileAuditSink implements AuditSink, AutoCloseable {
        private static final Logger log = LoggerFactory.getLogger(FileAuditSink.class);

        private final Writer writer;

        FileAuditSink(Path path) {
            try {
                Path parent = path.toAbsolutePath().getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                this.writer =
                        Files.newBufferedWriter(
                                path,
                                StandardCharsets.UTF_8,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot open audit log at " + path, e);
            }
        }

        @Override
        public void record(AuditRecord record) {
            synchronized (writer) {
                try {
                    writer.write(record.toJsonLine());
                    writer.write(System.lineSeparator());
                    writer.flush();
                } catch (IOException e) {
                    // A failed audit write must not take down the tool call, but it must be
                    // loud: an unaudited call is exactly what the log exists to prevent.
                    log.error("failed to write audit record for tool {}", record.tool(), e);
                }
            }
        }

        @Override
        public void close() throws IOException {
            synchronized (writer) {
                writer.close();
            }
        }
    }
}
