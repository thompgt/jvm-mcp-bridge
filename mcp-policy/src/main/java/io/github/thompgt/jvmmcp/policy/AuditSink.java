package io.github.thompgt.jvmmcp.policy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
     * Appends JSON lines to a file, rotating it at {@link FileAuditSink#DEFAULT_MAX_BYTES}.
     *
     * <p>Writes are synchronised and flushed per record. That is slower than buffering, and
     * deliberately so: an audit log that loses its last entries when the process dies fails
     * at exactly the moment it is needed. The cost is bounded by the tool call it accompanies,
     * which is already a network round trip to a database.
     */
    static AuditSink file(Path path) {
        return new FileAuditSink(path, FileAuditSink.DEFAULT_MAX_BYTES, FileAuditSink.DEFAULT_KEEP);
    }

    /**
     * As {@link #file(Path)}, with the rotation bounds stated.
     *
     * @param maxBytes size at which the current file is rolled aside. Not a hard cap on the
     *     file: the record that crosses the line is written whole, because half a JSON line is
     *     not an audit record.
     * @param keep how many rolled generations to keep. The oldest is deleted, so the audit log
     *     occupies at most {@code maxBytes * (keep + 1)} however long the process runs.
     */
    static AuditSink file(Path path, long maxBytes, int keep) {
        return new FileAuditSink(path, maxBytes, keep);
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

    /**
     * File-backed sink; see {@link AuditSink#file(Path)}.
     *
     * <p>The sink rotates the file itself rather than leaving it to logrotate. It holds one
     * appending handle for the life of the process, and an external rotator cannot work around
     * that: renaming the file on Linux leaves this handle writing to the renamed inode, so the
     * fresh file stays empty forever, and on Windows the rename fails outright while the handle
     * is open. Either way an audit log nobody can rotate is one that eventually fills a disk —
     * and the disk it fills is the one the bridge is running on.
     */
    final class FileAuditSink implements AuditSink, AutoCloseable {
        private static final Logger log = LoggerFactory.getLogger(FileAuditSink.class);

        /** 64 MB is a few million tool calls: long enough to investigate, small enough to move. */
        public static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

        /** Generations kept beside the live file, so the default ceiling is 640 MB. */
        public static final int DEFAULT_KEEP = 9;

        private final Path path;
        private final long maxBytes;
        private final int keep;

        private final Object lock = new Object();
        private Writer writer;
        private long written;

        /**
         * Size at which to try rotating. Normally {@code maxBytes}; pushed out by another
         * {@code maxBytes} when a rotation fails, so a directory that cannot be written to
         * costs one failed attempt per generation rather than one per audit record.
         */
        private long rotateAt;

        FileAuditSink(Path path, long maxBytes, int keep) {
            if (maxBytes <= 0) {
                throw new IllegalArgumentException("audit max-size must be positive, was " + maxBytes);
            }
            if (keep < 0) {
                throw new IllegalArgumentException("audit max-history cannot be negative, was " + keep);
            }
            this.path = path.toAbsolutePath();
            this.maxBytes = maxBytes;
            this.keep = keep;
            try {
                Path parent = this.path.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                // Appending to an existing log: start the count from what is already there, or
                // a restarted process would need another full maxBytes before it rotated.
                this.written = Files.exists(this.path) ? Files.size(this.path) : 0L;
                this.rotateAt = maxBytes;
                this.writer = open(this.path);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot open audit log at " + path, e);
            }
        }

        private static Writer open(Path path) throws IOException {
            return Files.newBufferedWriter(
                    path, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }

        @Override
        public void record(AuditRecord record) {
            String line = record.toJsonLine() + System.lineSeparator();
            synchronized (lock) {
                try {
                    writer.write(line);
                    writer.flush();
                    written += line.getBytes(StandardCharsets.UTF_8).length;
                    if (written >= rotateAt) {
                        rotate();
                    }
                } catch (IOException e) {
                    // A failed audit write must not take down the tool call, but it must be
                    // loud: an unaudited call is exactly what the log exists to prevent.
                    log.error("failed to write audit record for tool {}", record.tool(), e);
                }
            }
        }

        /**
         * Rolls {@code audit.log} to {@code audit.log.1}, shifting the older generations up and
         * dropping the oldest.
         *
         * <p>A rotation that fails must not lose the record that triggered it or stop the ones
         * after it, so a failure here is logged and the existing file keeps being appended to.
         * A log that grew past its bound is worse than one that did not; a log that stopped
         * recording is worse than both.
         */
        private void rotate() {
            try {
                writer.close();
                if (keep == 0) {
                    Files.deleteIfExists(path);
                } else {
                    Files.deleteIfExists(generation(keep));
                    for (int i = keep - 1; i >= 1; i--) {
                        Path from = generation(i);
                        if (Files.exists(from)) {
                            Files.move(from, generation(i + 1), StandardCopyOption.REPLACE_EXISTING);
                        }
                    }
                    Files.move(path, generation(1), StandardCopyOption.REPLACE_EXISTING);
                }
                writer = open(path);
                written = 0L;
                rotateAt = maxBytes;
                log.info("rotated the audit log at {} bytes", maxBytes);
            } catch (IOException e) {
                log.error("failed to rotate the audit log at {}; continuing to append", path, e);
                rotateAt = written + maxBytes;
                try {
                    writer = open(path);
                } catch (IOException reopen) {
                    // Nothing left to write to. Say so once, here, rather than once per call.
                    log.error("cannot reopen the audit log at {}; records will be lost", path, reopen);
                }
            }
        }

        private Path generation(int index) {
            return path.resolveSibling(path.getFileName() + "." + index);
        }

        @Override
        public void close() throws IOException {
            synchronized (lock) {
                writer.close();
            }
        }
    }
}
