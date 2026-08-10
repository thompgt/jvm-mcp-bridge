package io.github.thompgt.jvmmcp.jvm;

import io.github.thompgt.jvmmcp.core.Arguments;
import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.Schemas;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.PolicyEngine;
import io.modelcontextprotocol.spec.McpSchema;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordingFile;
import jdk.management.jfr.FlightRecorderMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code jvm.jfr_snapshot} — a short Flight Recorder run, returned as a summary.
 *
 * <p>A {@code .jfr} file is the wrong thing to give a model twice over. It is binary, so it
 * cannot be read at all without the consumer API; and it is enormous — ten seconds of profiling
 * on a busy service is tens of megabytes and hundreds of thousands of events. What someone
 * actually wants from it is four questions, and this tool answers those and returns nothing else:
 * where is the CPU going, what is being allocated, how long are the collections, and what are
 * threads blocking on.
 *
 * <p>This is the most expensive thing in this adapter and the only one that changes the target's
 * behaviour while it runs, so it is bounded in four ways:
 *
 * <ul>
 *   <li><b>Duration</b> is capped by configuration, not by the caller. A model asking for ten
 *       minutes of profiling during an incident is asking for the wrong thing confidently.
 *   <li><b>One at a time.</b> A second concurrent call is refused rather than queued; two
 *       overlapping recordings double an overhead that was accepted once.
 *   <li><b>Size.</b> The recording has a {@code maxSize}, and the stream is read to a temporary
 *       file under a hard byte cap rather than into this process's heap.
 *   <li><b>Cleanup is unconditional.</b> The recording is closed and the temporary file deleted
 *       in a finally, because a leaked recording keeps costing the target after the call that
 *       created it has been forgotten, and a leaked file is a profile of a production system
 *       sitting on disk.
 * </ul>
 *
 * <p>It reaches the target through {@code FlightRecorderMXBean}, which means it works over a JMX
 * connection to another process exactly as it does in-process — the sidecar case is the one that
 * matters, since the JVM worth profiling is rarely the one running the bridge.
 */
final class JfrTools {

    private static final Logger log = LoggerFactory.getLogger(JfrTools.class);

    /** The MBean the tool is guarded on. Allowlisting it is the opt-in for profiling at all. */
    static final String JFR_MBEAN = "jdk.management.jfr:type=FlightRecorder";

    /** Beyond the recording itself: stopping it, streaming it out, and reading it back. */
    private static final Duration TRANSFER_HEADROOM = Duration.ofSeconds(45);

    /** How much longer than the requested window the recording is allowed to run unattended. */
    private static final Duration DEAD_MANS_SWITCH = Duration.ofMinutes(1);

    /** Hard ceiling on the transferred recording, independent of the configured result caps. */
    private static final long MAX_RECORDING_BYTES = 64L * 1024 * 1024;

    /** Events read before the parse gives up and says the result is partial. */
    private static final int MAX_EVENTS = 400_000;

    private JfrTools() {}

    static List<BridgeTool> create(JvmTargetHandle handle, PolicyEngine policy) {
        return List.of(new JfrSnapshotTool(handle, policy));
    }

    static final class JfrSnapshotTool implements BridgeTool {

        private final JvmTargetHandle handle;
        private final PolicyEngine policy;

        /** See the class comment: concurrent recordings are refused, not serialised. */
        private final AtomicBoolean recording = new AtomicBoolean();

        JfrSnapshotTool(JvmTargetHandle handle, PolicyEngine policy) {
            this.handle = handle;
            this.policy = policy;
        }

        private int maxSeconds() {
            return (int) handle.jfrMaxDuration().toSeconds();
        }

        private int defaultSeconds() {
            return Math.min(10, maxSeconds());
        }

        @Override
        public McpSchema.Tool descriptor() {
            Map<String, Object> input = Schemas.object()
                    .optionalInteger(
                            "duration_seconds",
                            "How long to record. Default " + defaultSeconds() + ", capped at "
                                    + maxSeconds() + " by this server's configuration. Longer is"
                                    + " not better — ten seconds of a problem in progress says"
                                    + " more than a minute of an idle service.",
                            1,
                            Math.max(1, maxSeconds()))
                    .optionalString(
                            "configuration",
                            "'default' (about 1% overhead, and enough for hot methods and GC) or"
                                    + " 'profile' (denser sampling and full allocation profiling,"
                                    + " a few percent). Default is 'default': the JVM you most"
                                    + " want to profile is usually the one that can least afford"
                                    + " it.")
                    .build();

            Schemas.ObjectSchema method = Schemas.object()
                    .optionalString("method", "Class and method the sample was taken in.")
                    .optionalInteger("samples", "How many samples landed here.", 0, Integer.MAX_VALUE)
                    .optionalInteger("percent", "Share of all execution samples.", 0, 100)
                    .optionalString("calledFrom", "The most common caller, when there is a clear one.");

            Schemas.ObjectSchema allocation = Schemas.object()
                    .optionalString("type", "Class being allocated.")
                    .optionalInteger("estimatedBytes", "Allocation attributed to it, extrapolated by the JVM.")
                    .optionalInteger("samples", "Allocation samples for this type.", 0, Integer.MAX_VALUE)
                    .optionalString("allocatedIn", "The most common allocating method.");

            Schemas.ObjectSchema pause = Schemas.object()
                    .optionalString("phase", "Collection phase or cause.")
                    .optionalInteger("millis", "How long it took.");

            Schemas.ObjectSchema contention = Schemas.object()
                    .optionalString("monitor", "Class of the object being synchronised on.")
                    .optionalInteger("events", "How many times a thread waited for it.", 0, Integer.MAX_VALUE)
                    .optionalInteger("totalMillis", "Total time threads spent waiting for it.")
                    .optionalInteger("longestMillis", "The longest single wait.")
                    .optionalString("blockedIn", "The most common method waiting on it.");

            Map<String, Object> output = Schemas.object()
                    .optionalInteger("durationSeconds", "How long the recording actually ran.", 0, Integer.MAX_VALUE)
                    .optionalString("configuration", "Which JFR configuration was used.")
                    .optionalInteger("eventsParsed", "Events read out of the recording.", 0, Integer.MAX_VALUE)
                    .optionalInteger("executionSamples", "Total CPU samples, the denominator for percent.", 0,
                            Integer.MAX_VALUE)
                    .arrayOfObjects("hotMethods", "Where CPU samples landed, most first.", method)
                    .arrayOfObjects("allocation", "What was allocated, by type, most first.", allocation)
                    .optionalObject("gc", "count, totalPauseMillis, longestPauseMillis and the longest pauses.")
                    .arrayOfObjects("contention", "Monitors threads waited on, most time first.", contention)
                    .optionalBoolean("truncated", "True when a cap stopped the parse or shortened a list.")
                    .optionalString("assessment", "One line on what the recording points at.")
                    .build();

            return McpSchema.Tool.builder("jvm.jfr_snapshot", input)
                    .title("Profile the JVM briefly with Flight Recorder")
                    .description("Records "
                            + (handle.isEmbedded() ? "this bridge's own JVM" : "'" + handle.name() + "'")
                            + " with Flight Recorder for a few seconds and returns a summary:"
                            + " hottest methods, what is being allocated, GC pause times, and the"
                            + " monitors threads are blocking on.\n\n"
                            + "You get the summary, never the .jfr file — it is binary, and ten"
                            + " seconds of a busy service is tens of megabytes of events that"
                            + " answer the same four questions this result already answers.\n\n"
                            + "This is the most expensive tool here and the only one that changes"
                            + " the target's behaviour while it runs. The call blocks for the whole"
                            + " recording. Record while the problem is happening — a profile of a"
                            + " service that is currently fine says nothing about why it was slow"
                            + " ten minutes ago. One recording at a time; a second call while one"
                            + " is running is refused rather than queued.\n\n"
                            + "Recordings run for " + defaultSeconds() + "s by default and are capped"
                            + " at " + maxSeconds() + "s by this server's configuration. Longer is"
                            + " not better: ten seconds of a problem in progress says more than a"
                            + " minute of an idle service.\n\n"
                            + "Sampling is statistical. A method with three samples out of four"
                            + " hundred is noise, not a finding, and percentages from a short"
                            + " recording of a lightly loaded JVM mean very little.\n\n"
                            + "Nothing here can start a recording that outlives the call, write a"
                            + " file to the target, or dump the heap.")
                    .outputSchema(output)
                    .annotations(McpSchema.ToolAnnotations.builder()
                            .title("Profile the JVM briefly with Flight Recorder")
                            // Read-only in the sense that matters — it changes no application
                            // state — but it is not free, and the description says so plainly
                            // rather than relying on a client to infer cost from a hint.
                            .readOnlyHint(true)
                            .destructiveHint(false)
                            .idempotentHint(false)
                            .openWorldHint(false)
                            .build())
                    .build();
        }

        @Override
        public ToolOutcome call(Map<String, Object> arguments) {
            Arguments args = new Arguments(arguments);
            int requested = args.optionalInt("duration_seconds", defaultSeconds());
            String configuration = args.optionalString("configuration", "default")
                    .trim()
                    .toLowerCase(Locale.ROOT);

            if (!configuration.equals("default") && !configuration.equals("profile")) {
                return ToolOutcome.failure("configuration must be 'default' or 'profile', not '" + configuration
                        + "'. 'default' is about 1% overhead; 'profile' samples harder and adds"
                        + " full allocation profiling.");
            }
            int seconds = Math.max(1, Math.min(requested, maxSeconds()));

            return policy.guardRead("jvm.jfr_snapshot", List.of(JFR_MBEAN), 0, limits -> {
                if (!recording.compareAndSet(false, true)) {
                    return ToolOutcome.failure("a recording is already running against '" + handle.name()
                            + "'. Two overlapping recordings double an overhead that was accepted"
                            + " once, so this one is refused rather than queued. Wait for the other"
                            + " call to return.");
                }
                Path file = null;
                try {
                    // The recording's own bound, not the policy timeout: the policy timeout is
                    // sized for ordinary calls, and a recording that is cut off before it finishes
                    // costs the target the whole overhead and returns nothing for it.
                    Duration bound = Duration.ofSeconds(seconds).plus(TRANSFER_HEADROOM);
                    Path target = Files.createTempFile("jvm-mcp-bridge-", ".jfr");
                    file = target;

                    long bytes = handle.call(bound, connection -> record(connection, seconds, configuration, target));
                    if (bytes <= 0) {
                        return ToolOutcome.failure("the recording on '" + handle.name() + "' produced no data."
                                + " That usually means Flight Recorder is not available on the"
                                + " target — a JVM started with -XX:-FlightRecorder, or one whose"
                                + " jdk.management.jfr module is absent.");
                    }
                    return summarise(file, seconds, configuration, limits.maxRows());
                } finally {
                    recording.set(false);
                    if (file != null) {
                        try {
                            Files.deleteIfExists(file);
                        } catch (IOException e) {
                            // Worth a loud log: this is a profile of a production system.
                            log.error("failed to delete temporary JFR recording {}", file, e);
                        }
                    }
                }
            });
        }

        /**
         * Runs the recording and streams it to {@code target}.
         *
         * @return bytes transferred
         */
        private long record(
                javax.management.MBeanServerConnection connection,
                int seconds,
                String configuration,
                Path target)
                throws Exception {

            FlightRecorderMXBean jfr = ManagementFactory.getPlatformMXBean(connection, FlightRecorderMXBean.class);
            if (jfr == null) {
                return 0;
            }

            long id = jfr.newRecording();
            try {
                jfr.setPredefinedConfiguration(id, configuration);
                jfr.setRecordingOptions(
                        id,
                        Map.of(
                                "name", "jvm-mcp-bridge",
                                // A dead-man's switch, not the normal path — deliberately longer
                                // than the sleep below so the explicit stop is what ends the
                                // recording. Set equal to it, JFR stops the recording itself at
                                // the same instant and the stop below fails on a race with the
                                // JVM. Its purpose is the other case: a bridge that dies
                                // mid-recording leaves one that ends on its own rather than one
                                // that profiles the target until it is restarted.
                                "duration", (seconds + DEAD_MANS_SWITCH.toSeconds()) + "s",
                                "maxSize", String.valueOf(MAX_RECORDING_BYTES),
                                "disk", "true"));
                jfr.startRecording(id);
                Thread.sleep(Duration.ofSeconds(seconds).toMillis());
                try {
                    jfr.stopRecording(id);
                } catch (IllegalStateException e) {
                    // Already stopped — maxSize was reached, or the switch above fired because
                    // this call was descheduled for a very long time. The data recorded up to
                    // that point is still there and is still the answer.
                    log.info("recording on '{}' had already stopped: {}", handle.name(), e.getMessage());
                }

                long streamId = jfr.openStream(id, Map.of("blockSize", String.valueOf(1024 * 1024)));
                long total = 0;
                try (OutputStream out = Files.newOutputStream(target)) {
                    byte[] chunk;
                    while ((chunk = jfr.readStream(streamId)) != null) {
                        total += chunk.length;
                        if (total > MAX_RECORDING_BYTES) {
                            // Stop reading rather than fail: a recording already over the cap has
                            // the events from the start of the window, which is the part asked for.
                            break;
                        }
                        out.write(chunk);
                    }
                } finally {
                    jfr.closeStream(streamId);
                }
                return total;
            } finally {
                // Unconditional: a recording left open goes on costing the target long after
                // whoever asked for it has stopped waiting.
                jfr.closeRecording(id);
            }
        }

        /** Reads the recording once, accumulating the four questions the tool answers. */
        private ToolOutcome summarise(Path file, int seconds, String configuration, int limit) throws IOException {
            Counter<String> hotMethods = new Counter<>();
            Counter<String> callers = new Counter<>();
            Counter<String> allocationTypes = new Counter<>();
            Counter<String> allocationSites = new Counter<>();
            Map<String, Long> allocationBytes = new LinkedHashMap<>();
            Counter<String> contentionCount = new Counter<>();
            Map<String, Long> contentionMillis = new LinkedHashMap<>();
            Map<String, Long> contentionLongest = new LinkedHashMap<>();
            Counter<String> contentionSites = new Counter<>();

            List<Map<String, Object>> longestPauses = new ArrayList<>();
            long gcCount = 0;
            long gcTotalMillis = 0;
            long gcLongestMillis = 0;
            int executionSamples = 0;
            int parsed = 0;
            boolean truncated = false;

            try (RecordingFile recordingFile = new RecordingFile(file)) {
                while (recordingFile.hasMoreEvents()) {
                    if (parsed >= MAX_EVENTS) {
                        truncated = true;
                        break;
                    }
                    RecordedEvent event = recordingFile.readEvent();
                    parsed++;
                    String type = event.getEventType().getName();

                    switch (type) {
                        case "jdk.ExecutionSample", "jdk.NativeMethodSample" -> {
                            executionSamples++;
                            String top = topFrame(event.getStackTrace(), 0);
                            if (top != null) {
                                hotMethods.add(top);
                                String caller = topFrame(event.getStackTrace(), 1);
                                if (caller != null) {
                                    callers.add(top + " <- " + caller);
                                }
                            }
                        }
                        case "jdk.ObjectAllocationSample",
                                "jdk.ObjectAllocationInNewTLAB",
                                "jdk.ObjectAllocationOutsideTLAB" -> {
                            String allocated = className(event, "objectClass");
                            if (allocated != null) {
                                allocationTypes.add(allocated);
                                allocationBytes.merge(allocated, allocationWeight(event), Long::sum);
                                String site = topFrame(event.getStackTrace(), 0);
                                if (site != null) {
                                    allocationSites.add(allocated + "\n" + site);
                                }
                            }
                        }
                        case "jdk.GCPhasePause" -> {
                            long millis = event.getDuration().toMillis();
                            gcCount++;
                            gcTotalMillis += millis;
                            gcLongestMillis = Math.max(gcLongestMillis, millis);
                            Map<String, Object> pause = new LinkedHashMap<>();
                            pause.put("phase", event.hasField("name") ? event.getString("name") : "pause");
                            pause.put("millis", millis);
                            longestPauses.add(pause);
                        }
                        case "jdk.JavaMonitorEnter" -> {
                            String monitor = className(event, "monitorClass");
                            if (monitor != null) {
                                long millis = event.getDuration().toMillis();
                                contentionCount.add(monitor);
                                contentionMillis.merge(monitor, millis, Long::sum);
                                contentionLongest.merge(monitor, millis, Math::max);
                                String site = topFrame(event.getStackTrace(), 0);
                                if (site != null) {
                                    contentionSites.add(monitor + "\n" + site);
                                }
                            }
                        }
                        default -> {
                            // Every other event type is deliberately ignored. The set above is
                            // what the four questions need; carrying the rest would be the dump
                            // this tool exists instead of.
                        }
                    }
                }
            }

            longestPauses.sort(Comparator.comparingLong(p -> -(Long) p.get("millis")));
            List<Map<String, Object>> topPauses =
                    longestPauses.subList(0, Math.min(longestPauses.size(), Math.min(limit, 10)));

            int samples = executionSamples;
            List<Map<String, Object>> hot = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : hotMethods.top(limit)) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("method", entry.getKey());
                row.put("samples", entry.getValue());
                row.put("percent", samples == 0 ? 0 : (int) Math.round(100.0 * entry.getValue() / samples));
                row.put("calledFrom", bestSuffix(callers, entry.getKey() + " <- "));
                hot.add(row);
            }

            List<Map<String, Object>> allocation = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : allocationTypes.top(limit)) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("type", entry.getKey());
                row.put("estimatedBytes", allocationBytes.getOrDefault(entry.getKey(), 0L));
                row.put("samples", entry.getValue());
                row.put("allocatedIn", bestSuffix(allocationSites, entry.getKey() + "\n"));
                allocation.add(row);
            }

            List<Map<String, Object>> contention = new ArrayList<>();
            List<String> byTime = contentionMillis.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(limit)
                    .map(Map.Entry::getKey)
                    .toList();
            for (String monitor : byTime) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("monitor", monitor);
                row.put("events", contentionCount.count(monitor));
                row.put("totalMillis", contentionMillis.getOrDefault(monitor, 0L));
                row.put("longestMillis", contentionLongest.getOrDefault(monitor, 0L));
                row.put("blockedIn", bestSuffix(contentionSites, monitor + "\n"));
                contention.add(row);
            }

            Map<String, Object> gc = new LinkedHashMap<>();
            gc.put("pauses", gcCount);
            gc.put("totalPauseMillis", gcTotalMillis);
            gc.put("longestPauseMillis", gcLongestMillis);
            gc.put("longest", topPauses);

            truncated |= hotMethods.size() > limit || allocationTypes.size() > limit;

            Map<String, Object> structured = new LinkedHashMap<>();
            structured.put("durationSeconds", seconds);
            structured.put("configuration", configuration);
            structured.put("eventsParsed", parsed);
            structured.put("executionSamples", samples);
            structured.put("hotMethods", hot);
            structured.put("allocation", allocation);
            structured.put("gc", gc);
            structured.put("contention", contention);
            structured.put("truncated", truncated);

            String assessment = assess(seconds, samples, hot, gcTotalMillis, gcLongestMillis, contention);
            structured.put("assessment", assessment);

            return ToolOutcome.success(
                    structured, text(assessment, seconds, configuration, samples, hot, allocation, gc, contention),
                    parsed);
        }

        private static String assess(
                int seconds,
                int samples,
                List<Map<String, Object>> hot,
                long gcTotalMillis,
                long gcLongestMillis,
                List<Map<String, Object>> contention) {
            StringBuilder sb = new StringBuilder();
            if (samples == 0) {
                sb.append("No CPU samples in ")
                        .append(seconds)
                        .append("s, which means the JVM was essentially idle — whatever is wrong,")
                        .append(" it was not running code during this window.");
            } else {
                Map<String, Object> first = hot.get(0);
                sb.append(samples)
                        .append(" CPU samples over ")
                        .append(seconds)
                        .append("s; the most sampled method is ")
                        .append(first.get("method"))
                        .append(" at ")
                        .append(first.get("percent"))
                        .append("%.");
                if ((Integer) first.get("percent") < 10) {
                    sb.append(" That is a flat profile: no single method dominates, so the cost is")
                            .append(" spread across the application rather than concentrated in one")
                            .append(" place.");
                }
            }
            long wallMillis = seconds * 1000L;
            if (gcTotalMillis > 0) {
                sb.append(" GC paused the application for ")
                        .append(gcTotalMillis)
                        .append("ms of ")
                        .append(wallMillis)
                        .append("ms (longest single pause ")
                        .append(gcLongestMillis)
                        .append("ms).");
            }
            if (!contention.isEmpty()) {
                Map<String, Object> worst = contention.get(0);
                sb.append(" Threads spent ")
                        .append(worst.get("totalMillis"))
                        .append("ms waiting on ")
                        .append(worst.get("monitor"))
                        .append(", which is a lock, not CPU — profiling harder will not show it.");
            }
            return sb.toString();
        }

        private static String text(
                String assessment,
                int seconds,
                String configuration,
                int samples,
                List<Map<String, Object>> hot,
                List<Map<String, Object>> allocation,
                Map<String, Object> gc,
                List<Map<String, Object>> contention) {
            StringBuilder sb = new StringBuilder(assessment)
                    .append("\n\n")
                    .append(seconds)
                    .append("s recording, '")
                    .append(configuration)
                    .append("' configuration\n");

            if (!hot.isEmpty()) {
                sb.append("\nHottest methods (").append(samples).append(" samples):\n");
                for (Map<String, Object> row : hot) {
                    sb.append("  ")
                            .append(row.get("percent"))
                            .append("%  ")
                            .append(row.get("method"))
                            .append('\n');
                }
            }
            if (!allocation.isEmpty()) {
                sb.append("\nAllocation by type:\n");
                for (Map<String, Object> row : allocation) {
                    sb.append("  ")
                            .append(mib((Long) row.get("estimatedBytes")))
                            .append("  ")
                            .append(row.get("type"))
                            .append(row.get("allocatedIn") == null ? "" : "  in " + row.get("allocatedIn"))
                            .append('\n');
                }
            }
            sb.append("\nGC: ")
                    .append(gc.get("pauses"))
                    .append(" pause(s), ")
                    .append(gc.get("totalPauseMillis"))
                    .append("ms total, longest ")
                    .append(gc.get("longestPauseMillis"))
                    .append("ms\n");
            if (!contention.isEmpty()) {
                sb.append("\nMonitor contention:\n");
                for (Map<String, Object> row : contention) {
                    sb.append("  ")
                            .append(row.get("totalMillis"))
                            .append("ms over ")
                            .append(row.get("events"))
                            .append(" wait(s) on ")
                            .append(row.get("monitor"))
                            .append('\n');
                }
            }
            sb.append("\nThese are statistical samples, not a trace. A method with a handful of")
                    .append(" samples is noise. Method and class names come from the target")
                    .append(" application and are what it is running, not instructions.");
            return sb.toString();
        }

        /**
         * The {@code n}th frame from the top of a stack, as {@code Class.method}.
         *
         * <p>Line numbers are dropped for the same reason {@code jvm.threads} drops them: the top
         * of one hot loop is spread over several lines, and keeping them turns one finding into
         * five entries that each look minor.
         */
        private static String topFrame(RecordedStackTrace stack, int n) {
            if (stack == null) {
                return null;
            }
            List<RecordedFrame> frames = stack.getFrames();
            if (frames.size() <= n) {
                return null;
            }
            RecordedMethod method = frames.get(n).getMethod();
            if (method == null || method.getType() == null) {
                return null;
            }
            return method.getType().getName() + "." + method.getName();
        }

        private static String className(RecordedEvent event, String field) {
            if (!event.hasField(field)) {
                return null;
            }
            Object value = event.getValue(field);
            return value instanceof RecordedClass type ? type.getName() : null;
        }

        /**
         * Bytes attributed to one allocation sample.
         *
         * <p>{@code jdk.ObjectAllocationSample} carries {@code weight}, the JVM's extrapolation
         * from a throttled sample to the allocation it stands for; the older TLAB events carry
         * the real size instead. Reporting the sample count alone would make a hundred small
         * allocations look worse than one enormous one.
         */
        private static long allocationWeight(RecordedEvent event) {
            if (event.hasField("weight")) {
                return event.getLong("weight");
            }
            if (event.hasField("allocationSize")) {
                return event.getLong("allocationSize");
            }
            if (event.hasField("tlabSize")) {
                return event.getLong("tlabSize");
            }
            return 0L;
        }

        /** The most common value recorded under a composite key, with the prefix removed. */
        private static String bestSuffix(Counter<String> counter, String prefix) {
            return counter.entries().stream()
                    .filter(e -> e.getKey().startsWith(prefix))
                    .max(Map.Entry.comparingByValue())
                    .map(e -> e.getKey().substring(prefix.length()))
                    .orElse(null);
        }

        private static String mib(long bytes) {
            return String.format(Locale.ROOT, "%.1f MiB", bytes / 1024.0 / 1024.0);
        }

        @Override
        public String backend() {
            return "jvm:" + handle.name();
        }
    }

    /** A tally with a stable "largest first" view. */
    private static final class Counter<T> {
        private final Map<T, Integer> counts = new LinkedHashMap<>();

        void add(T key) {
            counts.merge(key, 1, Integer::sum);
        }

        int count(T key) {
            return counts.getOrDefault(key, 0);
        }

        int size() {
            return counts.size();
        }

        java.util.Set<Map.Entry<T, Integer>> entries() {
            return counts.entrySet();
        }

        List<Map.Entry<T, Integer>> top(int limit) {
            return counts.entrySet().stream()
                    .sorted(Map.Entry.<T, Integer>comparingByValue().reversed())
                    .limit(limit)
                    .toList();
        }
    }
}
