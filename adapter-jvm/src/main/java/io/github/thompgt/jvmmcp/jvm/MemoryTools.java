package io.github.thompgt.jvmmcp.jvm;

import io.github.thompgt.jvmmcp.core.Arguments;
import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.Schemas;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.PolicyEngine;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.modelcontextprotocol.spec.McpSchema;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * {@code jvm.memory} — heap, pools and collectors, expressed as change rather than as totals.
 *
 * <p>The platform's memory MBeans are almost all cumulative-since-start, and a single reading of
 * a cumulative counter is the classic way to be confidently wrong about a JVM. "The application
 * has spent 41 seconds in garbage collection" is not a fact about its health; it is a fact about
 * its age. The same 41 seconds is unremarkable over a fortnight and an outage over four minutes,
 * and nothing in the raw value distinguishes them.
 *
 * <p>So every counter here is reported with a delta and the interval it covers, from one of two
 * sources:
 *
 * <ul>
 *   <li><b>Since this bridge last looked.</b> The previous snapshot is kept per target, so a
 *       second call some minutes after the first answers "what changed while we were talking",
 *       which is the question being asked during an incident.
 *   <li><b>Sampled within the call.</b> {@code sample_seconds} takes two readings a fixed
 *       interval apart, for the first call, when there is no previous snapshot to subtract.
 * </ul>
 *
 * <p>The other correction this tool makes is which "used" to believe. {@link MemoryUsage#getUsed}
 * on a pool counts live objects and garbage alike, so a healthy young generation reads as nearly
 * full at almost any moment and a model reading it concludes the JVM is about to die. The number
 * that means what a reader thinks "used" means is {@link MemoryPoolMXBean#getCollectionUsage()},
 * measured immediately after the last collection of that pool: memory that survived. A tenured
 * pool whose post-collection usage climbs across calls is a leak, and it is the only shape in
 * this output that is one.
 */
final class MemoryTools {

    private MemoryTools() {}

    static List<BridgeTool> create(JvmTargetHandle handle, PolicyEngine policy) {
        return List.of(new MemoryTool(handle, policy));
    }

    /** The MBean the tool is guarded on; the pools and collectors are filtered individually. */
    static final String MEMORY_MBEAN = "java.lang:type=Memory";

    /** Longest in-call sampling interval, whatever the caller asks for and the timeout allows. */
    private static final int MAX_SAMPLE_SECONDS = 30;

    /** Left for the two readings and the round trips around them when clamping to the timeout. */
    private static final Duration SAMPLE_HEADROOM = Duration.ofSeconds(2);

    static final class MemoryTool implements BridgeTool {

        private final JvmTargetHandle handle;
        private final PolicyEngine policy;

        /**
         * The last reading, shared by every caller of this backend rather than held per
         * principal.
         *
         * <p>Deliberate, and the interval is always reported so it cannot mislead: two callers
         * interleaving gives the second one a delta over a very short window, which is visibly
         * useless rather than quietly wrong. Keying this by principal would instead give each
         * caller their own idea of "since last time", and an on-call engineer and the model
         * helping them would be reading different intervals while looking at the same incident.
         */
        private Snapshot previous;

        MemoryTool(JvmTargetHandle handle, PolicyEngine policy) {
            this.handle = handle;
            this.policy = policy;
        }

        @Override
        public McpSchema.Tool descriptor() {
            Map<String, Object> input = Schemas.object()
                    .optionalInteger(
                            "sample_seconds",
                            "Take two readings this many seconds apart and report the change"
                                    + " between them. Use on the first call, when there is no"
                                    + " previous reading to compare against. Omit on later calls:"
                                    + " the delta since the last call is reported anyway and covers"
                                    + " a longer, more informative interval.",
                            0,
                            MAX_SAMPLE_SECONDS)
                    .build();

            Schemas.ObjectSchema pool = Schemas.object()
                    .optionalString("name", "Pool name, e.g. 'G1 Old Gen'.")
                    .optionalString("type", "HEAP or NON_HEAP.")
                    .optionalInteger("usedBytes", "In use now, including garbage not yet collected.")
                    .optionalInteger("committedBytes", "Reserved from the OS for this pool.")
                    .optionalInteger("maxBytes", "Ceiling, or -1 when undefined.")
                    .optionalInteger("percentOfMax", "usedBytes as a percentage of maxBytes, -1 if no max.", -1, 100)
                    .optionalInteger("peakUsedBytes", "Highest usedBytes since the JVM started.")
                    .optionalInteger(
                            "afterLastGcBytes",
                            "Live data: usage measured just after this pool was last collected."
                                    + " -1 when the pool is not collected. This is the number to"
                                    + " read, not usedBytes.")
                    .optionalInteger(
                            "afterLastGcPercentOfMax", "afterLastGcBytes against maxBytes, -1 if unknown.", -1, 100)
                    .optionalInteger("afterLastGcDeltaBytes", "Change in afterLastGcBytes over the interval.")
                    .optionalBoolean(
                            "aboveUsageThreshold",
                            "True when the JVM's own configured threshold for this pool is exceeded.");

            Schemas.ObjectSchema collector = Schemas.object()
                    .optionalString("name", "Collector name, e.g. 'G1 Young Generation'.")
                    .optionalInteger("collections", "Cumulative count since the JVM started.")
                    .optionalInteger("totalMillis", "Cumulative time since the JVM started.")
                    .optionalInteger("collectionsInInterval", "Collections during the interval, -1 if unknown.")
                    .optionalInteger("millisInInterval", "Time collecting during the interval, -1 if unknown.")
                    .optionalInteger("averagePauseMillis", "millisInInterval / collectionsInInterval, -1 if none.");

            Map<String, Object> output = Schemas.object()
                    .optionalString("snapshotTime", "ISO-8601 instant this reading was taken.")
                    .optionalInteger("uptimeMillis", "How long the target JVM has been running.")
                    .optionalObject("heap", "Aggregate heap usage: used, committed, max, percentOfMax.")
                    .optionalObject("nonHeap", "Aggregate non-heap usage, in the same shape. Metaspace lives here.")
                    .arrayOfObjects("pools", "Per-pool detail, heap pools first.", pool)
                    .arrayOfObjects("collectors", "Per-collector counts and times.", collector)
                    .optionalObject(
                            "interval",
                            "millis, source ('previous call' or 'sampled in call'), and"
                                    + " gcPercentOfInterval — the share of wall clock spent"
                                    + " collecting. Absent on a first call with no sample_seconds.")
                    .optionalString(
                            "assessment", "One line naming the pool under most pressure and the GC cost, if any.")
                    .optionalInteger("hiddenByPolicy", "Pools and collectors excluded by the MBean allowlist.")
                    .build();

            return McpSchema.Tool.builder("jvm.memory", input)
                    .title("JVM memory and garbage collection")
                    .description("Reports heap, memory pools and collector activity for "
                            + (handle.isEmbedded() ? "this bridge's own JVM" : "'" + handle.name() + "'")
                            + ".\n\n"
                            + "Read afterLastGcBytes, not usedBytes. usedBytes counts garbage that"
                            + " has not been collected yet, so a healthy young generation is near"
                            + " its maximum most of the time and means nothing by it."
                            + " afterLastGcBytes is what survived the last collection of that pool,"
                            + " and a tenured pool whose afterLastGcBytes climbs call after call is"
                            + " the shape of a leak.\n\n"
                            + "Collection counts and times are cumulative since the JVM started, so"
                            + " they are reported with the change over an interval. On the first"
                            + " call pass sample_seconds to get one; after that the change since"
                            + " the previous call is included automatically, and covers a longer"
                            + " window.\n\n"
                            + "Reading these is cheap — no collection is triggered and nothing is"
                            + " paused. This server cannot make the JVM collect, and would not.")
                    .outputSchema(output)
                    .annotations(McpSchema.ToolAnnotations.builder()
                            .title("JVM memory and garbage collection")
                            .readOnlyHint(true)
                            .destructiveHint(false)
                            .idempotentHint(false)
                            .openWorldHint(false)
                            .build())
                    .build();
        }

        @Override
        public ToolOutcome call(Map<String, Object> arguments) {
            int sampleSeconds = new Arguments(arguments).optionalInt("sample_seconds", 0);
            PolicyProfile effective = policy.effectiveProfile();

            return policy.guardRead("jvm.memory", List.of(MEMORY_MBEAN), 0, limits -> {
                Duration allowed = limits.timeout().minus(SAMPLE_HEADROOM);
                if (sampleSeconds > 0 && allowed.toSeconds() < 1) {
                    return ToolOutcome.failure("sample_seconds cannot be used on this backend: its call timeout is "
                            + limits.timeout() + ", which leaves no room to take two readings."
                            + " Call this tool twice instead — the second call reports the change"
                            + " since the first.");
                }
                int sample = (int) Math.min(sampleSeconds, Math.max(0, allowed.toSeconds()));

                Reading reading = handle.call(limits.timeout(), connection -> {
                    Reading first = read(connection, effective);
                    if (sample <= 0) {
                        return first;
                    }
                    Thread.sleep(Duration.ofSeconds(sample).toMillis());
                    Reading second = read(connection, effective);
                    return second.withBaseline(first.snapshot());
                });

                Snapshot baseline;
                synchronized (this) {
                    baseline = reading.baseline() != null ? reading.baseline() : previous;
                    previous = reading.snapshot();
                }

                String source = reading.baseline() != null
                        ? "sampled in call"
                        : (baseline != null ? "previous call" : null);
                return render(reading, baseline, source, sample, sampleSeconds);
            });
        }

        /** One pass over the platform beans. Every value in a {@link Reading} comes from here. */
        private Reading read(MBeanServerConnection connection, PolicyProfile effective) throws Exception {
            MemoryMXBean memory = ManagementFactory.getPlatformMXBean(connection, MemoryMXBean.class);
            RuntimeMXBean runtime = ManagementFactory.getPlatformMXBean(connection, RuntimeMXBean.class);
            List<MemoryPoolMXBean> pools =
                    ManagementFactory.getPlatformMXBeans(connection, MemoryPoolMXBean.class);
            List<GarbageCollectorMXBean> collectors =
                    ManagementFactory.getPlatformMXBeans(connection, GarbageCollectorMXBean.class);

            int hidden = 0;
            List<PoolReading> poolReadings = new ArrayList<>();
            for (MemoryPoolMXBean pool : pools) {
                if (!visible(effective, "MemoryPool", pool.getName())) {
                    hidden++;
                    continue;
                }
                MemoryUsage usage = pool.getUsage();
                MemoryUsage afterGc = pool.getCollectionUsage();
                MemoryUsage peak = pool.getPeakUsage();
                boolean aboveThreshold = pool.isUsageThresholdSupported() && pool.isUsageThresholdExceeded();
                poolReadings.add(new PoolReading(
                        pool.getName(),
                        pool.getType().name(),
                        usage == null ? -1 : usage.getUsed(),
                        usage == null ? -1 : usage.getCommitted(),
                        usage == null ? -1 : usage.getMax(),
                        peak == null ? -1 : peak.getUsed(),
                        afterGc == null ? -1 : afterGc.getUsed(),
                        afterGc == null ? -1 : afterGc.getMax(),
                        aboveThreshold));
            }

            List<CollectorReading> collectorReadings = new ArrayList<>();
            for (GarbageCollectorMXBean collector : collectors) {
                if (!visible(effective, "GarbageCollector", collector.getName())) {
                    hidden++;
                    continue;
                }
                collectorReadings.add(new CollectorReading(
                        collector.getName(), collector.getCollectionCount(), collector.getCollectionTime()));
            }

            return new Reading(
                    Instant.now(),
                    runtime.getUptime(),
                    memory.getHeapMemoryUsage(),
                    memory.getNonHeapMemoryUsage(),
                    List.copyOf(poolReadings),
                    List.copyOf(collectorReadings),
                    hidden,
                    null);
        }

        /**
         * Whether one pool or collector may be reported.
         *
         * <p>Checked per bean rather than once for the tool, so a profile narrowed to specific
         * MBeans narrows this output too. A name that will not parse as an ObjectName — pool
         * names contain spaces and, on some collectors, characters JMX quotes — is treated as
         * not visible rather than as an error: failing open here would make an unparseable name
         * the way past the allowlist.
         */
        private static boolean visible(PolicyProfile effective, String type, String name) {
            try {
                return MBeanTools.readable(
                        effective, new ObjectName("java.lang:type=" + type + ",name=" + ObjectName.quote(name)))
                        || MBeanTools.readable(effective, new ObjectName("java.lang:type=" + type + ",name=" + name));
            } catch (MalformedObjectNameException e) {
                return false;
            }
        }

        private ToolOutcome render(
                Reading reading, Snapshot baseline, String source, int sample, int requestedSample) {
            Map<String, Object> structured = new LinkedHashMap<>();
            structured.put("snapshotTime", reading.at().toString());
            structured.put("uptimeMillis", reading.uptimeMillis());
            structured.put("heap", usage(reading.heap()));
            structured.put("nonHeap", usage(reading.nonHeap()));

            List<Map<String, Object>> pools = new ArrayList<>();
            for (PoolReading pool : reading.pools()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("name", pool.name());
                row.put("type", pool.type());
                row.put("usedBytes", pool.used());
                row.put("committedBytes", pool.committed());
                row.put("maxBytes", pool.max());
                row.put("percentOfMax", percent(pool.used(), pool.max()));
                row.put("peakUsedBytes", pool.peakUsed());
                row.put("afterLastGcBytes", pool.afterGcUsed());
                row.put("afterLastGcPercentOfMax", percent(pool.afterGcUsed(), effectiveMax(pool)));
                row.put(
                        "afterLastGcDeltaBytes",
                        baseline == null || pool.afterGcUsed() < 0 || !baseline.poolAfterGc().containsKey(pool.name())
                                ? 0L
                                : pool.afterGcUsed() - baseline.poolAfterGc().get(pool.name()));
                row.put("aboveUsageThreshold", pool.aboveThreshold());
                pools.add(row);
            }
            structured.put("pools", pools);

            long gcMillisInInterval = 0;
            List<Map<String, Object>> collectors = new ArrayList<>();
            for (CollectorReading collector : reading.collectors()) {
                long countDelta = -1;
                long millisDelta = -1;
                if (baseline != null && baseline.collectorCounts().containsKey(collector.name())) {
                    countDelta = collector.count() - baseline.collectorCounts().get(collector.name());
                    millisDelta = collector.millis() - baseline.collectorMillis().get(collector.name());
                    gcMillisInInterval += Math.max(0, millisDelta);
                }
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("name", collector.name());
                row.put("collections", collector.count());
                row.put("totalMillis", collector.millis());
                row.put("collectionsInInterval", countDelta);
                row.put("millisInInterval", millisDelta);
                row.put("averagePauseMillis", countDelta > 0 ? millisDelta / countDelta : -1L);
                collectors.add(row);
            }
            structured.put("collectors", collectors);

            long intervalMillis =
                    baseline == null ? -1 : reading.at().toEpochMilli() - baseline.atEpochMillis();
            if (baseline != null && intervalMillis > 0) {
                Map<String, Object> interval = new LinkedHashMap<>();
                interval.put("millis", intervalMillis);
                interval.put("source", source);
                interval.put("gcPercentOfInterval", percent(gcMillisInInterval, intervalMillis));
                structured.put("interval", interval);
            }

            String assessment = assess(reading, baseline, intervalMillis, gcMillisInInterval);
            structured.put("assessment", assessment);
            structured.put("hiddenByPolicy", reading.hiddenByPolicy());

            StringBuilder summary = new StringBuilder(assessment).append("\n\n");
            summary.append("heap ")
                    .append(mib(reading.heap().getUsed()))
                    .append(" / ")
                    .append(reading.heap().getMax() < 0 ? "no max" : mib(reading.heap().getMax()))
                    .append("   non-heap ")
                    .append(mib(reading.nonHeap().getUsed()))
                    .append("   up ")
                    .append(Duration.ofMillis(reading.uptimeMillis()).toString())
                    .append('\n');
            for (Map<String, Object> pool : pools) {
                summary.append("  ")
                        .append(pool.get("name"))
                        .append(" (")
                        .append(pool.get("type"))
                        .append(") used ")
                        .append(mib((Long) pool.get("usedBytes")))
                        .append(", live after last GC ")
                        .append((Long) pool.get("afterLastGcBytes") < 0
                                ? "n/a"
                                : mib((Long) pool.get("afterLastGcBytes")))
                        .append(deltaSuffix((Long) pool.get("afterLastGcDeltaBytes"), baseline != null))
                        .append('\n');
            }
            for (Map<String, Object> collector : collectors) {
                summary.append("  ")
                        .append(collector.get("name"))
                        .append(": ")
                        .append(collector.get("collections"))
                        .append(" collections, ")
                        .append(collector.get("totalMillis"))
                        .append("ms total");
                if ((Long) collector.get("collectionsInInterval") >= 0) {
                    summary.append(" — ")
                            .append(collector.get("collectionsInInterval"))
                            .append(" and ")
                            .append(collector.get("millisInInterval"))
                            .append("ms in the interval");
                }
                summary.append('\n');
            }

            if (baseline == null) {
                summary.append("\nNo interval: this is the first reading of '")
                        .append(handle.name())
                        .append("', so the counts above are totals since the JVM started and say")
                        .append(" nothing about now. Call again — or pass sample_seconds — for the")
                        .append(" change over a known window.");
            } else {
                summary.append("\nChange measured over ")
                        .append(Duration.ofMillis(intervalMillis))
                        .append(" (")
                        .append(source)
                        .append(").");
            }
            if (requestedSample > sample && sample > 0) {
                summary.append("\nsample_seconds was reduced to ")
                        .append(sample)
                        .append("s to fit inside this backend's call timeout.");
            }
            if (reading.hiddenByPolicy() > 0) {
                summary.append("\n")
                        .append(reading.hiddenByPolicy())
                        .append(" pool(s) or collector(s) are excluded by the MBean allowlist, so")
                        .append(" the per-pool list is not the whole JVM.");
            }

            return ToolOutcome.success(structured, summary.toString(), pools.size());
        }

        /**
         * The one line that answers the question the tool was called for.
         *
         * <p>Naming the pool under most pressure is the difference between a model relaying
         * numbers and a model reporting a diagnosis. It is stated as an observation with the
         * evidence attached rather than as a verdict, because the same numbers are a leak on a
         * tenured pool and normal operation on a young one, and this tool cannot tell which
         * workload it is looking at.
         */
        private static String assess(
                Reading reading, Snapshot baseline, long intervalMillis, long gcMillisInInterval) {
            PoolReading worst = null;
            int worstPercent = -1;
            for (PoolReading pool : reading.pools()) {
                int p = percent(pool.afterGcUsed(), effectiveMax(pool));
                if (p > worstPercent) {
                    worstPercent = p;
                    worst = pool;
                }
            }

            StringBuilder sb = new StringBuilder();
            if (worst == null || worstPercent < 0) {
                sb.append("No pool reports usage after a collection yet — nothing has been collected"
                        + " since the JVM started, which on a young process is normal.");
            } else {
                sb.append("Fullest pool after its last collection: ")
                        .append(worst.name())
                        .append(" at ")
                        .append(worstPercent)
                        .append("% of max (")
                        .append(mib(worst.afterGcUsed()))
                        .append(" live).");
                if (baseline != null) {
                    Long was = baseline.poolAfterGc().get(worst.name());
                    if (was != null && worst.afterGcUsed() >= 0) {
                        long delta = worst.afterGcUsed() - was;
                        sb.append(delta > 0
                                ? " It grew by " + mib(delta) + " over the interval, which is the shape"
                                        + " of a leak if it keeps growing across further calls."
                                : " It did not grow over the interval.");
                    }
                }
            }
            if (intervalMillis > 0) {
                int gcPercent = percent(gcMillisInInterval, intervalMillis);
                sb.append(" The JVM spent ")
                        .append(gcMillisInInterval)
                        .append("ms of the last ")
                        .append(intervalMillis)
                        .append("ms collecting (")
                        .append(gcPercent)
                        .append("%).");
                if (gcPercent >= 20) {
                    sb.append(" Above about 20% the application is spending more time collecting than"
                            + " working, and rising GC cost with a pool that is not shrinking is the"
                            + " approach to an OutOfMemoryError rather than a steady state.");
                }
            }
            return sb.toString();
        }

        /** Pools report a max on the pool and, separately, on the post-collection usage. */
        private static long effectiveMax(PoolReading pool) {
            return pool.afterGcMax() > 0 ? pool.afterGcMax() : pool.max();
        }

        private static Map<String, Object> usage(MemoryUsage usage) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("usedBytes", usage.getUsed());
            map.put("committedBytes", usage.getCommitted());
            map.put("maxBytes", usage.getMax());
            map.put("percentOfMax", percent(usage.getUsed(), usage.getMax()));
            return map;
        }

        private static int percent(long value, long of) {
            if (value < 0 || of <= 0) {
                return -1;
            }
            return (int) Math.min(100, Math.round(100.0 * value / of));
        }

        private static String mib(long bytes) {
            if (bytes < 0) {
                return "n/a";
            }
            return String.format(Locale.ROOT, "%.1f MiB", bytes / 1024.0 / 1024.0);
        }

        private static String deltaSuffix(long delta, boolean haveBaseline) {
            if (!haveBaseline || delta == 0) {
                return "";
            }
            return delta > 0 ? " (+" + mib(delta) + " since last reading)" : " (-" + mib(-delta) + " since last reading)";
        }

        @Override
        public String backend() {
            return "jvm:" + handle.name();
        }
    }

    /** One complete pass over the memory beans. */
    private record Reading(
            Instant at,
            long uptimeMillis,
            MemoryUsage heap,
            MemoryUsage nonHeap,
            List<PoolReading> pools,
            List<CollectorReading> collectors,
            int hiddenByPolicy,
            Snapshot baseline) {

        Reading withBaseline(Snapshot baseline) {
            return new Reading(at, uptimeMillis, heap, nonHeap, pools, collectors, hiddenByPolicy, baseline);
        }

        /** The subset worth keeping until the next call: everything a delta is taken against. */
        Snapshot snapshot() {
            Map<String, Long> afterGc = new LinkedHashMap<>();
            for (PoolReading pool : pools) {
                if (pool.afterGcUsed() >= 0) {
                    afterGc.put(pool.name(), pool.afterGcUsed());
                }
            }
            Map<String, Long> counts = new LinkedHashMap<>();
            Map<String, Long> millis = new LinkedHashMap<>();
            for (CollectorReading collector : collectors) {
                counts.put(collector.name(), collector.count());
                millis.put(collector.name(), collector.millis());
            }
            return new Snapshot(at.toEpochMilli(), Map.copyOf(afterGc), Map.copyOf(counts), Map.copyOf(millis));
        }
    }

    private record PoolReading(
            String name,
            String type,
            long used,
            long committed,
            long max,
            long peakUsed,
            long afterGcUsed,
            long afterGcMax,
            boolean aboveThreshold) {}

    private record CollectorReading(String name, long count, long millis) {}

    /** What a delta is taken against: the previous reading, reduced to its cumulative parts. */
    private record Snapshot(
            long atEpochMillis,
            Map<String, Long> poolAfterGc,
            Map<String, Long> collectorCounts,
            Map<String, Long> collectorMillis) {}
}
