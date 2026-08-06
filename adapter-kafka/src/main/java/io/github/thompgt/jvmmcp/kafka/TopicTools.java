package io.github.thompgt.jvmmcp.kafka;

import io.github.thompgt.jvmmcp.core.Arguments;
import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.Schemas;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.PolicyEngine;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

/**
 * Topic inspection, filtered through the same allowlist as everything else.
 *
 * <p>Topic allowlists are where the wildcard support in {@link PolicyProfile} earns its place:
 * a real deployment names topic families ({@code orders.*}), not individual topics, and an
 * operator forced to enumerate them would either miss one or write {@code *}.
 */
final class TopicTools {

    private TopicTools() {}

    static List<BridgeTool> create(KafkaBrokerHandle handle, PolicyEngine policy) {
        return List.of(new ListTopicsTool(handle, policy), new DescribeTopicTool(handle, policy));
    }

    /** {@code kafka.list_topics} — what the model may look at at all. */
    static final class ListTopicsTool implements BridgeTool {
        private final KafkaBrokerHandle handle;
        private final PolicyEngine policy;

        ListTopicsTool(KafkaBrokerHandle handle, PolicyEngine policy) {
            this.handle = handle;
            this.policy = policy;
        }

        @Override
        public McpSchema.Tool descriptor() {
            Map<String, Object> output = Schemas.object()
                    .optionalArrayOfStrings("topics", "Topic names this caller may read, sorted.")
                    .optionalInteger("count", "How many.", 0, Integer.MAX_VALUE)
                    .build();

            return McpSchema.Tool.builder("kafka.list_topics", Schemas.object().build())
                    .title("List readable Kafka topics")
                    .description("Lists the topics on the '" + handle.name()
                            + "' cluster that this server will let you read.\n\n"
                            + "This is the complete set, and internal topics (__consumer_offsets and"
                            + " friends) are never included. A topic not listed here is excluded by"
                            + " policy, and naming it in another tool will be refused.\n\n"
                            + "Start here. kafka.describe_topic gives partitions and retention for"
                            + " one of them.")
                    .outputSchema(output)
                    .annotations(McpSchema.ToolAnnotations.builder()
                            .title("List readable Kafka topics")
                            .readOnlyHint(true)
                            .destructiveHint(false)
                            .openWorldHint(false)
                            .build())
                    .build();
        }

        @Override
        public ToolOutcome call(Map<String, Object> arguments) {
            PolicyProfile effective = policy.effectiveProfile();

            return policy.guardRead("kafka.list_topics", List.of(), 0, limits -> {
                // listInternal(false): __consumer_offsets is not something a model should be
                // reading, and its presence in a listing is an invitation to try.
                Set<String> all = handle.await(handle.admin()
                        .listTopics(new ListTopicsOptions().listInternal(false))
                        .names());
                List<String> visible = all.stream()
                        .filter(effective::isReadable)
                        .sorted()
                        .toList();

                Map<String, Object> structured = Map.of("topics", visible, "count", visible.size());
                String summary = visible.isEmpty()
                        ? "No readable topics are configured for '" + handle.name() + "'."
                        : visible.size() + " readable topic(s): " + String.join(", ", visible);
                return ToolOutcome.success(structured, summary, visible.size());
            });
        }

        @Override
        public String backend() {
            return "kafka:" + handle.name();
        }
    }

    /** {@code kafka.describe_topic} — partitions, offsets, and the retention that explains gaps. */
    static final class DescribeTopicTool implements BridgeTool {
        private final KafkaBrokerHandle handle;
        private final PolicyEngine policy;

        /**
         * Reported when set on the topic. Retention and cleanup policy are here because they
         * are the answer to "where did the messages go" — a compacted topic with a short
         * retention has not lost data, and a model without these facts will say it has.
         */
        private static final List<String> INTERESTING_CONFIGS = List.of(
                "cleanup.policy", "retention.ms", "retention.bytes", "max.message.bytes", "min.insync.replicas");

        DescribeTopicTool(KafkaBrokerHandle handle, PolicyEngine policy) {
            this.handle = handle;
            this.policy = policy;
        }

        @Override
        public McpSchema.Tool descriptor() {
            Map<String, Object> input = Schemas.object()
                    .requiredString("topic", "Topic name, exactly as returned by kafka.list_topics.")
                    .build();

            Schemas.ObjectSchema partition = Schemas.object()
                    .optionalInteger("partition", "Partition number.", 0, Integer.MAX_VALUE)
                    .optionalInteger("leader", "Broker id currently leading it, -1 if none.", -1, Integer.MAX_VALUE)
                    .optionalInteger("replicas", "Replica count.", 0, Integer.MAX_VALUE)
                    .optionalInteger("inSyncReplicas", "In-sync replica count.", 0, Integer.MAX_VALUE)
                    // Unbounded: offsets outgrow an int, and a declared maximum here would fail
                    // validation on exactly the busiest topic anyone asks about.
                    .optionalInteger("earliestOffset", "First offset still retained.")
                    .optionalInteger("latestOffset", "Next offset to be written.")
                    .optionalInteger("messages", "latestOffset - earliestOffset.");

            Map<String, Object> output = Schemas.object()
                    .optionalString("topic", "The topic described.")
                    .optionalInteger("partitions", "Partition count.", 0, Integer.MAX_VALUE)
                    .arrayOfObjects("partitionDetail", "One entry per partition.", partition)
                    .optionalObject("config", "Non-default topic configuration worth knowing about.")
                    .optionalBoolean("underReplicated", "True if any partition has fewer ISRs than replicas.")
                    .build();

            return McpSchema.Tool.builder("kafka.describe_topic", input)
                    .title("Describe a Kafka topic")
                    .description("Returns partitions, leadership, replication, and per-partition offset"
                            + " bounds for one topic on '" + handle.name() + "'.\n\n"
                            + "The offsets are a snapshot taken now, and the log moves. Treat"
                            + " latestOffset as a reading at a moment, not a fixed end.\n\n"
                            + "cleanup.policy and retention.ms are included when set, because they are"
                            + " usually the answer to 'where did the older messages go' — a compacted"
                            + " or short-retention topic has not lost data, it has aged it out.")
                    .outputSchema(output)
                    .annotations(McpSchema.ToolAnnotations.builder()
                            .title("Describe a Kafka topic")
                            .readOnlyHint(true)
                            .destructiveHint(false)
                            .openWorldHint(false)
                            .build())
                    .build();
        }

        @Override
        public ToolOutcome call(Map<String, Object> arguments) {
            String topic = new Arguments(arguments).requireString("topic").trim();

            return policy.guardRead("kafka.describe_topic", List.of(topic), 0, limits -> {
                TopicDescription description = handle.await(
                                handle.admin().describeTopics(List.of(topic)).allTopicNames())
                        .get(topic);
                if (description == null) {
                    return ToolOutcome.failure("topic '" + topic + "' is allowed by policy but does not exist on"
                            + " the '" + handle.name() + "' cluster. Call kafka.list_topics to see"
                            + " what is actually there.");
                }

                Map<TopicPartition, Long> earliest = offsets(description, OffsetSpec.earliest());
                Map<TopicPartition, Long> latest = offsets(description, OffsetSpec.latest());

                List<Map<String, Object>> partitions = new ArrayList<>();
                boolean underReplicated = false;
                for (TopicPartitionInfo info : description.partitions()) {
                    TopicPartition tp = new TopicPartition(topic, info.partition());
                    long from = earliest.getOrDefault(tp, -1L);
                    long to = latest.getOrDefault(tp, -1L);
                    underReplicated |= info.isr().size() < info.replicas().size();

                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("partition", info.partition());
                    row.put("leader", info.leader() == null ? -1 : info.leader().id());
                    row.put("replicas", info.replicas().size());
                    row.put("inSyncReplicas", info.isr().size());
                    row.put("earliestOffset", from);
                    row.put("latestOffset", to);
                    row.put("messages", from >= 0 && to >= 0 ? to - from : -1L);
                    partitions.add(row);
                }

                Map<String, Object> structured = new LinkedHashMap<>();
                structured.put("topic", topic);
                structured.put("partitions", description.partitions().size());
                structured.put("partitionDetail", partitions);
                structured.put("config", topicConfig(topic));
                structured.put("underReplicated", underReplicated);

                return ToolOutcome.success(
                        structured, summarise(topic, partitions, underReplicated), partitions.size());
            });
        }

        private Map<TopicPartition, Long> offsets(TopicDescription description, OffsetSpec spec) throws Exception {
            Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
            for (TopicPartitionInfo info : description.partitions()) {
                request.put(new TopicPartition(description.name(), info.partition()), spec);
            }
            Map<TopicPartition, Long> resolved = new LinkedHashMap<>();
            handle.await(handle.admin().listOffsets(request).all())
                    .forEach((tp, result) -> resolved.put(tp, result.offset()));
            return resolved;
        }

        /** Only entries the operator actually set: a full dump is ~50 keys of Kafka defaults. */
        private Map<String, String> topicConfig(String topic) throws Exception {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config config = handle.await(handle.admin().describeConfigs(List.of(resource)).all())
                    .get(resource);
            Map<String, String> interesting = new LinkedHashMap<>();
            if (config == null) {
                return interesting;
            }
            for (String key : INTERESTING_CONFIGS) {
                ConfigEntry entry = config.get(key);
                if (entry != null && entry.value() != null) {
                    interesting.put(key, entry.value());
                }
            }
            return interesting;
        }

        private static String summarise(String topic, List<Map<String, Object>> partitions, boolean underReplicated) {
            long total = partitions.stream()
                    .mapToLong(p -> ((Number) p.get("messages")).longValue())
                    .filter(m -> m >= 0)
                    .sum();
            StringBuilder sb = new StringBuilder(topic)
                    .append(": ")
                    .append(partitions.size())
                    .append(" partition(s), ~")
                    .append(total)
                    .append(" retained message(s)\n");
            for (Map<String, Object> p : partitions) {
                sb.append("  p")
                        .append(p.get("partition"))
                        .append(" offsets ")
                        .append(p.get("earliestOffset"))
                        .append('-')
                        .append(p.get("latestOffset"))
                        .append(" leader ")
                        .append(p.get("leader"))
                        .append(" isr ")
                        .append(p.get("inSyncReplicas"))
                        .append('/')
                        .append(p.get("replicas"))
                        .append('\n');
            }
            if (underReplicated) {
                sb.append("\nAt least one partition is under-replicated: fewer in-sync replicas than")
                        .append(" replicas. That is a cluster health problem, not a consumer problem.");
            }
            return sb.toString();
        }

        @Override
        public String backend() {
            return "kafka:" + handle.name();
        }
    }

    /** Sorted set of the topics a caller may read, for denial messages and filtering. */
    static Set<String> visibleTopics(PolicyProfile profile, Set<String> candidates) {
        Set<String> visible = new TreeSet<>();
        for (String candidate : candidates) {
            if (profile.isReadable(candidate)) {
                visible.add(candidate.toLowerCase(Locale.ROOT));
            }
        }
        return visible;
    }
}
