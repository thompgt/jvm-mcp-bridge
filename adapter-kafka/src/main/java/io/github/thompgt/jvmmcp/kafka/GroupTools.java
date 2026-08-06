package io.github.thompgt.jvmmcp.kafka;

import io.github.thompgt.jvmmcp.core.Arguments;
import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.Schemas;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.PolicyEngine;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.modelcontextprotocol.spec.McpSchema;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Consumer group inspection and lag.
 *
 * <p>A group is visible only if it touches a topic the caller may read — committed offsets and
 * member assignments both count. That check is not ceremony: a group id and its topic
 * assignment name the systems a company runs and the data they move, and listing every group
 * would describe the parts of the cluster the allowlist exists to hide.
 */
final class GroupTools {

    private GroupTools() {}

    static List<BridgeTool> create(KafkaBrokerHandle handle, PolicyEngine policy) {
        return List.of(
                new ListGroupsTool(handle, policy),
                new DescribeGroupTool(handle, policy),
                new ConsumerLagTool(handle, policy));
    }

    /** {@code kafka.list_groups} — which consumers exist on the topics this caller can see. */
    static final class ListGroupsTool implements BridgeTool {
        private final KafkaBrokerHandle handle;
        private final PolicyEngine policy;

        ListGroupsTool(KafkaBrokerHandle handle, PolicyEngine policy) {
            this.handle = handle;
            this.policy = policy;
        }

        @Override
        public McpSchema.Tool descriptor() {
            Schemas.ObjectSchema group = Schemas.object()
                    .optionalString("group", "Consumer group id.")
                    // Upper-cased on the way out, everywhere. GroupState.toString() renders
                    // "Empty", and a tool description that says EMPTY while the data says Empty
                    // is exactly the drift that makes a model's string comparison miss.
                    .optionalString("state", "STABLE, EMPTY, PREPARING_REBALANCE, DEAD, …")
                    .optionalInteger("members", "Live members.", 0, Integer.MAX_VALUE)
                    .optionalArrayOfStrings("topics", "Readable topics this group consumes.");

            Map<String, Object> output = Schemas.object()
                    .arrayOfObjects("groups", "One entry per visible consumer group.", group)
                    .optionalInteger("count", "How many.", 0, Integer.MAX_VALUE)
                    .build();

            return McpSchema.Tool.builder("kafka.list_groups", Schemas.object().build())
                    .title("List consumer groups")
                    .description("Lists the consumer groups on '" + handle.name()
                            + "' that consume at least one topic you are allowed to read.\n\n"
                            + "A group in state EMPTY has committed offsets but no live consumer —"
                            + " that is a stopped or crashed application, and it is the usual"
                            + " explanation for lag that only grows. Use kafka.consumer_lag next to"
                            + " see how far behind one of these is.")
                    .outputSchema(output)
                    .annotations(McpSchema.ToolAnnotations.builder()
                            .title("List consumer groups")
                            .readOnlyHint(true)
                            .destructiveHint(false)
                            .openWorldHint(false)
                            .build())
                    .build();
        }

        @Override
        public ToolOutcome call(Map<String, Object> arguments) {
            PolicyProfile effective = policy.effectiveProfile();

            return policy.guardRead("kafka.list_groups", List.of(), 0, limits -> {
                // listConsumerGroups, not the newer listGroups: listGroups speaks a ListGroups
                // API version that Kafka-protocol brokers which are not Kafka do not all
                // implement, and against one that does not it returns an empty list rather
                // than an error — a silently empty result from a security-adjacent listing is
                // the worst of the two failure modes. Verified against Redpanda 25.1.
                List<String> ids = handle.await(handle.admin().listConsumerGroups().valid()).stream()
                        .map(ConsumerGroupListing::groupId)
                        // Never report the bridge's own peek groups: they are an artefact of a
                        // previous tool call, and a model shown one will try to explain it.
                        .filter(id -> !id.startsWith(KafkaBrokerHandle.PEEK_GROUP_PREFIX))
                        .sorted()
                        .toList();

                if (ids.isEmpty()) {
                    return ToolOutcome.success(
                            Map.of("groups", List.of(), "count", 0),
                            "No consumer groups exist on '" + handle.name() + "'.",
                            0);
                }

                Map<String, ConsumerGroupDescription> described =
                        handle.await(handle.admin().describeConsumerGroups(ids).all());
                Map<String, Map<TopicPartition, OffsetAndMetadata>> committed = committedFor(ids);

                List<Map<String, Object>> groups = new ArrayList<>();
                for (String id : ids) {
                    ConsumerGroupDescription description = described.get(id);
                    Set<String> topics = readableTopics(effective, description, committed.get(id));
                    if (topics.isEmpty()) {
                        continue;
                    }
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("group", id);
                    row.put("state", description == null ? "UNKNOWN" : description.groupState().toString().toUpperCase(Locale.ROOT));
                    row.put("members", description == null ? 0 : description.members().size());
                    row.put("topics", List.copyOf(topics));
                    groups.add(row);
                }

                Map<String, Object> structured = Map.of("groups", groups, "count", groups.size());
                String summary = groups.isEmpty()
                        ? "No consumer group on '" + handle.name() + "' consumes a topic you can read."
                        : groups.size() + " group(s): "
                                + groups.stream()
                                        .map(g -> g.get("group") + " (" + g.get("state") + ", " + g.get("members")
                                                + " member(s))")
                                        .toList();
                return ToolOutcome.success(structured, summary, groups.size());
            });
        }

        private Map<String, Map<TopicPartition, OffsetAndMetadata>> committedFor(List<String> ids) throws Exception {
            Map<String, org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec> specs = new LinkedHashMap<>();
            ids.forEach(id -> specs.put(id, new org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec()));
            // One request for every group: a per-group call would make listing cost a round
            // trip per group, which on a real cluster is the difference between a usable tool
            // and one the model gives up waiting for.
            return handle.await(handle.admin().listConsumerGroupOffsets(specs).all());
        }

        @Override
        public String backend() {
            return "kafka:" + handle.name();
        }
    }

    /** {@code kafka.describe_group} — members, assignment, and who owns which partition. */
    static final class DescribeGroupTool implements BridgeTool {
        private final KafkaBrokerHandle handle;
        private final PolicyEngine policy;

        DescribeGroupTool(KafkaBrokerHandle handle, PolicyEngine policy) {
            this.handle = handle;
            this.policy = policy;
        }

        @Override
        public McpSchema.Tool descriptor() {
            Map<String, Object> input = Schemas.object()
                    .requiredString("group", "Consumer group id, as returned by kafka.list_groups.")
                    .build();

            Schemas.ObjectSchema member = Schemas.object()
                    .optionalString("consumerId", "Member id assigned by the coordinator.")
                    .optionalString("clientId", "Client id the application set.")
                    .optionalString("host", "Where it is connected from.")
                    .optionalArrayOfStrings("assignment", "Readable partitions it owns, as topic-partition.");

            Map<String, Object> output = Schemas.object()
                    .optionalString("group", "The group described.")
                    .optionalString("state", "Group state.")
                    .optionalString("partitionAssignor", "Assignment strategy in use.")
                    .optionalString("coordinator", "Broker coordinating the group.")
                    .arrayOfObjects("members", "One entry per live member.", member)
                    .optionalArrayOfStrings("unassignedPartitions", "Readable partitions with no live owner.")
                    .build();

            return McpSchema.Tool.builder("kafka.describe_group", input)
                    .title("Describe a consumer group")
                    .description("Returns the members of one consumer group on '" + handle.name()
                            + "', where each is connected from, and which partitions it owns.\n\n"
                            + "Only partitions on topics you may read are shown, so a member's"
                            + " assignment may look smaller here than it is.\n\n"
                            + "unassignedPartitions is the one to look at first: a partition the"
                            + " group has committed offsets for but no member owns is not being"
                            + " consumed by anyone, and its lag will grow without bound.")
                    .outputSchema(output)
                    .annotations(McpSchema.ToolAnnotations.builder()
                            .title("Describe a consumer group")
                            .readOnlyHint(true)
                            .destructiveHint(false)
                            .openWorldHint(false)
                            .build())
                    .build();
        }

        @Override
        public ToolOutcome call(Map<String, Object> arguments) {
            String group = new Arguments(arguments).requireString("group").trim();
            PolicyProfile effective = policy.effectiveProfile();

            // Guarded as a metadata read and filtered on the way out, like schema.list_tables:
            // a group id is not itself a resource on the allowlist, and the data that is —
            // the topics — is not known until the broker answers.
            return policy.guardRead("kafka.describe_group", List.of(), 0, limits -> {
                ConsumerGroupDescription description = handle.await(
                                handle.admin().describeConsumerGroups(List.of(group)).all())
                        .get(group);
                Map<TopicPartition, OffsetAndMetadata> committed = handle.await(
                        handle.admin().listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata());

                if (description == null || readableTopics(effective, description, committed).isEmpty()) {
                    // Same answer whether the group does not exist or consumes nothing visible.
                    // Distinguishing them would confirm the existence of a group on a topic the
                    // allowlist hides, which is the fact it is hiding.
                    return ToolOutcome.failure("no consumer group '" + group + "' is visible on '" + handle.name()
                            + "'. Call kafka.list_groups to see the groups you can inspect.");
                }

                Set<String> owned = new TreeSet<>();
                List<Map<String, Object>> members = new ArrayList<>();
                for (MemberDescription member : description.members()) {
                    List<String> assignment = member.assignment().topicPartitions().stream()
                            .filter(tp -> effective.isReadable(tp.topic()))
                            .map(TopicPartition::toString)
                            .sorted()
                            .toList();
                    owned.addAll(assignment);

                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("consumerId", member.consumerId());
                    row.put("clientId", member.clientId());
                    row.put("host", member.host());
                    row.put("assignment", assignment);
                    members.add(row);
                }

                List<String> unassigned = committed.keySet().stream()
                        .filter(tp -> effective.isReadable(tp.topic()))
                        .map(TopicPartition::toString)
                        .filter(tp -> !owned.contains(tp))
                        .sorted()
                        .toList();

                Map<String, Object> structured = new LinkedHashMap<>();
                structured.put("group", group);
                structured.put("state", description.groupState().toString().toUpperCase(Locale.ROOT));
                structured.put("partitionAssignor", description.partitionAssignor());
                structured.put(
                        "coordinator",
                        description.coordinator() == null ? "unknown" : description.coordinator().idString());
                structured.put("members", members);
                structured.put("unassignedPartitions", unassigned);

                return ToolOutcome.success(structured, summarise(group, description, members, unassigned), members.size());
            });
        }

        private static String summarise(
                String group,
                ConsumerGroupDescription description,
                List<Map<String, Object>> members,
                List<String> unassigned) {
            StringBuilder sb = new StringBuilder(group)
                    .append(": ")
                    .append(description.groupState().toString().toUpperCase(Locale.ROOT))
                    .append(", ")
                    .append(members.size())
                    .append(" member(s)\n");
            for (Map<String, Object> member : members) {
                sb.append("  ")
                        .append(member.get("clientId"))
                        .append(" @ ")
                        .append(member.get("host"))
                        .append(" owns ")
                        .append(member.get("assignment"))
                        .append('\n');
            }
            if (!unassigned.isEmpty()) {
                sb.append("\nNo live member owns: ")
                        .append(String.join(", ", unassigned))
                        .append(". Nothing is consuming those partitions, so their lag grows until")
                        .append(" a consumer joins.");
            }
            return sb.toString();
        }

        @Override
        public String backend() {
            return "kafka:" + handle.name();
        }
    }

    /** {@code kafka.consumer_lag} — how far behind, per partition, as of a stated moment. */
    static final class ConsumerLagTool implements BridgeTool {
        private final KafkaBrokerHandle handle;
        private final PolicyEngine policy;

        ConsumerLagTool(KafkaBrokerHandle handle, PolicyEngine policy) {
            this.handle = handle;
            this.policy = policy;
        }

        @Override
        public McpSchema.Tool descriptor() {
            Map<String, Object> input = Schemas.object()
                    .requiredString("group", "Consumer group id.")
                    .optionalString("topic", "Restrict to one topic. Omit for every readable topic the group consumes.")
                    .build();

            Schemas.ObjectSchema partition = Schemas.object()
                    .optionalString("topic", "Topic name.")
                    .optionalInteger("partition", "Partition number.", 0, Integer.MAX_VALUE)
                    .optionalInteger("committedOffset", "Last offset the group committed, -1 if none.")
                    .optionalInteger("endOffset", "Next offset to be written, at snapshotTime.")
                    .optionalInteger("lag", "endOffset - committedOffset, -1 when the group has never committed.")
                    .optionalBoolean("hasOwner", "Whether a live member currently owns this partition.");

            Map<String, Object> output = Schemas.object()
                    .optionalString("group", "The group measured.")
                    .optionalString("state", "Group state at the time of the reading.")
                    .optionalString("snapshotTime", "ISO-8601 instant the end offsets were read.")
                    .optionalInteger("totalLag", "Sum of lag across the partitions reported.")
                    .optionalString("worstPartition", "Topic-partition carrying the most lag.")
                    .arrayOfObjects("partitions", "Per-partition breakdown, most lag first.", partition)
                    .build();

            return McpSchema.Tool.builder("kafka.consumer_lag", input)
                    .title("Measure consumer group lag")
                    .description("Reports how far behind a consumer group is on '" + handle.name()
                            + "', broken down by partition and sorted worst first.\n\n"
                            + "snapshotTime is the moment the end offsets were read, and it is in the"
                            + " result for a reason: lag is a difference between two moving numbers."
                            + " A reading is a photograph, not a level. Do not compare two readings"
                            + " taken seconds apart and call the difference a trend, and do not"
                            + " report a lag figure without saying when it was taken.\n\n"
                            + "Lag concentrated in one partition usually means a hot key or a stuck"
                            + " consumer, not an under-provisioned group; lag spread evenly across"
                            + " every partition usually means the group cannot keep up. hasOwner=false"
                            + " means nothing is consuming that partition at all.")
                    .outputSchema(output)
                    .annotations(McpSchema.ToolAnnotations.builder()
                            .title("Measure consumer group lag")
                            .readOnlyHint(true)
                            .destructiveHint(false)
                            // Not idempotent: the honest answer changes between calls.
                            .idempotentHint(false)
                            .openWorldHint(false)
                            .build())
                    .build();
        }

        @Override
        public ToolOutcome call(Map<String, Object> arguments) {
            Arguments args = new Arguments(arguments);
            String group = args.requireString("group").trim();
            String topicFilter = args.optionalString("topic", "").trim();
            PolicyProfile effective = policy.effectiveProfile();

            List<String> resources = topicFilter.isEmpty() ? List.of() : List.of(topicFilter);

            return policy.guardRead("kafka.consumer_lag", resources, 0, limits -> {
                Map<TopicPartition, OffsetAndMetadata> committed = handle.await(
                        handle.admin().listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata());

                Map<TopicPartition, OffsetSpec> wanted = new LinkedHashMap<>();
                for (TopicPartition tp : committed.keySet()) {
                    if (!effective.isReadable(tp.topic())) {
                        continue;
                    }
                    if (!topicFilter.isEmpty() && !tp.topic().equalsIgnoreCase(topicFilter)) {
                        continue;
                    }
                    wanted.put(tp, OffsetSpec.latest());
                }

                if (wanted.isEmpty()) {
                    return ToolOutcome.failure("group '" + group + "' has no committed offsets on any topic you"
                            + " can read" + (topicFilter.isEmpty() ? "" : " matching '" + topicFilter + "'")
                            + ". Call kafka.list_groups to see the groups you can inspect.");
                }

                ConsumerGroupDescription description = handle.await(
                                handle.admin().describeConsumerGroups(List.of(group)).all())
                        .get(group);
                Set<TopicPartition> owned = new java.util.HashSet<>();
                if (description != null) {
                    description.members().forEach(m -> owned.addAll(m.assignment().topicPartitions()));
                }

                // Read after the committed offsets, and stamped: end offsets move while this
                // runs, so a lag computed from them is as of now, not as of the commit read.
                Map<TopicPartition, Long> ends = new LinkedHashMap<>();
                handle.await(handle.admin().listOffsets(wanted).all())
                        .forEach((tp, result) -> ends.put(tp, result.offset()));
                Instant snapshotTime = Instant.now();

                List<Map<String, Object>> rows = new ArrayList<>();
                long totalLag = 0;
                for (Map.Entry<TopicPartition, Long> entry : ends.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    long end = entry.getValue();
                    OffsetAndMetadata offset = committed.get(tp);
                    long commit = offset == null ? -1L : offset.offset();
                    long lag = commit < 0 ? -1L : Math.max(0L, end - commit);
                    if (lag > 0) {
                        totalLag += lag;
                    }

                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("topic", tp.topic());
                    row.put("partition", tp.partition());
                    row.put("committedOffset", commit);
                    row.put("endOffset", end);
                    row.put("lag", lag);
                    row.put("hasOwner", owned.contains(tp));
                    rows.add(row);
                }
                rows.sort(Comparator.comparingLong((Map<String, Object> r) -> ((Number) r.get("lag")).longValue())
                        .reversed());

                Map<String, Object> structured = new LinkedHashMap<>();
                structured.put("group", group);
                structured.put("state", description == null ? "UNKNOWN" : description.groupState().toString().toUpperCase(Locale.ROOT));
                structured.put("snapshotTime", snapshotTime.toString());
                structured.put("totalLag", totalLag);
                structured.put("worstPartition", rows.isEmpty() ? "" : rows.get(0).get("topic") + "-" + rows.get(0).get("partition"));
                structured.put("partitions", rows);

                return ToolOutcome.success(structured, summarise(group, totalLag, snapshotTime, rows), rows.size());
            });
        }

        private static String summarise(
                String group, long totalLag, Instant snapshotTime, List<Map<String, Object>> rows) {
            StringBuilder sb = new StringBuilder(group)
                    .append(": total lag ")
                    .append(totalLag)
                    .append(" across ")
                    .append(rows.size())
                    .append(" partition(s), as of ")
                    .append(snapshotTime)
                    .append("\n");
            for (Map<String, Object> row : rows) {
                sb.append("  ")
                        .append(row.get("topic"))
                        .append('-')
                        .append(row.get("partition"))
                        .append(": lag ")
                        .append(row.get("lag"))
                        .append(" (committed ")
                        .append(row.get("committedOffset"))
                        .append(", end ")
                        .append(row.get("endOffset"))
                        .append(')');
                if (Boolean.FALSE.equals(row.get("hasOwner"))) {
                    sb.append("  [no live consumer]");
                }
                sb.append('\n');
            }
            sb.append("\nThis is a reading taken at ")
                    .append(snapshotTime)
                    .append(", not a steady state. Say so when reporting it.");
            return sb.toString();
        }

        @Override
        public String backend() {
            return "kafka:" + handle.name();
        }
    }

    /** Topics of a group that this caller may read, from both its assignment and its commits. */
    private static Set<String> readableTopics(
            PolicyProfile profile,
            ConsumerGroupDescription description,
            Map<TopicPartition, OffsetAndMetadata> committed) {
        Set<String> topics = new TreeSet<>();
        if (description != null) {
            for (MemberDescription member : description.members()) {
                member.assignment().topicPartitions().stream()
                        .map(TopicPartition::topic)
                        .filter(profile::isReadable)
                        .forEach(topics::add);
            }
        }
        if (committed != null) {
            // Commits matter as much as assignment: a stopped consumer has no assignment, and
            // a stopped consumer is exactly the one someone is looking for.
            committed.keySet().stream()
                    .map(TopicPartition::topic)
                    .filter(profile::isReadable)
                    .forEach(topics::add);
        }
        return topics;
    }
}
