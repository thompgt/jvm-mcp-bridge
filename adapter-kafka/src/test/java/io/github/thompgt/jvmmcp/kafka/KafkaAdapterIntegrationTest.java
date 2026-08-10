package io.github.thompgt.jvmmcp.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.AccessMode;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

/**
 * The Kafka tools against a real broker, with a real lagging consumer.
 *
 * <p>Phase 3's acceptance criterion is that the model can identify <em>which partition</em> is
 * behind, so the fixture is built to make that a question with a wrong answer available: the
 * group commits on partition 0 and never on partition 1, so a tool that reported only a total
 * would look correct and be useless.
 */
@Tag("integration")
@Testcontainers
class KafkaAdapterIntegrationTest {

    private static final String TOPIC = "orders.events";
    private static final String DLQ_TOPIC = "orders.dlq";
    private static final String LEGACY_DLQ_TOPIC = "orders.dlq.legacy";
    private static final String OTHER_TOPIC = "internal.secrets";
    private static final String GROUP = "orders-processor";

    @Container
    private static final RedpandaContainer KAFKA = new RedpandaContainer("redpandadata/redpanda:v25.1.7");

    private static KafkaBrokerHandle handle;
    private AuditSink.RecordingAuditSink audit;
    private KafkaAdapter adapter;

    @BeforeAll
    static void seed() throws Exception {
        Properties admin = new Properties();
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(
                            new NewTopic(TOPIC, 2, (short) 1),
                            new NewTopic(DLQ_TOPIC, 1, (short) 1),
                            new NewTopic(LEGACY_DLQ_TOPIC, 1, (short) 1),
                            new NewTopic(OTHER_TOPIC, 1, (short) 1)))
                    .all()
                    .get();
        }

        Properties producer = new Properties();
        producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producer)) {
            for (int i = 0; i < 10; i++) {
                p.send(new ProducerRecord<>(TOPIC, 0, "k" + i, "message " + i)).get();
            }
            for (int i = 0; i < 25; i++) {
                p.send(new ProducerRecord<>(TOPIC, 1, "k" + i, "message " + i)).get();
            }
            p.send(new ProducerRecord<>(OTHER_TOPIC, "k", "must never reach a model")).get();
            seedDeadLetters(p);
        }

        // A group that consumed some of partition 0 and none of partition 1, then stopped.
        // That is the shape of the outage this adapter exists to explain.
        Properties consumer = new Properties();
        consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumer.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumer)) {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> commits = new HashMap<>();
            commits.put(
                    new TopicPartition(TOPIC, 0),
                    new org.apache.kafka.clients.consumer.OffsetAndMetadata(6L));
            commits.put(
                    new TopicPartition(TOPIC, 1),
                    new org.apache.kafka.clients.consumer.OffsetAndMetadata(0L));
            c.commitSync(commits);
        }

        handle = new KafkaBrokerHandle(
                "orders-kafka", KAFKA.getBootstrapServers(), Duration.ofSeconds(15), Map.of());
    }

    /**
     * A dead-letter topic shaped like a real one: a dominant failure, a smaller distinct failure,
     * and a pile of messages carrying no error header at all.
     *
     * <p>The unclassified pile is deliberately larger than the second real failure. Ordering by
     * count alone would put "we could not tell" at the top and bury the NullPointerException,
     * which is the finding. The failure detail carries a different sku on every message, so
     * grouping on the detail header has a wrong answer available: fourteen classes of one.
     */
    private static void seedDeadLetters(KafkaProducer<String, String> p) throws Exception {
        for (int i = 0; i < 7; i++) {
            p.send(dead(DLQ_TOPIC, "order-" + i, "com.example.OutOfStockException", "sku " + (88_214 + i)
                    + " unavailable"))
                    .get();
        }
        for (int i = 0; i < 3; i++) {
            p.send(dead(DLQ_TOPIC, "order-npe-" + i, "java.lang.NullPointerException", "shippingAddress is null"))
                    .get();
        }
        for (int i = 0; i < 4; i++) {
            p.send(new ProducerRecord<>(DLQ_TOPIC, "order-bare-" + i, "{\"orderId\":" + i + "}"))
                    .get();
        }
        for (int i = 0; i < 3; i++) {
            p.send(new ProducerRecord<>(
                            LEGACY_DLQ_TOPIC,
                            null,
                            "legacy-" + i,
                            "{\"orderId\":" + i + "}",
                            List.of(new RecordHeader("reason", "TIMEOUT".getBytes(StandardCharsets.UTF_8)))))
                    .get();
        }
    }

    private static ProducerRecord<String, String> dead(String topic, String key, String fqcn, String detail) {
        return new ProducerRecord<>(
                topic,
                null,
                key,
                "{\"orderId\":\"" + key + "\"}",
                List.of(
                        new RecordHeader("x-exception-fqcn", fqcn.getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("x-exception-message", detail.getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("x-original-topic", TOPIC.getBytes(StandardCharsets.UTF_8))));
    }

    @AfterAll
    static void closeHandle() {
        if (handle != null) {
            handle.close();
        }
    }

    @BeforeEach
    void buildAdapter() {
        audit = new AuditSink.RecordingAuditSink();
        adapter = new KafkaAdapter(
                KafkaAdapterIntegrationTest.handle,
                PolicyProfile.builder("orders-kafka")
                        .mode(AccessMode.READ_ONLY)
                        .allowRead("orders.*")
                        .maxRows(25)
                        .maxResultBytes(512_000L)
                        .timeout(Duration.ofSeconds(15))
                        .build(),
                audit);
    }

    private ToolOutcome call(String tool, Map<String, Object> arguments) {
        BridgeTool found = adapter.tools().stream()
                .filter(t -> t.descriptor().name().equals(tool))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no such tool: " + tool));
        return found.call(arguments);
    }

    @Test
    @DisplayName("only allowlisted topics are listed, and never internal ones")
    void listTopicsIsFiltered() {
        ToolOutcome outcome = call("kafka.list_topics", Map.of());

        assertThat(outcome.error()).isFalse();
        assertThat(outcome.summary()).contains(TOPIC).doesNotContain(OTHER_TOPIC);
        assertThat(outcome.summary()).doesNotContain("__consumer_offsets");
    }

    @Test
    @DisplayName("a topic off the allowlist is refused by name, with the list that would work")
    void describeTopicRespectsTheAllowlist() {
        ToolOutcome outcome = call("kafka.describe_topic", Map.of("topic", OTHER_TOPIC));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains(OTHER_TOPIC).contains("orders.*");
        assertThat(audit.records()).anyMatch(record -> !record.allowed());
    }

    @Test
    void describeTopicReportsPartitionsAndOffsets() {
        ToolOutcome outcome = call("kafka.describe_topic", Map.of("topic", TOPIC));

        assertThat(outcome.error()).isFalse();
        Map<?, ?> structured = (Map<?, ?>) outcome.structured();
        assertThat(structured.get("partitions")).isEqualTo(2);
        assertThat(outcome.summary()).contains("p0 offsets 0-10").contains("p1 offsets 0-25");
    }

    @Test
    @DisplayName("lag is reported per partition, worst first, with the time it was read")
    void consumerLagIdentifiesTheWorstPartition() {
        ToolOutcome outcome = call("kafka.consumer_lag", Map.of("group", GROUP));

        assertThat(outcome.error()).isFalse();
        Map<?, ?> structured = (Map<?, ?>) outcome.structured();

        // p0: 10 written, 6 committed -> 4. p1: 25 written, 0 committed -> 25.
        assertThat(structured.get("totalLag")).isEqualTo(29L);
        assertThat(structured.get("worstPartition")).isEqualTo(TOPIC + "-1");
        assertThat(structured.get("snapshotTime")).asString().isNotEmpty();

        List<?> partitions = (List<?>) structured.get("partitions");
        assertThat(((Map<?, ?>) partitions.get(0)).get("lag")).isEqualTo(25L);
        assertThat(((Map<?, ?>) partitions.get(1)).get("lag")).isEqualTo(4L);

        // The model is told this is a reading, not a level. Lag reported without a time is
        // the mistake that turns a recovering consumer into an incident.
        assertThat(outcome.summary()).contains("reading taken at");
    }

    @Test
    @DisplayName("a group is invisible if it consumes nothing this caller may read")
    void groupsAreFilteredByTopicAllowlist() {
        KafkaAdapter narrow = new KafkaAdapter(
                handle,
                PolicyProfile.builder("orders-kafka")
                        .mode(AccessMode.READ_ONLY)
                        .allowRead("shipments.*")
                        .maxRows(25)
                        .maxResultBytes(512_000L)
                        .timeout(Duration.ofSeconds(15))
                        .build(),
                audit);

        ToolOutcome listed = narrow.tools().stream()
                .filter(t -> t.descriptor().name().equals("kafka.list_groups"))
                .findFirst()
                .orElseThrow()
                .call(Map.of());
        assertThat(listed.summary()).doesNotContain(GROUP);

        ToolOutcome described = narrow.tools().stream()
                .filter(t -> t.descriptor().name().equals("kafka.describe_group"))
                .findFirst()
                .orElseThrow()
                .call(Map.of("group", GROUP));
        // Same message a genuinely absent group gets: confirming it exists would confirm a
        // group on a topic the allowlist is hiding.
        assertThat(described.error()).isTrue();
        assertThat(described.summary()).contains("no consumer group").contains(GROUP);
    }

    @Test
    void listGroupsShowsTheStoppedConsumer() {
        ToolOutcome outcome = call("kafka.list_groups", Map.of());

        assertThat(outcome.error()).isFalse();
        // No live member: the group committed and went away, which is what EMPTY means.
        assertThat(outcome.summary()).contains(GROUP).contains("EMPTY");
    }

    @Test
    void describeGroupNamesThePartitionsNobodyOwns() {
        ToolOutcome outcome = call("kafka.describe_group", Map.of("group", GROUP));

        assertThat(outcome.error()).isFalse();
        Map<?, ?> structured = (Map<?, ?>) outcome.structured();
        assertThat((List<?>) structured.get("unassignedPartitions")).hasSize(2);
        assertThat(String.valueOf(structured.get("unassignedPartitions")))
                .contains(TOPIC + "-0")
                .contains(TOPIC + "-1");
        assertThat(outcome.summary()).contains("No live member owns");
    }

    @Test
    @DisplayName("peek from an explicit offset returns those messages and where to continue")
    void peekReadsFromAnExplicitOffset() {
        ToolOutcome outcome = call("kafka.peek", Map.of("topic", TOPIC, "partition", 0, "from_offset", 6));

        assertThat(outcome.error()).isFalse();
        Map<?, ?> structured = (Map<?, ?>) outcome.structured();

        // Offsets 6..9 of a partition holding 10 messages.
        assertThat(structured.get("returned")).isEqualTo(4);
        List<?> messages = (List<?>) structured.get("messages");
        Map<?, ?> first = (Map<?, ?>) messages.get(0);
        assertThat(first.get("offset")).isEqualTo(6L);
        assertThat(first.get("value")).isEqualTo("message 6");
        assertThat(first.get("valueEncoding")).isEqualTo("utf-8");

        // The resume point, so paging is an explicit read rather than a consumer this process
        // has to keep alive between tool calls.
        assertThat(((Map<?, ?>) structured.get("nextOffsets")).get("0")).isEqualTo(10L);
    }

    @Test
    @DisplayName("peek defaults to the tail and commits nothing")
    void peekNeverMovesARealConsumersOffset() throws Exception {
        ToolOutcome outcome = call("kafka.peek", Map.of("topic", TOPIC));

        assertThat(outcome.error()).isFalse();
        Map<?, ?> structured = (Map<?, ?>) outcome.structured();
        // 25 message budget over 2 partitions: 12 from the tail of each, and p0 only has 10.
        assertThat((Integer) structured.get("returned")).isEqualTo(22);
        assertThat(outcome.summary()).contains("No offsets were committed");

        // The point of the tool: the stuck consumer is exactly as stuck as it was.
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin client = Admin.create(props)) {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed = client
                    .listConsumerGroupOffsets(GROUP)
                    .partitionsToOffsetAndMetadata()
                    .get();
            assertThat(committed.get(new TopicPartition(TOPIC, 0)).offset()).isEqualTo(6L);
            assertThat(committed.get(new TopicPartition(TOPIC, 1)).offset()).isEqualTo(0L);

            // And it left no group of its own behind for the next person to wonder about.
            assertThat(client.listConsumerGroups().valid().get())
                    .noneMatch(g -> g.groupId().startsWith(KafkaBrokerHandle.PEEK_GROUP_PREFIX));
        }
    }

    @Test
    @DisplayName("peek says which cap stopped it and where to resume")
    void peekReportsTruncation() {
        ToolOutcome outcome =
                call("kafka.peek", Map.of("topic", TOPIC, "partition", 1, "from_offset", 0, "max_messages", 5));

        assertThat(outcome.error()).isFalse();
        Map<?, ?> structured = (Map<?, ?>) outcome.structured();
        assertThat(structured.get("returned")).isEqualTo(5);
        assertThat(structured.get("truncated")).isEqualTo(true);
        assertThat(structured.get("truncationReason")).isEqualTo("messages");
        assertThat(((Map<?, ?>) structured.get("nextOffsets")).get("1")).isEqualTo(5L);
        assertThat(outcome.summary()).contains("Continue with from_offset");
    }

    @Test
    @DisplayName("peek is refused on a topic off the allowlist")
    void peekRespectsTheAllowlist() {
        ToolOutcome outcome = call("kafka.peek", Map.of("topic", OTHER_TOPIC));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains(OTHER_TOPIC).contains("orders.*");
        assertThat(String.valueOf(outcome.structured())).doesNotContain("must never reach a model");
    }

    @Test
    @DisplayName("dlq_sample groups by exception class, most frequent first, unclassified last")
    void dlqSampleGroupsByErrorClass() {
        ToolOutcome outcome = call("kafka.dlq_sample", Map.of("topic", DLQ_TOPIC));

        assertThat(outcome.error()).isFalse();
        Map<?, ?> structured = (Map<?, ?>) outcome.structured();
        assertThat(structured.get("scanned")).isEqualTo(14);
        assertThat(structured.get("errorHeader")).isEqualTo("x-exception-fqcn");

        List<?> classes = (List<?>) structured.get("classes");
        assertThat(classes).hasSize(3);

        Map<?, ?> worst = (Map<?, ?>) classes.get(0);
        assertThat(worst.get("error")).isEqualTo("com.example.OutOfStockException");
        assertThat(worst.get("count")).isEqualTo(7);
        assertThat(worst.get("sharePercent")).isEqualTo(50);
        // Evidence, not a dump: the class holds seven messages and returns two of them.
        assertThat((List<?>) worst.get("samples")).hasSize(2);
        assertThat(worst.get("detail")).asString().startsWith("sku ");
        assertThat(worst.get("partitions")).isEqualTo(List.of("0@0-6"));

        assertThat(((Map<?, ?>) classes.get(1)).get("error")).isEqualTo("java.lang.NullPointerException");
        assertThat(((Map<?, ?>) classes.get(1)).get("count")).isEqualTo(3);

        // Four unclassified against three NPEs: count alone would rank this second, and it is
        // last because "we could not tell" is not a finding.
        Map<?, ?> unclassified = (Map<?, ?>) classes.get(2);
        assertThat(unclassified.get("error")).isEqualTo("(no x-exception-fqcn header)");
        assertThat(unclassified.get("count")).isEqualTo(4);
    }

    @Test
    @DisplayName("dlq_sample reads the whole window and commits nothing")
    void dlqSampleNeverCommits() throws Exception {
        ToolOutcome outcome = call("kafka.dlq_sample", Map.of("topic", DLQ_TOPIC));

        assertThat(outcome.error()).isFalse();
        // 14 messages under a 25 cap: complete, so the counts describe the topic and the summary
        // must not hedge them as a sample.
        assertThat(((Map<?, ?>) outcome.structured()).get("truncated")).isEqualTo(false);
        assertThat(outcome.summary()).contains("complete for it").contains("No offsets were committed");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin client = Admin.create(props)) {
            assertThat(client.listConsumerGroups().valid().get())
                    .noneMatch(g -> g.groupId().startsWith(KafkaBrokerHandle.PEEK_GROUP_PREFIX));
        }
    }

    @Test
    @DisplayName("a per-occurrence value in the error string collapses instead of making a class each")
    void dlqSampleNormalisesVaryingDetail() {
        ToolOutcome outcome =
                call("kafka.dlq_sample", Map.of("topic", DLQ_TOPIC, "error_header", "x-exception-message"));

        assertThat(outcome.error()).isFalse();
        List<?> classes = (List<?>) ((Map<?, ?>) outcome.structured()).get("classes");

        // Seven distinct skus, one failure. Grouping on the raw string would produce seven
        // classes of one, which is the raw dump this tool exists to replace.
        Map<?, ?> worst = (Map<?, ?>) classes.get(0);
        assertThat(worst.get("error")).isEqualTo("sku <n> unavailable");
        assertThat(worst.get("count")).isEqualTo(7);
    }

    @Test
    @DisplayName("a truncated scan says the counts are a window, and where to scan on from")
    void dlqSampleReportsItsWindow() {
        ToolOutcome outcome = call("kafka.dlq_sample", Map.of("topic", DLQ_TOPIC, "from_offset", 0, "scan_messages", 5));

        assertThat(outcome.error()).isFalse();
        Map<?, ?> structured = (Map<?, ?>) outcome.structured();
        assertThat(structured.get("scanned")).isEqualTo(5);
        assertThat(structured.get("truncationReason")).isEqualTo("messages");
        assertThat(((Map<?, ?>) structured.get("nextOffsets")).get("0")).isEqualTo(5L);
        assertThat(outcome.summary()).contains("describe a window").contains("Scan further back with from_offset");
    }

    @Test
    @DisplayName("an unrecognised header convention names the headers that are there")
    void dlqSampleSaysWhatItCouldNotGroupOn() {
        ToolOutcome outcome = call("kafka.dlq_sample", Map.of("topic", LEGACY_DLQ_TOPIC));

        assertThat(outcome.error()).isFalse();
        Map<?, ?> structured = (Map<?, ?>) outcome.structured();
        assertThat(structured.get("errorHeader")).isEqualTo("");
        assertThat(structured.get("headerNames")).isEqualTo(List.of("reason"));

        // Model-actionable: the recovery is a named parameter and a header name it now has.
        assertThat(outcome.summary()).contains("Headers seen: reason").contains("error_header");

        ToolOutcome regrouped =
                call("kafka.dlq_sample", Map.of("topic", LEGACY_DLQ_TOPIC, "error_header", "reason"));
        List<?> classes = (List<?>) ((Map<?, ?>) regrouped.structured()).get("classes");
        assertThat(((Map<?, ?>) classes.get(0)).get("error")).isEqualTo("TIMEOUT");
        assertThat(((Map<?, ?>) classes.get(0)).get("count")).isEqualTo(3);
    }

    @Test
    @DisplayName("dlq_sample is refused on a topic off the allowlist")
    void dlqSampleRespectsTheAllowlist() {
        ToolOutcome outcome = call("kafka.dlq_sample", Map.of("topic", OTHER_TOPIC));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains(OTHER_TOPIC).contains("orders.*");
        assertThat(String.valueOf(outcome.structured())).doesNotContain("must never reach a model");
    }

    @Test
    void everyCallIsAudited() {
        call("kafka.list_topics", Map.of());

        assertThat(audit.records()).hasSize(1);
        assertThat(audit.records().get(0).tool()).isEqualTo("kafka.list_topics");
        assertThat(audit.records().get(0).backend()).isEqualTo("orders-kafka");
    }
}
