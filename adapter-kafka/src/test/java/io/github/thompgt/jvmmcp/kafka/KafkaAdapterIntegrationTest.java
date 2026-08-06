package io.github.thompgt.jvmmcp.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.AccessMode;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
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
                            new NewTopic(TOPIC, 2, (short) 1), new NewTopic(OTHER_TOPIC, 1, (short) 1)))
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
    void everyCallIsAudited() {
        call("kafka.list_topics", Map.of());

        assertThat(audit.records()).hasSize(1);
        assertThat(audit.records().get(0).tool()).isEqualTo("kafka.list_topics");
        assertThat(audit.records().get(0).backend()).isEqualTo("orders-kafka");
    }
}
