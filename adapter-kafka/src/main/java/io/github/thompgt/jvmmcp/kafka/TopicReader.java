package io.github.thompgt.jvmmcp.kafka;

import io.github.thompgt.jvmmcp.policy.EffectiveLimits;
import io.github.thompgt.jvmmcp.policy.Redactor;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.Header;

/**
 * The one place in this adapter that reads message bodies.
 *
 * <p>Shared by {@code kafka.peek} and {@code kafka.dlq_sample} rather than copied into each,
 * because the properties that make reading safe are properties of <em>this loop</em>: it
 * assigns partitions instead of subscribing, so the bridge never joins a consumer group and
 * never triggers a rebalance; it commits nothing; and it stops at whichever of the message,
 * byte and time caps is reached first. A second tool with its own read loop would be a second
 * chance to get one of those wrong.
 */
final class TopicReader {

    /** Per-message render cap. A single 1 MB JSON blob is not a thing a model reads usefully. */
    private static final int MAX_RENDERED_CHARS = 2_000;

    /** How long one poll may block. Short, so the byte and time caps are checked often. */
    private static final Duration POLL_SLICE = Duration.ofMillis(500);

    /** Consecutive empty polls before concluding there is nothing more within the bounds. */
    private static final int EMPTY_POLLS_BEFORE_STOPPING = 2;

    /** Makes each read group id unique within the process; the broker never sees two the same. */
    private static final AtomicLong NONCE = new AtomicLong();

    private TopicReader() {}

    /**
     * What one bounded read returned, and where it stopped.
     *
     * @param truncationReason {@code messages}, {@code bytes}, {@code time}, or empty when the
     *     read ran out of topic rather than out of budget
     */
    record Batch(
            List<Map<String, Object>> messages,
            Map<TopicPartition, Long> starts,
            Map<TopicPartition, Long> next,
            Map<TopicPartition, Long> ends,
            String truncationReason) {

        boolean truncated() {
            return !truncationReason.isEmpty();
        }
    }

    /** The partitions of a topic, optionally narrowed to one. Empty when that one does not exist. */
    static List<TopicPartition> partitionsOf(TopicDescription description, int partition) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (TopicPartitionInfo info : description.partitions()) {
            if (partition < 0 || info.partition() == partition) {
                partitions.add(new TopicPartition(description.name(), info.partition()));
            }
        }
        return partitions;
    }

    static Map<TopicPartition, Long> bounds(KafkaBrokerHandle handle, List<TopicPartition> partitions, OffsetSpec spec)
            throws Exception {
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        partitions.forEach(tp -> request.put(tp, spec));
        Map<TopicPartition, Long> resolved = new LinkedHashMap<>();
        handle.await(handle.admin().listOffsets(request).all())
                .forEach((tp, result) -> resolved.put(tp, result.offset()));
        return resolved;
    }

    /**
     * Reads up to the limits, from {@code fromOffset} or — when that is negative — from the tail.
     *
     * @param fromOffset applied to every partition read, clamped into each one's range
     */
    static Batch read(
            KafkaBrokerHandle handle,
            String topic,
            List<TopicPartition> assignment,
            Map<TopicPartition, Long> earliest,
            Map<TopicPartition, Long> latest,
            long fromOffset,
            EffectiveLimits limits,
            Redactor redactor) {

        int maxMessages = limits.maxRows();
        // Each partition gets its own share of the message budget, so one busy partition
        // cannot fill the whole result and hide that the others exist. This is the same
        // reason kafka.consumer_lag breaks lag down instead of totalling it.
        int perPartition = Math.max(1, maxMessages / assignment.size());

        Map<TopicPartition, Long> starts = new LinkedHashMap<>();
        Map<TopicPartition, Long> ends = new LinkedHashMap<>();
        for (TopicPartition tp : assignment) {
            long low = earliest.getOrDefault(tp, 0L);
            long high = latest.getOrDefault(tp, 0L);
            long start = fromOffset >= 0
                    ? Math.max(fromOffset, low)
                    // Default: the tail. Someone asking what is on a topic means the recent
                    // messages, and starting at the beginning of a retained log answers a
                    // question nobody asked with data that is usually irrelevant.
                    : Math.max(low, high - perPartition);
            starts.put(tp, Math.min(start, high));
            ends.put(tp, high);
        }

        List<Map<String, Object>> messages = new ArrayList<>();
        Map<TopicPartition, Long> next = new LinkedHashMap<>(starts);
        if (starts.entrySet().stream().allMatch(e -> e.getValue() >= ends.get(e.getKey()))) {
            return new Batch(messages, starts, next, ends, "");
        }

        long budgetBytes = limits.maxResultBytes();
        long usedBytes = 0;
        String truncationReason = "";

        String groupId = KafkaBrokerHandle.peekGroupId(handle.name(), topic, NONCE.incrementAndGet());
        long deadline = System.nanoTime() + limits.timeout().toNanos();

        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(handle.consumerConfig(groupId, maxMessages, budgetBytes))) {
            // assign, never subscribe: subscribe would make this process a member of groupId and
            // put it through the rebalance protocol. assign takes the partitions directly and
            // joins nothing.
            consumer.assign(assignment);
            starts.forEach(consumer::seek);

            int emptyPolls = 0;
            while (messages.size() < maxMessages) {
                if (System.nanoTime() >= deadline) {
                    truncationReason = "time";
                    break;
                }
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(POLL_SLICE);
                if (polled.isEmpty()) {
                    if (++emptyPolls >= EMPTY_POLLS_BEFORE_STOPPING) {
                        break;
                    }
                    continue;
                }
                emptyPolls = 0;

                for (ConsumerRecord<byte[], byte[]> record : polled) {
                    if (messages.size() >= maxMessages) {
                        truncationReason = "messages";
                        break;
                    }
                    long size = sizeOf(record);
                    // The first message is admitted whatever its size: a result saying "one
                    // message, too large to show" is less useful than the message.
                    if (usedBytes + size > budgetBytes && !messages.isEmpty()) {
                        truncationReason = "bytes";
                        break;
                    }
                    usedBytes += size;
                    messages.add(render(record, redactor));
                    next.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                }
                if (!truncationReason.isEmpty()) {
                    break;
                }
                // Stop polling partitions that have reached the end offsets read up front, so a
                // topic being actively written to cannot keep this call going indefinitely.
                List<TopicPartition> finished = assignment.stream()
                        .filter(tp -> next.getOrDefault(tp, 0L) >= ends.getOrDefault(tp, 0L))
                        .toList();
                consumer.pause(finished);
                if (finished.size() == assignment.size()) {
                    break;
                }
            }
            if (truncationReason.isEmpty() && messages.size() >= maxMessages) {
                truncationReason = "messages";
            }
        }

        messages.sort(Comparator.comparingInt((Map<String, Object> m) -> ((Number) m.get("partition")).intValue())
                .thenComparingLong(m -> ((Number) m.get("offset")).longValue()));

        return new Batch(messages, starts, next, ends, truncationReason);
    }

    /** Wire size, near enough: this bounds a result, it is not accounting. */
    private static long sizeOf(ConsumerRecord<byte[], byte[]> record) {
        long size = (record.key() == null ? 0 : record.key().length)
                + (record.value() == null ? 0 : record.value().length);
        for (Header header : record.headers()) {
            size += header.key().length() + (header.value() == null ? 0 : header.value().length);
        }
        return size;
    }

    private static Map<String, Object> render(ConsumerRecord<byte[], byte[]> record, Redactor redactor) {
        Rendered key = decode(record.key());
        Rendered value = decode(record.value());
        String topic = record.topic();

        Map<String, Object> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            // Header names are the one part of a message with conventional meaning, so they are
            // what a redaction pattern can actually target — `orders.dlq.authorization`.
            headers.put(header.key(), redactor.apply(topic, header.key(), decode(header.value()).text()));
        }

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("partition", record.partition());
        row.put("offset", record.offset());
        row.put("timestamp", Instant.ofEpochMilli(record.timestamp()).toString());
        // `key` and `value` as pseudo-columns: an operator who knows a topic carries PII in the
        // body can write `orders.events.value` and have the payload withheld rather than having
        // to remove the topic from the allowlist entirely.
        row.put("key", redactor.apply(topic, "key", key.text()));
        row.put("value", redactor.apply(topic, "value", value.text()));
        row.put("valueEncoding", value.encoding());
        row.put("valueBytes", record.value() == null ? 0L : (long) record.value().length);
        row.put("headers", headers);
        row.put("truncated", key.truncated() || value.truncated());
        return row;
    }

    /** A rendered byte payload and how it had to be rendered. */
    private record Rendered(String text, String encoding, boolean truncated) {}

    /**
     * UTF-8 when the bytes are valid UTF-8, base64 otherwise.
     *
     * <p>Strict decoding, not the replacement-character kind: a lossy render of an Avro payload
     * looks like a corrupted string, and a model shown one will report data corruption. Base64
     * with the encoding named lets it say "this topic is binary" instead.
     */
    private static Rendered decode(byte[] bytes) {
        if (bytes == null) {
            return new Rendered(null, "utf-8", false);
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return truncate(decoder.decode(ByteBuffer.wrap(bytes)).toString(), "utf-8");
        } catch (CharacterCodingException e) {
            return truncate(Base64.getEncoder().encodeToString(bytes), "base64");
        }
    }

    private static Rendered truncate(String text, String encoding) {
        if (text.length() <= MAX_RENDERED_CHARS) {
            return new Rendered(text, encoding, false);
        }
        return new Rendered(
                text.substring(0, MAX_RENDERED_CHARS) + "… [truncated, " + text.length() + " chars]",
                encoding,
                true);
    }

    /** Offsets keyed by partition number as a string, because JSON object keys are strings. */
    static Map<String, Object> stringKeyed(Map<TopicPartition, Long> offsets) {
        Map<String, Object> out = new LinkedHashMap<>();
        offsets.forEach((tp, offset) -> out.put(String.valueOf(tp.partition()), offset));
        return out;
    }
}
