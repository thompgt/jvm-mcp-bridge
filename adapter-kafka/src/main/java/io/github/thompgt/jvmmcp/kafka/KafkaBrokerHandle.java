package io.github.thompgt.jvmmcp.kafka;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Owns the {@link Admin} client for one broker, and the client settings every call inherits.
 *
 * <p>Almost all of this class is timeout configuration, and that is the point. An
 * {@code AdminClient} pointed at an address that is not a broker does not fail — it retries,
 * quietly, for two minutes, because its defaults are tuned for an application that would rather
 * wait out a rolling restart than error. Here the caller is a model waiting on a tool call, and
 * a two-minute hang is indistinguishable from a broken server: the client times out, the model
 * retries, and now there are two hanging calls. Every bound below is set so a misconfigured or
 * unreachable broker comes back as a refusal within the policy timeout.
 */
public final class KafkaBrokerHandle implements AutoCloseable {

    private final String name;
    private final String bootstrapServers;
    private final Duration requestTimeout;
    private final Map<String, String> clientProperties;
    private final Admin admin;

    /**
     * @param clientProperties raw Kafka client settings, for the security config a real
     *     deployment needs ({@code security.protocol}, {@code sasl.jaas.config}, truststores).
     *     Applied first, so the timeouts below cannot be weakened from configuration.
     */
    public KafkaBrokerHandle(
            String name, String bootstrapServers, Duration requestTimeout, Map<String, String> clientProperties) {
        this.name = name;
        this.bootstrapServers = bootstrapServers;
        this.requestTimeout = requestTimeout;
        this.clientProperties = Map.copyOf(clientProperties);
        this.admin = Admin.create(adminConfig());
    }

    private Properties adminConfig() {
        Properties props = new Properties();
        props.putAll(clientProperties);
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "jvm-mcp-bridge-admin-" + name);

        long millis = requestTimeout.toMillis();
        // One request attempt, and the whole call, both bounded by the policy timeout. Without
        // DEFAULT_API_TIMEOUT the client retries a failed request until 60s regardless of
        // REQUEST_TIMEOUT, which is the part that surprises people.
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) millis);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) millis);
        // Bounded reconnect backoff, so a broker that comes back is picked up promptly rather
        // than after an exponential wait that outlasts the call that would have used it.
        props.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, Math.min(millis, 5_000L));
        // Retries are deliberately NOT capped. Capping them at 1 looks like it belongs with the
        // timeouts above and is not the same kind of bound: some admin responses are retriable by
        // design rather than by failure. A batched offset fetch across several consumer groups
        // whose coordinators are different brokers answers NOT_COORDINATOR for the ones the first
        // broker does not own, and the client is expected to re-route and ask again. One retry
        // turns that normal exchange into "Exceeded maxRetries after 2 tries" as soon as a cluster
        // has enough groups to spread across coordinators. DEFAULT_API_TIMEOUT_MS above is the
        // real bound and bounds the whole call however many attempts it takes.
        return props;
    }

    /**
     * Consumer settings for a bounded read.
     *
     * <p>Three of these are load-bearing and are set here rather than at each call site, so a
     * new tool cannot forget one: auto-commit is off and the group id is unique, so reading a
     * topic never moves a real consumer group's offsets; and {@code auto.offset.reset=none}
     * turns "the offset you asked for does not exist" into an error the caller can report
     * instead of silently reading from a different place than it said it would.
     *
     * @param groupId must be unique per call; see {@link #peekGroupId}
     */
    Properties consumerConfig(String groupId, int maxRecords, long maxBytes) {
        Properties props = new Properties();
        props.putAll(clientProperties);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "jvm-mcp-bridge-peek-" + name);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxRecords);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int) Math.min(maxBytes, Integer.MAX_VALUE));
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (int) Math.min(maxBytes, Integer.MAX_VALUE));
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) requestTimeout.toMillis());
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) requestTimeout.toMillis());
        // Bytes in, bytes out: this bridge shows a model what is on the topic, and guessing a
        // deserializer would turn a schema mismatch into a failed call instead of a raw value.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    /**
     * Producer settings for the gated write path.
     *
     * <p>Built per call rather than held open, and that is deliberate: a bridge that is in
     * read-only mode — which is every deployment that has not opted in — should not be holding a
     * producer at all, and a connection that only exists inside a permitted write is one fewer
     * thing to reason about when asking what this process can do to a cluster.
     *
     * <p>{@code acks=all} because a produce that is reported as done and then lost on a broker
     * failure is worse here than anywhere else: the model will tell someone the message was
     * replayed. {@code max.block.ms} is bounded for the same reason the admin timeouts are — the
     * default makes an unknown topic a sixty-second hang rather than an error.
     */
    Properties producerConfig() {
        Properties props = new Properties();
        props.putAll(clientProperties);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "jvm-mcp-bridge-produce-" + name);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, requestTimeout.toMillis());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) requestTimeout.toMillis());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) requestTimeout.toMillis());
        // Explicitly zero, and load-bearing rather than tidiness: the producer refuses to start
        // unless delivery.timeout >= linger + request.timeout, so leaving linger at its default
        // would make every one of these three values individually reasonable and the combination
        // a ConfigException at first write. Batching is pointless here anyway — this producer
        // exists to send exactly one record and then close.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return props;
    }

    /**
     * A group id that no real consumer uses.
     *
     * <p>Reusing an application's group id would make this bridge a member of it, triggering a
     * rebalance of the production consumers and — with auto-commit on anywhere in the path —
     * committing offsets for messages nothing processed. The prefix makes the origin obvious in
     * {@code kafka-consumer-groups --list} when someone finds one.
     */
    static String peekGroupId(String broker, String topic, long nonce) {
        return PEEK_GROUP_PREFIX + broker + "-" + topic + "-" + nonce;
    }

    /** Also how {@code kafka.list_groups} recognises and hides this bridge's own leftovers. */
    static final String PEEK_GROUP_PREFIX = "jvm-mcp-bridge-peek-";

    /**
     * Waits for an admin future within the request timeout.
     *
     * <p>Unwraps {@link ExecutionException} so callers see the Kafka error itself rather than a
     * wrapper whose message says nothing a model could act on.
     */
    <T> T await(KafkaFuture<T> future) throws Exception {
        try {
            return future.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception checked) {
                throw checked;
            }
            throw e;
        } catch (TimeoutException e) {
            throw new TimeoutException("broker '" + name + "' did not answer within " + requestTimeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    public Admin admin() {
        return admin;
    }

    public String name() {
        return name;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }


    @Override
    public void close() {
        // Bounded: close() otherwise waits for in-flight requests, and the request that is
        // hanging is exactly the reason the process is shutting down.
        admin.close(requestTimeout);
    }
}
