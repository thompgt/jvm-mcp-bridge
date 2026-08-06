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
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

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
        props.put(AdminClientConfig.RETRIES_CONFIG, 1);
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
     * A group id that no real consumer uses.
     *
     * <p>Reusing an application's group id would make this bridge a member of it, triggering a
     * rebalance of the production consumers and — with auto-commit on anywhere in the path —
     * committing offsets for messages nothing processed. The prefix makes the origin obvious in
     * {@code kafka-consumer-groups --list} when someone finds one.
     */
    static String peekGroupId(String broker, String topic, long nonce) {
        return "jvm-mcp-bridge-peek-" + broker + "-" + topic + "-" + nonce;
    }

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
