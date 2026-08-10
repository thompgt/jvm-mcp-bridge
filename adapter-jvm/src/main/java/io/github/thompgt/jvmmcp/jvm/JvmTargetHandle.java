package io.github.thompgt.jvmmcp.jvm;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The connection to one JVM, which is either this one or another one.
 *
 * <p>Both cases are an {@link MBeanServerConnection} and the tools cannot tell them apart, but
 * they are not equally safe and the difference is worth stating:
 *
 * <ul>
 *   <li><b>Embedded</b> — no URL configured, so the target is the JVM this bridge runs in. The
 *       platform {@code MBeanServer} is a field access; nothing can fail and nothing can hang.
 *       This is the mode where the bridge describes <em>itself</em>, which is useful mostly as a
 *       demonstration, because the interesting JVM is usually not the one running the bridge.
 *   <li><b>Sidecar</b> — a {@code service:jmx:rmi://…} URL, so every attribute read is a
 *       synchronous RMI round trip to another process. That process may be paused in a full GC,
 *       swapping, or gone; RMI's default is to wait on it more or less forever.
 * </ul>
 *
 * <p>So every call goes through {@link #call}, which bounds it. That the bound is imposed out
 * here rather than configured on the connector is not a shortcut: the JMX RMI transport takes
 * its read timeout from a JVM-wide system property ({@code sun.rmi.transport.tcp.responseTimeout}),
 * and a library that sets a global property to fix its own timeouts is a library that breaks
 * whatever else the host application does with RMI. A future on a bounded wait is local, honest,
 * and per call.
 *
 * <p>The cost of that choice is stated rather than hidden: cancelling the future returns an
 * answer to the caller but does not necessarily stop the RMI call, which stays parked in its
 * socket read until the TCP stack gives up. A stuck call must therefore not be able to hold up
 * the calls after it, and the threads are daemons so a hung target cannot keep this process
 * alive at shutdown.
 *
 * <p>Two bounds keep that from becoming a leak. The connection is guarded by a timed lock rather
 * than {@code synchronized}: a thread blocked on a monitor cannot be interrupted, so with an
 * intrinsic lock every call after the first hung one parked forever and {@code cancel(true)} did
 * nothing for any of them. And the pool has a ceiling — an unbounded pool turns one unreachable
 * target into one leaked thread per tool call for as long as the model keeps asking.
 */
public final class JvmTargetHandle implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JvmTargetHandle.class);

    private final String name;
    private final String jmxUrl;
    private final Duration connectTimeout;
    private final String username;
    private final String password;
    private final Duration jfrMaxDuration;
    private final ExecutorService calls;

    /**
     * How many JMX calls may be in flight against one target at once.
     *
     * <p>Sized for the leak, not for throughput: MCP calls arrive one or a few at a time, so this
     * is only ever reached by calls that are not coming back.
     */
    private static final int MAX_CONCURRENT_CALLS = 8;

    /** Guards {@link #connector}/{@link #connection}. Timed, so waiting on it cannot be forever. */
    private final ReentrantLock connectionLock = new ReentrantLock();

    private JMXConnector connector;
    private MBeanServerConnection connection;

    /**
     * @param jmxUrl a {@code service:jmx:…} URL, or {@code null}/blank to describe this JVM
     * @param username optional JMX credentials; ignored when embedded, where there is no
     *     authentication to do and no remote to do it against
     */
    public JvmTargetHandle(
            String name, String jmxUrl, Duration connectTimeout, String username, String password) {
        this(name, jmxUrl, connectTimeout, username, password, Duration.ofSeconds(30));
    }

    /**
     * @param jfrMaxDuration longest Flight Recorder run {@code jvm.jfr_snapshot} may ask for.
     *     Sits here rather than in the policy caps because it is not a bound on a result, it is a
     *     bound on how long this bridge may make the target carry profiling overhead — and it has
     *     to outlast the ordinary call timeout, which every other bound is derived from.
     */
    public JvmTargetHandle(
            String name,
            String jmxUrl,
            Duration connectTimeout,
            String username,
            String password,
            Duration jfrMaxDuration) {
        this.name = name;
        this.jfrMaxDuration = jfrMaxDuration;
        this.jmxUrl = jmxUrl == null || jmxUrl.isBlank() ? null : jmxUrl.trim();
        this.connectTimeout = connectTimeout;
        this.username = username;
        this.password = password;

        AtomicInteger counter = new AtomicInteger();
        java.util.concurrent.ThreadFactory threads = runnable -> {
            Thread thread = new Thread(runnable, "jvm-mcp-bridge-jmx-" + name + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        // Cached in shape — idle threads go away after a minute, and a handover queue means a
        // new call never waits behind a stuck one — but with a ceiling. Without one, a target
        // that accepts connections and never answers costs a thread per tool call, permanently,
        // because a cancelled RMI read does not actually stop. At the ceiling further calls are
        // refused with an answer the model can act on, which is the honest report of a target
        // that has stopped responding.
        this.calls = new ThreadPoolExecutor(
                0, MAX_CONCURRENT_CALLS, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threads);
    }

    /** Describes this JVM rather than another one. */
    public static JvmTargetHandle embedded(String name) {
        return new JvmTargetHandle(name, null, Duration.ofSeconds(5), null, null);
    }

    public String name() {
        return name;
    }

    /** True when the target is this process, which is worth telling the model. */
    public boolean isEmbedded() {
        return jmxUrl == null;
    }

    /**
     * The live connection, opening or reopening it if needed.
     *
     * <p>Reconnects on a broken connection rather than failing for the life of the process: the
     * target being restarted is the single most likely thing to have happened between two tool
     * calls, and a bridge that needed restarting whenever the application it watches did would
     * be useless in exactly the situation it exists for.
     */
    MBeanServerConnection connection(Duration bound) throws IOException, InterruptedException {
        if (jmxUrl == null) {
            // The platform server is a field access; there is nothing to guard and nothing that
            // can hang, so the lock is not taken at all.
            return ManagementFactory.getPlatformMBeanServer();
        }
        if (!connectionLock.tryLock(Math.max(1, bound.toMillis()), TimeUnit.MILLISECONDS)) {
            throw new IOException("JVM target '" + name + "' is busy: an earlier call to it has not"
                    + " returned and still holds the connection. The target is not answering;"
                    + " that is itself the answer. Retry once it recovers.");
        }
        try {
            return connectLocked();
        } finally {
            connectionLock.unlock();
        }
    }

    private MBeanServerConnection connectLocked() throws IOException {
        if (connection != null) {
            try {
                // Cheap liveness check: a stale RMI stub answers this, a dead one throws, and
                // finding out here is better than surfacing a broken pipe as a tool failure.
                connection.getMBeanCount();
                return connection;
            } catch (IOException e) {
                log.info("JMX connection to '{}' is stale, reconnecting: {}", name, e.toString());
                closeQuietly();
            }
        }
        Map<String, Object> environment = new HashMap<>();
        if (username != null && !username.isBlank()) {
            environment.put(JMXConnector.CREDENTIALS, new String[] {username, password == null ? "" : password});
        }
        connector = JMXConnectorFactory.connect(new JMXServiceURL(jmxUrl), environment);
        connection = connector.getMBeanServerConnection();
        return connection;
    }

    /**
     * Runs a JMX interaction under a hard time bound.
     *
     * <p>The body receives the connection rather than fetching it, so connecting is inside the
     * bound too — a target whose host silently drops packets hangs at connect, not at the first
     * attribute read.
     *
     * @param timeout the caller's {@code EffectiveLimits.timeout()}, so a narrowed profile gets
     *     the shorter wait it asked for
     */
    <T> T call(Duration timeout, JmxCall<T> body) throws Exception {
        Duration bound = timeout == null || timeout.isZero() || timeout.isNegative() ? connectTimeout : timeout;
        Future<T> future;
        try {
            future = calls.submit((Callable<T>) () -> body.run(connection(bound)));
        } catch (RejectedExecutionException e) {
            throw new TimeoutException("JVM target '" + name + "' has " + MAX_CONCURRENT_CALLS
                    + " calls outstanding that have not returned, so this one was not started."
                    + " The target has stopped answering rather than answering slowly; nothing"
                    + " asked of it now will come back. Report that, and try again later.");
        }
        try {
            return future.get(bound.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new TimeoutException("JVM target '" + name + "' did not answer within " + bound
                    + ". If it is under load or in a long GC pause, that is itself the answer;"
                    + " a shorter query (one MBean, named attributes) may still get through.");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception checked) {
                throw checked;
            }
            throw e;
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /** The body of a bounded JMX interaction. */
    @FunctionalInterface
    interface JmxCall<T> {
        T run(MBeanServerConnection connection) throws Exception;
    }

    Duration connectTimeout() {
        return connectTimeout;
    }

    Duration jfrMaxDuration() {
        return jfrMaxDuration;
    }

    private void closeQuietly() {
        if (connector != null) {
            try {
                connector.close();
            } catch (IOException e) {
                log.debug("closing stale JMX connector for '{}' failed", name, e);
            }
        }
        connector = null;
        connection = null;
    }

    @Override
    public void close() {
        // Timed, and closing anyway if it expires: shutting down around a call parked in an RMI
        // read is the case this exists for, and blocking on its lock would hang the shutdown
        // that is meant to escape it.
        boolean held = false;
        try {
            held = connectionLock.tryLock(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            closeQuietly();
        } finally {
            if (held) {
                connectionLock.unlock();
            }
        }
        // Not awaitTermination: a call still parked in an RMI socket read is exactly what this
        // is shutting down around, and waiting for it would reintroduce the hang at exit.
        calls.shutdownNow();
    }
}
