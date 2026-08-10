package io.github.thompgt.jvmmcp.jvm;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What a target that accepts connections and then says nothing costs this process.
 *
 * <p>The dangerous JMX target is not the one that is down — that fails fast — it is the one whose
 * socket is open and whose answers never arrive. A call to it cannot be cancelled: the RMI read
 * stays parked until the TCP stack gives up. Everything here is about making sure the *rest* of
 * the process survives that, because "one target stopped answering" must not become "the bridge
 * stopped answering".
 */
class JvmTargetHandleBoundsTest {

    private ServerSocket blackHole;
    private Thread accepter;
    private final List<Socket> accepted = new ArrayList<>();
    private JvmTargetHandle handle;

    /** A listener that completes the TCP handshake and then never writes a byte. */
    private String startBlackHole() throws IOException {
        blackHole = new ServerSocket(0);
        accepter = new Thread(
                () -> {
                    while (!blackHole.isClosed()) {
                        try {
                            accepted.add(blackHole.accept());
                        } catch (IOException e) {
                            return;
                        }
                    }
                },
                "black-hole-accepter");
        accepter.setDaemon(true);
        accepter.start();
        return "service:jmx:rmi:///jndi/rmi://127.0.0.1:" + blackHole.getLocalPort() + "/jmxrmi";
    }

    @AfterEach
    void stop() throws IOException {
        if (handle != null) {
            handle.close();
        }
        if (blackHole != null) {
            blackHole.close();
        }
        for (Socket socket : accepted) {
            socket.close();
        }
    }

    @Test
    @DisplayName("a hung target does not hold up the calls behind it")
    void aCallThatNeverReturnsDoesNotBlockTheNextOne() throws Exception {
        handle = new JvmTargetHandle("stuck", startBlackHole(), Duration.ofMillis(300), null, null);

        // First call: parks inside the connect, times out, and is cancelled — but the thread it
        // left behind is still in there holding whatever it holds.
        long firstStart = System.nanoTime();
        assertThat(catchFailure(() -> handle.call(Duration.ofMillis(300), c -> c.getMBeanCount())))
                .isNotNull();
        assertThat(Duration.ofNanos(System.nanoTime() - firstStart)).isLessThan(Duration.ofSeconds(10));

        // Second call: with `synchronized` on the connection this parked on a monitor, which
        // cancel(true) cannot interrupt, so it never returned at all. It must come back inside
        // its own bound and say something useful.
        long secondStart = System.nanoTime();
        Exception failure = catchFailure(() -> handle.call(Duration.ofMillis(300), c -> c.getMBeanCount()));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - secondStart);

        assertThat(failure).isNotNull();
        assertThat(elapsed).isLessThan(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("an unreachable target cannot cost a thread per call for ever")
    void outstandingCallsAreCappedRatherThanUnbounded() throws Exception {
        handle = new JvmTargetHandle("stuck", startBlackHole(), Duration.ofMillis(200), null, null);

        // Far more calls than the ceiling. Under an unbounded pool each one left a thread parked
        // for the life of the process; the ceiling turns that into a refusal instead.
        for (int i = 0; i < 40; i++) {
            catchFailure(() -> handle.call(Duration.ofMillis(200), c -> c.getMBeanCount()));
        }

        long parked = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("jvm-mcp-bridge-jmx-stuck-"))
                .count();

        assertThat(parked).isLessThanOrEqualTo(8);
    }

    @Test
    void theEmbeddedTargetTakesNoLockAndStillAnswers() throws Exception {
        handle = JvmTargetHandle.embedded("self");
        CountDownLatch done = new CountDownLatch(1);

        Integer count = handle.call(Duration.ofSeconds(5), connection -> {
            done.countDown();
            return connection.getMBeanCount();
        });

        assertThat(done.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(count).isPositive();
    }

    private interface Body {
        void run() throws Exception;
    }

    private static Exception catchFailure(Body body) {
        try {
            body.run();
            return null;
        } catch (Exception e) {
            return e;
        }
    }
}
