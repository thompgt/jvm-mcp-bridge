package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.BridgeServerFactory;
import io.github.thompgt.jvmmcp.core.ToolRegistry;
import io.modelcontextprotocol.server.McpSyncServer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point.
 *
 * <p>Under the stdio transport this runs as a plain process with no web server: stdin and
 * stdout <em>are</em> the protocol channel. That constraint drives the logging configuration —
 * anything written to stdout that is not a JSON-RPC message corrupts the stream and the client
 * disconnects, so logs go to stderr. It is the single most common way a stdio MCP server
 * appears broken.
 */
@SpringBootApplication
@EnableConfigurationProperties(BridgeProperties.class)
public class BridgeApplication {

    private static final Logger log = LoggerFactory.getLogger(BridgeApplication.class);

    /** Read from the jar manifest at runtime; falls back for an exploded run. */
    static String version() {
        String implementation = BridgeApplication.class.getPackage().getImplementationVersion();
        return implementation == null ? "0.1.0-SNAPSHOT" : implementation;
    }

    public static void main(String[] args) {
        BridgeProperties.Transport transport = transportFrom(args);

        SpringApplication application = new SpringApplication(BridgeApplication.class);
        if (transport == BridgeProperties.Transport.STDIO) {
            // No servlet container, and no banner: both would write to stdout.
            application.setWebApplicationType(org.springframework.boot.WebApplicationType.NONE);
            application.setBannerMode(org.springframework.boot.Banner.Mode.OFF);
        }

        var context = application.run(args);
        requireTransportAgreement(transport, context.getBean(BridgeProperties.class).getTransport());

        if (transport == BridgeProperties.Transport.HTTP) {
            // The servlet container is the thing keeping the JVM alive, and McpHttpConfiguration
            // has already built and registered the server. Nothing left for main to do.
            return;
        }

        // stdio: the transport reads on its own threads and there is no container, so main
        // has to block or the process would exit during the client's initialise.
        ToolRegistry registry = context.getBean(ToolRegistry.class);
        McpSyncServer server = BridgeServerFactory.stdio(registry, version(), Duration.ofSeconds(30));
        log.info("jvm-mcp-bridge {} serving {} tool(s) over stdio", version(), registry.toolCount());
        awaitShutdown(server);
    }

    /**
     * Reads the transport before the context starts, because the choice decides whether a web
     * server is created at all — by the time properties are bound it is too late.
     */
    private static BridgeProperties.Transport transportFrom(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--bridge.transport=")) {
                String value = arg.substring("--bridge.transport=".length());
                return BridgeProperties.Transport.valueOf(value.trim().toUpperCase(java.util.Locale.ROOT));
            }
        }
        return BridgeProperties.Transport.STDIO;
    }

    /**
     * Guards the one gap in reading the transport early: {@link #transportFrom} sees only the
     * command line, so {@code transport: http} set in a config file would leave the process
     * with no servlet container while the HTTP beans were still created. Rather than start a
     * server that answers nothing, say exactly what to do about it.
     */
    private static void requireTransportAgreement(
            BridgeProperties.Transport commandLine, BridgeProperties.Transport bound) {
        if (commandLine != bound) {
            throw new IllegalStateException(
                    "transport is '"
                            + bound.name().toLowerCase(java.util.Locale.ROOT)
                            + "' in configuration but the process started for '"
                            + commandLine.name().toLowerCase(java.util.Locale.ROOT)
                            + "'. The transport decides whether a web server is created at all,"
                            + " which is settled before configuration is read — pass it on the"
                            + " command line instead: --bridge.transport="
                            + bound.name().toLowerCase(java.util.Locale.ROOT));
        }
    }

    /**
     * Blocks until the client closes the transport or the JVM is asked to stop.
     *
     * <p>The stdio transport reads on its own threads, so main has nothing left to do; without
     * this the process would exit immediately and the client would see the server die during
     * initialise.
     */
    private static void awaitShutdown(McpSyncServer server) {
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            server.closeGracefully();
                            latch.countDown();
                        },
                        "bridge-shutdown"));
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
