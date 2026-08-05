package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.BridgeServerFactory;
import io.github.thompgt.jvmmcp.core.ToolRegistry;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.DefaultServerTransportSecurityValidator;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.server.transport.ServerTransportSecurityValidator;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Serves MCP over Streamable HTTP.
 *
 * <p>Only active for {@code bridge.transport=http}; under stdio no servlet container exists and
 * none of this is created. The SDK's transport provider <em>is</em> an {@code HttpServlet}, so
 * Boot needs nothing more than a {@link ServletRegistrationBean} — which is the whole reason
 * this repository does not depend on Spring AI's MCP starter (see ADR 001).
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "bridge.transport", havingValue = "http")
public class McpHttpConfiguration {

    private static final Logger log = LoggerFactory.getLogger(McpHttpConfiguration.class);

    @Bean
    public HttpServletStreamableServerTransportProvider mcpTransportProvider(BridgeProperties properties) {
        BridgeProperties.Http http = properties.getHttp();

        return HttpServletStreamableServerTransportProvider.builder()
                .jsonMapper(McpJsonDefaults.getMapper())
                .mcpEndpoint(http.getEndpoint())
                .keepAliveInterval(http.getKeepAliveInterval())
                .securityValidator(securityValidator(http))
                .build();
    }

    /**
     * Origin and Host validation, on by default.
     *
     * <p>A local MCP server that answers any Origin is reachable from any page the user has
     * open: the browser sends the request, the server is on localhost, and DNS rebinding does
     * the rest. The SDK ships this check; leaving it unconfigured would turn it off.
     */
    private static ServerTransportSecurityValidator securityValidator(BridgeProperties.Http http) {
        if (http.getAllowedOrigins().isEmpty() && http.getAllowedHosts().isEmpty()) {
            return DefaultServerTransportSecurityValidator.builder().build();
        }
        return DefaultServerTransportSecurityValidator.builder()
                .allowedOrigins(http.getAllowedOrigins())
                .allowedHosts(http.getAllowedHosts())
                .build();
    }

    /**
     * The MCP server itself. {@code destroyMethod} drains open sessions on shutdown instead of
     * dropping them, so a client sees a clean close rather than a broken stream.
     */
    @Bean(destroyMethod = "closeGracefully")
    public McpSyncServer mcpServer(
            HttpServletStreamableServerTransportProvider transport, ToolRegistry registry) {
        McpSyncServer server = BridgeServerFactory.streamableHttp(
                transport, registry, BridgeApplication.version(), Duration.ofSeconds(30));
        log.info(
                "jvm-mcp-bridge {} serving {} tool(s) over streamable HTTP",
                BridgeApplication.version(),
                registry.toolCount());
        return server;
    }

    /**
     * Mapped on the exact endpoint rather than a prefix. A prefix mapping would also answer
     * {@code /mcp/anything}, which is a second, unaudited way in.
     */
    @Bean
    public ServletRegistrationBean<HttpServletStreamableServerTransportProvider> mcpServletRegistration(
            HttpServletStreamableServerTransportProvider transport, BridgeProperties properties) {
        ServletRegistrationBean<HttpServletStreamableServerTransportProvider> registration =
                new ServletRegistrationBean<>(transport, properties.getHttp().getEndpoint());
        registration.setName("mcp");
        // Streaming responses: the servlet writes SSE events as they happen.
        registration.setAsyncSupported(true);
        registration.setLoadOnStartup(1);
        return registration;
    }
}
