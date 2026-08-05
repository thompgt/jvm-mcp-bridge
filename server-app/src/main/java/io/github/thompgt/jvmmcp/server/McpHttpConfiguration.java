package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.BridgeServerFactory;
import io.github.thompgt.jvmmcp.core.CallContext;
import io.github.thompgt.jvmmcp.core.ToolRegistry;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.DefaultServerTransportSecurityValidator;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.server.transport.ServerTransportSecurityValidator;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

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

    /**
     * Chooses how callers identify themselves.
     *
     * <p>{@code none} is available but has to be asked for twice, because an MCP endpoint with
     * no authentication is a database connection listening on a port. The second flag exists so
     * that turning it on is a deliberate sentence in a config file rather than a default nobody
     * revisited.
     */
    @Bean
    public BridgeAuthenticator bridgeAuthenticator(BridgeProperties properties) {
        BridgeProperties.Auth auth = properties.getHttp().getAuth();
        return switch (auth.getMode()) {
            case API_KEY -> new ApiKeyAuthenticator(auth);
            case OAUTH2 -> new JwtAuthenticator(auth);
            case NONE -> {
                if (!auth.isIUnderstandThisIsUnauthenticated()) {
                    throw new IllegalStateException(
                            "bridge.http.auth.mode=none serves every caller with no credential at"
                                + " all. If that is genuinely what you want — the bridge is behind"
                                + " a gateway that already authenticates — also set"
                                + " bridge.http.auth.i-understand-this-is-unauthenticated=true.");
                }
                log.warn(
                        "AUTHENTICATION IS DISABLED: every request to {} is served as 'anonymous'",
                        properties.getHttp().getEndpoint());
                yield BridgeAuthenticator.anonymous();
            }
        };
    }

    @Bean
    public HttpServletStreamableServerTransportProvider mcpTransportProvider(BridgeProperties properties) {
        BridgeProperties.Http http = properties.getHttp();

        return HttpServletStreamableServerTransportProvider.builder()
                .jsonMapper(McpJsonDefaults.getMapper())
                .mcpEndpoint(http.getEndpoint())
                .keepAliveInterval(http.getKeepAliveInterval())
                .securityValidator(securityValidator(http))
                // Copies the principal the filter already authenticated into the transport
                // context, which is where ToolRegistry reads it from when it binds CallContext.
                // This does not authenticate anything — by the time it runs, the filter has.
                .contextExtractor((HttpServletRequest request) -> McpTransportContext.create(
                        principalContext(request)))
                .build();
    }

    private static Map<String, Object> principalContext(HttpServletRequest request) {
        Object principal = request.getAttribute(McpAuthenticationFilter.PRINCIPAL_ATTRIBUTE);
        return principal == null ? Map.of() : Map.of(CallContext.TRANSPORT_KEY, principal);
    }

    /**
     * Ordered ahead of everything else: no other filter should see a request from a caller the
     * bridge has not identified.
     */
    @Bean
    public FilterRegistrationBean<McpAuthenticationFilter> mcpAuthenticationFilter(
            BridgeAuthenticator authenticator, BridgeProperties properties) {
        FilterRegistrationBean<McpAuthenticationFilter> registration =
                new FilterRegistrationBean<>(new McpAuthenticationFilter(authenticator));
        registration.addUrlPatterns(properties.getHttp().getEndpoint());
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        registration.setName("mcpAuthentication");
        return registration;
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
