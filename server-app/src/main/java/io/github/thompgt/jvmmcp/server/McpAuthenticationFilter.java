package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.Principal;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Refuses unauthenticated requests before they reach the MCP endpoint.
 *
 * <p>A filter rather than a check inside the transport's context extractor, because an
 * unauthenticated caller must never get as far as an MCP session. Rejecting later would mean
 * the server had already allocated session state for someone it does not know, and the failure
 * would surface to the client as a protocol error rather than a 401 it can act on.
 *
 * <p>The principal is left on the request for {@code McpHttpConfiguration}'s context extractor
 * to pick up; that is the only handoff, and it happens after authentication has succeeded.
 */
public final class McpAuthenticationFilter extends OncePerRequestFilter {

    /** Request attribute holding the authenticated principal. */
    public static final String PRINCIPAL_ATTRIBUTE = "jvm-mcp-bridge.principal";

    private static final Logger log = LoggerFactory.getLogger(McpAuthenticationFilter.class);

    private final BridgeAuthenticator authenticator;

    public McpAuthenticationFilter(BridgeAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {

        Optional<Principal> principal = authenticator.authenticate(request);
        if (principal.isEmpty()) {
            // Logged without the presented credential: a mistyped key is often a real key for
            // something else, and the audit log is read by more people than the secret store.
            log.warn(
                    "refused an unauthenticated request to {} from {}",
                    request.getRequestURI(),
                    request.getRemoteAddr());
            unauthorized(response);
            return;
        }

        request.setAttribute(PRINCIPAL_ATTRIBUTE, principal.get());
        chain.doFilter(request, response);
    }

    /**
     * A JSON-RPC-shaped error body, because the caller is an MCP client and will try to parse
     * the response before it looks at the status code.
     */
    private void unauthorized(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setHeader("WWW-Authenticate", authenticator.challenge());
        response.setContentType("application/json");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.getWriter()
                .write(
                        "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32001,"
                                + "\"message\":\"unauthenticated: present a valid credential in the"
                                + " configured header\"},\"id\":null}");
    }
}
