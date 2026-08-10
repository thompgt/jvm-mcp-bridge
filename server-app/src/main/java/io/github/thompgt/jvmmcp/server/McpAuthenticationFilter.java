package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.core.Principal;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
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
 *
 * <p>It also guards Actuator, which shares the port. {@code health} is published with
 * {@code show-details: always} and names every backend, its product and its version — a map of
 * the estate, free to anyone who can reach the port. The exception is the orchestrator probes:
 * a kubelet has no credential to present, and those endpoints answer with a status word and
 * nothing else.
 */
public final class McpAuthenticationFilter extends OncePerRequestFilter {

    /** Request attribute holding the authenticated principal. */
    public static final String PRINCIPAL_ATTRIBUTE = "jvm-mcp-bridge.principal";

    private static final Logger log = LoggerFactory.getLogger(McpAuthenticationFilter.class);

    private final BridgeAuthenticator authenticator;
    private final Set<String> openPaths;

    /**
     * @param openPaths request paths served without a credential. Only for endpoints that carry
     *     no information — the liveness and readiness probes, which return a status word.
     */
    public McpAuthenticationFilter(BridgeAuthenticator authenticator, Set<String> openPaths) {
        this.authenticator = authenticator;
        this.openPaths = Set.copyOf(openPaths);
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {

        if (openPaths.contains(pathWithinApplication(request))) {
            chain.doFilter(request, response);
            return;
        }

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

    /** The path the servlet container matched on, without the context path. */
    private static String pathWithinApplication(HttpServletRequest request) {
        String path = request.getRequestURI();
        String context = request.getContextPath();
        if (context != null && !context.isEmpty() && path.startsWith(context)) {
            path = path.substring(context.length());
        }
        // Trailing slash only: `/actuator/health/liveness/` is the same endpoint.
        return path.length() > 1 && path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
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
