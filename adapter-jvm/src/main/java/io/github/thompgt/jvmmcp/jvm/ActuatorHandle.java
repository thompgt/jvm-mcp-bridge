package io.github.thompgt.jvmmcp.jvm;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Locale;

/**
 * The HTTP side of a JVM target: its Spring Boot Actuator.
 *
 * <p>Actuator answers questions JMX cannot. {@code /health} knows whether the datasource and the
 * broker this application depends on are up, which no MXBean does; {@code /env} shows the resolved
 * configuration including which property source won, which is the answer to half of all "why is it
 * behaving like that in production" questions; {@code /metrics} carries the application's own
 * Micrometer meters — request rates, pool utilisation, cache hits — that nobody exposes over JMX.
 *
 * <p>It is also, from this bridge's point of view, an HTTP client pointed at an internal address
 * with a path a language model chooses, which is the shape of a server-side request forgery. Three
 * things keep it from being one:
 *
 * <ul>
 *   <li><b>The host is never the model's.</b> Only the path is taken from the call, and only after
 *       {@link ActuatorTools} has validated each segment against a strict character set — so there
 *       is no input that turns this into a request to another host, no {@code ..} traversal above
 *       the configured base, and no way to smuggle a second URL in.
 *   <li><b>Redirects are never followed.</b> A 302 from the target would otherwise be an
 *       instruction from the network to go somewhere this configuration never named.
 *   <li><b>The body is read under a cap.</b> {@code /env} on a large application is megabytes, and
 *       the process must not be made to hold an unbounded response because something asked for one.
 * </ul>
 */
public final class ActuatorHandle {

    private final URI baseUri;
    private final String authorization;
    private final HttpClient client;

    /**
     * @param baseUrl the Actuator root, e.g. {@code http://localhost:8080/actuator}
     * @param username optional HTTP basic credentials
     * @param token optional bearer token, used when no username is given
     */
    public ActuatorHandle(String baseUrl, String username, String password, String token, Duration connectTimeout) {
        String normalised = baseUrl.trim();
        if (!normalised.toLowerCase(Locale.ROOT).startsWith("http://")
                && !normalised.toLowerCase(Locale.ROOT).startsWith("https://")) {
            throw new IllegalArgumentException(
                    "actuator-base-url must start with http:// or https://, got '" + baseUrl + "'");
        }
        // A trailing slash makes resolve() behave: without it, resolving "health" against
        // ".../actuator" replaces the last segment and requests ".../health".
        this.baseUri = URI.create(normalised.endsWith("/") ? normalised : normalised + "/");

        if (username != null && !username.isBlank()) {
            String credentials = username + ":" + (password == null ? "" : password);
            this.authorization = "Basic "
                    + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        } else if (token != null && !token.isBlank()) {
            this.authorization = "Bearer " + token.trim();
        } else {
            this.authorization = null;
        }

        this.client = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                // See the class comment: a redirect is the network telling this process to go
                // somewhere the operator never configured.
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    public String baseUrl() {
        return baseUri.toString();
    }

    /**
     * Issues one GET and reads at most {@code maxBytes} of the response.
     *
     * @param relativePath already validated by the caller; this method does not sanitise it
     */
    Response get(String relativePath, Duration timeout, long maxBytes) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(baseUri.resolve(relativePath))
                .timeout(timeout)
                .header("Accept", "application/vnd.spring-boot.actuator.v3+json, application/json")
                .GET();
        if (authorization != null) {
            request.header("Authorization", authorization);
        }

        HttpResponse<InputStream> response =
                client.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream body = response.body()) {
            // One byte past the cap, so "there was more" is detectable rather than inferred from
            // a body that happens to be exactly the cap long.
            byte[] bytes = body.readNBytes((int) Math.min(maxBytes + 1, Integer.MAX_VALUE));
            boolean truncated = bytes.length > maxBytes;
            String text = new String(
                    truncated ? java.util.Arrays.copyOf(bytes, (int) maxBytes) : bytes, StandardCharsets.UTF_8);
            return new Response(
                    response.statusCode(),
                    response.headers().firstValue("content-type").orElse(""),
                    text,
                    truncated);
        }
    }

    /** One Actuator response, already bounded. */
    record Response(int status, String contentType, String body, boolean truncated) {

        boolean isJson() {
            String type = contentType.toLowerCase(Locale.ROOT);
            return type.contains("json");
        }
    }
}
