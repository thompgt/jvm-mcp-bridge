package io.github.thompgt.jvmmcp.server;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.json.McpJsonDefaults;
import java.net.http.HttpRequest;
import java.time.Duration;

/** Connects an MCP client to the locally running HTTP transport with a bearer credential. */
final class McpTestClients {

    private McpTestClients() {}

    static McpSyncClient connect(int port, String apiKey) {
        HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport.builder(
                        "http://localhost:" + port)
                .endpoint("/mcp")
                .jsonMapper(McpJsonDefaults.getMapper())
                .requestBuilder(HttpRequest.newBuilder().header("Authorization", "Bearer " + apiKey))
                .build();
        McpSyncClient client = McpClient.sync(transport)
                .requestTimeout(Duration.ofSeconds(30))
                .initializationTimeout(Duration.ofSeconds(30))
                .build();
        client.initialize();
        return client;
    }
}
