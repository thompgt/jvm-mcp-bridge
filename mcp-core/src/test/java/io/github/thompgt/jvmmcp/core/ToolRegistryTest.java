package io.github.thompgt.jvmmcp.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.modelcontextprotocol.spec.McpSchema;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class ToolRegistryTest {

    private static BridgeTool tool(String name, String backend, Function<Map<String, Object>, ToolOutcome> body) {
        return new BridgeTool() {
            @Override
            public McpSchema.Tool descriptor() {
                return McpSchema.Tool.builder(name, Schemas.object().build())
                        .description("test tool")
                        .build();
            }

            @Override
            public ToolOutcome call(Map<String, Object> arguments) {
                return body.apply(arguments);
            }

            @Override
            public String backend() {
                return backend;
            }
        };
    }

    @Test
    void registersToolsAndExposesThemAsSpecifications() {
        ToolRegistry registry = new ToolRegistry()
                .register(tool("sql.query", "jdbc", a -> ToolOutcome.success(Map.of(), "ok")))
                .register(tool("kafka.peek", "kafka", a -> ToolOutcome.success(Map.of(), "ok")));

        assertThat(registry.toolCount()).isEqualTo(2);
        assertThat(registry.toolNames()).containsExactly("sql.query", "kafka.peek");
        assertThat(registry.toSyncToolSpecifications()).hasSize(2);
    }

    @Test
    void rejectsDuplicateToolNamesNamingBothBackends() {
        ToolRegistry registry =
                new ToolRegistry().register(tool("sql.query", "jdbc", a -> ToolOutcome.success(Map.of(), "ok")));

        // Letting the second registration win silently would bind the model's calls to a
        // backend nobody configured them against, so this has to fail at startup.
        assertThatThrownBy(() -> registry.register(tool("sql.query", "other-db", a -> null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sql.query")
                .hasMessageContaining("jdbc")
                .hasMessageContaining("other-db");
    }

    @Test
    void badArgumentsBecomeAnErrorResultTheModelCanRead() {
        ToolRegistry registry = new ToolRegistry()
                .register(tool("sql.query", "jdbc", a -> {
                    throw new Arguments.BadArgumentException("missing required argument: sql");
                }));

        McpSchema.CallToolResult result = invokeFirst(registry);

        assertThat(result.isError()).isTrue();
        assertThat(textOf(result)).isEqualTo("missing required argument: sql");
    }

    @Test
    void unexpectedFailuresDoNotLeakInternalDetailToTheClient() {
        ToolRegistry registry = new ToolRegistry()
                .register(tool("sql.query", "jdbc", a -> {
                    throw new IllegalStateException("jdbc:postgresql://secret-host/db password=hunter2");
                }));

        McpSchema.CallToolResult result = invokeFirst(registry);

        assertThat(result.isError()).isTrue();
        // The connection string and credential must stay in the server log.
        assertThat(textOf(result)).doesNotContain("secret-host").doesNotContain("hunter2");
        // But the model still needs to know retrying is pointless.
        assertThat(textOf(result)).contains("retrying the same call will not help");
    }

    private static McpSchema.CallToolResult invokeFirst(ToolRegistry registry) {
        var spec = registry.toSyncToolSpecifications().get(0);
        return spec.callHandler().apply(null, McpSchema.CallToolRequest.builder("sql.query").build());
    }

    private static String textOf(McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().get(0)).text();
    }
}
