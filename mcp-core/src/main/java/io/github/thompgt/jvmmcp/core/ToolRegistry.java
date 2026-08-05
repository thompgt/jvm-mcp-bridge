package io.github.thompgt.jvmmcp.core;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects the tools every configured adapter contributes and converts them into the SDK's
 * tool specifications.
 *
 * <p>Tools are <em>registered</em> here rather than discovered by annotation scanning, and
 * that is deliberate: everything in this registry has already been wrapped by the policy
 * engine on the way in. A scanned annotation could register a tool that never passes through
 * it, which is exactly the failure {@code docs/adr/002} exists to prevent.
 */
public final class ToolRegistry {

    private static final Logger log = LoggerFactory.getLogger(ToolRegistry.class);

    private final Map<String, BridgeTool> tools = new LinkedHashMap<>();
    private final List<McpServerFeatures.SyncResourceSpecification> resources = new ArrayList<>();
    private final List<McpServerFeatures.SyncResourceTemplateSpecification> resourceTemplates =
            new ArrayList<>();

    /**
     * @throws IllegalStateException if two adapters claim the same tool name. Silently
     *     letting one win would mean the model calls a tool bound to a backend nobody
     *     intended, so this fails at startup instead.
     */
    public ToolRegistry register(BridgeTool tool) {
        String name = tool.descriptor().name();
        BridgeTool existing = tools.putIfAbsent(name, tool);
        if (existing != null) {
            throw new IllegalStateException(
                    "duplicate tool name '"
                            + name
                            + "' registered by backends '"
                            + existing.backend()
                            + "' and '"
                            + tool.backend()
                            + "'");
        }
        log.debug("registered tool {} from backend {}", name, tool.backend());
        return this;
    }

    public ToolRegistry registerAll(List<? extends BridgeTool> batch) {
        batch.forEach(this::register);
        return this;
    }

    public ToolRegistry registerResource(McpServerFeatures.SyncResourceSpecification resource) {
        resources.add(resource);
        return this;
    }

    public ToolRegistry registerResourceTemplate(
            McpServerFeatures.SyncResourceTemplateSpecification template) {
        resourceTemplates.add(template);
        return this;
    }

    /**
     * Adapts each registered tool to the SDK's call handler.
     *
     * <p>The handler catches {@link Arguments.BadArgumentException} and any unexpected
     * throwable and converts them into error <em>results</em> rather than letting them
     * escape as protocol errors. An MCP error terminates the call with no usable detail; an
     * error result carries a message the model reads and can correct against.
     */
    public List<McpServerFeatures.SyncToolSpecification> toSyncToolSpecifications() {
        List<McpServerFeatures.SyncToolSpecification> specs = new ArrayList<>(tools.size());
        for (BridgeTool tool : tools.values()) {
            specs.add(
                    McpServerFeatures.SyncToolSpecification.builder()
                            .tool(tool.descriptor())
                            .callHandler((exchange, request) -> invoke(tool, request))
                            .build());
        }
        return List.copyOf(specs);
    }

    private McpSchema.CallToolResult invoke(BridgeTool tool, McpSchema.CallToolRequest request) {
        try {
            return tool.call(request.arguments()).toCallToolResult();
        } catch (Arguments.BadArgumentException e) {
            return ToolOutcome.failure(e.getMessage()).toCallToolResult();
        } catch (RuntimeException e) {
            // The exception text may contain connection strings or fragments of internal
            // state, so it is logged here and deliberately not echoed to the client.
            log.error("tool {} failed unexpectedly", tool.descriptor().name(), e);
            return ToolOutcome.failure(
                            "tool '"
                                    + tool.descriptor().name()
                                    + "' failed unexpectedly; the server log has the detail."
                                    + " This is a server-side fault, not a problem with the"
                                    + " arguments — retrying the same call will not help.")
                    .toCallToolResult();
        }
    }

    public List<McpServerFeatures.SyncResourceSpecification> resources() {
        return List.copyOf(resources);
    }

    public List<McpServerFeatures.SyncResourceTemplateSpecification> resourceTemplates() {
        return List.copyOf(resourceTemplates);
    }

    public int toolCount() {
        return tools.size();
    }

    public List<String> toolNames() {
        return List.copyOf(tools.keySet());
    }
}
