package io.github.thompgt.jvmmcp.core;

import io.modelcontextprotocol.spec.McpSchema;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The result of running a tool, before it is turned into an MCP {@code CallToolResult}.
 *
 * <p>Two shapes, and the distinction matters to the model:
 *
 * <ul>
 *   <li>{@link #success} carries structured data plus a human-readable summary. The
 *       structured payload is what a client can parse; the text is what a model reads when
 *       the client does not use structured content.
 *   <li>{@link #failure} carries a message written <em>for the model</em>. It is surfaced
 *       with {@code isError=true}, which the SDK passes to the client rather than raising a
 *       protocol error — so the model can read the reason and adjust, instead of retrying
 *       the same call blindly.
 * </ul>
 *
 * @param structured structured payload, or {@code null} for a failure
 * @param summary text rendering shown to the model
 * @param error {@code true} if this represents a refused or failed call
 * @param rowCount number of records the payload represents; {@code -1} when not applicable.
 *     Recorded in the audit log so an operator can see how much data left the process.
 */
public record ToolOutcome(Object structured, String summary, boolean error, int rowCount) {

    public ToolOutcome {
        Objects.requireNonNull(summary, "summary");
    }

    public static ToolOutcome success(Object structured, String summary, int rowCount) {
        return new ToolOutcome(structured, summary, false, rowCount);
    }

    public static ToolOutcome success(Object structured, String summary) {
        return new ToolOutcome(structured, summary, false, -1);
    }

    /**
     * A refusal or an expected failure.
     *
     * <p>The message is read by the model. It should name what was refused and what would
     * work instead — {@code table "orders" is not in the allowlist; visible tables are:
     * customers, order_items} lets the model recover, where a stack trace does not.
     */
    public static ToolOutcome failure(String modelActionableMessage) {
        return new ToolOutcome(null, modelActionableMessage, true, -1);
    }

    /** Converts to the SDK's wire type. */
    public McpSchema.CallToolResult toCallToolResult() {
        McpSchema.CallToolResult.Builder builder = McpSchema.CallToolResult.builder()
                .content(List.<McpSchema.Content>of(McpSchema.TextContent.builder(summary).build()))
                .isError(error);
        if (structured != null) {
            builder.structuredContent(structured);
        }
        return builder.build();
    }

    /**
     * A failure result carrying the machine-readable denial detail alongside the message, so
     * a client that wants to render or log the refusal does not have to parse prose.
     */
    public static ToolOutcome denied(String message, Map<String, Object> detail) {
        return new ToolOutcome(detail, message, true, -1);
    }
}
