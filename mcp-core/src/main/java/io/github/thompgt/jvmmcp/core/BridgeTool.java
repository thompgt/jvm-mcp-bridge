package io.github.thompgt.jvmmcp.core;

import io.modelcontextprotocol.spec.McpSchema;
import java.util.Map;

/**
 * One tool exposed to MCP clients.
 *
 * <p>Implementations describe themselves and execute; they do not decide whether they are
 * allowed to run. That decision belongs to the policy engine, which wraps every tool before
 * it reaches the registry. See {@code docs/adr/002}.
 *
 * <p>The {@link #descriptor()} of a tool is prompt surface, not documentation: it is the
 * only thing the model reads before choosing to call it. A description should state what
 * the tool refuses and how its result is bounded, because a model that knows the limits
 * up front does not waste turns discovering them.
 */
public interface BridgeTool {

    /**
     * The MCP-visible declaration: name, description, input schema, and — where the tool
     * returns structured data — an output schema so clients get typed results rather than
     * a wall of text.
     */
    McpSchema.Tool descriptor();

    /**
     * Executes the tool.
     *
     * @param arguments raw arguments from the client, already validated against the input
     *     schema by the SDK when {@code validateToolInputs} is on
     * @return the outcome; implementations return {@link ToolOutcome#failure} for expected
     *     problems rather than throwing, so the model gets an actionable message instead of
     *     a transport-level error
     */
    ToolOutcome call(Map<String, Object> arguments);

    /** Short backend identifier used in audit records, e.g. {@code jdbc} or {@code kafka}. */
    String backend();
}
