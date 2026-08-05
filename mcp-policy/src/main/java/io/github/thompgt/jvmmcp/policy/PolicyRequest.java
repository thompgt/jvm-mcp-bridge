package io.github.thompgt.jvmmcp.policy;

import java.util.List;
import java.util.Objects;

/**
 * What an adapter asks the engine to decide on.
 *
 * <p>The {@code resources} are the crux. They must be the names the adapter <em>resolved</em>
 * — for SQL, the tables parsed out of the statement's AST — never a name the client supplied
 * or a substring of the raw request. Deciding against unresolved input is how a table
 * allowlist gets bypassed through a join, a view or a comment.
 *
 * @param tool tool name as called, for the audit record
 * @param resources resolved resource names this call will touch
 * @param write true if the call intends to modify the backend
 * @param requestedRows client's row preference, or 0 for none
 */
public record PolicyRequest(String tool, List<String> resources, boolean write, int requestedRows) {

    public PolicyRequest {
        Objects.requireNonNull(tool, "tool");
        resources = List.copyOf(Objects.requireNonNull(resources, "resources"));
    }

    public static PolicyRequest read(String tool, List<String> resources, int requestedRows) {
        return new PolicyRequest(tool, resources, false, requestedRows);
    }

    /** A read that touches no named resource, e.g. listing what is visible at all. */
    public static PolicyRequest readMetadata(String tool) {
        return new PolicyRequest(tool, List.of(), false, 0);
    }

    public static PolicyRequest write(String tool, List<String> resources) {
        return new PolicyRequest(tool, resources, true, 0);
    }
}
