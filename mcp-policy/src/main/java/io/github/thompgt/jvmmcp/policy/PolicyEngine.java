package io.github.thompgt.jvmmcp.policy;

import io.github.thompgt.jvmmcp.core.CallContext;
import io.github.thompgt.jvmmcp.core.Principal;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single gate between every adapter and every backend.
 *
 * <p>The API is shaped to make bypassing it awkward rather than merely discouraged. An
 * adapter does not ask "may I?" and then act — it hands the engine a {@link GuardedCall},
 * and the {@link EffectiveLimits} it needs to bound its own work only exist inside that
 * callback. There is no method here that returns a permission without also running the call
 * and auditing the result.
 *
 * @see <a href="file:../../../../../../../docs/adr/002-policy-engine-between-adapter-and-backend.md">ADR 002</a>
 */
public final class PolicyEngine {

    private static final Logger log = LoggerFactory.getLogger(PolicyEngine.class);

    private final PolicyProfiles profiles;
    private final AuditSink audit;
    private final Clock clock;
    private final Supplier<Principal> principal;

    public PolicyEngine(PolicyProfile profile, AuditSink audit) {
        this(PolicyProfiles.of(profile), audit);
    }

    public PolicyEngine(PolicyProfiles profiles, AuditSink audit) {
        this(profiles, audit, Clock.systemUTC(), CallContext::principal);
    }

    /**
     * @param principal resolved per call rather than fixed at construction. One engine serves
     *     every caller on a backend, so binding an identity here would attribute every HTTP
     *     request to whoever happened to start the server.
     */
    public PolicyEngine(
            PolicyProfiles profiles, AuditSink audit, Clock clock, Supplier<Principal> principal) {
        this.profiles = profiles;
        this.audit = audit;
        this.clock = clock;
        this.principal = principal;
    }

    /**
     * The backend default, which is what tool descriptors are built from.
     *
     * <p>Not the caller's profile — descriptors are generated once at startup, before anyone has
     * connected. {@link PolicyProfiles} guarantees named profiles only narrow this, so a
     * description is always an upper bound on what the caller will actually be allowed.
     */
    public PolicyProfile profile() {
        return profiles.defaultProfile();
    }

    /** The profile in force for whoever is calling right now. */
    public PolicyProfile effectiveProfile() {
        return profiles.forPrincipal(principal.get());
    }

    /** The body of a permitted call. Receives the bounds it must respect. */
    @FunctionalInterface
    public interface GuardedCall {
        ToolOutcome execute(EffectiveLimits limits) throws Exception;
    }

    /** Produces the resolved plan shown in dry-run mode, in place of touching the backend. */
    @FunctionalInterface
    public interface PlanDescriber {
        ToolOutcome describe(EffectiveLimits limits);
    }

    /**
     * Evaluates the request and, if permitted, runs the call — auditing either way.
     *
     * @param plan what to return instead of executing when the mode is {@code DRY_RUN}
     */
    public ToolOutcome guard(PolicyRequest request, PlanDescriber plan, GuardedCall call) {
        // Resolved once and reused, so a decision and the call it permits cannot be evaluated
        // against two different profiles if the binding were to change mid-call.
        PolicyProfile profile = effectiveProfile();
        Decision decision = evaluate(request, profile);
        long startedAt = clock.millis();

        if (!decision.allowed()) {
            audit(request, decision, -1, clock.millis() - startedAt);
            return ToolOutcome.denied(decision.reason(), decision.detail());
        }

        if (decision.effective().dryRun()) {
            ToolOutcome outcome = plan.describe(decision.effective());
            audit(request, decision, outcome.rowCount(), clock.millis() - startedAt);
            return outcome;
        }

        try {
            ToolOutcome outcome = call.execute(decision.effective());
            audit(request, decision, outcome.rowCount(), clock.millis() - startedAt);
            return outcome;
        } catch (Exception e) {
            // Audited as allowed-but-failed: the call was permitted and did reach the
            // backend, which is what an incident review needs to know.
            audit(request, decision, -1, clock.millis() - startedAt);
            log.error("guarded call for tool {} failed", request.tool(), e);
            throw new BackendCallException(
                    "tool '"
                            + request.tool()
                            + "' could not complete against backend '"
                            + profile.backendName()
                            + "'. The server log has the detail.",
                    e);
        }
    }

    /** A backend call that was permitted but failed. Message is safe to show a model. */
    public static final class BackendCallException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public BackendCallException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Decides without executing. Exposed for tests and for {@code dry-run} tooling; production
     * paths should go through {@link #guard} so the audit record is not optional.
     */
    public Decision evaluate(PolicyRequest request) {
        return evaluate(request, effectiveProfile());
    }

    private Decision evaluate(PolicyRequest request, PolicyProfile profile) {
        if (request.write()) {
            if (profile.mode() != AccessMode.READ_WRITE) {
                return Decision.deny(
                        "mode",
                        "backend '"
                                + profile.backendName()
                                + "' is in "
                                + profile.mode().name().toLowerCase(java.util.Locale.ROOT)
                                + " mode, so writes are refused. This is a server configuration"
                                + " setting and cannot be changed from a tool call.");
            }
            for (String resource : request.resources()) {
                if (!profile.isWritable(resource)) {
                    return Decision.denyWithAlternatives(
                            "allow-write", "writable resource", resource, profile.writableResources());
                }
            }
        } else {
            for (String resource : request.resources()) {
                if (!profile.isReadable(resource)) {
                    return Decision.denyWithAlternatives(
                            "allow-read", "resource", resource, profile.readableResources());
                }
            }
        }

        EffectiveLimits limits = profile.baseLimits().withRequestedRows(request.requestedRows());
        return Decision.allow(request.write() ? "allow-write" : "allow-read", limits);
    }

    private void audit(PolicyRequest request, Decision decision, int rows, long millis) {
        audit.record(
                new AuditRecord(
                        clock.instant(),
                        principal.get().name(),
                        // Every profile on a backend shares its name, so the default's is right
                        // whichever profile the decision was made against.
                        profiles.defaultProfile().backendName(),
                        request.tool(),
                        request.resources(),
                        decision.allowed(),
                        decision.rule(),
                        decision.reason(),
                        rows,
                        millis));
    }

    /** Convenience for the common read case with no dry-run distinction to draw. */
    public ToolOutcome guardRead(String tool, List<String> resources, int requestedRows, GuardedCall call) {
        PolicyRequest request = PolicyRequest.read(tool, resources, requestedRows);
        return guard(
                request,
                limits ->
                        ToolOutcome.success(
                                java.util.Map.of(
                                        "dryRun", true,
                                        "tool", tool,
                                        "resources", resources,
                                        "maxRows", limits.maxRows(),
                                        "timeoutMillis", limits.timeout().toMillis()),
                                "dry-run: this call is permitted and would read "
                                        + (resources.isEmpty() ? "backend metadata" : String.join(", ", resources))
                                        + " with at most "
                                        + limits.maxRows()
                                        + " rows. Nothing was executed."),
                call);
    }

    /**
     * The write counterpart, which is two gates rather than one: the backend must be in
     * {@link AccessMode#READ_WRITE}, <em>and</em> every named resource must be on the write
     * allowlist, which admits no wildcards. Failing either is a denial naming which.
     *
     * <p>An adapter must describe the mutation it intends through {@code plan} rather than
     * reusing the read plan, because a dry-run of a write is the only preview a caller gets of
     * something irreversible, and "this call is permitted" is not a preview.
     */
    public ToolOutcome guardWrite(String tool, List<String> resources, PlanDescriber plan, GuardedCall call) {
        return guard(PolicyRequest.write(tool, resources), plan, call);
    }

    /**
     * The timeout an adapter should apply when setting up a connection.
     *
     * <p>The backend default, because pools are built at startup. Per-call bounds come from the
     * {@link EffectiveLimits} handed to a {@link GuardedCall}, which do reflect the caller's
     * profile — an adapter that needs the caller's timeout must take it from there.
     */
    public Duration timeout() {
        return profiles.defaultProfile().timeout();
    }
}
