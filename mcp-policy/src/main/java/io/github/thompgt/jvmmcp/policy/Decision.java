package io.github.thompgt.jvmmcp.policy;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The outcome of asking the policy engine whether a call may proceed.
 *
 * <p>A denial always names the {@link #rule} that fired. That is not only for the audit log:
 * the reason is returned to the model, and a model told <em>why</em> it was refused stops
 * retrying the same call. A denial without a usable reason is a denial that costs turns.
 *
 * @param allowed whether the call may touch the backend
 * @param rule identifier of the rule that decided, e.g. {@code allow-tables}
 * @param reason model-readable explanation; empty when allowed
 * @param detail machine-readable context for the client and the audit record
 * @param effective the limits that apply to this call after config and request are merged
 */
public record Decision(
        boolean allowed,
        String rule,
        String reason,
        Map<String, Object> detail,
        EffectiveLimits effective) {

    public Decision {
        Objects.requireNonNull(rule, "rule");
        Objects.requireNonNull(reason, "reason");
        detail = detail == null ? Map.of() : Map.copyOf(detail);
    }

    public static Decision allow(String rule, EffectiveLimits effective) {
        return new Decision(true, rule, "", Map.of(), effective);
    }

    public static Decision deny(String rule, String reason) {
        return new Decision(false, rule, reason, Map.of(), EffectiveLimits.none());
    }

    public static Decision deny(String rule, String reason, Map<String, Object> detail) {
        return new Decision(false, rule, reason, detail, EffectiveLimits.none());
    }

    /**
     * A denial that tells the model what <em>would</em> work.
     *
     * <p>"not allowed" leaves the model guessing; "not allowed, these are" lets it recover on
     * the next call. Where the permitted set is enumerable, it should always be listed.
     */
    public static Decision denyWithAlternatives(
            String rule, String what, String subject, List<String> permitted) {
        String reason =
                permitted.isEmpty()
                        ? what + " '" + subject + "' is not permitted, and no " + what + " is configured"
                        : what
                                + " '"
                                + subject
                                + "' is not in the allowlist; permitted "
                                + what
                                + "s are: "
                                + String.join(", ", permitted);
        return new Decision(
                false,
                rule,
                reason,
                Map.of("denied", subject, "permitted", List.copyOf(permitted)),
                EffectiveLimits.none());
    }
}
