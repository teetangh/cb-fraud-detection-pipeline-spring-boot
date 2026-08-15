package com.fraud.decision.domain;

/**
 * The policy, loaded from {@code policy::default} in Couchbase — <b>not</b> from
 * application config.
 *
 * <p>That distinction is the whole point of ADR-0008. Policy is a *business*
 * property that changes for reasons unrelated to engineering (a promotional
 * period, a regulatory change, a seasonal fraud wave), so it must be changeable
 * without a deployment of any kind. Config would couple it to a release cycle.
 */
public record DecisionPolicy(
        String policyVersion,
        int allowBelow,
        int blockAtOrAbove
) {
    /**
     * Rejects a policy that cannot mean what it says.
     *
     * <p>An inverted policy ({@code allowBelow > blockAtOrAbove}) makes BLOCK
     * unreachable and silently allows everything — the fail-wide-open case this
     * system exists to prevent, arriving through a typo in a JSON document
     * rather than a decision. A rejected policy leaves the last known good one
     * in place.
     */
    public DecisionPolicy {
        if (allowBelow < 0 || blockAtOrAbove > 100 || allowBelow > blockAtOrAbove) {
            throw new IllegalArgumentException(
                    "Invalid policy: require 0 <= allowBelow <= blockAtOrAbove <= 100, got "
                    + "allowBelow=" + allowBelow + " blockAtOrAbove=" + blockAtOrAbove
                    + ". An inverted policy makes BLOCK unreachable and allows everything.");
        }
    }

    public Decision apply(int riskScore) {
        if (riskScore < allowBelow)       return Decision.ALLOW;
        if (riskScore >= blockAtOrAbove)  return Decision.BLOCK;
        return Decision.REVIEW;
    }
}
