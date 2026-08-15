package com.fraud.decision.config;

import com.fraud.decision.infra.PolicyCache;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Readiness stays DOWN until a policy has loaded.
 *
 * <p>Same asymmetry as scoring-service's ruleset indicator: degrading with a
 * STALE policy is safe; STARTING with none is not. Defaulting a policy would
 * silently invent business rules, and the most likely default — everything
 * ALLOW — is precisely the fail-wide-open case.
 */
@Component("policy")
public class DecisionReadinessIndicator implements HealthIndicator {

    private final PolicyCache policyCache;

    public DecisionReadinessIndicator(PolicyCache policyCache) {
        this.policyCache = policyCache;
    }

    @Override
    public Health health() {
        if (!policyCache.everLoaded()) {
            return Health.down()
                    .withDetail("reason", "no decision policy loaded yet — refusing traffic rather "
                                          + "than inventing thresholds")
                    .build();
        }
        return Health.up()
                .withDetail("policyVersion", policyCache.policy().policyVersion())
                .withDetail("allowBelow", policyCache.policy().allowBelow())
                .withDetail("blockAtOrAbove", policyCache.policy().blockAtOrAbove())
                .build();
    }
}
