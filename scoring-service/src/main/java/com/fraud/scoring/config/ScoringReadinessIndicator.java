package com.fraud.scoring.config;

import com.fraud.scoring.infra.RuleCache;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Readiness fails until a ruleset has been loaded at least once.
 *
 * <p>The asymmetry with {@link RuleCache}'s refresh behaviour is deliberate:
 * <b>degrading with a stale ruleset is safe; starting with no ruleset is not.</b>
 *
 * <p>With an empty ruleset every transaction scores 0, and 0 maps to ALLOW under
 * the seed policy — so a scoring service that started without its rules would
 * silently allow everything while reporting healthy. That is the fail-wide-open
 * case the whole system exists to prevent, arriving through a startup ordering
 * accident rather than a decision.
 */
@Component("ruleset")
public class ScoringReadinessIndicator implements HealthIndicator {

    private final RuleCache ruleCache;

    public ScoringReadinessIndicator(RuleCache ruleCache) {
        this.ruleCache = ruleCache;
    }

    @Override
    public Health health() {
        if (!ruleCache.everLoaded()) {
            return Health.down()
                    .withDetail("reason", "no ruleset loaded yet — refusing traffic, because an "
                                          + "empty ruleset scores every transaction 0, and 0 means ALLOW")
                    .build();
        }
        return Health.up()
                .withDetail("enabledRules", ruleCache.enabledRules().size())
                .withDetail("rulesetVersion", ruleCache.rulesetVersion())
                .build();
    }
}
