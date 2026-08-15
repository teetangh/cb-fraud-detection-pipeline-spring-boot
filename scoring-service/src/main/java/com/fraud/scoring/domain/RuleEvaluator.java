package com.fraud.scoring.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pure rule evaluation. No Spring, no drivers — unit-testable with plain JUnit
 * and no containers.
 *
 * <p>The score is the sum of the weights of every enabled rule whose test passes,
 * capped at 100. Deterministic and fully explainable: every contributing rule
 * lands in {@code triggeredRules} with its actual value and the threshold that
 * was in force at that moment.
 */
public final class RuleEvaluator {

    private static final Logger log = LoggerFactory.getLogger(RuleEvaluator.class);

    /**
     * The eight seed rules sum to 175. Without this cap a heavily-flagged
     * transaction would score outside the documented 0–100 range and the policy
     * comparison in decision-service would be meaningless.
     */
    public static final int MAX_SCORE = 100;

    private final Set<String> signalRegistry;

    public RuleEvaluator(Set<String> signalRegistry) {
        this.signalRegistry = signalRegistry;
    }

    public ScoreResult score(Map<String, Object> signals, List<FraudRule> enabledRules) {
        List<TriggeredRule> triggered = new ArrayList<>();
        int total = 0;

        for (FraudRule rule : enabledRules) {
            Object value = signals.get(rule.signalKey());

            if (value == null) {
                // Either the signal was degraded (Redis down — ADR-0014) or the
                // rule references a key enrichment never produces.
                //
                // The second case fails SILENTLY: the rule simply never fires,
                // and the analyst who wrote it believes it is live. With no
                // shared DTO jar there is no compiler to catch a typo like
                // `velocity_1min`. A loud WARN is the only defence left.
                if (!signalRegistry.contains(rule.signalKey())) {
                    log.warn("Rule {} references signalKey '{}' which is NOT in the signal "
                             + "registry — this rule can NEVER fire. Check docs/CONTRACTS.md.",
                             rule.ruleId(), rule.signalKey());
                }
                continue;
            }

            if (matches(rule.operator(), value)) {
                triggered.add(new TriggeredRule(
                        rule.ruleId(), rule.weight(), value,
                        rule.operator().thresholdOrNull(), rule.operator().name(),
                        rule.category(), rule.version()));
                total += rule.weight();
            }
        }

        return new ScoreResult(Math.min(total, MAX_SCORE), List.copyOf(triggered));
    }

    /** Exhaustive over the sealed hierarchy — no {@code default} branch by design. */
    private boolean matches(RuleOperator operator, Object value) {
        return switch (operator) {
            case RuleOperator.BooleanTrue b -> value instanceof Boolean bool && bool;
            case RuleOperator.GreaterThan g -> asNumber(value).compareTo(g.threshold()) > 0;
            case RuleOperator.LessThan l    -> asNumber(value).compareTo(l.threshold()) < 0;
            case RuleOperator.Equals e      -> asNumber(value).compareTo(e.threshold()) == 0;
        };
    }

    /**
     * Signals arrive as JSON, so an integer count may deserialize as Integer,
     * Long or Double depending on the value. Comparing via BigDecimal keeps the
     * threshold test exact — a double comparison here would make
     * {@code amount_vs_p90_ratio > 3.0} unreliable at the boundary.
     */
    private static BigDecimal asNumber(Object value) {
        return switch (value) {
            case BigDecimal bd -> bd;
            case Integer i     -> BigDecimal.valueOf(i);
            case Long l        -> BigDecimal.valueOf(l);
            case Double d      -> BigDecimal.valueOf(d);
            case Number n      -> new BigDecimal(n.toString());
            case String s      -> new BigDecimal(s);
            default -> throw new IllegalArgumentException(
                    "Signal value is not numeric: " + value + " (" + value.getClass() + ")");
        };
    }
}
