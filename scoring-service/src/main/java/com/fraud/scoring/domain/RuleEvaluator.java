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

            if (matches(rule, value)) {
                triggered.add(new TriggeredRule(
                        rule.ruleId(), rule.weight(), value,
                        rule.operator().thresholdOrNull(), rule.operator().name(),
                        rule.category(), rule.version()));
                total += rule.weight();
            }
        }

        // clamp, not just cap: a hand-edited rule with a negative weight (a
        // typo, not a deliberate feature — spec §8 documents weight as
        // 0-100) must not be able to push the score below the documented
        // floor either.
        return new ScoreResult(Math.clamp(total, 0, MAX_SCORE), List.copyOf(triggered));
    }

    /** Exhaustive over the sealed hierarchy — no {@code default} branch by design. */
    private boolean matches(FraudRule rule, Object value) {
        RuleOperator operator = rule.operator();
        if (operator instanceof RuleOperator.BooleanTrue) {
            return value instanceof Boolean bool && bool;
        }

        // A rule can point a numeric operator at a signal that turns out not
        // to be numeric — a rule/signal mismatch, not a JVM-level bug. That
        // must cost this ONE rule, not the whole transaction: the previous
        // behaviour threw out of score() entirely, which meant one
        // misconfigured rule silently zeroed out every OTHER rule's
        // contribution too, and — via the listener — could abort the ack and
        // stall the partition.
        BigDecimal actual = asNumberOrNull(value);
        if (actual == null) {
            log.warn("Rule {} expects a numeric signal for '{}' but the value was {} ({}) — "
                     + "treating as not-matched rather than aborting scoring.",
                     rule.ruleId(), rule.signalKey(), value,
                     value == null ? "null" : value.getClass().getSimpleName());
            return false;
        }
        return switch (operator) {
            case RuleOperator.BooleanTrue b -> false; // handled above
            case RuleOperator.GreaterThan g -> actual.compareTo(g.threshold()) > 0;
            case RuleOperator.LessThan l    -> actual.compareTo(l.threshold()) < 0;
            case RuleOperator.Equals e      -> actual.compareTo(e.threshold()) == 0;
        };
    }

    /**
     * Signals arrive as JSON, so an integer count may deserialize as Integer,
     * Long or Double depending on the value. Comparing via BigDecimal keeps the
     * threshold test exact — a double comparison here would make
     * {@code amount_vs_p90_ratio > 3.0} unreliable at the boundary.
     *
     * @return the numeric value, or {@code null} if {@code value} is not
     *   coercible to a number (a non-numeric string, a boolean compared
     *   against a numeric operator, and so on) — callers treat that as "this
     *   rule does not match," not as an error.
     */
    private static BigDecimal asNumberOrNull(Object value) {
        try {
            return switch (value) {
                case BigDecimal bd -> bd;
                case Integer i     -> BigDecimal.valueOf(i);
                case Long l        -> BigDecimal.valueOf(l);
                case Double d      -> BigDecimal.valueOf(d);
                case Number n      -> new BigDecimal(n.toString());
                case String s      -> new BigDecimal(s);
                default -> null;
            };
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
