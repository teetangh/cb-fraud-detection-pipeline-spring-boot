package com.fraud.scoring.domain;

import java.math.BigDecimal;

/**
 * Sealed so the evaluation switch is exhaustive.
 *
 * <p>Adding a ninth operator without handling it everywhere becomes a
 * <b>compile error</b> rather than a runtime surprise on a live transaction at
 * 3am. That is the entire reason this is a sealed interface and not an enum with
 * a {@code default} branch.
 */
public sealed interface RuleOperator {

    record GreaterThan(BigDecimal threshold) implements RuleOperator {}
    record LessThan(BigDecimal threshold) implements RuleOperator {}
    record Equals(BigDecimal threshold) implements RuleOperator {}
    record BooleanTrue() implements RuleOperator {}

    static RuleOperator parse(String operator, BigDecimal threshold) {
        return switch (operator) {
            case "GREATER_THAN" -> new GreaterThan(threshold);
            case "LESS_THAN"    -> new LessThan(threshold);
            case "EQUALS"       -> new Equals(threshold);
            case "BOOLEAN_TRUE" -> new BooleanTrue();
            default -> throw new IllegalArgumentException(
                    "Unknown rule operator '" + operator + "'. Valid: GREATER_THAN, LESS_THAN, "
                    + "EQUALS, BOOLEAN_TRUE (spec §8).");
        };
    }

    /** The threshold as stored, or null for operators that do not use one. */
    default BigDecimal thresholdOrNull() {
        return switch (this) {
            case GreaterThan g -> g.threshold();
            case LessThan l    -> l.threshold();
            case Equals e      -> e.threshold();
            case BooleanTrue b -> null;
        };
    }

    default String name() {
        return switch (this) {
            case GreaterThan g -> "GREATER_THAN";
            case LessThan l    -> "LESS_THAN";
            case Equals e      -> "EQUALS";
            case BooleanTrue b -> "BOOLEAN_TRUE";
        };
    }
}
