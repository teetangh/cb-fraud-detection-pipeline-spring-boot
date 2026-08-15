package com.fraud.scoring.domain;

import java.math.BigDecimal;

/**
 * One rule that fired, with everything needed to explain the decision a year
 * later: what the signal actually was, and what threshold was in force at that
 * moment (docs/CONTRACTS.md §3).
 *
 * <p>{@code threshold} is null for {@code BOOLEAN_TRUE} operators.
 */
public record TriggeredRule(
        String ruleId,
        int contribution,
        Object actualValue,
        BigDecimal threshold,
        String operator,
        String category,
        int ruleVersion
) {}
