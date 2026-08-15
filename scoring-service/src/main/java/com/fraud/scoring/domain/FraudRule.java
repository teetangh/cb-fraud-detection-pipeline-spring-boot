package com.fraud.scoring.domain;

import java.math.BigDecimal;

/**
 * A fraud rule, as stored at {@code rule::{ruleId}} in
 * {@code intelligence.fraud-rules} (spec §8).
 *
 * <p>Editable by a fraud analyst directly in Couchbase, with no code deploy —
 * which is spec §1's "rules must be updatable by a non-engineer" requirement.
 */
public record FraudRule(
        String ruleId,
        String description,
        String signalKey,
        RuleOperator operator,
        int weight,
        boolean enabled,
        String category,
        int version
) {}
