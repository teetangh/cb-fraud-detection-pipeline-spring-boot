package com.fraud.gateway.domain;

import java.util.List;

/**
 * What the caller gets back. docs/CONTRACTS.md §9.
 *
 * <p>{@code resolvedBy} is the most operationally important field in the whole
 * API: {@code PIPELINE} means a real decision arrived inside the budget,
 * {@code TIMEOUT_DEFAULT} means the safe default fired. In production
 * {@code TIMEOUT_DEFAULT ÷ total} is the system's primary SLO — the answer to
 * "what fraction of our decisions were actually decisions?"
 */
public record FraudDecisionResponse(
        String transactionId,
        String correlationId,
        Decision decision,
        int riskScore,
        List<TriggeredRule> triggeredRules,
        String policyVersion,
        DecisionSource resolvedBy,
        long latencyMs
) {
    public enum Decision { ALLOW, REVIEW, BLOCK }

    public enum DecisionSource {
        /** The real decision arrived via the pipeline within the budget. */
        PIPELINE,
        /**
         * The 150ms budget elapsed. REVIEW, never ALLOW — an unresolved decision
         * must never silently permit a transaction (§9.8, ADR-0004).
         */
        TIMEOUT_DEFAULT
    }

    public record TriggeredRule(String ruleId, int contribution, Object actualValue, Object threshold) {}

    /**
     * The Phase 2b placeholder. Until the Redis Pub/Sub wait lands in Phase 4b
     * there is no decision to report, so the gateway is explicit about that
     * rather than inventing an ALLOW — inventing one would be the exact
     * fail-wide-open behaviour ADR-0004 exists to prevent, and it would be
     * indistinguishable from a real ALLOW in the logs.
     */
    public static FraudDecisionResponse pendingPipeline(String transactionId,
                                                        String correlationId,
                                                        long latencyMs) {
        return new FraudDecisionResponse(transactionId, correlationId,
                Decision.REVIEW, 0, List.of(), "pending",
                DecisionSource.TIMEOUT_DEFAULT, latencyMs);
    }
}
