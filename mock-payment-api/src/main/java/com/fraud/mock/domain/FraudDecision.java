package com.fraud.mock.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * The gateway's response — docs/CONTRACTS.md §9.
 *
 * <p>{@code @JsonIgnoreProperties(ignoreUnknown = true)} is the TOLERANT READER
 * that makes independent deployment work (ADR-0002). With no shared DTO jar, an
 * additive change on the gateway must not break this consumer, and this
 * annotation is what guarantees that. It is not defensive clutter — removing it
 * would make every future field addition a coordinated, lockstep deploy.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FraudDecision(
        String transactionId,
        String correlationId,
        String decision,
        int riskScore,
        List<Object> triggeredRules,
        String policyVersion,
        String resolvedBy,
        long latencyMs
) {}
