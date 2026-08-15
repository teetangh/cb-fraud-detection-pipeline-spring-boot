package com.fraud.audit.domain;

import java.time.Instant;
import java.util.List;

/**
 * One append-only ledger entry. Key pattern
 * {@code audit::{transactionId}::{eventType}} (spec §8).
 */
public record AuditLedgerEntry(
        String transactionId,
        String customerId,
        int riskScore,
        String decision,
        List<Object> triggeredRules,
        String rulesetVersion,
        String policyVersion,
        boolean signalsDegraded,
        AuditEventType eventType,
        String serviceVersion,
        String correlationId,
        Instant recordedAt
) {
    public static final String DOC_TYPE = "AUDIT_LEDGER";

    public enum AuditEventType {
        DECISION_RECORDED,
        ACTION_EXECUTED,
        CASE_CREATED
    }

    public String documentKey() {
        return "audit::" + transactionId + "::" + eventType.name();
    }
}
