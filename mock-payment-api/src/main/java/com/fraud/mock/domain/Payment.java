package com.fraud.mock.domain;

import java.math.BigDecimal;
import java.time.Instant;

/** In-memory payment record. Deliberately not persisted — see PaymentStore. */
public record Payment(
        String paymentId,
        String transactionId,
        String correlationId,
        BigDecimal amount,
        Status status,
        String fraudDecision,
        int riskScore,
        String resolvedBy,
        Instant createdAt,
        /** Set when a reconciliation webhook has been APPLIED, not merely received. */
        Instant reconciledAt,
        String previousDecision
) {
    public enum Status {
        /** fraud ALLOW */   COMPLETED,
        /** fraud REVIEW */  HELD,
        /** fraud BLOCK */   DECLINED
    }

    /**
     * The mapping that makes the timeout default a real business state rather
     * than a fudge: REVIEW becomes HELD, which is recoverable. A fraudulent
     * COMPLETED payment often is not. That asymmetry is the whole argument in
     * ADR-0004, and this switch is where it becomes concrete.
     */
    public static Status statusFor(String fraudDecision) {
        return switch (fraudDecision) {
            case "ALLOW"  -> Status.COMPLETED;
            case "REVIEW" -> Status.HELD;
            case "BLOCK"  -> Status.DECLINED;
            default -> throw new IllegalArgumentException("Unknown fraud decision: " + fraudDecision);
        };
    }

    public Payment reconciled(String finalDecision, int newRiskScore, Instant at) {
        return new Payment(paymentId, transactionId, correlationId, amount,
                statusFor(finalDecision), finalDecision, newRiskScore, resolvedBy,
                createdAt, at, fraudDecision);
    }
}
