package com.fraud.decision.domain;

/**
 * ALLOW / REVIEW / BLOCK.
 *
 * <p>REVIEW is a real business state, not a fudge — it maps to HELD in
 * mock-payment-api. A held payment is recoverable; a fraudulent completed
 * payment often is not. That asymmetry is why REVIEW, never ALLOW, is the
 * timeout default (ADR-0004).
 */
public enum Decision {
    ALLOW, REVIEW, BLOCK
}
