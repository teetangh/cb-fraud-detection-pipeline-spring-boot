package com.fraud.ingestion.domain;

/**
 * What the caller gets back from an accept. Cached in Redis under
 * {@code idempotency:{transactionId}} so a duplicate retry receives the
 * <em>identical</em> response rather than a second, differently-shaped one.
 */
public record IngestionResult(
        String transactionId,
        String correlationId,
        Status status
) {
    public enum Status {
        /** Freshly accepted and durably committed. */
        ACCEPTED,
        /** A duplicate of an already-accepted transaction. Not an error. */
        DUPLICATE
    }

    public IngestionResult asDuplicate() {
        return new IngestionResult(transactionId, correlationId, Status.DUPLICATE);
    }
}
