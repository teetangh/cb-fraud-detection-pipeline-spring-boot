package com.fraud.ingestion.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * The outbox row, written in the SAME Couchbase ACID transaction as the
 * transaction record (§9.2, ADR-0005).
 *
 * <p>Without this, a crash between the database write and the Kafka publish leaves
 * a transaction that is durably recorded and <em>silently never scored</em> — no
 * error, no retry, no alert. That is the failure mode this type exists to
 * prevent.
 */
public record OutboxEvent(
        String outboxId,
        Status status,
        String targetTopic,
        TransactionRecord payload,
        Instant createdAt,
        /** null until the Kafka producer has acknowledged. */
        Instant publishedAt
) {
    public static final String DOC_TYPE = "OUTBOX_EVENT";

    public enum Status { PENDING, PUBLISHED }

    /** outboxId is a fresh UUID, deliberately distinct from transactionId (spec §8). */
    public static OutboxEvent pendingFor(TransactionRecord txn, String topic, Instant now) {
        return new OutboxEvent(UUID.randomUUID().toString(), Status.PENDING, topic, txn, now, null);
    }

    public String documentKey() {
        return "outbox::" + outboxId;
    }
}
