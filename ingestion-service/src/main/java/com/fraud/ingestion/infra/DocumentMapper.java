package com.fraud.ingestion.infra;

import com.couchbase.client.java.json.JsonObject;
import com.fraud.ingestion.domain.OutboxEvent;
import com.fraud.ingestion.domain.PaymentMethod;
import com.fraud.ingestion.domain.TransactionRecord;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Explicit record &lt;-&gt; {@link JsonObject} mapping.
 *
 * <p>Deliberately hand-written rather than delegating to the SDK's POJO
 * serializer. The stored document shape IS the contract (docs/CONTRACTS.md,
 * spec §8) and there is no shared DTO jar to enforce it — so the mapping is
 * written out where it can be read and reviewed, instead of being an emergent
 * property of annotations and a serializer's defaults.
 *
 * <p>It also pins the two things a serializer would get to choose: timestamps are
 * ISO-8601 strings, and {@code amount} round-trips through {@link BigDecimal}
 * without touching a double.
 */
public final class DocumentMapper {

    private DocumentMapper() {}

    public static JsonObject toJson(TransactionRecord txn) {
        return JsonObject.create()
                .put("docType", TransactionRecord.DOC_TYPE)
                .put("transactionId", txn.transactionId())
                .put("customerId", txn.customerId())
                .put("merchantId", txn.merchantId())
                .put("merchantCategoryCode", txn.merchantCategoryCode())
                .put("amount", txn.amount())
                .put("currency", txn.currency())
                .put("countryCode", txn.countryCode())
                .put("deviceId", txn.deviceId())
                .put("ipAddress", txn.ipAddress())
                .put("paymentMethod", txn.paymentMethod().name())
                .put("status", "PENDING")
                .put("correlationId", txn.correlationId())
                .put("createdAt", txn.createdAt().toString());
    }

    public static TransactionRecord toTransaction(JsonObject json) {
        return new TransactionRecord(
                json.getString("transactionId"),
                json.getString("customerId"),
                json.getString("merchantId"),
                json.getString("merchantCategoryCode"),
                json.getBigDecimal("amount"),
                json.getString("currency"),
                json.getString("countryCode"),
                json.getString("deviceId"),
                json.getString("ipAddress"),
                PaymentMethod.valueOf(json.getString("paymentMethod")),
                json.getString("correlationId"),
                Instant.parse(json.getString("createdAt"))
        );
    }

    public static JsonObject toJson(OutboxEvent event) {
        return JsonObject.create()
                .put("docType", OutboxEvent.DOC_TYPE)
                .put("outboxId", event.outboxId())
                .put("status", event.status().name())
                .put("targetTopic", event.targetTopic())
                .put("payload", toJson(event.payload()))
                .put("createdAt", event.createdAt().toString())
                .put("publishedAt", event.publishedAt() == null ? null : event.publishedAt().toString());
    }

    public static OutboxEvent toOutboxEvent(JsonObject json) {
        String publishedAt = json.getString("publishedAt");
        return new OutboxEvent(
                json.getString("outboxId"),
                OutboxEvent.Status.valueOf(json.getString("status")),
                json.getString("targetTopic"),
                toTransaction(json.getObject("payload")),
                Instant.parse(json.getString("createdAt")),
                publishedAt == null ? null : Instant.parse(publishedAt)
        );
    }
}
