package com.fraud.enrichment.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * {@code fraud.transactions.enriched} — docs/CONTRACTS.md §2.
 *
 * <p>The full raw envelope plus the signal map. The raw fields are carried
 * forward rather than referenced, so every downstream stage has everything it
 * needs without a lookup, and a replay from this topic alone is sufficient.
 */
public record EnrichedTransaction(
        String transactionId,
        String customerId,
        String merchantId,
        String merchantCategoryCode,
        BigDecimal amount,
        String currency,
        String countryCode,
        String deviceId,
        String ipAddress,
        PaymentMethod paymentMethod,
        String correlationId,
        Instant createdAt,

        Map<String, Object> signals,
        boolean signalsDegraded,
        List<String> degradedSignalKeys,
        Instant enrichedAt
) {
    public static EnrichedTransaction of(TransactionRecord txn, SignalSet signals, Instant enrichedAt) {
        return new EnrichedTransaction(
                txn.transactionId(), txn.customerId(), txn.merchantId(), txn.merchantCategoryCode(),
                txn.amount(), txn.currency(), txn.countryCode(), txn.deviceId(), txn.ipAddress(),
                txn.paymentMethod(), txn.correlationId(), txn.createdAt(),
                signals.signals(), signals.signalsDegraded(), signals.degradedSignalKeys(),
                enrichedAt);
    }
}
