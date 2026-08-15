package com.fraud.ingestion.domain;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The raw transaction, exactly as specified in docs/CONTRACTS.md §1 and the
 * raw-transactions document in spec §8.
 *
 * <p>There is no shared DTO jar (ADR-0002), so this record's field names ARE the
 * contract with every other service. Changing one here without changing
 * docs/CONTRACTS.md and every consumer is a runtime break with no compile error.
 *
 * <p>{@code amount} is {@link BigDecimal}, never {@code double} — a floating-point
 * amount is a rounding bug waiting for a currency with more than two decimals.
 */
public record TransactionRecord(

        /** Client-supplied. Doubles as the idempotency key (§9.1). */
        @NotBlank String transactionId,

        @NotBlank String customerId,
        @NotBlank String merchantId,

        @Pattern(regexp = "^[0-9]{4}$", message = "merchantCategoryCode must be a 4-digit MCC")
        String merchantCategoryCode,

        @NotNull @DecimalMin(value = "0", inclusive = false, message = "amount must be > 0")
        BigDecimal amount,

        @Pattern(regexp = "^[A-Z]{3}$", message = "currency must be ISO 4217")
        String currency,

        @Pattern(regexp = "^[A-Z]{2}$", message = "countryCode must be ISO 3166-1 alpha-2")
        String countryCode,

        @NotBlank String deviceId,
        String ipAddress,

        @NotNull PaymentMethod paymentMethod,

        /** Minted by gateway-service, never regenerated downstream. */
        @NotBlank String correlationId,

        Instant createdAt
) {
    public static final String DOC_TYPE = "TRANSACTION";

    /** Couchbase key pattern from spec §8. */
    public String documentKey() {
        return "txn::" + transactionId;
    }
}
