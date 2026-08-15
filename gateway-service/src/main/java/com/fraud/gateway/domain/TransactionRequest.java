package com.fraud.gateway.domain;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.math.BigDecimal;

/**
 * Inbound shape — docs/CONTRACTS.md §1 minus {@code correlationId}, which the
 * gateway mints.
 *
 * <p>Its own copy of the record, deliberately. There is no shared DTO jar
 * (ADR-0002), so this duplicates ingestion-service's {@code TransactionRecord}
 * by design: the two services can be deployed on different schedules, and a
 * shared jar would make that false in practice while remaining true on paper.
 */
public record TransactionRequest(
        @NotBlank String transactionId,
        @NotBlank String customerId,
        @NotBlank String merchantId,
        @Pattern(regexp = "^[0-9]{4}$", message = "merchantCategoryCode must be a 4-digit MCC")
        String merchantCategoryCode,
        @NotNull @DecimalMin(value = "0", inclusive = false, message = "amount must be > 0")
        BigDecimal amount,
        @Pattern(regexp = "^[A-Z]{3}$", message = "currency must be ISO 4217") String currency,
        @Pattern(regexp = "^[A-Z]{2}$", message = "countryCode must be ISO 3166-1 alpha-2") String countryCode,
        @NotBlank String deviceId,
        String ipAddress,
        @NotNull PaymentMethod paymentMethod
) {
    public enum PaymentMethod { CARD, BANK_TRANSFER, WALLET }
}
