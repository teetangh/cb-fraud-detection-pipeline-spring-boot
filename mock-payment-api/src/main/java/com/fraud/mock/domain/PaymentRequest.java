package com.fraud.mock.domain;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

/** docs/CONTRACTS.md §9 — POST /payments/initiate. */
public record PaymentRequest(
        @NotBlank String transactionId,
        @NotBlank String customerId,
        @NotBlank String merchantId,
        String merchantCategoryCode,
        @NotNull @DecimalMin(value = "0", inclusive = false) BigDecimal amount,
        String currency,
        String countryCode,
        @NotBlank String deviceId,
        String ipAddress,
        @NotBlank String paymentMethod
) {}
