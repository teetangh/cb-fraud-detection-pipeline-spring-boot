package com.fraud.enrichment.domain;

/** docs/CONTRACTS.md §1. Consumers must route unknown values to DLQ, not crash. */
public enum PaymentMethod {
    CARD,
    BANK_TRANSFER,
    WALLET
}
