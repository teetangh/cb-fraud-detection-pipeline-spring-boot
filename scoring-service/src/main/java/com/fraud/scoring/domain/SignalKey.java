package com.fraud.scoring.domain;

import java.util.Set;

/**
 * The signal key registry — scoring-service's OWN copy (ADR-0002: no shared DTO
 * jar). It must stay identical to enrichment-service's; a drift here means a
 * rule that scoring believes is valid but which enrichment never produces.
 *
 * <p>A hard contract with the {@code signalKey} field of every fraud rule
 * document (spec §8, docs/CONTRACTS.md).
 *
 * <p>A rule whose {@code signalKey} is not in this set can never fire. With no
 * shared DTO jar (ADR-0002) there is no compiler to catch a typo like
 * {@code velocity_1min}, so the fraud analyst who wrote the rule believes it is
 * live when it is not. scoring-service logs a WARN for any enabled rule whose
 * key is absent from this registry — turning a silent misconfiguration into a
 * loud one is the only defence left once the compiler is out of the picture.
 */
public final class SignalKey {

    private SignalKey() {}

    // ── Redis-backed: degrade when Redis is unavailable ──────────────────────
    public static final String VELOCITY_1M            = "velocity_1m";
    public static final String VELOCITY_1H            = "velocity_1h";
    public static final String DISTINCT_COUNTRIES_24H = "distinct_countries_24h";
    public static final String CUSTOMERS_PER_DEVICE   = "customers_per_device";
    public static final String IS_NEW_DEVICE_HIGH_AMT = "is_new_device_high_amt";

    // ── Couchbase-backed: durable, survive a Redis flush (ADR-0015) ──────────
    public static final String MERCHANT_RISK_SCORE    = "merchant_risk_score";
    public static final String LIFETIME_TXN_COUNT     = "lifetime_txn_count";
    public static final String AMOUNT_VS_P90_RATIO    = "amount_vs_p90_ratio";

    /** Computed from the message itself — never degrades, no I/O involved. */
    public static final String IS_OFF_HOURS_LARGE     = "is_off_hours_large";

    public static final Set<String> REGISTRY = Set.of(
            VELOCITY_1M, VELOCITY_1H, DISTINCT_COUNTRIES_24H, CUSTOMERS_PER_DEVICE,
            IS_NEW_DEVICE_HIGH_AMT, MERCHANT_RISK_SCORE, LIFETIME_TXN_COUNT,
            AMOUNT_VS_P90_RATIO, IS_OFF_HOURS_LARGE);
}
