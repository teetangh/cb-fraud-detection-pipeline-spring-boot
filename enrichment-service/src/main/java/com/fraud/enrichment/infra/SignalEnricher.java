package com.fraud.enrichment.infra;

import com.fraud.enrichment.domain.SignalKey;
import com.fraud.enrichment.domain.SignalSet;
import com.fraud.enrichment.domain.TransactionRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.function.Supplier;

/**
 * Computes the full signal set, degrading each signal independently.
 *
 * <p>The degradation policy is the whole point of this class: a signal that
 * cannot be computed is <b>omitted</b>, never zeroed (ADR-0014). One dead source
 * must not take the others with it either — hence a separate guarded call per
 * signal rather than one try block around everything.
 */
@Component
public class SignalEnricher {

    private static final Logger log = LoggerFactory.getLogger(SignalEnricher.class);

    private final RedisSignalSource redis;
    private final CouchbaseSignalSource couchbase;
    private final MeterRegistry meterRegistry;
    private final BigDecimal offHoursAmountThreshold;
    private final int offHoursStart;
    private final int offHoursEnd;

    public SignalEnricher(RedisSignalSource redis,
                          CouchbaseSignalSource couchbase,
                          MeterRegistry meterRegistry,
                          @Value("${fraud.signals.off-hours-amount-threshold:50000}") BigDecimal offHoursAmountThreshold,
                          @Value("${fraud.signals.off-hours-start-utc:0}") int offHoursStart,
                          @Value("${fraud.signals.off-hours-end-utc:5}") int offHoursEnd) {
        this.redis = redis;
        this.couchbase = couchbase;
        this.meterRegistry = meterRegistry;
        this.offHoursAmountThreshold = offHoursAmountThreshold;
        this.offHoursStart = offHoursStart;
        this.offHoursEnd = offHoursEnd;
    }

    public SignalSet enrich(TransactionRecord txn) {
        SignalSet.Builder signals = SignalSet.builder();

        // ── Redis-backed ────────────────────────────────────────────────────
        trySignal(signals, SignalKey.VELOCITY_1M, () -> redis.velocity1m(txn.customerId()));
        trySignal(signals, SignalKey.VELOCITY_1H, () -> redis.velocity1h(txn.customerId()));
        trySignal(signals, SignalKey.DISTINCT_COUNTRIES_24H,
                () -> redis.distinctCountries24h(txn.customerId(), txn.countryCode()));
        trySignal(signals, SignalKey.CUSTOMERS_PER_DEVICE,
                () -> redis.customersPerDevice(txn.deviceId(), txn.customerId()));
        trySignal(signals, SignalKey.IS_NEW_DEVICE_HIGH_AMT,
                () -> redis.isNewDeviceHighAmount(txn.customerId(), txn.deviceId(), txn.amount()));

        // ── Couchbase-backed ────────────────────────────────────────────────
        // The ratio depends on the lifetime count, so they are computed together:
        // if the counter is unavailable the ratio is not merely stale, it is
        // unqualified, and emitting it would re-enable the AMOUNT_DEVIATION false
        // positive the history gate exists to prevent.
        try {
            long lifetime = couchbase.incrementLifetimeCount(txn.customerId());
            signals.put(SignalKey.LIFETIME_TXN_COUNT, lifetime);
            signals.put(SignalKey.AMOUNT_VS_P90_RATIO,
                    couchbase.amountVsP90Ratio(txn.customerId(), txn.amount(), lifetime));
        } catch (Exception e) {
            degrade(signals, SignalKey.LIFETIME_TXN_COUNT, e);
            degrade(signals, SignalKey.AMOUNT_VS_P90_RATIO, e);
        }

        trySignal(signals, SignalKey.MERCHANT_RISK_SCORE,
                () -> couchbase.merchantRiskScore(txn.merchantCategoryCode()));

        // ── Pure computation: never degrades, no I/O ────────────────────────
        signals.put(SignalKey.IS_OFF_HOURS_LARGE, isOffHoursLarge(txn));

        SignalSet result = signals.build();
        if (result.signalsDegraded()) {
            log.warn("Signals degraded keys={} transactionId={} correlationId={} — the affected "
                     + "rules cannot fire and contribute nothing",
                     result.degradedSignalKeys(), txn.transactionId(), txn.correlationId());
        }
        return result;
    }

    private void trySignal(SignalSet.Builder signals, String key, Supplier<Object> compute) {
        try {
            signals.put(key, compute.get());
        } catch (Exception e) {
            degrade(signals, key, e);
        }
    }

    /** Records the signal as ABSENT. Never writes a zero — see {@link SignalSet}. */
    private void degrade(SignalSet.Builder signals, String key, Exception e) {
        signals.degrade(key);
        meterRegistry.counter("fraud.enrichment.signal.degraded", "signal", key).increment();
        log.debug("Signal {} unavailable", key, e);
    }

    private boolean isOffHoursLarge(TransactionRecord txn) {
        if (txn.createdAt() == null) {
            return false;
        }
        int hour = txn.createdAt().atZone(ZoneOffset.UTC).getHour();
        boolean offHours = hour >= offHoursStart && hour < offHoursEnd;
        return offHours && txn.amount().compareTo(offHoursAmountThreshold) > 0;
    }
}
