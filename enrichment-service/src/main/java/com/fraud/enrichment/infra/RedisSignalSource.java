package com.fraud.enrichment.infra;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

/**
 * The Redis-backed signals. Every method here is exactly ONE {@code EVAL} —
 * no check-then-act sequence ever crosses the network boundary (§9.3, ADR-0007).
 *
 * <p>Scripts live in {@code src/main/resources/lua/} as real {@code .lua} files
 * rather than inlined Java strings: they are independently testable and readable
 * by someone who does not read Java. Spring loads them once and invokes by SHA
 * via {@code EVALSHA}, handling the {@code NOSCRIPT} reload automatically.
 *
 * <p>Nothing here catches exceptions. Failure handling belongs to the caller
 * ({@code SignalEnricher}), which degrades each signal independently — a
 * swallowed exception here would silently produce a wrong value instead of an
 * honestly absent one.
 */
@Component
public class RedisSignalSource {

    private static final long VELOCITY_1M_TTL = 60;
    private static final long VELOCITY_1H_TTL = 3600;
    private static final long GEO_TTL         = 86_400;          // 24h
    private static final long DEVICE_TTL      = 30L * 86_400;    // 30d
    private static final long KNOWN_DEV_TTL   = 90L * 86_400;    // 90d

    private final StringRedisTemplate redis;
    private final RedisScript<Long> velocityScript;
    private final RedisScript<Long> setAddCountScript;
    private final RedisScript<Long> knownDeviceScript;
    private final BigDecimal highAmountThreshold;

    public RedisSignalSource(StringRedisTemplate redis,
                             @Value("${fraud.signals.high-amount-threshold:10000}") BigDecimal highAmountThreshold) {
        this.redis = redis;
        this.highAmountThreshold = highAmountThreshold;
        this.velocityScript = load("lua/velocity.lua");
        this.setAddCountScript = load("lua/set_add_count.lua");
        this.knownDeviceScript = load("lua/known_device.lua");
    }

    private static RedisScript<Long> load(String path) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(path));
        script.setResultType(Long.class);
        return script;
    }

    /**
     * Counts INCLUDING the current transaction — the script increments before
     * returning. An off-by-one here silently shifts every velocity rule by one
     * transaction, which is why T2 pins the 6th transaction specifically.
     */
    public long velocity1m(String customerId) {
        return redis.execute(velocityScript,
                List.of("velocity:1m:" + customerId), String.valueOf(VELOCITY_1M_TTL));
    }

    public long velocity1h(String customerId) {
        return redis.execute(velocityScript,
                List.of("velocity:1h:" + customerId), String.valueOf(VELOCITY_1H_TTL));
    }

    public long distinctCountries24h(String customerId, String countryCode) {
        return redis.execute(setAddCountScript,
                List.of("geo:countries:" + customerId), countryCode, String.valueOf(GEO_TTL));
    }

    public long customersPerDevice(String deviceId, String customerId) {
        return redis.execute(setAddCountScript,
                List.of("device:customers:" + deviceId), customerId, String.valueOf(DEVICE_TTL));
    }

    /**
     * True only when the device is new for this customer AND the amount is high.
     * The membership test and the add are one atomic step — see known_device.lua
     * for why separating them double-fires the rule.
     */
    public boolean isNewDeviceHighAmount(String customerId, String deviceId, BigDecimal amount) {
        long isNew = redis.execute(knownDeviceScript,
                List.of("device:known:" + customerId), deviceId, String.valueOf(KNOWN_DEV_TTL));
        return isNew == 1L && amount.compareTo(highAmountThreshold) > 0;
    }
}
