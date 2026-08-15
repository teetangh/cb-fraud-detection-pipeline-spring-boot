package com.fraud.gateway.infra;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Fixed-window rate limiting, one atomic Lua round trip (§9.3).
 *
 * <p><b>Fails OPEN.</b> If Redis is unavailable, requests are permitted rather
 * than rejected. That is a deliberate decision on a security control, so it is
 * stated explicitly rather than left implicit: this system sits in the payment
 * path, and a cache outage must not stop commerce (ADR-0014). The same reasoning
 * that makes signal degradation fail open applies here.
 */
@Component
public class RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);
    private static final Duration WINDOW = Duration.ofMinutes(1);

    private final ReactiveStringRedisTemplate redis;
    private final RedisScript<Long> script;
    private final int limitPerMinute;
    private final Counter rejectedCounter;
    private final Counter failOpenCounter;

    public RateLimiter(ReactiveStringRedisTemplate redis,
                       MeterRegistry meterRegistry,
                       @Value("${fraud.rate-limit.requests-per-minute:100}") int limitPerMinute) {
        this.redis = redis;
        // Loaded from a real .lua resource, not an inlined Java string — it is
        // independently testable and readable by someone who does not read Java.
        this.script = RedisScript.of(new ClassPathResource("lua/rate_limit.lua"), Long.class);
        this.limitPerMinute = limitPerMinute;
        this.rejectedCounter = Counter.builder("fraud.gateway.ratelimit.rejected")
                .register(meterRegistry);
        this.failOpenCounter = Counter.builder("fraud.gateway.ratelimit.failed_open")
                .description("Requests permitted because Redis was unreachable")
                .register(meterRegistry);
    }

    /** @return true if the request is permitted. */
    public Mono<Boolean> tryAcquire(String clientId) {
        String key = "ratelimit:" + clientId + ":" + Instant.now().truncatedTo(ChronoUnit.MINUTES).getEpochSecond();

        return redis.execute(script,
                        List.of(key),
                        List.of(String.valueOf(WINDOW.toSeconds()), String.valueOf(limitPerMinute)))
                .next()
                .map(count -> {
                    if (count < 0) {
                        rejectedCounter.increment();
                        log.info("Rate limit exceeded clientId={} limit={}/min", clientId, limitPerMinute);
                        return false;
                    }
                    return true;
                })
                .timeout(Duration.ofMillis(200))
                .onErrorResume(e -> {
                    failOpenCounter.increment();
                    log.warn("Rate limiter unavailable — FAILING OPEN and permitting the request. "
                             + "A cache outage must not stop payments (ADR-0014). clientId={}",
                             clientId, e);
                    return Mono.just(true);
                })
                // A Redis that returns nothing at all is treated the same way.
                .defaultIfEmpty(true);
    }
}
