package com.fraud.gateway;

import com.fraud.gateway.infra.RateLimiter;
import com.redis.testcontainers.RedisContainer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real Redis. The Lua script's atomicity and the fail-open behaviour are both
 * properties of the real server, and neither survives being faked.
 */
@DisplayName("Rate limiter — atomic window and fail-open")
class RateLimiterIT {

    private static final RedisContainer REDIS =
            new RedisContainer(DockerImageName.parse("redis:7.4.10-alpine"));

    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redisTemplate;

    @BeforeAll
    static void start() {
        REDIS.start();
        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        redisTemplate = new ReactiveStringRedisTemplate(connectionFactory);
    }

    @AfterAll
    static void stop() {
        if (connectionFactory != null) connectionFactory.destroy();
        REDIS.stop();
    }

    @Test
    @DisplayName("permits up to the limit, then rejects — and the window key gets a TTL")
    void enforcesLimitAndSetsTtl() {
        int limit = 5;
        RateLimiter limiter = new RateLimiter(redisTemplate, new SimpleMeterRegistry(), limit);
        String clientId = "cust-" + UUID.randomUUID();

        for (int i = 1; i <= limit; i++) {
            assertThat(limiter.tryAcquire(clientId).block())
                    .as("request %d of %d must be permitted", i, limit)
                    .isTrue();
        }
        assertThat(limiter.tryAcquire(clientId).block())
                .as("request %d exceeds the limit and must be rejected", limit + 1)
                .isFalse();

        // The TTL is the whole reason the counter is a Lua script. A key with
        // TTL -1 never expires, so the client is rate-limited FOREVER with
        // nothing in the logs to explain it (ADR-0007).
        String key = redisTemplate.keys("ratelimit:" + clientId + ":*").blockFirst();
        assertThat(key).isNotNull();
        Long ttl = redisTemplate.getExpire(key).block().toSeconds();
        assertThat(ttl)
                .as("the window key MUST have a positive TTL, or the client is limited forever")
                .isNotNull()
                .isGreaterThan(0L);
    }

    @Test
    @DisplayName("FAILS OPEN when Redis is unreachable — a cache outage must not stop payments")
    void failsOpenWhenRedisUnavailable() {
        // Point at a port with nothing on it: a connection failure, not a slow
        // response, which is the case a timeout alone would not cover.
        LettuceConnectionFactory dead = new LettuceConnectionFactory("127.0.0.1", 1);
        dead.afterPropertiesSet();
        RateLimiter limiter = new RateLimiter(
                new ReactiveStringRedisTemplate(dead), new SimpleMeterRegistry(), 5);

        try {
            assertThat(limiter.tryAcquire("cust-" + UUID.randomUUID()).block())
                    .as("with Redis down the request must be PERMITTED. Failing closed here "
                        + "would turn a cache outage into a total payment outage (ADR-0014)")
                    .isTrue();
        } finally {
            dead.destroy();
        }
    }
}
