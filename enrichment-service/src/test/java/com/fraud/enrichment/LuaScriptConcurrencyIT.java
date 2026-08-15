package com.fraud.enrichment;

import com.fraud.enrichment.infra.RedisSignalSource;
import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 exit criterion (spec §12): hammer the Lua scripts with concurrent
 * increments and assert the final count is <b>exactly</b> correct.
 *
 * <p>Real Redis. The atomicity being proven is a property of the server
 * executing the script, so a fake reproduces nothing.
 *
 * <p>Every assertion here is exact — "exactly 5000", "TTL &gt; 0", "fired once".
 * Approximate assertions ("at least", "roughly") would pass on the racy
 * implementation these scripts exist to replace, which is the failure mode
 * described in ADR-0007.
 */
@DisplayName("Lua signal scripts — atomicity under concurrency")
class LuaScriptConcurrencyIT {

    private static final RedisContainer REDIS =
            new RedisContainer(DockerImageName.parse("redis:7.4.10-alpine"));

    private static LettuceConnectionFactory connectionFactory;
    private static RedisSignalSource signals;
    private static StringRedisTemplate redis;

    @BeforeAll
    static void start() {
        REDIS.start();
        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        redis = new StringRedisTemplate(connectionFactory);
        signals = new RedisSignalSource(redis, new BigDecimal("10000"));
    }

    @AfterAll
    static void stop() {
        if (connectionFactory != null) connectionFactory.destroy();
        REDIS.stop();
    }

    @Test
    @DisplayName("velocity counter loses NO increments under 50 concurrent writers")
    void velocityCounterIsExactUnderConcurrency() throws Exception {
        String customerId = "cust-" + UUID.randomUUID();
        int threads = 50;
        int perThread = 100;

        AtomicLong maxObserved = new AtomicLong();
        runConcurrently(threads, perThread, () -> {
            long v = signals.velocity1m(customerId);
            maxObserved.accumulateAndGet(v, Math::max);
        });

        long finalCount = redis.opsForValue().get("velocity:1m:" + customerId) == null
                ? -1
                : Long.parseLong(redis.opsForValue().get("velocity:1m:" + customerId));

        // EXACTLY. Not "at least", not "approximately". A lost update here means
        // a customer's velocity is understated and the rule that should have
        // fired does not.
        assertThat(finalCount)
                .as("%d threads x %d increments must produce exactly %d — any shortfall is a "
                    + "lost update, i.e. velocity silently under-counted",
                    threads, perThread, threads * perThread)
                .isEqualTo((long) threads * perThread);

        // Each caller saw a distinct, monotonically increasing value: the script
        // returns the post-increment count, so the highest observed must equal
        // the total. If two callers saw the same number, one increment was lost.
        assertThat(maxObserved.get()).isEqualTo((long) threads * perThread);
    }

    @Test
    @DisplayName("the velocity key ALWAYS ends up with a TTL — never immortal")
    void velocityKeyAlwaysGetsTtl() throws Exception {
        String customerId = "cust-" + UUID.randomUUID();

        // Concurrent creation is exactly the window where a separate INCR then
        // EXPIRE can leave the key with no TTL at all.
        runConcurrently(20, 25, () -> signals.velocity1m(customerId));

        Long ttl = redis.getExpire("velocity:1m:" + customerId);
        assertThat(ttl)
                .as("TTL -1 means the key is IMMORTAL: it accumulates every future transaction "
                    + "from this customer, so VELOCITY_1M fires on every payment they ever make "
                    + "and nothing in the logs explains it (ADR-0007)")
                .isNotNull()
                .isGreaterThan(0L)
                .isLessThanOrEqualTo(60L);
    }

    @Test
    @DisplayName("set cardinality is exact and the set gets a TTL")
    void setAddCountIsExactUnderConcurrency() throws Exception {
        String customerId = "cust-" + UUID.randomUUID();
        int distinctCountries = 40;

        ExecutorService pool = Executors.newFixedThreadPool(20);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(distinctCountries);
        AtomicInteger errors = new AtomicInteger();

        for (int i = 0; i < distinctCountries; i++) {
            // Two-letter codes AA..BN, all distinct.
            String country = String.valueOf((char) ('A' + i / 26)) + (char) ('A' + i % 26);
            pool.submit(() -> {
                try {
                    release.await();
                    signals.distinctCountries24h(customerId, country);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }
        release.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();
        assertThat(errors.get()).isZero();

        Long cardinality = redis.opsForSet().size("geo:countries:" + customerId);
        assertThat(cardinality)
                .as("every distinct country must be counted exactly once")
                .isEqualTo((long) distinctCountries);

        assertThat(redis.getExpire("geo:countries:" + customerId))
                .as("an immortal geo set would count every country the customer has EVER "
                    + "transacted from, so GEO_ANOMALY (threshold 1) would fire forever on "
                    + "anyone who has travelled")
                .isGreaterThan(0L);
    }

    @Test
    @DisplayName("a genuinely new device reports new EXACTLY ONCE across concurrent callers")
    void knownDeviceFiresOnceUnderConcurrency() throws Exception {
        String customerId = "cust-" + UUID.randomUUID();
        String deviceId = "dev-" + UUID.randomUUID();
        BigDecimal highAmount = new BigDecimal("25000");

        AtomicInteger reportedNew = new AtomicInteger();
        runConcurrently(30, 1, () -> {
            if (signals.isNewDeviceHighAmount(customerId, deviceId, highAmount)) {
                reportedNew.incrementAndGet();
            }
        });

        // THE assertion. Split into SISMEMBER + SADD, concurrent callers would
        // all read "not a member" before any of them wrote, and
        // NEW_DEVICE_HIGH_AMOUNT (weight 25) would fire multiple times for one
        // genuinely-new device — inflating the score of a transaction that
        // should have counted it once.
        assertThat(reportedNew.get())
                .as("exactly one caller may see this device as new; %d concurrent callers", 30)
                .isEqualTo(1);
    }

    @Test
    @DisplayName("high-amount gate: a new device below the threshold does not fire")
    void newDeviceBelowThresholdDoesNotFire() {
        String customerId = "cust-" + UUID.randomUUID();
        // The rule is new-device AND high-amount. A new device on a small
        // purchase is ordinary behaviour and must not contribute 25 points.
        assertThat(signals.isNewDeviceHighAmount(customerId, "dev-cheap", new BigDecimal("50")))
                .isFalse();
    }

    private static void runConcurrently(int threads, int perThread, Runnable task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    release.await();
                    for (int i = 0; i < perThread; i++) {
                        task.run();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }
        release.countDown();   // release them all at once — that is the point
        assertThat(done.await(120, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();
        assertThat(errors.get()).as("no worker may have thrown").isZero();
    }
}
