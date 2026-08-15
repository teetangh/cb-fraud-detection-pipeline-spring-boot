package com.fraud.gateway;

import com.fraud.gateway.domain.FraudDecisionResponse;
import com.fraud.gateway.infra.DecisionWaiter;
import com.redis.testcontainers.RedisContainer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The sync facade — spec §5/§6, ADR-0003, ADR-0004.
 *
 * <p>Real Redis. The property under test is that a message published to a channel
 * with no subscriber is <b>discarded</b>, which is a real-server behaviour and
 * the entire reason the subscribe/ingest ordering matters.
 */
@DisplayName("Sync facade — the bounded, event-driven wait")
class DecisionWaiterIT {

    private static final RedisContainer REDIS =
            new RedisContainer(DockerImageName.parse("redis:7.4.10-alpine"));

    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveRedisMessageListenerContainer container;
    private static StringRedisTemplate publisher;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    static void start() {
        REDIS.start();
        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        container = new ReactiveRedisMessageListenerContainer(connectionFactory);
        publisher = new StringRedisTemplate(connectionFactory);
    }

    @AfterAll
    static void stop() {
        if (container != null) container.destroy();
        if (connectionFactory != null) connectionFactory.destroy();
        REDIS.stop();
    }

    private DecisionWaiter waiter(long timeoutMs) {
        return new DecisionWaiter(container, MAPPER, new SimpleMeterRegistry(), timeoutMs);
    }

    private static String decisionJson(String decision, int score) {
        return """
               {"transactionId":"txn-1","decision":"%s","riskScore":%d,"policyVersion":"v1",
                "triggeredRules":[{"ruleId":"VELOCITY_1M","contribution":30,
                                   "actualValue":6,"threshold":5}]}
               """.formatted(decision, score);
    }

    // ── THE test ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("SUBSCRIBE-BEFORE-INGEST: a decision published the instant ingestion runs is still caught")
    void decisionPublishedDuringIngestIsCaught() {
        String correlationId = UUID.randomUUID().toString();
        AtomicBoolean ingestRan = new AtomicBoolean();

        // The "ingestion call" publishes the decision IMMEDIATELY — simulating a
        // pipeline that completes faster than the gateway could subscribe if it
        // subscribed afterwards. On a warm stack that is the real case: the
        // pipeline finishes in under 40ms.
        //
        // THIS TEST FAILS IF THE ORDERING IS REVERSED. Redis Pub/Sub has no
        // persistence, so with subscribe-after-ingest the publish lands on a
        // channel with zero subscribers, is discarded, and this times out to
        // TIMEOUT_DEFAULT. That is the whole point of ADR-0003 — and note the
        // failure would get WORSE the faster the pipeline got.
        FraudDecisionResponse response = waiter(2000)
                .awaitDecision("txn-1", correlationId,
                        () -> Mono.fromRunnable(() -> {
                            ingestRan.set(true);
                            publisher.convertAndSend("decision:" + correlationId,
                                                     decisionJson("BLOCK", 85));
                        }),
                        System.nanoTime())
                .block(Duration.ofSeconds(10));

        assertThat(ingestRan).isTrue();
        assertThat(response).isNotNull();
        assertThat(response.resolvedBy())
                .as("the decision must be resolved by the PIPELINE. TIMEOUT_DEFAULT here means "
                    + "the publish landed on a channel with no subscriber — i.e. ingestion ran "
                    + "before the subscription was live (ADR-0003)")
                .isEqualTo(FraudDecisionResponse.DecisionSource.PIPELINE);
        assertThat(response.decision()).isEqualTo(FraudDecisionResponse.Decision.BLOCK);
        assertThat(response.riskScore()).isEqualTo(85);
        assertThat(response.triggeredRules()).singleElement()
                .satisfies(r -> assertThat(r.ruleId()).isEqualTo("VELOCITY_1M"));
        assertThat(response.policyVersion()).isEqualTo("v1");
    }

    // ── §9.8: the default ────────────────────────────────────────────────────

    @Test
    @DisplayName("no publish within the budget → REVIEW, never ALLOW, tagged TIMEOUT_DEFAULT")
    void timesOutToReviewNeverAllow() {
        String correlationId = UUID.randomUUID().toString();
        long start = System.nanoTime();

        FraudDecisionResponse response = waiter(150)
                .awaitDecision("txn-1", correlationId, Mono::empty, start)
                .block(Duration.ofSeconds(10));

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(response).isNotNull();
        assertThat(response.decision())
                .as("§9.8: an unresolved decision must NEVER silently permit a transaction. "
                    + "ALLOW here would turn every GC pause and rebalance into 'let everything "
                    + "through' — and load spikes correlate with fraud waves.")
                .isEqualTo(FraudDecisionResponse.Decision.REVIEW);
        assertThat(response.resolvedBy())
                .isEqualTo(FraudDecisionResponse.DecisionSource.TIMEOUT_DEFAULT);

        // Upper AND lower bound. The upper proves it did not wait for a slow
        // pipeline; the lower proves the timeout is real and not an immediate
        // error path masquerading as one.
        assertThat(elapsedMs)
                .as("must return at ~150ms, not instantly and not after the pipeline")
                .isBetween(120L, 3000L);
    }

    @Test
    @DisplayName("a decision arriving AFTER the timeout does not resurrect the response")
    void lateDecisionDoesNotOverrideTheDefault() throws Exception {
        String correlationId = UUID.randomUUID().toString();

        FraudDecisionResponse response = waiter(150)
                .awaitDecision("txn-1", correlationId, Mono::empty, System.nanoTime())
                .block(Duration.ofSeconds(10));

        assertThat(response.resolvedBy())
                .isEqualTo(FraudDecisionResponse.DecisionSource.TIMEOUT_DEFAULT);

        // The pipeline finishes later, as it always does — the wait was bounded,
        // not the pipeline. This publish must be harmless: the caller already has
        // its answer, and reconciliation (Phase 5) is what corrects it.
        Thread.sleep(200);
        publisher.convertAndSend("decision:" + correlationId, decisionJson("ALLOW", 5));

        assertThat(response.decision()).isEqualTo(FraudDecisionResponse.Decision.REVIEW);
    }

    // ── leak check ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("timed-out waits do not leak Redis subscriptions")
    void timedOutWaitsUnsubscribe() {
        int waits = 40;
        for (int i = 0; i < waits; i++) {
            waiter(60).awaitDecision("txn-" + i, UUID.randomUUID().toString(),
                            Mono::empty, System.nanoTime())
                    .block(Duration.ofSeconds(5));
        }

        // Without teardown on the timeout path the container would hold one
        // channel per timed-out request, and Redis would accumulate dead
        // subscriptions until it stopped accepting new ones.
        Object raw = publisher.execute((org.springframework.data.redis.core.RedisCallback<Object>) conn ->
                conn.execute("PUBSUB", "CHANNELS".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                       "decision:*".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        int channels = raw instanceof java.util.List<?> list ? list.size() : 0;

        assertThat(channels)
                .as("after %d timed-out waits, no decision:* channels may remain subscribed — "
                    + "one leaked channel per timed-out request would eventually stop Redis "
                    + "accepting new subscriptions", waits)
                .isLessThanOrEqualTo(1);
    }
}
