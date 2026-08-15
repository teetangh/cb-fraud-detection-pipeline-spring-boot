package com.fraud.gateway.infra;

import com.fraud.gateway.domain.FraudDecisionResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * The bounded, event-driven wait — the single most important behaviour in the
 * system (spec §5, §6, ADR-0003, ADR-0004).
 *
 * <p>A suspended {@code Mono} holds <b>no thread</b>. The event loop is free
 * between the subscribe and the wakeup, which is why concurrency here is bounded
 * by real work rather than by thread-pool size.
 */
@Component
public class DecisionWaiter {

    private static final Logger log = LoggerFactory.getLogger(DecisionWaiter.class);

    private final ReactiveRedisMessageListenerContainer listenerContainer;
    private final ObjectMapper objectMapper;
    private final Duration timeout;
    private final Counter pipelineCounter;
    private final Counter timeoutCounter;

    public DecisionWaiter(ReactiveRedisMessageListenerContainer listenerContainer,
                          ObjectMapper objectMapper,
                          MeterRegistry meterRegistry,
                          @Value("${fraud.decision.timeout-ms:150}") long timeoutMs) {
        this.listenerContainer = listenerContainer;
        this.objectMapper = objectMapper;
        this.timeout = Duration.ofMillis(timeoutMs);
        // TIMEOUT_DEFAULT / total is the system's primary SLO: "what fraction of
        // our decisions were actually decisions?" A safe default that fires
        // quietly is nearly as bad as the wrong default.
        this.pipelineCounter = meterRegistry.counter("fraud.gateway.decision.resolved", "source", "PIPELINE");
        this.timeoutCounter = meterRegistry.counter("fraud.gateway.decision.resolved", "source", "TIMEOUT_DEFAULT");
    }

    /**
     * Subscribes to {@code decision:{correlationId}}, and only once the
     * subscription is <b>confirmed active</b> runs {@code afterSubscribed} —
     * which is the call to ingestion.
     *
     * <p><b>The ordering here is the whole design, and getting it backwards is
     * the defect the spec's own prose contains.</b> Redis Pub/Sub has no
     * persistence and no replay: a message published to a channel with zero
     * subscribers is simply discarded. On a warm stack the pipeline completes in
     * under 40ms — faster than this service could finish an HTTP round trip to
     * ingestion and then subscribe. Subscribe-after-ingest therefore loses a race
     * it only loses when the pipeline is <b>healthy and fast</b>: every request
     * would burn the full 150ms and return TIMEOUT_DEFAULT, and the faster the
     * pipeline got, the worse it would look.
     *
     * <p>{@code receiveLater()} rather than {@code receive()} is what makes this
     * expressible: {@code receive()} subscribes lazily with no signal for when
     * the subscription is live, while {@code receiveLater()} returns a
     * {@code Mono} that completes once it is.
     */
    public Mono<FraudDecisionResponse> awaitDecision(String transactionId,
                                                     String correlationId,
                                                     Supplier<Mono<Void>> afterSubscribed,
                                                     long startNanos) {
        ChannelTopic topic = ChannelTopic.of("decision:" + correlationId);

        return listenerContainer.receiveLater(topic)
                // The lambda body runs when receiveLater's Mono EMITS — i.e. once
                // the Redis subscription is confirmed live. Only then is it safe
                // to trigger ingestion.
                //
                // NOT doFirst(...): that runs at subscription-assembly time,
                // BEFORE receiveLater's Mono completes, so ingestion would fire
                // against a channel nobody is listening on yet. That was the
                // first implementation here and the test below caught it —
                // the decision was published, discarded, and the wait timed out.
                .flatMapMany(messages -> {
                    afterSubscribed.get().subscribe(
                            ignored -> { },
                            error -> log.error("Ingestion call failed correlationId={} — the wait "
                                               + "will time out to REVIEW", correlationId, error));
                    return messages;
                })
                .next()
                .map(message -> parse(message.getMessage(), transactionId, correlationId, startNanos))
                .timeout(timeout)
                .doOnNext(r -> pipelineCounter.increment())
                .onErrorResume(TimeoutException.class,
                        e -> Mono.fromSupplier(() -> timeoutDefault(transactionId, correlationId, startNanos)))
                .onErrorResume(e -> {
                    // Redis unreachable, malformed payload, anything else: still
                    // answer, still REVIEW. Never ALLOW (§9.8).
                    log.error("Decision wait failed for correlationId={} — defaulting to REVIEW",
                              correlationId, e);
                    return Mono.fromSupplier(() -> timeoutDefault(transactionId, correlationId, startNanos));
                });
        // .next() cancels the upstream subscription after the first element, and
        // the error paths cancel too — so the container does not leak a channel
        // per timed-out request. Redis would otherwise accumulate dead
        // subscriptions until it stopped accepting new ones.
    }

    private FraudDecisionResponse timeoutDefault(String transactionId, String correlationId,
                                                 long startNanos) {
        timeoutCounter.increment();
        long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
        // WARN, not ERROR: this is a designed path, not a failure. But it must be
        // visible — a default that fires silently is nearly as dangerous as the
        // wrong default.
        log.warn("Decision timeout after {}ms — returning REVIEW (never ALLOW) transactionId={} "
                 + "correlationId={}. The pipeline continues; the real decision will still be "
                 + "written and reconciled by webhook if it differs.",
                 latencyMs, transactionId, correlationId);
        return new FraudDecisionResponse(transactionId, correlationId,
                FraudDecisionResponse.Decision.REVIEW, 0, List.of(), "unresolved",
                FraudDecisionResponse.DecisionSource.TIMEOUT_DEFAULT, latencyMs);
    }

    private FraudDecisionResponse parse(String payload, String transactionId,
                                        String correlationId, long startNanos) {
        JsonNode node = objectMapper.readTree(payload);
        List<FraudDecisionResponse.TriggeredRule> rules = new ArrayList<>();
        JsonNode triggered = node.get("triggeredRules");
        if (triggered != null && triggered.isArray()) {
            triggered.forEach(t -> rules.add(new FraudDecisionResponse.TriggeredRule(
                    t.path("ruleId").asString(null),
                    t.path("contribution").asInt(0),
                    t.get("actualValue"),
                    t.get("threshold"))));
        }
        long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
        return new FraudDecisionResponse(
                transactionId, correlationId,
                FraudDecisionResponse.Decision.valueOf(node.path("decision").asString("REVIEW")),
                node.path("riskScore").asInt(0),
                rules,
                node.path("policyVersion").asString("unknown"),
                FraudDecisionResponse.DecisionSource.PIPELINE,
                latencyMs);
    }
}
