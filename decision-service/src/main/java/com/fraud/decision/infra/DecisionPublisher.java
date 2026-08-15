package com.fraud.decision.infra;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Publishes to {@code decision:{correlationId}} — the message that wakes up the
 * gateway request still waiting on the caller's behalf.
 *
 * <p>Sub-millisecond wakeup, versus the 10ms average a 20ms poll loop would waste
 * on every single request (ADR-0003).
 */
@Component
public class DecisionPublisher {

    private static final Logger log = LoggerFactory.getLogger(DecisionPublisher.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final Counter publishFailed;

    public DecisionPublisher(StringRedisTemplate redis, ObjectMapper objectMapper,
                             MeterRegistry meterRegistry) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.publishFailed = Counter.builder("fraud.decision.publish.failed")
                .description("Pub/Sub publishes that failed — expect matching gateway timeouts")
                .register(meterRegistry);
    }

    /**
     * Fire-and-forget, and <b>a failure here is never fatal and never retried</b>.
     *
     * <p>Redis Pub/Sub has no persistence: if the gateway is not subscribed, the
     * message is discarded. That is acceptable by design, because this channel is
     * only an optimisation — the authoritative decision is already durably in
     * Couchbase before this is called, and the gateway's 150ms timeout plus
     * webhook reconciliation covers every missed message.
     *
     * <p>Retrying would spend the caller's remaining budget on a message that is
     * very likely already too late to matter. A missed publish is never a lost
     * decision.
     */
    public void publish(String correlationId, ObjectNode decision) {
        if (correlationId == null || correlationId.isBlank()) {
            log.warn("No correlationId on the decision — cannot wake any waiting caller");
            publishFailed.increment();
            return;
        }
        try {
            redis.convertAndSend("decision:" + correlationId,
                                 objectMapper.writeValueAsString(decision));
        } catch (Exception e) {
            publishFailed.increment();
            log.warn("Pub/Sub publish failed correlationId={} — the caller will time out to "
                     + "REVIEW and be reconciled by webhook. The decision is already durable.",
                     correlationId, e);
        }
    }
}
