package com.fraud.ingestion.infra;

import com.fraud.ingestion.domain.OutboxEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Drains PENDING outbox rows to Kafka (§9.2, ADR-0005).
 *
 * <p>The ordering here is the entire guarantee: publish, wait for the broker
 * acknowledgement, and only then mark PUBLISHED. A crash anywhere before the
 * mark re-publishes on restart — at-least-once, which is correct, because
 * consumers are idempotent on {@code transactionId}. A crash after a premature
 * mark would lose the message permanently.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH_SIZE = 100;
    private static final long ACK_TIMEOUT_SECONDS = 5;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final boolean enabled;

    private final Counter publishedCounter;
    private final Counter failedCounter;
    private final Timer publishLatency;
    private final AtomicLong pendingGauge = new AtomicLong();

    public OutboxPublisher(OutboxRepository outboxRepository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper,
                           MeterRegistry meterRegistry,
                           /*
                            * Test hook for T4. Setting this false freezes the
                            * publisher AFTER the Couchbase commit and BEFORE the
                            * Kafka publish, which is the dangerous window the
                            * outbox exists to survive. It lives in main code
                            * because the alternative — killing containers
                            * mid-test — is slow and flaky.
                            *
                            * MUST be true in any real run.
                            */
                           @Value("${outbox.publisher.enabled:true}") boolean enabled) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.enabled = enabled;

        this.publishedCounter = Counter.builder("fraud.ingestion.outbox.published")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("fraud.ingestion.outbox.publish.failed")
                .register(meterRegistry);
        this.publishLatency = Timer.builder("fraud.ingestion.outbox.publish.latency")
                .description("Couchbase commit to Kafka ack").register(meterRegistry);

        // The earliest available signal that Kafka is unreachable or the
        // publisher is dead — well ahead of any consumer-lag alarm.
        meterRegistry.gauge("fraud.ingestion.outbox.pending", pendingGauge);

        if (!enabled) {
            log.warn("OutboxPublisher is DISABLED (outbox.publisher.enabled=false). "
                     + "Transactions will commit but NOT be published. This is a test hook — "
                     + "if you are seeing this outside a test, the pipeline is silently stalled.");
        }
    }

    /**
     * Non-reentrant: only one poll runs at a time.
     *
     * <p>This guard is load-bearing. Two callers invoke this method — the
     * scheduled timer and the post-commit nudge — and without it they race: both
     * read the same PENDING row before either has marked it PUBLISHED, and the
     * transaction is published to Kafka TWICE.
     *
     * <p>That is not a benign at-least-once duplicate. Enrichment's velocity
     * counter is the one piece of state that is not idempotent under redelivery,
     * so a systematic double-publish would double every customer's velocity
     * count — inflating a signal that gets people's payments blocked. T3 caught
     * exactly this.
     *
     * <p>Skipping (rather than queueing) is correct: the nudge is only a latency
     * optimisation, and the timer is the correctness backstop, so a dropped
     * nudge costs at most one poll interval.
     *
     * <p>NOTE: this makes publishing safe within one process. Multiple ingestion
     * instances polling concurrently could still double-publish; closing that
     * needs a CAS claim on the outbox row before publishing. Not done here
     * because ingestion runs single-instance in this build — recorded so the
     * limit is a known one rather than a surprise.
     */
    private final java.util.concurrent.atomic.AtomicBoolean polling =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:200}")
    public void publishPending() {
        if (!enabled) {
            return;
        }
        if (!polling.compareAndSet(false, true)) {
            return;   // a poll is already in flight; it will pick these rows up
        }
        try {
            List<OutboxEvent> pending = outboxRepository.findPending(BATCH_SIZE);
            pendingGauge.set(pending.size());
            for (OutboxEvent event : pending) {
                publish(event);
            }
        } catch (Exception e) {
            // Never let a poll failure kill the scheduler thread — the next tick
            // must still run, or the pipeline stalls silently.
            log.error("Outbox poll failed; will retry on next tick", e);
        } finally {
            polling.set(false);
        }
    }

    /** Post-commit nudge: latency optimisation only. The timer is the guarantee. */
    @EventListener
    @Async
    public void onCommitted(TransactionIngestor.OutboxRecordCommitted event) {
        publishPending();
    }

    private void publish(OutboxEvent event) {
        Timer.Sample sample = Timer.start();
        try {
            String payload = objectMapper.writeValueAsString(event.payload());

            // Key by customerId — load-bearing, not incidental. It puts all of one
            // customer's transactions on one partition, consumed in order by one
            // instance, which is what stops two consumers racing on that
            // customer's velocity counter (ADR-0012).
            SendResult<String, String> ack = kafkaTemplate
                    .send(event.targetTopic(), event.payload().customerId(), payload)
                    .get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // ONLY after the broker has acknowledged.
            outboxRepository.markPublished(event.outboxId(), Instant.now());

            publishedCounter.increment();
            sample.stop(publishLatency);
            log.debug("Published outboxId={} transactionId={} partition={} offset={}",
                      event.outboxId(), event.payload().transactionId(),
                      ack.getRecordMetadata().partition(), ack.getRecordMetadata().offset());

        } catch (Exception e) {
            failedCounter.increment();
            // Left PENDING on purpose — the next tick retries it. Nothing is lost.
            log.error("Failed to publish outboxId={} transactionId={}; row stays PENDING and "
                      + "will be retried", event.outboxId(), event.payload().transactionId(), e);
        }
    }
}
