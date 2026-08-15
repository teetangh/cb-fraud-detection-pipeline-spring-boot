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

import java.time.Duration;
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

    /**
     * How long a publish claim is honoured before another publisher may take the
     * row (issue #12).
     *
     * <p>Comfortably above {@link #ACK_TIMEOUT_SECONDS}. If it were below, a
     * publish that is merely slow — not dead — would be reclaimed underneath
     * itself and republished, manufacturing the duplicate the claim exists to
     * prevent. The cost of erring high is only that a genuinely crashed
     * publisher's rows wait this long before retry; the cost of erring low is a
     * correctness bug.
     */
    private static final Duration CLAIM_STALE_AFTER = Duration.ofSeconds(30);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final boolean enabled;

    private final Counter publishedCounter;
    private final Counter failedCounter;
    private final Counter staleSkipCounter;
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
        // Rows the GSI still reported as PENDING that the nudge had already
        // published. Expected to be non-zero in normal operation — it is the
        // index lag being absorbed, not an error.
        this.staleSkipCounter = Counter.builder("fraud.ingestion.outbox.stale_skipped")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("fraud.ingestion.outbox.publish.failed")
                .register(meterRegistry);
        this.publishLatency = Timer.builder("fraud.ingestion.outbox.publish.latency")
                // Measures publish() -> broker ack, NOT commit -> ack. The distinction
                // mattered: while the nudge still went through findPending, this
                // metric read a healthy ~15ms and completely hid a ~195ms GSI
                // index-lag gap between commit and pickup. Named for what it
                // actually measures.
                .description("OutboxPublisher.publish() invocation to Kafka broker ack")
                .register(meterRegistry);

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
     *
     * <p>NOTE: a second, narrower window remains even single-instance: if
     * {@code publish()}'s ack {@code .get(ACK_TIMEOUT_SECONDS, ...)} times out
     * while the broker actually accepted the send, or {@code markPublished}
     * itself fails after a successful ack, the row stays PENDING and the next
     * tick republishes — a genuine duplicate on the topic, not just delay. This
     * is the same general class of at-least-once duplicate ADR-0005 already
     * accepts ("a crash after the Kafka ack but before the PUBLISHED update
     * re-publishes on restart... duplicates are absorbed... by consumers keyed
     * on transactionId"), not the systematic every-time race this guard closes
     * above. Fully closing it needs the same CAS-claim mechanism as the
     * cross-instance case, plus a stable dedup key consumed by
     * enrichment-service (not yet built — Phase 3) before the velocity counter
     * increments. Tracked in
     * <a href="https://github.com/teetangh/cb-fraud-detection-pipeline-spring-boot/issues/12">issue #12</a>.
     */
    private final java.util.concurrent.atomic.AtomicBoolean polling =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    /**
     * Set when a nudge arrives while a poll is already running, so that poll
     * rescans before releasing instead of the new row waiting a full tick.
     *
     * <p>This is not a micro-optimisation. Without it, measured end-to-end latency
     * clustered hard at 199-209ms — the poll interval — because a nudge that
     * collided with a running poll was simply dropped, and the row it was
     * announcing waited for the next tick. 200ms is more than the entire 150ms
     * decision budget, so effectively every colliding transaction timed out.
     *
     * <p>The earlier code discarded the nudge with the comment "a poll is already
     * in flight; it will pick these rows up". <b>That was false</b>: the running
     * poll had already executed its {@code findPending} query before this row
     * committed, so it could not see it. A comment asserting a guarantee the code
     * does not provide is worse than no comment.
     */
    private final java.util.concurrent.atomic.AtomicBoolean rescanRequested =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:200}")
    public void publishPending() {
        if (!enabled) {
            return;
        }
        if (!polling.compareAndSet(false, true)) {
            // A poll is in flight and has ALREADY queried. Ask it to look again
            // before it finishes, rather than losing this row to the next tick.
            rescanRequested.set(true);
            return;
        }
        try {
            do {
                rescanRequested.set(false);
                drainOnce();
                // Loop if a nudge arrived while we were draining. Checked AFTER
                // the drain and cleared BEFORE it, so a nudge that lands
                // mid-drain is never lost.
            } while (rescanRequested.get());
        } finally {
            polling.set(false);
        }
    }

    private void drainOnce() {
        try {
            List<OutboxEvent> pending = outboxRepository.findPending(BATCH_SIZE);
            for (OutboxEvent event : pending) {
                // CLAIM before publishing, never merely check.
                //
                // findPending reads a GSI that lags ~195ms, so a row the nudge has
                // already published still shows up here as PENDING. A re-read
                // would confirm that truthfully and publish it anyway — both
                // parties observe PENDING because neither has acked yet. The
                // claim is atomic, so exactly one of them proceeds (issue #12).
                if (!outboxRepository.claim(event.outboxId(), CLAIM_STALE_AFTER)) {
                    staleSkipCounter.increment();
                    continue;
                }
                publish(event);
            }
        } catch (Exception e) {
            // Never let a poll failure kill the scheduler thread — the next tick
            // must still run, or the pipeline stalls silently.
            log.error("Outbox poll failed; will retry on next tick", e);
        }
    }

    /**
     * The backlog gauge, sampled on its OWN slow schedule — deliberately not on
     * the drain path.
     *
     * <p>It cannot be {@code pending.size()}: {@code findPending} is capped at
     * {@code BATCH_SIZE}, so the gauge would saturate at 100 and could never show
     * the "monotonic climb" ADR-0005 names as the earliest signal that Kafka is
     * unreachable. That was a real defect, caught in review on PR #11.
     *
     * <p>But the first fix put {@code countPending()} inside the drain, which
     * <b>doubled the N1QL query count on the highest-QPS path in the system</b> —
     * two queries per poll instead of one, on the critical path of every
     * transaction. A monitoring read has no business competing with the work it
     * monitors.
     *
     * <p>5s resolution is ample: this gauge answers "is the backlog growing", a
     * question about trend, not about any individual transaction.
     */
    @Scheduled(fixedDelayString = "${outbox.gauge-interval-ms:5000}")
    public void sampleBacklogGauge() {
        if (!enabled) {
            return;
        }
        try {
            pendingGauge.set(outboxRepository.countPending());
        } catch (Exception e) {
            log.debug("Outbox backlog gauge sample failed", e);
        }
    }

    /**
     * Post-commit nudge: latency optimisation only. The timer is the guarantee.
     *
     * <p>The executor MUST be named explicitly. Both {@code outboxNudgeExecutor}
     * (from {@link com.fraud.ingestion.config.AsyncConfig}) and Spring's
     * auto-configured {@code taskScheduler} (present because
     * {@code @EnableScheduling} is on, for {@link #publishPending()}) implement
     * {@code TaskExecutor}, so an unqualified {@code @Async} is ambiguous.
     * Spring resolves that ambiguity by logging a WARN and silently falling back
     * to an unbounded {@code SimpleAsyncTaskExecutor} — one new thread per
     * invocation, no queue, no {@code CallerRunsPolicy} — which defeats the
     * bounded pool {@code AsyncConfig} exists to provide.
     */
    @EventListener
    @Async("outboxNudgeExecutor")
    public void onCommitted(TransactionIngestor.OutboxRecordCommitted committed) {
        if (!enabled) {
            return;
        }
        // Publish the event we were handed. No findPending, so no waiting on the
        // GSI to index a row committed microseconds ago.
        //
        // Still goes through the claim: being the fast path is not the same as
        // being the only path. The scheduled poll may already have picked this
        // row up, and whichever arrives second must drop it (issue #12).
        OutboxEvent event = committed.event();
        if (!outboxRepository.claim(event.outboxId(), CLAIM_STALE_AFTER)) {
            staleSkipCounter.increment();
            return;
        }
        publish(event);
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
            if (e instanceof InterruptedException) {
                // Restore the flag we just swallowed by catching it — otherwise
                // the scheduler/async thread loses its interrupt status and a
                // shutdown signal delivered here is silently dropped.
                Thread.currentThread().interrupt();
            }
            failedCounter.increment();
            sample.stop(publishLatency);
            // Left PENDING on purpose — the next tick retries it. Nothing is lost.
            //
            // Drop the claim too, or the retry waits out CLAIM_STALE_AFTER for no
            // reason: we know this publisher is alive and has given up, which is
            // exactly the case the staleness timeout is a poor substitute for.
            outboxRepository.releaseClaim(event.outboxId());
            log.error("Failed to publish outboxId={} transactionId={}; row stays PENDING and "
                      + "will be retried", event.outboxId(), event.payload().transactionId(), e);
        }
    }
}
