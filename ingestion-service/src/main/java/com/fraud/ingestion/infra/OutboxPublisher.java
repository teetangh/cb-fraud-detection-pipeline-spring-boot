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
     * <p>Comfortably above the worst-case duration of {@link #publish}. If it
     * were below, a publish that is merely slow — not dead — would be reclaimed
     * underneath itself and republished, manufacturing the duplicate the claim
     * exists to prevent. The cost of erring high is only that a genuinely
     * crashed publisher's rows wait this long before retry; the cost of erring
     * low is a correctness bug.
     *
     * <p>"Worst case" is <b>not</b> {@link #ACK_TIMEOUT_SECONDS} on its own, and
     * reading it that way is how this window gets set too low. That timeout
     * bounds the wait on the ack future; it does not bound
     * {@code KafkaProducer.send()}, which blocks in {@code waitOnMetadata}
     * before the future exists. The real bound is the sum, and it holds only
     * because {@code max.block.ms} is pinned to 5s in {@code application.yml}:
     *
     * <pre>
     *   5.0s  max.block.ms          send() blocking on metadata / buffer
     * + 5.0s  ACK_TIMEOUT_SECONDS   waiting on the broker ack
     * + 2.5s  markPublished         Couchbase KV default timeout
     * = 12.5s                       against a 30s claim
     * </pre>
     *
     * <p>Change either constant, or unpin {@code max.block.ms}, and that margin
     * is what has to be rechecked.
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
        // Publish attempts that correctly stood down because another publisher
        // owned the row: it was already PUBLISHED (the GSI-lag case), it carried
        // a live claim, or this caller lost the CAS race. Expected to be non-zero
        // in normal operation — it is contention being absorbed, not an error.
        //
        // Deliberately one counter and not three: all three mean the same thing
        // operationally ("someone else has it, we did not double-publish"). The
        // event that IS worth distinguishing — reclaiming a dead publisher's row
        // — is logged at WARN by OutboxRepository.claim rather than folded in
        // here, because it means a publisher died, not that two of them raced.
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
     * <p><b>This is an efficiency guard, not the correctness mechanism</b> — and
     * the distinction is the whole of issue #12. It keeps two callers of
     * <em>this method</em> from doing the same {@code findPending} + publish work
     * concurrently. It has never constrained the post-commit nudge at all:
     * {@link #onCommitted} calls {@link #publish} directly and does not route
     * through here. So the flag could not have been what stopped the
     * double-publish, which is exactly why T3 kept failing intermittently while
     * this comment claimed the race was closed.
     *
     * <p>What actually guarantees one publisher per row is the CAS claim on the
     * row itself ({@link OutboxRepository#claim}). That holds across threads,
     * across the nudge/poll split, and across ingestion instances — none of
     * which this flag can reach.
     *
     * <p>The duplicate being prevented is not a benign at-least-once one.
     * Enrichment's velocity counter is the one piece of state that is not
     * idempotent under redelivery, so a systematic double-publish would double
     * every customer's velocity count — inflating a signal that gets people's
     * payments blocked.
     *
     * <p>Skipping (rather than queueing) is correct: the nudge is only a latency
     * optimisation, and the timer is the correctness backstop, so a dropped
     * nudge costs at most one poll interval.
     *
     * <p>NOTE: one at-least-once window remains, and is accepted. If
     * {@code publish()}'s ack {@code .get(ACK_TIMEOUT_SECONDS, ...)} times out
     * while the broker actually accepted the send, or {@code markPublished}
     * itself fails after a successful ack, the row stays PENDING, its claim is
     * released, and the next tick republishes — a genuine duplicate on the
     * topic. {@code delivery.timeout.ms} (4s) is pinned below
     * {@code ACK_TIMEOUT_SECONDS} (5s) precisely to narrow it: the producer gives
     * up before the publisher does, so a timed-out send is usually a genuinely
     * failed one. What is left is the ordinary at-least-once duplicate ADR-0005
     * already accepts ("a crash after the Kafka ack but before the PUBLISHED
     * update re-publishes on restart... duplicates are absorbed... by consumers
     * keyed on transactionId") — not the systematic every-time race the claim
     * closes.
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
                boolean claimed;
                try {
                    // CLAIM before publishing, never merely check.
                    //
                    // findPending reads a GSI that lags ~195ms, so a row the nudge
                    // has already published still shows up here as PENDING. A
                    // re-read would confirm that truthfully and publish it anyway
                    // — both parties observe PENDING because neither has acked
                    // yet. The claim is atomic, so exactly one proceeds (#12).
                    claimed = outboxRepository.claim(event.outboxId(), CLAIM_STALE_AFTER);
                } catch (Exception e) {
                    // Isolate the ROW, not the batch.
                    //
                    // claim() is the only call in this loop that can throw —
                    // publish() handles its own failures. findPending is
                    // ORDER BY createdAt ASC, so letting an exception escape here
                    // would abort the drain at the OLDEST failing row on every
                    // single tick, and every row behind it would stop draining.
                    // Permanently, and silently: no counter moves, and the only
                    // symptom is this line repeating. A row that cannot be
                    // claimed is simply retried next tick; a row that cannot be
                    // claimed AND takes the backlog with it is an outage.
                    log.error("Claim failed for outboxId={}; skipping this row, the batch "
                              + "continues", event.outboxId(), e);
                    continue;
                }
                if (!claimed) {
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
