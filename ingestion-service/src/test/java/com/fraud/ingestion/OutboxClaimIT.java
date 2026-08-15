package com.fraud.ingestion;

import com.fraud.ingestion.domain.IngestionResult;
import com.fraud.ingestion.domain.OutboxEvent;
import com.fraud.ingestion.domain.TransactionRecord;
import com.fraud.ingestion.infra.OutboxRepository;
import com.fraud.ingestion.infra.TransactionIngestor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12 — the outbox publish claim is atomic.
 *
 * <p>The publisher has two paths to the same row: the post-commit nudge (fast)
 * and the scheduled poll (the durability guarantee). Before this, the poll
 * re-read the row and published if it still looked PENDING — check-then-act,
 * which narrows the race without closing it. Both paths can truthfully observe
 * PENDING, because neither has acked yet, and both then publish.
 *
 * <p>That surfaced as intermittent T3 failures with two records on the topic for
 * one transaction, distinguishable by how the amount serialised:
 * {@code "amount":149.50} from the nudge's in-memory event versus
 * {@code "amount":149.5} from the row the timer re-read out of Couchbase.
 *
 * <p>These tests go at the claim directly rather than trying to provoke the
 * timing window through the publisher, because a test that reproduces a race
 * only sometimes cannot demonstrate that the race is gone. {@code contendedClaim}
 * runs the exact concurrency the bug needs and asserts a hard invariant.
 *
 * <p>The publisher is DISABLED here so its own polling cannot claim rows out from
 * under the assertions.
 */
@SpringBootTest(properties = "outbox.publisher.enabled=false")
@DisplayName("Issue #12 — outbox claim is atomic under contention")
class OutboxClaimIT extends AbstractIngestionIT {

    private static final Duration STALE_AFTER = Duration.ofSeconds(30);

    @Autowired TransactionIngestor ingestor;
    @Autowired OutboxRepository outboxRepository;

    @Test
    @DisplayName("32 concurrent claimants on one row: exactly one wins")
    void contendedClaimYieldsExactlyOneWinner() throws Exception {
        String outboxId = ingestPendingRow();

        int claimants = 32;
        ExecutorService pool = Executors.newFixedThreadPool(claimants);
        CountDownLatch release = new CountDownLatch(1);

        Callable<Boolean> attempt = () -> {
            release.await();
            return outboxRepository.claim(outboxId, STALE_AFTER);
        };

        List<Future<Boolean>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < claimants; i++) {
            futures.add(pool.submit(attempt));
        }
        release.countDown();               // fire them all at once

        long winners = 0;
        for (Future<Boolean> f : futures) {
            if (f.get(30, TimeUnit.SECONDS)) {
                winners++;
            }
        }
        pool.shutdown();

        assertThat(winners)
                .as("""
                    exactly one claimant may publish. More than one is a real duplicate on \
                    the topic, and enrichment's velocity counter is not idempotent under \
                    redelivery — so a systematic double-publish inflates the very signal \
                    the fraud rules key on. Zero would mean the row can never be published \
                    at all, which is worse.""")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("a claim blocks a second claimant while it is live")
    void liveClaimIsHonoured() throws Exception {
        String outboxId = ingestPendingRow();

        assertThat(outboxRepository.claim(outboxId, STALE_AFTER))
                .as("the first claimant on an unclaimed PENDING row must win")
                .isTrue();

        assertThat(outboxRepository.claim(outboxId, STALE_AFTER))
                .as("a second claimant must be refused while the first claim is live")
                .isFalse();
    }

    @Test
    @DisplayName("an expired claim is reclaimable — a crashed publisher must not strand the row")
    void expiredClaimIsReclaimable() throws Exception {
        String outboxId = ingestPendingRow();

        assertThat(outboxRepository.claim(outboxId, STALE_AFTER)).isTrue();

        // Simulate the claimant dying mid-publish: no release, no markPublished.
        // A zero staleness window is the same question as "has the claim aged
        // out?", asked without making the test sleep for 30 seconds.
        assertThat(outboxRepository.claim(outboxId, Duration.ZERO))
                .as("""
                    a publisher that dies mid-publish must not strand the row forever. \
                    The row stays PENDING and the claim expires, so the next poll picks \
                    it up — this is what removes the need for a separate reaper and a \
                    second index.""")
                .isTrue();
    }

    @Test
    @DisplayName("releasing a claim lets the next tick retry immediately")
    void releasedClaimIsImmediatelyReclaimable() throws Exception {
        String outboxId = ingestPendingRow();

        assertThat(outboxRepository.claim(outboxId, STALE_AFTER)).isTrue();
        outboxRepository.releaseClaim(outboxId);

        assertThat(outboxRepository.claim(outboxId, STALE_AFTER))
                .as("""
                    after a FAILED publish the claim is dropped explicitly, so the retry \
                    happens on the next 200ms tick rather than waiting out the 30s \
                    staleness window. We know the publisher is alive and has given up; \
                    the timeout is a poor substitute for that knowledge.""")
                .isTrue();
    }

    @Test
    @DisplayName("a published row cannot be claimed again")
    void publishedRowIsNotClaimable() throws Exception {
        String outboxId = ingestPendingRow();

        assertThat(outboxRepository.claim(outboxId, STALE_AFTER)).isTrue();
        outboxRepository.markPublished(outboxId, java.time.Instant.now());

        assertThat(outboxRepository.claim(outboxId, Duration.ZERO))
                .as("""
                    status is checked before staleness, so even a long-expired claim on an \
                    already-PUBLISHED row is not republished. Without this ordering the \
                    expiry path would resurrect completed work.""")
                .isFalse();
    }

    /** Ingests a transaction and returns the outboxId of the PENDING row it wrote. */
    private String ingestPendingRow() {
        TransactionRecord txn = DuplicateIngestionIT.sampleTransaction(
                "txn-" + UUID.randomUUID(), "cust-claim-" + UUID.randomUUID());
        IngestionResult result = ingestor.ingest(txn);
        assertThat(result.status()).isEqualTo(IngestionResult.Status.ACCEPTED);

        // findPending reads the GSI with NOT_BOUNDED, which lags ~195ms behind the
        // commit — so poll rather than reading once. Reading once here would make
        // these tests fail for a reason that has nothing to do with the claim.
        return org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> outboxRepository.findPending(100).stream()
                                .filter(e -> e.payload().transactionId()
                                              .equals(txn.transactionId()))
                                .map(OutboxEvent::outboxId)
                                .findFirst()
                                .orElse(null),
                       java.util.Objects::nonNull);
    }
}
