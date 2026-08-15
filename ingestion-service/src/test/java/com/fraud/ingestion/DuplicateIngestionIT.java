package com.fraud.ingestion;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fraud.ingestion.domain.IngestionResult;
import com.fraud.ingestion.domain.PaymentMethod;
import com.fraud.ingestion.domain.TransactionRecord;
import com.fraud.ingestion.infra.TransactionIngestor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * T3 — duplicate request idempotency (spec §10, ADR-0006).
 *
 * <p>The two requests are driven CONCURRENTLY from two threads, released together
 * by a latch. That is the whole point: a sequential version of this test passes
 * with only the Redis check in place and proves nothing. It is the concurrent
 * case that exercises the window where both requests miss the cache and both
 * proceed — which only Couchbase {@code insert()} closes.
 */
@SpringBootTest(properties = "outbox.publisher.enabled=true")
@DisplayName("T3 — duplicate request idempotency")
// Spring caches contexts across test classes, keyed by config — so this
// context (publisher ENABLED) and OutboxDurabilityIT's (publisher DISABLED)
// are cached side by side, both alive, both pointed at the same static
// Kafka/Redis/Couchbase containers. Left alone, this context's live
// @Scheduled poller could drain a row OutboxDurabilityIT expects to observe
// as PENDING. AFTER_CLASS closes this context (and its poller) once this
// class's tests finish, before OutboxDurabilityIT's disabled-publisher
// context is ever created.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DuplicateIngestionIT extends AbstractIngestionIT {

    @Autowired TransactionIngestor ingestor;
    @Autowired Cluster cluster;
    @Autowired @Qualifier("rawTransactionsCollection") Collection rawTransactions;

    @Test
    @DisplayName("two concurrent identical transactionIds produce exactly one record and one event")
    void concurrentDuplicatesResolveToOne() throws Exception {
        String transactionId = "txn-" + UUID.randomUUID();
        String customerId = "cust-dup-" + UUID.randomUUID();
        TransactionRecord txn = sampleTransaction(transactionId, customerId);

        // ── fire both requests genuinely simultaneously ──────────────────────
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch release = new CountDownLatch(1);

        Callable<IngestionResult> attempt = () -> {
            release.await();
            return ingestor.ingest(txn);
        };
        Future<IngestionResult> first = pool.submit(attempt);
        Future<IngestionResult> second = pool.submit(attempt);

        release.countDown();
        IngestionResult resultA = first.get(30, TimeUnit.SECONDS);
        IngestionResult resultB = second.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        // ── both callers get the SAME decision-relevant answer ───────────────
        assertThat(resultA.transactionId()).isEqualTo(transactionId);
        assertThat(resultB.transactionId()).isEqualTo(transactionId);
        assertThat(resultA.correlationId())
                .as("both callers must receive the same correlationId, or they act on "
                    + "different identities for one transaction")
                .isEqualTo(resultB.correlationId());

        // Exactly one of them was the winner.
        List<IngestionResult.Status> statuses = List.of(resultA.status(), resultB.status());
        assertThat(statuses).containsExactlyInAnyOrder(
                IngestionResult.Status.ACCEPTED, IngestionResult.Status.DUPLICATE);

        // ── exactly ONE document in Couchbase ────────────────────────────────
        // A KV read, not a N1QL COUNT. The key is deterministic (txn::{id}) and
        // KV is immediately consistent, whereas a COUNT depends on GSI catch-up
        // and reports 0 for a document that is definitely committed. That is not
        // hypothetical — it failed here once the outbox nudge got fast enough to
        // beat the index, which is exactly the ~195ms GSI lag that turned out to
        // dominate end-to-end latency.
        //
        // "Exactly one" is guaranteed by the key itself; what insert()-vs-upsert()
        // actually decides is whether the ORIGINAL survived, which is asserted
        // below via correlationId, and whether a SECOND outbox row was written,
        // which is asserted next.
        assertThat(rawTransactions.exists("txn::" + transactionId).exists())
                .as("the transaction must be durably committed exactly once")
                .isTrue();
        assertThat(rawTransactions.get("txn::" + transactionId).contentAsObject()
                        .getString("correlationId"))
                .as("upsert() would have OVERWRITTEN the original with the retry's "
                    + "correlationId; insert() preserves the first writer's")
                .isEqualTo(resultA.correlationId());

        // ── exactly ONE outbox row ───────────────────────────────────────────
        // This one genuinely needs N1QL: the outbox key is a fresh UUID, so it
        // can only be found by querying on payload.transactionId. Wrapped in
        // Awaitility because even REQUEST_PLUS is only consistent as of the
        // moment the query is issued.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            long outboxCount = cluster.query("""
                            SELECT RAW COUNT(*)
                            FROM `%s`.`transactions`.`outbox` o
                            WHERE o.payload.transactionId = $txnId
                            """.formatted(BUCKET),
                            QueryOptions.queryOptions()
                                    .parameters(com.couchbase.client.java.json.JsonObject.create()
                                            .put("txnId", transactionId))
                                    .scanConsistency(QueryScanConsistency.REQUEST_PLUS))
                    .rowsAs(Long.class).getFirst();
            assertThat(outboxCount)
                    .as("a second outbox row means a second pipeline event, which "
                        + "double-increments the velocity counter")
                    .isEqualTo(1L);
        });

        // ── exactly ONE Kafka event ──────────────────────────────────────────
        // This is the assertion that catches the real damage of upsert(): a second
        // outbox row means a second pipeline event, which double-increments the
        // velocity counter and can get a legitimately retrying customer blocked.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<ConsumerRecord<String, String>> matching = drainMatching(RAW_TOPIC, transactionId);
            assertThat(matching)
                    .as("exactly one event on %s for transactionId=%s", RAW_TOPIC, transactionId)
                    .hasSize(1);
            assertThat(matching.getFirst().key())
                    .as("keyed by customerId so one customer's events stay on one partition "
                        + "and in order (ADR-0012)")
                    .isEqualTo(customerId);
        });
    }


    static TransactionRecord sampleTransaction(String transactionId, String customerId) {
        return new TransactionRecord(
                transactionId, customerId, "merch-1", "5411",
                new BigDecimal("149.50"), "INR", "IN",
                "dev-1", "203.0.113.44", PaymentMethod.CARD,
                UUID.randomUUID().toString(), Instant.now());
    }
}
