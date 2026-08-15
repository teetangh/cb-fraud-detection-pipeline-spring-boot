package com.fraud.ingestion;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fraud.ingestion.domain.TransactionRecord;
import com.fraud.ingestion.infra.OutboxPublisher;
import com.fraud.ingestion.infra.OutboxRepository;
import com.fraud.ingestion.infra.TransactionIngestor;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * T4 — outbox durability under a simulated crash (spec §10, ADR-0005).
 *
 * <p>The context runs with {@code outbox.publisher.enabled=false}, which freezes
 * the publisher in exactly the dangerous window: AFTER the Couchbase commit,
 * BEFORE the Kafka publish. This is where a naive dual-write silently loses a
 * transaction forever — durably recorded, never scored, no error raised.
 *
 * <p>Proves the property that matters: <em>no transaction is ever silently
 * dropped, only delayed.</em>
 */
@SpringBootTest(properties = "outbox.publisher.enabled=false")
@DisplayName("T4 — outbox durability under simulated crash")
class OutboxDurabilityIT extends AbstractIngestionIT {

    @Autowired TransactionIngestor ingestor;
    @Autowired OutboxRepository outboxRepository;
    @Autowired Cluster cluster;
    @Autowired @Qualifier("rawTransactionsCollection") Collection rawTransactions;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper objectMapper;
    @Autowired MeterRegistry meterRegistry;

    @Test
    @DisplayName("a crash between commit and publish delays the transaction, never loses it")
    void pendingOutboxSurvivesAndRepublishes() {
        String transactionId = "txn-" + UUID.randomUUID();
        String customerId = "cust-outbox-" + UUID.randomUUID();
        TransactionRecord txn = DuplicateIngestionIT.sampleTransaction(transactionId, customerId);

        ingestor.ingest(txn);

        // ── the transaction IS durable ───────────────────────────────────────
        // A KV get, NOT a N1QL count. The key is deterministic (txn::{id}), and
        // KV reads are immediately consistent because no index is involved. A
        // N1QL COUNT here depends on the GSI having caught up, so it can report
        // 0 for a document that is definitely committed — which is exactly how
        // this assertion failed first time round, sending me looking for a
        // durability bug that did not exist.
        //
        // Rule of thumb: key lookup -> KV. Search -> N1QL.
        assertThat(rawTransactions.exists(txn.documentKey()).exists())
                .as("the transaction must be committed even though it was never published")
                .isTrue();

        // ── the outbox row is PENDING ────────────────────────────────────────
        // This one genuinely needs N1QL: the outbox key is a fresh UUID, so it
        // can only be found by querying on payload.transactionId. Wrapped in
        // Awaitility because even REQUEST_PLUS is only consistent as of the
        // moment the query is issued.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(outboxStatusOf(transactionId)).isEqualTo("PENDING"));

        // ── and NOTHING was published ────────────────────────────────────────
        // This negative assertion carries as much weight as the positive ones:
        // without it, the test would pass on a system that published eagerly and
        // never used the outbox at all.
        assertThat(drainMatching(RAW_TOPIC, transactionId))
                .as("publisher is frozen, so no event may exist yet")
                .isEmpty();

        // ── restart the publisher; the row must drain ────────────────────────
        // A second instance with enabled=true, rather than mutating the frozen
        // bean — the production class keeps no test-only setter.
        OutboxPublisher restarted = new OutboxPublisher(
                outboxRepository, kafkaTemplate, objectMapper, meterRegistry, true);
        restarted.publishPending();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<ConsumerRecord<String, String>> published = drainMatching(RAW_TOPIC, transactionId);
            assertThat(published)
                    .as("restarting the publisher must pick the PENDING row up and publish it")
                    .hasSize(1);
        });

        // ── and is marked PUBLISHED only now ─────────────────────────────────
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(outboxStatusOf(transactionId)).isEqualTo("PUBLISHED"));
    }

    private String outboxStatusOf(String transactionId) {
        return cluster.query("""
                        SELECT RAW o.status FROM `%s`.`transactions`.`outbox` o
                        WHERE o.payload.transactionId = $txnId
                        """.formatted(BUCKET),
                        QueryOptions.queryOptions()
                                .parameters(JsonObject.create().put("txnId", transactionId))
                                // Read-after-write: NOT_BOUNDED would legitimately
                                // return nothing here (LLD §7).
                                .scanConsistency(QueryScanConsistency.REQUEST_PLUS))
                .rowsAs(String.class).getFirst();
    }
}
