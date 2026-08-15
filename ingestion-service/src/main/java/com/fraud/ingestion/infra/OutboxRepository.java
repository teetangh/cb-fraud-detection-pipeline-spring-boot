package com.fraud.ingestion.infra;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fraud.ingestion.config.CouchbaseProperties;
import com.fraud.ingestion.domain.OutboxEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public class OutboxRepository {

    private final Cluster cluster;
    private final Collection outbox;
    private final String bucket;

    public OutboxRepository(Cluster cluster,
                            @Qualifier("outboxCollection") Collection outbox,
                            CouchbaseProperties props) {
        this.cluster = cluster;
        this.outbox = outbox;
        this.bucket = props.bucket();
    }

    /**
     * Backed by {@code idx_outbox_pending}, a PARTIAL index
     * ({@code WHERE status = "PENDING"}).
     *
     * <p>This is the highest-QPS query in the system — the publisher runs it
     * continuously — so its cost must track the pending backlog rather than the
     * total transaction history. A full index on {@code status} would grow
     * without bound and slowly turn the publisher into the bottleneck.
     *
     * <p>Scan consistency is deliberately {@code NOT_BOUNDED}: a row missed on
     * this tick is picked up on the next, and the 200ms timer is the retry.
     * Paying for {@code REQUEST_PLUS} on every poll would be waste (LLD §7).
     */
    public List<OutboxEvent> findPending(int limit) {
        String statement = """
                SELECT RAW o
                FROM `%s`.`transactions`.`outbox` o
                WHERE o.status = "PENDING"
                ORDER BY o.createdAt ASC
                LIMIT $limit
                """.formatted(bucket);

        QueryResult result = cluster.query(statement, QueryOptions.queryOptions()
                .parameters(JsonObject.create().put("limit", limit))
                .scanConsistency(QueryScanConsistency.NOT_BOUNDED));

        return result.rowsAsObject().stream()
                .map(DocumentMapper::toOutboxEvent)
                .toList();
    }

    /**
     * Called ONLY after the Kafka producer has acknowledged. Reversing that order
     * makes the whole outbox pattern pointless: a crash between the mark and the
     * publish would lose exactly the message the pattern exists to protect.
     *
     * <p>A sub-document mutation rather than a full replace, so it cannot clobber
     * the payload.
     */
    public void markPublished(String outboxId, Instant publishedAt) {
        outbox.mutateIn("outbox::" + outboxId, List.of(
                MutateInSpec.replace("status", OutboxEvent.Status.PUBLISHED.name()),
                MutateInSpec.replace("publishedAt", publishedAt.toString())
        ));
    }

    /** Gauge source: should hover at ~0. A monotonic climb means Kafka is unreachable. */
    public long countPending() {
        String statement = """
                SELECT RAW COUNT(*)
                FROM `%s`.`transactions`.`outbox` o
                WHERE o.status = "PENDING"
                """.formatted(bucket);
        return cluster.query(statement).rowsAs(Long.class).getFirst();
    }
}
