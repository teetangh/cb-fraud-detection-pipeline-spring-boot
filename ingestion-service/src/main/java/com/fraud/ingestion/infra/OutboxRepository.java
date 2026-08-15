package com.fraud.ingestion.infra;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fraud.ingestion.config.CouchbaseProperties;
import com.fraud.ingestion.domain.OutboxEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Repository
public class OutboxRepository {

    private static final Logger log = LoggerFactory.getLogger(OutboxRepository.class);

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

    /**
     * Atomically claims a row for publishing. Returns true only for the caller
     * that won; everyone else gets false and must not publish.
     *
     * <p><b>Why a CAS claim and not a re-read (issue #12).</b> The previous
     * version re-read the row by KEY and published if it still looked PENDING.
     * That is check-then-act, and it leaves the window open rather than closing
     * it: the post-commit nudge and the scheduled poll can both observe PENDING
     * — truthfully, because neither has acked yet — and both then publish. T3
     * caught exactly that, as two records on the topic carrying the same
     * transactionId from different sources:
     *
     * <pre>
     *   "amount":149.50   ← the nudge, publishing its in-memory event
     *   "amount":149.5    ← the timer, publishing the row it re-read from Couchbase
     * </pre>
     *
     * <p>This is not a tolerable at-least-once duplicate. Enrichment's velocity
     * counter is not idempotent under redelivery, so a systematic double-publish
     * would double every customer's velocity — inflating exactly the signal the
     * fraud rules key on.
     *
     * <p>Couchbase CAS makes the transition atomic: both claimants read the same
     * CAS value, both attempt the mutation with it, and the loser is rejected
     * with {@link CasMismatchException}.
     *
     * <p><b>Crash recovery falls out for free.</b> The claim is a {@code claimedAt}
     * stamp, and the row's status stays PENDING — so a publisher that dies
     * mid-publish leaves a row that {@code findPending} still returns, and whose
     * claim simply expires after {@code staleAfter}. No stranded state, no
     * reaper, and no second index for a status the row only holds for
     * milliseconds at a time.
     *
     * @param staleAfter how long a claim is honoured before another publisher may
     *   take it. MUST exceed the producer ack timeout, or a slow-but-healthy
     *   publish gets reclaimed underneath itself and produces the very duplicate
     *   this method exists to prevent.
     */
    public boolean claim(String outboxId, Duration staleAfter) {
        String key = "outbox::" + outboxId;
        try {
            LookupInResult row = outbox.lookupIn(key, List.of(
                    LookupInSpec.get("status"),
                    LookupInSpec.get("claimedAt")));

            if (!OutboxEvent.Status.PENDING.name().equals(row.contentAs(0, String.class))) {
                return false;                      // already published by someone
            }

            // exists() rather than a null check: claimedAt is absent on a fresh
            // row, and contentAs on an absent path throws rather than returning
            // null.
            if (row.exists(1)) {
                Instant heldSince = Instant.parse(row.contentAs(1, String.class));
                if (heldSince.isAfter(Instant.now().minus(staleAfter))) {
                    return false;                  // a live claim, someone is on it
                }
                log.warn("Reclaiming outboxId={} — claim from {} is older than {}; the previous "
                         + "publisher probably died mid-publish", outboxId, heldSince, staleAfter);
            }

            outbox.mutateIn(key,
                    List.of(MutateInSpec.upsert("claimedAt", Instant.now().toString())),
                    MutateInOptions.mutateInOptions().cas(row.cas()));
            return true;

        } catch (CasMismatchException e) {
            // The expected, healthy outcome of a race — not an error.
            return false;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    /**
     * Drops a claim after a failed publish so the next tick retries immediately
     * rather than waiting out {@code staleAfter}.
     *
     * <p>Best-effort by design: if this fails, the claim still expires on its own.
     * Throwing here would replace a short delay with a lost row.
     */
    public void releaseClaim(String outboxId) {
        try {
            outbox.mutateIn("outbox::" + outboxId, List.of(MutateInSpec.remove("claimedAt")));
        } catch (Exception e) {
            log.debug("Could not release claim on outboxId={}; it will expire on its own",
                      outboxId, e);
        }
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
