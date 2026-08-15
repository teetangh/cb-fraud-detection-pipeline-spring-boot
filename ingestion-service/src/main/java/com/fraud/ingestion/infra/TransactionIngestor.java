package com.fraud.ingestion.infra;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.fraud.ingestion.domain.IngestionResult;
import com.fraud.ingestion.domain.OutboxEvent;
import com.fraud.ingestion.domain.TransactionRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

/**
 * The durability boundary. Everything in this class exists to make one statement
 * true: <em>once we return, the transaction cannot be silently lost.</em>
 *
 * <p>Implements §9.1 (idempotent ingestion) and §9.2 (transactional outbox).
 */
@Component
public class TransactionIngestor {

    private static final Logger log = LoggerFactory.getLogger(TransactionIngestor.class);

    private final Cluster cluster;
    private final Collection rawTransactions;
    private final Collection outbox;
    private final IdempotencyCache idempotencyCache;
    private final ApplicationEventPublisher events;
    private final String rawTopic;

    private final Counter acceptedCounter;
    private final Counter duplicateCounter;
    private final Timer transactionTimer;

    public TransactionIngestor(Cluster cluster,
                               @Qualifier("rawTransactionsCollection") Collection rawTransactions,
                               @Qualifier("outboxCollection") Collection outbox,
                               IdempotencyCache idempotencyCache,
                               ApplicationEventPublisher events,
                               MeterRegistry meterRegistry,
                               @Value("${fraud.topics.raw:fraud.transactions.raw}") String rawTopic) {
        this.cluster = cluster;
        this.rawTransactions = rawTransactions;
        this.outbox = outbox;
        this.idempotencyCache = idempotencyCache;
        this.events = events;
        this.rawTopic = rawTopic;

        this.acceptedCounter = Counter.builder("fraud.ingestion.accepted")
                .description("Transactions durably accepted").register(meterRegistry);
        // A health signal, NOT an error. Clients retry after ambiguous timeouts;
        // duplicates are expected traffic.
        this.duplicateCounter = Counter.builder("fraud.ingestion.duplicate.suppressed")
                .description("Duplicate transactionIds suppressed").register(meterRegistry);
        this.transactionTimer = Timer.builder("fraud.ingestion.transaction.duration")
                .description("Couchbase multi-document ACID transaction").register(meterRegistry);
    }

    public IngestionResult ingest(TransactionRecord txn) {
        // ── Layer 1: Redis fast path. An optimisation only. ──────────────────
        Optional<IngestionResult> cached = idempotencyCache.lookup(txn.transactionId());
        if (cached.isPresent()) {
            duplicateCounter.increment();
            log.debug("Idempotency cache hit, returning prior result transactionId={} correlationId={}",
                      txn.transactionId(), txn.correlationId());
            return cached.get().asDuplicate();
        }

        // ── Layer 2: Couchbase. THE authoritative guard. ─────────────────────
        try {
            IngestionResult result = transactionTimer.recordCallable(() -> commit(txn));
            acceptedCounter.increment();
            idempotencyCache.remember(result);

            // Wake the publisher now rather than waiting up to a full poll
            // interval. The scheduled poll remains the correctness backstop;
            // this is purely the latency optimisation. BOTH are needed.
            events.publishEvent(new OutboxRecordCommitted(txn.transactionId()));

            log.info("Accepted transactionId={} customerId={} correlationId={}",
                     txn.transactionId(), txn.customerId(), txn.correlationId());
            return result;

        } catch (Exception e) {
            if (isDuplicateKey(e)) {
                // Normal control flow, not an error. Logged at DEBUG and counted —
                // logging this at ERROR trains people to ignore errors.
                duplicateCounter.increment();
                log.debug("Duplicate suppressed by Couchbase insert() transactionId={}",
                          txn.transactionId());
                return readExisting(txn);
            }
            throw new IngestionFailedException(
                    "Could not durably accept transaction " + txn.transactionId(), e);
        }
    }

    /**
     * The transaction record and its outbox row commit atomically, or not at all.
     *
     * <p>Written separately (the naive alternative), a crash between the two leaves
     * the transaction durably recorded and the pipeline never seeing it — no
     * error, no retry, no alert, and the transaction is simply never checked for
     * fraud. See ADR-0005.
     */
    private IngestionResult commit(TransactionRecord txn) {
        Instant now = Instant.now();
        OutboxEvent event = OutboxEvent.pendingFor(txn, rawTopic, now);
        TransactionRecord stamped = txn.createdAt() == null
                ? new TransactionRecord(txn.transactionId(), txn.customerId(), txn.merchantId(),
                                        txn.merchantCategoryCode(), txn.amount(), txn.currency(),
                                        txn.countryCode(), txn.deviceId(), txn.ipAddress(),
                                        txn.paymentMethod(), txn.correlationId(), now)
                : txn;
        OutboxEvent eventForStamped = new OutboxEvent(event.outboxId(), event.status(),
                event.targetTopic(), stamped, event.createdAt(), null);

        cluster.transactions().run(ctx -> {
            // insert(), NEVER upsert(). upsert() on a duplicate would silently
            // overwrite the original AND write a second outbox row — producing a
            // second pipeline event that double-increments the velocity counter,
            // which can get a legitimately retrying customer blocked (§9.1).
            ctx.insert(rawTransactions, stamped.documentKey(), DocumentMapper.toJson(stamped));
            ctx.insert(outbox, eventForStamped.documentKey(), DocumentMapper.toJson(eventForStamped));
        });

        return new IngestionResult(stamped.transactionId(), stamped.correlationId(),
                                   IngestionResult.Status.ACCEPTED);
    }

    /**
     * The SDK wraps the cause in a TransactionFailedException, so catching
     * {@code DocumentExistsException} directly does not work. That bug only shows
     * up under the concurrent-duplicate case, which is exactly what T3 exercises.
     */
    private static boolean isDuplicateKey(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof DocumentExistsException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    /** Return the SAME response the original caller got, so both act identically. */
    private IngestionResult readExisting(TransactionRecord txn) {
        try {
            JsonObject existing = rawTransactions.get(txn.documentKey()).contentAsObject();
            return new IngestionResult(existing.getString("transactionId"),
                                       existing.getString("correlationId"),
                                       IngestionResult.Status.DUPLICATE);
        } catch (Exception e) {
            // The insert lost the race but the document is not readable — the
            // committing transaction may still be finalising. The caller's own
            // retry is the right resolution; do not invent a response.
            throw new IngestionFailedException(
                    "Duplicate detected for " + txn.transactionId() + " but the existing record "
                    + "could not be read; retry", e);
        }
    }

    /** Signals the OutboxPublisher that there is fresh work, ahead of its timer. */
    public record OutboxRecordCommitted(String transactionId) {}

    public static class IngestionFailedException extends RuntimeException {
        public IngestionFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
