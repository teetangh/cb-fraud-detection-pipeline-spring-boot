package com.fraud.audit.infra;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fraud.audit.config.CouchbaseConfig.CouchbaseProperties;
import com.fraud.audit.domain.AuditLedgerEntry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The only implementation of {@link AuditLedgerRepository}.
 *
 * <p>It implements the interface exactly — it does not widen it. Adding a public
 * {@code update} here would defeat the guarantee just as surely as putting one on
 * the interface, since callers could reach it by concrete type.
 */
@Repository
public class CouchbaseAuditLedgerRepository implements AuditLedgerRepository {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseAuditLedgerRepository.class);

    private final Collection ledger;
    private final Cluster cluster;
    private final String bucket;
    private final Counter appendedCounter;
    private final Counter duplicateCounter;

    public CouchbaseAuditLedgerRepository(@Qualifier("auditLedgerCollection") Collection ledger,
                                          Cluster cluster,
                                          CouchbaseProperties props,
                                          MeterRegistry meterRegistry) {
        this.ledger = ledger;
        this.cluster = cluster;
        this.bucket = props.bucket();
        this.appendedCounter = Counter.builder("fraud.audit.ledger.appended").register(meterRegistry);
        this.duplicateCounter = Counter.builder("fraud.audit.ledger.duplicate.suppressed")
                .register(meterRegistry);
    }

    @Override
    public boolean append(AuditLedgerEntry entry) {
        try {
            ledger.insert(entry.documentKey(), toJson(entry));
            appendedCounter.increment();
            return true;
        } catch (DocumentExistsException e) {
            // NORMAL under at-least-once delivery (ADR-0010): a redelivered
            // message re-appends the same key. Counted, not an error.
            duplicateCounter.increment();
            log.debug("Ledger entry already exists key={}", entry.documentKey());
            return false;
        }
    }

    @Override
    public Optional<AuditLedgerEntry> findByKey(String key) {
        try {
            return Optional.of(fromJson(ledger.get(key).contentAsObject()));
        } catch (DocumentNotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<AuditLedgerEntry> findByTransactionId(String transactionId) {
        String statement = """
                SELECT a.* FROM `%s`.`audit`.`audit-ledger` a
                WHERE a.transactionId = $txnId
                """.formatted(bucket);
        return cluster.query(statement, QueryOptions.queryOptions()
                        .parameters(JsonObject.create().put("txnId", transactionId))
                        // Read-after-write: NOT_BOUNDED would legitimately return
                        // nothing for an entry just appended (LLD §7).
                        .scanConsistency(QueryScanConsistency.REQUEST_PLUS))
                .rowsAsObject().stream().map(CouchbaseAuditLedgerRepository::fromJson).toList();
    }

    private static JsonObject toJson(AuditLedgerEntry e) {
        return JsonObject.create()
                .put("docType", AuditLedgerEntry.DOC_TYPE)
                .put("transactionId", e.transactionId())
                .put("customerId", e.customerId())
                .put("riskScore", e.riskScore())
                .put("decision", e.decision())
                .put("triggeredRules", JsonArray.from(e.triggeredRules()))
                .put("rulesetVersion", e.rulesetVersion())
                .put("policyVersion", e.policyVersion())
                .put("signalsDegraded", e.signalsDegraded())
                .put("eventType", e.eventType().name())
                .put("serviceVersion", e.serviceVersion())
                .put("correlationId", e.correlationId())
                .put("recordedAt", e.recordedAt().toString());
    }

    @SuppressWarnings("unchecked")
    private static AuditLedgerEntry fromJson(JsonObject json) {
        JsonArray rules = json.getArray("triggeredRules");
        return new AuditLedgerEntry(
                json.getString("transactionId"),
                json.getString("customerId"),
                json.getInt("riskScore"),
                json.getString("decision"),
                rules == null ? List.of() : (List<Object>) (List<?>) rules.toList(),
                json.getString("rulesetVersion"),
                json.getString("policyVersion"),
                Boolean.TRUE.equals(json.getBoolean("signalsDegraded")),
                AuditLedgerEntry.AuditEventType.valueOf(json.getString("eventType")),
                json.getString("serviceVersion"),
                json.getString("correlationId"),
                Instant.parse(json.getString("recordedAt")));
    }
}
