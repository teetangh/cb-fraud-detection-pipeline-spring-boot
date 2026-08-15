package com.fraud.decision.infra;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * {@code decision::{transactionId}} in {@code audit.decisions} — immutable once
 * written (spec §8).
 */
@Repository
public class DecisionRepository {

    private static final Logger log = LoggerFactory.getLogger(DecisionRepository.class);

    private final Collection decisions;
    private final ObjectMapper objectMapper;
    private final Counter duplicateCounter;

    public DecisionRepository(@Qualifier("decisionsCollection") Collection decisions,
                              ObjectMapper objectMapper,
                              MeterRegistry meterRegistry) {
        this.decisions = decisions;
        this.objectMapper = objectMapper;
        this.duplicateCounter = Counter.builder("fraud.decision.duplicate.suppressed")
                .register(meterRegistry);
    }

    /**
     * {@code insert()}, not {@code upsert()} — a decision is written once and
     * never revised. A later correction is a new audit-ledger entry, not an edit.
     *
     * <p>A redelivered message throws {@link DocumentExistsException}, which is
     * NORMAL under at-least-once delivery (ADR-0010) and is counted rather than
     * logged as an error.
     */
    public void insert(String transactionId, ObjectNode decision) {
        String key = "decision::" + transactionId;
        try {
            decisions.insert(key, JsonObject.fromJson(objectMapper.writeValueAsString(decision)));
        } catch (DocumentExistsException e) {
            duplicateCounter.increment();
            log.debug("Decision already recorded for transactionId={} (redelivery)", transactionId);
        }
    }
}
