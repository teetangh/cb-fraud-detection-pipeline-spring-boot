package com.fraud.enrichment.api;

import com.fraud.enrichment.domain.EnrichedTransaction;
import com.fraud.enrichment.domain.SignalSet;
import com.fraud.enrichment.domain.TransactionRecord;
import com.fraud.enrichment.infra.SignalEnricher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * {@code fraud.transactions.raw} → {@code fraud.transactions.enriched}.
 *
 * <p>Consumer group {@code fraud-enrichment-group}, with
 * {@code CooperativeStickyAssignor} and {@code MANUAL_IMMEDIATE} acking
 * configured in {@code application.yml} (§9.5, §9.6).
 */
@Component
public class RawTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(RawTransactionListener.class);

    private final SignalEnricher enricher;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String enrichedTopic;
    private final Counter processedCounter;
    private final Timer duration;

    public RawTransactionListener(SignalEnricher enricher,
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper,
                                  MeterRegistry meterRegistry,
                                  @Value("${fraud.topics.enriched:fraud.transactions.enriched}") String enrichedTopic) {
        this.enricher = enricher;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.enrichedTopic = enrichedTopic;
        this.processedCounter = Counter.builder("fraud.enrichment.processed").register(meterRegistry);
        this.duration = Timer.builder("fraud.enrichment.duration").register(meterRegistry);
    }

    @KafkaListener(topics = "${fraud.topics.raw:fraud.transactions.raw}",
                   groupId = "${spring.kafka.consumer.group-id:fraud-enrichment-group}")
    public void onRawTransaction(String payload, Acknowledgment acknowledgment) throws Exception {
        Timer.Sample sample = Timer.start();
        TransactionRecord txn = objectMapper.readValue(payload, TransactionRecord.class);

        // Set up the MDC before any work, so a failure downstream still logs with
        // its correlation ID — which is exactly when it is most needed.
        MDC.put("correlationId", txn.correlationId());
        try {
            SignalSet signals = enricher.enrich(txn);

            EnrichedTransaction enriched = EnrichedTransaction.of(txn, signals, Instant.now());
            String json = objectMapper.writeValueAsString(enriched);

            // Keyed by customerId, same as the raw topic. Load-bearing: it keeps
            // one customer's events on one partition and in order, which is what
            // stops two consumers racing on that customer's velocity counter
            // (ADR-0012). The Lua scripts make each mutation atomic; the key is
            // what makes the SEQUENCE single-writer. Both are required.
            kafkaTemplate.send(enrichedTopic, txn.customerId(), json)
                    .get(5, TimeUnit.SECONDS);

            // ACK ONLY NOW — after the downstream publish is durable (§9.6).
            // Acking earlier would mean a crash in this window loses the message
            // silently rather than redelivering it.
            acknowledgment.acknowledge();

            processedCounter.increment();
            sample.stop(duration);
            log.info("Enriched transactionId={} customerId={} degraded={} correlationId={}",
                     txn.transactionId(), txn.customerId(), signals.signalsDegraded(),
                     txn.correlationId());
        } finally {
            // Thread pools reuse threads; a leaked MDC entry attributes one
            // transaction's logs to another, which is worse than none.
            MDC.remove("correlationId");
        }
    }
}
