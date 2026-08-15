package com.fraud.decision.api;

import com.fraud.decision.domain.Decision;
import com.fraud.decision.domain.DecisionPolicy;
import com.fraud.decision.infra.DecisionPublisher;
import com.fraud.decision.infra.DecisionRepository;
import com.fraud.decision.infra.PolicyCache;
import io.micrometer.core.instrument.DistributionSummary;
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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * {@code fraud.transactions.scored} → {@code fraud.transactions.decisioned},
 * plus the Redis publish that ends the gateway's wait.
 *
 * <p>Answers the business question — given this score, what do we do — and knows
 * nothing about how the score was computed (ADR-0008).
 */
@Component
public class ScoredTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(ScoredTransactionListener.class);

    private final PolicyCache policyCache;
    private final DecisionRepository decisionRepository;
    private final DecisionPublisher decisionPublisher;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String decisionedTopic;
    private final MeterRegistry meterRegistry;
    private final DistributionSummary scoreSummary;
    private final Timer duration;

    public ScoredTransactionListener(PolicyCache policyCache,
                                     DecisionRepository decisionRepository,
                                     DecisionPublisher decisionPublisher,
                                     KafkaTemplate<String, String> kafkaTemplate,
                                     ObjectMapper objectMapper,
                                     MeterRegistry meterRegistry,
                                     @Value("${fraud.topics.decisioned:fraud.transactions.decisioned}") String decisionedTopic) {
        this.policyCache = policyCache;
        this.decisionRepository = decisionRepository;
        this.decisionPublisher = decisionPublisher;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.decisionedTopic = decisionedTopic;
        this.meterRegistry = meterRegistry;
        this.scoreSummary = DistributionSummary.builder("fraud.decision.score").register(meterRegistry);
        this.duration = Timer.builder("fraud.decision.duration").register(meterRegistry);
    }

    @KafkaListener(topics = "${fraud.topics.scored:fraud.transactions.scored}",
                   groupId = "${spring.kafka.consumer.group-id:fraud-decision-group}")
    public void onScoredTransaction(String payload, Acknowledgment acknowledgment) throws Exception {
        Timer.Sample sample = Timer.start();
        ObjectNode scored = (ObjectNode) objectMapper.readTree(payload);
        String correlationId = text(scored, "correlationId");
        String transactionId = text(scored, "transactionId");
        MDC.put("correlationId", correlationId);

        try {
            DecisionPolicy policy = policyCache.policy();
            if (policy == null) {
                // No policy has loaded yet — PolicyCache.policy starts null and is
                // only ever populated by a refresh. This is reachable in
                // practice: a restart with an unacked backlog on
                // fraud.transactions.scored (exactly the redelivery scenario
                // manual-ack exists to make safe) can have Kafka deliver a
                // message before the first scheduled refresh completes against
                // Couchbase. Refuse to decide rather than NPE or invent a
                // default — the record is left unacknowledged so it is
                // redelivered once a policy is loaded (§9.8's fail-closed spirit
                // applied to "no policy" the same way it applies to "no answer
                // in time").
                throw new IllegalStateException(
                        "No decision policy loaded yet — refusing to decide (fail-closed, not "
                        + "fail-open) transactionId=" + transactionId);
            }
            int riskScore = scored.path("riskScore").asInt();
            Decision decision = policy.apply(riskScore);

            ObjectNode decisioned = buildDecisioned(scored, decision, policy, transactionId);

            // ── ORDER IS LOAD-BEARING ────────────────────────────────────────
            //
            // 1. Couchbase FIRST. The document is the record of truth; the Redis
            //    publish is an optimisation. Publishing first would create a
            //    window where the caller has been told BLOCK but no durable
            //    record exists — and if this process then died, the caller acted
            //    on a decision the system has no memory of making. In a payment
            //    system that is an unreconcilable discrepancy.
            decisionRepository.insert(transactionId, decisioned);

            // 2. Redis wakeup SECOND, and before Kafka: the caller is on a 150ms
            //    clock, the audit pipeline is not.
            decisionPublisher.publish(correlationId, decisioned);

            // 3. Kafka last.
            kafkaTemplate.send(decisionedTopic, text(scored, "customerId"),
                               objectMapper.writeValueAsString(decisioned))
                    .get(5, TimeUnit.SECONDS);

            acknowledgment.acknowledge();

            scoreSummary.record(riskScore);
            sample.stop(duration);
            meterRegistry.counter("fraud.decision.made", "decision", decision.name()).increment();
            if (scored.path("signalsDegraded").asBoolean(false)) {
                meterRegistry.counter("fraud.decision.degraded").increment();
            }
            logExplainability(decision, riskScore, transactionId, scored, decisioned, policy);

        } finally {
            MDC.remove("correlationId");
        }
    }

    private ObjectNode buildDecisioned(ObjectNode scored, Decision decision,
                                       DecisionPolicy policy, String transactionId) {
        ObjectNode d = objectMapper.createObjectNode();
        d.put("transactionId", transactionId);
        d.put("customerId", text(scored, "customerId"));
        d.put("merchantId", text(scored, "merchantId"));
        d.set("amount", scored.get("amount"));
        d.put("currency", text(scored, "currency"));
        d.put("correlationId", text(scored, "correlationId"));
        d.put("riskScore", scored.path("riskScore").asInt());
        d.put("decision", decision.name());
        d.set("triggeredRules", scored.get("triggeredRules"));
        d.put("rulesetVersion", text(scored, "rulesetVersion"));
        // Both version fields, independently. This pair is what lets an auditor
        // distinguish "the model changed" from "the policy changed" for a
        // historical decision — a distinction that cannot be reconstructed after
        // the fact (ADR-0008).
        d.put("policyVersion", policy.policyVersion());
        d.put("signalsDegraded", scored.path("signalsDegraded").asBoolean(false));
        d.put("decisionAt", Instant.now().toString());
        return d;
    }

    /** Spec §11: full triggeredRules breakdown at INFO for every non-ALLOW decision. */
    private void logExplainability(Decision decision, int riskScore, String transactionId,
                                   ObjectNode scored, ObjectNode decisioned, DecisionPolicy policy) {
        if (decision == Decision.ALLOW) {
            log.debug("Decision ALLOW transactionId={} score={}", transactionId, riskScore);
            return;
        }
        log.info("Decision transactionId={} decision={} score={} rules={} policyVersion={} "
                 + "rulesetVersion={} signalsDegraded={}",
                 transactionId, decision, riskScore, decisioned.get("triggeredRules"),
                 policy.policyVersion(), text(scored, "rulesetVersion"),
                 scored.path("signalsDegraded").asBoolean(false));
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asString();
    }
}
