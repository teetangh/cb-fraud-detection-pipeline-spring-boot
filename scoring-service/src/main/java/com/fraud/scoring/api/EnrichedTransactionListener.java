package com.fraud.scoring.api;

import com.fraud.scoring.domain.RuleEvaluator;
import com.fraud.scoring.domain.ScoreResult;
import com.fraud.scoring.domain.TriggeredRule;
import com.fraud.scoring.infra.RuleCache;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@code fraud.transactions.enriched} → {@code fraud.transactions.scored}.
 *
 * <p>Answers an objective question — given these signals and this ruleset, what
 * is the risk score — and has no opinion about what the score means. That
 * belongs to decision-service (ADR-0008).
 */
@Component
public class EnrichedTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(EnrichedTransactionListener.class);

    private final RuleCache ruleCache;
    private final RuleEvaluator evaluator;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String scoredTopic;
    private final MeterRegistry meterRegistry;
    private final DistributionSummary scoreSummary;

    public EnrichedTransactionListener(RuleCache ruleCache,
                                       RuleEvaluator evaluator,
                                       KafkaTemplate<String, String> kafkaTemplate,
                                       ObjectMapper objectMapper,
                                       MeterRegistry meterRegistry,
                                       @Value("${fraud.topics.scored:fraud.transactions.scored}") String scoredTopic) {
        this.ruleCache = ruleCache;
        this.evaluator = evaluator;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.scoredTopic = scoredTopic;
        this.scoreSummary = DistributionSummary.builder("fraud.scoring.score")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "${fraud.topics.enriched:fraud.transactions.enriched}",
                   groupId = "${spring.kafka.consumer.group-id:fraud-scoring-group}")
    public void onEnrichedTransaction(String payload, Acknowledgment acknowledgment) throws Exception {
        // A payload that fails to parse, or parses to something other than a
        // JSON object (an array, a scalar, an empty body), used to throw here
        // uncaught. That skipped acknowledgment.acknowledge() below, so with
        // manual commits (§9.6) the offset was never advanced — the same
        // record redelivers forever and processing on the WHOLE partition
        // stops, not just this one record. One malformed message must cost
        // one message, not the partition.
        JsonNode parsed;
        try {
            parsed = objectMapper.readTree(payload);
        } catch (Exception e) {
            log.error("Unparseable payload on fraud.transactions.enriched — acknowledging so one "
                      + "bad message does not block the partition. length={}",
                      payload == null ? 0 : payload.length(), e);
            acknowledgment.acknowledge();
            return;
        }
        if (!(parsed instanceof ObjectNode enriched)) {
            log.error("Payload on fraud.transactions.enriched is not a JSON object ({}) — "
                      + "acknowledging so one bad message does not block the partition.",
                      parsed.getClass().getSimpleName());
            acknowledgment.acknowledge();
            return;
        }
        String correlationId = text(enriched, "correlationId");
        MDC.put("correlationId", correlationId);

        try {
            // ONE atomic read: the rules, the version describing them, and the
            // count must all come from the SAME snapshot, or a refresh landing
            // mid-transaction could stamp a rulesetVersion that does not
            // actually describe the rules that produced riskScore (ADR-0008).
            RuleCache.Snapshot ruleset = ruleCache.snapshot();
            Map<String, Object> signals = toSignalMap(enriched.get("signals"));
            ScoreResult result = evaluator.score(signals, ruleset.rules());

            ObjectNode scored = enriched.deepCopy();
            scored.put("riskScore", result.riskScore());
            scored.set("triggeredRules", objectMapper.valueToTree(result.triggeredRules()));
            scored.put("evaluatedRuleCount", ruleset.rules().size());
            scored.put("rulesetVersion", ruleset.rulesetVersion());
            scored.put("scoredAt", Instant.now().toString());

            kafkaTemplate.send(scoredTopic, text(enriched, "customerId"),
                               objectMapper.writeValueAsString(scored))
                    .get(5, TimeUnit.SECONDS);

            // Ack ONLY after the downstream publish is durable (§9.6).
            acknowledgment.acknowledge();

            scoreSummary.record(result.riskScore());
            result.triggeredRules().forEach(r ->
                    meterRegistry.counter("fraud.scoring.rule.triggered", "ruleId", r.ruleId())
                            .increment());

            logExplainability(enriched, result, ruleset.rulesetVersion());
        } finally {
            MDC.remove("correlationId");
        }
    }

    /**
     * Spec §11 requires the full breakdown at INFO for every non-ALLOW-bound
     * score, so explainability is demonstrable in the logs and not only in the
     * stored document — it has to work at 3am with {@code grep}, without a query
     * console.
     *
     * <p>The threshold here mirrors the seed policy's ALLOW cutoff. It is a
     * logging heuristic, not a decision: scoring deliberately does not know the
     * policy (ADR-0008).
     *
     * @param rulesetVersion the version from the SAME snapshot used to score
     *   this transaction — not re-read from {@link RuleCache} here, which could
     *   by now reflect a newer refresh and log a version that does not match
     *   the score actually reported above.
     */
    private void logExplainability(ObjectNode enriched, ScoreResult result, String rulesetVersion) {
        String txnId = text(enriched, "transactionId");
        if (result.riskScore() >= 30 || !result.triggeredRules().isEmpty()) {
            String breakdown = result.triggeredRules().stream()
                    .map(r -> "%s(+%d actual=%s threshold=%s)".formatted(
                            r.ruleId(), r.contribution(), r.actualValue(), r.threshold()))
                    .collect(Collectors.joining(", ", "[", "]"));
            log.info("Scored transactionId={} score={} rules={} rulesetVersion={} degraded={}",
                     txnId, result.riskScore(), breakdown, rulesetVersion,
                     enriched.path("signalsDegraded").asBoolean(false));
        } else {
            log.debug("Scored transactionId={} score={} (no rules triggered)", txnId, result.riskScore());
        }
    }

    private static Map<String, Object> toSignalMap(JsonNode signals) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (signals == null || signals.isNull()) {
            return map;
        }
        signals.propertyStream().forEach(entry -> {
            JsonNode v = entry.getValue();
            // Preserve the JSON type: a boolean signal must stay a Boolean, or
            // BOOLEAN_TRUE rules silently never match.
            if (v.isBoolean())      map.put(entry.getKey(), v.asBoolean());
            else if (v.isNumber())  map.put(entry.getKey(), v.decimalValue());
            else if (!v.isNull())   map.put(entry.getKey(), v.asString());
            // null is treated as absent — same as a degraded signal (ADR-0014).
        });
        return map;
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asString();
    }
}
