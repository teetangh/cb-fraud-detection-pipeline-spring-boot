package com.fraud.audit.api;

import com.fraud.audit.domain.AuditLedgerEntry;
import com.fraud.audit.domain.AuditLedgerEntry.AuditEventType;
import com.fraud.audit.infra.AuditLedgerRepository;
import com.fraud.audit.infra.CallerNotificationLookup;
import com.fraud.audit.infra.WebhookClient;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * {@code fraud.transactions.decisioned} → the ledger, alerts, cases, and the
 * reconciliation webhook.
 */
@Component
public class DecisionedTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(DecisionedTransactionListener.class);

    private final AuditLedgerRepository ledger;
    private final WebhookClient webhookClient;
    private final CallerNotificationLookup callerLookup;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final String actionedTopic;
    private final String alertsTopic;
    private final String serviceVersion;

    public DecisionedTransactionListener(AuditLedgerRepository ledger,
                                         WebhookClient webhookClient,
                                         CallerNotificationLookup callerLookup,
                                         KafkaTemplate<String, String> kafkaTemplate,
                                         ObjectMapper objectMapper,
                                         MeterRegistry meterRegistry,
                                         @Value("${fraud.topics.actioned:fraud.transactions.actioned}") String actionedTopic,
                                         @Value("${fraud.topics.alerts:fraud.alerts.realtime}") String alertsTopic,
                                         @Value("${fraud.service-version:1.0.0}") String serviceVersion) {
        this.ledger = ledger;
        this.webhookClient = webhookClient;
        this.callerLookup = callerLookup;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.actionedTopic = actionedTopic;
        this.alertsTopic = alertsTopic;
        this.serviceVersion = serviceVersion;
    }

    @KafkaListener(topics = "${fraud.topics.decisioned:fraud.transactions.decisioned}",
                   groupId = "${spring.kafka.consumer.group-id:fraud-audit-group}")
    public void onDecisioned(String payload, Acknowledgment acknowledgment) throws Exception {
        ObjectNode d = (ObjectNode) objectMapper.readTree(payload);
        String transactionId = text(d, "transactionId");
        String correlationId = text(d, "correlationId");
        String decision = text(d, "decision");
        MDC.put("correlationId", correlationId);

        try {
            // ── 1. The ledger first. This is the unit of work. ───────────────
            ledger.append(entry(d, AuditEventType.DECISION_RECORDED, decision));

            // ── 2. ACK HERE — deliberately BEFORE the webhook. ───────────────
            //
            // If the commit waited on webhook delivery, an unreachable receiver
            // would block the partition: no progress, growing lag, and eventually
            // a backlog of unaudited decisions. A third party's availability
            // would become this pipeline's availability.
            //
            // Webhook delivery is retried independently and its outcome is
            // recorded on the actioned message, so a failure is visible rather
            // than silent. This is a deliberate split of "the audit record is
            // durable" from "the notification was delivered" — conflating them
            // would trade a strong guarantee for a weak one.
            acknowledgment.acknowledge();

            List<ObjectNode> actions = new ArrayList<>();
            actions.add(action("AUDIT_WRITTEN", "SUCCESS",
                               "audit::" + transactionId + "::DECISION_RECORDED"));

            if (!"ALLOW".equals(decision)) {
                publishAlert(d, transactionId, correlationId, decision);
                actions.add(action("ALERT_RAISED", "SUCCESS", alertsTopic));

                // Only reconcile on a GENUINE difference. The gateway records
                // what it actually told the caller; if that already matches the
                // final decision there is nothing to correct, and notifying
                // anyway would make the reconciliation metric meaningless — its
                // whole purpose is to answer "how often did we tell a caller the
                // wrong thing?" (docs/CONTRACTS.md §9).
                //
                // Unknown ⇒ notify. Over-notifying is safe (the receiver is
                // idempotent on transactionId); under-notifying leaves a caller
                // holding a stale REVIEW for a transaction we actually BLOCKED.
                String previousDecision = callerLookup.whatCallerWasTold(correlationId)
                        .orElse("REVIEW");

                if (callerLookup.needsReconciliation(correlationId, decision)) {
                    boolean delivered = webhookClient.notifyReconciliation(
                            transactionId, correlationId,
                            previousDecision, decision, d.path("riskScore").asInt(),
                            d.get("triggeredRules"), text(d, "policyVersion"));

                    actions.add(action("WEBHOOK_NOTIFIED", delivered ? "SUCCESS" : "FAILED",
                                       delivered ? "reconciled " + previousDecision + " -> " + decision
                                                 : "delivery exhausted after 3 attempts"));
                    if (delivered) {
                        ledger.append(entry(d, AuditEventType.ACTION_EXECUTED, decision));
                        meterRegistry.counter("fraud.audit.reconciliation").increment();
                    }
                } else {
                    actions.add(action("WEBHOOK_NOTIFIED", "SKIPPED",
                                       "caller already told " + decision + " — nothing to reconcile"));
                }
            } else {
                actions.add(action("WEBHOOK_NOTIFIED", "SKIPPED", "decision == ALLOW"));
            }

            if ("BLOCK".equals(decision)) {
                ledger.append(entry(d, AuditEventType.CASE_CREATED, decision));
                actions.add(action("CASE_CREATED", "SUCCESS", "case::" + UUID.randomUUID()));
                meterRegistry.counter("fraud.audit.case.created").increment();
            }

            publishActioned(d, transactionId, correlationId, decision, actions);

        } finally {
            MDC.remove("correlationId");
        }
    }

    private AuditLedgerEntry entry(ObjectNode d, AuditEventType type, String decision) {
        JsonNode rules = d.get("triggeredRules");
        List<Object> ruleList = new ArrayList<>();
        if (rules != null && rules.isArray()) {
            rules.forEach(r -> ruleList.add(objectMapper.convertValue(r, java.util.Map.class)));
        }
        return new AuditLedgerEntry(
                text(d, "transactionId"), text(d, "customerId"),
                d.path("riskScore").asInt(), decision, ruleList,
                text(d, "rulesetVersion"), text(d, "policyVersion"),
                d.path("signalsDegraded").asBoolean(false),
                type, serviceVersion, text(d, "correlationId"), Instant.now());
    }

    /** Keyed by merchantId — the one topic not keyed by customer (ADR-0012). */
    private void publishAlert(ObjectNode d, String transactionId, String correlationId,
                              String decision) throws Exception {
        ObjectNode alert = objectMapper.createObjectNode();
        alert.put("alertId", "alert-" + UUID.randomUUID());
        alert.put("merchantId", text(d, "merchantId"));
        alert.put("customerId", text(d, "customerId"));
        alert.put("transactionId", transactionId);
        alert.put("correlationId", correlationId);
        alert.put("decision", decision);
        alert.put("riskScore", d.path("riskScore").asInt());
        alert.set("triggeredRuleIds", d.get("triggeredRules"));
        alert.put("raisedAt", Instant.now().toString());

        try {
            kafkaTemplate.send(alertsTopic, text(d, "merchantId"),
                               objectMapper.writeValueAsString(alert)).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Counted and logged, but must not block the ledger write.
            meterRegistry.counter("fraud.audit.alert.failed").increment();
            log.error("Alert publish failed transactionId={}", transactionId, e);
        }
    }

    private void publishActioned(ObjectNode d, String transactionId, String correlationId,
                                 String decision, List<ObjectNode> actions) throws Exception {
        ObjectNode actioned = objectMapper.createObjectNode();
        actioned.put("transactionId", transactionId);
        actioned.put("customerId", text(d, "customerId"));
        actioned.put("correlationId", correlationId);
        actioned.put("decision", decision);
        actioned.put("riskScore", d.path("riskScore").asInt());
        actioned.set("actions", objectMapper.valueToTree(actions));
        actioned.put("actionedAt", Instant.now().toString());

        kafkaTemplate.send(actionedTopic, text(d, "customerId"),
                           objectMapper.writeValueAsString(actioned)).get(5, TimeUnit.SECONDS);
    }

    private ObjectNode action(String type, String status, String detail) {
        ObjectNode a = objectMapper.createObjectNode();
        a.put("type", type);
        a.put("status", status);
        a.put("detail", detail);
        return a;
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asString();
    }
}
