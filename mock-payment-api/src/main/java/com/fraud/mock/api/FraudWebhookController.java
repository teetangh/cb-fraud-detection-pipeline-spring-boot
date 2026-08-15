package com.fraud.mock.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fraud.mock.domain.Payment;
import com.fraud.mock.infra.PaymentStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/**
 * Receives reconciliation from action-audit-service when the pipeline's true
 * decision differs from what this caller was already told (docs/CONTRACTS.md §9).
 *
 * <p>This endpoint is what makes the 150ms timeout default acceptable rather than
 * merely safe: having told a caller REVIEW, the system owes them the real answer
 * when it arrives. Without it, a timeout would permanently degrade a transaction
 * the pipeline was about to ALLOW.
 */
@RestController
@RequestMapping("/webhooks")
public class FraudWebhookController {

    private static final Logger log = LoggerFactory.getLogger(FraudWebhookController.class);

    private final PaymentStore store;
    private final Counter receivedCounter;
    private final Counter duplicateCounter;

    public FraudWebhookController(PaymentStore store, MeterRegistry meterRegistry) {
        this.store = store;
        this.receivedCounter = Counter.builder("payment.webhook.received").register(meterRegistry);
        this.duplicateCounter = Counter.builder("payment.webhook.duplicate").register(meterRegistry);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ReconciliationPayload(
            String transactionId,
            String correlationId,
            String previousDecision,
            String finalDecision,
            int riskScore,
            String policyVersion,
            String reason
    ) {}

    @PostMapping("/fraud-decision")
    public ResponseEntity<Void> reconcile(@RequestBody ReconciliationPayload payload) {
        receivedCounter.increment();

        store.update(payload.transactionId(), existing -> {
            // AT-LEAST-ONCE. The sender retries, so a redelivery must not apply
            // the reconciliation twice — in a real payment system that is a
            // double refund or a double reversal. Keying on transactionId and
            // guarding on reconciledAt is what makes this safe.
            if (existing.reconciledAt() != null) {
                duplicateCounter.increment();
                log.debug("Duplicate reconciliation ignored transactionId={}", payload.transactionId());
                return existing;
            }
            log.info("Reconciled transactionId={} {} -> {} correlationId={}",
                     payload.transactionId(), payload.previousDecision(),
                     payload.finalDecision(), payload.correlationId());
            return existing.reconciled(payload.finalDecision(), payload.riskScore(), Instant.now());
        });

        // 200 even for an unknown transactionId: the sender must not retry
        // forever over a payment this stub never saw.
        return ResponseEntity.ok().build();
    }
}
