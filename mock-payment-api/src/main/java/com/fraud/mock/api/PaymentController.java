package com.fraud.mock.api;

import com.fraud.mock.domain.FraudDecision;
import com.fraud.mock.domain.Payment;
import com.fraud.mock.domain.PaymentRequest;
import com.fraud.mock.infra.FraudGatewayClient;
import com.fraud.mock.infra.PaymentStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    private final FraudGatewayClient gatewayClient;
    private final PaymentStore store;
    private final MeterRegistry meterRegistry;
    private final Timer fraudLatency;

    public PaymentController(FraudGatewayClient gatewayClient,
                             PaymentStore store,
                             MeterRegistry meterRegistry) {
        this.gatewayClient = gatewayClient;
        this.store = store;
        this.meterRegistry = meterRegistry;
        // THE end-to-end number spec §11 requires. Taken at the outermost caller,
        // so it includes every hop and every queue — the only latency figure that
        // reflects what a real client experiences.
        this.fraudLatency = Timer.builder("payment.fraud.latency")
                .description("mock-payment-api -> fraud decision received, full round trip")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }

    @PostMapping("/initiate")
    public ResponseEntity<Payment> initiate(@Valid @RequestBody PaymentRequest request) {
        Timer.Sample sample = Timer.start(meterRegistry);

        FraudDecision decision;
        try {
            decision = gatewayClient.evaluate(request);
        } catch (Exception e) {
            // A payment service that cannot reach its fraud check must NOT assume
            // approval. Same reasoning as ADR-0004, one layer up.
            log.error("Fraud gateway unreachable for transactionId={} — DECLINING rather than "
                      + "assuming approval", request.transactionId(), e);
            Payment declined = new Payment(
                    "pay-" + UUID.randomUUID(), request.transactionId(), "unavailable",
                    request.amount(), Payment.Status.DECLINED, "BLOCK", 0,
                    "GATEWAY_UNAVAILABLE", Instant.now(), null, null);
            store.save(declined);
            meterRegistry.counter("payment.status", "status", "DECLINED").increment();
            return ResponseEntity.ok(declined);
        }

        sample.stop(fraudLatency);

        Payment payment = new Payment(
                "pay-" + UUID.randomUUID(),
                request.transactionId(),
                decision.correlationId(),
                request.amount(),
                Payment.statusFor(decision.decision()),
                decision.decision(),
                decision.riskScore(),
                decision.resolvedBy(),
                Instant.now(), null, null);

        store.save(payment);
        meterRegistry.counter("payment.status", "status", payment.status().name()).increment();
        meterRegistry.counter("payment.resolved", "source",
                decision.resolvedBy() == null ? "UNKNOWN" : decision.resolvedBy()).increment();

        log.info("Payment {} transactionId={} decision={} score={} resolvedBy={} correlationId={}",
                 payment.status(), request.transactionId(), decision.decision(),
                 decision.riskScore(), decision.resolvedBy(), decision.correlationId());

        return ResponseEntity.ok(payment);
    }

    /** How T7 asserts that a reconciliation was actually APPLIED, not merely sent. */
    @GetMapping("/{transactionId}")
    public ResponseEntity<Payment> get(@PathVariable String transactionId) {
        return store.find(transactionId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
