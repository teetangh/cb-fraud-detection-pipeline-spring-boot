package com.fraud.mock;

import com.fraud.mock.api.FraudWebhookController;
import com.fraud.mock.domain.Payment;
import com.fraud.mock.infra.PaymentStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #14 — the reconciliation webhook receiver.
 *
 * <p>This service is a stub, but this endpoint is not incidental to the design.
 * It is what makes the 150ms REVIEW-on-timeout default acceptable rather than
 * merely safe ({@code ADR-0004}): having told a caller REVIEW, the system owes
 * them the real answer when it arrives.
 *
 * <p>The behaviour under test is the one whose failure mode the controller names
 * itself — reconciliation is <b>at-least-once</b>, so a redelivery must not apply
 * twice. In a real payment system that is a double refund or a double reversal.
 * The stub's job is to be a faithful receiver of that contract, and an
 * unexercised idempotency guard is indistinguishable from a broken one.
 */
@DisplayName("Issue #14 — reconciliation webhook is idempotent")
class ReconciliationWebhookTest {

    private PaymentStore store;
    private MeterRegistry meterRegistry;
    private FraudWebhookController controller;

    @BeforeEach
    void setUp() {
        store = new PaymentStore();
        meterRegistry = new SimpleMeterRegistry();
        controller = new FraudWebhookController(store, meterRegistry);
    }

    @Test
    @DisplayName("a redelivered reconciliation is applied exactly once")
    void redeliveryIsAppliedOnce() {
        String txnId = heldPayment();

        controller.reconcile(payload(txnId, "REVIEW", "BLOCK", 91));
        Instant firstReconciledAt = store.find(txnId).orElseThrow().reconciledAt();
        assertThat(firstReconciledAt).isNotNull();

        // The sender retries. Same payload, same transactionId.
        controller.reconcile(payload(txnId, "REVIEW", "BLOCK", 91));

        Payment after = store.find(txnId).orElseThrow();
        assertThat(after.reconciledAt())
                .as("""
                    reconciledAt must not move on a redelivery. If it does, the guard is \
                    keyed on nothing and the second delivery re-applied the reconciliation \
                    — a double reversal in a real payment system.""")
                .isEqualTo(firstReconciledAt);

        assertThat(counter("payment.webhook.received"))
                .as("both deliveries must still be counted as received — suppression happens "
                    + "after the count, or the metric would hide the retry traffic")
                .isEqualTo(2.0);
        assertThat(counter("payment.webhook.duplicate"))
                .as("exactly one of the two must be recorded as a suppressed duplicate")
                .isEqualTo(1.0);
    }

    @Test
    @DisplayName("the reconciled decision actually changes the payment's business state")
    void reconciliationChangesBusinessState() {
        String txnId = heldPayment();

        controller.reconcile(payload(txnId, "REVIEW", "ALLOW", 12));

        Payment after = store.find(txnId).orElseThrow();
        assertThat(after.status())
                .as("""
                    REVIEW->ALLOW must move the payment out of HELD. A webhook that records \
                    the new decision without changing the status would leave the caller's \
                    money held on a transaction the pipeline allowed — the exact harm the \
                    reconciliation path exists to undo (ADR-0004).""")
                .isEqualTo(Payment.Status.COMPLETED);
        assertThat(after.fraudDecision()).isEqualTo("ALLOW");
        assertThat(after.previousDecision())
                .as("the superseded decision must be retained, or an auditor cannot tell "
                    + "what the caller was originally told")
                .isEqualTo("REVIEW");
        assertThat(after.riskScore()).isEqualTo(12);
    }

    @Test
    @DisplayName("an unknown transactionId returns 200, so the sender stops retrying")
    void unknownTransactionIsAcknowledged() {
        ResponseEntity<Void> response =
                controller.reconcile(payload("txn-never-seen-" + UUID.randomUUID(),
                                             "REVIEW", "BLOCK", 80));

        assertThat(response.getStatusCode().value())
                .as("""
                    a non-2xx here would make action-audit-service retry forever over a \
                    payment this stub never saw, turning an unknown ID into an unbounded \
                    retry loop against a healthy receiver.""")
                .isEqualTo(200);
    }

    @Test
    @DisplayName("concurrent redeliveries still apply exactly once")
    void concurrentRedeliveriesApplyOnce() throws Exception {
        String txnId = heldPayment();

        int senders = 16;
        ExecutorService pool = Executors.newFixedThreadPool(senders);
        CountDownLatch release = new CountDownLatch(1);

        List<Future<?>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < senders; i++) {
            futures.add(pool.submit(() -> {
                release.await();
                return controller.reconcile(payload(txnId, "REVIEW", "BLOCK", 91));
            }));
        }
        release.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(counter("payment.webhook.duplicate"))
                .as("""
                    with 16 simultaneous deliveries, exactly one may apply and 15 must be \
                    suppressed. PaymentStore.update is a computeIfPresent precisely so these \
                    cannot interleave into a lost update; this asserts that holds under real \
                    contention rather than only in sequence.""")
                .isEqualTo(senders - 1.0);

        // Counting a suppression and actually suppressing it are different claims,
        // and the counter above only proves the first. A guard that increments the
        // duplicate counter and then re-applies anyway satisfies it while every one
        // of the 16 deliveries reverses the payment again.
        //
        // Payment.reconciled() carries the CURRENT decision into previousDecision,
        // so a second apply overwrites REVIEW with BLOCK. This is deterministic
        // regardless of which thread won the race.
        Payment after = store.find(txnId).orElseThrow();
        assertThat(after.previousDecision())
                .as("""
                    only one delivery may APPLY, not merely be counted as suppressed. A \
                    second apply would carry the already-reconciled BLOCK into \
                    previousDecision, losing the REVIEW the caller was originally told.""")
                .isEqualTo("REVIEW");
        assertThat(after.riskScore())
                .as("the single applied reconciliation is the one that set the score")
                .isEqualTo(91);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** A payment already HELD by a REVIEW decision — the state reconciliation acts on. */
    private String heldPayment() {
        String txnId = "txn-" + UUID.randomUUID();
        store.save(new Payment(
                "pay-" + UUID.randomUUID(), txnId, UUID.randomUUID().toString(),
                new BigDecimal("149.50"), Payment.Status.HELD, "REVIEW", 55,
                "TIMEOUT_DEFAULT", Instant.now(), null, null));
        return txnId;
    }

    private FraudWebhookController.ReconciliationPayload payload(
            String txnId, String previous, String finalDecision, int score) {
        return new FraudWebhookController.ReconciliationPayload(
                txnId, UUID.randomUUID().toString(), previous, finalDecision,
                score, "policy-v1", "reconciled by test");
    }

    private double counter(String name) {
        return meterRegistry.find(name).counter() == null
                ? 0.0
                : meterRegistry.find(name).counter().count();
    }
}
