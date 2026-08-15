package com.fraud.mock.infra;

import com.fraud.mock.domain.Payment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * In-memory, on purpose.
 *
 * <p>Giving this service real persistence would invite treating it as part of
 * the system under test. It is a stub with a pulse — the thing being built is
 * the pipeline behind it.
 */
@Component
public class PaymentStore {

    private final Map<String, Payment> byTransactionId = new ConcurrentHashMap<>();

    public void save(Payment payment) {
        byTransactionId.put(payment.transactionId(), payment);
    }

    public Optional<Payment> find(String transactionId) {
        return Optional.ofNullable(byTransactionId.get(transactionId));
    }

    /**
     * Atomic read-modify-write. The reconciliation webhook is at-least-once, so
     * concurrent deliveries of the same reconciliation must not interleave into
     * a lost update.
     */
    public Optional<Payment> update(String transactionId, UnaryOperator<Payment> mutator) {
        return Optional.ofNullable(
                byTransactionId.computeIfPresent(transactionId, (id, existing) -> mutator.apply(existing)));
    }
}
