package com.fraud.audit.infra;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Reads what gateway-service told the caller, so reconciliation fires only on a
 * genuine difference (docs/CONTRACTS.md §9).
 *
 * <p><b>Fails toward notifying.</b> If Redis is unavailable, or no record exists
 * (expired, or the caller never went through the gateway), the webhook is sent.
 * Over-notifying is safe — the receiver is idempotent on {@code transactionId}.
 * Under-notifying is not: a caller left holding a stale REVIEW for a transaction
 * the pipeline actually BLOCKED is the failure this whole mechanism exists to
 * prevent.
 */
@Component
public class CallerNotificationLookup {

    private static final Logger log = LoggerFactory.getLogger(CallerNotificationLookup.class);

    private final StringRedisTemplate redis;
    private final Counter suppressedCounter;

    public CallerNotificationLookup(StringRedisTemplate redis, MeterRegistry meterRegistry) {
        this.redis = redis;
        this.suppressedCounter = Counter.builder("fraud.audit.reconciliation.suppressed")
                .description("Webhooks not sent because the caller already had the right answer")
                .register(meterRegistry);
    }

    /** @return what the caller was told, e.g. "REVIEW", or empty if unknown. */
    public Optional<String> whatCallerWasTold(String correlationId) {
        if (correlationId == null || correlationId.isBlank()) {
            return Optional.empty();
        }
        try {
            String raw = redis.opsForValue().get("told:" + correlationId);
            if (raw == null) {
                return Optional.empty();
            }
            // Stored as "{decision}:{resolvedBy}".
            return Optional.of(raw.split(":", 2)[0]);
        } catch (Exception e) {
            log.debug("Caller-notification lookup failed for {} — will notify", correlationId, e);
            return Optional.empty();
        }
    }

    /**
     * @return true if the caller needs telling, i.e. the final decision differs
     *   from what they were given — or we cannot establish that it doesn't.
     */
    public boolean needsReconciliation(String correlationId, String finalDecision) {
        Optional<String> told = whatCallerWasTold(correlationId);
        if (told.isEmpty()) {
            return true;    // unknown ⇒ notify
        }
        boolean differs = !told.get().equals(finalDecision);
        if (!differs) {
            suppressedCounter.increment();
            log.debug("No reconciliation needed for {}: caller already has {}",
                      correlationId, finalDecision);
        }
        return differs;
    }
}
