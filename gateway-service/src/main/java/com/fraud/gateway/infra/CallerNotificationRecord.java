package com.fraud.gateway.infra;

import com.fraud.gateway.domain.FraudDecisionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Records what the gateway actually told each caller, so action-audit-service can
 * later tell whether the true decision <em>differs</em> from it.
 *
 * <p>Without this, action-audit-service has no visibility into whether the
 * gateway resolved by PIPELINE or by TIMEOUT_DEFAULT, so it must fire a
 * reconciliation webhook for <b>every</b> non-ALLOW decision — including the
 * overwhelming majority where the caller already received the correct answer
 * and there is nothing to reconcile.
 *
 * <p>That is not merely wasteful. `docs/CONTRACTS.md` §9 and
 * `docs/lld/action-audit-service.md` both state the webhook fires only on a
 * genuine difference, and a receiver that trusts that documentation would treat
 * every callback as "the answer changed". Reconciliation traffic would also stop
 * being a signal: the whole point of the
 * {@code fraud.audit.reconciliation} metric is to answer "how often did we tell
 * a caller the wrong thing?", which is meaningless if it fires every time.
 *
 * <p>Redis, short TTL, fail-open: this is an optimisation for webhook targeting,
 * never a correctness mechanism. If it is unavailable, action-audit-service
 * falls back to notifying — over-notifying is safe, under-notifying is not.
 */
@Component
public class CallerNotificationRecord {

    private static final Logger log = LoggerFactory.getLogger(CallerNotificationRecord.class);

    /** Long enough for the pipeline to finish well past the 150ms budget. */
    private static final Duration TTL = Duration.ofMinutes(10);

    private final ReactiveStringRedisTemplate redis;

    public CallerNotificationRecord(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    public static String key(String correlationId) {
        return "told:" + correlationId;
    }

    /**
     * Stores {@code {decision}:{resolvedBy}} — e.g. {@code REVIEW:TIMEOUT_DEFAULT}
     * or {@code BLOCK:PIPELINE}.
     */
    public Mono<Void> record(FraudDecisionResponse response) {
        String value = response.decision().name() + ":" + response.resolvedBy().name();
        return redis.opsForValue()
                .set(key(response.correlationId()), value, TTL)
                .doOnError(e -> log.debug("Could not record caller notification for {} — "
                                          + "audit will fall back to notifying",
                                          response.correlationId()))
                .onErrorReturn(true)
                .then();
    }
}
