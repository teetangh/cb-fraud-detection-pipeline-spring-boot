package com.fraud.ingestion.infra;

import com.fraud.ingestion.domain.IngestionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Optional;

/**
 * Layer 1 of idempotency (§9.1): a fast path, and <em>only</em> a fast path.
 *
 * <p>All correctness comes from Couchbase {@code insert()} in
 * {@link TransactionIngestor}. Redis narrows the window; it cannot close it —
 * two genuinely concurrent duplicates can both miss the cache before either
 * writes it, and the cache can be down, flushed or evicted (this instance runs
 * {@code allkeys-lru}).
 *
 * <p>Consequently every method here degrades to a miss rather than throwing. A
 * Redis outage costs latency, never correctness (ADR-0006, ADR-0014).
 */
@Component
public class IdempotencyCache {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyCache.class);
    private static final Duration TTL = Duration.ofHours(24);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public IdempotencyCache(StringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    private static String key(String transactionId) {
        return "idempotency:" + transactionId;
    }

    public Optional<IngestionResult> lookup(String transactionId) {
        try {
            String cached = redis.opsForValue().get(key(transactionId));
            if (cached == null) {
                return Optional.empty();
            }
            return Optional.of(objectMapper.readValue(cached, IngestionResult.class));
        } catch (Exception e) {
            // Deliberately swallowed: a cache failure must not fail an ingest.
            log.warn("Idempotency cache lookup failed for {} — falling through to Couchbase, "
                     + "which is the authoritative guard", transactionId, e);
            return Optional.empty();
        }
    }

    public void remember(IngestionResult result) {
        try {
            redis.opsForValue().set(key(result.transactionId()),
                                    objectMapper.writeValueAsString(result),
                                    TTL);
        } catch (Exception e) {
            log.warn("Idempotency cache write failed for {} — duplicates will still be caught "
                     + "by Couchbase insert(), just more slowly", result.transactionId(), e);
        }
    }
}
