package com.fraud.enrichment.infra;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.IncrementOptions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * The durable signals. These live in Couchbase rather than Redis on one rule:
 * <b>does losing this silently produce a wrong answer?</b> (ADR-0015)
 */
@Component
public class CouchbaseSignalSource {

    private final Collection profiles;
    private final int p90MinHistory;

    public CouchbaseSignalSource(@Qualifier("customerProfilesCollection") Collection profiles,
                                 @Value("${enrichment.p90-min-history:10}") int p90MinHistory) {
        this.profiles = profiles;
        this.p90MinHistory = p90MinHistory;
    }

    /**
     * Durable lifetime transaction counter, via the Binary Collection counter API
     * (spec §3 requires this API be used).
     *
     * <p>{@code increment} with {@code initial} is atomic AND sets the starting
     * value on creation in one operation — the Couchbase-native equivalent of
     * what {@code velocity.lua} does for the ephemeral Redis windows. No expiry:
     * this one is permanent.
     *
     * <p>Not in Redis, deliberately. Redis runs {@code allkeys-lru}, so under
     * memory pressure this counter would be evicted and reset to zero — with
     * {@code signalsDegraded} UNSET, because Redis was up, it just forgot. A
     * silent wrong answer is worse than a loud missing one.
     */
    public long incrementLifetimeCount(String customerId) {
        return profiles.binary()
                .increment("counter::txn::" + customerId,
                           IncrementOptions.incrementOptions().delta(1).initial(1))
                .content();
    }

    /**
     * {@code amount ÷ the customer's p90 historical amount}.
     *
     * <p>Returns a neutral {@code 1.0} until the customer has at least
     * {@code p90MinHistory} transactions, because <b>p90 over two transactions is
     * noise</b>. A customer whose first purchase was ₹100 and whose second is
     * ₹400 shows a ratio of ~4.0 and trips AMOUNT_DEVIATION (threshold 3.0,
     * weight 20) on entirely normal behaviour — so every new customer making a
     * slightly larger second purchase would be flagged. The signal is only
     * emitted once it means something (ADR-0015).
     */
    public BigDecimal amountVsP90Ratio(String customerId, BigDecimal amount, long lifetimeCount) {
        if (lifetimeCount < p90MinHistory) {
            return BigDecimal.ONE;
        }
        try {
            JsonObject profile = profiles.get("profile::" + customerId).contentAsObject();
            BigDecimal p90 = profile.getBigDecimal("p90Amount");
            if (p90 == null || p90.compareTo(BigDecimal.ZERO) <= 0) {
                return BigDecimal.ONE;
            }
            return amount.divide(p90, 4, RoundingMode.HALF_UP);
        } catch (DocumentNotFoundException e) {
            // Counter says they have history but no profile document exists yet —
            // neutral rather than inventing a ratio.
            return BigDecimal.ONE;
        }
    }

    /** MCC risk score, seeded by infra/couchbase-init. Absent MCC ⇒ neutral 50. */
    public int merchantRiskScore(String merchantCategoryCode) {
        if (merchantCategoryCode == null || merchantCategoryCode.isBlank()) {
            return 50;
        }
        try {
            return profiles.get("mcc::" + merchantCategoryCode)
                    .contentAsObject().getInt("riskScore");
        } catch (DocumentNotFoundException e) {
            return 50;
        }
    }
}
