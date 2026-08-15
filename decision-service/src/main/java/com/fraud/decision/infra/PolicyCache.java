package com.fraud.decision.infra;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fraud.decision.config.CouchbaseConfig.CouchbaseProperties;
import com.fraud.decision.domain.DecisionPolicy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * In-process policy cache. Same shape as scoring-service's {@code RuleCache},
 * and deliberately a separate service's separate document — an auditor must be
 * able to tell "the policy changed" from "the ruleset changed" (ADR-0008).
 */
@Component
public class PolicyCache {

    private static final Logger log = LoggerFactory.getLogger(PolicyCache.class);

    private final Cluster cluster;
    private final String bucket;
    private final Counter refreshFailed;

    private volatile DecisionPolicy policy;

    public PolicyCache(Cluster cluster, CouchbaseProperties props, MeterRegistry meterRegistry) {
        this.cluster = cluster;
        this.bucket = props.bucket();
        this.refreshFailed = Counter.builder("fraud.decision.policy.refresh.failed")
                .register(meterRegistry);
    }

    public DecisionPolicy policy() {
        return policy;
    }

    public boolean everLoaded() {
        return policy != null;
    }

    @Scheduled(fixedDelayString = "${policy.refresh-interval-ms:60000}")
    public void scheduledRefresh() {
        refresh(false);
    }

    public void refresh(boolean consistent) {
        try {
            DecisionPolicy loaded = load(consistent);
            this.policy = loaded;
            log.info("Policy refreshed version={} allowBelow={} blockAtOrAbove={}",
                     loaded.policyVersion(), loaded.allowBelow(), loaded.blockAtOrAbove());
        } catch (Exception e) {
            refreshFailed.increment();
            // Keep the last known good policy. A rejected or unreadable policy
            // must never silently become a permissive one.
            log.error("Policy refresh FAILED — continuing with the last known good policy ({})",
                      policy == null ? "NONE LOADED" : policy.policyVersion(), e);
        }
    }

    private DecisionPolicy load(boolean consistent) {
        String statement = """
                SELECT p.policyVersion, p.allowBelow, p.blockAtOrAbove
                FROM `%s`.`intelligence`.`fraud-rules` p
                WHERE p.docType = "DECISION_POLICY"
                LIMIT 1
                """.formatted(bucket);

        List<JsonObject> rows = cluster.query(statement, QueryOptions.queryOptions()
                        .scanConsistency(consistent ? QueryScanConsistency.REQUEST_PLUS
                                                    : QueryScanConsistency.NOT_BOUNDED))
                .rowsAsObject();

        if (rows.isEmpty()) {
            throw new IllegalStateException(
                    "No DECISION_POLICY document found. Refusing to invent one — defaulting a "
                    + "policy would silently invent business rules.");
        }
        JsonObject row = rows.getFirst();
        // The compact constructor rejects an inverted policy, so an invalid
        // document throws here and the last known good one survives.
        return new DecisionPolicy(row.getString("policyVersion"),
                                  row.getInt("allowBelow"),
                                  row.getInt("blockAtOrAbove"));
    }
}
