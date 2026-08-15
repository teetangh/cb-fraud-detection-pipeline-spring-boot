package com.fraud.scoring.infra;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fraud.scoring.config.CouchbaseConfig.CouchbaseProperties;
import com.fraud.scoring.domain.FraudRule;
import com.fraud.scoring.domain.RuleOperator;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

@Repository
public class RuleRepository {

    private final Cluster cluster;
    private final String bucket;

    public RuleRepository(Cluster cluster, CouchbaseProperties props) {
        this.cluster = cluster;
        this.bucket = props.bucket();
    }

    /**
     * Backed by {@code idx_rules_enabled (enabled, category)}.
     *
     * <p>{@code docType} is filtered too, because the decision policy document
     * lives in this same collection — without it, {@code policy::default} would
     * be parsed as a rule and blow up on a missing {@code signalKey}.
     */
    public List<FraudRule> findEnabled(boolean consistent) {
        String statement = """
                SELECT r.*
                FROM `%s`.`intelligence`.`fraud-rules` r
                WHERE r.docType = "FRAUD_RULE" AND r.enabled = true
                """.formatted(bucket);

        QueryOptions options = QueryOptions.queryOptions().scanConsistency(
                consistent ? QueryScanConsistency.REQUEST_PLUS
                           : QueryScanConsistency.NOT_BOUNDED);

        return cluster.query(statement, options).rowsAsObject().stream()
                .map(RuleRepository::toRule)
                .toList();
    }

    /**
     * The plan for the query above. The integration suite asserts this does NOT
     * fall back to a primary scan — an index that exists but which the planner
     * ignores is indistinguishable from no index at p99, and the failure only
     * shows up under data volume, long after the code was written.
     */
    public String explainFindEnabled() {
        String statement = """
                EXPLAIN SELECT r.*
                FROM `%s`.`intelligence`.`fraud-rules` r
                WHERE r.docType = "FRAUD_RULE" AND r.enabled = true
                """.formatted(bucket);
        return cluster.query(statement).rowsAsObject().getFirst().toString();
    }

    /**
     * Reads {@code threshold} tolerantly.
     *
     * <p>{@code JsonObject.getBigDecimal()} <b>casts</b> rather than coerces, so it
     * throws {@code ClassCastException: Integer cannot be cast to BigDecimal} for
     * a threshold stored as a JSON integer. The seed rules in
     * {@code infra/couchbase-init/seed-fraud-rules.json} store thresholds
     * exactly that way — {@code "threshold": 5} for VELOCITY_1M, {@code 3.0} for
     * AMOUNT_DEVIATION — so this would have failed on every rule load in the
     * real system, and the failure surfaces as an EMPTY ruleset.
     *
     * <p>That is the dangerous shape: an empty ruleset scores every transaction 0,
     * and 0 means ALLOW. It would have looked like "no rules triggered" rather
     * than "the rules never loaded". {@code ScoringReadinessIndicator} refuses
     * traffic until a ruleset has loaded for exactly this reason.
     *
     * <p>A fraud analyst editing a threshold by hand will type {@code 5}, not
     * {@code 5.0}. Both must work.
     */
    private static BigDecimal readThreshold(JsonObject json) {
        Object raw = json.get("threshold");
        return switch (raw) {
            case null            -> null;   // BOOLEAN_TRUE operators carry no threshold
            case BigDecimal bd   -> bd;
            case Integer i       -> BigDecimal.valueOf(i);
            case Long l          -> BigDecimal.valueOf(l);
            case Double d        -> BigDecimal.valueOf(d);
            case Number n        -> new BigDecimal(n.toString());
            case String s        -> new BigDecimal(s);
            default -> throw new IllegalArgumentException(
                    "Rule threshold is not numeric: " + raw + " (" + raw.getClass() + ")");
        };
    }

    private static FraudRule toRule(JsonObject json) {
        BigDecimal threshold = readThreshold(json);
        return new FraudRule(
                json.getString("ruleId"),
                json.getString("description"),
                json.getString("signalKey"),
                RuleOperator.parse(json.getString("operator"), threshold),
                json.getInt("weight"),
                json.getBoolean("enabled"),
                json.getString("category"),
                json.getInt("version"));
    }
}
