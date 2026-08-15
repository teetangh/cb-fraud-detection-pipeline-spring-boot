package com.fraud.scoring;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutateInSpec;
import com.fraud.scoring.domain.RuleEvaluator;
import com.fraud.scoring.domain.ScoreResult;
import com.fraud.scoring.domain.SignalKey;
import com.fraud.scoring.infra.RuleCache;
import com.fraud.scoring.infra.RuleRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * T8 — hot-reload of a fraud rule with no restart (spec §10), plus T2's scoring
 * half and the spec §8 EXPLAIN requirement.
 *
 * <p>Rules are edited <b>directly in Couchbase</b>, not through an admin API,
 * because that is exactly how a fraud analyst would do it and it is the path the
 * requirement actually describes.
 */
@SpringBootTest(properties = {
        // The production interval is 60s. A 60-second sleep in a test suite is
        // unacceptable, so the timer path is exercised at 2s — the MECHANISM is
        // what is being verified; the interval is configuration.
        "rules.refresh-interval-ms=2000"
})
@DisplayName("T8 — rule hot-reload without restart")
class RuleHotReloadIT {

    private static final String BUCKET = "fraud-detection";

    private static final CouchbaseContainer COUCHBASE =
            new CouchbaseContainer(DockerImageName.parse("couchbase/server:community-7.6.2"))
                    // CE rejects ANALYTICS/EVENTING outright (ADR-0013).
                    .withEnabledServices(CouchbaseService.KV, CouchbaseService.QUERY,
                                         CouchbaseService.INDEX)
                    .withBucket(new BucketDefinition(BUCKET).withPrimaryIndex(false))
                    .withStartupTimeout(Duration.ofMinutes(5));

    private static Collection rulesCollection;

    @Autowired RuleCache ruleCache;
    @Autowired RuleRepository ruleRepository;
    @Autowired RuleEvaluator evaluator;

    @BeforeAll
    static void startInfrastructure() {
        COUCHBASE.start();
        try (Cluster cluster = Cluster.connect(COUCHBASE.getConnectionString(),
                                               COUCHBASE.getUsername(), COUCHBASE.getPassword())) {
            var collections = cluster.bucket(BUCKET).collections();
            collections.createScope("intelligence");
            sleep(2000);
            // Two-arg form: CollectionSpec always sends maxTTL, which CE rejects
            // with "Supported in enterprise edition only" (ADR-0001).
            collections.createCollection("intelligence", "fraud-rules");
            sleep(3000);

            cluster.query("""
                    CREATE INDEX idx_rules_enabled
                    ON `%s`.`intelligence`.`fraud-rules`(enabled, category)
                    """.formatted(BUCKET));

            Collection rules = cluster.bucket(BUCKET).scope("intelligence").collection("fraud-rules");
            seedRules(rules);
            cluster.bucket(BUCKET).waitUntilReady(Duration.ofMinutes(1));
        }
        rulesCollection = Cluster.connect(COUCHBASE.getConnectionString(),
                        COUCHBASE.getUsername(), COUCHBASE.getPassword())
                .bucket(BUCKET).scope("intelligence").collection("fraud-rules");
    }

    /** The two seed rules these assertions depend on, matching spec §8 exactly. */
    private static void seedRules(Collection rules) {
        rules.insert("rule::VELOCITY_1M", JsonObject.create()
                .put("docType", "FRAUD_RULE").put("ruleId", "VELOCITY_1M")
                .put("description", "velocity 1m").put("signalKey", SignalKey.VELOCITY_1M)
                .put("operator", "GREATER_THAN").put("threshold", 5).put("weight", 30)
                .put("enabled", true).put("category", "VELOCITY").put("version", 1));
        rules.insert("rule::GEO_ANOMALY", JsonObject.create()
                .put("docType", "FRAUD_RULE").put("ruleId", "GEO_ANOMALY")
                .put("description", "geo anomaly").put("signalKey", SignalKey.DISTINCT_COUNTRIES_24H)
                .put("operator", "GREATER_THAN").put("threshold", 1).put("weight", 25)
                .put("enabled", true).put("category", "GEO").put("version", 1));
        // The decision policy lives in this same collection. If the repository
        // did not filter on docType it would be parsed as a rule and blow up.
        rules.insert("policy::default", JsonObject.create()
                .put("docType", "DECISION_POLICY").put("policyVersion", "v1")
                .put("allowBelow", 30).put("blockAtOrAbove", 70));
    }

    @DynamicPropertySource
    static void wire(DynamicPropertyRegistry registry) {
        registry.add("couchbase.connection-string", COUCHBASE::getConnectionString);
        registry.add("couchbase.username", COUCHBASE::getUsername);
        registry.add("couchbase.password", COUCHBASE::getPassword);
        registry.add("couchbase.bucket", () -> BUCKET);
    }

    // ── T2's scoring half ────────────────────────────────────────────────────

    @Test
    @DisplayName("T2 — velocity_1m = 6 triggers VELOCITY_1M with the weight from Couchbase")
    void velocityRuleTriggersFromLoadedRuleset() {
        ruleCache.refresh(true);

        // An empty ruleset would make every assertion below meaningless — and it
        // is the realistic failure, since a rule-parsing error surfaces as an
        // empty ruleset rather than as an exception at the call site.
        assertThat(ruleCache.enabledRules())
                .as("rules must actually have loaded from Couchbase")
                .isNotEmpty();

        ScoreResult result = evaluator.score(
                Map.of(SignalKey.VELOCITY_1M, 6), ruleCache.enabledRules());

        assertThat(result.triggeredRules())
                .extracting(t -> t.ruleId())
                .contains("VELOCITY_1M");

        // DERIVED from the loaded ruleset, never hardcoded — spec §10 T2 is
        // explicit about asserting against the actual computed score. Hardcoding
        // 30 would break the moment someone legitimately retunes the weight.
        int expected = ruleCache.enabledRules().stream()
                .filter(r -> r.ruleId().equals("VELOCITY_1M"))
                .mapToInt(r -> r.weight()).sum();
        assertThat(result.riskScore()).isEqualTo(expected);

        // The policy document lives in this same collection and must NOT have
        // been parsed as a rule — without the docType filter it would be, and it
        // has no signalKey.
        assertThat(ruleCache.enabledRules()).extracting(r -> r.ruleId())
                .doesNotContain("policy::default")
                .doesNotContainNull();
    }

    // ── T8 proper ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a weight edited directly in Couchbase takes effect on force-refresh, no restart")
    void forcedRefreshPicksUpAnEditedWeight() {
        ruleCache.refresh(true);
        String versionBefore = ruleCache.rulesetVersion();
        int before = evaluator.score(Map.of(SignalKey.VELOCITY_1M, 6), ruleCache.enabledRules()).riskScore();
        assertThat(before).isEqualTo(30);

        // Exactly what a fraud-ops actor does: edit the document. No API, no deploy.
        rulesCollection.mutateIn("rule::VELOCITY_1M", List.of(
                MutateInSpec.replace("weight", 50),
                MutateInSpec.replace("version", 2)));

        ruleCache.refresh(true);

        int after = evaluator.score(Map.of(SignalKey.VELOCITY_1M, 6), ruleCache.enabledRules()).riskScore();
        assertThat(after)
                .as("the edited weight must be in effect with no restart")
                .isEqualTo(50);
        assertThat(ruleCache.rulesetVersion())
                .as("rulesetVersion must change, or an auditor cannot tell which ruleset "
                    + "produced a historical decision (ADR-0008)")
                .isNotEqualTo(versionBefore);

        // restore
        rulesCollection.mutateIn("rule::VELOCITY_1M", List.of(
                MutateInSpec.replace("weight", 30), MutateInSpec.replace("version", 1)));
        ruleCache.refresh(true);
    }

    @Test
    @DisplayName("disabling a rule removes it from the ruleset")
    void disablingARuleRemovesIt() {
        rulesCollection.mutateIn("rule::GEO_ANOMALY", List.of(MutateInSpec.replace("enabled", false)));
        ruleCache.refresh(true);

        // Guard against a VACUOUS pass: "GEO_ANOMALY is absent" and "score is 0"
        // are both trivially true of an EMPTY ruleset, so without this the test
        // stays green when rule loading is completely broken — which is exactly
        // what happened on the first run here.
        assertThat(ruleCache.enabledRules())
                .as("the ruleset must still contain the other rules, or this test proves nothing")
                .isNotEmpty();
        assertThat(ruleCache.enabledRules()).extracting(r -> r.ruleId())
                .doesNotContain("GEO_ANOMALY");
        assertThat(evaluator.score(Map.of(SignalKey.DISTINCT_COUNTRIES_24H, 5),
                                   ruleCache.enabledRules()).riskScore())
                .as("a disabled rule must contribute nothing")
                .isZero();

        rulesCollection.mutateIn("rule::GEO_ANOMALY", List.of(MutateInSpec.replace("enabled", true)));
        ruleCache.refresh(true);
    }

    @Test
    @DisplayName("the 60s TIMER path also picks up an edit — both mechanisms, per spec §10")
    void scheduledRefreshAlsoPicksUpAnEdit() {
        ruleCache.refresh(true);
        assertThat(evaluator.score(Map.of(SignalKey.VELOCITY_1M, 6), ruleCache.enabledRules()).riskScore())
                .isEqualTo(30);

        rulesCollection.mutateIn("rule::VELOCITY_1M", List.of(
                MutateInSpec.replace("weight", 45), MutateInSpec.replace("version", 3)));

        // No forced refresh — the scheduler must do it unaided. Interval is 2s in
        // this context; production is 60s.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(evaluator.score(Map.of(SignalKey.VELOCITY_1M, 6),
                                           ruleCache.enabledRules()).riskScore())
                        .isEqualTo(45));

        rulesCollection.mutateIn("rule::VELOCITY_1M", List.of(
                MutateInSpec.replace("weight", 30), MutateInSpec.replace("version", 1)));
        ruleCache.refresh(true);
    }

    // ── spec §8: verify with EXPLAIN ─────────────────────────────────────────

    @Test
    @DisplayName("the ruleset query uses the GSI index, not a primary scan")
    void ruleQueryUsesIndexScan() {
        String plan = ruleRepository.explainFindEnabled();

        // Assert the REQUIREMENT — not a primary scan, and it used our index —
        // rather than matching one operator spelling. Couchbase picks among
        // IndexScan3, IndexCountScan2, DistinctScan and IntersectScan by query
        // shape, so contains("IndexScan") fails on perfectly good plans.
        assertThat(plan)
                .as("a primary scan means the index is not being used, which at p99 is "
                    + "indistinguishable from having no index:%n%s", plan)
                .doesNotContain("PrimaryScan");
        assertThat(plan)
                .as("plan must use idx_rules_enabled:%n%s", plan)
                .contains("idx_rules_enabled");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
