package com.fraud.scoring.infra;

import com.fraud.scoring.domain.FraudRule;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/**
 * In-process ruleset cache, refreshed on a timer and on demand.
 *
 * <p>The hot path does <b>zero I/O</b> — rule evaluation is ~2ms of the latency
 * budget precisely because it never touches Couchbase per transaction.
 */
@Component
public class RuleCache {

    private static final Logger log = LoggerFactory.getLogger(RuleCache.class);

    private final RuleRepository repository;
    private final Counter refreshCounter;
    private final Counter refreshFailedCounter;

    /**
     * Swapped atomically as a whole immutable list.
     *
     * <p>A transaction scored mid-refresh must see either the entire old ruleset
     * or the entire new one — never a mixture. A half-applied ruleset would
     * produce a score corresponding to no ruleset that ever existed, which is
     * unexplainable after the fact and would make {@code rulesetVersion} a lie.
     */
    private volatile List<FraudRule> rules = List.of();
    private volatile String rulesetVersion = "0:empty";
    private volatile boolean everLoaded = false;

    public RuleCache(RuleRepository repository, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.refreshCounter = Counter.builder("fraud.scoring.rules.refresh").register(meterRegistry);
        this.refreshFailedCounter = Counter.builder("fraud.scoring.rules.refresh.failed")
                .register(meterRegistry);
        meterRegistry.gauge("fraud.scoring.rules.enabled", this, c -> c.rules.size());
    }

    public List<FraudRule> enabledRules() {
        return rules;
    }

    public String rulesetVersion() {
        return rulesetVersion;
    }

    public boolean everLoaded() {
        return everLoaded;
    }

    @Scheduled(fixedDelayString = "${rules.refresh-interval-ms:60000}")
    public void scheduledRefresh() {
        refresh(false);
    }

    /**
     * @param consistent when true, reads with {@code REQUEST_PLUS}. Required for
     *   the force-refresh endpoint: the caller has just edited a rule and is
     *   asking for it NOW, and N1QL defaults to {@code NOT_BOUNDED}, so a
     *   stale read would defeat the entire point of the endpoint (LLD §7).
     */
    public void refresh(boolean consistent) {
        try {
            List<FraudRule> loaded = repository.findEnabled(consistent);
            this.rules = List.copyOf(loaded);
            this.rulesetVersion = computeVersion(loaded);
            this.everLoaded = true;
            refreshCounter.increment();
            log.info("Ruleset refreshed count={} version={} consistent={}",
                     loaded.size(), rulesetVersion, consistent);
        } catch (Exception e) {
            refreshFailedCounter.increment();
            // Keep serving the last known good ruleset. Scoring with slightly
            // stale rules beats not scoring — but note the startup asymmetry in
            // ScoringReadinessIndicator: degrading with stale rules is safe,
            // STARTING with no rules is not, because everything would score 0
            // and 0 means ALLOW.
            log.error("Ruleset refresh FAILED — continuing with the last known good ruleset "
                      + "(count={}, version={})", rules.size(), rulesetVersion, e);
        }
    }

    /**
     * {@code {enabledCount}:{sha256 of the sorted rule tuples}}.
     *
     * <p>Stamped on every scored message so an auditor can tell exactly which
     * ruleset produced a historical decision — and, paired with
     * {@code policyVersion} from decision-service, can distinguish "the model
     * changed" from "the policy changed" (ADR-0008). That distinction cannot be
     * reconstructed after the fact, so it must be recorded at decision time.
     */
    private static String computeVersion(List<FraudRule> rules) {
        String canonical = rules.stream()
                .sorted(Comparator.comparing(FraudRule::ruleId))
                .map(r -> r.ruleId() + "|" + r.version() + "|" + r.weight() + "|"
                          + r.operator().thresholdOrNull() + "|" + r.enabled())
                .reduce("", (a, b) -> a + ";" + b);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return rules.size() + ":sha256-" + HexFormat.of().formatHex(digest).substring(0, 12);
        } catch (Exception e) {
            return rules.size() + ":unhashed";
        }
    }
}
