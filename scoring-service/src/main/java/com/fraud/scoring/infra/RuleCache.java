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
     * The ruleset, its version, and whether it has ever loaded, as ONE
     * immutable value.
     *
     * <p>Previously these were three independent volatile fields. A transaction
     * scored between two of those writes could read the NEW rule list but the
     * OLD {@code rulesetVersion} (or vice versa), so the stamped version would
     * not actually describe the rules that produced the score — silently
     * defeating the one property {@code rulesetVersion} exists for (ADR-0008:
     * an auditor can tell exactly which ruleset produced a historical
     * decision). Bundling all three into one record swapped through a single
     * volatile reference makes that tear impossible: a reader always sees a
     * snapshot that is internally consistent, either entirely before or
     * entirely after any given refresh.
     */
    public record Snapshot(List<FraudRule> rules, String rulesetVersion, boolean everLoaded) {}

    private volatile Snapshot snapshot = new Snapshot(List.of(), "0:empty", false);

    public RuleCache(RuleRepository repository, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.refreshCounter = Counter.builder("fraud.scoring.rules.refresh").register(meterRegistry);
        this.refreshFailedCounter = Counter.builder("fraud.scoring.rules.refresh.failed")
                .register(meterRegistry);
        meterRegistry.gauge("fraud.scoring.rules.enabled", this, c -> c.snapshot.rules().size());
    }

    /**
     * One atomic read of everything a scoring decision needs to stay
     * self-consistent: the rules, the version that describes them, and whether
     * they have ever loaded. Callers that need more than one of these fields
     * for the SAME transaction (the listener) must call this once and reuse
     * the result, rather than calling {@link #enabledRules()} and
     * {@link #rulesetVersion()} separately — those can race a concurrent
     * {@link #refresh}.
     */
    public Snapshot snapshot() {
        return snapshot;
    }

    public List<FraudRule> enabledRules() {
        return snapshot.rules();
    }

    public String rulesetVersion() {
        return snapshot.rulesetVersion();
    }

    public boolean everLoaded() {
        return snapshot.everLoaded();
    }

    @Scheduled(fixedDelayString = "${rules.refresh-interval-ms:60000}")
    public void scheduledRefresh() {
        refresh(false);
    }

    /**
     * Synchronized so the scheduled timer and a concurrent force-refresh
     * (POST /admin/rules/refresh) cannot interleave their reads of Couchbase
     * with their writes of {@link #snapshot} — without this, two overlapping
     * refreshes could each compute a version from a different read and then
     * publish them out of order, again breaking the one-version-per-ruleset
     * guarantee.
     *
     * @param consistent when true, reads with {@code REQUEST_PLUS}. Required for
     *   the force-refresh endpoint: the caller has just edited a rule and is
     *   asking for it NOW, and N1QL defaults to {@code NOT_BOUNDED}, so a
     *   stale read would defeat the entire point of the endpoint (LLD §7).
     */
    public synchronized void refresh(boolean consistent) {
        try {
            List<FraudRule> loaded = List.copyOf(repository.findEnabled(consistent));
            String version = computeVersion(loaded);
            this.snapshot = new Snapshot(loaded, version, true);
            refreshCounter.increment();
            log.info("Ruleset refreshed count={} version={} consistent={}",
                     loaded.size(), version, consistent);
        } catch (Exception e) {
            refreshFailedCounter.increment();
            // Keep serving the last known good ruleset. Scoring with slightly
            // stale rules beats not scoring — but note the startup asymmetry in
            // ScoringReadinessIndicator: degrading with stale rules is safe,
            // STARTING with no rules is not, because everything would score 0
            // and 0 means ALLOW.
            Snapshot current = this.snapshot;
            log.error("Ruleset refresh FAILED — continuing with the last known good ruleset "
                      + "(count={}, version={})", current.rules().size(), current.rulesetVersion(), e);
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
