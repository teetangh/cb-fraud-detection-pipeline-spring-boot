package com.fraud.scoring.api;

import com.fraud.scoring.domain.FraudRule;
import com.fraud.scoring.infra.RuleCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Admin surface for the ruleset.
 *
 * <p>Both refresh mechanisms exist because both are needed: the 60s timer is the
 * production behaviour a fraud analyst relies on, and the force endpoint is for
 * operational urgency — and lets [T8] assert hot reload without a 60-second
 * sleep in the test suite.
 *
 * <p><b>Unauthenticated.</b> Internal-network-only under Compose, which is a
 * deployment assumption rather than a control. Tracked in issue #6 alongside the
 * OAuth2/JWKS work — stated here so the gap is visible in the code, not only in
 * a backlog.
 */
@RestController
@RequestMapping("/admin")
public class RuleAdminController {

    private static final Logger log = LoggerFactory.getLogger(RuleAdminController.class);

    private final RuleCache ruleCache;
    private final Set<String> signalRegistry;

    public RuleAdminController(RuleCache ruleCache,
                               @Value("#{T(com.fraud.scoring.domain.SignalKey).REGISTRY}") Set<String> signalRegistry) {
        this.ruleCache = ruleCache;
        this.signalRegistry = signalRegistry;
    }

    /** Forces an immediately-consistent reload. */
    @PostMapping("/rules/refresh")
    public ResponseEntity<Map<String, Object>> refresh() {
        log.info("Forced ruleset refresh requested");
        ruleCache.refresh(true);
        return ResponseEntity.ok(Map.of(
                "rulesetVersion", ruleCache.rulesetVersion(),
                "enabledRules", ruleCache.enabledRules().size()));
    }

    /**
     * {@code signalKeyValid} is the operationally important field.
     *
     * <p>A rule whose signal key is not in the registry can never fire, and looks
     * completely live in the database. With no shared DTO jar there is no
     * compiler to catch the typo, so surfacing it here — alongside the WARN the
     * evaluator logs — is how a fraud analyst finds out their rule is dead.
     */
    @GetMapping("/rules")
    public ResponseEntity<Map<String, Object>> rules() {
        List<Map<String, Object>> rules = ruleCache.enabledRules().stream()
                .map(this::describe)
                .toList();
        return ResponseEntity.ok(Map.of(
                "rulesetVersion", ruleCache.rulesetVersion(),
                "count", rules.size(),
                "rules", rules));
    }

    /**
     * {@code Map.of} throws {@code NullPointerException} on any null key or
     * value. {@code category} (and in principle {@code signalKey}) come
     * straight from an analyst-edited Couchbase document and can be null on a
     * document that omits the field — exactly the malformed-rule case this
     * endpoint's whole purpose is to surface. A null-tolerant map keeps the
     * endpoint answering instead of 500-ing on the very rule it exists to
     * diagnose.
     */
    private Map<String, Object> describe(FraudRule r) {
        Map<String, Object> described = new LinkedHashMap<>();
        described.put("ruleId", r.ruleId());
        described.put("signalKey", r.signalKey());
        // signalRegistry is built with Set.of(...), whose contains(null)
        // throws NullPointerException rather than returning false — so a
        // null signalKey (another malformed-document case) is guarded
        // explicitly instead of being handed to it.
        described.put("signalKeyValid", r.signalKey() != null && signalRegistry.contains(r.signalKey()));
        described.put("operator", r.operator().name());
        described.put("threshold", String.valueOf(r.operator().thresholdOrNull()));
        described.put("weight", r.weight());
        described.put("category", r.category());
        described.put("version", r.version());
        return described;
    }
}
