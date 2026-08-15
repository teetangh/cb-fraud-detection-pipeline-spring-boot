package com.fraud.decision.api;

import com.fraud.decision.infra.PolicyCache;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Policy admin. Unauthenticated — internal-network-only under Compose is a
 * deployment assumption, not a control. Tracked in issue #6.
 */
@RestController
@RequestMapping("/admin")
public class PolicyAdminController {

    private final PolicyCache policyCache;

    public PolicyAdminController(PolicyCache policyCache) {
        this.policyCache = policyCache;
    }

    /** REQUEST_PLUS reload — the caller just edited the policy and wants it now. */
    @PostMapping("/policy/refresh")
    public ResponseEntity<Map<String, Object>> refresh() {
        policyCache.refresh(true);
        return ResponseEntity.ok(describe());
    }

    @GetMapping("/policy")
    public ResponseEntity<Map<String, Object>> policy() {
        return ResponseEntity.ok(describe());
    }

    private Map<String, Object> describe() {
        var p = policyCache.policy();
        if (p == null) {
            return Map.of("loaded", false);
        }
        return Map.of("loaded", true, "policyVersion", p.policyVersion(),
                      "allowBelow", p.allowBelow(), "blockAtOrAbove", p.blockAtOrAbove());
    }
}
