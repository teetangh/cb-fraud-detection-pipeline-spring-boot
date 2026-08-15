package com.fraud.audit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Side effects live here, isolated from the decision path so a slow webhook
 * receiver or a downstream outage can never delay a decision.
 *
 * <p>Owns the append-only regulatory ledger (§9.7).
 */
@SpringBootApplication
public class AuditApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuditApplication.class, args);
    }
}
