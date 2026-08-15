package com.fraud.enrichment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Computes the behavioural signals the rule engine evaluates.
 *
 * <p>This is where the Redis atomicity requirements (§9.3, ADR-0007) and the
 * fail-open degradation policy (ADR-0014) actually live.
 */
@SpringBootApplication
public class EnrichmentApplication {
    public static void main(String[] args) {
        SpringApplication.run(EnrichmentApplication.class, args);
    }
}
