package com.fraud.decision;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Applies policy thresholds to the score, and publishes the wakeup that ends the
 * gateway's bounded wait.
 *
 * <p>Knows nothing about how the score was computed — that is scoring-service's
 * concern, and keeping them apart is what lets an auditor distinguish a model
 * change from a policy change (ADR-0008).
 */
@SpringBootApplication
@EnableScheduling   // PolicyCache 60s refresh
public class DecisionApplication {
    public static void main(String[] args) {
        SpringApplication.run(DecisionApplication.class, args);
    }
}
