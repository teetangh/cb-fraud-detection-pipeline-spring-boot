package com.fraud.scoring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The weighted rule engine. Produces an objective score and its full derivation;
 * has no opinion about what the score means (ADR-0008).
 */
@SpringBootApplication
@EnableScheduling   // RuleCache 60s refresh
public class ScoringApplication {
    public static void main(String[] args) {
        SpringApplication.run(ScoringApplication.class, args);
    }
}
