package com.fraud.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Owns the durability boundary of the whole pipeline.
 *
 * <p>Once {@code POST /internal/v1/ingest} returns 202, the transaction cannot be
 * lost. Everything before that point may fail freely; everything after is
 * recoverable. That single property is this service's reason to exist.
 */
@SpringBootApplication
@EnableScheduling   // OutboxPublisher
public class IngestionApplication {
    public static void main(String[] args) {
        SpringApplication.run(IngestionApplication.class, args);
    }
}
