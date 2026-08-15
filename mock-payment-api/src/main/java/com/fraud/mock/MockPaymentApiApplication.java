package com.fraud.mock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Stands in for a pre-existing payment service. NOT part of the fraud pipeline.
 *
 * <p>The fraud pipeline is designed to be dropped INTO an existing payment
 * system rather than to be the front door itself. Without a real caller in
 * front of it, the most important boundary in the design — where the
 * synchronous/asynchronous seam sits, and who is blocked waiting on whom —
 * could only ever be described, never demonstrated.
 *
 * <p>Deliberately thin. It is not the thing being built.
 */
@SpringBootApplication
public class MockPaymentApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(MockPaymentApiApplication.class, args);
    }
}
