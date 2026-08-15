package com.fraud.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Hooks;

/**
 * The reactive edge. From Phase 4b this service holds one suspended wait per
 * in-flight payment, which is why it is the only WebFlux service in the system
 * (ADR-0003).
 */
@SpringBootApplication
public class GatewayApplication {

    public static void main(String[] args) {
        // Bridges the Reactor Context to the MDC so correlation IDs survive
        // thread hops across reactive operators. Without this, log lines simply
        // have no correlationId — silently, and only noticed during an incident.
        Hooks.enableAutomaticContextPropagation();
        SpringApplication.run(GatewayApplication.class, args);
    }
}
