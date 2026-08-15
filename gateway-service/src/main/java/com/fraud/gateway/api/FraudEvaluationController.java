package com.fraud.gateway.api;

import com.fraud.gateway.domain.FraudDecisionResponse;
import com.fraud.gateway.domain.TransactionRequest;
import com.fraud.gateway.infra.HmacJwtVerifier;
import com.fraud.gateway.infra.IngestionClient;
import com.fraud.gateway.infra.RateLimiter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/fraud/v1")
public class FraudEvaluationController {

    private static final Logger log = LoggerFactory.getLogger(FraudEvaluationController.class);

    private final HmacJwtVerifier jwtVerifier;
    private final RateLimiter rateLimiter;
    private final IngestionClient ingestionClient;
    private final Timer latencyTimer;
    private final Counter authFailedCounter;

    public FraudEvaluationController(HmacJwtVerifier jwtVerifier,
                                     RateLimiter rateLimiter,
                                     IngestionClient ingestionClient,
                                     MeterRegistry meterRegistry) {
        this.jwtVerifier = jwtVerifier;
        this.rateLimiter = rateLimiter;
        this.ingestionClient = ingestionClient;
        this.latencyTimer = Timer.builder("fraud.gateway.decision.latency")
                .description("Full evaluate round trip").register(meterRegistry);
        this.authFailedCounter = Counter.builder("fraud.gateway.auth.failed").register(meterRegistry);
    }

    /**
     * PHASE 2b SCOPE: authenticate, rate limit, mint the correlation ID, and make
     * the fast synchronous call to ingestion.
     *
     * <p>The bounded Redis Pub/Sub wait — the actual sync facade — lands in Phase
     * 4b, once decision-service exists to publish into it. Until then this
     * returns {@code REVIEW} with {@code resolvedBy=TIMEOUT_DEFAULT}, which is
     * honest: no decision was reached.
     *
     * <p>It deliberately does NOT return a placeholder ALLOW. That would be the
     * precise fail-wide-open behaviour ADR-0004 exists to prevent, and — worse —
     * it would be indistinguishable in the logs and metrics from a real ALLOW,
     * so the safety property would look implemented while being absent.
     */
    @PostMapping("/evaluate")
    public Mono<ResponseEntity<FraudDecisionResponse>> evaluate(
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
            @Valid @RequestBody TransactionRequest request,
            ServerWebExchange exchange) {

        long startNanos = System.nanoTime();
        String correlationId = (String) exchange.getAttributes()
                .get(CorrelationIdWebFilter.CONTEXT_KEY);

        if (!jwtVerifier.verify(authorization)) {
            authFailedCounter.increment();
            log.warn("Rejected unauthenticated request correlationId={}", correlationId);
            return Mono.just(ResponseEntity.status(HttpStatus.UNAUTHORIZED).build());
        }

        return rateLimiter.tryAcquire(request.customerId())
                .flatMap(permitted -> {
                    if (!permitted) {
                        return Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                                .header(HttpHeaders.RETRY_AFTER, "60")
                                .<FraudDecisionResponse>build());
                    }
                    return ingestionClient.ingest(request, correlationId)
                            .then(Mono.fromSupplier(() -> {
                                long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
                                latencyTimer.record(java.time.Duration.ofMillis(latencyMs));
                                log.info("Ingested transactionId={} correlationId={} latencyMs={}",
                                         request.transactionId(), correlationId, latencyMs);
                                return ResponseEntity.ok(FraudDecisionResponse.pendingPipeline(
                                        request.transactionId(), correlationId, latencyMs));
                            }));
                })
                .onErrorResume(e -> {
                    // Ingestion unreachable means nothing was made durable, so
                    // there is nothing to reconcile later. Failing loudly is right
                    // when the alternative is a promise we cannot keep.
                    log.error("Evaluate failed correlationId={}", correlationId, e);
                    return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build());
                });
    }
}
