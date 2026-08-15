package com.fraud.gateway.api;

import com.fraud.gateway.domain.FraudDecisionResponse;
import com.fraud.gateway.domain.TransactionRequest;
import com.fraud.gateway.infra.CallerNotificationRecord;
import com.fraud.gateway.infra.DecisionWaiter;
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
    private final DecisionWaiter decisionWaiter;
    private final CallerNotificationRecord callerRecord;
    private final Timer latencyTimer;
    private final Counter authFailedCounter;

    public FraudEvaluationController(HmacJwtVerifier jwtVerifier,
                                     RateLimiter rateLimiter,
                                     IngestionClient ingestionClient,
                                     DecisionWaiter decisionWaiter,
                                     CallerNotificationRecord callerRecord,
                                     MeterRegistry meterRegistry) {
        this.jwtVerifier = jwtVerifier;
        this.rateLimiter = rateLimiter;
        this.ingestionClient = ingestionClient;
        this.decisionWaiter = decisionWaiter;
        this.callerRecord = callerRecord;
        this.latencyTimer = Timer.builder("fraud.gateway.decision.latency")
                .description("Full evaluate round trip").register(meterRegistry);
        this.authFailedCounter = Counter.builder("fraud.gateway.auth.failed").register(meterRegistry);
    }

    /**
     * The sync facade: authenticate, rate limit, mint the correlation ID,
     * SUBSCRIBE to the decision channel, then call ingestion, then wait up to
     * 150ms for the real decision.
     *
     * <p>On timeout the caller gets REVIEW with {@code resolvedBy=TIMEOUT_DEFAULT}
     * — never ALLOW (§9.8, ADR-0004). The pipeline is not cancelled: the real
     * decision is still written to Couchbase and reconciled by webhook if it
     * differs.
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
                    // SUBSCRIBE FIRST, then ingest. The ingestion call is passed
                    // as a supplier so DecisionWaiter can run it only once the
                    // Pub/Sub subscription is confirmed live — reversing this
                    // loses a race precisely when the pipeline is healthy and
                    // fast (ADR-0003).
                    return decisionWaiter.awaitDecision(
                                    request.transactionId(), correlationId,
                                    () -> ingestionClient.ingest(request, correlationId),
                                    startNanos)
                            .flatMap(decision -> {
                                latencyTimer.record(java.time.Duration.ofMillis(decision.latencyMs()));
                                log.info("Decision transactionId={} decision={} score={} "
                                         + "resolvedBy={} latencyMs={} correlationId={}",
                                         request.transactionId(), decision.decision(),
                                         decision.riskScore(), decision.resolvedBy(),
                                         decision.latencyMs(), correlationId);
                                // Record what we actually told the caller, so
                                // action-audit-service only reconciles on a
                                // GENUINE difference rather than on every
                                // non-ALLOW decision (docs/CONTRACTS.md §9).
                                return callerRecord.record(decision)
                                        .thenReturn(ResponseEntity.ok(decision));
                            });
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
