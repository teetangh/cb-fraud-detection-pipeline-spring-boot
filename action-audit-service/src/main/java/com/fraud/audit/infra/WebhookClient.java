package com.fraud.audit.infra;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reconciliation webhook back to mock-payment-api.
 *
 * <p>This is what makes the 150ms timeout default tolerable rather than merely
 * safe: having told a caller REVIEW, the system owes them the real answer when it
 * arrives. Without it, a timeout would permanently degrade a transaction the
 * pipeline was about to ALLOW.
 */
@Component
public class WebhookClient {

    private static final Logger log = LoggerFactory.getLogger(WebhookClient.class);
    private static final int MAX_ATTEMPTS = 3;

    private final RestClient restClient;
    private final Counter sentCounter;
    private final Counter failedCounter;
    private final Timer latency;

    public WebhookClient(RestClient.Builder builder,
                         MeterRegistry meterRegistry,
                         @Value("${fraud.webhook.base-url}") String baseUrl,
                         @Value("${fraud.webhook.timeout-ms:2000}") long timeoutMs) {
        var factory = new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(timeoutMs));
        factory.setReadTimeout(Duration.ofMillis(timeoutMs));
        this.restClient = builder.baseUrl(baseUrl).requestFactory(factory).build();
        this.sentCounter = meterRegistry.counter("fraud.audit.webhook.sent", "outcome", "SUCCESS");
        this.failedCounter = meterRegistry.counter("fraud.audit.webhook.sent", "outcome", "FAILED");
        this.latency = Timer.builder("fraud.audit.webhook.latency").register(meterRegistry);
    }

    /**
     * @return true if delivered. Retried in place with backoff, but
     *   <b>deliberately OUTSIDE the Kafka offset commit</b> — an unreachable
     *   receiver must not block ledger progress or stall the partition. A third
     *   party's availability is not allowed to become this pipeline's
     *   availability.
     */
    public boolean notifyReconciliation(String transactionId, String correlationId,
                                        String previousDecision, String finalDecision,
                                        int riskScore, Object triggeredRules,
                                        String policyVersion) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("transactionId", transactionId);
        body.put("correlationId", correlationId);
        body.put("previousDecision", previousDecision);
        body.put("finalDecision", finalDecision);
        body.put("riskScore", riskScore);
        body.put("triggeredRules", triggeredRules);
        body.put("policyVersion", policyVersion);
        body.put("reason", "RECONCILIATION");

        Timer.Sample sample = Timer.start();
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                restClient.post()
                        .uri("/webhooks/fraud-decision")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", correlationId)
                        .body(body)
                        .retrieve()
                        .toBodilessEntity();

                sentCounter.increment();
                sample.stop(latency);
                log.info("Reconciliation delivered transactionId={} {} -> {} attempt={}",
                         transactionId, previousDecision, finalDecision, attempt);
                return true;

            } catch (Exception e) {
                if (attempt == MAX_ATTEMPTS) {
                    failedCounter.increment();
                    sample.stop(latency);
                    // ERROR, never silent: an undelivered reconciliation means a
                    // caller is still acting on stale information.
                    log.error("Reconciliation FAILED after {} attempts transactionId={} {} -> {}. "
                              + "The caller is still acting on the stale decision.",
                              MAX_ATTEMPTS, transactionId, previousDecision, finalDecision, e);
                    return false;
                }
                sleep(200L * (1L << (attempt - 1)));   // 200ms, 400ms
            }
        }
        return false;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
