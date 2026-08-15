package com.fraud.gateway.infra;

import com.fraud.gateway.domain.TransactionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reactive call to ingestion-service.
 *
 * <p>{@link WebClient}, not {@code RestClient}: a blocking call inside the
 * gateway's reactive chain would hold a thread for the whole request and defeat
 * the reason this service is reactive at all (ADR-0003).
 */
@Component
public class IngestionClient {

    private static final Logger log = LoggerFactory.getLogger(IngestionClient.class);

    private final WebClient webClient;
    private final Duration timeout;

    public IngestionClient(WebClient.Builder builder,
                           @Value("${fraud.ingestion.base-url}") String baseUrl,
                           @Value("${fraud.ingestion.timeout-ms:2000}") long timeoutMs) {
        this.webClient = builder.baseUrl(baseUrl).build();
        this.timeout = Duration.ofMillis(timeoutMs);
    }

    /**
     * Fast, synchronous-in-spirit call. Returns once ingestion has durably
     * committed the transaction — that 202 is the point after which the
     * transaction cannot be lost.
     */
    public Mono<Void> ingest(TransactionRequest request, String correlationId) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("transactionId", request.transactionId());
        body.put("customerId", request.customerId());
        body.put("merchantId", request.merchantId());
        body.put("merchantCategoryCode", request.merchantCategoryCode());
        body.put("amount", request.amount());
        body.put("currency", request.currency());
        body.put("countryCode", request.countryCode());
        body.put("deviceId", request.deviceId());
        body.put("ipAddress", request.ipAddress());
        body.put("paymentMethod", request.paymentMethod().name());
        body.put("correlationId", correlationId);

        return webClient.post()
                .uri("/internal/v1/ingest")
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-Correlation-Id", correlationId)
                .bodyValue(body)
                .retrieve()
                .toBodilessEntity()
                .timeout(timeout)
                .doOnError(e -> log.error("Ingestion call failed correlationId={}", correlationId, e))
                .then();
    }
}
