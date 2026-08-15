package com.fraud.mock.infra;

import com.fraud.mock.domain.FraudDecision;
import com.fraud.mock.domain.PaymentRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class FraudGatewayClient {

    private static final Logger log = LoggerFactory.getLogger(FraudGatewayClient.class);

    private final RestClient restClient;
    private final HmacJwtMinter jwtMinter;

    public FraudGatewayClient(RestClient.Builder builder,
                              HmacJwtMinter jwtMinter,
                              @Value("${fraud.gateway.base-url}") String baseUrl,
                              @Value("${fraud.gateway.connect-timeout-ms:500}") long connectMs,
                              @Value("${fraud.gateway.read-timeout-ms:2000}") long readMs) {
        var factory = new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(connectMs));
        factory.setReadTimeout(Duration.ofMillis(readMs));
        this.restClient = builder.baseUrl(baseUrl).requestFactory(factory).build();
        this.jwtMinter = jwtMinter;
    }

    /**
     * BLOCKS on the answer. That is the whole point of this service — it is the
     * caller that cannot complete a payment without knowing ALLOW/REVIEW/BLOCK,
     * and demonstrating that constraint is why it exists.
     *
     * <p>The read timeout is deliberately far above the gateway's own 150ms
     * budget. The gateway is guaranteed to answer within that budget — either the
     * real decision or the safe REVIEW default — so a client timeout BELOW it
     * would abort a request that was about to succeed and manufacture an error
     * out of a working system. Each retry would then create a duplicate
     * transaction, which idempotency absorbs but which pollutes the picture for
     * no reason.
     */
    public FraudDecision evaluate(PaymentRequest request) {
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
        body.put("paymentMethod", request.paymentMethod());

        return restClient.post()
                .uri("/fraud/v1/evaluate")
                .contentType(MediaType.APPLICATION_JSON)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + jwtMinter.mint("mock-payment-api"))
                .body(body)
                .retrieve()
                .body(FraudDecision.class);
    }
}
