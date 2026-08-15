package com.fraud.ingestion.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Puts {@code correlationId} into the MDC so every log line carries it and
 * {@code docker compose logs | grep <id>} reconstructs one transaction's journey
 * across all seven services (spec §11).
 *
 * <p>The ID is minted by gateway-service and propagated; this service only mints
 * a fallback for direct calls (tests, curl).
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String correlationId = request.getHeader(HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        MDC.put(MDC_KEY, correlationId);
        response.setHeader(HEADER, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            // MUST be cleared. Thread pools reuse threads, so a leaked MDC entry
            // attributes one transaction's logs to another — worse than having no
            // correlation ID at all, because it is confidently wrong.
            MDC.remove(MDC_KEY);
        }
    }
}
