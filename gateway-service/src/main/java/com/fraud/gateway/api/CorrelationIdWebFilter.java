package com.fraud.gateway.api;

import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Mints the {@code correlationId} for the entire pipeline. This is the ONLY
 * place it is created; every downstream service propagates it and never
 * regenerates it.
 *
 * <p><b>This is the single most common way correlation IDs silently break on
 * WebFlux.</b> A {@code ThreadLocal}-backed MDC does not survive reactive
 * operators — the chain hops threads and the MDC is simply lost, with no error.
 * The logs then have no correlation ID at all, and nobody notices until an
 * incident, which is exactly when it is needed.
 *
 * <p>So the ID is written into the Reactor {@link reactor.util.context.Context},
 * and {@code Hooks.enableAutomaticContextPropagation()} (set in
 * {@code GatewayApplication}) bridges Context to MDC for logging.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdWebFilter implements WebFilter {

    public static final String HEADER = "X-Correlation-Id";
    public static final String CONTEXT_KEY = "correlationId";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String correlationId = exchange.getRequest().getHeaders().getFirst(HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        final String id = correlationId;

        exchange.getResponse().getHeaders().set(HEADER, id);
        exchange.getAttributes().put(CONTEXT_KEY, id);

        return chain.filter(exchange)
                .contextWrite(ctx -> ctx.put(CONTEXT_KEY, id))
                // Also set it on the current thread so logging inside the filter
                // chain itself is attributed. Cleared in doFinally — thread pools
                // reuse threads, and a leaked MDC entry attributes one
                // transaction's logs to another, which is worse than none.
                .doFirst(() -> MDC.put(CONTEXT_KEY, id))
                .doFinally(signal -> MDC.remove(CONTEXT_KEY));
    }
}
