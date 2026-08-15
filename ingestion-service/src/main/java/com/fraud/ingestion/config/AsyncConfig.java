package com.fraud.ingestion.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Executor for the post-commit outbox nudge.
 *
 * <p>Small and bounded on purpose: the nudge is a latency optimisation, and the
 * scheduled poll is the correctness backstop. If this queue saturates under
 * load, the nudge is DROPPED rather than queued further or run inline — a
 * dropped nudge costs at most one poll interval of delay, which is the ~0ms
 * hot-path contribution ADR-0005 promises. {@code CallerRunsPolicy} would
 * instead run the outbox poll (up to {@code BATCH_SIZE} rows, each waiting up
 * to {@code ACK_TIMEOUT_SECONDS} on a Kafka ack) on the ingest request thread
 * that just committed the transaction, which is exactly the hot-path cost this
 * design exists to avoid — so the policy must be {@link
 * java.util.concurrent.ThreadPoolExecutor.DiscardPolicy}, not {@code
 * CallerRunsPolicy}.
 */
@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean("outboxNudgeExecutor")
    public ThreadPoolTaskExecutor outboxNudgeExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(2);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("outbox-nudge-");
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());
        executor.initialize();
        return executor;
    }
}
