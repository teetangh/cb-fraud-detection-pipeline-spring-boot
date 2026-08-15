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
 * load, dropping nudges costs at most one poll interval of delay — so a caller
 * runs the task inline rather than the pool growing without limit.
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
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
