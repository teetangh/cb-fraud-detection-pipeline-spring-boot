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
        // Sized for BLOCKING work. The nudge no longer just wakes a poller — it
        // performs the Kafka publish itself and blocks up to ACK_TIMEOUT_SECONDS
        // waiting for the broker ack, so a 1-2 thread pool serialises every
        // transaction behind one in-flight ack.
        //
        // Measured: with core=1/max=2/queue=50, a burst of 30 transactions saw
        // only 8 reach a decision — the rest had their nudges DISCARDED and fell
        // back to the timer, which is correct but slow. The pool must be able to
        // absorb the arrival rate, not just signal it.
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("outbox-nudge-");
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());
        executor.initialize();
        return executor;
    }
}
