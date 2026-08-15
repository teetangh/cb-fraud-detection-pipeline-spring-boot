package com.fraud.gateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;

@Configuration
public class RedisListenerConfig {

    /**
     * Requires no external threading resources — it uses the Lettuce driver
     * threads to publish messages, which is what allows the gateway to hold many
     * concurrent waits on a handful of threads.
     */
    @Bean(destroyMethod = "destroyLater")
    public ReactiveRedisMessageListenerContainer listenerContainer(ReactiveRedisConnectionFactory factory) {
        return new ReactiveRedisMessageListenerContainer(factory);
    }
}
