package com.fraud.ingestion.config;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Raw Couchbase SDK wiring. Spring Data Couchbase is deliberately absent —
 * §9.2 requires {@code cluster.transactions().run(...)}, which Spring Data does
 * not expose, and dropping it also removes the largest Spring Boot 4
 * compatibility unknown (ADR-0001).
 */
@Configuration
@EnableConfigurationProperties(CouchbaseProperties.class)
public class CouchbaseConfig {

    public static final String SCOPE_TRANSACTIONS = "transactions";
    public static final String COLL_RAW_TRANSACTIONS = "raw-transactions";
    public static final String COLL_OUTBOX = "outbox";

    @Bean(destroyMethod = "disconnect")
    public Cluster couchbaseCluster(CouchbaseProperties props) {
        return Cluster.connect(props.connectionString(), props.username(), props.password());
    }

    @Bean
    public Bucket couchbaseBucket(Cluster cluster, CouchbaseProperties props) {
        Bucket bucket = cluster.bucket(props.bucket());
        // Fail fast and loudly at startup rather than on the first request.
        // Couchbase is the one dependency whose absence makes a 202 a lie, so
        // this service must not report ready without it.
        bucket.waitUntilReady(Duration.ofSeconds(30));
        return bucket;
    }

    @Bean("rawTransactionsCollection")
    public Collection rawTransactionsCollection(Bucket bucket) {
        return bucket.scope(SCOPE_TRANSACTIONS).collection(COLL_RAW_TRANSACTIONS);
    }

    @Bean("outboxCollection")
    public Collection outboxCollection(Bucket bucket) {
        return bucket.scope(SCOPE_TRANSACTIONS).collection(COLL_OUTBOX);
    }
}
