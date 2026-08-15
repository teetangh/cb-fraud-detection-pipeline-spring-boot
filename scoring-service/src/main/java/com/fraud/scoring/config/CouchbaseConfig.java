package com.fraud.scoring.config;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@EnableConfigurationProperties(CouchbaseConfig.CouchbaseProperties.class)
public class CouchbaseConfig {

    @ConfigurationProperties(prefix = "couchbase")
    public record CouchbaseProperties(String connectionString, String username,
                                      String password, String bucket) {}

    @Bean(destroyMethod = "disconnect")
    public Cluster couchbaseCluster(CouchbaseProperties props) {
        return Cluster.connect(props.connectionString(), props.username(), props.password());
    }

    @Bean
    public Bucket couchbaseBucket(Cluster cluster, CouchbaseProperties props) {
        Bucket bucket = cluster.bucket(props.bucket());
        bucket.waitUntilReady(Duration.ofSeconds(30));
        return bucket;
    }
}
