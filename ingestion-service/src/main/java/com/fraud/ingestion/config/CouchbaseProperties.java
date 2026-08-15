package com.fraud.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "couchbase")
public record CouchbaseProperties(
        String connectionString,
        String username,
        String password,
        String bucket
) {}
