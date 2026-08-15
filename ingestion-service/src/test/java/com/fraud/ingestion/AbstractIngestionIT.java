package com.fraud.ingestion;

import com.couchbase.client.java.Cluster;
import com.redis.testcontainers.RedisContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REAL Kafka, REAL Redis, REAL Couchbase. Never in-memory fakes — the whole
 * point of the spec §9 requirements is that they hold against real Couchbase
 * transaction semantics and real Kafka delivery, neither of which a fake
 * reproduces.
 *
 * <p>Containers are {@code static} so they are created once for the whole class
 * rather than per test method. On a memory-constrained machine that is the
 * difference between a suite that runs and one that does not.
 */
public abstract class AbstractIngestionIT {

    protected static final String BUCKET = "fraud-detection";
    protected static final String RAW_TOPIC = "fraud.transactions.raw";

    // kafka-native starts in a couple of seconds versus ~20 for the JVM image.
    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"));

    protected static final RedisContainer REDIS =
            new RedisContainer(DockerImageName.parse("redis:7.4.10-alpine"));

    /**
     * CouchbaseContainer throws ContainerLaunchException if ANALYTICS or EVENTING
     * are enabled against a Community image — those genuinely are Enterprise
     * features. KV, QUERY and INDEX are what this system uses (ADR-0013).
     */
    protected static final CouchbaseContainer COUCHBASE =
            new CouchbaseContainer(DockerImageName.parse("couchbase/server:community-7.6.2"))
                    .withEnabledServices(CouchbaseService.KV,
                                         CouchbaseService.QUERY,
                                         CouchbaseService.INDEX)
                    .withBucket(new BucketDefinition(BUCKET).withPrimaryIndex(false))
                    .withStartupTimeout(Duration.ofMinutes(5));

    /**
     * The containers are static and therefore shared across every subclass in the
     * JVM, but {@code @BeforeAll} runs once PER SUBCLASS — so provisioning would
     * run again for the second test class and fail with ScopeExistsException.
     * Guarded so the schema is created exactly once.
     */
    private static volatile boolean provisioned = false;

    @BeforeAll
    static synchronized void startInfrastructure() {
        KAFKA.start();
        REDIS.start();
        COUCHBASE.start();
        if (!provisioned) {
            provisionCouchbase();
            provisionKafka();
            provisioned = true;
        }
    }

    /**
     * Testcontainers creates the bucket but not the scopes and collections, so
     * this mirrors what infra/couchbase-init/init.sh does in Docker Compose.
     *
     * <p>Deliberately NOT sharing code with that script: this is a second,
     * independent expression of the same schema, so a drift between them shows
     * up as a test failure rather than as two things wrong in the same way.
     */
    private static void provisionCouchbase() {
        try (Cluster cluster = Cluster.connect(COUCHBASE.getConnectionString(),
                                               COUCHBASE.getUsername(),
                                               COUCHBASE.getPassword())) {
            var collections = cluster.bucket(BUCKET).collections();

            collections.createScope("transactions");
            // Scope and collection creation are ASYNCHRONOUS in Couchbase —
            // creating a collection immediately after its scope fails if the
            // cluster has not caught up. Same race the init script handles.
            awaitQuietly(Duration.ofSeconds(2));

            // Two-arg createCollection(scope, name), NOT createCollection(CollectionSpec).
            //
            // CollectionSpec carries a maxTTL and the SDK always sends it, which
            // Couchbase CE rejects outright:
            //   {"errors":{"maxTTL":"Supported in enterprise edition only"}}
            // So the CollectionSpec overload is simply unusable on Community
            // Edition — it fails before it can create anything.
            collections.createCollection("transactions", "raw-transactions");
            collections.createCollection("transactions", "outbox");
            awaitQuietly(Duration.ofSeconds(3));

            cluster.query("""
                    CREATE INDEX idx_outbox_pending
                    ON `%s`.`transactions`.`outbox`(status)
                    WHERE status = "PENDING"
                    """.formatted(BUCKET));

            cluster.bucket(BUCKET).waitUntilReady(Duration.ofMinutes(1));
        }
    }

    private static void provisionKafka() {
        // 6 partitions, matching spec §7 — T3 relies on customerId keying putting
        // a customer's events on one partition.
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(
                java.util.Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(java.util.List.of(
                    new org.apache.kafka.clients.admin.NewTopic(RAW_TOPIC, 6, (short) 1)
            )).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("Could not create test topic " + RAW_TOPIC, e);
        }
    }

    private static void awaitQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Reads every record on {@code topic} matching {@code needle}, from the
     * beginning.
     *
     * <p>Uses {@code assign()} + {@code seekToBeginning()} rather than
     * {@code subscribe()} — deliberately. A subscribing consumer must discover
     * the coordinator, join the group and wait out
     * {@code group.initial.rebalance.delay.ms} before it is assigned anything,
     * which is several seconds. Inside an Awaitility retry loop that cost is paid
     * again on every attempt, and the assertion times out while the message is
     * sitting on the broker the whole time.
     *
     * <p>Manual assignment skips group coordination entirely: deterministic, and
     * fast enough to poll repeatedly.
     */
    protected static List<ConsumerRecord<String, String>> drainMatching(String topic, String needle) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> matching = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            // Poll until two consecutive empty batches — the log is short in tests,
            // so this drains it quickly and deterministically.
            int emptyBatches = 0;
            while (emptyBatches < 2) {
                var records = consumer.poll(Duration.ofMillis(300));
                if (records.isEmpty()) {
                    emptyBatches++;
                } else {
                    emptyBatches = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.value().contains(needle)) {
                            matching.add(record);
                        }
                    }
                }
            }
        }
        return matching;
    }

    @DynamicPropertySource
    static void wireProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("couchbase.connection-string", COUCHBASE::getConnectionString);
        registry.add("couchbase.username", COUCHBASE::getUsername);
        registry.add("couchbase.password", COUCHBASE::getPassword);
        registry.add("couchbase.bucket", () -> BUCKET);
    }
}
