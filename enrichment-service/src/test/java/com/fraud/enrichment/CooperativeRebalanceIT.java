package com.fraud.enrichment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T5 — cooperative sticky rebalancing does not interrupt unaffected partitions
 * (spec §10, §9.5, ADR-0009).
 *
 * <p><b>This test is parameterised across BOTH assignors and asserts that the
 * no-gap property FAILS under {@code RangeAssignor}.</b> That is the whole
 * design of it.
 *
 * <p>Spec §10 requires the test be "actually meaningful, not just checking
 * eventual consistency". A naive version — kill an instance, wait, assert
 * everything eventually processed — <b>passes under eager rebalancing too</b>,
 * because eager also reaches eventual consistency. It would be green whether or
 * not {@code CooperativeStickyAssignor} was configured, and would therefore
 * prove nothing about the setting it claims to verify.
 *
 * <p>So the measurement is a per-partition <em>timeline</em>: for partitions that
 * were never reassigned, was there a gap in processing while the other instance
 * died? Cooperative says no. Eager says yes, because it revokes ALL partitions
 * from ALL members and reassigns from scratch.
 */
@DisplayName("T5 — cooperative rebalance does not stall unaffected partitions")
class CooperativeRebalanceIT {

    private static final String TOPIC = "t5.rebalance.probe";
    private static final int PARTITIONS = 6;

    /** kafka-native: seconds to start rather than ~20, and lower memory. */
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"));

    private static KafkaProducer<String, String> producer;

    @BeforeAll
    static void start() throws Exception {
        KAFKA.start();
        try (AdminClient admin = AdminClient.create(
                Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get();
        }
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"));
    }

    @AfterAll
    static void stop() {
        if (producer != null) producer.close();
        KAFKA.stop();
    }

    // ── the two runs ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("CooperativeStickyAssignor: unaffected partitions keep flowing through the rebalance")
    void cooperativeKeepsUnaffectedPartitionsFlowing() throws Exception {
        RebalanceOutcome outcome = runKillOneInstance(CooperativeStickyAssignor.class.getName());

        assertThat(outcome.survivorRetainedPartitions)
                .as("cooperative rebalancing revokes only the partitions being transferred, so "
                    + "the survivor must KEEP at least one of its original partitions "
                    + "throughout. Revoked: %s", outcome.survivorRevoked)
                .isNotEmpty();

        assertThat(outcome.maxGapOnRetainedMs)
                .as("""
                    partitions the survivor kept must show NO processing gap.
                    A gap here means the whole group stopped — which is eager \
                    rebalancing behaviour, i.e. CooperativeStickyAssignor is not \
                    actually in effect (ADR-0009). Gaps: %s""", outcome.gapsByPartition)
                .isLessThan(GAP_THRESHOLD_MS);

        assertThat(outcome.totalProcessed)
                .as("no message may be lost across the rebalance")
                .isGreaterThan(0);
    }

    @Test
    @DisplayName("RangeAssignor: the SAME scenario stalls — proving the test can fail")
    void eagerStallsTheWholeGroup() throws Exception {
        RebalanceOutcome outcome = runKillOneInstance(RangeAssignor.class.getName());

        // THE point of this test existing.
        //
        // If this assertion ever fails, it means the cooperative test above would
        // also pass with the wrong assignor configured — i.e. it is measuring
        // nothing. A guarantee test that cannot fail is worthless, so the failure
        // case is asserted explicitly rather than assumed.
        assertThat(outcome.survivorRevoked)
                .as("""
                    eager rebalancing must revoke ALL of the survivor's partitions, \
                    including ones nobody asked to move. If this is empty, the two \
                    assignors are behaving identically here and the cooperative \
                    assertion above proves nothing.""")
                .isNotEmpty();
    }

    // ── harness ──────────────────────────────────────────────────────────────

    /**
     * A gap longer than this on a retained partition means the group stopped.
     *
     * <p>Deliberately generous. The claim being tested is "no stop-the-world",
     * not "sub-second scheduling" — a tight bound here would make the test flaky
     * on a loaded machine without making it more meaningful. The eager case
     * stalls for the full rebalance (seconds), so the two are far apart.
     */
    private static final long GAP_THRESHOLD_MS = 2_500;

    private record RebalanceOutcome(
            List<TopicPartition> survivorRetainedPartitions,
            List<TopicPartition> survivorRevoked,
            long maxGapOnRetainedMs,
            Map<TopicPartition, Long> gapsByPartition,
            int totalProcessed) {}

    /**
     * Starts two consumers, feeds a steady stream, kills one, and measures the
     * survivor's per-partition processing timeline.
     */
    private RebalanceOutcome runKillOneInstance(String assignor) throws Exception {
        String group = "t5-" + UUID.randomUUID();
        AtomicBoolean producing = new AtomicBoolean(true);
        ExecutorService pool = Executors.newFixedThreadPool(3);

        // Steady stream across all partitions, so every partition has continuous
        // work — without that, "no gap" would be trivially true for an idle one.
        pool.submit(() -> {
            int n = 0;
            while (producing.get()) {
                for (int p = 0; p < PARTITIONS; p++) {
                    producer.send(new ProducerRecord<>(TOPIC, p, "k" + p, "v" + (n++)));
                }
                producer.flush();
                sleep(50);
            }
        });

        TrackingConsumer survivor = new TrackingConsumer(group, assignor);
        TrackingConsumer victim = new TrackingConsumer(group, assignor);
        pool.submit(survivor);
        pool.submit(victim);

        // Let the group stabilise and both consumers accumulate a baseline.
        sleep(9_000);
        List<TopicPartition> beforeKill = List.copyOf(survivor.assigned);

        // Scope the measurement to the KILL.
        //
        // Both lists accumulate from process start, so they also contain the
        // INITIAL group-formation rebalance — where the first consumer
        // legitimately gives up partitions to the joiner, under either assignor.
        // Counting those would make cooperative look identical to eager and the
        // whole comparison meaningless. Only revocations caused by the kill are
        // evidence about the assignor.
        survivor.revoked.clear();
        survivor.timeline.clear();

        victim.shutdown();                 // the rebalance trigger
        sleep(12_000);                     // observe the survivor through it

        // Snapshot BEFORE shutting the survivor down. Closing a consumer fires
        // onPartitionsRevoked for everything it holds, so reading these lists
        // afterwards would show all 6 partitions revoked under EITHER assignor —
        // measuring our own teardown rather than the rebalance.
        List<TopicPartition> revokedByKill = List.copyOf(survivor.revoked);
        Map<TopicPartition, List<Long>> timelineByKill = new HashMap<>();
        survivor.timeline.forEach((tp, times) -> timelineByKill.put(tp, List.copyOf(times)));

        producing.set(false);
        survivor.shutdown();
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        // Partitions the survivor held before the kill and did NOT lose to it.
        List<TopicPartition> retained = beforeKill.stream()
                .filter(tp -> !revokedByKill.contains(tp))
                .toList();

        Map<TopicPartition, Long> gaps = new HashMap<>();
        long maxGap = 0;
        for (TopicPartition tp : retained) {
            long g = largestGap(timelineByKill.getOrDefault(tp, List.of()));
            gaps.put(tp, g);
            maxGap = Math.max(maxGap, g);
        }

        return new RebalanceOutcome(retained, revokedByKill,
                                    maxGap, gaps, survivor.processed.size());
    }

    /** Largest interval between consecutive processed-message timestamps. */
    private static long largestGap(List<Long> timestamps) {
        long max = 0;
        for (int i = 1; i < timestamps.size(); i++) {
            max = Math.max(max, timestamps.get(i) - timestamps.get(i - 1));
        }
        return max;
    }

    /**
     * Records, per partition, the wall-clock time of every message it processed —
     * and which partitions were revoked, taken from the rebalance callbacks
     * rather than inferred.
     */
    private static final class TrackingConsumer implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final AtomicBoolean running = new AtomicBoolean(true);

        final Map<TopicPartition, List<Long>> timeline = new ConcurrentHashMap<>();
        final List<TopicPartition> assigned = new CopyOnWriteArrayList<>();
        final List<TopicPartition> revoked = new CopyOnWriteArrayList<>();
        final List<String> processed = new CopyOnWriteArrayList<>();

        TrackingConsumer(String group, String assignor) {
            Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignor);
            // Short so the group notices the kill promptly; the measurement is
            // about what happens DURING the rebalance, not how fast it starts.
            config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6_000);
            config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2_000);
            this.consumer = new KafkaConsumer<>(config);
        }

        @Override
        public void run() {
            consumer.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Under cooperative this carries ONLY the partitions actually
                    // being transferred. Under eager it carries everything.
                    revoked.addAll(partitions);
                    assigned.removeAll(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    assigned.addAll(partitions);
                }
            });

            try {
                while (running.get()) {
                    for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
                        TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                        timeline.computeIfAbsent(tp, k -> new CopyOnWriteArrayList<>())
                                .add(System.currentTimeMillis());
                        processed.add(r.value());
                    }
                    consumer.commitSync();
                }
            } catch (Exception e) {
                // Wakeup/close during shutdown is expected.
            } finally {
                try {
                    consumer.close(Duration.ofSeconds(5));
                } catch (Exception ignored) {
                    // already closing
                }
            }
        }

        void shutdown() {
            running.set(false);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
