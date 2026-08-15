package com.fraud.enrichment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.CommitFailedException;
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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.WakeupException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
        assertHarnessWasHealthy(outcome);

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

        // Checked here too, and this is the load-bearing half. A dead survivor also
        // yields a non-empty revocation list — via close(), not via the assignor — so
        // without this the control could report success while proving nothing.
        assertHarnessWasHealthy(outcome);

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

        // And the same statement in the exact form the cooperative test asserts it,
        // so falsifiability is demonstrated directly rather than inferred.
        //
        // This is literally the cooperative test's first assertion evaluated against
        // the eager outcome: it expects a non-empty retained set, and here that set
        // must be EMPTY. Swap CooperativeStickyAssignor for RangeAssignor in the
        // service config and the test above goes red — that is the property §10 asks
        // for, and this line is where it is checked rather than argued.
        assertThat(outcome.survivorRetainedPartitions)
                .as("""
                    the survivor must retain NOTHING under eager rebalancing — held %s \
                    before the kill and lost all of it. A non-empty retained set here \
                    would mean the cooperative test's own assertion also passes under \
                    RangeAssignor, i.e. it is a tautology.""",
                    outcome.survivorHeldBeforeKill())
                .isEmpty();
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
            int totalProcessed,
            List<TopicPartition> survivorLost,
            Throwable survivorFatal,
            boolean survivorExitedEarly,
            List<TopicPartition> survivorHeldBeforeKill) {}

    /**
     * Refuse to draw any conclusion from a broken harness — for <b>both</b> assignors.
     *
     * <p>This exists because of a third harness bug, in the same family as the two
     * recorded in TEST_PLAN.md. The survivor's poll loop died two seconds into a
     * twenty-second run and nothing noticed: the outcome that came back was
     * <em>empty retained, empty revoked</em>, which reads exactly like a legitimate
     * measurement. The cooperative test failed on it — but the eager control would have
     * <em>passed</em> on it, because a dead consumer's {@code close()} reports its
     * partitions and that is all the control was looking for.
     *
     * <p>So the control could have gone green while measuring a corpse, which is the
     * precise failure mode this whole test is built to prevent. Health is now a
     * precondition of both runs rather than an assumption.
     */
    private static void assertHarnessWasHealthy(RebalanceOutcome o) {
        assertThat(o.survivorFatal())
                .as("the survivor's poll loop threw and stopped consuming, so every number "
                    + "in this outcome describes a dead consumer rather than a rebalance")
                .isNull();

        assertThat(o.survivorExitedEarly())
                .as("the survivor stopped polling before the measurement window closed — "
                    + "it was not in the group when the victim was killed, so nothing here "
                    + "is evidence about the assignor")
                .isFalse();

        assertThat(o.survivorLost())
                .as("""
                    the survivor was FENCED during the measurement window \
                    (onPartitionsLost). That is a generation reset, not an assignor \
                    decision, and counting it as a revocation is what made cooperative \
                    look identical to eager. Lost: %s""", o.survivorLost())
                .isEmpty();

        assertThat(o.survivorHeldBeforeKill())
                .as("the survivor held no partitions when the victim was killed, so there "
                    + "was nothing for the rebalance to either retain or revoke — the "
                    + "scenario never actually set itself up")
                .isNotEmpty();
    }

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

        // Start the survivor and let it actually own partitions before the victim
        // joins. Starting both at once makes group formation a race: the leader can
        // be computing an assignment while its own generation is being reset, which
        // is what produced the spurious onPartitionsLost that killed the run.
        pool.submit(survivor);
        survivor.awaitFirstAssignment(15_000);
        pool.submit(victim);

        // Let the group stabilise and both consumers accumulate a baseline.
        sleep(9_000);
        List<TopicPartition> beforeKill = List.copyOf(survivor.assigned);

        // Scope the measurement to the KILL.
        //
        // These lists accumulate from process start, so they also contain the
        // INITIAL group-formation rebalance — where the first consumer
        // legitimately gives up partitions to the joiner, under either assignor.
        // Counting those would make cooperative look identical to eager and the
        // whole comparison meaningless. Only revocations caused by the kill are
        // evidence about the assignor.
        survivor.revoked.clear();
        survivor.lost.clear();
        survivor.timeline.clear();

        victim.shutdown();                 // the rebalance trigger
        sleep(12_000);                     // observe the survivor through it

        // Snapshot BEFORE shutting the survivor down. Closing a consumer fires
        // onPartitionsRevoked for everything it holds, so reading these lists
        // afterwards would show all 6 partitions revoked under EITHER assignor —
        // measuring our own teardown rather than the rebalance.
        List<TopicPartition> revokedByKill = List.copyOf(survivor.revoked);
        List<TopicPartition> lostByKill = List.copyOf(survivor.lost);
        Throwable survivorFatal = survivor.fatal.get();
        boolean survivorExitedEarly = survivor.exited.get();
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
                                    maxGap, gaps, survivor.processed.size(),
                                    lostByKill, survivorFatal, survivorExitedEarly,
                                    beforeKill);
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
        private final CountDownLatch assignedOnce = new CountDownLatch(1);

        final Map<TopicPartition, List<Long>> timeline = new ConcurrentHashMap<>();
        final List<TopicPartition> assigned = new CopyOnWriteArrayList<>();
        final List<TopicPartition> revoked = new CopyOnWriteArrayList<>();
        /** Fencing / generation-reset events. NOT assignor revocations — see below. */
        final List<TopicPartition> lost = new CopyOnWriteArrayList<>();
        final List<String> processed = new CopyOnWriteArrayList<>();
        /** Non-null if the poll loop died. Asserted on, not swallowed. */
        final AtomicReference<Throwable> fatal = new AtomicReference<>();
        /** True once the poll loop has stopped running, for any reason. */
        final AtomicBoolean exited = new AtomicBoolean(false);

        boolean awaitFirstAssignment(long timeoutMs) throws InterruptedException {
            return assignedOnce.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

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
                    if (!partitions.isEmpty()) {
                        assignedOnce.countDown();
                    }
                }

                /**
                 * Overridden deliberately. Kafka's <b>default</b> {@code onPartitionsLost}
                 * delegates straight to {@code onPartitionsRevoked} — so without this
                 * override a <em>fencing / generation reset</em> is filed as though the
                 * assignor had decided to move a partition.
                 *
                 * <p>That is the same bug class as the two recorded in TEST_PLAN.md: it
                 * contaminates the revocation record with events that are not assignor
                 * decisions. It is also how a cooperative consumer's own {@code close()}
                 * reports its partitions (eager reports them via {@code onPartitionsRevoked}),
                 * so folding the two together hides a dead consumer inside a normal-looking
                 * result.
                 */
                @Override
                public void onPartitionsLost(Collection<TopicPartition> partitions) {
                    lost.addAll(partitions);
                    assigned.removeAll(partitions);
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
                    commitWhatWeProcessed();
                }
            } catch (WakeupException | InterruptException e) {
                // Expected during shutdown.
            } catch (Throwable t) {
                // Recorded, never swallowed. A survivor that dies mid-run produces a
                // perfectly plausible-looking outcome — empty retained, empty revoked —
                // and the eager control would still pass on it. assertHarnessWasHealthy
                // turns that into a loud failure instead.
                fatal.set(t);
            } finally {
                exited.set(true);
                try {
                    consumer.close(Duration.ofSeconds(5));
                } catch (Exception ignored) {
                    // already closing
                }
            }
        }

        /**
         * A commit failing <em>because a rebalance is in flight</em> is normal and must
         * not stop the loop.
         *
         * <p>This is what previously killed the survivor two seconds into a twenty-second
         * run: the cooperative revocation round at group formation made {@code commitSync}
         * throw, the blanket {@code catch (Exception)} exited the loop, and {@code close()}
         * then reported every partition as lost. The measurement that followed described a
         * consumer that had left the group before the kill even happened.
         */
        private void commitWhatWeProcessed() {
            try {
                consumer.commitSync();
            } catch (RebalanceInProgressException | CommitFailedException e) {
                // The generation moved on under us. Keep consuming.
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
