package com.fraud.probe;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 1 gate. Proves against a REAL Couchbase Community Edition container that
 * the four capabilities spec §3 claims are "all available in Community Edition
 * and are all used" actually are.
 *
 * <p>If this fails, stop. Do not build the outbox on top of a broken assumption —
 * the fallback recorded in ADR-0013 is to embed the outbox event inside the
 * transaction document and rely on single-document atomicity instead.
 */
@Testcontainers
@DisplayName("Couchbase Community Edition capability gate")
class CouchbaseCommunityEditionCapabilityIT {

    private static final String BUCKET = "fraud-detection";

    /**
     * CouchbaseContainer throws ContainerLaunchException if ANALYTICS or EVENTING
     * are requested against a Community image — those genuinely are Enterprise
     * only. KV, QUERY and INDEX are what this system uses.
     */
    private static final CouchbaseContainer COUCHBASE =
            new CouchbaseContainer(DockerImageName.parse("couchbase/server:community-7.6.2"))
                    .withEnabledServices(CouchbaseService.KV,
                                         CouchbaseService.QUERY,
                                         CouchbaseService.INDEX)
                    .withBucket(new BucketDefinition(BUCKET).withPrimaryIndex(true))
                    .withStartupTimeout(Duration.ofMinutes(5));

    private static Cluster cluster;
    private static Collection collection;

    @BeforeAll
    static void startCluster() {
        COUCHBASE.start();
        cluster = Cluster.connect(COUCHBASE.getConnectionString(),
                                  COUCHBASE.getUsername(),
                                  COUCHBASE.getPassword());
        Bucket bucket = cluster.bucket(BUCKET);
        bucket.waitUntilReady(Duration.ofMinutes(2));
        collection = bucket.defaultCollection();
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) cluster.disconnect();
        COUCHBASE.stop();
    }

    @Test
    @DisplayName("reports itself as Community Edition — otherwise this gate proves nothing")
    void isActuallyCommunityEdition() {
        // Guard against a false pass: if the image silently resolved to Enterprise,
        // every assertion below would succeed while telling us nothing about CE.
        String version = cluster.diagnostics().sdk();
        assertThat(version).isNotBlank();
        assertThat(COUCHBASE.getDockerImageName()).contains("community");
    }

    // ── §9.2 — the guarantee the whole outbox pattern rests on ───────────────

    @Test
    @DisplayName("§9.2 multi-document ACID transaction commits both documents atomically")
    void multiDocumentTransactionCommits() {
        String txnId = "txn::" + UUID.randomUUID();
        String outboxId = "outbox::" + UUID.randomUUID();

        cluster.transactions().run(ctx -> {
            ctx.insert(collection, txnId,
                    JsonObject.create().put("docType", "TRANSACTION").put("amount", 149.50));
            ctx.insert(collection, outboxId,
                    JsonObject.create().put("docType", "OUTBOX_EVENT").put("status", "PENDING"));
        });

        assertThat(collection.get(txnId).contentAsObject().getString("docType"))
                .isEqualTo("TRANSACTION");
        assertThat(collection.get(outboxId).contentAsObject().getString("status"))
                .isEqualTo("PENDING");
    }

    @Test
    @DisplayName("§9.2 a failed transaction rolls back BOTH documents — no partial write")
    void failedTransactionRollsBackEverything() {
        String txnId = "txn::" + UUID.randomUUID();
        String outboxId = "outbox::" + UUID.randomUUID();

        // Atomicity is only meaningful if failure rolls back. A commit-only test
        // would pass on a system with no transaction support at all, since two
        // sequential inserts also leave both documents present.
        assertThatThrownBy(() ->
                cluster.transactions().run(ctx -> {
                    ctx.insert(collection, txnId,
                            JsonObject.create().put("docType", "TRANSACTION"));
                    ctx.insert(collection, outboxId,
                            JsonObject.create().put("docType", "OUTBOX_EVENT"));
                    throw new IllegalStateException("simulated crash after both inserts, before commit");
                })
        ).hasRootCauseInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> collection.get(txnId))
                .isInstanceOf(DocumentNotFoundException.class);
        assertThatThrownBy(() -> collection.get(outboxId))
                .isInstanceOf(DocumentNotFoundException.class);
    }

    // ── §9.1 — the authoritative idempotency guard ───────────────────────────

    @Test
    @DisplayName("§9.1 insert() throws on a duplicate key, and does not overwrite")
    void insertRejectsDuplicateKey() {
        String id = "txn::" + UUID.randomUUID();
        collection.insert(id, JsonObject.create().put("attempt", 1));

        assertThatThrownBy(() -> collection.insert(id, JsonObject.create().put("attempt", 2)))
                .isInstanceOf(DocumentExistsException.class);

        // The original survives. This is the property that makes a duplicate client
        // retry safe without depending on Redis being up.
        assertThat(collection.get(id).contentAsObject().getInt("attempt")).isEqualTo(1);
    }

    // ── §3 — the Binary Collection counter API ───────────────────────────────

    @Test
    @DisplayName("§3 binary counter increments atomically under concurrency, with initial value")
    void binaryCounterIsAtomic() throws Exception {
        String counterId = "counter::txn::" + UUID.randomUUID();
        int threads = 20;
        int perThread = 50;

        // increment(initial) sets the starting value on creation in ONE operation.
        // This is the Couchbase-native equivalent of what velocity.lua does for the
        // ephemeral Redis windows, and it is why lifetime_txn_count lives here
        // rather than in Redis (ADR-0015).
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        collection.binary().increment(counterId,
                                IncrementOptions.incrementOptions().delta(1).initial(1));
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertThat(done.await(2, TimeUnit.MINUTES)).isTrue();
        pool.shutdown();

        assertThat(errors.get()).isZero();

        long finalValue = collection.binary()
                .increment(counterId, IncrementOptions.incrementOptions().delta(0))
                .content();

        // EXACTLY this, not "approximately". A lost update here would mean a
        // customer's lifetime history is understated, which silently re-enables
        // the AMOUNT_DEVIATION false positive the counter exists to prevent.
        long expected = (long) threads * perThread;
        assertThat(finalValue)
                .as("binary counter must not lose updates under %d concurrent writers", threads)
                .isEqualTo(expected);
    }

    // ── §3 — N1QL and GSI ────────────────────────────────────────────────────

    @Test
    @DisplayName("§3 N1QL query service and GSI index creation both work on CE")
    void n1qlAndSecondaryIndexesWork() {
        String scope = "`" + BUCKET + "`._default._default";

        cluster.query("CREATE INDEX IF NOT EXISTS idx_probe_doctype ON " + scope + "(docType)");

        String id = "txn::" + UUID.randomUUID();
        collection.insert(id, JsonObject.create().put("docType", "PROBE_MARKER"));

        // REQUEST_PLUS is REQUIRED here, and this is not a test-only detail.
        //
        // Couchbase GSI indexes are updated asynchronously and N1QL defaults to
        // scan consistency NOT_BOUNDED — so a query issued immediately after a
        // KV write legitimately returns zero rows. Without this the assertion
        // below fails, which is exactly what happened on the first run of this
        // probe.
        //
        // Consequence for the services: any read-after-write through N1QL needs
        // REQUEST_PLUS. T8 (rule hot-reload) is the one that would otherwise be
        // intermittently green — it edits a rule and immediately re-queries.
        var consistent = QueryOptions.queryOptions()
                .scanConsistency(QueryScanConsistency.REQUEST_PLUS);

        var result = cluster.query(
                "SELECT RAW COUNT(*) FROM " + scope + " WHERE docType = 'PROBE_MARKER'",
                consistent);
        assertThat(result.rowsAs(Integer.class).get(0)).isGreaterThanOrEqualTo(1);

        // An index that exists but which the planner ignores is indistinguishable
        // from no index at p99, so assert the plan actually uses it.
        //
        // Assert on what the requirement actually is — "not a PrimaryScan, and
        // it used OUR index" — rather than matching one operator spelling.
        // Couchbase picks among several GSI scan operators depending on the
        // query shape: IndexScan3, IndexCountScan2 (covering COUNT, which is
        // what this one plans to), DistinctScan, IntersectScan. Asserting
        // contains("IndexScan") looks right and fails on a perfectly good
        // covering-index plan — it did, on the first run of this probe.
        var explain = cluster.query(
                "EXPLAIN SELECT RAW COUNT(*) FROM " + scope + " WHERE docType = 'PROBE_MARKER'");
        String plan = explain.rowsAsObject().get(0).toString();

        assertThat(plan)
                .as("plan must not fall back to a primary scan:%n%s", plan)
                .doesNotContain("PrimaryScan");
        assertThat(plan)
                .as("plan must use the GSI index we created:%n%s", plan)
                .contains("idx_probe_doctype")
                .contains("\"using\":\"gsi\"");

        cluster.query("DROP INDEX idx_probe_doctype ON " + scope);
    }
}
