# Test Plan — T1 through T10

Spec §10 defines ten scenarios as **acceptance criteria**. Each is implemented as an automated
test against **real** infrastructure — real Kafka, real Redis, real Couchbase via Testcontainers,
or the live Compose stack for end-to-end. Never in-memory fakes: the whole point of the §9
requirements is that they hold against real Kafka rebalancing, real Couchbase transaction
semantics and real Redis Lua atomicity, none of which a fake reproduces.

**No assertion is weakened to make a test pass.** Where a test would otherwise pass regardless of
the behaviour it claims to verify, it is noted and strengthened — see [T5](#t5) especially.

## Coverage map

| ID | Scenario | Phase | Lives in | Verifies |
|---|---|---|---|---|
| [T1](#t1) | Clean allow path | 4 | `gateway-service` | [ADR-0003](adr/0003-pubsub-not-polling.md) |
| [T2](#t2) | Velocity-triggered escalation | 3 | `scoring-service` | [ADR-0007](adr/0007-lua-atomic-counters.md), [ADR-0012](adr/0012-customer-id-partition-key.md) |
| [T3](#t3) | Duplicate request idempotency | 2 | `ingestion-service` | [ADR-0006](adr/0006-idempotent-ingestion.md) |
| [T4](#t4) | Outbox durability under crash | 2 | `ingestion-service` | [ADR-0005](adr/0005-transactional-outbox.md) |
| [T5](#t5) | Cooperative sticky rebalance | 6 | `enrichment-service` | [ADR-0009](adr/0009-cooperative-sticky-assignor.md) |
| [T6](#t6) | Redis outage degrades safely | 4 | `enrichment-service` | [ADR-0014](adr/0014-redis-fail-open.md) |
| [T7](#t7) | Timeout + reconciliation | 4, 5 | `gateway`, `action-audit` | [ADR-0004](adr/0004-review-not-allow-on-timeout.md) |
| [T8](#t8) | Hot-reload a rule | 3 | `scoring-service` | [ADR-0008](adr/0008-scoring-decision-split.md) |
| [T9](#t9) | Ledger cannot be mutated | 5 | `action-audit-service` | [ADR-0011](adr/0011-append-only-ledger.md) |
| [T10](#t10) | Full end-to-end smoke | 5 | `scripts/smoke-test.sh` | everything |

---

## T1 — Clean allow path

**Class:** `CleanAllowPathIT`

A transaction with no unusual signals — new customer, single transaction, normal amount, known
device — resolves to **ALLOW** with `riskScore < 30`, **within the 150ms window**.

```
Given a customer with no transaction history
When  a single normal transaction is submitted via mock-payment-api
Then  decision == ALLOW
And   riskScore < 30
And   resolvedBy == PIPELINE          ← not the timeout fallback
And   observed latency < 150ms
```

**`resolvedBy == PIPELINE` is the load-bearing assertion.** Without it the test passes even if the
pipeline never responded and the gateway returned its REVIEW default — no, worse: it would pass if
the *decision* happened to be ALLOW by default. Asserting `PIPELINE` is what proves the Pub/Sub
wakeup actually worked, and it is the assertion that fails if the subscribe-before-ingest ordering
([ADR-0003](adr/0003-pubsub-not-polling.md)) is ever reversed.

---

## T2 — Velocity-triggered escalation

**Class:** `VelocityEscalationIT`

Six transactions for one `customerId` within a few seconds, exceeding `VELOCITY_1M`'s threshold
of 5.

```
Given 6 transactions for the same customerId within 60 seconds
When  the 6th is scored
Then  triggeredRules contains VELOCITY_1M
And   the entry has actualValue == 6, threshold == 5
And   riskScore == sum of triggered rule weights   ← computed, not hardcoded
And   decision == policy.apply(riskScore)          ← derived, not hardcoded
```

Spec §10 is explicit that this must assert **against the actual computed score, not a hardcoded
expectation**. With only `VELOCITY_1M` firing, the score is 30 → **REVIEW** under the seed policy
(ALLOW < 30 ≤ REVIEW ≤ 69 < BLOCK). Hardcoding `BLOCK` would be wrong; hardcoding `30` would
break the moment someone legitimately retunes a weight. The test derives both from the ruleset and
policy it loaded, so it verifies the *mechanism* rather than a snapshot of the configuration.

This also implicitly proves ordering: six transactions counted correctly in sequence requires them
to have landed on one partition and been processed by one consumer
([ADR-0012](adr/0012-customer-id-partition-key.md)).

**Plus a focused concurrency test** (`VelocityLuaScriptConcurrencyTest`, Phase 3 exit criterion):
hammer `velocity.lua` from 50 threads × 100 increments and assert the final value is **exactly
5000** — not "approximately", not "at least". Also asserts the key has a positive TTL, which is
the failure mode [ADR-0007](adr/0007-lua-atomic-counters.md) exists to prevent.

---

## T3 — Duplicate request idempotency

**Class:** `DuplicateIngestionIT`

```
Given the same transactionId submitted twice, CONCURRENTLY (2 threads)
Then  exactly ONE document exists at txn::{transactionId}
And   exactly ONE event was published to fraud.transactions.raw for it
And   both callers received the same decision and correlationId
```

Driven from two threads against the live service, with a `CountDownLatch` so both requests are
genuinely in flight simultaneously. A sequential version of this test passes with a plain Redis
check and proves nothing — it is the concurrent case that exercises the window where both requests
miss the cache and both proceed, which only Couchbase `insert()` closes.

The Kafka assertion consumes `fraud.transactions.raw` from the beginning and counts records
matching the `transactionId`. Counting is essential: this catches the specific
`upsert()` failure mode where a second outbox row produces a **second** pipeline event and
double-increments the velocity counter.

---

## T4 — Outbox durability under simulated crash

**Class:** `OutboxDurabilityIT`

```
Given outbox.publisher.enabled=false   ← the test hook: publisher frozen
When  a transaction is ingested
Then  the Couchbase transaction record exists
And   the outbox record exists with status == PENDING
And   NO event was published to fraud.transactions.raw

When  the publisher is re-enabled
Then  within 5s the event appears on fraud.transactions.raw
And   the outbox record status becomes PUBLISHED
```

Simulates a crash in the dangerous window — after the Couchbase commit, before the Kafka publish —
without killing a process, which would be slow and flaky. Proves the property that matters: **no
transaction is ever silently dropped, only delayed.**

The negative assertion (`NO event published`) is as important as the positive one. Without it the
test would pass on a system that published eagerly and never used the outbox at all.

---

## T5 — Cooperative sticky rebalance does not interrupt unaffected partitions

**Class:** `CooperativeRebalanceIT` — the hardest test in the suite

```
Given an enrichment consumer group with 2 instances over 6 partitions
And   a steady stream of transactions across all partitions
When  one instance is killed mid-stream
Then  partitions NOT reassigned show NO processing gap
And   reassigned partitions resume within a bounded time
And   no message is lost
```

### Why this test is written the way it is

Spec §10 requires that it be *"actually meaningful, not just checking eventual consistency"*. A
naive version — kill an instance, wait, assert everything eventually processed — **passes under
`RangeAssignor` too**, because eager rebalancing also reaches eventual consistency. It would be
green whether or not `CooperativeStickyAssignor` was ever configured, and would prove nothing
about the setting it claims to verify.

So the test is **parameterized across both assignors**, and the eager run is asserted as an
explicit *control*:

| Assignor | Assertion | Result |
|---|---|---|
| `CooperativeStickyAssignor` | the survivor **retains** partitions nobody asked to move, and those partitions show **no processing gap** | PASS |
| `RangeAssignor` | the **same scenario** revokes **all** of the survivor's partitions | PASS — i.e. the control fires |

If the `RangeAssignor` case ever passed vacuously — no revocations — the two assignors would be
behaving identically in this harness and the cooperative assertion above it would be measuring
nothing. That is why the control is asserted rather than assumed.

The gap is measured by recording a per-partition timeline of processed-message timestamps and
looking for an interval exceeding a threshold on partitions the survivor kept — determined from
`ConsumerRebalanceListener` callbacks, not guessed.

### Scoping the measurement window — two harness bugs worth recording

Both of these made cooperative look identical to eager, i.e. the test reported a real-looking
problem that did not exist. In a test whose entire value is distinguishing the two assignors,
that is the failure mode to guard against:

1. **Revocations accumulated from process start**, so they included the *initial group-formation*
   rebalance — where the first consumer legitimately hands a share of partitions to the joiner
   under **either** assignor. The window is now scoped to the kill.
2. **Closing a consumer fires `onPartitionsRevoked` for everything it holds**, and the list was
   read *after* the survivor's own teardown — measuring our shutdown rather than the rebalance.
   All 6 partitions appeared revoked under cooperative. The snapshot is now taken before shutdown.

**Flakiness control:** rebalance timing is inherently racy, so the gap threshold is deliberately
generous (2.5s). The claim under test is "no stop-the-world", not "sub-second scheduling" — a
tight bound would add flakiness on a loaded machine without adding meaning, and the eager case
stalls for the full rebalance, so the two are far apart.

**Result:** `Tests run: 2, Failures: 0, Errors: 0` against real Kafka
(`apache/kafka-native:4.3.1`, 6 partitions, 2 consumers, steady producer stream).

---

## T6 — Redis outage degrades, does not fail closed or wide open

**Class:** `RedisOutageDegradationIT`

```
Given Redis is paused (Testcontainers pause, not stop — a paused container
      refuses connections the way a partitioned one does)
When  an otherwise-clean transaction is submitted
Then  the request does NOT hang and does NOT error
And   signalsDegraded == true
And   degradedSignalKeys contains velocity_1m, velocity_1h,
      distinct_countries_24h, customers_per_device, is_new_device_high_amt
And   signals map does NOT CONTAIN those keys      ← key ABSENT, not zero
And   is_off_hours_large IS present                ← never degrades, pure computation
And   riskScore is computed from the remaining signals
And   decision == ALLOW for an otherwise-clean transaction
```

**`assertThat(signals).doesNotContainKey("velocity_1m")` is the assertion that matters** — it is
what distinguishes the correct behaviour from the plausible-looking alternative of defaulting
missing signals to `0`. A test asserting only `velocity_1m == 0` would pass on the zero-defaulting
implementation that [ADR-0014](adr/0014-redis-fail-open.md) specifically rejects, and the
distinction is invisible in the resulting decision unless you look for absence.

---

## T7 — Sync-facade timeout path with later reconciliation

**Classes:** `SyncFacadeTimeoutIT` (Phase 4), `WebhookReconciliationIT` (Phase 5)

### Part A — the timeout

```
Given the pipeline is artificially delayed beyond 150ms (test profile hook)
When  a transaction is submitted via mock-payment-api
Then  the HTTP caller receives a response in ~150ms (not on pipeline completion)
And   decision == REVIEW
And   resolvedBy == TIMEOUT_DEFAULT
And   mock-payment-api reports status HELD
```

Asserts an **upper and a lower** bound on the response time (roughly 140–400ms). The upper bound
proves it did not wait for the slow pipeline; the lower bound proves the timeout is real and not
an immediate error path masquerading as a timeout.

### Part B — the reconciliation

```
When  the real pipeline eventually completes (asynchronously, after the response was sent)
Then  decision::{transactionId} exists in Couchbase with the TRUE decision
And   IF the true decision != REVIEW
      THEN mock-payment-api's /webhooks/fraud-decision was called
      And  the payload has previousDecision=REVIEW, finalDecision=<true>
      And  GET /payments/{transactionId} shows reconciledAt set
```

Part B is what makes the timeout default acceptable rather than merely safe: having told a caller
REVIEW, the system owes them the real answer. Asserting on `reconciledAt` via the payment record —
rather than just "a webhook was sent" — proves the reconciliation was actually *applied*.

A second case asserts webhook **idempotency**: delivering the same reconciliation twice leaves
`reconciledAt` unchanged and increments the duplicate counter.

---

## T8 — Hot-reload of a fraud rule with no restart

**Class:** `RuleHotReloadIT`

```
Given scoring-service is running with the seed ruleset
And   a baseline transaction scores X with VELOCITY_1M contributing 30

When  VELOCITY_1M.weight is changed 30 → 50 directly in Couchbase
      (as a fraud-ops actor would — no API, no deploy)
And   POST /admin/rules/refresh is called
Then  WITHOUT any restart, a new equivalent transaction scores X + 20
And   the triggeredRules entry shows contribution == 50
And   rulesetVersion changed

And   separately: with enabled=false, VELOCITY_1M no longer appears in triggeredRules
```

Both refresh mechanisms are exercised: the forced endpoint (fast, deterministic) and the 60s timer
(with the interval overridden to ~2s in the test profile, since a 60-second sleep in a test suite
is unacceptable — the *mechanism* is verified, the production interval is configuration).

The edit is made **directly against Couchbase**, not through an admin API, because that is exactly
how a fraud analyst would do it and it is the path the requirement actually describes.

---

## T9 — Append-only ledger cannot be mutated

**Class:** `AuditLedgerImmutabilityTest` — a plain unit test, no containers

```java
@Test
void ledgerRepositoryExposesNoMutatingMethods() {
    var forbidden = Set.of("update","delete","remove","save","upsert","replace","merge");
    var violations = Arrays.stream(AuditLedgerRepository.class.getMethods())
        .map(Method::getName)
        .filter(n -> forbidden.stream().anyMatch(f -> n.toLowerCase().startsWith(f)))
        .toList();

    assertThat(violations)
        .withFailMessage("""
            AuditLedgerRepository must remain append-only (ADR-0011).
            Found mutating method(s): %s
            Corrections are made by APPENDING a correcting entry, never by editing.
            """, violations)
        .isEmpty();
}
```

Plus a runtime assertion that `append()` on an existing key throws rather than overwriting.

The failure message explains *why* rather than just *what*, because the audience for this failure
is a future contributor — human or agent — who added a `save()` method for a perfectly reasonable
one-off data fix and has no idea it was forbidden.

---

## T10 — Full end-to-end smoke test

**Script:** `scripts/smoke-test.sh` — run against the live Compose stack, not Testcontainers

```
1.  Preflight: disk, RAM, docker compose up, wait for all health checks
2.  Verify all 9 Kafka topics exist with correct partition counts
3.  Verify the 8 seed rules are queryable via N1QL
4.  T1 scenario via curl → assert ALLOW, riskScore < 30, resolvedBy=PIPELINE
5.  T2 scenario via curl → 6 rapid transactions, assert VELOCITY_1M triggered
6.  T3 scenario → duplicate transactionId, assert one record
7.  Correlation check → docker compose logs | grep <correlationId> spans all 7 services
8.  Latency check → curl /actuator/prometheus, extract payment.fraud.latency p50/p99
9.  Print a scenario-by-scenario PASS/FAIL summary
```

Spec §12 is explicit: the build is not done until T1–T10 pass **against the real Compose stack**,
not only against Testcontainers-isolated tests. This script is the difference between "the
components work in isolation" and "the system works", and it is what a human runs to demo it live.

---

## Running

```bash
# unit + integration for one service (needs Docker for Testcontainers)
cd scoring-service && ./mvnw verify

# everything
./scripts/test-all.sh

# end-to-end against a live stack
docker compose up -d && ./scripts/smoke-test.sh
```

## Resource note

Testcontainers spins up real Couchbase (~1.5 GB) and Kafka per test class. On a constrained
machine, containers are shared per class via static fields and reused across classes where safe
(`testcontainers.reuse.enable=true`). Without reuse the suite goes from roughly 4 minutes to 25.
`apache/kafka-native` is used in tests specifically for its faster startup and lower memory
footprint.
