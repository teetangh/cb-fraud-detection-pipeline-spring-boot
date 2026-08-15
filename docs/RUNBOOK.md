# Runbook — what breaks, how you notice, what you do

Organised by **symptom first**, because that is what you have at 3am. The design rationale behind
each behaviour is in the linked ADR.

## The three dashboards that matter

If you look at nothing else:

| Signal | Query | Healthy | Meaning |
|---|---|---|---|
| **Default-decision rate** | `fraud_gateway_decision_resolved_total{source="TIMEOUT_DEFAULT"} / fraud_gateway_decision_resolved_total` | < 1% | *What fraction of decisions were real decisions?* The system's primary SLO. |
| **Outbox depth** | `fraud_ingestion_outbox_pending` | ≈ 0 | Anything above zero and climbing means Kafka is unreachable or the publisher is dead. Earliest warning available. |
| **Degraded signal rate** | `fraud_enrichment_signal_degraded_total` | 0 | Decisions being made partly blind. |

---

## Symptom: every payment comes back REVIEW

### Check first

```bash
curl -s localhost:8081/actuator/prometheus | grep 'decision_resolved'
```

If `source="TIMEOUT_DEFAULT"` is ~100%, the pipeline is not answering in time or the wakeup is
broken.

### Cause 1 — the pipeline is genuinely slow or stalled

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups
```

Growing `LAG` on a group localises the stall to that stage. Then check that service's health and
logs.

### Cause 2 — Pub/Sub wakeup is broken, pipeline is fine

The tell: `docker compose logs decision-service` shows decisions being made *fast*, and
`decision::{transactionId}` documents exist with sensible `decisionAt` timestamps, but the gateway
still times out.

```bash
# is anyone actually subscribed?
docker compose exec redis redis-cli PUBSUB CHANNELS 'decision:*'
docker compose exec redis redis-cli --timeout 0 PSUBSCRIBE 'decision:*'   # watch live
```

If publishes are visible but the gateway is not subscribed, this is the
**subscribe-before-ingest** race in [ADR-0003](adr/0003-pubsub-not-polling.md). It is the most
confusing failure in the system because **it gets worse as the pipeline gets faster** — a healthy,
fast pipeline publishes before the gateway has subscribed, and everything times out while every
component reports healthy.

Look for `receiveLater()` having been replaced with `receive()`, or the ingestion call having been
moved before the subscription.

### Cause 3 — Redis is down

Gateway logs `Decision timeout, defaulting to REVIEW` alongside connection errors. Payments still
flow (correctly — [ADR-0014](adr/0014-redis-fail-open.md)), but every one is HELD.

---

## Symptom: Redis is down

**Expected behaviour — this is designed, not broken:**

- Payments **keep flowing**. Redis is not on the critical durability path.
- Velocity, geo and device signals are **omitted** (not zeroed) and `signalsDegraded: true`.
- Scoring proceeds on the remaining signals; otherwise-clean transactions lean **ALLOW**.
- Rate limiting fails open — requests are not limited rather than rejected.
- Idempotency still works, because its authority is Couchbase `insert()`, not Redis
  ([ADR-0006](adr/0006-idempotent-ingestion.md)).
- Gateway cannot subscribe, so callers get REVIEW `TIMEOUT_DEFAULT`.

**How you notice**

```bash
curl -s localhost:8083/actuator/prometheus | grep signal_degraded
docker compose logs enrichment-service | grep '"level":"WARN"' | grep degraded
```

**Accepted risk, stated plainly:** velocity-based fraud is more likely to succeed while Redis is
down. Every decision made in this window carries `signalsDegraded: true` on the decision document
and in the ledger, so the affected transactions are identifiable afterwards for re-examination.

**Recovery:** restart Redis. Counters rebuild from live traffic — they are TTL-bounded working
state, not a system of record, so nothing needs restoring. Expect a brief under-detection window
while the 1m/1h windows refill.

```bash
docker compose restart redis
docker compose exec redis redis-cli ping
```

---

## Symptom: Kafka is down or unreachable

**Expected behaviour:**

- Ingestion **still returns 202** — the Couchbase commit is the durability boundary.
- Outbox rows accumulate as `PENDING`. **Nothing is lost.**
- No transaction is scored, so all callers time out to REVIEW.
- On recovery, the publisher drains the backlog automatically.

**How you notice** — the outbox gauge is the leading indicator, well before consumer lag alarms:

```bash
curl -s localhost:8082/actuator/prometheus | grep outbox_pending
```

```sql
SELECT COUNT(*) FROM `fraud-detection`.`transactions`.`outbox` WHERE status = "PENDING";
```

**Recovery:** restart Kafka. Watch the gauge return to ~0. If it does not drain, check the
publisher is enabled (`outbox.publisher.enabled` — the [T4](TEST_PLAN.md#t4) test hook, which must
never be `false` in a real run) and check producer errors in the ingestion logs.

---

## Symptom: Couchbase is down

**This is the one place the system fails closed**, deliberately.

- Ingestion returns **503**. Nothing is accepted that cannot be made durable — returning 202
  without a commit would be a promise the system cannot keep.
- scoring-service keeps serving its **last known good ruleset** and continues scoring.
- decision-service cannot write decisions → no ack → redelivery on recovery.
- action-audit-service cannot append → no ack → redelivery.

**Recovery:** restart Couchbase, wait for health, then verify the bucket survived:

```bash
docker compose restart couchbase
curl -s -u Administrator:password http://localhost:8091/pools/default/buckets/fraud-detection | jq .name
```

In-flight Kafka messages redeliver automatically because their offsets were never committed
([ADR-0010](adr/0010-manual-offset-commits.md)).

---

## Symptom: a consumer group is stuck; lag grows and never drains

Almost always a **poison message** the error handler cannot move aside.

```bash
docker compose logs enrichment-service --tail 200 | grep -i 'deserial\|ErrorHandler\|DLQ'

# is anything landing in the DLQs?
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fraud.transactions.enriched.dlq --from-beginning --max-messages 5
```

If the consumer spins on the same offset and **nothing reaches the DLQ**,
`ErrorHandlingDeserializer` is probably not wrapping the deserializers. Without it a poison message
fails inside `poll()` before any error handler can see it, and the partition is stuck permanently
— a genuine outage rather than a dropped message. See [LLD §6](LLD.md#6-kafka-error-handling-and-the-dlq).

**Manual last resort** — skip a single stuck offset (records what was skipped first):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group fraud-enrichment-group \
  --topic fraud.transactions.enriched:3 --reset-offsets --shift-by 1 --execute
```

Requires the group to be stopped. Skipping means that transaction is never scored — record the
`transactionId` before doing it.

---

## Symptom: a deploy or restart causes a latency spike

**Expected, and bounded.** Cooperative sticky rebalancing means only the partitions actually
transferring pause; the rest keep flowing
([ADR-0009](adr/0009-cooperative-sticky-assignor.md)). Expect a brief bump in
`TIMEOUT_DEFAULT`, not a cliff.

**If the whole group stalls instead**, cooperative rebalancing is not in effect. Verify:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group fraud-enrichment-group --state
```

Every member must use `CooperativeStickyAssignor` — a **mixed** group falls back to eager, so one
misconfigured instance degrades the entire group. This is the most likely cause of "we set the
assignor but it did not help".

---

## Symptom: one customer is flagged on every transaction, forever

The signature of the immortal-counter bug that [ADR-0007](adr/0007-lua-atomic-counters.md) exists
to prevent.

```bash
docker compose exec redis redis-cli TTL velocity:1m:cust-00417
```

`-1` means **the key exists with no TTL** — it will never expire, accumulates forever, and
`VELOCITY_1M` fires on every future transaction from that customer. Nothing in the logs explains
it: the rule is firing correctly against a counter that is lying.

**Immediate fix:**
```bash
docker compose exec redis redis-cli DEL velocity:1m:cust-00417
```

**Then find the real cause** — a TTL-less counter means some code path is issuing `INCR` outside
`velocity.lua`. Audit for direct `redis.increment(...)` calls; every counter mutation must go
through a script.

**Sweep for others:**
```bash
docker compose exec redis redis-cli --scan --pattern 'velocity:*' | \
  while read k; do t=$(docker compose exec -T redis redis-cli TTL "$k"); \
  [ "$t" = "-1" ] && echo "NO TTL: $k"; done
```

---

## Symptom: a rule was edited but nothing changed

```bash
curl -s localhost:8084/admin/rules | jq '.rules[] | {ruleId, weight, enabled, signalKeyValid}'
```

1. **`signalKeyValid: false`** — the rule's `signalKey` is not one enrichment produces, so it can
   **never fire**. A typo like `velocity_1min` silently disables a rule while it looks live in the
   database. With no shared DTO jar there is no compiler to catch this
   ([ADR-0002](adr/0002-no-shared-dto-jar.md)); the `WARN` log and this flag are the only defence.
   Cross-check against the registry in [CONTRACTS.md](CONTRACTS.md#the-signal-key-registry).
2. **Cache not refreshed** — up to 60s by design. Force it:
   `curl -X POST localhost:8084/admin/rules/refresh`
3. **`enabled` is a string** — `"true"` rather than `true` in the document. The N1QL predicate
   `enabled = true` will not match it, and the rule silently vanishes from the ruleset.

---

## Symptom: duplicate transactions in the audit trail

Should be impossible. If it happens, check in this order:

```bash
curl -s localhost:8082/actuator/prometheus | grep duplicate_suppressed
```

```sql
SELECT transactionId, COUNT(*) c FROM `fraud-detection`.`transactions`.`raw-transactions`
GROUP BY transactionId HAVING COUNT(*) > 1;
```

More than one row per `transactionId` means the transaction write is using `upsert()` instead of
`insert()`, which also produces a **second outbox row and a second pipeline event** — which
double-increments the velocity counter and can get a legitimate retrying customer blocked.
[ADR-0006](adr/0006-idempotent-ingestion.md), [T3](TEST_PLAN.md#t3).

---

## Tracing one transaction end to end

The correlation ID is the primary forensic tool, and it works without any tracing backend:

```bash
# every log line across all 7 services, in order
docker compose logs --no-color | grep '5b8e0c1a-7d44-4a90-9c3e-2f61b8d05a77'

# just the decision
docker compose logs decision-service | grep 5b8e0c1a | jq -r '.message'
```

The durable trail:

```sql
SELECT * FROM `fraud-detection`.`transactions`.`raw-transactions` USE KEYS "txn::txn-8f2a…";
SELECT * FROM `fraud-detection`.`audit`.`decisions`            USE KEYS "decision::txn-8f2a…";
SELECT * FROM `fraud-detection`.`audit`.`audit-ledger` WHERE transactionId = "txn-8f2a…";
```

The ledger is append-only, so its entries are the definitive history — they cannot have been
edited ([ADR-0011](adr/0011-append-only-ledger.md)).

---

## Measuring latency for real

```bash
# generate load
for i in $(seq 1 200); do
  curl -s -X POST localhost:8080/payments/initiate \
    -H 'Content-Type: application/json' \
    -d "{\"customerId\":\"cust-$RANDOM\",\"merchantId\":\"merch-1\",\"amount\":100.00,
         \"currency\":\"INR\",\"countryCode\":\"IN\",\"deviceId\":\"dev-$i\",
         \"paymentMethod\":\"CARD\",\"transactionId\":\"txn-load-$i\"}' > /dev/null
done

curl -s localhost:8080/actuator/prometheus | grep payment_fraud_latency
```

**Warm up first.** The first ~50 requests include JIT and connection-pool warmup and will look
terrible. Also note the Compose config sets `-XX:TieredStopAtLevel=1` to save memory, which
disables the C2 compiler — **remove it before quoting p99 numbers**, or you are measuring the JIT
setting rather than the design ([LLD §8](LLD.md#8-resource-footprint)).

---

## Local resource exhaustion

The most common local failure is not a bug — it is the machine.

```bash
./scripts/preflight.sh          # checks free RAM and disk, fails loudly
docker system df
docker stats --no-stream
```

Full stack needs roughly **4.6 GB RAM** and **~4 GB disk** for images.

```bash
docker system prune -a --volumes     # reclaim aggressively (destroys Couchbase data)
docker compose --profile infra up -d # or run a subset
```

If Couchbase will not start, it is almost always the memory quota exceeding what the container was
given. If Kafka will not start on a single node, check that
`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` and
`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1` are set — the defaults of 3 cannot be satisfied
by one broker and the broker fails to create its internal topics
([ADR-0013](adr/0013-couchbase-ce-single-node-kraft.md)).
