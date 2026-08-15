# Interview Preparation

How to talk about this system. Ordered by what actually gets asked.

---

## 0. The 90-second version

> "It's a real-time fraud detection pipeline — seven Spring Boot services. A payment comes in and
> has to get back ALLOW, REVIEW or BLOCK before it can complete, within 150 milliseconds.
>
> The interesting tension is that the caller needs a **synchronous** answer, but the pipeline
> wants to be **asynchronous** internally — Kafka between every stage — for failure isolation and
> replay. So I built a sync facade: one fast synchronous leg to durably accept the transaction,
> then the gateway opens a **push-based** subscription on a Redis Pub/Sub channel keyed by
> correlation ID and waits, non-blocking, on WebFlux. Decision service publishes there the moment
> it decides. Sub-millisecond wakeup, and no thread is held while waiting.
>
> If 150ms elapses first, it returns **REVIEW — never ALLOW** — and the pipeline keeps running.
> The real decision still gets written, and if it disagrees, a webhook reconciles.
>
> The durability guarantee is a **transactional outbox**: the transaction record and its outbox
> event are written in one Couchbase multi-document ACID transaction, and a background publisher
> pushes to Kafka and marks it published only after the broker acks. So there's no window where a
> transaction is accepted but never scored."

Then stop. That's three hooks — sync facade, fail-safe default, outbox — and they'll pick one.

---

## 1. Lead with these three

Ranked by how few candidates can discuss them well.

### #1 — Outbox instead of two-phase commit

**Very likely asked as:** *"You write to Couchbase and publish to Kafka. How do you keep them
consistent?"*

> "You can't — not with a distributed transaction. There's no XA across Couchbase and Kafka;
> Kafka has no XA support, and 2PC needs a coordinator that blocks everything if it dies mid-round.
>
> So I reduce it to a problem I *can* solve atomically: write the transaction record **and** an
> outbox row in **one local ACID transaction** in Couchbase. After that commit, either both exist
> or neither does. Then a background publisher reads PENDING rows, publishes to Kafka, and marks
> PUBLISHED only after the producer ack.
>
> The failure mode I'm avoiding is specific and nasty: with a naive dual-write, if you crash
> between the Couchbase write and the Kafka send, the transaction is durably recorded and the
> pipeline **never sees it**. No error, no retry, no alert — it silently never gets checked for
> fraud. You'd only find it by reconciling the transaction table against Kafka, which nobody does.
>
> The cost is at-least-once: crash after the ack but before marking PUBLISHED and you republish.
> That's fine — consumers are idempotent, keyed on transactionId."

**Follow-up: "Why not Debezium / CDC?"** Reasonable and arguably better at scale — tail the
database's change log instead of polling. Rejected here because it adds Kafka Connect plus a
connector to operate, and Couchbase's eventing/CDC story pulls in Enterprise features. The polling
publisher is ~40 lines with a partial index on `status = "PENDING"`. At this scale, correct and
boring wins.

**Follow-up: "Why not just publish to Kafka first, then write?"** Then a crash between them
produces a Kafka event for a transaction that doesn't exist, and the pipeline scores a phantom. It
moves the bug, it doesn't fix it.

### #2 — The bounded, push-based wait

**Asked as:** *"How does a synchronous caller get an answer from an async pipeline?"*

Draw it. Then give the latency math, because that's what separates a real answer from a
memorised one:

> "The obvious approach is to poll — check Couchbase every 20ms until the decision shows up. That
> costs you **an average of 10ms and a worst case of 20ms of pure waiting on every single
> request**, on top of real processing time. Against a 100ms p99 budget that's 10–20% of the
> entire budget spent doing nothing. And tightening the interval doesn't remove the waste, it just
> converts it into load on the thing you're polling.
>
> Push costs sub-millisecond wakeup. And on WebFlux the waiting request holds **no thread** — it's
> a suspended Mono, so concurrency is bounded by real work rather than by thread-pool size."

**The detail that shows you actually built it:**

> "There's an ordering trap. Redis Pub/Sub has no persistence — publish to a channel with no
> subscriber and the message is just gone. On a warm stack my pipeline completes in under 40ms,
> which is faster than the gateway can call ingestion and *then* subscribe. So you have to
> **subscribe before you trigger ingestion**, and you need `receiveLater()` rather than
> `receive()`, because `receiveLater()` returns a Mono that completes when the subscription is
> confirmed active.
>
> Get it backwards and every request times out — and the failure gets **worse the faster your
> pipeline is**, which is a genuinely confusing thing to debug. The spec I worked from actually
> had the ordering wrong; I caught it and wrote it up."

That last point is worth making. Finding a real bug in a spec is a strong signal.

**Follow-up: "Why not WebSockets / SSE / long polling?"** Those are for pushing to an *external*
client. This is internal service-to-service where the caller is already blocked on an HTTP
request. Redis Pub/Sub is already a dependency and adds no new infrastructure.

**Follow-up: "What if the gateway has multiple instances?"** It works — Pub/Sub broadcasts to all
subscribers, and only the instance holding that `correlationId`'s request has a subscription on
that channel. This is exactly why the channel is keyed by correlation ID rather than being one
shared topic.

### #3 — Cooperative sticky rebalancing, with a test that proves it

**Asked as:** *"What happens during a deploy?"* or *"Tell me about consumer rebalancing."*

> "Every consumer group explicitly sets `CooperativeStickyAssignor`, not the default.
>
> With eager rebalancing, **any** membership change revokes **all** partitions from **all** members
> — the whole group stops, including partitions nobody was going to move. During a rolling deploy
> of three instances that's six full-group stalls. And each stalled partition means transactions
> sitting unscored while the caller's 150ms budget burns, so a routine deploy converts directly
> into a spike of held payments.
>
> Cooperative only revokes the partitions actually transferring. Everything else keeps flowing."

**Then the part that lands:**

> "The test for this is parameterized across **both** assignors, and it asserts the scenario
> *fails* under `RangeAssignor`. Because the naive version — kill an instance, wait, assert
> everything eventually processed — passes under both. Eager rebalancing also reaches eventual
> consistency. A test that passes whether or not the setting does anything proves nothing about
> the setting."

Very few candidates have written a test that has to fail in one configuration to be meaningful.

**Follow-up: "Gotchas?"** All members must agree — a **mixed** group falls back to eager, so one
misconfigured instance degrades the whole group. And migrating a live eager group needs a
two-phase rollout: add cooperative as a second strategy, deploy, then remove eager.

---

## 2. Trade-off drills

### Why REVIEW and not ALLOW on timeout?

> "Because ALLOW-on-timeout means every degradation becomes an attack window. A GC pause, a
> rebalance, a Redis blip — each silently becomes 'let everything through'. And load spikes and
> fraud waves are **correlated**, so it fails open exactly when you need it most.
>
> Worse, it's self-concealing. No errors, no failed payments, no complaints. The only graph that
> would show it is the one nobody built. Someone who can add 50ms of latency has a blanket
> approval channel.
>
> REVIEW maps to HELD, which is recoverable. A fraudulent completed payment often isn't. That
> asymmetry is the whole argument — and I export `resolvedBy` as a metric so the default can't
> fire quietly."

### Why are scoring and decision separate services?

The interviewer is testing whether you over-engineer. Lead with the audit argument, not the
scaling one:

> "A regulator asks: *why was this transaction blocked in March but an identical one allowed in
> May?* If scoring and decision are fused there's one version number covering both, and the honest
> answer is 'something changed and we can't tell you what.'
>
> Split, the decision record carries `rulesetVersion` and `policyVersion` independently, so the
> answer is precise: the model was identical, the policy threshold moved on April 2nd. You cannot
> reconstruct that after the fact — it has to be designed in before the decisions are written,
> because they're immutable once written.
>
> The operational argument is secondary: raising a BLOCK cutoff for a promo weekend is a business
> decision on a business timescale. Fused, it inherits the full engineering release cycle."

**Be ready to concede:** "It costs one Kafka hop, about 14ms. At 69ms end-to-end there's room. If
the budget got tight, collapsing these two is the first thing I'd consider — and I'd be explicit
that I was trading away audit granularity, not getting a free win."

Volunteering the cost is what makes it sound like a decision rather than a preference.

### Why Lua scripts for Redis?

> "`INCR` then `EXPIRE` as two calls is a check-then-act race. If you crash or get descheduled
> between them, the key exists with **no TTL at all**. It's immortal. It accumulates every future
> transaction from that customer, so `velocity_1m` climbs past the threshold and stays there — and
> the velocity rule fires on **every subsequent transaction they ever make**.
>
> The customer is silently held on every payment, and nothing in the logs explains it, because the
> rule is firing correctly against a counter that's lying. Diagnosing it means noticing one key
> has `TTL -1`, which nobody thinks to check.
>
> One Lua script, executed server-side, atomic."

**The pairing that gets missed — say it before they ask:**

> "Atomicity alone isn't enough. Lua stops two *operations* interleaving; it doesn't stop two
> *consumer instances* processing the same customer at once. That's what the `customerId`
> partition key is for — one customer's events land on one partition, consumed by one instance in
> order. You need both. Neither is sufficient alone."

### Why omit degraded signals instead of defaulting to zero?

The most subtle point in the design and a genuine differentiator:

> "`velocity_1m: 0` isn't 'unknown' — it's a positive claim that the customer has been quiet,
> which is the strongest *exonerating* statement you can make about them. During a Redis outage
> you'd be manufacturing exculpatory evidence for every transaction, and because the values look
> like real measurements, it's **indistinguishable from a genuine quiet period** in the stored
> record.
>
> An auditor reviewing a transaction that got through would see zero velocity, zero country
> diversity, zero shared devices — all false, all recorded as fact, nothing flagging them as
> fabricated. That corrupts the audit trail's *truthfulness*, not just its completeness.
>
> Omitting is honest: the rule can't be evaluated, so it doesn't fire, and `signalsDegraded: true`
> is carried onto the decision and into the ledger. The test asserts the key is **absent**, not
> that it's zero — otherwise it'd pass on the implementation I'm rejecting."

### "You already have Couchbase. Why also run Redis?"

A fair attack, and "Redis is faster" is not an answer — Couchbase can do almost everything Redis is
doing here. Know the API well enough to concede that first:

> "Most of what I put in Redis, Couchbase does natively. `binary().increment(key,
> initial(1).expiry(60s))` is an atomic increment that sets the value *and* the TTL on creation —
> it solves the same `INCR`-then-`EXPIRE` race my Lua script does, without Lua. Sub-document
> `arrayAddUnique` gives me atomic test-and-add for the device sets. So the honest answer isn't
> 'Redis is faster.'
>
> Redis is there for **one capability and one property**.
>
> The capability is **Pub/Sub** — a cheap, ephemeral, per-correlation-ID channel that a suspended
> request subscribes to and tears down 40ms later. Couchbase has no equivalent: DCP is a
> replication stream, not a request-scoped channel, and Eventing is Enterprise-only and
> mutation-triggered. The sync facade — the best part of this design — has no Couchbase-native
> implementation.
>
> The property is **blast-radius asymmetry**. Couchbase down fails closed: 503, reject. Redis down
> fails open: signals degrade, payments flow. That only works *because they're separate failure
> domains*. Collapse them and you're forced to fail closed on everything — so a hiccup in a
> disposable TTL-bounded counter store stops all payments."

**Then show you actually split the data on a principle, not by habit:**

> "The rule is: does losing this silently produce a *wrong* answer? Velocity windows are
> disposable — lose them and you set `signalsDegraded`, loudly. But `lifetime_txn_count` is a
> durable Couchbase binary counter, because Redis runs `allkeys-lru` and would evict it under
> pressure, resetting a customer's lifetime history to zero **with `signalsDegraded` unset** —
> Redis was up, it just forgot. A silent wrong answer is worse than a loud missing one."

**And the false positive it prevents** — this is the part that shows judgement rather than recall:

> "That counter exists because `AMOUNT_DEVIATION` fires on `amount_vs_p90_ratio > 3.0`, and p90
> over two transactions is noise. First purchase ₹100, second ₹400 — ratio 4.0, rule fires, on
> completely normal behaviour. Every new customer with a slightly larger second purchase gets
> flagged. So the ratio stays neutral at 1.0 until they've got at least 10 transactions."

### Why no shared DTO jar?

> "It'd make the contracts compile-time safe, and it turns seven independently deployable services
> into a distributed monolith. Add one field and you rebuild and redeploy every producer and
> consumer in lockstep — otherwise you get `NoSuchMethodError` at runtime.
>
> It works fine right up until two services need to deploy on different schedules, which is the
> entire reason they're separate services.
>
> I pay for it with tolerant readers — unknown fields ignored, never fatal — which is what makes
> additive change safe one service at a time. Plus JSON Schema validated in each service's own
> tests, and end-to-end tests that catch cross-service drift."

**Be ready to concede:** "The real loss is that a typo in a signal key is a runtime bug, not a
compile error — a rule referencing `velocity_1min` never fires and looks live in the database. So
scoring logs a WARN on every evaluation for an unknown signal key and exposes it on an admin
endpoint. Turning a silent misconfiguration into a loud one is the only defence once you've given
up the compiler."

---

## 3. Patterns present, and pointedly absent

### Present

Outbox · sync facade over async backbone · idempotent receiver (two-layer) · fail-safe default ·
cooperative sticky rebalancing · manual post-work offset commit · partition-key-as-ordering ·
DLQ with opaque payload · append-only ledger enforced by the type system · policy/model separation ·
server-side atomicity via Lua · hot-reloadable config · graceful degradation · tolerant reader ·
correlation ID propagation.

### Absent — and this is the better material

| Pattern | Why not |
|---|---|
| **2PC / XA** | Never. The whole point of the outbox. No XA across Couchbase and Kafka; 2PC blocks on coordinator failure. |
| **Saga** | Nothing to compensate — this is a read-only *decision* flow, not multi-service money movement. If pressed on where one would go: the downstream capture/reversal that mock-payment-api stands in for. Webhook reconciliation is a compensating *notification*, not a saga step. |
| **Service discovery** | Compose DNS. Seven services on one host — Eureka/Consul is infrastructure to operate for zero benefit. In k8s it's Services + DNS, still no Eureka. |
| **Exactly-once (Kafka transactions)** | At-least-once plus idempotent consumers is cheaper and more available. EOS costs throughput and only holds *within* Kafka — it never covered the Couchbase write, which is the part that matters. |
| **Event sourcing / CQRS** | Partial: append-only ledger and Kafka as a real log, but state isn't rebuilt from events. |

### Name this gap before they do

> "There's no **circuit breaker** on gateway → ingestion. Redis fail-open is graceful degradation,
> not a breaker, and the 150ms timeout is a crude bulkhead at best. If ingestion got slow rather
> than dead, the gateway would keep hammering it with requests that were already doomed to time
> out. I'd add Resilience4j there — that's the first thing I'd fix."

Naming your own gap, unprompted, with the specific fix, is worth more than any pattern you *did*
implement.

---

## 4. Failure-mode rapid fire

| Question | Answer |
|---|---|
| Redis dies? | Payments flow. Signals omitted, `signalsDegraded: true`, decisions lean ALLOW. Rate limiting fails open. Idempotency survives — its authority is Couchbase `insert()`. Accepted risk, flagged on every affected record. |
| Kafka dies? | Ingestion still returns 202. Outbox accumulates PENDING. **Nothing lost.** Drains on recovery. Everyone times out to REVIEW meanwhile. |
| Couchbase dies? | **Only place it fails closed** — 503, reject. Accepting without durability is a promise you can't keep. |
| Consumer crashes mid-message? | Offset uncommitted → redelivery. Idempotent consumers absorb it. |
| Duplicate client retry? | Redis cache hit, or `DocumentExistsException` → identical response. |
| Whole pipeline slow? | REVIEW + `TIMEOUT_DEFAULT`, real decisions still land, webhook reconciles. |
| Poison message? | 3 retries → DLQ with the payload as an **opaque string** (you can't re-parse what failed to parse). Offset committed — moving it aside *is* success. |
| How do you debug one transaction? | `docker compose logs \| grep <correlationId>` — every line, all seven services, structured JSON with the ID in MDC. |

**The one that catches people:** *"What's not idempotent?"*

> "The velocity counter. Replaying a message re-increments it — that's the sharp edge of
> at-least-once here. Partition keying plus cooperative rebalancing reduce duplicates to genuine
> crash recovery, where a briefly inflated count biases toward **caution**, which is a fail-safe
> error. I could make it exactly-once with a dedupe set inside the same Lua script; I chose not to
> because it doubles Redis state for a rare, self-correcting, safe-direction error."

Knowing precisely which of your guarantees is weakest, and why you accepted it, is the answer.

---

## 5. Numbers to have ready

| | |
|---|---|
| Services / topics / partitions | 7 / 9 (6 + 3 DLQ) / 6 main, 3 alerts+DLQ |
| Latency budget | ~69ms typical, 150ms hard cap, 100ms p99 target |
| Dominant costs | 2 Couchbase writes (~22ms), 4 Kafka hops (~25ms) |
| Poll waste avoided | 10ms average, 20ms worst case, **per request** |
| Score thresholds | ALLOW < 30 ≤ REVIEW ≤ 69 < BLOCK |
| Seed rules | 8, weights summing to 175, **capped at 100** |
| Retention | 7d pipeline, 30d decisioned/actioned/DLQ, 1d alerts |
| Footprint | ~4.6 GB RAM full stack |

**"Why 6 partitions?"** — Caps consumer parallelism at 6 per group. Divides evenly by 1, 2, 3 and
6, so common scaling steps produce balanced assignments. And it's effectively permanent: for a
stateful keyed topic, repartitioning breaks velocity continuity mid-flight.

**"Why cap the score at 100?"** — The eight rules sum to 175. Without the cap, a heavily-flagged
transaction scores outside the documented 0–100 range and the policy comparison breaks.

---

## 6. Scaling — "now do 50,000 TPS"

Think out loud in this order:

1. **What breaks first?** Not the services — they're stateless and scale horizontally. It's
   **partition count**: 6 partitions caps you at 6 consumers per group. And repartitioning a
   stateful keyed topic breaks velocity continuity, so it's a migration, not a config change.
2. **Redis** becomes the hot spot. Cluster it — keys are already customer/device-scoped so they
   shard naturally. Watch for the Lua-scripts-must-be-single-slot constraint in cluster mode.
3. **Couchbase**: the outbox poll is the highest-QPS query. The partial index keeps it
   proportional to the backlog, not to total history. Then scale data/index nodes separately.
4. **Kafka**: multi-broker, RF=3, `min.insync.replicas=2`, `acks=all`, rack-aware.
5. **Only then** talk about the app.

**The senior move:** "I'd want to know the *shape* first. 50k TPS spread over millions of
customers is easy — it shards cleanly. 50k TPS where 10% is one merchant on a Black Friday is a
hot-partition problem, and that changes the answer completely."

---

## 7. Questions worth asking them

- "Where does the fraud team sit relative to engineering, and how do they currently change a
  threshold?" — probes whether hot-reload is real for them.
- "What's your current p99 on the decision path, and what fraction of decisions hit a default?"
- "How do you distinguish a model change from a policy change when auditing a historical
  decision?" — if they can't, you've just described the problem your design solves.
- "What's your poison-message story?"

---

## 8. Traps

| Trap | Do this instead |
|---|---|
| "It's microservices, so it scales" | Name what breaks first: partition count. |
| Claiming exactly-once | At-least-once + idempotent consumers. Say why EOS wasn't worth it. |
| Saying "we use Kafka for reliability" | Kafka is transport. Durability is the Couchbase commit. |
| Over-claiming the local setup | "Single broker, RF=1 — a fake 3-broker Compose setup would test nothing, since they'd share one disk and one failure domain." |
| Reciting patterns | Every pattern here has a *named failure mode* it prevents. Lead with the failure. |
| Defending everything | Volunteer the circuit-breaker gap and the velocity-counter non-idempotency. |

---

## 9. Whiteboard order

1. Two boxes: caller, pipeline. **"Sync caller, async internals — that's the tension."**
2. The seven services and five topics, left to right.
3. **Zoom into the wait.** Draw subscribe → ingest → publish → wake. Mark the 150ms.
4. **Zoom into the outbox.** Draw the one ACID transaction around two documents.
5. Only then: Redis keys, rules, policy.

Steps 3 and 4 are the interview. Everything else is context.
