# ADR-0003 — Push-based Redis Pub/Sub wait, and subscribe before ingest

**Status:** Accepted · Spec §6 · **Contains one deliberate correction to the spec**

## Context

gateway-service must return a decision to its synchronous caller within 150ms, while the decision
is produced by an asynchronous Kafka pipeline four services downstream. Something must bridge the
two.

## Decision

gateway-service is built on **Spring WebFlux**. The wait is a `Mono` subscribed to a Redis
Pub/Sub channel `decision:{correlationId}` via `ReactiveRedisMessageListenerContainer`, with
`.timeout(Duration.ofMillis(150))` and `.onErrorReturn(REVIEW)`.

**The subscription is established and confirmed active *before* the call to ingestion-service.**

## Naive alternative A — poll for the result

A loop polling Couchbase or Redis every 20ms until the decision appears.

### Failure mode

Two costs, both structural:

- **Wasted latency on every request.** A 20ms poll interval adds an average of 10ms and a worst
  case of 20ms of pure waiting, on top of whatever the real processing time was. Against a 100ms
  p99 budget that is 10–20% of the entire budget spent doing nothing. Tightening the interval to
  reduce the waste multiplies the load on the store being polled — the waste is not removable,
  only relocatable.
- **A thread per in-flight request.** Under a blocking servlet model, concurrency is capped by
  thread-pool size rather than by actual work. 200 concurrent payments means 200 threads mostly
  asleep.

Push costs sub-millisecond wakeup and, on WebFlux, **zero threads while waiting** — the request is
a suspended `Mono` and the event loop is free.

## Naive alternative B — subscribe after the ingestion call returns

This is what spec §5's prose describes: call ingestion, receive the 202, *then* open the
subscription.

### Failure mode

**A race the gateway loses precisely when the system is healthiest.** Redis Pub/Sub has no
persistence, no replay, and no delivery guarantee — a message published to a channel with zero
subscribers is discarded. On a warm stack the pipeline completes in under 40ms, which is faster
than the gateway can complete an HTTP round trip to ingestion and then establish a subscription.

So `decision-service` publishes into a channel nobody is listening on, the gateway waits the full
150ms, times out, and returns `TIMEOUT_DEFAULT`. **Every request.** The system would appear to be
timing out constantly while in fact being perfectly healthy, and the faster the pipeline got, the
worse it would look.

This is a genuine defect in the spec's prose rather than an ambiguity, so it is corrected here and
called out rather than silently "fixed".

## Consequences

- gateway-service is the only reactive service in the system. That asymmetry is intentional: it is
  the only service that holds many concurrent waits.
- Blocking calls inside the gateway's reactive chain would defeat the entire purpose, so the
  ingestion call uses the reactive `WebClient`, not `RestClient`.
- Pub/Sub delivery is **fire-and-forget by design**, and this is safe because the channel is only
  an optimisation. The authoritative decision is durably in Couchbase *before* the publish, and
  the timeout path plus webhook reconciliation covers every missed message. A missed publish is
  never a lost decision.
- The subscription must be torn down on both the success and timeout paths, or the container leaks
  a channel per timed-out request.

## Verified by

[T1](../TEST_PLAN.md#t1) — decision resolves via the pipeline within the window, asserting
`resolvedBy: PIPELINE` (this is what would fail under alternative B).
[T7](../TEST_PLAN.md#t7) — an artificially delayed pipeline returns at ~150ms, not on pipeline
completion.
