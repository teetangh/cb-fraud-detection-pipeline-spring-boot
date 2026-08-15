# ADR-0015 — Two stores, and which counters live where

**Status:** Accepted · Spec §3 ("the Java SDK's Binary Collection counter API… **are all used**")

## Context

The system runs both Redis and Couchbase. That invites a fair challenge: **Couchbase can do almost
everything Redis is doing here** — why operate two stores?

Checked against the real SDK rather than assumed:

```java
BinaryCollection.increment(String id, IncrementOptions options)
IncrementOptions.delta(long).initial(long).expiry(Duration)
```

`increment` with `initial` and `expiry` is an **atomic increment that sets both the starting value
and the TTL on creation**. It solves natively — with no Lua — the exact race
[ADR-0007](0007-lua-atomic-counters.md) exists to prevent: the `INCR`-then-`EXPIRE` interleaving
that leaves an immortal counter and silently flags a customer forever.

Nearly every Redis usage here has a direct Couchbase equivalent:

| Redis usage | Couchbase equivalent |
|---|---|
| `velocity.lua` | `binary().increment(k, initial(1).expiry(60s))` |
| `set_add_count.lua` | `MutateInSpec.arrayAddUnique()` + expiry |
| `known_device.lua` | `arrayAddUnique` → `PathExistsException` **is** an atomic test-and-add |
| `idempotency:{txnId}` | KV insert with TTL |
| rate limiting | binary counter with expiry |

So the challenge is real, and "we use Redis because it's fast" is not an answer.

## Decision

Keep both stores, with a principled split:

- **Redis — ephemeral, TTL-bounded, hot-path working state.** Velocity windows, geo/device sets,
  idempotency cache, rate limits. All disposable: losing them costs detection quality for one TTL
  window, never correctness.
- **Couchbase — durable records and durable aggregates.** Transactions, outbox, rules, policy,
  decisions, the audit ledger, **and lifetime counters via the Binary Collection API**.

### Redis earns its place for one capability and one property

**The capability: Pub/Sub.** A cheap, ephemeral, per-`correlationId` channel that a suspended HTTP
request subscribes to and tears down 40ms later. Couchbase has no equivalent. DCP is a low-level
replication stream, not a request-scoped channel. Eventing Functions are Enterprise-only and are
mutation-triggered, not subscriber-shaped. **The sync facade — the core of this design — has no
Couchbase-native implementation.**

**The property: blast-radius asymmetry.** From the failure-mode table:

> Couchbase down → ingestion **fails closed**, 503.
> Redis down → signals degrade, **fails open**, payments keep flowing.

That asymmetry only exists *because they are separate systems*. Collapse them into one cluster and
it is gone: you cannot fail open on signals and closed on durability when one outage takes out
both. You would be forced to fail closed on everything — so a hiccup in a disposable, TTL-bounded
counter store would **stop all payments**. The availability posture in
[ADR-0014](0014-redis-fail-open.md) is only implementable with two failure domains.

Secondarily: velocity counters are the highest-write, most disposable data in the system; the
audit ledger is permanent and regulatory. One cluster means a velocity spike contends for memory
quota and I/O with the compliance record.

### Couchbase gets the durable counter, and it fixes a real false-positive

`lifetime_txn_count` — a per-customer transaction counter, incremented via
`binary().increment("counter::txn::{customerId}", initial(1))`, no expiry. Durable, survives a
Redis flush, and lives with the customer profile where it belongs.

This is not a token use of the API to satisfy the spec. It closes a genuine defect in
`AMOUNT_DEVIATION`:

> `amount_vs_p90_ratio > 3.0` fires `AMOUNT_DEVIATION` at weight 20. But **p90 over 2
> transactions is noise.** A customer whose first purchase was ₹100 and whose second is ₹400 shows
> a ratio of ~4.0 and trips the rule — on entirely normal behaviour. Every new customer making a
> slightly larger second purchase gets flagged.

So `amount_vs_p90_ratio` returns a neutral `1.0` unless `lifetime_txn_count >= 10`
(`enrichment.p90-min-history`, default 10). The signal is only emitted once it means something.

This is the correct home for the counter regardless of the spec requirement: it must survive a
Redis flush (a customer's lifetime history is not disposable), it is written once per transaction
rather than on a hot window, and it is read alongside the profile document that already lives in
Couchbase.

## Naive alternatives

**A — Use only Couchbase.** Loses Pub/Sub, so the sync facade regresses to polling — 10ms average
waste per request ([ADR-0003](0003-pubsub-not-polling.md)) — and loses the fail-open/fail-closed
asymmetry, so a counter-store hiccup stops all payments.

**B — Use only Redis.** Not viable at all: no multi-document ACID transactions, so no outbox
([ADR-0005](0005-transactional-outbox.md)), so no durability guarantee.

**C — Put lifetime counters in Redis too.** They would be lost on a flush or eviction, and
`maxmemory-policy allkeys-lru` will evict them under pressure. A customer's lifetime history
silently resetting to zero would re-enable the `AMOUNT_DEVIATION` false positive above, and
`signalsDegraded` would not be set because Redis was *up* — it just forgot. A silent wrong answer
is worse than a loud missing one.

## Consequences

- **Two counter mechanisms, with a defensible boundary**: ephemeral hot-path windows in Redis via
  Lua; durable lifetime aggregates in Couchbase via the Binary Collection API. The boundary is
  "does losing this silently produce a wrong answer?" — if yes, Couchbase.
- Binary counters are **not** transactional — `increment` cannot participate in
  `cluster.transactions().run(...)`. So the lifetime counter is incremented in enrichment
  alongside the other signals, not inside ingestion's ACID transaction.
- **Same at-least-once caveat as velocity**: a redelivered message re-increments the lifetime
  counter. The error is small, bounded by genuine crash-recovery cases, and self-correcting in
  direction (a slightly high lifetime count makes the p90 guard *more* permissive, i.e. it lets a
  real signal through sooner — the safe direction).
- Redis remains genuinely optional for correctness: with it down, `lifetime_txn_count` and
  `amount_vs_p90_ratio` still work, because they are Couchbase-backed.

## Verified by

The lifetime counter's atomicity is covered by the same concurrency-hammer approach as
[T2](../TEST_PLAN.md#t2)'s Lua test: N threads incrementing, asserting the final value is exactly
N. A dedicated test asserts `amount_vs_p90_ratio == 1.0` below the history threshold and a real
ratio above it, which is the assertion that would catch the false positive regressing.
