# ADR-0005 — Transactional outbox, not dual-write

**Status:** Accepted · Spec §9.2

## Context

ingestion-service must do two things when it accepts a transaction: record it durably in
Couchbase, and get it onto `fraud.transactions.raw` so the pipeline can see it. These are two
different systems. There is no distributed transaction spanning Couchbase and Kafka.

## Decision

Write the transaction record **and** an outbox record in a **single Couchbase multi-document ACID
transaction**, using the Couchbase Java SDK's `cluster.transactions().run(...)` directly.

A separate background `OutboxPublisher` polls for `status = "PENDING"` outbox rows, publishes each
to Kafka, and marks it `PUBLISHED` **only after the producer acknowledges**.

Spring Data Couchbase is not used for this — it does not expose multi-document transactions. The
raw SDK is used throughout, which has the side benefit of removing Spring Data from the dependency
graph entirely.

## Naive alternative

Write to Couchbase, then publish to Kafka, as two sequential unrelated operations:

```java
couchbase.insert(txn);        // ok
kafka.send(event);            // ← crash here
```

## Failure mode

**A transaction that is durably recorded and never scored. Silently.**

If the process dies between the two calls — deploy, OOM, node loss — the transaction exists in
Couchbase, the caller was told it was accepted, and the fraud pipeline never sees it. There is no
error, no retry, no alert. The transaction simply never gets checked for fraud, and the only way
to discover it is to reconcile the transaction table against Kafka after the fact, which nobody
does.

This is the worst class of bug in the system: it violates the "no transaction may be silently
lost" requirement in a way that produces no signal at all.

Reversing the order does not help — publishing first then writing means a crash produces a Kafka
event for a transaction that does not exist, and the pipeline scores a phantom.

## Consequences

- **The commit is the durability boundary.** Once `cluster.transactions().run(...)` returns, the
  transaction cannot be lost — only delayed. Everything downstream is recoverable.
- **At-least-once, not exactly-once.** A crash after the Kafka ack but before the `PUBLISHED`
  update re-publishes on restart. This is correct and intentional: duplicates are absorbed by the
  idempotency guards ([ADR-0006](0006-idempotent-ingestion.md)) and by consumers keyed on
  `transactionId`. Chasing exactly-once here would add cost for a problem already solved
  downstream.
- **The pending-outbox depth is a first-class health metric.** It should hover near zero. A
  monotonically climbing gauge means Kafka is unreachable or the publisher is dead — and it is the
  earliest available signal of either.
- The publisher's poll is the highest-QPS query in the system, which is why
  `idx_outbox_pending` is a **partial** index (`WHERE status = "PENDING"`) — its cost tracks the
  pending backlog, not the total transaction history.
- A post-commit nudge wakes the publisher immediately rather than waiting for the next 200ms tick,
  so the outbox contributes ~0ms to the hot path in the normal case while the timer remains the
  correctness backstop.
- **Two publish paths mean the row must be *claimed*, not merely checked** (issue #12). The nudge
  and the scheduled poll can both reach the same row, and both can truthfully observe it as
  `PENDING` because neither has acked yet. A re-read before publishing narrows that window without
  closing it — it is check-then-act. The claim is a CAS-guarded `claimedAt` stamp, so exactly one
  publisher proceeds and the loser is rejected with `CasMismatchException`.

  This is the one place where at-least-once is *not* good enough. Enrichment's velocity counter is
  not idempotent under redelivery, so a systematic double-publish would double every customer's
  velocity — inflating precisely the signal the fraud rules key on. Absorbing an occasional
  duplicate is fine; manufacturing one on every transaction is not.

  The claim deliberately leaves `status` as `PENDING`, so a publisher that dies mid-publish leaves
  a row `findPending` still returns and whose claim simply expires. Crash recovery falls out of the
  design rather than needing a reaper and a second index for a state rows hold for milliseconds.

## Verified by

[T4](../TEST_PLAN.md#t4) — a test hook stops the publisher immediately after the Couchbase commit
and before the Kafka publish. Asserts the outbox row remains `PENDING`, and that restarting the
publisher picks it up and publishes it. No transaction dropped, only delayed.
