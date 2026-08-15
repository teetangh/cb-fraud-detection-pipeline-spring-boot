# ADR-0010 — Manual offset commits, strictly after the unit of work

**Status:** Accepted · Spec §7, §9.6

## Context

A Kafka consumer's committed offset is a claim: *"everything before this point has been
processed."* When that claim is made relative to when the work actually happens determines whether
a crash loses data or merely repeats it.

## Decision

`enable.auto.commit=false` everywhere. Spring Kafka listener containers run with
`AckMode.MANUAL_IMMEDIATE`, and each listener calls `acknowledgment.acknowledge()` **only after
its own unit of work is durably complete** — the Couchbase write has returned, and/or the
downstream Kafka publish has been acknowledged by the broker.

Per service:

| Service | Commits after |
|---|---|
| enrichment-service | producer ack on `fraud.transactions.enriched` |
| scoring-service | producer ack on `fraud.transactions.scored` |
| decision-service | Couchbase decision write **and** producer ack on `fraud.transactions.decisioned` |
| action-audit-service | Couchbase ledger insert (webhook delivery is retried separately — see below) |

## Naive alternative

Spring Kafka's default auto-commit, which commits offsets on a timer (5s by default).

## Failure mode

**Silent message loss.** Auto-commit fires on a wall-clock timer, entirely decoupled from whether
the work succeeded. The dangerous interleaving:

```
 t=0.0s  poll() returns message M
 t=0.1s  processing begins — Couchbase write in flight
 t=0.2s  auto-commit timer fires → offset committed, M declared done
 t=0.3s  process crashes; the Couchbase write never landed
 ─────── restart: consumer resumes AFTER M. M is never redelivered.
```

The transaction is gone. No error was raised, nothing was retried, no alert fired — the offset
says it was handled. For a fraud pipeline this means a transaction was accepted, recorded, and
**never scored**, with no trace.

Committing after the work inverts this: a crash before the commit causes **redelivery**, not loss.
Redelivery is a problem you can solve (idempotency, [ADR-0006](0006-idempotent-ingestion.md));
silent loss is not, because you never learn it happened.

## Consequences

- **At-least-once delivery**, so every consumer must be idempotent. Decision writes are keyed
  `decision::{transactionId}`; ledger entries are keyed `audit::{transactionId}::{eventType}`.
  Reprocessing overwrites like-for-like or is rejected as a duplicate — never double-counted.
- **The velocity counter is the sharp edge.** It is the one piece of state where reprocessing is
  *not* naturally idempotent: replaying a message would increment it again. This is why the
  counter is incremented in enrichment, *before* the commit, and why the combination of
  `customerId` keying and cooperative rebalancing matters — those minimise duplicate processing to
  genuine crash-recovery cases, where a slightly inflated short-window velocity count is an
  acceptable, fail-safe error (it biases toward caution).
- **Webhook delivery is deliberately outside the commit boundary.** action-audit-service commits
  after the ledger insert, not after the webhook succeeds — an unreachable webhook receiver must
  not block ledger progress or stall the partition. Webhook retries are tracked separately, with
  their own retry state.
- Throughput is marginally lower than auto-commit because commits are more frequent. Irrelevant at
  this scale, and the correct trade at any scale where losing a payment matters.

## Verified by

Consumer restart tests asserting redelivery of uncommitted messages, and
[T3](../TEST_PLAN.md#t3)'s duplicate-suppression assertions proving redelivery is absorbed
without double-processing.
