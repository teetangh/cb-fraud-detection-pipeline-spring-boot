# ADR-0006 — Two-layer idempotency, with `insert()` as the authority

**Status:** Accepted · Spec §9.1

## Context

`transactionId` is client-supplied and doubles as the idempotency key. Clients retry after
ambiguous network timeouts — the request may or may not have been processed, and the client
cannot tell. Retries are therefore normal traffic, not an error condition.

## Decision

Two layers, in this order:

1. **Redis `idempotency:{transactionId}`** — checked first. On a hit, return the cached response
   without reprocessing. TTL 24h. This is an **optimisation**.
2. **Couchbase `insert()`, never `upsert()`** — the transaction document write uses `insert()`,
   which throws `DocumentExistsException` on a duplicate key. On that exception, read the existing
   record and return the same response. This is the **authority**.

## Naive alternative

Use `upsert()` for the transaction write, and rely on the Redis check to catch duplicates.

## Failure mode

Two distinct failures, and the second is worse:

**1. The original record is silently overwritten.** `upsert()` succeeds on a duplicate key,
replacing the first record's `createdAt`, `correlationId`, and any state it had accumulated. The
audit trail now shows the retry, not the original.

**2. A second Kafka event is published for one transaction.** This is the serious one. The outbox
row is written inside the same transaction, so an overwriting duplicate write produces a *second*
outbox row and therefore a second `fraud.transactions.raw` event. That event is enriched
independently — and **increments the velocity counter a second time**. A customer who retried once
due to flaky wifi now looks like they transacted twice in a minute. Enough retries and a
legitimate customer trips the velocity rule and gets blocked, for the offence of having a poor
network connection.

**Why Redis alone is insufficient.** Redis is a cache: it can be down, flushed, or evicted. More
fundamentally, two genuinely concurrent duplicate requests can *both* miss the cache before either
writes it — a check-then-act race that no TTL setting fixes. Redis narrows the window; it cannot
close it. `insert()` closes it, because uniqueness is enforced by the store that owns the data, at
the moment of the write.

## Consequences

- Layer 1 handles the common case cheaply (sub-millisecond, no Couchbase round trip).
- Layer 2 is correct under Redis loss, cache flush, and true concurrency. Correctness never
  depends on the cache being up — which is what makes Redis genuinely optional here, consistent
  with [ADR-0014](0014-redis-fail-open.md).
- `DocumentExistsException` is **normal control flow**, not an error. It is logged at DEBUG and
  counted, never logged at ERROR — a duplicate-suppression counter is a health signal, an ERROR
  log is noise that trains people to ignore logs.
- Both duplicate callers receive the same **decision-relevant** fields — `transactionId` and
  `correlationId` — so the identity they act on is identical and both will be woken by the same
  `decision:{correlationId}` publication.

  The `status` field deliberately differs: the winner gets `ACCEPTED`, the loser `DUPLICATE`.
  That is not a violation of §9.1. §9.1 is about not *reprocessing* a duplicate and not emitting a
  second Kafka event; T3 asserts exactly one `ACCEPTED` and one `DUPLICATE` precisely because
  distinguishing them is useful — it is what makes duplicate suppression observable at the caller
  rather than only in a counter. The fraud *decision* — the thing spec §10 T3 requires both callers
  to receive identically — does not exist yet at ingestion time; it arrives later via the gateway's
  Pub/Sub wait, keyed by the `correlationId` both callers share.

## Verified by

[T3](../TEST_PLAN.md#t3) — two concurrent requests with an identical `transactionId`. Asserts
exactly one document in Couchbase, exactly one Kafka event on `fraud.transactions.raw`, and
identical responses to both callers. The test drives both requests from two threads against the
live service specifically so it exercises the race, not just the sequential path.
