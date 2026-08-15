# Conformance — spec §9 and §14, audited against the code

Spec §14 makes two claims that are easy to *assert* and hard to *verify*: that all of T1–T10
pass, and that **"every non-negotiable requirement in Section 9 is implemented as described,
not approximated."**

This document is the audit behind the second claim. Each requirement is checked against the
actual call site, not against intent or memory. File and line references are given so the check
is repeatable rather than something you have to take on trust.

Where a requirement is satisfied by a mechanism *other* than the literal one it names, that is
called out explicitly rather than quietly counted as a pass — see [9.3](#93--atomic-velocity-counters).

---

## §14 Definition of Done

| # | Requirement | Status |
|---|---|---|
| 1 | `docker compose up` brings up all 7 services + infra, no manual steps | ✅ 12 containers, init jobs one-shot with `depends_on: service_healthy` |
| 2 | `scripts/smoke-test.sh` passes with a scenario-by-scenario summary | ✅ **18/18** against the live stack |
| 3 | All of T1–T10 exist as automated tests and pass | ✅ see [TEST_PLAN.md](TEST_PLAN.md) |
| 4 | README documents run, test, per-scenario curl, and "what would break / how you'd notice" | ✅ [README §What would break](../README.md#what-would-break-and-how-you-would-notice) covers Redis outage and Kafka rebalance |
| 5 | Every §9 requirement implemented as described | ✅ audited below |

---

## §9 Non-negotiable correctness requirements

### 9.1 — Idempotent ingestion

Two independent guards, as the spec requires — a Redis fast path **and** an authoritative
database guard that holds even when Redis is missed entirely.

| Guard | Where | Mechanism |
|---|---|---|
| Fast path | `IdempotencyCache` | `GET idempotency:{transactionId}` → cached response returned without reprocessing |
| **Authoritative** | `TransactionIngestor:143` | `ctx.insert(...)` — throws `DocumentExistsException` on a duplicate key |

`upsert()` appears **nowhere** in any authoritative write path. The only `replace` in the
codebase is `OutboxRepository:85`, transitioning an outbox row `PENDING → PUBLISHED`, which is a
deliberate state transition on a different document — not an overwrite of a transaction record.

Verified by **T3**: two concurrent identical requests produce exactly one document, exactly one
Kafka event, and the same response to both callers.

### 9.2 — The outbox pattern

```java
cluster.transactions().run(ctx -> {                     // TransactionIngestor:138
    ctx.insert(rawTransactions, stamped.documentKey(), ...);        // :143
    ctx.insert(outbox,          eventForStamped.documentKey(), ...);// :144
});
```

Both documents commit in a single Couchbase multi-document ACID transaction via the raw SDK.
Spring Data Couchbase is deliberately **not** used anywhere in the project — it does not expose
this API ([ADR-0005](adr/0005-transactional-outbox.md), `CouchbaseConfig:14`).

Ordering on the publish side is the part that actually matters:

```java
outboxRepository.claim(event.outboxId(), CLAIM_STALE_AFTER);     // OutboxPublisher:237, :323
producer.send(...).get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);   // OutboxPublisher:341
outboxRepository.markPublished(event.outboxId(), Instant.now()); // OutboxPublisher:344
```

`markPublished` is strictly *after* the broker ack. A crash between the two leaves the row
`PENDING` and it is republished — at-least-once, never at-most-once.

Both publish paths — the scheduled poll (`:237`) and the post-commit nudge (`:323`) — take the
CAS-guarded claim first, so exactly one of them sends a given row (`OutboxRepository:128`). A
re-read that merely *checked* the status was not enough: both paths can truthfully observe `PENDING`
because neither has acked yet. Verified by `OutboxClaimIT`, and negative-tested — with the CAS
removed, all 32 concurrent claimants win.

Verified by **T4**: crash between commit and publish → the record stays `PENDING`, and restart
republishes it.

> **Known residual window** ([#12](../../issues/12)): if the ack times out but the broker did in
> fact accept the send, or if `markPublished` itself fails, the event is published twice.
> Consumers are idempotent on `transactionId`, so this is a duplicate-work cost rather than a
> correctness break — but it is tracked, not dismissed.

### 9.3 — Atomic velocity counters

Every **check-then-act** sequence against Redis is a single `EVAL`:

| Script | Service | Guards |
|---|---|---|
| `lua/velocity.lua` | enrichment | `INCR` + conditional `EXPIRE` — the exact race §9.3 names |
| `lua/set_add_count.lua` | enrichment | `SADD` + `EXPIRE` + `SCARD` (distinct-geo / distinct-device signals) |
| `lua/known_device.lua` | enrichment | membership test + add, in one round trip |
| `lua/rate_limit.lua` | gateway | `INCR` + conditional `EXPIRE` on the caller's window |

**The one apparent exception, examined rather than waved through.**
`IdempotencyCache` does a `GET` (line 45) and a later `SET` (line 60) as two separate calls,
which is textbook check-then-act. It is *not* a §9.3 violation, and could not be fixed by Lua:

- the "act" is an entire Couchbase ACID transaction, which cannot execute inside a Redis script;
- §9.1 explicitly designs this Redis check as a **non-authoritative** fast path, with `insert()`
  as "the FINAL authoritative guard even if Redis is unavailable or missed."

So two concurrent requests *are* permitted to both miss the cache. `insert()` then makes exactly
one win and the loser returns the same response. Atomicity is enforced where the spec puts it —
in Couchbase — and T3 proves it holds under genuine concurrency.

Additionally verified by a dedicated concurrency hammer test proving the Lua counter is exact
under parallel load, and by **T2**.

### 9.4 — Scoring and decision are separate services

Genuinely separate deployables, not classes in one process:

| | `scoring-service` | `decision-service` |
|---|---|---|
| Consumer group | `fraud-scoring-group` | `fraud-decision-group` |
| Consumes | `fraud.transactions.enriched` | `fraud.transactions.scored` |
| Produces | `fraud.transactions.scored` | `fraud.transactions.decisioned` |
| Config source | `fraud-rules` collection | `policy` document |
| Container | own image, own `mem_limit` | own image, own `mem_limit` |

The score is objective and the policy is a separate, independently reloadable input — so an
auditor reviewing a historical decision can tell "the model changed" from "the policy changed"
([ADR-0008](adr/0008-scoring-decision-split.md)).

### 9.5 — Cooperative sticky rebalancing

Explicit on **all four** consumers — audited, not assumed:

```
enrichment-service/src/main/resources/application.yml:32
scoring-service/src/main/resources/application.yml:24
decision-service/src/main/resources/application.yml:31
action-audit-service/src/main/resources/application.yml:32
    partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

Four services declare a `@KafkaListener`; four set the assignor. No consumer is left on the
client default.

Verified by **T5**, which is parameterized across both assignors and asserts the `RangeAssignor`
control *does* revoke everything — so the cooperative assertion is falsifiable rather than
tautological.

### 9.6 — Manual, post-work offset commits

Also all four, both halves of the setting:

```yaml
enable-auto-commit: false     # never the wall-clock timer
ack-mode: MANUAL_IMMEDIATE    # we decide when
```

and in every listener the `ack.acknowledge()` call is placed strictly after the durable unit of
work — the Couchbase write and/or the downstream Kafka publish — never before.

`PolicyCache` is the sharp edge here: on restart with an unacked backlog it throws explicitly
rather than proceeding with a null policy, leaving the record unacked for redelivery. Failing
loudly and redelivering is correct; silently scoring against no policy would not be.

### 9.7 — Append-only audit ledger

`AuditLedgerRepository` exposes exactly three methods:

```java
boolean append(AuditLedgerEntry entry);                                 // insert(), not upsert()
Optional<AuditLedgerEntry> findByKey(String key);
List<AuditLedgerEntry>     findByTransactionId(String transactionId);
```

`append` returns `boolean` rather than `void` so a duplicate — normal under at-least-once
redelivery — is reported to the caller instead of being swallowed or raised as an error.

No `update`, `delete`, `upsert`, `replace`, `remove`, or `save` — at the **Java interface level**,
so a future change cannot silently edit history without visibly adding a mutating method first.

Verified by **T9**, which asserts this reflectively against **both** the interface *and* the
implementation class — checking only the interface would miss a mutating method added to the impl
and reached by casting.

### 9.8 — The sync-facade timeout has a named, deliberate default

```java
return new FraudDecisionResponse(transactionId, correlationId,
        Decision.REVIEW, 0, List.of(), "unresolved",
        DecisionSource.TIMEOUT_DEFAULT, latencyMs);   // DecisionWaiter:128-130
```

`REVIEW`, never `ALLOW`. The source is carried in the response as `TIMEOUT_DEFAULT` rather than
being indistinguishable from a real decision, and is emitted as
`fraud.gateway.decision.resolved{source=TIMEOUT_DEFAULT}` — because a safe default that fires
*quietly* is nearly as bad as an unsafe one. That ratio is the system's primary SLO: what
fraction of our decisions were actually decisions?

Verified by **T7** (delayed pipeline → REVIEW at ~150ms, then reconciliation) and by T10's
assertion that the happy path resolves by `PIPELINE` — not merely that it returned a plausible
decision.

---

## What this audit does not claim

- It verifies the requirements are **implemented**; it does not claim the system is
  production-ready. The tracked gaps ([#9](../../issues/9) circuit breaker,
  [#24](../../issues/24) consumer-group dropout detection) are real and remain open. The
  systematic outbox double-publish ([#12](../../issues/12)) is closed by the CAS claim above;
  the ordinary at-least-once duplicate on an ack timeout remains, and is accepted
  ([ADR-0005](adr/0005-transactional-outbox.md)).
- Single-node Kafka at RF=1 and Couchbase CE single-node are development topologies. The
  multi-broker design is written up but not provisioned ([#5](../../issues/5)).
- Auth is HMAC-shared-secret, a deliberate simplification the spec permits (§13); a real
  deployment needs OAuth2/JWKS ([#6](../../issues/6)).
