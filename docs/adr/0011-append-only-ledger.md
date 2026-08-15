# ADR-0011 — Ledger immutability enforced by the type system

**Status:** Accepted · Spec §8, §9.7

## Context

`audit.audit-ledger` is the regulatory record of what this system decided and why. Its value
depends entirely on being trustworthy, and it is trustworthy only if it cannot be edited after the
fact.

Couchbase Community Edition has no insert-only RBAC role — that is an Enterprise feature. So the
guarantee has to come from somewhere else.

## Decision

The ledger repository exposes **exactly one** mutating method:

```java
public interface AuditLedgerRepository {
    void append(AuditLedgerEntry entry);   // insert() — throws on duplicate key
    Optional<AuditLedgerEntry> findByKey(String key);
    List<AuditLedgerEntry> findByTransactionId(String transactionId);
}
```

There is no `update`. No `delete`. No `upsert`. No `save`. Not "we agreed not to call them" —
**the methods do not exist**, so calling one is a compile error.

`append()` uses Couchbase `insert()`, so re-appending the same
`audit::{transactionId}::{eventType}` key throws rather than overwriting.

## Naive alternative

A generic CRUD repository — `CouchbaseRepository<AuditLedgerEntry, String>` or a hand-rolled
`GenericRepository<T>` — reused for this collection as for every other. It is less code and it is
consistent with the rest of the codebase.

## Failure mode

**Nothing prevents a future change from editing history, and the change will look reasonable when
it is made.**

The realistic path is not malice. It is a Tuesday, a bug is found where some ledger entries were
written with a wrong `serviceVersion`, and someone writes a one-off backfill: `ledgerRepo.save(...)`
over the affected rows. It passes review — it is a small, well-intentioned data fix using a method
that was right there. The audit trail is now something that has been edited, which in a regulated
context means it is no longer an audit trail at all. And there is no way to prove *what* was
changed, because the change overwrote the evidence.

The same applies with more force to changes made by an AI agent in a later session, working from a
task description and reaching for the obvious available method. Spec §9.7 names this case
explicitly, and it is the strongest argument for the approach: **conventions are not enforceable
against a future contributor who has not read the convention.** A missing method is.

## Consequences

- Corrections are made by **appending a correcting entry**, never by editing. The ledger grows
  monotonically; history is reconstructed by replay. This is how ledgers have worked since double
  entry bookkeeping, and it is the right model.
- The ledger collection has no delete path at all, including for retention. Retention would be a
  bulk administrative operation performed outside this codebase, deliberately.
- Duplicate `append()` throws `DocumentExistsException`, which is caught and counted as a
  duplicate-suppressed event rather than an error — redelivery from
  [ADR-0010](0010-manual-offset-commits.md) makes this a normal occurrence.
- Only this repository is constrained. `decisions` is immutable-by-convention (write-once, keyed
  by `transactionId`) but is not type-constrained, because it is not the regulatory record.

## Verified by

[T9](../TEST_PLAN.md#t9) — a reflection test enumerating every method on
`AuditLedgerRepository` and asserting that none matches `update*`, `delete*`, `remove*`, `save*`,
`upsert*`, or `replace*`. The test exists so that a future contributor who adds such a method
gets a red build with an explanatory message, rather than a passing one.
