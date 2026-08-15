# ADR-0014 — A Redis outage fails open, and signals are omitted rather than zeroed

**Status:** Accepted · Spec §10 (T6)

## Context

Redis holds the real-time behavioural signals: velocity counters, the 24h country set, the
device→customers map, plus idempotency cache entries and rate-limit counters. It is a cache and a
counter store, not a system of record. It will sometimes be unavailable.

This system sits **in the payment path**. Its availability posture is therefore a business
decision, not merely a technical one.

## Decision

Three separate choices, which are often conflated:

1. **Fail open, not closed.** A Redis outage must not fail the transaction. Payments keep flowing.
2. **Omit, do not default.** Signals that cannot be computed are **absent from the `signals` map**
   and listed in `degradedSignalKeys`, with `signalsDegraded: true`. They are never set to `0`.
3. **Be loud.** The degraded flag propagates onto the decision record and into the audit ledger,
   and is exported as a metric.

A rule whose signal key is absent does not evaluate and contributes nothing to the score. Scoring
proceeds on the signals that *are* available — including the Couchbase-backed ones
(`merchant_risk_score`, `amount_vs_p90_ratio`) and the pure-computation one (`is_off_hours_large`,
derived from the message itself and therefore never degraded).

## Naive alternative A — fail closed

Return an error, or BLOCK, when signals are unavailable.

### Failure mode

**A Redis outage becomes a total payment outage.** A cache being down stops all commerce. The
blast radius of a degraded fraud check is escalated into a complete revenue stop — a strictly
worse outcome than accepting slightly weaker fraud detection for the duration. For a component in
the payment path, this is the wrong trade almost regardless of the fraud numbers.

## Naive alternative B — default missing signals to zero

Fail open, but populate `velocity_1m: 0`, `distinct_countries_24h: 0`, and so on.

### Failure mode

**Zero is not "unknown" — it is a positive claim of innocence, invented from nothing.**

`velocity_1m: 0` asserts *"this customer has made no transactions in the last minute"*, which is
the strongest possible exonerating statement about them. During a Redis outage the system would be
manufacturing exculpatory evidence for every transaction it sees, and — because the values look
like real measurements — that fabrication would be **indistinguishable from a genuine quiet
period** in the stored decision record.

An auditor reviewing a transaction that got through during the outage would see a decision record
stating the customer had zero recent velocity, zero country diversity, and a shared-device count
of zero. All false, all recorded as fact, with nothing marking them as fabricated. That is
considerably worse than recording "we could not measure this", because it corrupts the audit
trail's truthfulness rather than merely its completeness.

Omission is honest and self-documenting: the rule cannot be evaluated, so it does not fire, and
the record says so.

## Consequences

- **Accepted risk, stated plainly:** during a Redis outage the system biases toward ALLOW for
  otherwise-clean transactions. Velocity-based fraud is more likely to succeed while Redis is
  down. This is a conscious trade of detection for availability, and it is signposted rather than
  hidden.
- The signal is not silent. `signalsDegraded` is on the decisioned message, the decision document,
  and the ledger entry, and is a Micrometer counter — so "how many decisions were made blind?" is
  a query, and any decision made during the outage is identifiable after the fact for
  re-examination.
- Every Redis call is wrapped with a short timeout and its failure handled per-signal, so one dead
  signal does not take the others down.
- **Gateway rate limiting also fails open** — if Redis is down, requests are not rate limited
  rather than being rejected. Same reasoning, and worth stating explicitly because "fail open" on
  a security control is exactly the kind of decision that should never be implicit.
- Idempotency degrades safely because its authority is Couchbase `insert()`, not Redis
  ([ADR-0006](0006-idempotent-ingestion.md)). Correctness is preserved during the outage; only
  speed is lost.

## Verified by

[T6](../TEST_PLAN.md#t6) — with Redis paused, a submitted transaction does not hang and does not
error; the affected signals are absent (**not zero** — the test asserts key absence explicitly,
which is what distinguishes this from alternative B); the score is computed from the remaining
signals; and an otherwise-clean transaction leans ALLOW.
