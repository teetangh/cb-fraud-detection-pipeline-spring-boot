# ADR-0004 — The timeout default is REVIEW, never ALLOW

**Status:** Accepted · Spec §9.8

## Context

When the 150ms budget elapses without a decision, gateway-service must return *something*. That
something is a policy choice with real money attached, so it is made explicitly and recorded here
rather than falling out of an `orElse()` somewhere.

## Decision

Return **REVIEW**, tagged `resolvedBy: TIMEOUT_DEFAULT`.

The pipeline is **not** cancelled. It continues, the real decision is written to Couchbase, and if
it differs from REVIEW a webhook reconciles with the caller.

## Naive alternative

Return ALLOW, on the reasoning that most transactions are legitimate, so the pipeline probably
would have allowed it anyway — and blocking good customers on an internal timeout is bad business.

That argument is not stupid. It is wrong for a specific, structural reason.

## Failure mode

**Every degradation becomes an attack window.** A GC pause, a Kafka rebalance, a Redis blip, a
slow Couchbase write — any of them silently converts into *"let every ambiguous transaction
through"*. The system's protection would fail exactly when it is under stress, which is exactly
when fraud is most likely to be happening: load spikes and fraud waves are correlated, not
independent.

Worse, it is **self-concealing**. ALLOW-on-timeout produces no errors, no failed payments, no
customer complaints. The graph that would reveal it — decisions resolved by default rather than by
the pipeline — is the one nobody built, because nothing looked broken. An attacker who can induce
50ms of extra latency gets a blanket approval channel.

The correct posture for a control in the payment path is: **degrade toward caution, and be loud
about it.**

## Consequences

- REVIEW is a real business state, not a fudge: it maps to `HELD` in mock-payment-api. A held
  payment is recoverable — a fraudulent completed payment often is not. The asymmetry of those two
  errors is the whole argument.
- **`resolvedBy` is exported as a metric**, so `TIMEOUT_DEFAULT ÷ total` is the system's primary
  SLO. A default that fires quietly is nearly as bad as the wrong default; this is what stops it
  being quiet.
- Reconciliation is mandatory, not optional. Having told the caller REVIEW, we owe them the real
  answer when it arrives — otherwise a timeout would permanently degrade a transaction that the
  pipeline was about to ALLOW.
- Tuning the timeout is now a safe operation in one direction only: lowering it increases the
  `TIMEOUT_DEFAULT` rate (more friction, no added risk); raising it increases tail latency for the
  caller. Neither direction can accidentally create the ALLOW-on-degradation hole.

## Verified by

[T7](../TEST_PLAN.md#t7) — delayed pipeline returns REVIEW at ~150ms, and the later true decision
both lands in Couchbase and triggers the reconciliation webhook when it differs.
