# ADR-0012 — `customerId` as the Kafka partition key

**Status:** Accepted · Spec §7

## Context

Velocity detection — "how many transactions has this customer made in the last minute?" — is
stateful per customer. Stateful stream processing is only correct if the state has a single
writer.

## Decision

Every transaction topic is keyed by **`customerId`**, with 6 partitions.

`fraud.alerts.realtime` deliberately breaks the pattern and keys by **`merchantId`**, because its
consumers ask a per-merchant question ("is this merchant under attack?"), and merchant-ordered
delivery is what makes that answerable.

## Naive alternative

Key by `transactionId` (maximally even distribution — every message a distinct key), or use no
key at all (round-robin).

Both are attractive: they spread load perfectly across partitions, and no partition can become
hot because one customer is busy.

## Failure mode

**Two consumer instances race on the same customer's velocity counter, and the velocity rule
silently stops working.**

Kafka guarantees ordering only *within* a partition, and a partition has exactly one consumer per
group. Key by `transactionId` and one customer's six rapid transactions scatter across six
partitions, processed by up to six instances concurrently:

```
 instance A: txn5 → INCR velocity:1m:cust-417 → 5   (not > 5, no fire)
 instance B: txn6 → INCR velocity:1m:cust-417 → 6   (> 5, fires)
```

That looks fine. Now consider the reads that build the rest of the signal set, and the general
case where processing is not a single atomic increment: the 24h country set, the device set, and
the ordering-dependent "is this a new device" check all become racy. Two instances interleaving on
`distinct_countries_24h` can each observe a pre-update view and both conclude the country count is
1, so `GEO_ANOMALY` never fires for the very transaction pair that constitutes the anomaly.

The deeper problem is that **the failure is invisible**. Nothing errors. The signals are merely
*slightly wrong*, in the direction of under-detection, under exactly the load conditions where
detection matters. You would only discover it by auditing fraud that got through — long after the
money left.

Keying by `customerId` makes all of one customer's transactions land on one partition, consumed in
order by one instance. The race cannot occur because the concurrency does not exist.

## Consequences

- **Atomicity and ordering are separate guarantees, and both are needed.** The Lua scripts
  ([ADR-0007](0007-lua-atomic-counters.md)) make each individual mutation atomic; the partition key
  makes the sequence single-writer. Lua alone still permits two instances interleaving *sequences*;
  partitioning alone still permits an interleaved `INCR`/`EXPIRE` within one instance. This pairing
  is the most commonly missed point in the design.
- **Hot partition risk is accepted.** A single very high-volume customer concentrates on one
  partition, and no amount of scaling helps that customer's throughput. This is the correct trade
  — correctness over even distribution — and at fraud-detection volumes per customer it is not a
  practical constraint. If it ever became one, the fix is a composite key
  (`customerId` + time bucket) with a matching change to the velocity window semantics, not a
  change of key.
- **Consumer parallelism is capped at 6** per group (partitions = maximum useful consumers). A
  7th instance would idle. 6 was chosen as a small multiple that divides evenly by 1, 2, 3, and 6
  instances, so common scaling steps produce balanced assignments.
- Repartitioning later would break velocity continuity mid-flight — partition count is effectively
  permanent for a stateful keyed topic. Worth getting right at creation.

## Verified by

[T2](../TEST_PLAN.md#t2) — 6 transactions for one `customerId` in quick succession must trip
`VELOCITY_1M` on the 6th, which requires them to have been counted in order by a single consumer.
[T5](../TEST_PLAN.md#t5) exercises this under an active rebalance.
