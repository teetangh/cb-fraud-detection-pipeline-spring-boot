# ADR-0009 — Explicit `CooperativeStickyAssignor` on every consumer group

**Status:** Accepted · Spec §7, §9.5

## Context

Four services consume Kafka. Consumer groups rebalance whenever membership changes: a deploy, a
pod restart, a crash, a scale-up. Rebalancing is routine, not exceptional — a rolling deploy of a
3-instance service triggers six of them.

## Decision

Every consumer group explicitly sets:

```properties
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

Explicitly, on all four consumer groups, not left to the client default.

## Naive alternative

Leave it unset and take the Kafka client default (`RangeAssignor`, eager rebalancing).

## Failure mode

**Eager rebalancing stops the entire consumer group, including partitions that were never going
to move.**

The eager protocol works in two phases: every member revokes **all** its partitions, then the
group leader computes a fresh assignment and everyone re-joins. So when one instance of a
6-partition, 2-consumer group dies:

```
eager:        [P0 P1 P2] [P3 P4 P5]        instance B dies
              ↓ ALL revoked — including A's, which nobody asked for
              [  stop-the-world  ]          ← A processes nothing
              [P0 P1 P2 P3 P4 P5]           A resumes with everything

cooperative:  [P0 P1 P2] [P3 P4 P5]        instance B dies
              [P0 P1 P2] [ revoked ]        ← A never stops
              [P0 P1 P2 P3 P4 P5]           A incrementally picks up P3–P5
```

Instance A had three healthy partitions and was doing useful work; the default assignor stops it
anyway, for the entire rebalance duration. During a rolling deploy that is repeated per instance,
so a "zero-downtime" deploy produces a series of full-group stalls.

The consequence is not just latency. Every stalled partition means transactions sitting unscored
while the caller's 150ms budget burns — so a routine deploy converts directly into a spike of
`resolvedBy: TIMEOUT_DEFAULT`, i.e. a spike of transactions held for manual review, on a schedule
set by the deploy pipeline.

There is also a correctness edge cited in spec §9.5: during the window before the old consumer's
offset commit is visible to the new owner, both can briefly process overlapping messages,
double-counting stateful signals like velocity.

## Consequences

- **All members must agree.** Cooperative and eager members in the same group cannot interoperate;
  a mixed group falls back to eager or fails to stabilise. This is why the setting is applied
  uniformly rather than per-service, and why it is set explicitly rather than inherited — an
  inherited default that changes in a future client version would silently break the property.
- Migrating an existing eager group to cooperative requires a two-phase rollout (add cooperative
  as a *second* strategy, deploy, then remove eager). Greenfield here, so not applicable — but
  worth knowing before anyone "just changes the config" on a running system.
- Rebalances become cheaper but slightly more numerous, since cooperative rebalancing may need
  more than one round to converge. This is the correct trade: several short partial pauses beat
  one long total stop.

## Verified by

[T5](../TEST_PLAN.md#t5) — a 2-instance consumer group over ≥4 partitions, one instance killed
mid-stream. Asserts the **unaffected** partitions never gap, and that reassigned partitions resume
within a bounded time.

The test is **parameterized across both assignors** and asserts that the same scenario *fails* the
no-gap check under `RangeAssignor`. Without that half, the test would only prove eventual
consistency — which eager rebalancing also provides — and would pass whether or not the setting
did anything. A test that passes under both configurations proves nothing about the configuration.
