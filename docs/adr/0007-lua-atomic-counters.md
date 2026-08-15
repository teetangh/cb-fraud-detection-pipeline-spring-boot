# ADR-0007 — Every Redis check-then-act is one Lua `EVAL`

**Status:** Accepted · Spec §9.3

## Context

Velocity signals are counters with TTLs. Creating one requires two operations: increment it, and
— only if it was just created — set its expiry. That is a check-then-act sequence, and
check-then-act across a network is a race by construction.

## Decision

Every Redis counter mutation, and every other check-then-act against Redis, is a **single Lua
script executed via `EVAL`**. Redis executes Lua scripts atomically server-side: no other command
runs between the script's first and last statement.

```lua
-- velocity: increment, and set TTL only on creation
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return current
```

Scripts are loaded once via `SCRIPT LOAD` and invoked by SHA (`EVALSHA`) through Spring's
`RedisScript` abstraction, which handles the `NOSCRIPT` reload automatically.

## Naive alternative

Two application-issued calls:

```java
Long current = redis.increment(key);
if (current == 1) redis.expire(key, 60);
```

## Failure mode

**A counter that never expires — which permanently frames an innocent customer.**

Interleaving of two concurrent requests for the same customer:

```
 T1: INCR key        → 1
 T2: INCR key        → 2
 T1: sees 1, EXPIRE key 60      ✓ TTL set
 …60s pass, key expires…
 T3: INCR key        → 1
 T2: (delayed retry) sees 2, skips EXPIRE
```

More damaging is the inverse: if the process crashes or is descheduled between `INCR` and
`EXPIRE`, the key exists with **no TTL at all**. It is now immortal. It accumulates every future
transaction that customer makes, forever. Their `velocity_1m` climbs past 5 and stays there, so
`VELOCITY_1M` fires on **every subsequent transaction** for the rest of time.

The customer is silently blocked or held on every payment, and nothing in the logs explains it —
the rule is firing correctly against a counter that is lying. Diagnosing this means noticing that
one key in Redis has `TTL -1`, which nobody thinks to check.

The related failure — TTL reset by a concurrent request sliding the window — corrupts the
measurement more subtly: the "last 60 seconds" is quietly no longer 60 seconds.

## Consequences

- All four Redis-backed signals use scripts: the 1m and 1h velocity counters, the 24h country set
  (`SADD` + conditional `EXPIRE` + `SCARD` in one script), and the device→customers set.
- The scripts are checked into `enrichment-service` as `.lua` resources, not inlined as Java
  strings — they are testable in isolation and readable by someone who does not read Java.
- **Ordering still matters independently.** Atomic increments stop two *operations* interleaving;
  they do not stop two *consumer instances* processing the same customer concurrently. That is
  what the `customerId` partition key is for ([ADR-0012](0012-customer-id-partition-key.md)). Both
  are required; neither is sufficient alone. This pairing is the single most commonly missed point
  in the design.
- Lua keeps Redis single-threaded for the script's duration, so scripts stay O(1)-ish. None loop
  over unbounded collections.

## Verified by

A dedicated concurrency test that hammers the script from many threads and asserts the final count
is **exactly** the number of increments issued — not "approximately", and not merely
"non-decreasing". Plus a TTL assertion that the key has a positive TTL after concurrent creation.
