-- Velocity counter: increment, and set the TTL ONLY on creation (§9.3, ADR-0007).
--
-- KEYS[1] = velocity:1m:{customerId}  or  velocity:1h:{customerId}
-- ARGV[1] = window TTL in seconds
-- Returns the count AFTER increment — so the 6th transaction in a minute reads 6,
-- which is what makes GREATER_THAN 5 fire on the 6th and not the 7th.
--
-- Why this must be one script rather than INCR then EXPIRE from the application:
--
-- If the process crashes, is descheduled, or the connection drops between the
-- two calls, the key exists with NO TTL AT ALL. It is then immortal — it
-- accumulates every future transaction that customer ever makes, so velocity_1m
-- climbs past the threshold and stays there, and VELOCITY_1M fires on every
-- subsequent transaction for the rest of time.
--
-- The customer is silently held on every payment, and nothing in the logs
-- explains it: the rule is firing correctly against a counter that is lying.
-- Diagnosing it means noticing that one key has TTL -1, which nobody thinks to
-- check. See docs/RUNBOOK.md, "one customer is flagged on every transaction".

local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return current
