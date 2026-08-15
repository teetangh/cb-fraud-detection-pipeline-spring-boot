-- Fixed-window rate limiter, executed atomically server-side (§9.3).
--
-- KEYS[1] = ratelimit:{clientId}:{minute}
-- ARGV[1] = window TTL in seconds
-- ARGV[2] = max requests permitted in the window
--
-- Returns the count after increment, or -1 if the limit is exceeded.
--
-- INCR and EXPIRE issued as two round-trips from the application is a
-- check-then-act race: a crash or deschedule between them leaves the key with
-- NO TTL at all, so it never expires and the client is rate-limited forever
-- with nothing in the logs to explain it. One script, one atomic step.

local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
if current > tonumber(ARGV[2]) then
  return -1
end
return current
