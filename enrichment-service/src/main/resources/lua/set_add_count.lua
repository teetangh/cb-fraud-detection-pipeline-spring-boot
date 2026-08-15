-- Add a member to a set, set the TTL only when the set is new, return cardinality.
--
-- KEYS[1] = geo:countries:{customerId}  or  device:customers:{deviceId}
-- ARGV[1] = member to add (countryCode / customerId)
-- ARGV[2] = TTL in seconds
-- Returns the set cardinality AFTER the add.
--
-- Same atomicity requirement as velocity.lua: SADD followed by a separate
-- EXPIRE leaves a window in which the set can end up with no TTL and never
-- expire. A permanent geo:countries set would make distinct_countries_24h count
-- every country the customer has EVER transacted from, so GEO_ANOMALY
-- (threshold 1) would fire forever on anyone who has ever travelled.
--
-- TTL < 0 covers both -1 (exists, no TTL) and -2 (does not exist), so a set
-- that somehow lost its TTL gets one back rather than staying immortal.

redis.call('SADD', KEYS[1], ARGV[1])
if redis.call('TTL', KEYS[1]) < 0 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return redis.call('SCARD', KEYS[1])
