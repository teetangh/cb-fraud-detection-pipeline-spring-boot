-- Atomically TEST membership and ADD, returning whether the device was new.
--
-- KEYS[1] = device:known:{customerId}
-- ARGV[1] = deviceId
-- ARGV[2] = TTL in seconds
-- Returns 1 if this device had NOT been seen for this customer before, else 0.
--
-- The test and the add must not be separable. Issued as SISMEMBER then SADD
-- from the application, two concurrent transactions from a genuinely new device
-- would BOTH run SISMEMBER before either SADD, both see "not a member", and
-- both report is_new_device_high_amt = true.
--
-- NEW_DEVICE_HIGH_AMOUNT (weight 25) would then fire twice for what is one
-- genuinely-new device — inflating the score of a transaction that should have
-- counted it once, and pushing a REVIEW toward a BLOCK on a customer who simply
-- bought two things at once from a new phone.

local isNew = 0
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  isNew = 1
end
redis.call('SADD', KEYS[1], ARGV[1])
if redis.call('TTL', KEYS[1]) < 0 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return isNew
