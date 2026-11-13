-- Atomically claim ONE rider for ONE or TWO orders (batch). All-or-nothing by construction:
-- every check happens before the first write, and the whole script runs without interleaving.
-- KEYS[1]=rider hash  KEYS[2]=riders:avail  KEYS[3..]=assign:<orderId> guards
-- ARGV: nowMs maxAgeMs riderId batchId orderId...   (ARGV[i+2] is the order for KEYS[i])
local r = redis.call('HMGET', KEYS[1], 'status', 'rx')
if not r[1] then return {0, 'unknown_rider'} end
if r[1] ~= 'available' then return {0, 'rider_unavailable'} end
if tonumber(ARGV[1]) - tonumber(r[2]) > tonumber(ARGV[2]) then
  redis.call('HSET', KEYS[1], 'status', 'offline')
  redis.call('ZREM', KEYS[2], ARGV[3])
  return {0, 'rider_stale'}
end
for i = 3, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then return {0, 'order_already_assigned'} end
end
local list = ''
for i = 3, #KEYS do
  redis.call('SET', KEYS[i], ARGV[3])
  if i > 3 then list = list .. ',' end
  list = list .. ARGV[i + 2]
end
redis.call('HSET', KEYS[1], 'status', 'busy', 'orders', list, 'batch', ARGV[4])
redis.call('ZREM', KEYS[2], ARGV[3])
return {1, 'ok', list}
