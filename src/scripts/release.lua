-- Return a rider to the available pool and drop the per-order guards.
-- KEYS[1]=rider hash  KEYS[2]=riders:avail   ARGV[1]=riderId
local r = redis.call('HMGET', KEYS[1], 'orders', 'lat', 'lng')
if not r[1] and not r[2] then return 0 end
if r[1] then
  for id in string.gmatch(r[1], '[^,]+') do redis.call('DEL', 'assign:' .. id) end
end
redis.call('HSET', KEYS[1], 'status', 'available', 'orders', '', 'batch', '')
if r[2] and r[3] then redis.call('GEOADD', KEYS[2], r[3], r[2], ARGV[1]) end
return 1
