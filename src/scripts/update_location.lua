-- Validate + store one GPS sample atomically.
-- KEYS[1]=rider hash  KEYS[2]=riders:avail (geo)  KEYS[3]=per-device rate counter
-- ARGV: id lat lng ts seq nowMs persistMs phaseMs ratePerSec
-- Returns {0, reason} when rejected, {1, status, orders, persistFlag} when accepted.
local n = redis.call('INCR', KEYS[3])
if n == 1 then redis.call('PEXPIRE', KEYS[3], 2000) end
if n > tonumber(ARGV[9]) then return {0, 'rate_limited'} end

local cur = redis.call('HMGET', KEYS[1], 'seq', 'ts', 'status', 'orders', 'persist')
local ts, seq, now = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])
if cur[1] then
  if seq <= tonumber(cur[1]) then return {0, 'stale_seq'} end   -- duplicate or older sequence
  if ts < tonumber(cur[2]) then return {0, 'stale_ts'} end      -- newer seq but older clock
end

local orders = cur[4] or ''
local status = cur[3] or 'available'
if status == 'offline' then
  if orders ~= '' then status = 'busy' else status = 'available' end
end

-- Cluster-wide throttle for durable history: at most one row per rider per persistMs,
-- staggered by a per-rider phase so 2,000 riders do not all write in the same second.
local persist = tonumber(cur[5])
if not persist then persist = now - tonumber(ARGV[7]) + tonumber(ARGV[8]) end
local flag = 0
if now - persist >= tonumber(ARGV[7]) then flag = 1; persist = now end

redis.call('HSET', KEYS[1], 'lat', ARGV[2], 'lng', ARGV[3], 'ts', ARGV[4], 'seq', ARGV[5],
  'rx', ARGV[6], 'status', status, 'persist', tostring(persist), 'orders', orders)

-- Update geo sets: avail pool (available only) and all-riders index (all)
if status == 'available' then redis.call('GEOADD', KEYS[2], ARGV[3], ARGV[2], ARGV[1]) end

return {1, status, orders, flag}
