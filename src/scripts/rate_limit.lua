-- Fixed-window limiter shared by every app instance.  KEYS[1]=counter  ARGV: limit windowMs
local c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
if c > tonumber(ARGV[1]) then return {0, c} end
return {1, c}
