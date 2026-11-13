-- Line protocol between the dev Redis (Node) and this Lua host.
--   Node -> Lua : {"op":"eval","script":"...","keys":[...],"argv":[...]}   and   {"r":<reply>} / {"err":"msg"}
--   Lua -> Node : {"call":[cmd,...]}  and finally {"done":<value>} / {"error":"msg"}
io.stdout:setvbuf('no')

local function jdecode(s)
  local pos = 1
  local function ws() pos = s:find('%S', pos) or (#s + 1) end
  local value
  local function str()
    local out = {}
    pos = pos + 1
    while true do
      local e = s:find('["\\]', pos)
      out[#out + 1] = s:sub(pos, e - 1)
      if s:sub(e, e) == '"' then pos = e + 1; break end
      local n = s:sub(e + 1, e + 1)
      if n == 'u' then out[#out + 1] = utf8.char(tonumber(s:sub(e + 2, e + 5), 16)); pos = e + 6
      else out[#out + 1] = ({ n = '\n', t = '\t', r = '\r', b = '\b', f = '\f' })[n] or n; pos = e + 2 end
    end
    return table.concat(out)
  end
  function value()
    ws()
    local c = s:sub(pos, pos)
    if c == '"' then return str()
    elseif c == '[' then
      local t = {}; pos = pos + 1; ws()
      if s:sub(pos, pos) == ']' then pos = pos + 1; return t end
      while true do
        t[#t + 1] = value(); ws()
        local d = s:sub(pos, pos); pos = pos + 1
        if d == ']' then return t end
      end
    elseif c == '{' then
      local t = {}; pos = pos + 1; ws()
      if s:sub(pos, pos) == '}' then pos = pos + 1; return t end
      while true do
        ws(); local k = str(); ws(); pos = pos + 1
        t[k] = value(); ws()
        local d = s:sub(pos, pos); pos = pos + 1
        if d == '}' then return t end
      end
    elseif s:sub(pos, pos + 3) == 'null' then pos = pos + 4; return false
    elseif s:sub(pos, pos + 3) == 'true' then pos = pos + 4; return true
    elseif s:sub(pos, pos + 4) == 'false' then pos = pos + 5; return false
    else
      local num = s:match('^-?%d+%.?%d*[eE]?[+-]?%d*', pos)
      pos = pos + #num
      return math.tointeger(tonumber(num)) or tonumber(num)
    end
  end
  return value()
end

local function jstr(x)
  return '"' .. x:gsub('[%c"\\]', function(c)
    return ({ ['"'] = '\\"', ['\\'] = '\\\\', ['\n'] = '\\n', ['\r'] = '\\r', ['\t'] = '\\t' })[c] or string.format('\\u%04x', c:byte())
  end) .. '"'
end

local function jencode(v)
  local t = type(v)
  if t == 'string' then return jstr(v)
  elseif t == 'number' then return string.format('%d', math.floor(v))
  elseif t == 'boolean' then return v and '1' or 'null'
  elseif t == 'nil' then return 'null'
  elseif t == 'table' then
    local parts = {}
    for i = 1, #v do parts[i] = jencode(v[i]) end
    return '[' .. table.concat(parts, ',') .. ']'
  end
  return 'null'
end

local function toarg(a)
  if type(a) == 'number' then
    if math.type(a) == 'integer' then return tostring(a) end
    return string.format('%.17g', a)
  end
  return tostring(a)
end

local redis = {}
function redis.call(...)
  local args = {}
  for i, a in ipairs({ ... }) do args[i] = toarg(a) end
  io.write('{"call":' .. jencode(args) .. '}\n')
  local reply = jdecode(io.read('l'))
  if type(reply) == 'table' and reply.err then error(reply.err, 0) end
  return reply.r
end
function redis.pcall(...) local ok, r = pcall(redis.call, ...); if ok then return r end; return { err = r } end
function redis.error_reply(m) return { err = m } end
function redis.status_reply(m) return { ok = m } end

local cache = {}
while true do
  local line = io.read('l')
  if not line then break end
  local m = jdecode(line)
  local fn = cache[m.script]
  local ok, err = true, nil
  if not fn then
    fn, err = load(m.script, 'script', 't', setmetatable({ redis = redis }, { __index = _G }))
    if fn then cache[m.script] = fn end
  end
  if not fn then
    io.write('{"error":' .. jstr('compile: ' .. tostring(err)) .. '}\n')
  else
    -- Redis exposes KEYS/ARGV as globals; the env table is shared, so set them per call.
    local env = debug.getupvalue(fn, 1) and select(2, debug.getupvalue(fn, 1))
    env.KEYS = m.keys; env.ARGV = m.argv
    local pok, res = pcall(fn)
    if pok then io.write('{"done":' .. jencode(res) .. '}\n')
    else io.write('{"error":' .. jstr(tostring(res)) .. '}\n') end
  end
end
