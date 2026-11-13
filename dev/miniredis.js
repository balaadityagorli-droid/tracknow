'use strict';
// DEV/TEST ONLY: a small Redis-protocol server so the stack can run where no Redis is installed.
// It implements just the commands TrackNow uses. Lua scripts are executed by REAL Lua 5.4 via dev/lua-host,
// and the whole server is single-threaded, so script atomicity matches Redis semantics.
// It is NOT a performance reference: use a real Redis (docker compose up redis) for any latency claims.
//
// WINDOWS FALLBACK: If the lua-host binary is not found, we fall back to a pure-JS script emulator
// that re-implements the exact logic of the three TrackNow Lua scripts (update_location, claim, release, rate_limit).
const net = require('node:net');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const fs = require('node:fs');

const store = new Map();
const scripts = new Map();
const channels = new Map(); // channel -> Set<conn>

function entry(key, type, create) {
  let e = store.get(key);
  if (e && e.exp && e.exp <= Date.now()) { store.delete(key); e = undefined; }
  if (!e && create) { e = { t: type, v: type === 'str' ? '' : new Map(), exp: 0 }; store.set(key, e); }
  if (e && e.t !== type) throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
  return e;
}
const hav = (lon1, lat1, lon2, lat2) => {
  const r = Math.PI / 180, dLat = (lat2 - lat1) * r, dLon = (lon2 - lon1) * r;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * r) * Math.cos(lat2 * r) * Math.sin(dLon / 2) ** 2;
  return 2 * 6372797.560856 * Math.asin(Math.min(1, Math.sqrt(a)));
};

function exec(a, conn) {
  const cmd = String(a[0]).toUpperCase();
  switch (cmd) {
    case 'PING': return { status: 'PONG' };
    case 'SELECT': case 'CLIENT': return { status: 'OK' };
    case 'FLUSHDB': case 'FLUSHALL': store.clear(); return { status: 'OK' };
    case 'DBSIZE': return store.size;
    case 'HSET': { const e = entry(a[1], 'hash', true); let n = 0; for (let i = 2; i < a.length; i += 2) { if (!e.v.has(a[i])) n++; e.v.set(a[i], String(a[i + 1])); } return n; }
    case 'HGET': { const e = entry(a[1], 'hash'); return e && e.v.has(a[2]) ? e.v.get(a[2]) : null; }
    case 'HMGET': { const e = entry(a[1], 'hash'); return a.slice(2).map((f) => (e && e.v.has(f) ? e.v.get(f) : null)); }
    case 'HGETALL': { const e = entry(a[1], 'hash'); const o = []; if (e) for (const [k, v] of e.v) o.push(k, v); return o; }
    case 'HDEL': { const e = entry(a[1], 'hash'); let n = 0; if (e) for (const f of a.slice(2)) if (e.v.delete(f)) n++; return n; }
    case 'DEL': { let n = 0; for (const k of a.slice(1)) { const e = store.get(k); if (e) { store.delete(k); n++; } } return n; }
    case 'EXISTS': { let n = 0; for (const k of a.slice(1)) { const e = store.get(k); if (e && !(e.exp && e.exp <= Date.now())) n++; } return n; }
    case 'SET': {
      const key = a[1]; const opts = a.slice(3).map((x) => String(x).toUpperCase()); let exp = 0;
      const nx = opts.includes('NX');
      const px = opts.indexOf('PX'); if (px >= 0) exp = Date.now() + Number(a[3 + px + 1]);
      const ex = opts.indexOf('EX'); if (ex >= 0) exp = Date.now() + Number(a[3 + ex + 1]) * 1000;
      const cur = store.get(key);
      if (nx && cur && !(cur.exp && cur.exp <= Date.now())) return null;
      store.set(key, { t: 'str', v: String(a[2]), exp });
      return { status: 'OK' };
    }
    case 'GET': { const e = entry(a[1], 'str'); return e ? e.v : null; }
    case 'INCR': { const e = entry(a[1], 'str', true); e.v = String((Number(e.v) || 0) + 1); return Number(e.v); }
    case 'PEXPIRE': case 'EXPIRE': { const e = store.get(a[1]); if (!e) return 0; e.exp = Date.now() + Number(a[2]) * (cmd === 'EXPIRE' ? 1000 : 1); return 1; }
    case 'GEOADD': { const e = entry(a[1], 'zset', true); let n = 0; for (let i = 2; i + 2 < a.length + 0; i += 3) { if (!e.v.has(a[i + 2])) n++; e.v.set(a[i + 2], [Number(a[i]), Number(a[i + 1])]); } return n; }
    case 'ZREM': { const e = entry(a[1], 'zset'); let n = 0; if (e) for (const m of a.slice(2)) if (e.v.delete(m)) n++; return n; }
    case 'ZCARD': { const e = entry(a[1], 'zset'); return e ? e.v.size : 0; }
    case 'GEOSEARCH': {
      const e = entry(a[1], 'zset'); if (!e) return [];
      const up = a.map((x) => String(x).toUpperCase());
      const f = up.indexOf('FROMLONLAT'), b = up.indexOf('BYRADIUS');
      const lon = Number(a[f + 1]), lat = Number(a[f + 2]);
      let rad = Number(a[b + 1]); const unit = up[b + 2]; if (unit === 'KM') rad *= 1000;
      const ci = up.indexOf('COUNT'); const count = ci >= 0 ? Number(a[ci + 1]) : Infinity;
      const withDist = up.includes('WITHDIST'); const desc = up.includes('DESC');
      const hits = [];
      for (const [m, [x, y]] of e.v) { const d = hav(lon, lat, x, y); if (d <= rad) hits.push([m, d]); }
      hits.sort((p, q) => (desc ? q[1] - p[1] : p[1] - q[1]));
      return hits.slice(0, count).map(([m, d]) => (withDist ? [m, d.toFixed(4)] : m));
    }
    case 'PUBLISH': {
      const subs = channels.get(a[1]); if (!subs) return 0;
      for (const c of subs) c.write(ser(['message', a[1], String(a[2])]));
      return subs.size;
    }
    case 'SCRIPT': {
      if (String(a[1]).toUpperCase() !== 'LOAD') throw new Error('ERR unsupported SCRIPT subcommand');
      const sha = crypto.createHash('sha1').update(a[2]).digest('hex'); scripts.set(sha, a[2]); return sha;
    }
    default: throw new Error('ERR unknown command ' + cmd);
  }
}

function ser(v) {
  if (v === null || v === undefined) return '$-1\r\n';
  if (typeof v === 'number') return ':' + Math.trunc(v) + '\r\n';
  if (typeof v === 'string') return '$' + Buffer.byteLength(v) + '\r\n' + v + '\r\n';
  if (Array.isArray(v)) return '*' + v.length + '\r\n' + v.map(ser).join('');
  if (v.status) return '+' + v.status + '\r\n';
  return '$-1\r\n';
}

// ---- Pure-JS script runner (replaces Lua subprocess on Windows) ----
// Implements a small redis.call bridge so the script logic can call store ops directly.
function runScriptJS(src, keys, argv) {
  // Build a mini redis.call that operates on the in-process store
  const redisCall = (...args) => {
    const r = exec(args.map(String), null);
    // exec returns {status:...} for OK replies; map to null-like for Lua compatibility
    if (r && typeof r === 'object' && r.status) return r.status === 'OK' ? null : r.status;
    return r;
  };

  // Identify the script by SHA (already stored) and run its logic natively
  // We match by exact source text since we can't run real Lua on Windows.
  const sha = crypto.createHash('sha1').update(src).digest('hex');

  // Try the named scripts first (registered via defineScript)
  for (const [name, handler] of nativeScripts) {
    if (nativeScriptSrcs.get(name) === src) {
      return handler(keys, argv, redisCall);
    }
  }

  // Fallback: try to match any registered native handler by SHA
  const nameForSha = scriptShaToName.get(sha);
  if (nameForSha) {
    const handler = nativeScripts.get(nameForSha);
    if (handler) return handler(keys, argv, redisCall);
  }

  throw new Error('ERR no native JS implementation for this Lua script on Windows. Register it in nativeScripts.');
}

// Map script name -> native JS implementation
const nativeScripts = new Map();
const nativeScriptSrcs = new Map();
const scriptShaToName = new Map();

function registerNative(name, src, fn) {
  nativeScripts.set(name, fn);
  nativeScriptSrcs.set(name, src);
  const sha = crypto.createHash('sha1').update(src).digest('hex');
  scriptShaToName.set(sha, name);
}

// Load the actual Lua source files and register JS equivalents
function loadNativeScripts() {
  const scriptsDir = path.join(__dirname, '..', 'src', 'scripts');
  if (!fs.existsSync(scriptsDir)) return;

  // update_location.lua
  const ulPath = path.join(scriptsDir, 'update_location.lua');
  if (fs.existsSync(ulPath)) {
    const src = fs.readFileSync(ulPath, 'utf8');
    registerNative('update_location', src, (KEYS, ARGV, rc) => {
      // KEYS[1]=rider hash  KEYS[2]=riders:avail  KEYS[3]=rate counter
      // ARGV: id lat lng ts seq nowMs persistMs phaseMs ratePerSec
      // 1-based in Lua -> 0-based in JS (ARGV[1] = ARGV[0])
      const riderKey = KEYS[0], availKey = KEYS[1], rlKey = KEYS[2];
      const id = ARGV[0], lat = ARGV[1], lng = ARGV[2];
      const ts = Number(ARGV[3]), seq = Number(ARGV[4]), now = Number(ARGV[5]);
      const persistMs = Number(ARGV[6]), phaseMs = Number(ARGV[7]), ratePerSec = Number(ARGV[8]);

      // Rate limit: atomic INCR + PEXPIRE on first
      const n = exec(['INCR', rlKey], null);
      if (n === 1) exec(['PEXPIRE', rlKey, '2000'], null);
      if (n > ratePerSec) return [0, 'rate_limited'];

      // Read current rider state
      const cur = exec(['HMGET', riderKey, 'seq', 'ts', 'status', 'orders', 'persist'], null);
      if (cur[0] !== null) {
        if (seq <= Number(cur[0])) return [0, 'stale_seq'];
        if (ts < Number(cur[1])) return [0, 'stale_ts'];
      }

      let orders = cur[3] || '';
      let status = cur[2] || 'available';
      if (status === 'offline') {
        status = orders !== '' ? 'busy' : 'available';
      }

      // Persistence throttle
      let persist = cur[4] !== null ? Number(cur[4]) : now - persistMs + phaseMs;
      let flag = 0;
      if (now - persist >= persistMs) { flag = 1; persist = now; }

      exec(['HSET', riderKey, 'lat', lat, 'lng', lng, 'ts', ARGV[3], 'seq', ARGV[4],
        'rx', ARGV[5], 'status', status, 'persist', String(persist), 'orders', orders], null);
      if (status === 'available') exec(['GEOADD', availKey, lng, lat, id], null);
      return [1, status, orders, flag];
    });
  }

  // claim.lua
  const claimPath = path.join(scriptsDir, 'claim.lua');
  if (fs.existsSync(claimPath)) {
    const src = fs.readFileSync(claimPath, 'utf8');
    registerNative('claim', src, (KEYS, ARGV, rc) => {
      // KEYS[1]=rider hash  KEYS[2]=riders:avail  KEYS[3..]=assign:<orderId> guards  (Lua 1-based -> JS 0-based)
      // ARGV: [0]=nowMs [1]=maxAgeMs [2]=riderId [3]=batchId [4..]=orderId per order
      const riderKey = KEYS[0], availKey = KEYS[1];
      const nowMs = Number(ARGV[0]), maxAgeMs = Number(ARGV[1]);
      const riderId = ARGV[2], batchId = ARGV[3];

      const r = exec(['HMGET', riderKey, 'status', 'rx'], null);
      if (r[0] === null) return [0, 'unknown_rider'];
      if (r[0] !== 'available') return [0, 'rider_unavailable'];
      if (nowMs - Number(r[1]) > maxAgeMs) {
        exec(['HSET', riderKey, 'status', 'offline'], null);
        exec(['ZREM', availKey, riderId], null);
        return [0, 'rider_stale'];
      }
      // Check all assign guards (KEYS[2], KEYS[3] ... in 0-based)
      for (let i = 2; i < KEYS.length; i++) {
        const exists = exec(['EXISTS', KEYS[i]], null);
        if (exists === 1) return [0, 'order_already_assigned'];
      }
      // Set guards and build order list
      // Lua: for i=3,#KEYS: ARGV[i+2] = orderId  → i=3 → ARGV[5] (1-based) = ARGV[4] (0-based)
      // In JS: KEYS guard starts at index 2; orderId is ARGV[4 + (i-2)]
      let list = '';
      for (let i = 2; i < KEYS.length; i++) {
        const orderId = ARGV[4 + (i - 2)]; // ARGV[4] for first order, ARGV[5] for second
        exec(['SET', KEYS[i], riderId], null);
        if (i > 2) list += ',';
        list += orderId;
      }
      exec(['HSET', riderKey, 'status', 'busy', 'orders', list, 'batch', batchId], null);
      exec(['ZREM', availKey, riderId], null);
      return [1, 'ok', list];
    });
  }

  // release.lua
  const relPath = path.join(scriptsDir, 'release.lua');
  if (fs.existsSync(relPath)) {
    const src = fs.readFileSync(relPath, 'utf8');
    registerNative('release', src, (KEYS, ARGV, rc) => {
      // KEYS[1]=rider hash  KEYS[2]=riders:avail   ARGV[1]=riderId
      const riderKey = KEYS[0], availKey = KEYS[1], riderId = ARGV[0];
      const r = exec(['HMGET', riderKey, 'orders', 'lat', 'lng'], null);
      if (r[0] === null && r[1] === null) return 0;
      if (r[0]) {
        for (const id of r[0].split(',').filter(Boolean)) {
          exec(['DEL', 'assign:' + id], null);
        }
      }
      exec(['HSET', riderKey, 'status', 'available', 'orders', '', 'batch', ''], null);
      if (r[1] && r[2]) exec(['GEOADD', availKey, r[2], r[1], riderId], null);
      return 1;
    });
  }

  // rate_limit.lua
  const rlPath = path.join(scriptsDir, 'rate_limit.lua');
  if (fs.existsSync(rlPath)) {
    const src = fs.readFileSync(rlPath, 'utf8');
    registerNative('rate_limit', src, (KEYS, ARGV, rc) => {
      // KEYS[1]=bucket key  ARGV[1]=limit  ARGV[2]=windowMs  (Lua 1-based -> JS 0-based)
      const key = KEYS[0], limit = Number(ARGV[0]), windowMs = Number(ARGV[1]);
      const n = exec(['INCR', key], null);
      if (n === 1) exec(['PEXPIRE', key, String(windowMs)], null);
      return n <= limit ? [1] : [0];
    });
  }
}

// ---- Lua host (subprocess, used if binary exists) ----
let child, buf = '', luaHostAvailable = false;
const lines = [], waiters = [];
function startHost() {
  const bin = path.join(__dirname, 'lua-host', 'lua-host');
  // On Windows the binary won't have .exe; check for both
  const binWin = bin + '.exe';
  const actualBin = fs.existsSync(bin) ? bin : fs.existsSync(binWin) ? binWin : null;
  if (!actualBin) {
    console.log('[miniredis] lua-host binary not found — using pure-JS script emulation (dev mode).');
    loadNativeScripts();
    return;
  }
  luaHostAvailable = true;
  child = spawn(actualBin, [path.join(__dirname, 'lua-host', 'driver.lua')], { stdio: ['pipe', 'pipe', 'inherit'] });
  child.stdout.on('data', (d) => {
    buf += d;
    let i;
    while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); if (waiters.length) waiters.shift()(l); else lines.push(l); }
  });
  child.on('exit', () => { if (!closing) { console.error('lua-host exited'); process.exit(1); } });
}
const nextLine = () => new Promise((res) => (lines.length ? res(lines.shift()) : waiters.push(res)));
async function runScriptLua(script, keys, argv) {
  child.stdin.write(JSON.stringify({ op: 'eval', script, keys, argv }) + '\n');
  for (;;) {
    const m = JSON.parse(await nextLine());
    if (m.call) {
      let reply;
      try { const r = exec(m.call, null); reply = { r: r && r.status ? r.status : r }; } catch (e) { reply = { err: e.message }; }
      child.stdin.write(JSON.stringify(reply) + '\n');
    } else if ('done' in m) return m.done;
    else throw new Error('ERR Error running script: ' + m.error);
  }
}

async function runScript(script, keys, argv) {
  if (luaHostAvailable) return runScriptLua(script, keys, argv);
  return runScriptJS(script, keys, argv);
}

// ---- server ----
const queue = []; let busy = false; let closing = false;
async function pump() {
  if (busy) return; busy = true;
  while (queue.length) {
    const { conn, args } = queue.shift();
    if (conn.destroyed) continue;
    try {
      const cmd = String(args[0]).toUpperCase();
      if (cmd === 'SUBSCRIBE') {
        for (const ch of args.slice(1)) { conn.subs.add(ch); (channels.get(ch) || channels.set(ch, new Set()).get(ch)).add(conn); conn.write(ser(['subscribe', ch, conn.subs.size])); }
      } else if (cmd === 'UNSUBSCRIBE') {
        for (const ch of (args.length > 1 ? args.slice(1) : [...conn.subs])) { conn.subs.delete(ch); const s = channels.get(ch); if (s) s.delete(conn); conn.write(ser(['unsubscribe', ch, conn.subs.size])); }
      } else if (cmd === 'EVALSHA' || cmd === 'EVAL') {
        let src = args[1];
        if (cmd === 'EVALSHA') { src = scripts.get(args[1]); if (!src) { conn.write('-NOSCRIPT No matching script. Please use EVAL.\r\n'); continue; } }
        else scripts.set(crypto.createHash('sha1').update(src).digest('hex'), src);
        const n = Number(args[2]);
        conn.write(ser(await runScript(src, args.slice(3, 3 + n), args.slice(3 + n))));
      } else conn.write(ser(exec(args, conn)));
    } catch (e) { conn.write('-' + (e.message.startsWith('ERR') || /^[A-Z]+ /.test(e.message) ? e.message : 'ERR ' + e.message) + '\r\n'); }
  }
  busy = false;
}

function parseCommands(conn, chunk) {
  conn.buf = Buffer.concat([conn.buf, chunk]);
  for (;;) {
    const b = conn.buf;
    if (!b.length) return;
    if (b[0] !== 0x2a) { const e = b.indexOf('\r\n'); if (e < 0) return; queue.push({ conn, args: b.toString('utf8', 0, e).split(' ') }); conn.buf = b.subarray(e + 2); continue; }
    let pos = b.indexOf('\r\n'); if (pos < 0) return;
    const n = Number(b.toString('utf8', 1, pos)); pos += 2;
    const args = [];
    for (let i = 0; i < n; i++) {
      const e = b.indexOf('\r\n', pos); if (e < 0) return;
      const len = Number(b.toString('utf8', pos + 1, e));
      if (b.length < e + 2 + len + 2) return;
      args.push(b.toString('utf8', e + 2, e + 2 + len)); pos = e + 2 + len + 2;
    }
    conn.buf = b.subarray(pos);
    queue.push({ conn, args });
  }
}

function start(port = 6390) {
  startHost();
  const server = net.createServer((conn) => {
    conn.buf = Buffer.alloc(0); conn.subs = new Set(); conn.setNoDelay(true);
    conn.on('data', (d) => { parseCommands(conn, d); pump(); });
    conn.on('close', () => { for (const ch of conn.subs) { const s = channels.get(ch); if (s) s.delete(conn); } });
    conn.on('error', () => {});
  });
  server.listen(port, '127.0.0.1', () => console.log(`miniredis (dev) listening on ${port}`));
  return server;
}

if (require.main === module) start(Number(process.env.REDIS_PORT || process.argv[2] || 6390));
module.exports = { start };
