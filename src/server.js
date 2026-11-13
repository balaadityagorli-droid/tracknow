'use strict';
// One stateless TrackNow app instance: HTTP API + WebSocket push. Run two or more against the same Redis + DB.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { Redis } = require('./redis');
const ws = require('./ws');
const { Metrics, merge, pctl, count } = require('./metrics');
const dbm = require('./db');

const ENV = process.env;
const PORT = +ENV.PORT || 3001;
const ID = ENV.INSTANCE_ID || `inst-${PORT}`;
const PEERS = (ENV.PEERS || '').split(',').filter(Boolean);
const RURL = new URL(ENV.REDIS_URL || 'redis://127.0.0.1:6379');
const DB_PATH = ENV.DB_PATH || path.join(__dirname, '..', 'data', 'tracknow.db');
const PERSIST_MS = +ENV.PERSIST_MS || 90000;        // one durable history row per rider per 90 s (≈22 writes/s for 2,000 riders)
const MAX_AGE_MS = +ENV.RIDER_MAX_AGE_MS || 15000;  // riders older than this are not assignable
const RIDER_RATE = +ENV.RIDER_RATE_PER_SEC || 10;   // per-device accepted+rejected updates per second
const RADIUS_M = +ENV.SEARCH_RADIUS_M || 5000;
const PUBLIC = path.join(__dirname, '..', 'public');

const m = new Metrics();
const db = dbm.open(DB_PATH, m);
const rc = new Redis({ host: RURL.hostname, port: +RURL.port || 6379 });
const rs = new Redis({ host: RURL.hostname, port: +RURL.port || 6379, pubsub: true });
for (const f of fs.readdirSync(path.join(__dirname, 'scripts'))) {
  const src = fs.readFileSync(path.join(__dirname, 'scripts', f), 'utf8');
  rc.defineScript(f.replace('.lua', ''), src);
}

class HttpError extends Error { constructor(status, code, detail) { super(code); this.status = status; this.code = code; this.detail = detail; } }

// ---------- Redis health ----------
let redisUp = false, ready = false, wasDown = false, riders = new Map();
const gate = () => { if (!redisUp || !ready) { m.inc('redis_refused'); throw new HttpError(503, 'REDIS_UNAVAILABLE', 'Live coordination is paused; no assignment was made.'); } };

rc.on('up', () => { redisUp = true; recover(); });
rc.on('down', () => { redisUp = false; ready = false; wasDown = true; m.inc('redis_down_events'); sendHb(); });
rs.on('up', () => { rs.subscribe('ops:loc'); rs.subscribe('orders:events'); });
rc.start(); rs.start();
rs.subscribe('ops:loc'); rs.subscribe('orders:events');

async function recover() {
  try {
    await rebuild();
    ready = true;
    if (wasDown) { m.inc('redis_recoveries'); wasDown = false; }
  } catch (e) { console.error(ID, 'rebuild failed', e.message); setTimeout(() => redisUp && recover(), 500); }
  sendHb();
}
// Redis may have restarted empty: restore rider exclusivity from the database (the source of truth) before accepting traffic.
async function rebuild() {
  const active = db.activeAssignments();
  const byRider = new Map();
  for (const a of active) { (byRider.get(a.rider_id) || byRider.set(a.rider_id, { ids: [], batch: a.batch_id || '' }).get(a.rider_id)).ids.push(a.id); }
  for (const [rid, v] of byRider) {
    await rc.call('HSET', `rider:${rid}`, 'status', 'busy', 'orders', v.ids.join(','), 'batch', v.batch);
    for (const id of v.ids) await rc.call('SET', `assign:${id}`, rid);
    await rc.call('ZREM', 'riders:avail', rid);
  }
  for (const rid of db.takeReleases()) {
    if (db.activeForRider(rid).length === 0) await rc.script('release', [`rider:${rid}`, 'riders:avail'], [rid]);
    db.doneRelease(rid);
  }
}

// chaos flag (dashboard "interrupt Redis"): every instance drops its Redis connections while the flag is set
let chaos = false;
setInterval(() => {
  const until = +db.getFlag('redis_chaos_until') || 0;
  const on = Date.now() < until;
  if (on !== chaos) { chaos = on; rc.block(on); rs.block(on); }
}, 400).unref();

// ---------- WebSocket registry ----------
const trackers = new Map();            // orderId -> Set<client>
const opsClients = new Set(), boardClients = new Set();
const opsDirty = new Set();
const knownInstances = new Map();

function sendAll(set, str) { const f = ws.frame(str); for (const c of set) c.sendFrame(f); }
function sendHb() {
  const f = ws.frame(JSON.stringify({ t: 'hb', ts: Date.now(), redis: redisUp && ready, inst: ID }));
  for (const set of trackers.values()) for (const c of set) c.sendFrame(f);
  for (const c of opsClients) c.sendFrame(f);
  for (const c of boardClients) c.sendFrame(f);
  if (!(redisUp && ready)) m.inc('hb_stale_sent');
}
setInterval(sendHb, 2000).unref();

rs.on('message', (ch, msg) => {
  if (ch === 'ops:loc') {
    const arr = JSON.parse(msg);
    for (const u of arr) { riders.set(u[0], { lat: u[1], lng: u[2], ts: u[3], seq: u[4], st: u[5] }); opsDirty.add(u[0]); }
  } else if (ch === 'orders:events') {
    const e = JSON.parse(msg);
    if (e.t === 'order') sendAll(boardOps(), JSON.stringify(e));
    else if (e.t === 'rider') { const r = riders.get(e.id); if (r) { r.st = e.st; opsDirty.add(e.id); } }
    else if (e.t === 'reset') { m.reset(); riders = new Map(); sendAll(boardOps(), msg); }
  } else if (ch.startsWith('track:')) {
    const set = trackers.get(ch.slice(6));
    if (!set) return;
    const f = ws.frame(msg);
    for (const c of set) c.sendFrame(f);
    m.inc('fanout_msgs', set.size);
  }
});
const boardOps = () => new Set([...opsClients, ...boardClients]);

async function publish(ch, str) {
  try { await rc.call('PUBLISH', ch, str); return true; }
  catch (e) {          // Redis down: deliver to this instance's own clients so the UI still reflects committed state
    if (ch === 'orders:events') { const ev = JSON.parse(str); if (ev.t === 'order') sendAll(boardOps(), str); }
    return false;
  }
}
const publishOrder = (order) => publish('orders:events', JSON.stringify({ t: 'order', order }));

setInterval(() => {              // ops map: send only riders that changed, at most once per second
  if (!opsClients.size) { opsDirty.clear(); return; }
  if (!opsDirty.size) return;
  const d = [];
  for (const id of opsDirty) { const r = riders.get(id); if (r) d.push([id, r.lat, r.lng, r.st, r.ts]); }
  opsDirty.clear();
  sendAll(opsClients, JSON.stringify({ t: 'riders', d }));
}, 1000).unref();
setInterval(() => { const cut = Date.now() - 120000; for (const [id, r] of riders) if (r.ts < cut) riders.delete(id); }, 10000).unref();

// ---------- location ingest ----------
const histBuf = [];
setInterval(() => { if (histBuf.length) { try { db.insertHistory(histBuf.splice(0, histBuf.length)); } catch (e) { console.error('history', e.message); } } }, 1000).unref();
const phase = (id) => { let h = 0; for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) >>> 0; return h % PERSIST_MS; };

async function ingest(list) {
  if (!redisUp || !ready) { m.inc('loc_rejected_redis_down', list.length); throw new HttpError(503, 'REDIS_UNAVAILABLE'); }
  const now = Date.now(), sec = Math.floor(now / 1000);
  const res = await Promise.all(list.map((u) =>
    rc.script('update_location', [`rider:${u.riderId}`, 'riders:avail', `rl:${u.riderId}:${sec}`],
      [u.riderId, u.lat, u.lng, u.ts, u.seq, now, PERSIST_MS, phase(u.riderId), RIDER_RATE]).catch((e) => e)));
  const out = { accepted: 0, stale: 0, rateLimited: 0, errors: 0 }, ops = [], pubs = [];
  res.forEach((r, i) => {
    const u = list[i];
    if (r instanceof Error) { out.errors++; return; }
    if (r[0] === 0) { if (r[1] === 'rate_limited') out.rateLimited++; else out.stale++; return; }
    out.accepted++;
    ops.push([u.riderId, u.lat, u.lng, u.ts, u.seq, r[1]]);
    if (r[2]) for (const oid of r[2].split(',')) pubs.push([`track:${oid}`, JSON.stringify({ t: 'loc', o: oid, r: u.riderId, lat: u.lat, lng: u.lng, ts: u.ts, seq: u.seq })]);
    if (r[3]) histBuf.push([u.riderId, u.lat, u.lng, u.ts, u.seq]);
  });
  m.inc('loc_accepted', out.accepted); m.inc('loc_rejected_stale', out.stale); m.inc('loc_rate_limited', out.rateLimited);
  if (out.errors) m.inc('loc_errors', out.errors);
  if (ops.length) publish('ops:loc', JSON.stringify(ops));
  for (const [ch, p] of pubs) publish(ch, p);
  return out;
}

function parseLocs(body) {
  const arr = Array.isArray(body) ? body : [body];
  if (arr.length > 2000) throw new HttpError(413, 'TOO_MANY_UPDATES');
  const ok = [];
  for (const u of arr) {
    if (!u || typeof u.riderId !== 'string' || u.riderId.length > 40 || !/^[\w.-]+$/.test(u.riderId) ||
        !(Math.abs(u.lat) <= 85) || !(Math.abs(u.lng) <= 180) || !Number.isFinite(u.ts) || !Number.isInteger(u.seq) || u.seq < 0) { m.inc('loc_invalid'); continue; }
    ok.push(u);
  }
  return ok;
}

// ---------- assignment ----------
async function claimNearest(orders, lat, lng) {
  const t0 = performance.now();
  const cands = await rc.call('GEOSEARCH', 'riders:avail', 'FROMLONLAT', lng, lat, 'BYRADIUS', RADIUS_M, 'm', 'ASC', 'COUNT', 8);
  m.observe('search_ms', performance.now() - t0);
  const batchId = orders.length > 1 ? 'B' + crypto.randomBytes(3).toString('hex') : '';
  for (const riderId of cands) {
    const r = await rc.script('claim', [`rider:${riderId}`, 'riders:avail', ...orders.map((o) => `assign:${o.id}`)],
      [Date.now(), MAX_AGE_MS, riderId, batchId, ...orders.map((o) => o.id)]);
    if (r[0] === 1) {
      let rows;
      try { rows = db.assign(orders.map((o) => o.id), riderId, batchId || null); }
      catch (e) {            // DB refused (e.g. order cancelled meanwhile): undo the Redis claim so nothing is half-assigned
        await rc.script('release', [`rider:${riderId}`, 'riders:avail'], [riderId]).catch(() => db.queueRelease(riderId));
        throw e;
      }
      m.observe('assign_ms', performance.now() - t0);
      publish('orders:events', JSON.stringify({ t: 'rider', id: riderId, st: 'busy' }));
      for (const o of rows) publishOrder(o);
      return { riderId, batchId: batchId || null, orders: rows };
    }
    if (r[1] === 'rider_unavailable') m.inc('assign_conflicts_prevented');        // another instance won this rider first
    if (r[1] === 'order_already_assigned') { m.inc('assign_conflicts_prevented'); throw new HttpError(409, 'ORDER_ALREADY_ASSIGNED'); }
  }
  throw new HttpError(404, 'NO_RIDER_AVAILABLE');
}
function loadAssignable(ids) {
  const orders = ids.map((id) => db.getOrder(id));
  for (const o of orders) {
    if (!o) throw new HttpError(404, 'ORDER_NOT_FOUND');
    if (o.rider_id) throw new HttpError(409, 'ALREADY_ASSIGNED', o.id);
    if (!dbm.ASSIGNABLE.includes(o.status)) throw new HttpError(409, 'ORDER_NOT_ASSIGNABLE', o.id);
  }
  return orders;
}
async function assignSingle(id) {
  gate();
  const [o] = loadAssignable([id]);
  try { const r = await claimNearest([o], o.pickup_lat, o.pickup_lng); m.inc('assign_ok'); return r; }
  catch (e) { m.inc('assign_failed'); throw e; }
}
async function assignBatch(ids) {
  gate();
  if (ids.length !== 2 || ids[0] === ids[1]) throw new HttpError(400, 'BATCH_NEEDS_TWO_DISTINCT_ORDERS');
  const orders = loadAssignable(ids);
  if (orders[0].restaurant_id !== orders[1].restaurant_id) throw new HttpError(400, 'BATCH_NEEDS_SAME_RESTAURANT');
  m.inc('batch_attempts');
  try { const r = await claimNearest(orders, orders[0].pickup_lat, orders[0].pickup_lng); m.inc('batch_ok'); return r; }
  catch (e) { m.inc('batch_failed_clean'); throw e; }
}
async function releaseIfIdle(riderId) {
  if (!riderId || db.activeForRider(riderId).length) return;
  try { await rc.script('release', [`rider:${riderId}`, 'riders:avail'], [riderId]); publish('orders:events', JSON.stringify({ t: 'rider', id: riderId, st: 'available' })); }
  catch (e) { db.queueRelease(riderId); }          // Redis down: remembered in the DB and applied on recovery
}
async function customerLimit(customerId) {
  if (!redisUp) return;                              // limiter fails open; order integrity does not depend on it
  const r = await rc.script('rate_limit', [`rl:cust:${customerId}`], [30, 10000]).catch(() => [1]);
  if (r[0] === 0) { m.inc('customer_rate_limited'); throw new HttpError(429, 'RATE_LIMITED'); }
}

// ---------- race fixtures (used by the dashboard and tests to prove "no double assignment") ----------
const otherPeer = () => PEERS.find((p) => !p.endsWith(':' + PORT));
async function assignVia(url, path, body, local) {
  if (!url) return local();
  const r = await fetch(url + path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  if (!r.ok) throw new Error(String(r.status));
  return r.json();
}
async function race(kind, trials) {
  const tag = crypto.randomBytes(2).toString('hex');
  const out = { kind, trials, exactly_one: 0, violations: 0 };
  for (let i = 0; i < trials; i++) {
    const rid = `RACE-${tag}-${i}`, lat = 30 + i * 0.25, lng = 70 + i * 0.25;
    await ingest([{ riderId: rid, lat, lng, ts: Date.now(), seq: 1 }]);
    const mk = (r) => db.createOrder({ restaurantId: r, customerId: 'race', pickup: { lat, lng }, dropoff: { lat: lat + 0.01, lng }, }, null).order;
    const peer = otherPeer();
    let ids, results, assigned;
    if (kind === 'single') {
      const [a, b] = [mk('race'), mk('race')]; ids = [a.id, b.id];
      results = await Promise.allSettled([assignSingle(a.id), assignVia(peer, `/api/orders/${b.id}/assign`, {}, () => assignSingle(b.id))]);
      assigned = ids.filter((id) => db.getOrder(id).rider_id === rid).length;
      const ok = results.filter((r) => r.status === 'fulfilled').length;
      if (ok === 1 && assigned === 1) out.exactly_one++; else out.violations++;
    } else {
      const rest = 'race-b' + i, o = [mk(rest), mk(rest), mk(rest), mk(rest)]; ids = o.map((x) => x.id);
      results = await Promise.allSettled([assignBatch([ids[0], ids[1]]), assignVia(peer, '/api/assign/batch', { orderIds: [ids[2], ids[3]] }, () => assignBatch([ids[2], ids[3]]))]);
      const rows = ids.map((id) => db.getOrder(id));
      const assignedRows = rows.filter((r) => r.rider_id === rid);
      const okCount = results.filter((r) => r.status === 'fulfilled').length;
      const sameBatch = new Set(assignedRows.map((r) => r.batch_id)).size === 1;
      if (okCount === 1 && assignedRows.length === 2 && sameBatch) out.exactly_one++; else out.violations++;
    }
    m.inc(kind === 'single' ? 'race_single_trials' : 'race_batch_trials');
    if (out.violations) m.inc(kind === 'single' ? 'race_single_violations' : 'race_batch_violations', 0);
    db.finishRace(ids);
    await rc.call('DEL', `rider:${rid}`, ...ids.map((id) => `assign:${id}`)).catch(() => {});
    await rc.call('ZREM', 'riders:avail', rid).catch(() => {});
    riders.delete(rid);
  }
  m.inc(kind === 'single' ? 'race_single_violations' : 'race_batch_violations', out.violations);
  return out;
}

// ---------- metrics for the dashboard ----------
function selfSnapshot() {
  m.gauges.viewers_track = [...trackers.values()].reduce((a, s) => a + s.size, 0);
  m.gauges.viewers_ops = opsClients.size; m.gauges.viewers_board = boardClients.size;
  return m.snapshot(ID);
}
setInterval(() => { if (redisUp) rc.call('HSET', 'instances', ID, JSON.stringify(selfSnapshot())).catch(() => {}); }, 1000).unref();

async function cluster() {
  const mine = selfSnapshot();
  const all = new Map([[ID, mine]]);
  if (redisUp) {
    try {
      const h = await rc.call('HGETALL', 'instances');
      for (let i = 0; i < h.length; i += 2) { const s = JSON.parse(h[i + 1]); if (s.id !== ID) all.set(s.id, s); }
    } catch (e) {}
  }
  const now = Date.now();
  for (const [id, s] of all) knownInstances.set(id, s);
  const instances = [...knownInstances.entries()].map(([id, s]) => ({ id, up: id === ID || now - s.ts < 4000, viewers: s.gauges.viewers_track || 0 }));
  const live = [...all.values()].filter((s) => s.id === ID || now - s.ts < 4000);
  const mg = merge(live);
  const t = mg.totals, h = mg.hists;
  const cnt = db.counters(), inv = db.invariants();
  const hb = +db.getFlag('worker_hb') || 0;
  const wstate = now - hb > 3000 ? 'down' : db.getFlag('notifier_paused') === '1' ? 'paused' : 'up';
  let active = 0; for (const r of riders.values()) if (now - r.ts < 5000) active++;
  return {
    ts: now, instance: ID, instances, redis: { up: redisUp, ready, chaos },
    riders: { active, known: riders.size },
    rates: { loc_in: mg.rates.loc_accepted || 0, db_loc_writes: mg.rates.db_loc_writes || 0, loc_rejected: (mg.rates.loc_rejected_stale || 0), rate_limited: mg.rates.loc_rate_limited || 0 },
    totals: t,
    latency: {
      delivery_p95: pctl(h.delivery_ms, 0.95), delivery_n: count(h.delivery_ms),
      search_p95: pctl(h.search_ms, 0.95), search_n: count(h.search_ms),
      assign_p95: pctl(h.assign_ms, 0.95), assign_n: count(h.assign_ms),
      reconnect_p95: pctl(h.reconnect_ms, 0.95), reconnect_n: count(h.reconnect_ms),
    },
    viewers: { total: mg.gauges.viewers_track || 0, ops: mg.gauges.viewers_ops || 0, board: mg.gauges.viewers_board || 0 },
    notif: { pending: db.pendingCount(), processed: cnt.notif_processed || 0, dup_suppressed: cnt.notif_dup_suppressed || 0, worker: wstate },
    inv,
  };
}

// ---------- HTTP ----------
const CORS = { 'access-control-allow-origin': '*', 'access-control-allow-headers': '*', 'access-control-allow-methods': 'GET,POST,OPTIONS' };
function send(res, code, obj) { res.writeHead(code, { 'content-type': 'application/json', ...CORS }); res.end(JSON.stringify(obj)); }
function readBody(req) {
  return new Promise((resolve, reject) => {
    const ch = []; let n = 0;
    req.on('data', (d) => { n += d.length; if (n > 2e6) { reject(new HttpError(413, 'BODY_TOO_LARGE')); req.destroy(); } else ch.push(d); });
    req.on('end', () => { if (!n) return resolve({}); try { resolve(JSON.parse(Buffer.concat(ch).toString())); } catch (e) { reject(new HttpError(400, 'BAD_JSON')); } });
    req.on('error', reject);
  });
}
const num = (x) => typeof x === 'number' && Number.isFinite(x);
const point = (p) => p && num(p.lat) && num(p.lng) && Math.abs(p.lat) <= 85 && Math.abs(p.lng) <= 180;
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };

async function route(req, res) {
  const u = new URL(req.url, 'http://x');
  const p = u.pathname, M = req.method;
  if (M === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }
  let mt;
  if (p.startsWith('/api/track/')) { m.inc('polling_requests'); return send(res, 426, { error: 'USE_WEBSOCKET', detail: 'Tracking is push-only. Open /ws?kind=track&order=<id>.' }); }
  if (M === 'GET' && p === '/api/config') return send(res, 200, { instance: ID, peers: PEERS });
  if (M === 'GET' && p === '/api/health') return send(res, 200, { ok: true, instance: ID, redis: redisUp && ready });
  if (M === 'GET' && p === '/api/metrics') return send(res, 200, await cluster());
  if (M === 'GET' && p === '/api/orders') return send(res, 200, { orders: db.listOrders(300) });
  if ((mt = p.match(/^\/api\/orders\/([\w-]+)$/)) && M === 'GET') { const o = db.getOrder(mt[1]); return o ? send(res, 200, { order: o, notifications: db.notificationsFor(o.id) }) : send(res, 404, { error: 'ORDER_NOT_FOUND' }); }

  if (M === 'POST' && p === '/api/orders') {
    const b = await readBody(req);
    if (typeof b.restaurantId !== 'string' || typeof b.customerId !== 'string' || !point(b.pickup) || !point(b.dropoff)) throw new HttpError(400, 'INVALID_ORDER');
    await customerLimit(b.customerId);
    const { order, replay } = db.createOrder(b, req.headers['idempotency-key'] || null);
    if (!replay) publishOrder(order);
    return send(res, replay ? 200 : 201, { order, replay });
  }
  if ((mt = p.match(/^\/api\/orders\/([\w-]+)\/status$/)) && M === 'POST') {
    const b = await readBody(req);
    if (typeof b.to !== 'string') throw new HttpError(400, 'INVALID_STATUS');
    const o0 = db.getOrder(mt[1]);
    if (o0) await customerLimit(o0.customer_id);
    const r = db.transition(mt[1], b.to, b.requestId || req.headers['idempotency-key'] || null);
    if (!r.noop) { publishOrder(r.order); if (dbm.TERMINAL.includes(r.order.status)) await releaseIfIdle(r.order.rider_id); }
    return send(res, 200, r);
  }
  if ((mt = p.match(/^\/api\/orders\/([\w-]+)\/assign$/)) && M === 'POST') return send(res, 200, await assignSingle(mt[1]));
  if (M === 'POST' && p === '/api/assign/batch') { const b = await readBody(req); if (!Array.isArray(b.orderIds)) throw new HttpError(400, 'INVALID_BATCH'); return send(res, 200, await assignBatch(b.orderIds)); }

  if (M === 'POST' && (p === '/api/rider/location' || p === '/api/rider/locations')) {
    const list = parseLocs(await readBody(req));
    return send(res, 200, list.length ? await ingest(list) : { accepted: 0 });
  }

  if (M === 'GET' && p === '/api/invariants') return send(res, 200, db.invariants());
  if (p.startsWith('/api/admin/') && M === 'POST') {
    const b = await readBody(req);
    if (p === '/api/admin/flag') { if (typeof b.key !== 'string') throw new HttpError(400, 'KEY'); db.setFlag(b.key, b.value); return send(res, 200, { ok: true }); }
    if (p === '/api/admin/redis-outage') { db.setFlag('redis_chaos_until', Date.now() + (+b.seconds || 15) * 1000); return send(res, 200, { ok: true }); }
    if (p === '/api/admin/redeliver') return send(res, 200, { reset: db.redeliver(+b.n || 3) });
    if (p === '/api/admin/kill') { send(res, 200, { ok: true, instance: ID }); return setTimeout(() => process.exit(137), 30); }
    if (p === '/api/admin/race') return send(res, 200, await race(b.kind === 'batch' ? 'batch' : 'single', Math.min(+b.trials || 10, 50)));
    if (p === '/api/admin/reset') {
      db.reset(); m.reset(); riders = new Map();
      await rc.call('FLUSHDB').catch(() => {});
      await publish('orders:events', JSON.stringify({ t: 'reset' }));
      return send(res, 200, { ok: true });
    }
    throw new HttpError(404, 'NOT_FOUND');
  }

  if (M === 'GET') {
    let f = p === '/' ? '/index.html' : p;
    const full = path.normalize(path.join(PUBLIC, f));
    if (full.startsWith(PUBLIC) && fs.existsSync(full) && fs.statSync(full).isFile()) {
      res.writeHead(200, { 'content-type': MIME[path.extname(full)] || 'application/octet-stream', 'cache-control': 'no-store' });
      return fs.createReadStream(full).pipe(res);
    }
  }
  throw new HttpError(404, 'NOT_FOUND');
}

const server = http.createServer((req, res) => {
  route(req, res).catch((e) => {
    if (e.status) return send(res, e.status, { error: e.code, detail: e.detail });
    if (/REDIS_(DOWN|TIMEOUT)/.test(e.message)) return send(res, 503, { error: 'REDIS_UNAVAILABLE' });
    console.error(ID, e); send(res, 500, { error: 'INTERNAL' });
  });
});

// ---------- WebSocket endpoints ----------
function addTracker(c, orderId) {
  let set = trackers.get(orderId);
  if (!set) { set = new Set(); trackers.set(orderId, set); rs.subscribe('track:' + orderId); }
  set.add(c);
}
function dropTracker(c) {
  const set = trackers.get(c.orderId);
  if (!set) return;
  set.delete(c);
  if (!set.size) { trackers.delete(c.orderId); rs.unsubscribe('track:' + c.orderId); }
}
async function snapshotFor(orderId) {
  const order = db.getOrder(orderId);
  const snap = { t: 'snap', inst: ID, order, notifications: db.notificationsFor(orderId), redis: redisUp && ready, loc: null };
  if (order && order.rider_id && redisUp) {
    try { const v = await rc.call('HMGET', `rider:${order.rider_id}`, 'lat', 'lng', 'ts', 'seq'); if (v[0]) snap.loc = { r: order.rider_id, lat: +v[0], lng: +v[1], ts: +v[2], seq: +v[3] }; } catch (e) {}
  }
  return snap;
}
server.on('upgrade', (req, socket) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname !== '/ws') return socket.destroy();
  const c = ws.upgrade(req, socket);
  if (!c) return;
  const kind = u.searchParams.get('kind');
  c.kind = kind;
  c.on('slow', () => m.inc('ws_slow_dropped'));
  c.on('message', (s) => {
    let d; try { d = JSON.parse(s); } catch (e) { return; }
    if (d.t === 'lat' && d.ms >= 0 && d.ms < 600000) m.observe('delivery_ms', d.ms);
    else if (d.t === 'reconnect' && d.ms >= 0) { m.observe('reconnect_ms', d.ms); m.inc('reconnects'); }
    else if (d.t === 'back') m.inc('marker_backwards');
    else if (d.t === 'dup') m.inc('notif_dup_visible');
    else if (d.t === 'snap') snapshotFor(c.orderId).then((x) => c.send(JSON.stringify(x)));
  });
  c.on('close', () => { if (kind === 'track') dropTracker(c); opsClients.delete(c); boardClients.delete(c); });
  if (kind === 'track') {
    const oid = u.searchParams.get('order') || '';
    if (!db.getOrder(oid)) { c.send(JSON.stringify({ t: 'err', error: 'ORDER_NOT_FOUND' })); return c.terminate(); }
    c.orderId = oid; addTracker(c, oid);
    snapshotFor(oid).then((x) => c.send(JSON.stringify(x)));
  } else if (kind === 'ops') {
    opsClients.add(c);
    const rows = []; for (const [id, r] of riders) rows.push([id, r.lat, r.lng, r.st, r.ts]);
    c.send(JSON.stringify({ t: 'snap', inst: ID, riders: rows, orders: db.listOrders(100), redis: redisUp && ready }));
  } else if (kind === 'board') {
    boardClients.add(c);
    c.send(JSON.stringify({ t: 'snap', inst: ID, orders: db.listOrders(100), redis: redisUp && ready }));
  } else c.terminate();
});
setInterval(async () => { if (opsClients.size) { try { sendAll(opsClients, JSON.stringify({ t: 'metrics', d: await cluster() })); } catch (e) { console.error(e.message); } } }, 1000).unref();

server.listen(PORT, () => console.log(`${ID} listening on :${PORT} (redis ${RURL.host}, db ${DB_PATH})`));
process.on('SIGTERM', () => process.exit(0));
