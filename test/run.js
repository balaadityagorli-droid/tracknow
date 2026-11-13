'use strict';
// End-to-end scenarios against a freshly started stack (2 app instances + worker + Redis).
//   node test/run.js            (uses the dev Redis stand-in)   |   node test/run.js --real-redis
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const { spawn } = require('node:child_process');
const { startStack } = require('../dev/stack');

const real = process.argv.includes('--real-redis');
const P = [3101, 3102];
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'tracknow-'));
const stack = startStack({ ports: P, miniRedis: !real, redisPort: real ? 6379 : 6392, dbPath: path.join(tmp, 't.db'), respawnMs: 2500, quiet: true });
const results = [];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const check = (name, ok, detail = '') => { results.push({ name, ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? '  — ' + detail : ''}`); };
const api = async (port, method, p, body, headers = {}) => {
  const r = await fetch(`http://localhost:${port}${p}`, { method, headers: { 'content-type': 'application/json', ...headers }, body: body ? JSON.stringify(body) : undefined });
  return { status: r.status, body: await r.json().catch(() => ({})) };
};
const metrics = async (port = P[1]) => (await api(port, 'GET', '/api/metrics')).body;
async function waitFor(fn, ms = 10000, step = 200) { const t = Date.now(); while (Date.now() - t < ms) { try { const v = await fn(); if (v) return v; } catch (e) {} await sleep(step); } return false; }
const child = [];
function run(file, args) { const c = spawn(process.execPath, ['--no-warnings', path.join(__dirname, '..', file), ...args], { stdio: ['ignore', 'pipe', 'pipe'] }); let out = ''; c.stdout.on('data', (d) => (out += d)); c.stderr.on('data', (d) => (out += d)); c.out = () => out; child.push(c); return c; }
const send = (port, list) => api(port, 'POST', '/api/rider/locations', list);
const mkOrder = (port, lat, lng, extra = {}) => api(port, 'POST', '/api/orders', { restaurantId: 'rest-1', customerId: 'c-' + Math.random().toString(36).slice(2), pickup: { lat, lng }, dropoff: { lat: lat + 0.01, lng }, ...extra });
function watch(port, orderId) {
  const w = { msgs: [], ws: null, closed: false };
  w.ws = new WebSocket(`ws://localhost:${port}/ws?kind=track&order=${orderId}`);
  w.ws.onmessage = (e) => w.msgs.push(JSON.parse(e.data));
  w.ws.onclose = () => (w.closed = true);
  return w;
}

(async () => {
  try {
    for (const p of P) if (!(await waitFor(async () => (await api(p, 'GET', '/api/health')).body.redis === true, 15000))) throw new Error('instance :' + p + ' not ready');
    await api(P[0], 'POST', '/api/admin/reset', {});
    await sleep(500);

    // 1 ── load + two instances
    const riders = run('sim/riders.js', ['--riders', '150', '--urls', P.map((p) => `http://localhost:${p}`).join(','), '--control', '4301']);
    await sleep(9000);                                   // let the 5 s rolling rate window fill
    let m = await metrics();
    check('150 riders reporting once per second across both instances', m.riders.active >= 140 && m.rates.loc_in > 135, `active=${m.riders.active} accepted/s=${m.rates.loc_in.toFixed(0)}`);
    check('durable location writes stay far below 50/s', m.rates.db_loc_writes < 50, `db_loc_writes/s=${m.rates.db_loc_writes.toFixed(2)} (history throttled to 1 row/rider/90s)`);

    // 2 ── competing assignments
    const rs = (await api(P[0], 'POST', '/api/admin/race', { kind: 'single', trials: 15 })).body;
    check('15 simultaneous single-assignment races (one per instance) → exactly one winner each', rs.exactly_one === 15 && rs.violations === 0, JSON.stringify(rs));
    const rb = (await api(P[1], 'POST', '/api/admin/race', { kind: 'batch', trials: 10 })).body;
    check('10 competing batch races → exactly one batch wins, both orders or neither', rb.exactly_one === 10 && rb.violations === 0, JSON.stringify(rb));

    // 3 ── GPS validation + marker never backwards (isolated rider so the order is deterministic)
    const T = 'T-gps';
    await send(P[0], [{ riderId: T, lat: 40, lng: 60, ts: Date.now(), seq: 1 }]);
    const ord = (await mkOrder(P[0], 40.0001, 60)).body.order;
    const asg = await api(P[1], 'POST', `/api/orders/${ord.id}/assign`, {});
    check('nearest available rider assigned (isolated rider T-gps)', asg.status === 200 && asg.body.riderId === T, `status=${asg.status}`);
    const v = watch(P[1], ord.id);
    await waitFor(() => v.msgs.some((x) => x.t === 'snap'), 3000);
    const t0 = Date.now(), before = (await metrics()).totals.loc_rejected_stale || 0;
    const samples = [[2, 40.001, t0 + 1], [3, 40.002, t0 + 2], [3, 40.0025, t0 + 3] /*dup*/, [2, 40.001, t0 + 4] /*older seq*/, [9, 40.0005, t0 - 9000] /*newer seq, older clock*/, [4, 40.003, t0 + 5]];
    for (const [seq, lat, ts] of samples) { await send(P[seq % 2], [{ riderId: T, lat, lng: 60, ts, seq }]); await sleep(120); }
    await sleep(500);
    const locs = v.msgs.filter((x) => x.t === 'loc');
    const seqs = locs.map((x) => x.seq), lats = locs.map((x) => x.lat);
    check('viewer receives only valid samples, strictly forward (seq 2,3,4)', JSON.stringify(seqs) === '[2,3,4]', `seqs=${JSON.stringify(seqs)}`);
    check('marker latitude only ever increases (never moves backwards)', lats.every((x, i) => i === 0 || x > lats[i - 1]));
    check('duplicate, older and older-clock samples were counted as rejected', ((await metrics()).totals.loc_rejected_stale || 0) - before >= 3);
    const snapLoc = (await (async () => { const w = watch(P[0], ord.id); await waitFor(() => w.msgs.some((x) => x.t === 'snap'), 3000); w.ws.close(); return w.msgs.find((x) => x.t === 'snap'); })()).loc;
    check('a reconnecting viewer is resynced to the latest accepted position', snapLoc && snapLoc.seq === 4, `snapshot seq=${snapLoc && snapLoc.seq}`);
    v.ws.close();

    // 4 ── lifecycle + notifications across a worker outage
    const lc = (await mkOrder(P[0], 17.4, 78.4)).body.order;
    const bad = await api(P[0], 'POST', `/api/orders/${lc.id}/status`, { to: 'delivered' });
    check('invalid transition placed → delivered is rejected by the server', bad.status === 409 && bad.body.error === 'INVALID_TRANSITION');
    await waitFor(async () => (await metrics()).notif.pending === 0, 5000);
    const nv = watch(P[1], lc.id);
    await waitFor(() => nv.msgs.some((x) => x.t === 'snap'), 3000);
    stack.kill('worker');
    await sleep(300);
    const rid = 'req-1';
    await api(P[0], 'POST', `/api/orders/${lc.id}/status`, { to: 'accepted', requestId: rid });
    const rep = await api(P[1], 'POST', `/api/orders/${lc.id}/status`, { to: 'accepted', requestId: rid });
    await api(P[1], 'POST', `/api/orders/${lc.id}/status`, { to: 'preparing', requestId: 'req-2' });
    check('a repeated status request is ignored (no second event)', rep.body.noop === true);
    await sleep(800);
    const mid = await metrics();
    check('while the worker is down, updates stay pending and the customer sees none', mid.notif.pending >= 2 && !nv.msgs.some((x) => x.t === 'notify'), `pending=${mid.notif.pending}, worker=${mid.notif.worker}`);
    const back = await waitFor(() => nv.msgs.filter((x) => x.t === 'notify').length >= 2, 12000);
    const vers = nv.msgs.filter((x) => x.t === 'notify').map((x) => x.v);
    check('after the worker restarts, each status arrives once and in order', back && JSON.stringify(vers) === '[2,3]', `versions=${JSON.stringify(vers)}`);
    const dupBefore = (await metrics()).notif.dup_suppressed;
    await api(P[0], 'POST', '/api/admin/redeliver', { n: 3 });
    await sleep(1200);
    const after = await metrics();
    check('forced redelivery of 3 events is suppressed (no repeat reaches the customer)', after.notif.dup_suppressed - dupBefore === 3 && nv.msgs.filter((x) => x.t === 'notify').length === 2, `suppressed +${after.notif.dup_suppressed - dupBefore}`);
    nv.ws.close();

    // 5 ── instance failover
    const viewers = run('sim/viewers.js', ['--viewers', '200', '--orders', '20', '--urls', P.map((p) => `http://localhost:${p}`).join(',')]);
    await sleep(9000);
    m = await metrics(P[1]);
    check('viewers connected across both instances', m.viewers.total >= 180 && m.instances.filter((i) => i.up).length === 2, `viewers=${m.viewers.total}`);
    check('measured map delay p95 under 2 s', m.latency.delivery_n > 20 && m.latency.delivery_p95 < 2000, `p95≤${m.latency.delivery_p95} ms over ${m.latency.delivery_n} samples (dev Redis, 1 core)`);
    stack.kill('app1');
    const rec = await waitFor(async () => (await metrics(P[1])).latency.reconnect_n >= 80, 12000, 300);
    m = await metrics(P[1]);
    check('after killing app1, its viewers reconnect to app2 within 5 s', rec && m.latency.reconnect_p95 < 5000, `reconnects=${m.latency.reconnect_n}, p95≤${m.latency.reconnect_p95} ms`);
    check('no viewer ever saw a rider marker move backwards', (m.totals.marker_backwards || 0) === 0, `marker_backwards=${m.totals.marker_backwards || 0}`);
    check('polling counter stays zero for push-only tracking', (m.totals.polling_requests || 0) === 0);
    const poll = await api(P[1], 'GET', '/api/track/anything');
    check('a polling attempt is refused (426) and counted', poll.status === 426 && (await metrics(P[1])).totals.polling_requests === 1);
    viewers.kill('SIGKILL');
    await waitFor(async () => (await api(P[0], 'GET', '/api/health')).body.redis === true, 15000);

    // 6 ── Redis interruption (connection loss) + Redis data loss (restart empty)
    const o2 = (await mkOrder(P[1], 17.41, 78.41)).body.order;
    await api(P[1], 'POST', '/api/admin/redis-outage', { seconds: 5 });
    await waitFor(async () => (await api(P[1], 'GET', '/api/health')).body.redis === false, 4000, 100);
    const refused = await api(P[1], 'POST', `/api/orders/${o2.id}/assign`, {});
    const inDb = (await api(P[1], 'GET', `/api/orders/${o2.id}`)).body.order;
    check('during a Redis outage, assignment is refused (503) and nothing is half-assigned', refused.status === 503 && !inDb.rider_id, `status=${refused.status}`);
    const stillOrders = await api(P[1], 'POST', '/api/orders', { restaurantId: 'rest-1', customerId: 'c-out', pickup: { lat: 17.4, lng: 78.4 }, dropoff: { lat: 17.41, lng: 78.4 } });
    check('orders and status changes keep working from the database during the outage', stillOrders.status === 201);
    const healed = await waitFor(async () => (await api(P[1], 'GET', '/api/health')).body.redis === true, 15000, 200);
    check('after the outage, instances recover on their own', !!healed);
    const okAgain = await api(P[1], 'POST', `/api/orders/${o2.id}/assign`, {});
    check('assignment works again after recovery', okAgain.status === 200, `status=${okAgain.status}`);

    const X = 'T-wipe';
    await send(P[0], [{ riderId: X, lat: 45, lng: 60, ts: Date.now(), seq: 1 }]);
    const ox = (await mkOrder(P[0], 45.0001, 60)).body.order;
    const ax = await api(P[0], 'POST', `/api/orders/${ox.id}/assign`, {});
    stack.kill('redis');                               // Redis comes back EMPTY after respawn
    await sleep(500);
    await waitFor(async () => (await api(P[0], 'GET', '/api/health')).body.redis === false, 5000, 100);
    const ready = await waitFor(async () => (await api(P[0], 'GET', '/api/health')).body.redis === true && (await api(P[1], 'GET', '/api/health')).body.redis === true, 20000, 300);
    await send(P[0], [{ riderId: X, lat: 45, lng: 60, ts: Date.now(), seq: 50 }]);   // rider re-announces itself after the wipe
    const oy = (await mkOrder(P[0], 45.0002, 60)).body.order;
    const ay = await api(P[0], 'POST', `/api/orders/${oy.id}/assign`, {});
    check('after Redis restarts EMPTY, the busy rider is rebuilt from the DB and is not assigned twice', ax.status === 200 && ready && ay.status === 404, `second assign → ${ay.status} ${ay.body.error || ''}`);

    // 7 ── invariants from the database
    const inv = (await api(P[0], 'GET', '/api/invariants')).body;
    check('database invariant: no rider has conflicting active orders', inv.double_assign_violations === 0, JSON.stringify(inv));
    check('database invariant: no partial batches, notifications recorded in order with none missing', inv.partial_batches === 0 && inv.notif_out_of_order === 0 && inv.notif_missing === 0);
  } catch (e) { console.error('test harness error:', e); check('harness', false, e.message); }
  finally {
    for (const c of child) c.kill('SIGKILL');
    stack.stop();
    const failed = results.filter((r) => !r.ok).length;
    console.log(`\n${results.length - failed}/${results.length} checks passed`);
    process.exit(failed ? 1 : 0);
  }
})();
