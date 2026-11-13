'use strict';
// Viewer load generator: creates assigned orders, then opens many tracking WebSockets spread over all instances.
// Each viewer checks that the rider seq never goes backwards and reports delivery latency / reconnect time.
//   node sim/viewers.js --viewers 3000 --orders 300 --urls http://localhost:3001,http://localhost:3002
const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? process.argv[i + 1] : d; };
const V = +arg('viewers', 3000), O = +arg('orders', 300), CHURN = +arg('churn', 0);
const URLS = arg('urls', 'http://localhost:3001,http://localhost:3002').split(',');
const SAMPLE = +arg('sample', 0.05);
const rnd = (a, b) => a + Math.random() * (b - a);
const stats = { connected: 0, msgs: 0, lat: [], back: 0, dupNotify: 0, reconnects: 0 };

async function api(base, path, body, method = 'POST') {
  const r = await fetch(base + path, { method, headers: { 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  return { status: r.status, body: await r.json().catch(() => ({})) };
}
async function makeOrders() {
  const ids = []; let i = 0;
  const worker = async () => {
    while (ids.length < O && i < O * 3) {
      const n = i++, base = URLS[n % URLS.length];
      const c = await api(base, '/api/orders', { restaurantId: 'sim-rest', customerId: 'sim-cust-' + n, pickup: { lat: rnd(17.37, 17.49), lng: rnd(78.36, 78.54) }, dropoff: { lat: 17.43, lng: 78.45 } });
      if (c.status !== 201) continue;
      const a = await api(base, `/api/orders/${c.body.order.id}/assign`, {});
      if (a.status === 200) ids.push(c.body.order.id);
    }
  };
  await Promise.all(Array.from({ length: 16 }, worker));
  return ids.slice(0, O);
}

function viewer(orderId, k) {
  const v = { orderId, k, lastSeq: -1, lastVer: 0, closedAt: 0, ws: null, stop: false };
  const connect = () => {
    const base = URLS[v.k % URLS.length].replace('http', 'ws');
    const ws = new WebSocket(`${base}/ws?kind=track&order=${orderId}`);
    v.ws = ws;
    ws.onopen = () => { stats.connected++; };
    ws.onmessage = (e) => {
      const d = JSON.parse(e.data); stats.msgs++;
      if (d.t === 'snap') {
        if (v.closedAt) { const ms = Date.now() - v.closedAt; v.closedAt = 0; stats.reconnects++; ws.send(JSON.stringify({ t: 'reconnect', ms })); }
        if (d.loc) v.lastSeq = Math.max(v.lastSeq, d.loc.seq);
        for (const n of d.notifications || []) v.lastVer = Math.max(v.lastVer, n.version);
      } else if (d.t === 'loc') {
        if (d.seq < v.lastSeq) { stats.back++; ws.send('{"t":"back"}'); }       // strictly older = marker would move backwards
        else if (d.seq === v.lastSeq) return;                                       // same sample twice (snapshot + live): ignore
        else v.lastSeq = d.seq;
        if (Math.random() < SAMPLE) { const ms = Date.now() - d.ts; stats.lat.push(ms); ws.send(JSON.stringify({ t: 'lat', ms })); }
      } else if (d.t === 'notify') {
        if (d.v <= v.lastVer) { stats.dupNotify++; ws.send('{"t":"dup"}'); } else v.lastVer = d.v;
      }
    };
    ws.onclose = () => {
      stats.connected = Math.max(0, stats.connected - 1);
      if (v.stop) return;
      if (!v.closedAt) v.closedAt = Date.now();
      v.k++;                                           // rotate to the next instance
      setTimeout(connect, 50 + Math.random() * 150);
    };
    ws.onerror = () => {};
  };
  connect();
  return v;
}

(async () => {
  console.log(`creating up to ${O} assigned orders…`);
  const ids = await makeOrders();
  if (!ids.length) { console.error('no orders could be assigned — are riders running?'); process.exit(1); }
  console.log(`${ids.length} orders ready; opening ${V} viewers`);
  const vs = [];
  for (let i = 0; i < V; i++) { vs.push(viewer(ids[i % ids.length], i)); if (i % 100 === 99) await new Promise((r) => setTimeout(r, 20)); }
  if (CHURN) setInterval(() => { for (let i = 0; i < Math.max(1, V * CHURN); i++) { const w = vs[Math.floor(Math.random() * vs.length)]; if (w.ws && w.ws.readyState === 1) w.ws.close(); } }, 2000);
  let last = 0;
  setInterval(() => {
    const l = stats.lat.splice(0).sort((a, b) => a - b), p95 = l.length ? l[Math.floor(l.length * 0.95)] : null;
    console.log(`[viewers] connected=${stats.connected} msgs/s=${((stats.msgs - last) / 5).toFixed(0)} p95=${p95}ms (n=${l.length}) backwards=${stats.back} dupNotify=${stats.dupNotify} reconnects=${stats.reconnects}`);
    last = stats.msgs;
  }, 5000);
})();
