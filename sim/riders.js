'use strict';
// Rider simulator: N riders, one location per second each, with deliberate bad-GPS injection.
// Features realistic Hyderabad road clustering and active order route synchronization.
//   node sim/riders.js --riders 2000 --urls http://localhost:3001,http://localhost:3002
const http = require('node:http');

const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? process.argv[i + 1] : d; };
const N = +arg('riders', 2000);
const URLS = arg('urls', 'http://localhost:3001,http://localhost:3002').split(',');
const CTL = +arg('control', 4200);
const CHUNK = +arg('chunk', 250);

// Key Hyderabad delivery hubs
const HUBS = [
  { name: 'Hitec City', lat: 17.4486, lng: 78.3752, weight: 0.22 },
  { name: 'Gachibowli', lat: 17.4399, lng: 78.3489, weight: 0.16 },
  { name: 'Jubilee Hills', lat: 17.4325, lng: 78.4121, weight: 0.18 },
  { name: 'Banjara Hills', lat: 17.4162, lng: 78.4423, weight: 0.14 },
  { name: 'Begumpet', lat: 17.4391, lng: 78.4563, weight: 0.10 },
  { name: 'Secunderabad', lat: 17.4412, lng: 78.5015, weight: 0.08 },
  { name: 'Ameerpet', lat: 17.4374, lng: 78.4482, weight: 0.05 },
  { name: 'Charminar', lat: 17.3616, lng: 78.4747, weight: 0.04 },
  { name: 'Kukatpally', lat: 17.4932, lng: 78.3984, weight: 0.03 }
];

const rnd = (a, b) => a + Math.random() * (b - a);

function pickHub() {
  const r = Math.random();
  let acc = 0;
  for (const h of HUBS) {
    acc += h.weight;
    if (r <= acc) return h;
  }
  return HUBS[0];
}

// Generate riders seeded around real road clusters
const riders = Array.from({ length: N }, (_, i) => {
  const hub = pickHub();
  const radius = rnd(0.002, 0.022);
  const angle = rnd(0, 6.28);
  const lat = hub.lat + Math.cos(angle) * radius;
  const lng = hub.lng + Math.sin(angle) * radius;
  return {
    id: 'R' + String(i + 1).padStart(4, '0'),
    lat,
    lng,
    hdg: rnd(0, 6.28),
    seq: 0,
    hubLat: hub.lat,
    hubLng: hub.lng,
    prev: null,
    last: null,
    offlineUntil: 0,
  };
});

const riderMap = new Map(riders.map(r => [r.id, r]));
const chunks = [];
for (let i = 0; i < N; i += CHUNK) chunks.push(riders.slice(i, i + CHUNK));
const chaos = { dup: 0, late: 0, oldts: 0, burst: 0 };
const stats = { sent: 0, accepted: 0, stale: 0, rateLimited: 0, errors: 0, refused: 0 };
let tick = 0;

// Active order targets: riderId -> { status, targetLat, targetLng }
const activeTargets = new Map();

async function pollActiveOrders() {
  for (const base of URLS) {
    try {
      const res = await fetch(base + '/api/orders').catch(() => null);
      if (res && res.ok) {
        const data = await res.json();
        if (Array.isArray(data.orders)) {
          const seen = new Set();
          for (const o of data.orders) {
            if (!o.rider_id) continue;
            seen.add(o.rider_id);
            if (o.status === 'placed' || o.status === 'accepted' || o.status === 'preparing') {
              activeTargets.set(o.rider_id, {
                status: o.status,
                targetLat: o.pickup_lat,
                targetLng: o.pickup_lng
              });
            } else if (o.status === 'picked_up') {
              activeTargets.set(o.rider_id, {
                status: o.status,
                targetLat: o.drop_lat,
                targetLng: o.drop_lng
              });
            } else {
              activeTargets.delete(o.rider_id);
            }
          }
          for (const rid of activeTargets.keys()) {
            if (!seen.has(rid)) activeTargets.delete(rid);
          }
          return;
        }
      }
    } catch (e) {}
  }
}
setInterval(pollActiveOrders, 2500).unref();

function step(r) {
  r.prev = { lat: r.lat, lng: r.lng };
  const target = activeTargets.get(r.id);

  if (target) {
    // Rider is assigned to an order: steer directly towards the order target!
    const dLat = target.targetLat - r.lat;
    const dLng = target.targetLng - r.lng;
    const dist = Math.sqrt(dLat * dLat + dLng * dLng);

    if (dist > 0.0003) {
      // Move towards target (~35 km/h ≈ 0.00009 deg/sec)
      r.hdg = Math.atan2(dLng, dLat);
      const stepDist = Math.min(dist, 0.00010);
      r.lat += Math.cos(r.hdg) * stepDist;
      r.lng += Math.sin(r.hdg) * stepDist;
    } else {
      // Holding position at destination
      r.lat = target.targetLat;
      r.lng = target.targetLng;
    }
  } else {
    // Unassigned rider cruising along local neighborhood road network
    r.hdg += rnd(-0.25, 0.25);
    // Keep riders within greater Hyderabad metropolitan area
    const distFromHub = Math.sqrt((r.lat - r.hubLat) ** 2 + (r.lng - r.hubLng) ** 2);
    if (distFromHub > 0.035) {
      r.hdg = Math.atan2(r.hubLng - r.lng, r.hubLat - r.lat) + rnd(-0.3, 0.3);
    }
    r.lat = Math.min(17.55, Math.max(17.32, r.lat + Math.cos(r.hdg) * 0.000045));
    r.lng = Math.min(78.60, Math.max(78.30, r.lng + Math.sin(r.hdg) * 0.000045));
  }
}

function build(c) {
  const out = [], now = Date.now();
  for (const r of chunks[c]) {
    if (now < r.offlineUntil) continue;
    step(r);
    r.seq++;
    const u = { riderId: r.id, lat: r.lat, lng: r.lng, ts: now, seq: r.seq };
    r.last = u;
    out.push(u);
    if (chaos.dup > 0) { chaos.dup--; out.push({ ...u }); }
    if (chaos.late > 0 && r.prev) {
      chaos.late--;
      out.push({ riderId: r.id, lat: r.prev.lat, lng: r.prev.lng, ts: now - 4000, seq: Math.max(0, r.seq - 1) });
    }
    if (chaos.oldts > 0) {
      chaos.oldts--;
      out.push({ riderId: r.id, lat: r.prev ? r.prev.lat : r.lat, lng: r.prev ? r.prev.lng : r.lng, ts: now - 10000, seq: r.seq + 1000000 });
    }
    if (chaos.burst > 0) {
      chaos.burst--;
      for (let k = 0; k < 20; k++) {
        r.seq++;
        out.push({ riderId: r.id, lat: r.lat, lng: r.lng, ts: Date.now(), seq: r.seq });
      }
    }
  }
  return out;
}

const agent = new http.Agent({ keepAlive: true, maxSockets: 16 });

function post(base, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const u = new URL(base + '/api/rider/locations');
    const req = http.request({
      hostname: u.hostname,
      port: u.port,
      path: u.pathname,
      method: 'POST',
      agent,
      headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) }
    }, (res) => {
      const ch = [];
      res.on('data', (d) => ch.push(d));
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(ch).toString()) }); }
        catch (e) { reject(e); }
      });
    });
    req.setTimeout(4000, () => req.destroy(new Error('timeout')));
    req.on('error', reject);
    req.end(data);
  });
}

async function send(c) {
  const body = build(c);
  if (!body.length) return;
  stats.sent += body.length;
  for (let k = 0; k < URLS.length; k++) {
    try {
      const r = await post(URLS[(c + tick + k) % URLS.length], body);
      if (r.status === 200) {
        stats.accepted += r.body.accepted || 0;
        stats.stale += r.body.stale || 0;
        stats.rateLimited += r.body.rateLimited || 0;
        stats.errors += r.body.errors || 0;
        return;
      }
      if (r.status === 503) { stats.refused += body.length; return; }
    } catch (e) {}
  }
  stats.errors += body.length;
}

setInterval(() => {
  tick++;
  chunks.forEach((_, c) => setTimeout(() => send(c), c * (1000 / chunks.length)));
}, 1000);

http.createServer((req, res) => {
  const cors = { 'access-control-allow-origin': '*', 'access-control-allow-headers': '*', 'content-type': 'application/json' };
  if (req.method === 'OPTIONS') { res.writeHead(204, cors); return res.end(); }
  if (req.url === '/stats') { res.writeHead(200, cors); return res.end(JSON.stringify({ riders: N, ...stats })); }
  if (req.method === 'POST' && req.url === '/chaos') {
    let b = '';
    req.on('data', (d) => (b += d));
    req.on('end', () => {
      const { mode, n = 50 } = JSON.parse(b || '{}');
      if (mode === 'offline') {
        for (const r of riders.slice().sort(() => Math.random() - 0.5).slice(0, n)) r.offlineUntil = Date.now() + 30000;
      } else if (mode in chaos) chaos[mode] += n;
      else { res.writeHead(400, cors); return res.end('{"error":"unknown mode"}'); }
      res.writeHead(200, cors);
      res.end(JSON.stringify({ ok: true, mode, n }));
    });
    return;
  }
  res.writeHead(404, cors);
  res.end('{}');
}).listen(CTL, () => console.log(`rider simulator: ${N} riders across Hyderabad hubs → ${URLS.join(', ')} (control :${CTL})`));

let prev = { ...stats };
setInterval(() => {
  const d = (k) => stats[k] - prev[k];
  console.log(`[riders] sent/s=${(d('sent') / 5).toFixed(0)} accepted/s=${(d('accepted') / 5).toFixed(0)} stale=${d('stale')} rateLimited=${d('rateLimited')} refused=${d('refused')} errors=${d('errors')}`);
  prev = { ...stats };
}, 5000);
