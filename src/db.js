'use strict';
// SQLite (WAL) is the source of truth for orders, the outbox and notifications.
// Shared by every app instance and the worker, so cross-process behaviour is real (file locks, busy_timeout).
const { DatabaseSync } = require('node:sqlite');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

class DbError extends Error {
  constructor(status, code, detail) { super(code); this.status = status; this.code = code; this.detail = detail; }
}

const ALLOWED = {
  placed: ['accepted', 'cancelled'],
  accepted: ['preparing', 'cancelled'],
  preparing: ['picked_up', 'cancelled'],
  picked_up: ['delivered'],
  delivered: [],
  cancelled: [],
};
const TERMINAL = ['delivered', 'cancelled'];
const ASSIGNABLE = ['placed', 'accepted', 'preparing'];

function open(file, metrics) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const db = new DatabaseSync(file);
  db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;');
  db.exec(`
    CREATE TABLE IF NOT EXISTS orders (
      id TEXT PRIMARY KEY, restaurant_id TEXT NOT NULL, customer_id TEXT NOT NULL,
      pickup_lat REAL, pickup_lng REAL, drop_lat REAL, drop_lng REAL,
      status TEXT NOT NULL, version INTEGER NOT NULL, rider_id TEXT, batch_id TEXT,
      created_at INTEGER, updated_at INTEGER);
    CREATE INDEX IF NOT EXISTS orders_rider ON orders(rider_id);
    CREATE TABLE IF NOT EXISTS idem (key TEXT PRIMARY KEY, result TEXT, created_at INTEGER);
    CREATE TABLE IF NOT EXISTS outbox (
      id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL, version INTEGER NOT NULL, status TEXT NOT NULL,
      created_at INTEGER, processed_at INTEGER, attempts INTEGER DEFAULT 0, last_error TEXT,
      UNIQUE(order_id, version));
    CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(processed_at, id);
    CREATE TABLE IF NOT EXISTS notifications (
      order_id TEXT NOT NULL, version INTEGER NOT NULL, status TEXT NOT NULL, delivered_at INTEGER,
      PRIMARY KEY (order_id, version));
    CREATE TABLE IF NOT EXISTS location_history (rider_id TEXT, lat REAL, lng REAL, ts INTEGER, seq INTEGER);
    CREATE TABLE IF NOT EXISTS flags (key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS releases (rider_id TEXT PRIMARY KEY);
  `);

  const w = (n = 1) => metrics && metrics.inc('db_other_writes', n);
  const q = (sql) => db.prepare(sql);
  const st = {
    getOrder: q('SELECT * FROM orders WHERE id=?'),
    insOrder: q('INSERT INTO orders (id,restaurant_id,customer_id,pickup_lat,pickup_lng,drop_lat,drop_lng,status,version,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)'),
    insOutbox: q('INSERT INTO outbox (order_id,version,status,created_at) VALUES (?,?,?,?)'),
    getIdem: q('SELECT result FROM idem WHERE key=?'),
    insIdem: q('INSERT OR IGNORE INTO idem (key,result,created_at) VALUES (?,?,?)'),
    setStatus: q('UPDATE orders SET status=?, version=version+1, updated_at=? WHERE id=? AND version=?'),
    assign: q(`UPDATE orders SET rider_id=?, batch_id=?, updated_at=? WHERE id=? AND rider_id IS NULL AND status IN ('placed','accepted','preparing')`),
    activeForRider: q(`SELECT * FROM orders WHERE rider_id=? AND status NOT IN ('delivered','cancelled')`),
    activeAll: q(`SELECT id, rider_id, batch_id FROM orders WHERE rider_id IS NOT NULL AND status NOT IN ('delivered','cancelled')`),
    list: q(`SELECT * FROM orders WHERE restaurant_id NOT LIKE 'race%' ORDER BY created_at DESC, rowid DESC LIMIT ?`),
    notifs: q('SELECT order_id, version, status, delivered_at FROM notifications WHERE order_id=? ORDER BY version'),
    nextEvents: q(`SELECT o.* FROM outbox o WHERE o.processed_at IS NULL AND NOT EXISTS
      (SELECT 1 FROM outbox p WHERE p.order_id=o.order_id AND p.version<o.version AND p.processed_at IS NULL)
      ORDER BY o.id LIMIT ?`),
    insNotif: q('INSERT OR IGNORE INTO notifications (order_id,version,status,delivered_at) VALUES (?,?,?,?)'),
    doneOutbox: q('UPDATE outbox SET processed_at=? WHERE id=? AND processed_at IS NULL'),
    failOutbox: q('UPDATE outbox SET attempts=attempts+1, last_error=? WHERE id=?'),
    pending: q('SELECT COUNT(*) AS n FROM outbox WHERE processed_at IS NULL'),
    redeliver: q('UPDATE outbox SET processed_at=NULL WHERE id IN (SELECT id FROM outbox WHERE processed_at IS NOT NULL ORDER BY id DESC LIMIT ?)'),
    insHist: q('INSERT INTO location_history (rider_id,lat,lng,ts,seq) VALUES (?,?,?,?,?)'),
    getFlag: q('SELECT value FROM flags WHERE key=?'),
    setFlag: q('INSERT INTO flags (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value'),
    bump: q('INSERT INTO counters (name,value) VALUES (?,?) ON CONFLICT(name) DO UPDATE SET value=value+excluded.value'),
    counters: q('SELECT name, value FROM counters'),
    queueRelease: q('INSERT OR IGNORE INTO releases (rider_id) VALUES (?)'),
    releases: q('SELECT rider_id FROM releases'),
    delRelease: q('DELETE FROM releases WHERE rider_id=?'),
  };

  function tx(fn) {
    db.exec('BEGIN IMMEDIATE');
    try { const r = fn(); db.exec('COMMIT'); return r; } catch (e) { try { db.exec('ROLLBACK'); } catch (_) {} throw e; }
  }

  const api = {
    raw: db, ALLOWED, TERMINAL, ASSIGNABLE,

    createOrder(input, idemKey) {
      return tx(() => {
        const key = idemKey ? 'order:' + idemKey : null;
        if (key) { const hit = st.getIdem.get(key); if (hit) return { order: JSON.parse(hit.result), replay: true }; }
        const id = 'O' + crypto.randomBytes(4).toString('hex'), now = Date.now();
        st.insOrder.run(id, input.restaurantId, input.customerId, input.pickup.lat, input.pickup.lng, input.dropoff.lat, input.dropoff.lng, 'placed', 1, now, now);
        st.insOutbox.run(id, 1, 'placed', now);
        const order = st.getOrder.get(id);
        if (key) st.insIdem.run(key, JSON.stringify(order), now);
        w(3);
        return { order, replay: false };
      });
    },

    transition(id, to, requestId) {
      return tx(() => {
        const key = requestId ? `tr:${id}:${requestId}` : null;
        if (key) { const hit = st.getIdem.get(key); if (hit) return { order: JSON.parse(hit.result), noop: true }; }
        const o = st.getOrder.get(id);
        if (!o) throw new DbError(404, 'ORDER_NOT_FOUND');
        if (o.status === to) return { order: o, noop: true };           // repeated request: ignored
        if (!ALLOWED[o.status] || !ALLOWED[o.status].includes(to)) throw new DbError(409, 'INVALID_TRANSITION', `${o.status} -> ${to}`);
        if (to === 'picked_up' && !o.rider_id) throw new DbError(409, 'NO_RIDER_ASSIGNED');
        const now = Date.now();
        const r = st.setStatus.run(to, now, id, o.version);
        if (r.changes !== 1) throw new DbError(409, 'CONCURRENT_UPDATE');
        st.insOutbox.run(id, o.version + 1, to, now);
        const order = st.getOrder.get(id);
        if (key) st.insIdem.run(key, JSON.stringify(order), now);
        w(3);
        return { order, noop: false };
      });
    },

    // Both orders are updated or none are (rollback on any mismatch).
    assign(ids, riderId, batchId) {
      return tx(() => {
        const now = Date.now();
        for (const id of ids) {
          const r = st.assign.run(riderId, batchId || null, now, id);
          if (r.changes !== 1) throw new DbError(409, 'ORDER_NOT_ASSIGNABLE', id);
        }
        w(ids.length);
        return ids.map((id) => st.getOrder.get(id));
      });
    },

    getOrder: (id) => st.getOrder.get(id),
    listOrders: (n = 200) => st.list.all(n),
    activeForRider: (rid) => st.activeForRider.all(rid),
    activeAssignments: () => st.activeAll.all(),
    notificationsFor: (id) => st.notifs.all(id),

    nextEvents: (n = 50) => st.nextEvents.all(n),
    completeEvent(ev) {           // notification effect + outbox mark in ONE transaction; PK makes the effect idempotent
      return tx(() => {
        const now = Date.now();
        const r = st.insNotif.run(ev.order_id, ev.version, ev.status, now);
        st.doneOutbox.run(now, ev.id);
        w(2);
        return { inserted: r.changes === 1 };
      });
    },
    failEvent: (id, err) => st.failOutbox.run(String(err).slice(0, 200), id),
    pendingCount: () => st.pending.get().n,
    redeliver: (n) => st.redeliver.run(n).changes,

    insertHistory(rows) {
      tx(() => { for (const r of rows) st.insHist.run(r[0], r[1], r[2], r[3], r[4]); });
      metrics && metrics.inc('db_loc_writes', rows.length);
    },

    getFlag: (k) => { const r = st.getFlag.get(k); return r ? r.value : null; },
    setFlag: (k, v) => st.setFlag.run(k, String(v)),
    bump: (name, n = 1) => st.bump.run(name, n),
    counters() { const o = {}; for (const r of st.counters.all()) o[r.name] = r.value; return o; },

    queueRelease: (rid) => st.queueRelease.run(rid),
    takeReleases: () => st.releases.all().map((r) => r.rider_id),
    doneRelease: (rid) => st.delRelease.run(rid),

    invariants() {
      const dbl = db.prepare(`SELECT rider_id, COUNT(*) c, COUNT(DISTINCT COALESCE(batch_id, id)) g FROM orders
        WHERE rider_id IS NOT NULL AND status NOT IN ('delivered','cancelled') GROUP BY rider_id HAVING g > 1 OR c > 2`).all();
      const partial = db.prepare(`SELECT batch_id, COUNT(*) c FROM orders WHERE batch_id IS NOT NULL AND batch_id != '' GROUP BY batch_id HAVING c <> 2`).all();
      const assigned = db.prepare('SELECT COUNT(*) n FROM orders WHERE rider_id IS NOT NULL').get().n;
      const batches = db.prepare(`SELECT COUNT(DISTINCT batch_id) n FROM orders WHERE batch_id IS NOT NULL AND batch_id != ''`).get().n;
      const ooo = db.prepare(`SELECT COUNT(*) n FROM notifications a JOIN notifications b
        ON a.order_id=b.order_id AND a.version<b.version AND a.rowid>b.rowid`).get().n;
      const missing = db.prepare(`SELECT COUNT(*) n FROM outbox o WHERE processed_at IS NOT NULL AND NOT EXISTS
        (SELECT 1 FROM notifications n WHERE n.order_id=o.order_id AND n.version=o.version)`).get().n;
      const processed = db.prepare('SELECT COUNT(*) n FROM notifications').get().n;
      return { double_assign_violations: dbl.length, partial_batches: partial.length, assigned_orders: assigned, batches_committed: batches,
        notif_out_of_order: ooo, notif_missing: missing, notif_recorded: processed };
    },

    finishRace(ids) {           // test-fixture cleanup only: bypasses lifecycle on purpose, never used for real orders
      tx(() => { for (const id of ids) db.prepare(`UPDATE orders SET status='cancelled' WHERE id=?`).run(id); });
    },

    reset() {
      db.exec('DELETE FROM orders; DELETE FROM idem; DELETE FROM outbox; DELETE FROM notifications; DELETE FROM location_history; DELETE FROM counters; DELETE FROM releases; DELETE FROM flags;');
    },
  };
  return api;
}

module.exports = { open, DbError, ALLOWED, TERMINAL, ASSIGNABLE };
