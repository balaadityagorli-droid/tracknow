'use strict';
// Notification worker. Reads the transactional outbox in per-order version order, records each notification
// exactly once (PRIMARY KEY order_id+version), then pushes it to whichever instance holds the customer's socket.
// Safe to kill at any time and to run twice: unfinished events stay pending and duplicates are suppressed.
const path = require('node:path');
const { Redis } = require('./redis');
const { Metrics } = require('./metrics');
const dbm = require('./db');

const RURL = new URL(process.env.REDIS_URL || 'redis://127.0.0.1:6379');
const db = dbm.open(process.env.DB_PATH || path.join(__dirname, '..', 'data', 'tracknow.db'), new Metrics());
const rc = new Redis({ host: RURL.hostname, port: +RURL.port || 6379 }).start();
const ID = process.env.WORKER_ID || 'worker-' + process.pid;
let busy = false;

async function loop() {
  if (busy) return; busy = true;
  try {
    db.setFlag('worker_hb', Date.now());
    if (db.getFlag('notifier_paused') === '1') return;          // simulated notification-service outage
    for (const ev of db.nextEvents(50)) {
      try {
        const { inserted } = db.completeEvent(ev);
        if (!inserted) { db.bump('notif_dup_suppressed'); continue; }   // already recorded: never push it again
        db.bump('notif_processed');
        const msg = JSON.stringify({ t: 'notify', o: ev.order_id, v: ev.version, s: ev.status, at: Date.now() });
        await rc.call('PUBLISH', `track:${ev.order_id}`, msg).catch(() => {});   // best effort: clients resync from the DB on reconnect
      } catch (e) { db.failEvent(ev.id, e.message); console.error(ID, 'event failed', ev.id, e.message); }
    }
  } catch (e) { console.error(ID, e.message); } finally { busy = false; }
}
setInterval(loop, 150);
console.log(ID, 'started');
process.on('SIGTERM', () => process.exit(0));
