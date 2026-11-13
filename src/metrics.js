'use strict';
// Per-instance counters, rolling rates and rolling histograms; snapshots merge across instances.
const EDGES = [0.5, 1, 2, 3, 5, 7, 10, 15, 20, 30, 40, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000];
const RATE_WINDOW = 5, HIST_WINDOW = 10;

function bucket(v) { let i = 0; while (i < EDGES.length && v > EDGES[i]) i++; return i; }
const zeros = () => new Array(EDGES.length + 1).fill(0);

class Metrics {
  constructor() { this.reset(); this.timer = setInterval(() => this.tick(), 1000); this.timer.unref(); }
  reset() { this.totals = {}; this.sec = {}; this.ring = []; this.cur = {}; this.hring = {}; this.gauges = {}; }
  inc(name, n = 1) { this.totals[name] = (this.totals[name] || 0) + n; this.sec[name] = (this.sec[name] || 0) + n; }
  observe(name, ms) { (this.cur[name] || (this.cur[name] = zeros()))[bucket(ms)]++; }
  tick() {
    this.ring.push(this.sec); if (this.ring.length > RATE_WINDOW) this.ring.shift(); this.sec = {};
    for (const n of new Set([...Object.keys(this.cur), ...Object.keys(this.hring)])) {
      const r = this.hring[n] || (this.hring[n] = []);
      r.push(this.cur[n] || zeros()); if (r.length > HIST_WINDOW) r.shift();
    }
    this.cur = {};
  }
  snapshot(id) {
    const rates = {};
    for (const s of this.ring) for (const [k, v] of Object.entries(s)) rates[k] = (rates[k] || 0) + v;
    const div = Math.max(1, this.ring.length);
    for (const k of Object.keys(rates)) rates[k] = rates[k] / div;
    const hists = {};
    for (const [n, r] of Object.entries(this.hring)) { const h = zeros(); for (const a of r) a.forEach((c, i) => (h[i] += c)); hists[n] = h; }
    return { id, ts: Date.now(), totals: { ...this.totals }, rates, hists, gauges: { ...this.gauges } };
  }
}

function merge(snaps) {
  const out = { totals: {}, rates: {}, hists: {}, gauges: {} };
  for (const s of snaps) {
    for (const [k, v] of Object.entries(s.totals)) out.totals[k] = (out.totals[k] || 0) + v;
    for (const [k, v] of Object.entries(s.rates)) out.rates[k] = (out.rates[k] || 0) + v;
    for (const [k, v] of Object.entries(s.gauges)) out.gauges[k] = (out.gauges[k] || 0) + v;
    for (const [k, h] of Object.entries(s.hists)) { const t = out.hists[k] || (out.hists[k] = zeros()); h.forEach((c, i) => (t[i] += c)); }
  }
  return out;
}

// Upper bucket edge containing quantile q (conservative: never under-reports).
function pctl(h, q) {
  if (!h) return null;
  const n = h.reduce((a, b) => a + b, 0);
  if (!n) return null;
  let acc = 0;
  for (let i = 0; i < h.length; i++) { acc += h[i]; if (acc >= q * n) return i < EDGES.length ? EDGES[i] : EDGES[EDGES.length - 1] * 2; }
  return null;
}
const count = (h) => (h ? h.reduce((a, b) => a + b, 0) : 0);

module.exports = { Metrics, merge, pctl, count, EDGES };
