'use strict';
// Minimal dependency-free Redis client (RESP2): commands, pipelining via cork, pub/sub, EVALSHA scripts.
const net = require('node:net');
const crypto = require('node:crypto');
const { EventEmitter } = require('node:events');

class RedisError extends Error {}

class Parser {
  constructor(onReply) { this.buf = Buffer.alloc(0); this.onReply = onReply; }
  push(chunk) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    while (this.buf.length) {
      const r = this.parse(0);
      if (!r) break;
      this.buf = this.buf.subarray(r.next);
      this.onReply(r.value);
    }
  }
  parse(pos) {
    if (pos >= this.buf.length) return null;
    const t = String.fromCharCode(this.buf[pos]);
    const e = this.buf.indexOf('\r\n', pos);
    if (e < 0) return null;
    const head = this.buf.toString('utf8', pos + 1, e);
    const next = e + 2;
    switch (t) {
      case '+': return { value: head, next };
      case '-': return { value: new RedisError(head), next };
      case ':': return { value: Number(head), next };
      case '$': {
        const n = Number(head);
        if (n < 0) return { value: null, next };
        if (this.buf.length < next + n + 2) return null;
        return { value: this.buf.toString('utf8', next, next + n), next: next + n + 2 };
      }
      case '*': {
        const n = Number(head);
        if (n < 0) return { value: null, next };
        const arr = []; let p = next;
        for (let i = 0; i < n; i++) { const r = this.parse(p); if (!r) return null; arr.push(r.value); p = r.next; }
        return { value: arr, next: p };
      }
      default: throw new Error('bad RESP byte ' + t);
    }
  }
}

function encode(args) {
  let out = '*' + args.length + '\r\n';
  for (const a of args) { const s = String(a); out += '$' + Buffer.byteLength(s) + '\r\n' + s + '\r\n'; }
  return out;
}

class Redis extends EventEmitter {
  constructor({ host = '127.0.0.1', port = 6379, timeoutMs = 1500, pubsub = false } = {}) {
    super();
    Object.assign(this, { host, port, timeoutMs, pubsub });
    this.up = false; this.blocked = false; this.sock = null; this.pending = [];
    this.channels = new Set(); this.scripts = new Map(); this.backoff = 100; this._corked = false;
  }
  start() { this._connect(); return this; }
  _connect() {
    if (this.blocked || (this.sock && !this.sock.destroyed)) return;
    const s = net.connect({ host: this.host, port: this.port });
    this.sock = s; s.setNoDelay(true);
    const parser = new Parser((v) => this._onReply(v));
    s.on('connect', () => {
      if (this.sock !== s) return;
      this.up = true; this.backoff = 100;
      for (const ch of this.channels) this._write(encode(['SUBSCRIBE', ch]));
      this.emit('up');
    });
    s.on('data', (d) => { try { parser.push(d); } catch (e) { s.destroy(); } });
    s.on('error', () => {});
    s.on('close', () => {
      if (this.sock !== s) return;
      const was = this.up;
      this.up = false; this.sock = null;
      this._failAll(new Error('REDIS_DOWN'));
      if (was) this.emit('down');
      if (!this.blocked) setTimeout(() => this._connect(), this.backoff);
      this.backoff = Math.min(this.backoff * 2, 1000);
    });
  }
  block(on) {            // fault injection: behave as if Redis is unreachable
    this.blocked = on;
    if (on) { if (this.sock) this.sock.destroy(); } else this._connect();
  }
  _write(str) {
    if (!this.sock) return;
    if (!this._corked) {
      this._corked = true; this.sock.cork();
      process.nextTick(() => { this._corked = false; if (this.sock) this.sock.uncork(); });
    }
    this.sock.write(str);
  }
  _failAll(err) { const p = this.pending; this.pending = []; for (const e of p) { clearTimeout(e.timer); if (!e.dead) e.reject(err); } }
  _onReply(v) {
    if (this.pubsub && Array.isArray(v)) {
      if (v[0] === 'message') return this.emit('message', v[1], v[2]);
      if (v[0] === 'subscribe' || v[0] === 'unsubscribe') return;
    }
    const e = this.pending.shift();
    if (!e) return;
    clearTimeout(e.timer);
    if (e.dead) return;
    if (v instanceof RedisError) e.reject(v); else e.resolve(v);
  }
  call(...args) {
    if (!this.up) return Promise.reject(new Error('REDIS_DOWN'));
    return new Promise((resolve, reject) => {
      const e = { resolve, reject, dead: false };
      e.timer = setTimeout(() => { e.dead = true; reject(new Error('REDIS_TIMEOUT')); }, this.timeoutMs);
      this.pending.push(e);
      this._write(encode(args));
    });
  }
  subscribe(ch) { if (this.channels.has(ch)) return; this.channels.add(ch); if (this.up) this._write(encode(['SUBSCRIBE', ch])); }
  unsubscribe(ch) { if (!this.channels.delete(ch)) return; if (this.up) this._write(encode(['UNSUBSCRIBE', ch])); }
  defineScript(name, src) { this.scripts.set(name, { src, sha: crypto.createHash('sha1').update(src).digest('hex') }); }
  async script(name, keys, args) {
    const s = this.scripts.get(name);
    const run = () => this.call('EVALSHA', s.sha, keys.length, ...keys, ...args);
    try { return await run(); } catch (e) {
      if (!/NOSCRIPT/.test(e.message)) throw e;
      await this.call('SCRIPT', 'LOAD', s.src);
      return run();
    }
  }
}

module.exports = { Redis, RedisError };
