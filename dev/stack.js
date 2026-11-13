'use strict';
// Starts redis (dev stand-in unless --real-redis), two app instances and the notification worker.
// Automatically frees ports if previous processes were left running, and cleans up children on exit.
const { spawn, execSync } = require('node:child_process');
const path = require('node:path');
const root = path.join(__dirname, '..');

function freePort(port) {
  if (process.platform === 'win32') {
    try {
      const out = execSync(`netstat -ano -p tcp | findstr :${port}`, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] });
      let killed = false;
      for (const line of out.split('\n')) {
        const parts = line.trim().split(/\s+/);
        if (parts.length >= 5 && parts[3] === 'LISTENING') {
          const pid = parts[4];
          if (pid && pid !== '0' && pid !== String(process.pid)) {
            try { execSync(`taskkill /F /PID ${pid}`, { stdio: 'ignore' }); killed = true; } catch (e) {}
          }
        }
      }
      if (killed) {
        const start = Date.now();
        while (Date.now() - start < 300) {}
      }
    } catch (e) {}
  } else {
    try {
      execSync(`fuser -k ${port}/tcp`, { stdio: 'ignore' });
    } catch (e) {}
  }
}

function startStack(o = {}) {
  const ports = o.ports || [3001, 3002];
  const useMini = o.miniRedis !== false;
  const redisPort = o.redisPort || (useMini ? 6390 : 6379);
  const withSim = o.withSim || process.argv.includes('--sim') || process.argv.includes('--with-riders');

  // Pre-flight: free ports if occupied by leftover zombie processes
  [redisPort, ...ports, 4200].forEach(freePort);

  const env = {
    ...process.env,
    REDIS_URL: `redis://127.0.0.1:${redisPort}`,
    PEERS: ports.map((p) => `http://localhost:${p}`).join(','),
    DB_PATH: o.dbPath || path.join(root, 'data', 'tracknow.db'),
    ...(o.env || {}),
  };

  const defs = [];
  if (useMini) {
    defs.push({ name: 'redis', cmd: process.execPath, args: [path.join(__dirname, 'miniredis.js'), String(redisPort)], env, port: redisPort });
  }
  ports.forEach((p, i) => {
    defs.push({
      name: `app${i + 1}`,
      cmd: process.execPath,
      args: ['--no-warnings', path.join(root, 'src', 'server.js')],
      env: { ...env, PORT: String(p), INSTANCE_ID: `app${i + 1}` },
      port: p
    });
  });
  defs.push({ name: 'worker', cmd: process.execPath, args: ['--no-warnings', path.join(root, 'src', 'worker.js')], env });

  if (withSim) {
    defs.push({
      name: 'riders',
      cmd: process.execPath,
      args: [path.join(root, 'sim', 'riders.js'), '--riders', '2000', '--urls', ports.map((p) => `http://localhost:${p}`).join(',')],
      env,
      delay: 1500,
      port: 4200
    });
  }

  const procs = new Map();
  let stopping = false;

  const killProc = (c) => {
    if (!c || !c.pid) return;
    try {
      if (process.platform === 'win32') {
        execSync(`taskkill /F /T /PID ${c.pid}`, { stdio: 'ignore' });
      } else {
        c.kill('SIGKILL');
      }
    } catch (e) {}
  };

  const spawnOne = (d) => {
    const doSpawn = () => {
      if (stopping) return;
      const c = spawn(d.cmd, d.args, { env: d.env, stdio: ['ignore', 'pipe', 'pipe'] });
      procs.set(d.name, c);
      const log = (b) => {
        if (!o.quiet) {
          for (const l of String(b).split('\n')) {
            if (l.trim()) console.log(`[${d.name}] ${l}`);
          }
        }
      };
      c.stdout.on('data', log);
      c.stderr.on('data', log);
      c.on('exit', (code) => {
        if (stopping) return;
        if (!o.quiet) console.log(`[${d.name}] exited (${code}); restarting in ${o.respawnMs || 6000} ms`);
        setTimeout(() => {
          if (!stopping) {
            if (d.port) freePort(d.port);
            spawnOne(d);
          }
        }, o.respawnMs || 6000);
      });
    };
    if (d.delay) {
      setTimeout(doSpawn, d.delay);
    } else {
      doSpawn();
    }
  };

  defs.forEach(spawnOne);

  const stop = () => {
    if (stopping) return;
    stopping = true;
    for (const c of procs.values()) killProc(c);
    [redisPort, ...ports].forEach(freePort);
  };

  return {
    ports,
    env,
    procs,
    kill: (name, sig = 'SIGKILL') => procs.get(name) && killProc(procs.get(name)),
    stop,
  };
}

if (require.main === module) {
  const stack = startStack({ miniRedis: !process.argv.includes('--real-redis') });
  console.log('TrackNow stack up → http://localhost:3001  (second instance on :3002)');
  const cleanup = () => {
    stack.stop();
    process.exit(0);
  };
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);
  process.on('exit', () => stack.stop());
}

module.exports = { startStack };
