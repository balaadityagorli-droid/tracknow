'use strict';
// Shared client helpers: failover-aware WebSocket, real canvas tile map, RiderMarker, and utilities.
const TN = (() => {
  const css = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim();

  async function endpoints() {
    let peers = [];
    try { peers = (await (await fetch('/api/config')).json()).peers || []; } catch (e) {}
    const own = location.origin;
    const fix = peers.map((p) => p.replace('localhost', location.hostname));
    return { own, peers: fix, all: [own, ...fix.filter((p) => p !== own)] };
  }

  // WebSocket that rotates between instances, reports reconnect time, and treats a silent link as dropped.
  function connect({ kind, order, onMsg, onState }) {
    let eps = [location.origin], idx = 0, ws = null, last = 0, closedAt = 0, attempt = 0, state = '', stopped = false;
    const set = (s, ep) => { if (s !== state) { state = s; onState && onState(s, ep || eps[idx % eps.length]); } };
    endpoints().then((e) => { eps = e.all; });
    // Initial state shown immediately
    onState && onState('connecting', location.origin);
    function open() {
      if (stopped) return;
      const ep = eps[idx % eps.length];
      const base = ep.replace(/^http/, 'ws');
      const q = new URLSearchParams({ kind }); if (order) q.set('order', order);
      const sock = (ws = new WebSocket(`${base}/ws?${q}`));
      sock.onopen = () => { last = Date.now(); attempt = 0; set('connecting', ep); };
      sock.onmessage = (e) => {
        last = Date.now();
        let d;
        try { d = JSON.parse(e.data); } catch (err) { return; }
        if (d.t === 'snap') {
          if (closedAt) { sock.send(JSON.stringify({ t: 'reconnect', ms: Date.now() - closedAt })); closedAt = 0; }
          set('live', ep);
        }
        onMsg(d);
      };
      sock.onclose = () => {
        if (stopped || ws !== sock) return;
        if (!closedAt) closedAt = Date.now();
        set(navigator.onLine ? 'reconnecting' : 'offline', ep);
        idx++; attempt++;
        const delay = Math.min(3000, 150 * Math.min(attempt, 10) + Math.random() * 500);
        setTimeout(open, delay);
      };
      sock.onerror = () => {};
    }
    setInterval(() => { if (ws && ws.readyState === 1 && Date.now() - last > 6000) ws.close(); }, 1000);
    addEventListener('offline', () => set('offline'));
    addEventListener('online', () => { if (state !== 'live') open(); });
    open();
    return {
      send: (o) => ws && ws.readyState === 1 && ws.send(JSON.stringify(o)),
      close() { stopped = true; ws && ws.close(); }
    };
  }

  // Web Mercator tile cache
  const tileCache = new Map();
  const loadingTiles = new Set();

  function fetchTile(z, x, y, onDone) {
    const key = `${z}/${x}/${y}`;
    if (tileCache.has(key)) return tileCache.get(key);
    if (loadingTiles.has(key)) return null;
    loadingTiles.add(key);

    const img = new Image();
    img.crossOrigin = 'anonymous';
    // ESRI World Street Map (free, no API key required) — z/y/x order!
    img.src = `https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/${z}/${y}/${x}`;
    img.onload = () => {
      loadingTiles.delete(key);
      tileCache.set(key, img);
      onDone && onDone();
    };
    img.onerror = () => {
      // Fallback 1: CartoDB Light (no-labels variant, rarely rate-limited)
      const fb1 = new Image();
      fb1.crossOrigin = 'anonymous';
      fb1.src = `https://a.basemaps.cartocdn.com/light_all/${z}/${x}/${y}.png`;
      fb1.onload = () => { loadingTiles.delete(key); tileCache.set(key, fb1); onDone && onDone(); };
      fb1.onerror = () => {
        // Fallback 2: OpenStreetMap standard
        const fb2 = new Image();
        fb2.crossOrigin = 'anonymous';
        fb2.src = `https://tile.openstreetmap.org/${z}/${x}/${y}.png`;
        fb2.onload = () => { loadingTiles.delete(key); tileCache.set(key, fb2); onDone && onDone(); };
        fb2.onerror = () => { loadingTiles.delete(key); };
      };
    };
    return null;
  }

  // Realistic Interactive Map with Real Street Tiles & Hyderabad Vector Geography
  class MapView {
    constructor(canvas, { center = [17.435, 78.448], zoom = 12.4, draw } = {}) {
      Object.assign(this, {
        c: canvas,
        x: canvas.getContext('2d'),
        lat: center[0],
        lng: center[1],
        zoom: zoom,
        w: 0,
        h: 0,
        dirty: true,
        drawFn: draw || (() => {})
      });

      new ResizeObserver(() => this.resize()).observe(canvas.parentElement);

      canvas.addEventListener('wheel', (e) => {
        e.preventDefault();
        const r = canvas.getBoundingClientRect(), px = e.clientX - r.left, py = e.clientY - r.top;
        const before = this.unproject(px, py);
        const delta = -Math.sign(e.deltaY) * 0.35;
        this.zoom = Math.max(9, Math.min(18, this.zoom + delta));
        const after = this.unproject(px, py);
        this.lat += before[0] - after[0];
        this.lng += before[1] - after[1];
        this.dirty = true;
      }, { passive: false });

      let drag = null;
      canvas.addEventListener('pointerdown', (e) => {
        drag = { x: e.clientX, y: e.clientY };
        canvas.setPointerCapture(e.pointerId);
        canvas.style.cursor = 'grabbing';
      });
      canvas.addEventListener('pointermove', (e) => {
        if (!drag) return;
        const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
        drag = { x: e.clientX, y: e.clientY };
        const p1 = this.unproject(this.w / 2, this.h / 2);
        const p0 = this.unproject(this.w / 2 - dx, this.h / 2 - dy);
        this.lat += p0[0] - p1[0];
        this.lng += p0[1] - p1[1];
        this.dirty = true;
      });
      canvas.addEventListener('pointerup', () => { drag = null; canvas.style.cursor = 'grab'; });

      const loop = () => {
        if (this.dirty) {
          this.dirty = false;
          this.render();
        }
        requestAnimationFrame(loop);
      };
      loop();
    }

    zoomIn() { this.zoom = Math.min(18, this.zoom + 0.6); this.dirty = true; }
    zoomOut() { this.zoom = Math.max(9, this.zoom - 0.6); this.dirty = true; }

    // Standard Web Mercator World Coordinates (pixels at current zoom)
    latLngToWorld(lat, lng) {
      const worldSize = 256 * Math.pow(2, this.zoom);
      const x = (lng + 180) / 360 * worldSize;
      const latRad = Math.max(-85, Math.min(85, lat)) * Math.PI / 180;
      const y = (1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * worldSize;
      return [x, y];
    }

    worldToLatLng(x, y) {
      const worldSize = 256 * Math.pow(2, this.zoom);
      const lng = (x / worldSize) * 360 - 180;
      const n = Math.PI - 2 * Math.PI * (y / worldSize);
      const lat = (180 / Math.PI) * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
      return [lat, lng];
    }

    project(lat, lng) {
      const [wx, wy] = this.latLngToWorld(lat, lng);
      const [cx, cy] = this.latLngToWorld(this.lat, this.lng);
      return [this.w / 2 + (wx - cx), this.h / 2 + (wy - cy)];
    }

    unproject(px, py) {
      const [cx, cy] = this.latLngToWorld(this.lat, this.lng);
      const wx = cx + (px - this.w / 2);
      const wy = cy + (py - this.h / 2);
      return this.worldToLatLng(wx, wy);
    }

    fit(pts, pad = 70) {
      if (!pts || !pts.length || !this.w) return;
      const la = pts.map(p => p[0]), ln = pts.map(p => p[1]);
      const minLa = Math.min(...la), maxLa = Math.max(...la);
      const minLn = Math.min(...ln), maxLn = Math.max(...ln);
      this.lat = (minLa + maxLa) / 2;
      this.lng = (minLn + maxLn) / 2;

      // Find best zoom
      for (let z = 16; z >= 9; z -= 0.5) {
        this.zoom = z;
        const [x1, y1] = this.project(minLa, minLn);
        const [x2, y2] = this.project(maxLa, maxLn);
        if (Math.abs(x2 - x1) <= (this.w - pad * 2) && Math.abs(y2 - y1) <= (this.h - pad * 2)) {
          this.zoom = Math.max(10.5, Math.min(16, z));
          break;
        }
      }
      this.dirty = true;
    }

    resize() {
      const r = this.c.parentElement.getBoundingClientRect(), dpr = window.devicePixelRatio || 1;
      this.w = r.width;
      this.h = r.height;
      this.c.width = r.width * dpr;
      this.c.height = r.height * dpr;
      this.x.setTransform(dpr, 0, 0, dpr, 0, 0);
      this.dirty = true;
      this.onresize && this.onresize();
    }

    // Render underlying real map tiles + vector fallback + overlay
    render() {
      const x = this.x;
      x.clearRect(0, 0, this.w, this.h);

      // 1. Draw styled urban vector base (offline-safe structure)
      this.drawVectorBase(x);

      // 2. Draw real raster slippy map tiles
      this.drawRasterTiles(x);

      // 3. Call page-specific overlay (riders, routes, pins)
      this.drawFn(x, (la, ln) => this.project(la, ln));
    }

    drawVectorBase(x) {
      // Landmass
      x.fillStyle = '#f2f4f3';
      x.fillRect(0, 0, this.w, this.h);

      const p = (lat, lng) => this.project(lat, lng);

      // Water bodies: Hussain Sagar Lake
      const hussainSagar = [
        [17.432, 78.468], [17.435, 78.475], [17.428, 78.483],
        [17.421, 78.480], [17.418, 78.473], [17.423, 78.465], [17.432, 78.468]
      ];
      x.fillStyle = '#c5e2f7';
      x.strokeStyle = '#9dc6e8';
      x.lineWidth = 1.5;
      x.beginPath();
      hussainSagar.forEach((pt, i) => {
        const [px, py] = p(pt[0], pt[1]);
        i ? x.lineTo(px, py) : x.moveTo(px, py);
      });
      x.closePath();
      x.fill();
      x.stroke();

      // Durgam Cheruvu (Hitec City)
      const durgam = [
        [17.438, 78.384], [17.435, 78.390], [17.431, 78.388], [17.433, 78.381]
      ];
      x.beginPath();
      durgam.forEach((pt, i) => {
        const [px, py] = p(pt[0], pt[1]);
        i ? x.lineTo(px, py) : x.moveTo(px, py);
      });
      x.closePath();
      x.fill();

      // Musi River flowing across south Hyderabad
      const musiRiver = [
        [17.385, 78.360], [17.382, 78.400], [17.378, 78.440],
        [17.370, 78.480], [17.365, 78.520], [17.360, 78.560]
      ];
      x.strokeStyle = '#c5e2f7';
      x.lineWidth = Math.max(3, this.zoom * 0.8);
      x.beginPath();
      musiRiver.forEach((pt, i) => {
        const [px, py] = p(pt[0], pt[1]);
        i ? x.lineTo(px, py) : x.moveTo(px, py);
      });
      x.stroke();

      // Major arterial highways (Outer Ring Road, PVNR Expressway, NH65)
      const majorHighways = [
        // NH65 / Mumbai Highway (East-West)
        [[17.495, 78.350], [17.460, 78.410], [17.438, 78.450], [17.390, 78.530]],
        // PVNR Elevated Expressway
        [[17.395, 78.442], [17.350, 78.420], [17.300, 78.400]],
        // Inner Ring Road / Begumpet corridor
        [[17.442, 78.450], [17.440, 78.500], [17.410, 78.530]],
        // Financial District - Hitec City Corridor
        [[17.415, 78.330], [17.445, 78.380], [17.450, 78.420]]
      ];

      x.strokeStyle = '#e2e7e4';
      x.lineWidth = Math.max(2, this.zoom * 0.5);
      majorHighways.forEach(road => {
        x.beginPath();
        road.forEach((pt, i) => {
          const [px, py] = p(pt[0], pt[1]);
          i ? x.lineTo(px, py) : x.moveTo(px, py);
        });
        x.stroke();
      });

      // City district labels for realism
      const districts = [
        { name: 'HITEC CITY', lat: 17.448, lng: 78.375 },
        { name: 'GACHIBOWLI', lat: 17.440, lng: 78.348 },
        { name: 'JUBILEE HILLS', lat: 17.432, lng: 78.412 },
        { name: 'BANJARA HILLS', lat: 17.416, lng: 78.442 },
        { name: 'BEGUMPET', lat: 17.440, lng: 78.460 },
        { name: 'SECUNDERABAD', lat: 17.441, lng: 78.501 },
        { name: 'CHARMINAR', lat: 17.361, lng: 78.474 },
        { name: 'KUKATPALLY', lat: 17.493, lng: 78.398 }
      ];

      x.fillStyle = 'rgba(100, 116, 122, 0.45)';
      x.font = 'bold 11px system-ui, sans-serif';
      x.textAlign = 'center';
      districts.forEach(d => {
        const [px, py] = p(d.lat, d.lng);
        if (px >= 20 && px <= this.w - 20 && py >= 20 && py <= this.h - 20) {
          x.fillText(d.name, px, py);
        }
      });
    }

    drawRasterTiles(x) {
      const zInt = Math.floor(Math.max(9, Math.min(18, this.zoom)));
      const numTiles = Math.pow(2, zInt);
      const worldSize = 256 * Math.pow(2, this.zoom);
      const [cx, cy] = this.latLngToWorld(this.lat, this.lng);

      // Viewport bounds in world coords
      const wx0 = cx - this.w / 2;
      const wy0 = cy - this.h / 2;
      const wx1 = cx + this.w / 2;
      const wy1 = cy + this.h / 2;

      // Scale between integer zoom and fractional zoom
      const scale = Math.pow(2, this.zoom - zInt);
      const tileSize = 256 * scale;

      const tx0 = Math.floor((wx0 / (256 * Math.pow(2, this.zoom))) * numTiles);
      const ty0 = Math.floor((wy0 / (256 * Math.pow(2, this.zoom))) * numTiles);
      const tx1 = Math.floor((wx1 / (256 * Math.pow(2, this.zoom))) * numTiles);
      const ty1 = Math.floor((wy1 / (256 * Math.pow(2, this.zoom))) * numTiles);

      for (let tx = tx0; tx <= tx1; tx++) {
        for (let ty = ty0; ty <= ty1; ty++) {
          if (tx < 0 || tx >= numTiles || ty < 0 || ty >= numTiles) continue;

          // Screen coords for this tile
          const tileWorldX = (tx / numTiles) * worldSize;
          const tileWorldY = (ty / numTiles) * worldSize;
          const screenX = this.w / 2 + (tileWorldX - cx);
          const screenY = this.h / 2 + (tileWorldY - cy);

          const img = fetchTile(zInt, tx, ty, () => {
            this.dirty = true;
          });

          if (img && img.complete && img.naturalWidth) {
            x.drawImage(img, screenX, screenY, tileSize, tileSize);
          }
        }
      }
    }
  }

  // Smooth linear interpolation for rider marker animation
  class RiderMarker {
    constructor(lat, lng) {
      this.lat = lat;
      this.lng = lng;
      this.targetLat = lat;
      this.targetLng = lng;
      this.t = 1;
      this.hdg = 0;
    }
    update(lat, lng) {
      if (lat !== this.lat || lng !== this.lng) {
        this.hdg = Math.atan2(lng - this.lng, lat - this.lat);
      }
      this.targetLat = lat;
      this.targetLng = lng;
      this.t = 0;
    }
    tick(dt) {
      if (this.t >= 1) return;
      this.t = Math.min(1, this.t + dt / 800);
      this.lat = this.lat + (this.targetLat - this.lat) * this.t;
      this.lng = this.lng + (this.targetLng - this.lng) * this.t;
    }
    pos() { return [this.lat, this.lng]; }
    heading() { return this.hdg; }
  }

  const hav = (a, b, c, d) => {
    const r = Math.PI / 180, dl = (c - a) * r, dn = (d - b) * r;
    const h = Math.sin(dl / 2) ** 2 + Math.cos(a * r) * Math.cos(c * r) * Math.sin(dn / 2) ** 2;
    return 2 * 6371000 * Math.asin(Math.sqrt(h));
  };
  const rid = () => Math.random().toString(36).slice(2, 10);
  const STATUS = {
    placed: 'Placed',
    accepted: 'Accepted',
    preparing: 'Preparing',
    picked_up: 'Picked up',
    delivered: 'Delivered',
    cancelled: 'Cancelled'
  };
  const STATUS_STEPS = ['placed', 'accepted', 'preparing', 'picked_up', 'delivered'];

  async function api(path, body, base = '') {
    const r = await fetch(base + path, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body)
    });
    return { status: r.status, body: await r.json().catch(() => ({})) };
  }

  function fmtDist(meters) {
    if (meters < 1000) return Math.round(meters) + ' m';
    return (meters / 1000).toFixed(2) + ' km';
  }

  function fmtEta(meters, speedKmh = 35) {
    const sec = Math.round((meters / ((speedKmh * 1000) / 3600)));
    if (sec < 60) return `${sec}s`;
    const min = Math.ceil(sec / 60);
    return `${min} min`;
  }

  return { css, endpoints, connect, MapView, RiderMarker, hav, rid, STATUS, STATUS_STEPS, api, fmtDist, fmtEta };
})();
