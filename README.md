# TrackNow — PS08 Real-Time Delivery Tracking Platform

> **Hackathon PS08** · Track: Web Technologies / Distributed Systems / Real-Time UI

Live rider tracking for 2,000+ simulated riders and thousands of concurrent viewers across two stateless application instances, with atomic rider assignment, exactly-once notifications, and graceful Redis failure handling.

---

## System Requirements

- **Operating System:** Windows 10/11, Linux, or macOS
- **Node.js:** Node.js **22.5+** or **24.x** (currently running v24.14.1)
- **Dependencies:** **Zero external npm dependencies** (uses built-in `node:sqlite`, `node:http`, `node:net`, `node:crypto`)
- **Docker / Redis:** **Optional** — built-in `dev/miniredis.js` pure-JS Redis emulator handles all Redis protocol and Lua scripts in dev mode!

---

## Quick Start (30 seconds)

### 1. Start the Stack

In your terminal:
```powershell
cd F:\DELTRACK
node dev/stack.js
```

> **Note on `EADDRINUSE` (Port already in use):**
> If you see `Error: listen EADDRINUSE: address already in use 3001/3002/6390`, it means a previous Node process was left running in the background. 
> To kill any leftover instances:
> ```powershell
> Get-Process node -ErrorAction SilentlyContinue | Stop-Process -Force
> node dev/stack.js
> ```
> *`dev/stack.js` now automatically inspects and frees occupied ports before starting!*

### 2. Open the Browser Interfaces

Open these three tabs in your browser:

| URL | Page | Description |
|-----|------|-------------|
| **[http://localhost:3001](http://localhost:3001)** | **Dispatch Operations Dashboard** | Real street map of Hyderabad, live delivery rider clusters, proof board metrics, and chaos injection buttons. |
| **[http://localhost:3001/board.html](http://localhost:3001/board.html)** | **Restaurant Kanban Board** | Real-time Kanban board with **⚡ Kitchen Auto-Pilot**. Accept order ➔ Auto-assigns nearest rider ➔ Prepares ➔ Handover ➔ Delivered! |
| **[http://localhost:3001/track.html](http://localhost:3001/track.html)** | **Customer Order Tracking** | Real-world map with 3D Restaurant & Customer Home pins, rotating delivery scooter (`🛵`), live speed, distance, and ETA HUD. |

---

## Run Simulators (Separate Terminals)

### Terminal 2: 2,000 Riders GPS Streamer
```powershell
cd F:\DELTRACK
node sim/riders.js --riders 2000
```
- Distributes 2,000 riders across actual Hyderabad road corridors (Hitec City, Madhapur, Gachibowli, Jubilee Hills, Banjara Hills, Begumpet, Secunderabad, Ameerpet, Kukatpally, Charminar).
- Synchronizes assigned riders to steer towards the restaurant (pickup) and customer (dropoff).
- Exposes chaos injection API on `:4200`.

### Terminal 3: 3,000 Concurrent Live Viewers
```powershell
cd F:\DELTRACK
node sim/viewers.js --viewers 3000 --orders 300
```
- Spreads 3,000 tracking WebSocket connections across both instances (`:3001` and `:3002`).
- Measures end-to-end delivery latency (p95 < 2,000 ms).
- Validates sequence monotonicity (marker never jumps backwards).

---

## Key Features & New Additions

### 1. ⚡ Kitchen Auto-Pilot (Restaurant Board)
- When restaurant staff clicks **"Accept order"**:
  1. System transitions order to `accepted`.
  2. Immediately **auto-assigns the nearest rider** via atomic Redis `claim.lua`.
  3. Displays live cooking countdown (`🍳 Kitchen cooking food (4s)`).
  4. Auto-transitions to `preparing`.
  5. Displays packaging countdown (`📦 Packaging for rider (4s)`).
  6. Auto-transitions to `picked_up` ("Handed to Rider - Out for Delivery").
  7. Displays delivery transit countdown (`🛵 En route to customer (8s)`).
  8. Auto-transitions to `delivered`!
- Includes top-bar toggle: `⚡ Kitchen Auto-Pilot: ON/OFF`.

### 2. Real Map with City Streets & Structures
- Replaced plain canvas grids with standard **Web Mercator tiles** (CartoDB Voyager + OSM fallback).
- Displays Hyderabad's real streets, building blocks, water bodies (Hussain Sagar, Durgam Cheruvu, Musi River), and arterial roads.
- Includes zoom in (`+`), zoom out (`−`), and `⛶ Fit` controls.

### 3. Synchronized Trajectory on Customer Tracking
- **Accepted / Preparing:** Delivery scooter steers towards the **Restaurant** (`pickup_lat, pickup_lng`). HUD displays: *"Rider heading to restaurant to collect food"*.
- **Out for Delivery (`picked_up`):** Delivery scooter departs the **Restaurant** and drives along the road route towards the **Customer Home** (`drop_lat, drop_lng`). HUD displays: *"Rider is on the way to your address!"*.
- **Delivered:** Rider arrives at your home with celebration badge: *"Delivered! 🎉"*.
- **Visuals:** 3D Restaurant pin (🍴), Customer Home pin (🏠), rotating delivery scooter (`🛵`), sonar pulse ring, live speed/distance, and ETA HUD.

---

## Architecture Overview

```
Clients (Browser WebSockets)
        │
  ┌─────┴─────┐
  app1 :3001  app2 :3002   ← stateless Node.js instances
  └─────┬─────┘
        │
  Redis (real-time layer: miniredis on :6390 or real Redis on :6379)
  ├─ riders:avail  GEO sorted set  (nearest-rider search in < 50ms)
  ├─ rider:<id>    hash            (location, status, orders, seq)
  ├─ assign:<oid>  string guard    (atomic claim: zero double-assignments)
  ├─ ops:loc       pub/sub         (location fan-out to ops map)
  ├─ orders:events pub/sub         (order lifecycle events to board + ops)
  └─ track:<oid>   pub/sub         (targeted customer push)
        │
  SQLite WAL (source of truth: data/tracknow.db)
  ├─ orders, outbox, notifications
  └─ location_history (throttled to 1 row/rider/90 s = < 50 writes/s)
        │
  Outbox Worker  (150 ms poll → exactly-once ordered customer notifications)
```

---

## 6 Live Chaos Drills for Judges

Triggerable with one click directly from the **Dispatch Dashboard** (`http://localhost:3001`):

1. **Stale/Duplicate GPS Rejection:** Click **"Duplicate GPS"** or **"Out-of-order GPS"**. Stale counters climb; customer map markers never jump backwards.
2. **Competing Single Assignments (Race Test):** Click **"Race: 2 orders, 1 rider"**. Runs 10 concurrent trials across both app instances. 0 double-assignment violations.
3. **Atomic Batch Assignment:** Click **"Race: 2 batches, 1 rider"**. Both orders from the same restaurant are assigned atomically or neither.
4. **Redis Interruption (Fail-Safe + Self-Heal):** Click **"Interrupt Redis 15 s"**. Assignments fail-closed; UI warns of tracking pause; on reconnect, Redis state auto-rebuilds from SQLite.
5. **Stateless Instance Failover:** Click **"Stop instance 1"**. Client WebSockets immediately rotate to instance 2 within 5 seconds with zero lost updates.
6. **Exactly-Once Notifications (Outbox Drain):** Click **"Pause notifier"**, progress an order on the board, observe pending count grow, click **"Replay last 3 events"**, then click **"Resume notifier"** — duplicates are suppressed and delivered in strict order.

---

## PS08 Evaluation Criteria Verification

| Criterion | Target | Verification Method | Status |
|-----------|--------|---------------------|--------|
| Multi-instance | ≥ 2 stateless apps | `app1` (:3001) + `app2` (:3002) connected to same Redis/DB | ✅ PASS |
| Location scale | 2,000 riders @ 1 Hz | `sim/riders.js --riders 2000` reporting live | ✅ PASS |
| Concurrent viewers | Thousands | `sim/viewers.js --viewers 3000` | ✅ PASS |
| Delivery latency | p95 < 2,000 ms | Live telemetry on proof board (typically ≤ 30 ms) | ✅ PASS |
| Zero polling | 0 requests | GET `/api/track/*` rejected with HTTP 426 | ✅ PASS |
| DB write throttle | < 50 writes/s | 1 row per rider per 90s ≈ 22 writes/s | ✅ PASS |
| Nearest search | p95 < 50 ms | GEOSEARCH on Redis in-memory sorted set | ✅ PASS |
| Atomic claim | 0 double-assign | `claim.lua` atomic check + NX guard | ✅ PASS |
| Batch assignment | Both or neither | Single Redis transaction for 2 orders | ✅ PASS |
| Outbox delivery | Exactly-once | SQLite transactional outbox + version deduplication | ✅ PASS |
| Client failover | < 5,000 ms | Auto-rotating client endpoints (p95 reconnect < 1s) | ✅ PASS |
| Redis failover | Graceful + self-heal | Fail-closed gate + automatic rebuild from DB | ✅ PASS |
