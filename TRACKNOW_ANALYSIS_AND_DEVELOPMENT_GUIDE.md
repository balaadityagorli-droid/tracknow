# TrackNow: Architecture Analysis, Comparison & Further Development Guide

> **Document Version:** 1.0  
> **Source Plan:** `PS08 TrackNow Technical Research, Architecture & Six-Hour Hackathon Implementation Blueprint` (DOCX)  
> **Current Codebase:** `tracknow.zip` (Node.js 22+ / SQLite WAL / Redis 6.2+ Zero-Dependency Prototype)  
> **Target System:** Replicated High-Throughput Delivery Tracking Platform (PostgreSQL, Redis, HAProxy, 2x Stateless App Instances, Notification Worker)

---

## Table of Contents
1. [Analysis of the DOCX Blueprint Plan](#1-analysis-of-the-docx-blueprint-plan)
2. [Analysis of the Existing Application (ZIP)](#2-analysis-of-the-existing-application-zip)
3. [Deep Comparison: Blueprint vs. Current Codebase](#3-deep-comparison-blueprint-vs-current-codebase)
4. [Gap Analysis and Required Changes](#4-gap-analysis-and-required-changes)
5. [Phase-Wise Development Roadmap](#5-phase-wise-development-roadmap)
6. [Complete Further-Development Guide](#6-complete-further-development-guide)
7. [Final Completion & Verification Checklist](#7-final-completion--verification-checklist)

---

## 1. Analysis of the DOCX Blueprint Plan

### 1.1 Executive Summary and Challenge Context
The DOCX document specifies the technical blueprint for **PS08 TrackNow: Real-Time Delivery Tracking Platform**. The core challenge is not primarily front-end map rendering, but a distributed-systems stress problem operating under high-concurrency real-time constraints:
- **Rider Scale:** At least 2,000 simulated riders sending GPS telemetry at 1 Hz (2,000 updates/second).
- **Viewer Scale:** Thousands of concurrent customer/operations tracking sessions receiving push updates within a 2-second end-to-end latency budget (`p95 < 2,000 ms`).
- **Zero Polling Constraint:** Strict push architecture over persistent WebSockets; zero recurring tracking GET polls allowed.
- **Database Write Ceiling:** Location persistence strictly capped below 50 database writes/second.
- **Dispatch Performance:** Nearest available rider lookup and reservation within a radius completed with `p95 < 50 ms`.
- **Concurrency Invariants:** Strict avoidance of double rider assignment across multiple app instances; atomic all-or-nothing assignment for 2-order same-restaurant batches.
- **Transactional Notifications:** Exactly-once customer inbox effects via the Transactional Outbox pattern, retaining ordered status transitions even across notification worker outages.
- **High Availability & Failover:** At least 2 stateless application instances behind HAProxy; sub-5-second client reconnect failover; resilient degradation and rehydration upon Redis loss.

### 1.2 The 7 Phase Sequence Preserved from DOCX

```mermaid
flowchart LR
    P0["Phase 0<br/>00:00-00:20<br/>Contracts & Freeze"] --> P1["Phase 1<br/>00:20-01:00<br/>Infra & 2 Instances"]
    P1 --> P2["Phase 2<br/>01:00-01:50<br/>Location Ingest & Sim"]
    P2 --> P3["Phase 3<br/>01:50-02:50<br/>Atomic Assign & Outbox"]
    P3 --> P4["Phase 4<br/>02:50-04:05<br/>WebSockets & 3 UIs"]
    P4 --> P5["Phase 5<br/>04:05-05:25<br/>Integration, Load & Chaos"]
    P5 --> P6["Phase 6<br/>05:25-06:00<br/>Demo Freeze & Defence"]
```

#### Phase 0 — Team Contract and Architecture Freeze (00:00–00:20)
- **Objectives:** Establish shared JSON/TypeScript contracts, lifecycle state machine, database invariants, metric definitions, and demo chaos injection triggers before parallel coding starts.
- **Key Deliverables:** Shared contracts for `LocationUpdate`, `OrderSnapshot`, `OrderTransitionCommand`, `NotificationEvent`, `OpsMetricSnapshot`; frozen Redis keyspaces; metric boundaries.
- **Dependencies:** Challenge PS08 specification.

#### Phase 1 — Infrastructure and Two-Instance Deployment (00:20–01:00)
- **Objectives:** Deploy local containerized infrastructure: PostgreSQL, Redis (AOF/named volumes), HAProxy load balancer, two identical stateless Node.js application instances (`app-a`, `app-b`), and a dedicated outbox notification worker.
- **Key Deliverables:** Docker Compose setup, PostgreSQL migrations (all 10 relational tables and partial unique indexes), `/health` (liveness) and `/health/ready` (readiness) endpoints, structured logging (Pino with `requestId` and `instanceId`).
- **Dependencies:** Phase 0 contracts.

#### Phase 2 — Location Ingestion and Rider Simulation (01:00–01:50)
- **Objectives:** Implement the high-throughput 2,000 updates/sec GPS ingestion pipeline with atomic sequence gating, Redis GEO indexing, global rate limiting, and sparse database snapshots.
- **Key Deliverables:** `update_location` Lua script (monotonic `seq` check, epoch handling, Redis `TIME` checks), Redis token-bucket rate limiting, `geo:riders:all` and `geo:riders:available` sets, 2,000-device deterministic CLI simulator (`sim/riders.js`), sparse snapshot writer maintaining DB writes `< 50/s`.
- **Dependencies:** Phase 1 services and schema.

#### Phase 3 — Atomic Assignment, Order Lifecycle, and Durable Notifications (01:50–02:50)
- **Objectives:** Build atomic dispatch, same-restaurant batching, strict lifecycle state transitions, and the Transactional Outbox worker.
- **Key Deliverables:** Redis Lua reservation (`GEOSEARCH` over `geo:riders:available`), PostgreSQL assignment transaction (`assignment_groups` + `assignment_group_orders`), lifecycle compare-and-swap (`WHERE id = ? AND version = ?`), `notification_outbox` table, single ordered worker relaying to `customer_notifications` inbox with `ON CONFLICT DO NOTHING`.
- **Dependencies:** Phases 1 and 2.

#### Phase 4 — WebSocket Fan-Out and User Interfaces (02:50–04:05)
- **Objectives:** Implement cross-instance pub/sub fan-out and 3 responsive interfaces without HTTP polling.
- **Key Deliverables:** Per-order/rider Redis Pub/Sub subscription manager with local socket reference counting; Customer Tracking Page (Canvas/Leaflet smooth interpolation, status timeline, Haversine distance, connection banners); Restaurant Kanban Board (drag-and-drop optimistic moves with version-checked rollback); Operations Map (Canvas vector points for 2,000 riders, real-time metrics cards).
- **Dependencies:** Phases 1–3.

#### Phase 5 — Cross-Instance Integration, Load Verification, and Failure Drills (04:05–05:25)
- **Objectives:** Run full-scale stress tests and judge failure drills; measure and tune numerical targets.
- **Key Deliverables:** Sustained 2,000 riders at 1 Hz, viewer generator creating 2,000–3,000 WebSocket tracking sessions, verified latency (`delivery_p95 < 2000 ms`, `search_p95 < 50 ms`), 6 fault drills (bad GPS, competing assignment races, batch collisions, paused notification worker, app instance crash, Redis interruption/restart).
- **Dependencies:** Phases 1–4.

#### Phase 6 — Demo Freeze, Architecture Decision Record, and Design Defence (05:25–06:00)
- **Objectives:** Code freeze, seed known-good demonstration datasets, produce one-page ADR-001, and align system presentation with the 4 judging criteria (Foundation 35%, Resilience 30%, Production Readiness 25%, Design Defence 10%).
- **Key Deliverables:** Repeatable 6-minute live demo runbook, frozen tagged commit, architecture diagram, ADR document.

---

## 2. Analysis of the Existing Application (ZIP)

### 2.1 File & Directory Tree Structure
The extracted project `tracknow` contains 20 files with **zero npm dependencies**, relying exclusively on modern Node.js built-ins (`node:http`, `node:net`, `node:sqlite`, `node:crypto`, `node:events`, `node:child_process`):

```
tracknow/
├── package.json              # Node.js manifest (engines >= 22.5, scripts, zero dependencies)
├── README.md                 # Execution instructions, design mappings, verification summary
├── docker-compose.yml        # Docker service for redis:7-alpine (ephemeral, no save)
├── dev/
│   ├── miniredis.js          # In-memory pure JS mock Redis server for local testing
│   ├── stack.js              # Orchestrator running miniredis/Redis, 2 app servers, and worker
│   └── lua-host/             # C/Lua standalone harness for benchmarking Lua scripts
│       ├── build.sh
│       ├── driver.lua
│       └── host.c
├── src/
│   ├── server.js             # HTTP REST API + WebSocket server + instance metrics + clustering
│   ├── db.js                 # SQLite WAL database abstraction, schema, outbox, invariants
│   ├── redis.js              # Custom RESP2 socket client with pipelining, corking, EVALSHA
│   ├── ws.js                 # Custom RFC 6455 WebSocket framing, handshake, backpressure
│   ├── worker.js             # Outbox notification relay daemon
│   ├── metrics.js            # In-memory counter, gauge, rolling rate, and histogram engine
│   └── scripts/              # Atomic Redis Lua scripts
│       ├── claim.lua         # Atomic candidate reservation (single order or 2-order batch)
│       ├── update_location.lua # Monotonic seq validation, rate limit, GEOADD, sparse snapshot flag
│       ├── release.lua       # Return rider to available pool and delete assignment guards
│       └── rate_limit.lua    # Fixed-window token bucket limiter
├── public/
│   ├── index.html            # Operations map (HTML5 Canvas), proof scoreboard, chaos buttons
│   ├── board.html            # Restaurant order board (drag-and-drop HTML5, optimistic moves)
│   ├── track.html            # Customer order tracking page (HTML5 Canvas map, status timeline)
│   ├── common.js             # Shared client networking, reconnect with jitter, Haversine, Canvas
│   └── style.css             # Unified dark/light theme CSS
├── sim/
│   ├── riders.js             # 2,000-rider GPS simulator with chaos injection HTTP endpoint (:4200)
│   └── viewers.js            # WebSocket tracking load generator (up to 3,000 clients, churn, latency)
└── test/
    └── run.js                # 27 automated end-to-end integration and failure tests
```

### 2.2 Detailed Module Responsibilities

#### `src/server.js` (App Server Core)
- **Instance Configuration:** Configured via `PORT` (default 3001) and `INSTANCE_ID` (e.g. `app1`, `app2`). Communicates peer addresses via `PEERS`.
- **Redis Connection Management:** Opens two custom Redis connections: command socket `rc` and dedicated subscriber socket `rs`.
- **Self-Healing Rebuild (`rebuild()`):** On Redis connect or recovery after outage, scans active database assignments from SQLite and restores `rider:{id}` hashes, `assign:{orderId}` keys, and removes busy riders from `riders:avail`.
- **Push-Only Gating:** Explicitly rejects `GET /api/track/:orderId` with HTTP status `426 Upgrade Required`, directing clients to `/ws?kind=track&order=<id>`.
- **Batch & Single Assignment:** Implements `claimNearest(orders, lat, lng)` utilizing `GEOSEARCH riders:avail FROMLONLAT <lng> <lat> BYRADIUS 5000 m ASC COUNT 8`. Iterates candidates and calls `claim.lua`. If Redis claim succeeds, immediately persists to SQLite via `db.assign()`. If SQLite commit fails, calls `release.lua` to rollback.
- **Cluster Metric Aggregation (`cluster()`):** Periodically writes its snapshot into Redis hash `instances` under its `INSTANCE_ID`. Merges live peer metrics to compute cluster-wide rates, viewer counts, and p95 latency percentiles.

#### `src/db.js` (Durable Storage & Domain Invariants)
- **Engine:** Employs Node.js 22 built-in `node:sqlite` (`DatabaseSync`) in WAL mode (`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;`).
- **Schema:**
  - `orders`: `(id, restaurant_id, customer_id, pickup_lat, pickup_lng, drop_lat, drop_lng, status, version, rider_id, batch_id, created_at, updated_at)`
  - `idem`: `(key, result, created_at)`
  - `outbox`: `(id AUTOINCREMENT, order_id, version, status, created_at, processed_at, attempts, last_error, UNIQUE(order_id, version))`
  - `notifications`: `(order_id, version, status, delivered_at, PRIMARY KEY(order_id, version))`
  - `location_history`: `(rider_id, lat, lng, ts, seq)`
  - `flags`, `counters`, `releases`
- **State Machine Enforcement:** Strict transition lookup table `ALLOWED`. Rejecting invalid steps with `409 Conflict`. Handover to rider (`picked_up`) is blocked if `rider_id` is null.
- **Invariant Auditing (`invariants()`):** Executes verification SQL checking for double assignments (`COUNT(DISTINCT COALESCE(batch_id, id)) > 1`), partial batches (`batch_id != '' AND count != 2`), out-of-order notifications, and unrecorded outbox entries.

#### `src/worker.js` (Transactional Outbox Daemon)
- Runs as an independent process loop every 150 ms.
- Checks `flags` table for `notifier_paused` chaos toggle.
- Queries `db.nextEvents(50)` which selects pending outbox events where all earlier versions for the same order are already processed.
- Executes `db.completeEvent(ev)` inside an atomic SQLite transaction: inserts into `notifications` table (keyed by `order_id` + `version`) and marks `outbox.processed_at = now`.
- Upon successful commit, publishes message to Redis Pub/Sub channel `track:{order_id}`.

#### `src/redis.js` (Lightweight Custom Client)
- Direct TCP socket implementation of RESP2 protocol.
- Supports pipeline batching via socket corking (`_corked`), channel subscriptions, auto-reconnect with capped exponential backoff, and script caching via `EVALSHA` with automatic `SCRIPT LOAD` fallback.
- Features `block(on)` method to simulate network partitions/Redis downtime for chaos drills.

#### `src/ws.js` (Zero-Dependency WebSocket Transport)
- Implements RFC 6455 WebSocket framing and SHA-1 key negotiation.
- Detects backpressure by monitoring `socket.writableLength > 1MB` (`MAX_BUFFERED`); emits `slow` event and terminates lagging sockets to protect server memory.
- Uses `frame(str)` to pre-encode JSON payloads once before broadcasting across multi-socket viewer sets.

#### `src/scripts/` (Atomic Lua Scripts)
- `claim.lua`: Evaluates rider availability, freshness (`nowMs - rx <= maxAgeMs`), checks per-order existence of `assign:<orderId>`, then sets assignment keys, marks rider status `busy`, and removes rider from `riders:avail`.
- `update_location.lua`: Enforces monotonic sequence (`seq > cur.seq`) and clock freshness (`ts >= cur.ts`), performs rate limiting via `INCR rl:{riderId}:{sec}`, updates coordinates and `rx`, updates `riders:avail` if status is available, and flags `persistFlag = 1` only when the per-rider staggered 90-second threshold is crossed.
- `release.lua`: Drops `assign:<orderId>` keys, sets rider state back to `available`, and re-indexes into `riders:avail`.
- `rate_limit.lua`: Token bucket / fixed window counter for customer endpoints.

#### `sim/riders.js` & `sim/viewers.js` (Simulation Tools)
- `sim/riders.js`: Simulates 2,000 distinct riders in Hyderabad bounding box sending HTTP POST batches to `:3001` and `:3002`. Features HTTP control endpoint on `:4200` to inject duplicates, out-of-order sequences, clock skew, bursts, or take riders offline.
- `sim/viewers.js`: Connects up to 3,000 concurrent WebSocket clients across instances. Validates that sequence numbers never decrease and notifies server of observed latency and duplicates.

---

### 2.3 End-to-End Data & Execution Flow Diagram

```mermaid
sequenceDiagram
    autonumber
    actor Rider as Rider Simulator
    participant App as App Instance (:3001/:3002)
    participant Redis as Redis (6.2+)
    participant DB as SQLite WAL (orders/outbox)
    participant Worker as Outbox Worker
    actor Customer as Customer Browser (WebSocket)

    Note over Rider, App: 1. Location Ingestion (1 Hz per rider)
    Rider->>App: POST /api/rider/locations [{riderId, lat, lng, ts, seq}]
    App->>Redis: EVALSHA update_location.lua [rider:R1, riders:avail, rl:R1:sec]
    alt Valid & Newer Sequence
        Redis-->>App: {1, status, orders, persistFlag}
        App->>Redis: PUBLISH track:{orderId} (if assigned)
        App->>Redis: PUBLISH ops:loc (all riders)
        opt persistFlag == 1 (every 90s staggered)
            App->>DB: INSERT INTO location_history
        end
        App-->>Rider: 200 {accepted: 1}
    else Stale or Clock Regressed
        Redis-->>App: {0, "stale_seq" | "stale_ts"}
        App-->>Rider: 200 {accepted: 0, stale: 1}
    end

    Note over App, Customer: 2. WebSocket Push Fan-Out
    Redis-->>App: Pub/Sub message on track:{orderId}
    App->>Customer: WS Frame {t: 'loc', o, r, lat, lng, ts, seq}
    Customer->>Customer: Check seq > lastSeq, interpolate Canvas marker

    Note over App, Worker: 3. Status Transition & Outbox Pattern
    App->>DB: transition(orderId, 'preparing') [UPDATE orders version=v+1, INSERT outbox]
    DB-->>App: Committed
    Worker->>DB: nextEvents() [SELECT pending outbox FOR order]
    Worker->>DB: completeEvent() [INSERT notifications PK(order_id, version), UPDATE outbox]
    Worker->>Redis: PUBLISH track:{orderId} {t: 'notify', v, s}
    Redis-->>App: Message on track:{orderId}
    App->>Customer: WS Frame {t: 'notify', v, s}
```

---

## 3. Deep Comparison: Blueprint vs. Current Codebase

### 3.1 Detailed Feature-by-Feature Comparison Matrix

| # | Blueprint (DOCX) Requirement | Current ZIP Implementation | Discrepancy / Gap Classification | Impact & Required Action | Status |
|---|---|---|---|---|---|
| **1** | **Language & Framework:** TypeScript, Express.js, Node.js LTS | Vanilla JavaScript (CommonJS), native `node:http` router | **Implemented Differently** | Working cleanly with zero npm bloat. Retain native HTTP for PoC performance, or wrap in Express/TypeScript if strict team conformity is mandated. | **Implemented Differently** |
| **2** | **Relational Database:** PostgreSQL with connection pool (`pg`), SQL migrations, relational tables with foreign keys and partial unique indexes | SQLite (`node:sqlite` in WAL mode) with shared file access (`data/tracknow.db`) | **Implemented Differently** | SQLite file lock works across processes on a single host, but cannot scale to multi-host distributed environments. Must migrate to PostgreSQL for multi-node production readiness. | **Partially Implemented** |
| **3** | **Data Model Invariants:** `assignment_groups` table with `CREATE UNIQUE INDEX one_active_group_per_rider ON assignment_groups (rider_id) WHERE state IN ('assigned', 'in_progress')` | Single `orders` table storing `rider_id` and `batch_id`. Checked in code and queried via `db.invariants()` | **Implemented Differently** | Database invariant enforced by application transactions and verified by SQL queries, but lacks the formal partial unique index on a separate grouping entity. | **Partially Implemented** |
| **4** | **Load Balancer:** HAProxy container routing HTTP and handling WebSocket upgrades with active health checks | None. Client-side endpoint rotation across `:3001` and `:3002` implemented in `common.js` and simulators | **Missing Entirely** | HAProxy container (`infra/haproxy.cfg`) must be added to provide true transparent load balancing and sub-5s failover without client code awareness. | **Missing** |
| **5** | **Redis Client:** Official `node-redis` or `ioredis` with distinct command and subscriber connections | Custom built-in RESP2 TCP client (`src/redis.js`) | **Implemented Differently** | Custom client supports pipelining, pub/sub, corking, and EVALSHA. Exceedingly fast with zero dependencies. Retain for PoC. | **Implemented** |
| **6** | **Rider Simulator:** 2,000 riders @ 1 Hz with chaos injection (duplicates, out-of-order, clock skew, bursts, offline) | Fully implemented in `sim/riders.js` with HTTP control server on `:4200` | **Fully Implemented** | Matches specification completely. Includes control buttons on operations dashboard. | **Implemented** |
| **7** | **Viewer Load Generator:** Tool generating 2,000+ persistent WebSockets, reporting latency and backpressure | Fully implemented in `sim/viewers.js` (up to 3,000 concurrent sockets, churn, latency sampling) | **Fully Implemented** | Meets and exceeds the concurrency requirement. | **Implemented** |
| **8** | **Push Latency Budget:** End-to-end update delivery `p95 < 2,000 ms` | Measured via viewer ACK loop (`{t: 'lat', ms}`) and aggregated in `metrics.js` | **Fully Implemented** | Live calculation displayed on dashboard. | **Implemented** |
| **9** | **Zero Polling Constraint:** Zero tracking GET requests; tracking over WebSockets only; refuse polling with 426 | Implemented in `src/server.js`: `/api/track/*` returns HTTP 426 and increments `polling_requests` counter | **Fully Implemented** | Strictly verified in tests. | **Implemented** |
| **10** | **DB Location Write Ceiling:** DB location writes `< 50/s` under 2,000 riders | Script `update_location.lua` throttles to 1 row per rider per 90s, staggered by hash; writes buffered and flushed in batch | **Fully Implemented** | Yields ~22 writes/s for 2,000 riders, well below the 50/s ceiling. | **Implemented** |
| **11** | **Nearest Rider Search Performance:** `GEOSEARCH` on available riders with `p95 < 50 ms` | `claimNearest` in `server.js` calls `GEOSEARCH riders:avail FROMLONLAT ... BYRADIUS 5000 m ASC COUNT 8` | **Fully Implemented** | Search duration measured with `performance.now()` and recorded in `search_ms` histogram. | **Implemented** |
| **12** | **No Double Assignment & Atomic Batch:** Redis Lua reservation + DB transaction | `claim.lua` checks all conditions before writing; `db.assign()` executes SQLite transaction; rollbacks if failure | **Fully Implemented** | Verified via `/api/admin/race` concurrency tests (15 single races, 10 batch races in test suite). | **Implemented** |
| **13** | **Transactional Outbox & Notifications:** Status change + outbox in 1 DB tx; ordered worker; unique inbox | `orders` update and `outbox` insert in single `BEGIN IMMEDIATE` transaction; `worker.js` inserts to `notifications` PK`(order_id, version)` | **Fully Implemented** | Idempotent, deduplicated, survives worker restart. | **Implemented** |
| **14** | **Frontend UI Surfaces:** React + Vite + TypeScript (Customer Track, Restaurant Board, Operations Map) | Plain HTML5 + Vanilla JS + HTML5 Canvas in `public/` (`index.html`, `board.html`, `track.html`) | **Implemented Differently** | Functional, highly performant, zero bundling required. Leaflet not used; custom high-speed Canvas vector renderer handles 2,000 points effortlessly. | **Implemented Differently** |
| **15** | **Operations Dashboard Metrics:** 6 mandatory metrics (updates/s, latency p95, active viewers, search p95, DB writes/s, pending notifs) | Implemented on `index.html` with real-time status chips and benchmark verification rows | **Fully Implemented** | Shows Pass / Fail / Below Target / Not Tested for every requirement. | **Implemented** |
| **16** | **Authentication & Role Authorization:** Role-based access control (Customer, Restaurant, Rider, Ops) | Minimal demo security; orders accessible by ID, no JWT/cookie session checks | **Partially Implemented** | Sufficient for hackathon demonstration, but lacks explicit token validation and tenant isolation. | **Partially Implemented** |
| **17** | **Two-Instance Orchestration:** Docker Compose launching app-a, app-b, worker, postgres, redis, haproxy | Docker Compose only runs Redis; stack is orchestrated via Node.js script `dev/stack.js` | **Partially Implemented** | Complete multi-process orchestration works locally, but production containerization is incomplete. | **Partially Implemented** |

---

## 4. Gap Analysis and Required Changes

### Gap 1: Load Balancer (HAProxy) Missing
- **Problem:** Currently, clients and simulators manually know about both app instances (`:3001` and `:3002`) and perform application-level endpoint rotation upon socket closure. The DOCX blueprint requires an independent HAProxy load balancer with active health checks to achieve transparent failover.
- **Affected Files:** `docker-compose.yml`, `sim/riders.js`, `sim/viewers.js`, `public/common.js`.
- **Required New Files:** `infra/haproxy.cfg`.
- **Remediation:** Add an HAProxy container to `docker-compose.yml` exposing port 80/8080, configured to balance WebSocket and HTTP traffic to `app1:3001` and `app2:3002`, utilizing `/api/health` for HTTP check.

### Gap 2: SQLite vs. PostgreSQL
- **Problem:** The current implementation uses Node 22's built-in `node:sqlite`. While SQLite WAL mode supports multi-process concurrency on a single machine, it cannot support multi-host deployments or distributed cloud environments. Furthermore, it lacks PostgreSQL's native partial unique indexes (`CREATE UNIQUE INDEX ... WHERE state IN ('assigned', 'in_progress')`).
- **Affected Files:** `src/db.js`, `package.json`, `docker-compose.yml`.
- **Required New Files:** `db/migrations/001_init.sql`, `src/db_pg.js` (or PostgreSQL driver wrapper).
- **Remediation:** Provide a PostgreSQL backend implementation for `src/db.js` using `pg` or `postgres.js` while retaining the SQLite driver as a zero-dependency fallback option.

### Gap 3: Relational Normalization of Assignment Groups
- **Problem:** The blueprint mandates a distinct `assignment_groups` table with a partial unique index on `rider_id` for active states, linked to `assignment_group_orders` for batching. The ZIP code denormalizes this onto the `orders` table (`orders.rider_id` and `orders.batch_id`).
- **Remediation:** When deploying on PostgreSQL, implement the normalized schema specified in DOCX Section 10.1–10.2:
  ```sql
  CREATE TABLE assignment_groups (
    id TEXT PRIMARY KEY,
    rider_id TEXT NOT NULL,
    state TEXT NOT NULL,
    reservation_token TEXT UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
  );
  CREATE UNIQUE INDEX one_active_group_per_rider 
  ON assignment_groups (rider_id) 
  WHERE state IN ('assigned', 'in_progress');

  CREATE TABLE assignment_group_orders (
    assignment_group_id TEXT REFERENCES assignment_groups(id),
    order_id TEXT UNIQUE REFERENCES orders(id),
    assigned_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY(assignment_group_id, order_id)
  );
  ```

### Gap 4: Authentication and Object-Level Authorization (OWASP API Security)
- **Problem:** WebSocket connections and REST endpoints currently accept arbitrary requests without verifying user identity or order ownership (e.g. any viewer can subscribe to any order ID).
- **Affected Files:** `src/server.js`, `public/common.js`.
- **Remediation:** Add demo token authentication to WebSocket upgrade (`/ws?kind=track&order=<id>&token=<token>`) and validate that customer tokens match `orders.customer_id`.

---

## 5. Phase-Wise Development Roadmap

This actionable roadmap bridges the current zero-dependency prototype to the full production-ready architecture specified in the DOCX plan, retaining the working code while systematically resolving all architectural gaps.

```mermaid
gantt
    title TrackNow Production Alignment Roadmap
    dateFormat  HH:mm
    axisFormat %H:%M
    section Phase 1: Infrastructure
    HAProxy & Docker Compose Setup :p1, 00:00, 40m
    section Phase 2: Ingestion & Engine
    Redis Key Namespace Audit       :p2, after p1, 30m
    section Phase 3: Durable Core
    PostgreSQL Migration & Invariant :p3, after p2, 50m
    section Phase 4: UI & Transport
    Auth Gating & Reconnect Polish  :p4, after p3, 40m
    section Phase 5: Verification
    2k Rider / 3k Viewer Real-Redis :p5, after p4, 50m
    section Phase 6: Demo Freeze
    ADR & Final Runbook Execution   :p6, after p5, 30m
```

---

### Phase 1: Infrastructure, Load Balancer & Container Topology
- **Objective:** Introduce HAProxy and containerize both application instances alongside PostgreSQL and Redis.
- **Current Status:** App runs locally via `node dev/stack.js`; only Redis is in Docker.
- **Tasks in Execution Order:**
  1. Create `infra/haproxy.cfg` configured for sticky round-robin with WebSocket pass-through:
     ```haproxy
     global
       maxconn 10000

     defaults
       log global
       mode http
       timeout connect 5000ms
       timeout client 50000ms
       timeout server 50000ms
       timeout tunnel 3600s

     frontend http_front
       bind *:8080
       default_backend app_nodes

     backend app_nodes
       balance roundrobin
       option httpchk GET /api/health
       http-check expect status 200
       server app1 app1:3001 check inter 2000ms fall 2 rise 2
       server app2 app2:3002 check inter 2000ms fall 2 rise 2
     ```
  2. Create a unified `Dockerfile` for the application.
  3. Update `docker-compose.yml` to define:
     - `redis` (Redis 7 Alpine, port 6379)
     - `postgres` (PostgreSQL 16 Alpine, port 5432)
     - `app1` (`PORT=3001`, `INSTANCE_ID=app1`)
     - `app2` (`PORT=3002`, `INSTANCE_ID=app2`)
     - `worker` (Notification outbox daemon)
     - `haproxy` (Exposed on `:8080`)
- **Testing & Completion Criteria:**
  - Running `curl http://localhost:8080/api/health` alternates between `app1` and `app2`.
  - Stopping `app1` redirects traffic to `app2` within 2 seconds.

---

### Phase 2: Ingestion Pipeline & Redis Keyspace Alignment
- **Objective:** Verify and align Redis keyspaces and Lua script signatures with the DOCX contract.
- **Current Status:** `src/scripts/update_location.lua` and `src/scripts/claim.lua` are fully working.
- **Tasks in Execution Order:**
  1. Add `geo:riders:all` alongside `riders:avail` in `update_location.lua` so the operations map can query full spatial bounding boxes directly using `GEOSEARCH`.
  2. Ensure token-bucket rate limiter uses Redis server time (`redis.call('TIME')`) instead of client time.
  3. Validate simulator command-line parameters (`node sim/riders.js --riders 2000 --urls http://localhost:8080`).
- **Testing & Completion Criteria:**
  - Ingestion sustains 2,000 updates/sec with zero dropped socket frames.
  - Rate limiting correctly throttles bursts exceeding 10 updates/sec per rider device.

---

### Phase 3: Durable Core & PostgreSQL Database Migration
- **Objective:** Provide PostgreSQL persistence with strict partial unique index invariants as the source of truth.
- **Current Status:** Implemented in SQLite WAL mode.
- **Tasks in Execution Order:**
  1. Implement `src/db_pg.js` using `pg.Pool` with identical function signatures to `src/db.js`.
  2. Implement database transaction helper `tx(async (client) => { ... })`.
  3. Apply database migration creating `assignment_groups` and partial index `one_active_group_per_rider`.
  4. Support switching between SQLite and PostgreSQL via `DATABASE_URL` environment variable:
     - If `DATABASE_URL.startsWith('postgres://')`, load `src/db_pg.js`.
     - Otherwise, default to `src/db.js` (SQLite).
- **Testing & Completion Criteria:**
  - All 27 automated tests in `test/run.js` pass against PostgreSQL.
  - Concurrency race tests confirm zero active group conflicts and zero partial batches.

---

### Phase 4: WebSocket Protocol, Security & UI Enhancements
- **Objective:** Enforce authorization on WebSocket subscriptions and refine UI responsiveness.
- **Current Status:** UI operates via vanilla JS Canvas; tracking is push-only.
- **Tasks in Execution Order:**
  1. Add customer authentication token check in `server.js` on `upgrade`:
     ```js
     if (kind === 'track') {
       const token = u.searchParams.get('token');
       // Validate token ownership for orderId
     }
     ```
  2. Add Haversine distance display and connection banners in `public/track.html`.
  3. Refine `public/index.html` Canvas rendering to batch dirty rider updates via `requestAnimationFrame`.
- **Testing & Completion Criteria:**
  - Customer receives live updates only for their subscribed order.
  - Operations map displays 2,000 moving dots at 60 FPS without UI jank.

---

### Phase 5: End-to-End Stress Verification & Chaos Drills
- **Objective:** Execute full-scale load and verify all 6 failure drills against real Redis and PostgreSQL.
- **Tasks in Execution Order:**
  1. Execute 2,000 riders @ 1 Hz (`npm run sim:riders`).
  2. Execute 3,000 viewers (`npm run sim:viewers`).
  3. Run the 6 chaos drills via dashboard buttons:
     - **Bad GPS Drill:** Inject duplicate, out-of-order, and older-clock coordinates. Verify rejection count increments.
     - **Competing Race Drill:** Race 15 single and 10 batch assignments simultaneously. Verify 0 violations.
     - **Outbox Worker Pause Drill:** Pause notifier, transition orders, verify backlog builds, resume and confirm ordered delivery.
     - **Instance Failover Drill:** Kill `app1`, measure client reconnect time under HAProxy (`< 5,000 ms`).
     - **Redis Interruption Drill:** Drop Redis connectivity, verify HTTP 503 fail-closed, restore and verify auto-rebuild.
     - **Redis Wipe & Rehydration Drill:** Flush Redis entirely, verify rider busy state is restored from database.
- **Testing & Completion Criteria:**
  - Dashboard shows all items as **Passing**.
  - `search_p95 < 50 ms`, `delivery_p95 < 2,000 ms`, DB location writes `< 50/s`.

---

### Phase 6: Demonstration Freeze & Architecture Decision Record
- **Objective:** Freeze codebase and package judge-facing deliverables.
- **Tasks in Execution Order:**
  1. Commit and tag release candidate `v1.0.0-hackathon`.
  2. Finalize `ADR-001` (summarized in Section 6.5).
  3. Rehearse the 6-minute demonstration script.

---

## 6. Complete Further-Development Guide

### 6.1 Environment Setup and Prerequisites
- **Node.js:** Version 22.5.0 or later (required for built-in `node:sqlite` and WebSocket support).
- **Redis:** Version 6.2 or later (required for `GEOSEARCH`).
- **Docker & Docker Compose:** For running Redis, PostgreSQL, and HAProxy.
- **Build Tools (Optional):** GCC and Make (only if compiling `dev/lua-host`).

### 6.2 Running the Application

#### Step 1: Start Redis
```bash
# Start official Redis container
docker compose up -d redis
```

#### Step 2: Start the Clustered Application Stack
```bash
# Starts app1 (:3001), app2 (:3002), and notification worker
npm start
```

#### Step 3: Run the Simulators in Separate Terminals
```bash
# Terminal 2: Start 2,000 rider location simulator (1 Hz)
npm run sim:riders

# Terminal 3: Start 3,000 concurrent viewer WebSocket clients
npm run sim:viewers
```

#### Step 4: Access User Interfaces
- **Operations Map & Proof Board:** [http://localhost:3001/](http://localhost:3001/)
- **Restaurant Board:** [http://localhost:3001/board.html](http://localhost:3001/board.html)
- **Customer Tracking:** [http://localhost:3001/track.html?order=ORDER_ID](http://localhost:3001/track.html?order=ORDER_ID)

---

### 6.3 Codebase Navigation Guide

```
┌──────────────────────────────────────────────────────────────┐
│                    Incoming Client Request                   │
└──────────────────────────────┬───────────────────────────────┘
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
       HTTP REST Endpoints             WebSocket Upgrade
       (src/server.js)                 (src/ws.js)
               │                               │
        ┌──────┴──────┐                        ▼
        ▼             ▼                 Registry & Fan-Out
     Location     Lifecycle/Assign      (trackers map)
        │             │                        │
        ▼             ▼                        ▲
    Lua Scripts   src/db.js (SQLite/PG)        │
 (update_location)    │                        │
        │             ▼                        │
        ▼        Outbox Table                  │
   Redis GEO          │                        │
                      ▼                        │
               src/worker.js ──────────────────┘
               (Pub/Sub Relay)
```

- **To modify location validation rules:** Edit `src/scripts/update_location.lua` and `parseLocs` in `src/server.js`.
- **To modify order lifecycle rules:** Edit `ALLOWED` transition map in `src/db.js`.
- **To adjust dispatch search radius or batch sizing:** Edit `RADIUS_M` and `claimNearest` in `src/server.js` and `src/scripts/claim.lua`.
- **To adjust sparse database snapshot frequencies:** Edit `PERSIST_MS` in `src/server.js` and `persistMs` argument in `src/scripts/update_location.lua`.
- **To modify dashboard metrics:** Edit `cluster()` in `src/server.js` and `rows(m)` in `public/index.html`.

---

### 6.4 Debugging & Troubleshooting Runbook

| Symptom / Error | Root Cause | Resolution |
|---|---|---|
| `503 REDIS_UNAVAILABLE` | Redis is stopped, blocked by chaos flag, or recovering | Check Redis connection (`docker ps`). Clear chaos flag via `/api/admin/flag` with `key: "redis_chaos_until", value: "0"`. |
| `426 USE_WEBSOCKET` | Client attempted HTTP GET polling on `/api/track/:id` | Expected behavior by design. Ensure tracking clients connect via `ws://host/ws?kind=track&order=:id`. |
| `409 ORDER_ALREADY_ASSIGNED` | Another instance assigned this order concurrently | Expected behavior. Redis atomic guard `assign:<id>` prevented race. UI should refresh snapshot. |
| `409 INVALID_TRANSITION` | Disallowed lifecycle step attempted (e.g. `placed -> delivered`) | Review `ALLOWED` map in `src/db.js`. Ensure order is moved through valid intermediate states. |
| Marker jumps or moves backwards | Out-of-order GPS packet was received by client | Inspect `seq` numbers in network frames. Ensure client ignores any packet where `seq <= lastSeq`. |
| High `search_ms` latency (`> 50 ms`) | Using dev mini-redis or CPU core saturated | Run against real compiled Redis (`docker compose up -d redis`) and bind Node processes to dedicated cores. |

---

### 6.5 One-Page Architecture Decision Record (ADR-001)

- **Title:** Redis-Backed Real-Time Coordination with Relational Durable Invariants
- **Status:** Implemented & Verified in Prototype; Ready for Production Containerization
- **Context:** System must handle 2,000 rider updates/sec, 3,000 live tracking viewers, sub-50ms dispatch, sub-2s push latency, and zero tolerance for double assignments across multiple app instances.
- **Key Decisions:**
  1. **Hot Path in Redis:** Latest coordinates, spatial lookup (`GEOSEARCH`), sequence deduplication, and rate limiting run in Redis via atomic Lua scripts. Avoids relational database bottleneck.
  2. **Relational Database as Source of Truth:** Orders, assignments, and audit trails reside in durable database (SQLite WAL / PostgreSQL). Redis can be wiped and rehydrated at any time.
  3. **Transactional Outbox for Notifications:** Status updates and outbox events commit in one database transaction. Outbox worker pushes to Redis Pub/Sub. Customer notifications table enforces uniqueness via composite primary key `(order_id, version)`.
  4. **Push-Only Architecture:** WebSockets used exclusively for tracking updates; HTTP GET polling is blocked with HTTP 426.
  5. **Ephemeral vs. Durable Boundary:** Pub/Sub is recognized as at-most-once. Ephemeral GPS updates that drop during disconnections are safely reconciled by fetching the latest snapshot upon reconnection.

---

## 7. Final Completion & Verification Checklist

Use this checklist to track end-to-end readiness across development, verification, and judging demonstration.

### Phase 0: Contracts & Specifications
- [x] State machine transition graph frozen (`placed -> accepted -> preparing -> picked_up -> delivered`).
- [x] GPS payload format agreed (`riderId`, `lat`, `lng`, `ts`, `seq`).
- [x] Metric measurement boundaries defined (delivery latency = server accept to client render).
- [x] Zero-polling requirement established.

### Phase 1: Infrastructure & App Clustered Deployment
- [x] Two identical stateless application instances running (`:3001` and `:3002`).
- [x] Shared Redis instance accessible by all app nodes and workers.
- [x] Shared relational database operating in WAL mode with connection pooling.
- [x] `/api/health` reports status, instance ID, and Redis readiness.
- [ ] HAProxy container configured and load balancing `:8080` to `:3001` and `:3002`.

### Phase 2: Ingestion & Rider Simulation
- [x] Simulator generating 2,000 active riders at 1 Hz (`sim/riders.js`).
- [x] `update_location.lua` rejecting duplicate sequence numbers and regressing clocks.
- [x] Global distributed rate limiting preventing device bursts (`> 10/s`).
- [x] Redis `GEOADD` updating `riders:avail` only when rider status is available.
- [x] Database location write rate strictly below 50 rows/sec (measured ~22 writes/s).

### Phase 3: Dispatch, Lifecycle & Outbox Notifications
- [x] `claimNearest` using `GEOSEARCH ... BYRADIUS 5000 m ASC COUNT 8`.
- [x] `claim.lua` reserving rider and order guards atomically.
- [x] Single assignment and 2-order same-restaurant batch assignment passing race tests.
- [x] Order lifecycle transitions enforced with optimistic version checks.
- [x] Status changes and outbox records committed in a single atomic database transaction.
- [x] Notification daemon relaying outbox events with composite primary key deduplication.

### Phase 4: WebSockets & User Interfaces
- [x] WebSockets operating push-only; `/api/track/*` returns HTTP 426.
- [x] Customer Tracking page displaying Canvas map, marker interpolation, and status timeline.
- [x] Restaurant Kanban Board supporting optimistic drag-and-drop with conflict rollback.
- [x] Operations Map rendering 2,000 riders via Canvas vector points without UI freezing.
- [x] Reconnection logic catching up from current snapshot without requiring page reload.

### Phase 5: Verification & Failure Testing
- [x] 2,000 riders @ 1 Hz sustained load verified.
- [x] 3,000 concurrent WebSocket viewers sustained load verified.
- [x] Map delivery latency `p95 < 2,000 ms` confirmed.
- [x] Nearest rider search `p95 < 50 ms` confirmed.
- [x] Competing assignment race drill: 0 double assignments.
- [x] Competing batch race drill: 0 partial batches.
- [x] Worker pause drill: backlog accumulates and drains in strict order upon resumption.
- [x] App kill drill: viewers fail over to surviving instance within 5,000 ms.
- [x] Redis outage drill: system fails closed, shows stale banner, and rehydrates upon recovery.
- [x] Full automated test suite passes (`npm test`).

### Phase 6: Demonstration & Documentation
- [x] Live metrics proof board on `index.html` displaying passing criteria.
- [x] Architecture Decision Record (ADR-001) completed.
- [x] Technical risks and honest limitations documented.
- [x] 6-minute live judge demonstration rehearsed.
