# TrackNow — Architecture

## System Flow

```mermaid
flowchart TD
    subgraph Clients["Browser Clients"]
        C1["Customer\ntrack.html\nWebSocket track"]
        C2["Restaurant\nboard.html\nWebSocket board"]
        C3["Ops\nindex.html\nWebSocket ops"]
    end

    LB["HAProxy :8080\nRound-robin + WS tunnel\nStick by source IP"]

    subgraph App["Stateless App Instances (Node.js)"]
        A1["app1 :3001\nHTTP + WebSocket\nMetrics, ingest, assign"]
        A2["app2 :3002\nHTTP + WebSocket\nMetrics, ingest, assign"]
    end

    subgraph RedisLayer["Redis (Real-time layer)"]
        R1["riders:avail\nGEO sorted set\n≤5000m radius search"]
        R2["rider:&lt;id&gt; hash\nseq, ts, lat, lng, status\norders, batch, rx, persist"]
        R3["assign:&lt;oid&gt;\nGuard key (atomic claim)"]
        R4["rl:&lt;id&gt;:&lt;sec&gt;\nPer-device rate limit"]
        R5["ops:loc channel\nBatch location fan-out"]
        R6["orders:events channel\nOrder state fan-out"]
        R7["track:&lt;oid&gt; channel\nPer-order location push"]
        R8["instances hash\nCross-instance metrics"]
    end

    subgraph DB["SQLite WAL (source of truth)"]
        D1["orders table\nFull lifecycle + assignment"]
        D2["outbox table\nTransactional outbox"]
        D3["notifications table\nExactly-once delivery log"]
        D4["location_history\n1 row/rider/90 s"]
        D5["flags / counters\nChaos + metrics"]
    end

    W["Outbox Worker\nsrc/worker.js\n150 ms poll"]

    SIM["Rider Simulator\nsim/riders.js\n2000 riders @ 1 Hz\nChaos API :4200"]
    VW["Viewer Simulator\nsim/viewers.js\n3000 WS connections"]

    C1 & C2 & C3 --> LB
    LB --> A1 & A2

    A1 & A2 --> R1 & R2 & R3 & R4 & R5 & R6 & R7 & R8
    A1 & A2 --> D1 & D2 & D3 & D4 & D5

    R5 -.->|PUBLISH| A1 & A2
    R6 -.->|PUBLISH| A1 & A2
    R7 -.->|PUBLISH| A1 & A2

    A1 & A2 -.->|fan-out WS frame| C1 & C2 & C3

    W --> D2 & D3
    SIM -->|POST /api/rider/locations| A1 & A2
    VW -->|WS /ws?kind=track| LB
```

## Key Design Decisions

| Concern | Solution |
|---------|----------|
| Location scale | GPS batched in chunks of 250, ingested via `update_location.lua` atomically |
| Zero double-assignment | `claim.lua` Lua script: check + write in one atomic MULTI-exec |
| Atomic batch | Same Lua script claims both orders or neither |
| Stale GPS rejection | Monotonic `seq` + `ts` checked inside Lua |
| Rate limiting | Fixed-window per-device via `rl:<id>:<sec>` INCR + PEXPIRE |
| Fan-out routing | Per-order Redis channel `track:<orderId>` → only the right viewers |
| Exactly-once notifications | Transactional Outbox in SQLite; `PRIMARY KEY (order_id, version)` prevents duplicates |
| DB write throttle | Persist flag in Redis hash; 1 row per rider per 90 s = ≈22 writes/s for 2000 riders |
| Redis failure | Fail-closed on assignment, fail-open on rate-limit; `rebuild()` restores state from DB on recovery |
| Instance failover | Client rotates to next endpoint in `eps[]` on WebSocket close; reconnect p95 < 5 s |
| Ops map efficiency | Only dirty riders sent per second; client-side Canvas renders 2000 dots at 60 fps |

## Data Path: Location Update

```mermaid
sequenceDiagram
    participant Rider as Rider Simulator
    participant App as App Instance (any)
    participant Redis
    participant DB as SQLite
    participant Customer as Customer Browser

    Rider->>App: POST /api/rider/locations [batch]
    App->>Redis: EVALSHA update_location.lua (per rider)
    Redis-->>App: {accepted, status, orders, persistFlag}
    App->>Redis: PUBLISH ops:loc [batch update]
    App->>Redis: PUBLISH track:<orderId> {lat,lng,ts,seq}
    opt persistFlag=1 (every 90 s)
        App->>DB: INSERT location_history (buffered)
    end
    Redis-->>App: (ops:loc subscriber) broadcast to ops clients
    Redis-->>App: (track:<oid> subscriber) forward to tracking clients
    App-->>Customer: WebSocket frame {t:'loc', lat, lng, ts, seq}
```

## Data Path: Rider Assignment

```mermaid
sequenceDiagram
    participant Client
    participant App
    participant Redis
    participant DB

    Client->>App: POST /api/orders/<id>/assign
    App->>Redis: GEOSEARCH riders:avail FROMLONLAT ... ASC COUNT 8
    loop For each candidate rider
        App->>Redis: EVALSHA claim.lua (atomic check+write)
        alt Rider available & order not taken
            Redis-->>App: [1, 'ok', orderList]
            App->>DB: UPDATE orders SET rider_id (transaction)
            App->>Redis: PUBLISH orders:events {t:'rider', st:'busy'}
            App-->>Client: 200 {riderId, orders}
        else Already taken
            Redis-->>App: [0, 'rider_unavailable']
            Note over App,Redis: Try next candidate
        end
    end
```
