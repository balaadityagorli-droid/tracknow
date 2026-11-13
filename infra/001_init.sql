-- TrackNow PostgreSQL schema (for production upgrade; dev uses SQLite WAL)
-- Apply with: psql $DATABASE_URL -f infra/001_init.sql

CREATE TABLE IF NOT EXISTS orders (
  id              TEXT PRIMARY KEY,
  restaurant_id   TEXT NOT NULL,
  customer_id     TEXT NOT NULL,
  pickup_lat      DOUBLE PRECISION,
  pickup_lng      DOUBLE PRECISION,
  drop_lat        DOUBLE PRECISION,
  drop_lng        DOUBLE PRECISION,
  status          TEXT NOT NULL,
  version         INTEGER NOT NULL,
  rider_id        TEXT,
  batch_id        TEXT,
  created_at      BIGINT,
  updated_at      BIGINT
);
CREATE INDEX IF NOT EXISTS orders_rider ON orders(rider_id);
CREATE INDEX IF NOT EXISTS orders_status ON orders(status) WHERE status NOT IN ('delivered','cancelled');

-- Prevents the same restaurant's two orders being partially batched:
-- partial unique index enforces that each batch_id covers exactly 2 rows.
CREATE TABLE IF NOT EXISTS assignment_groups (
  batch_id        TEXT NOT NULL,
  order_id        TEXT NOT NULL REFERENCES orders(id),
  PRIMARY KEY (batch_id, order_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ag_one_per_batch
  ON assignment_groups(batch_id)
  WHERE batch_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS idem (
  key         TEXT PRIMARY KEY,
  result      TEXT,
  created_at  BIGINT
);

CREATE TABLE IF NOT EXISTS outbox (
  id            BIGSERIAL PRIMARY KEY,
  order_id      TEXT NOT NULL,
  version       INTEGER NOT NULL,
  status        TEXT NOT NULL,
  created_at    BIGINT,
  processed_at  BIGINT,
  attempts      INTEGER DEFAULT 0,
  last_error    TEXT,
  UNIQUE(order_id, version)
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(processed_at, id)
  WHERE processed_at IS NULL;

CREATE TABLE IF NOT EXISTS notifications (
  order_id      TEXT NOT NULL,
  version       INTEGER NOT NULL,
  status        TEXT NOT NULL,
  delivered_at  BIGINT,
  PRIMARY KEY (order_id, version)
);

CREATE TABLE IF NOT EXISTS location_history (
  rider_id  TEXT,
  lat       DOUBLE PRECISION,
  lng       DOUBLE PRECISION,
  ts        BIGINT,
  seq       INTEGER
);
-- Partial index: keep only last 24 h, prune older rows via cron
CREATE INDEX IF NOT EXISTS lh_recent ON location_history(ts DESC)
  WHERE ts > (extract(epoch from now()) * 1000)::BIGINT - 86400000;

CREATE TABLE IF NOT EXISTS flags    (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value BIGINT NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS releases (rider_id TEXT PRIMARY KEY);
