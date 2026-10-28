-- Lumina Link System — link-centric schema (PostgreSQL port of link-centric-system.html).
-- Epoch timestamps are BIGINT. Enums via CHECK. Idempotent (IF NOT EXISTS).

-- ---- Automation (campaign) ----
CREATE TABLE IF NOT EXISTS campaigns (
  id           BIGSERIAL PRIMARY KEY,
  name         TEXT NOT NULL,
  mode         TEXT NOT NULL DEFAULT 'practice' CHECK (mode IN ('practice','live')),
  source       TEXT CHECK (source IN ('sheet','clippers','sideshift','manual')),
  source_ref   TEXT,
  goal_views   BIGINT,
  start_date   TEXT,
  end_date     TEXT,
  budget_usd   DOUBLE PRECISION,
  cap_usd      DOUBLE PRECISION,
  margin_pct   DOUBLE PRECISION NOT NULL DEFAULT 12.5,
  approved     BOOLEAN NOT NULL DEFAULT false,
  created_at   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS creators (
  id           BIGSERIAL PRIMARY KEY,
  external_id  TEXT,
  handle       TEXT,
  name         TEXT,
  platform     TEXT,
  created_at   BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS creators_external ON creators(external_id);

CREATE TABLE IF NOT EXISTS campaign_creators (
  campaign_id  BIGINT NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  creator_id   BIGINT NOT NULL REFERENCES creators(id) ON DELETE CASCADE,
  included     BOOLEAN NOT NULL DEFAULT true,
  cancelled    BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (campaign_id, creator_id)
);

-- ---- Core: the Link ----
CREATE TABLE IF NOT EXISTS links (
  id               BIGSERIAL PRIMARY KEY,
  campaign_id      BIGINT REFERENCES campaigns(id) ON DELETE SET NULL,
  creator_id       BIGINT REFERENCES creators(id) ON DELETE SET NULL,
  url              TEXT NOT NULL,
  url_key          TEXT NOT NULL,
  platform         TEXT NOT NULL,
  source           TEXT CHECK (source IN ('sheet','clippers','sideshift','manual')),
  provider         TEXT,
  mode             TEXT NOT NULL DEFAULT 'approve',
  target_views     BIGINT NOT NULL DEFAULT 0,
  target_likes     BIGINT NOT NULL DEFAULT 0,
  target_comments  BIGINT NOT NULL DEFAULT 0,
  actual_views     BIGINT NOT NULL DEFAULT 0,
  actual_likes     BIGINT NOT NULL DEFAULT 0,
  actual_comments  BIGINT NOT NULL DEFAULT 0,
  active           BOOLEAN NOT NULL DEFAULT true,
  last_polled_at   BIGINT,
  last_error       TEXT,
  created_at       BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS links_url_key  ON links(url_key);
CREATE INDEX IF NOT EXISTS links_campaign ON links(campaign_id);

CREATE TABLE IF NOT EXISTS post_snapshots (
  id       BIGSERIAL PRIMARY KEY,
  link_id  BIGINT NOT NULL REFERENCES links(id) ON DELETE CASCADE,
  t        BIGINT NOT NULL,
  views    BIGINT,
  likes    BIGINT,
  comments BIGINT
);
CREATE INDEX IF NOT EXISTS post_snapshots_link_t ON post_snapshots(link_id, t);

-- ---- Orders + delivery ----
CREATE TABLE IF NOT EXISTS orders (
  id              BIGSERIAL PRIMARY KEY,
  link_id         BIGINT NOT NULL REFERENCES links(id) ON DELETE CASCADE,
  kind            TEXT NOT NULL CHECK (kind IN ('views','likes','comments')),
  source          TEXT NOT NULL DEFAULT 'manual' CHECK (source IN ('manual','auto')),
  target_qty      BIGINT NOT NULL,
  delivered       BIGINT NOT NULL DEFAULT 0,
  start_count     BIGINT,
  state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending','running','stuck','completed','partial','failed','cancelled')),
  provider        TEXT NOT NULL,
  service         TEXT NOT NULL,
  est_cost        DOUBLE PRECISION NOT NULL DEFAULT 0,
  comment_lines   JSONB,
  panel_order_id  TEXT,
  created_at      BIGINT NOT NULL,
  last_checked_at BIGINT,
  completed_at    BIGINT
);
CREATE INDEX IF NOT EXISTS orders_link      ON orders(link_id);
CREATE INDEX IF NOT EXISTS orders_state     ON orders(state);
CREATE INDEX IF NOT EXISTS orders_link_kind ON orders(link_id, kind);

CREATE TABLE IF NOT EXISTS order_snapshots (
  id        BIGSERIAL PRIMARY KEY,
  order_id  BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  t         BIGINT NOT NULL,
  delivered BIGINT
);
CREATE INDEX IF NOT EXISTS order_snapshots_order_t ON order_snapshots(order_id, t);

-- ---- Comment pipeline ----
CREATE TABLE IF NOT EXISTS comments (
  id          BIGSERIAL PRIMARY KEY,
  link_id     BIGINT NOT NULL REFERENCES links(id) ON DELETE CASCADE,
  text        TEXT NOT NULL,
  state       TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','posted','rejected')),
  order_id    BIGINT REFERENCES orders(id) ON DELETE SET NULL,
  model       TEXT,
  created_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS comments_link  ON comments(link_id);
CREATE INDEX IF NOT EXISTS comments_state ON comments(link_id, state);

CREATE TABLE IF NOT EXISTS transcripts (
  id          BIGSERIAL PRIMARY KEY,
  link_id     BIGINT NOT NULL REFERENCES links(id) ON DELETE CASCADE,
  transcript  TEXT,
  video_url   TEXT,
  duration    BIGINT,
  created_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS transcripts_link ON transcripts(link_id);

CREATE TABLE IF NOT EXISTS briefs (
  id          BIGSERIAL PRIMARY KEY,
  campaign_id BIGINT NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  text        TEXT NOT NULL,
  created_at  BIGINT NOT NULL
);

-- ---- Ops / identity ----
CREATE TABLE IF NOT EXISTS users (
  id         BIGSERIAL PRIMARY KEY,
  username   TEXT NOT NULL UNIQUE,
  pw_hash    TEXT NOT NULL,
  active     BOOLEAN NOT NULL DEFAULT true,
  created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
  id       BIGSERIAL PRIMARY KEY,
  at       BIGINT NOT NULL,
  username TEXT,
  action   TEXT NOT NULL,
  target   TEXT,
  detail   TEXT
);

CREATE TABLE IF NOT EXISTS pauses (
  id         BIGSERIAL PRIMARY KEY,
  scope      TEXT NOT NULL,
  key        TEXT NOT NULL,
  label      TEXT,
  created_by TEXT,
  created_at BIGINT NOT NULL,
  UNIQUE (scope, key)
);
