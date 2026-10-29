"""Phase 0: link-centric Postgres store (runs alongside the console's SQLite state).

The new campaign-detail analytics (links, snapshots, orders, creators) live here so
they are first-class and durable; the existing planner/automation keeps its SQLite.
Reads DATABASE_URL (Render internal conn string — no SSL inside the network).
Init is tolerant: if Postgres is unreachable the app still boots (the tracker
endpoints just return 503 until it's back).
"""
from __future__ import annotations

import logging
import os
import re
import time
import urllib.parse

log = logging.getLogger("pg")

DB_URL = os.environ.get("DATABASE_URL", "")
_pool = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS tracker_campaigns (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT NOT NULL,
    source_ref  TEXT NOT NULL,
    name        TEXT NOT NULL DEFAULT '',
    company     TEXT NOT NULL DEFAULT '',
    views_goal  BIGINT,
    created_at  BIGINT NOT NULL,
    UNIQUE (source, source_ref)
);
CREATE TABLE IF NOT EXISTS tracker_creators (
    id      BIGSERIAL PRIMARY KEY,
    source  TEXT NOT NULL,
    ext_id  TEXT NOT NULL,
    name    TEXT NOT NULL DEFAULT '',
    handle  TEXT NOT NULL DEFAULT '',
    UNIQUE (source, ext_id)
);
CREATE TABLE IF NOT EXISTS tracker_campaign_creators (
    campaign_id BIGINT NOT NULL REFERENCES tracker_campaigns(id) ON DELETE CASCADE,
    creator_id  BIGINT NOT NULL REFERENCES tracker_creators(id) ON DELETE CASCADE,
    PRIMARY KEY (campaign_id, creator_id)
);
CREATE TABLE IF NOT EXISTS tracker_links (
    id            BIGSERIAL PRIMARY KEY,
    campaign_id   BIGINT NOT NULL REFERENCES tracker_campaigns(id) ON DELETE CASCADE,
    creator_id    BIGINT REFERENCES tracker_creators(id) ON DELETE SET NULL,
    url           TEXT NOT NULL,
    url_key       TEXT NOT NULL,
    platform      TEXT NOT NULL DEFAULT '',
    ext_post_id   TEXT,
    title         TEXT NOT NULL DEFAULT '',
    thumbnail     TEXT NOT NULL DEFAULT '',
    uploaded_at   BIGINT,
    target_views  BIGINT NOT NULL DEFAULT 0,
    target_likes  BIGINT NOT NULL DEFAULT 0,
    target_comments BIGINT NOT NULL DEFAULT 0,
    created_at    BIGINT NOT NULL,
    UNIQUE (campaign_id, url_key)
);
CREATE INDEX IF NOT EXISTS ix_tracker_links_campaign ON tracker_links(campaign_id);
CREATE TABLE IF NOT EXISTS tracker_post_snapshots (
    id        BIGSERIAL PRIMARY KEY,
    link_id   BIGINT NOT NULL REFERENCES tracker_links(id) ON DELETE CASCADE,
    t         BIGINT NOT NULL,
    views     BIGINT,
    likes     BIGINT,
    comments  BIGINT,
    shares    BIGINT
);
CREATE INDEX IF NOT EXISTS ix_tracker_snap_link_t ON tracker_post_snapshots(link_id, t);
CREATE TABLE IF NOT EXISTS tracker_orders (
    id             BIGSERIAL PRIMARY KEY,
    link_id        BIGINT NOT NULL REFERENCES tracker_links(id) ON DELETE CASCADE,
    kind           TEXT NOT NULL CHECK (kind IN ('views','likes','comments')),
    provider       TEXT,
    service        TEXT,
    panel_order_id TEXT,
    qty            BIGINT,
    remains        BIGINT,
    status         TEXT NOT NULL DEFAULT 'pending',
    charge         DOUBLE PRECISION,
    created_at     BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_tracker_orders_link ON tracker_orders(link_id);
"""


def url_key(u: str) -> str:
    """Normalize a post URL so the same post dedupes: drop scheme/query/fragment,
    lowercase host, strip a trailing slash."""
    u = (u or "").strip()
    if not u:
        return ""
    try:
        p = urllib.parse.urlsplit(u if "//" in u else "https://" + u)
        host = (p.netloc or "").lower().removeprefix("www.")
        path = re.sub(r"/+$", "", p.path)
        return f"{host}{path}"
    except ValueError:
        return u.lower()


def available() -> bool:
    return _pool is not None


async def init() -> None:
    """Create the pool and the tracker tables. Tolerant: logs and returns on failure."""
    global _pool
    if not DB_URL:
        log.warning("DATABASE_URL not set; tracker (Postgres) disabled")
        return
    try:
        import asyncpg
        _pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5, command_timeout=30)
        async with _pool.acquire() as c:
            await c.execute(SCHEMA)
        log.info("tracker Postgres ready")
    except Exception as e:  # noqa: BLE001
        _pool = None
        log.warning("tracker Postgres unavailable: %s", e)


async def _require():
    if _pool is None:
        raise RuntimeError("tracker store (Postgres) is not available")
    return _pool


async def import_pull(source: str, ref: str, name: str, company: str,
                      links: list[dict], views_goal: int | None = None) -> dict:
    """Upsert a pulled campaign + its creators + links + a first snapshot each.
    `links` = the preview dicts: {url, platform, creator, views, likes, comments, uploaded_at, thumbnail}."""
    pool = await _require()
    now = int(time.time())
    async with pool.acquire() as c:
        async with c.transaction():
            cid = await c.fetchval(
                """INSERT INTO tracker_campaigns (source, source_ref, name, company, views_goal, created_at)
                   VALUES ($1,$2,$3,$4,$5,$6)
                   ON CONFLICT (source, source_ref) DO UPDATE
                     SET name = EXCLUDED.name, company = EXCLUDED.company,
                         views_goal = COALESCE(EXCLUDED.views_goal, tracker_campaigns.views_goal)
                   RETURNING id""",
                source, str(ref), name or "", company or "", views_goal, now)
            n_links = n_creators = n_snaps = 0
            seen_creators: dict[str, int] = {}
            for l in links:
                url = (l.get("url") or "").strip()
                if not url:
                    continue
                cr_name = (l.get("creator") or "").strip()
                creator_id = None
                if cr_name:
                    key = cr_name.lower()
                    creator_id = seen_creators.get(key)
                    if creator_id is None:
                        creator_id = await c.fetchval(
                            """INSERT INTO tracker_creators (source, ext_id, name, handle)
                               VALUES ($1,$2,$3,$4)
                               ON CONFLICT (source, ext_id) DO UPDATE SET name = EXCLUDED.name
                               RETURNING id""",
                            source, cr_name, cr_name, l.get("handle") or "")
                        seen_creators[key] = creator_id
                        n_creators += 1
                        await c.execute(
                            """INSERT INTO tracker_campaign_creators (campaign_id, creator_id)
                               VALUES ($1,$2) ON CONFLICT DO NOTHING""", cid, creator_id)
                link_id = await c.fetchval(
                    """INSERT INTO tracker_links
                         (campaign_id, creator_id, url, url_key, platform, title, thumbnail, uploaded_at, created_at)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                       ON CONFLICT (campaign_id, url_key) DO UPDATE
                         SET creator_id = COALESCE(EXCLUDED.creator_id, tracker_links.creator_id),
                             platform = EXCLUDED.platform, thumbnail = EXCLUDED.thumbnail
                       RETURNING id""",
                    cid, creator_id, url, url_key(url), l.get("platform") or "",
                    l.get("title") or "", l.get("thumbnail") or "", l.get("uploaded_at"), now)
                n_links += 1
                if any(l.get(k) is not None for k in ("views", "likes", "comments", "shares")):
                    await c.execute(
                        """INSERT INTO tracker_post_snapshots (link_id, t, views, likes, comments, shares)
                           VALUES ($1,$2,$3,$4,$5,$6)""",
                        link_id, now, l.get("views"), l.get("likes"), l.get("comments"), l.get("shares"))
                    n_snaps += 1
    return {"campaign_id": cid, "links": n_links, "creators": n_creators, "snapshots": n_snaps}


async def list_campaigns() -> list[dict]:
    pool = await _require()
    rows = await pool.fetch(
        """SELECT c.*, (SELECT COUNT(*) FROM tracker_links l WHERE l.campaign_id = c.id) AS link_count
           FROM tracker_campaigns c ORDER BY c.created_at DESC""")
    return [dict(r) for r in rows]


async def campaign_links(cid: int) -> list[dict]:
    pool = await _require()
    rows = await pool.fetch(
        """SELECT l.*, cr.name AS creator_name,
                  (SELECT s.views FROM tracker_post_snapshots s
                   WHERE s.link_id = l.id ORDER BY s.t DESC LIMIT 1) AS latest_views
           FROM tracker_links l LEFT JOIN tracker_creators cr ON cr.id = l.creator_id
           WHERE l.campaign_id = $1 ORDER BY latest_views DESC NULLS LAST""", cid)
    return [dict(r) for r in rows]
