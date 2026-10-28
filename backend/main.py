"""Lumina Link System — minimal FastAPI backend over the link-centric Postgres schema.

Creates the schema on boot (idempotent) and exposes a small read/write API for links
and orders. This is a fresh greenfield service — no panel money-moving logic here yet.
"""
from __future__ import annotations

import os
import pathlib
import re
import time
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DB_URL = os.environ.get("DATABASE_URL", "")
SCHEMA = pathlib.Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    if not DB_URL:
        raise RuntimeError("DATABASE_URL is not set")
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as c:
        await c.execute(SCHEMA)
    yield
    await pool.close()


app = FastAPI(title="Lumina Link System", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def url_key(url: str) -> str:
    return re.sub(r"[?#].*$", "", (url or "").strip().lower()).rstrip("/")


@app.get("/health")
async def health():
    async with pool.acquire() as c:
        counts = {}
        for t in ("campaigns", "creators", "links", "orders", "comments", "post_snapshots"):
            counts[t] = await c.fetchval(f"SELECT COUNT(*) FROM {t}")
    return {"ok": True, "service": "lumina-link-system", "counts": counts}


class LinkIn(BaseModel):
    url: str
    platform: str
    campaign_id: int | None = None
    creator_id: int | None = None
    provider: str | None = None
    target_views: int = 0
    target_likes: int = 0
    target_comments: int = 0


@app.get("/api/links")
async def list_links():
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT * FROM links ORDER BY id DESC LIMIT 200")
    return {"links": [dict(r) for r in rows]}


@app.post("/api/links")
async def create_link(b: LinkIn):
    async with pool.acquire() as c:
        row = await c.fetchrow(
            "INSERT INTO links (campaign_id, creator_id, url, url_key, platform, source, provider, "
            "target_views, target_likes, target_comments, created_at) "
            "VALUES ($1,$2,$3,$4,$5,'manual',$6,$7,$8,$9,$10) RETURNING *",
            b.campaign_id, b.creator_id, b.url, url_key(b.url), b.platform, b.provider,
            b.target_views, b.target_likes, b.target_comments, int(time.time()))
    return dict(row)


@app.get("/api/links/{lid}")
async def get_link(lid: int):
    async with pool.acquire() as c:
        row = await c.fetchrow("SELECT * FROM links WHERE id=$1", lid)
        if not row:
            raise HTTPException(404, "no such link")
        orders = await c.fetch("SELECT * FROM orders WHERE link_id=$1 ORDER BY id DESC", lid)
        snaps = await c.fetch(
            "SELECT t, views, likes, comments FROM post_snapshots WHERE link_id=$1 ORDER BY t DESC LIMIT 50", lid)
    return {"link": dict(row), "orders": [dict(o) for o in orders], "snapshots": [dict(s) for s in snaps]}


class OrderIn(BaseModel):
    link_id: int
    kind: str          # views | likes | comments
    target_qty: int
    provider: str
    service: str
    source: str = "manual"
    est_cost: float = 0


@app.post("/api/orders")
async def create_order(b: OrderIn):
    if b.kind not in ("views", "likes", "comments"):
        raise HTTPException(400, "kind must be views, likes or comments")
    async with pool.acquire() as c:
        row = await c.fetchrow(
            "INSERT INTO orders (link_id, kind, source, target_qty, provider, service, est_cost, created_at) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *",
            b.link_id, b.kind, b.source, b.target_qty, b.provider, b.service, b.est_cost, int(time.time()))
    return dict(row)


@app.get("/api/orders")
async def list_orders(state: str | None = None, link_id: int | None = None):
    q, args = "SELECT * FROM orders", []
    where = []
    if state:
        args.append(state); where.append(f"state=${len(args)}")
    if link_id:
        args.append(link_id); where.append(f"link_id=${len(args)}")
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY id DESC LIMIT 200"
    async with pool.acquire() as c:
        rows = await c.fetch(q, *args)
    return {"orders": [dict(r) for r in rows]}
