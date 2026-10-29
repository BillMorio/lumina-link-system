"""Lumina Engagement Console — FastAPI backend.

One API over all three PerfectPanel panels (SMMGlow / MoreThanPanel / JAP), mirroring
the Telegram bots: sequenced orders, bulk engagement (4 sizing modes), fast likes, and
a live job monitor driven by the in-process drip scheduler.
"""
from __future__ import annotations

import asyncio
import csv
import hmac
import io
import random
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

import automation
import campaigns
import users
import clippers
import db
import engine
import panels as panels_mod
import score
from config import (
    API_ACCESS_CODE, ENGAGEMENT_PLATFORMS, PROVIDERS, RATE_MAP,
    SMART_VIEWS_MIN, detect_platform, provider_bot, provider_label, providers,
    COMMENT_MAP, LOW_BALANCE_USD, comment_service, effective_service_map, rate_for, service_for,
)
from panels import PanelError, client_for

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger("console")


async def _audit(who: str, action: str, target, detail=None) -> None:
    """v2.7: one audit_log row per money/automation action (username = the caller)."""
    await asyncio.to_thread(automation.audit, who, action, target, detail)


@asynccontextmanager
async def lifespan(app: FastAPI):
    import persist
    import pg
    import tracker
    await pg.init()           # Phase 0 tracker store; tolerant if Postgres is down
    await persist.restore()   # console SQLite back from Postgres before anything opens it
    db.init_db()
    persist.start()           # back the console SQLite up to Postgres every 30 s when it changed
    tracker.start()           # Phase 1 snapshot worker (no-op while the store is down)
    score.load_overrides()
    stuck = campaigns.recover_stuck()
    if stuck:
        log.warning("marked %d queue row(s) stuck in 'placing' as failed", stuck)
    from scheduler import run_scheduler
    task = asyncio.create_task(run_scheduler())
    prewarm = asyncio.create_task(panels_mod.prewarm_services())
    log.info("console API started")
    yield
    task.cancel()
    await persist.shutdown()  # final backup: Render stops the old instance on every deploy


app = FastAPI(title="Lumina Engagement Console", version="1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# --------------------------------------------------------------------------- #
# Auth (shared access code, mirrors the bot's /login)
# --------------------------------------------------------------------------- #
def _is_code(authorization: str) -> bool:
    token = authorization.replace("Bearer ", "").strip()
    return bool(token) and hmac.compare_digest(token.encode(), API_ACCESS_CODE.encode())


def require_auth(authorization: str = Header(default=""), x_lec_user: str = Header(default="")) -> str:
    """Bearer = a user login token, or the shared access code (operator/back-compat). With the access code only,
    header X-LEC-User names the person for audit / created_by (letters, digits, _ . @ - kept, max 40)."""
    token = authorization.replace("Bearer ", "").strip()
    if _is_code(authorization):
        return re.sub(r"[^A-Za-z0-9_.@-]", "", x_lec_user.strip())[:40] or "operator"
    user = users.user_for_token(token) if token else None
    if not user:
        raise HTTPException(status_code=401, detail="Signed out. Sign in again.")
    return user


class LoginBody(BaseModel):
    username: str = ""
    password: str = ""
    code: str = ""


_LOGIN_FAILS: dict[str, list[float]] = {}


def _login_guard(request: Request) -> str:
    """10 failed sign-ins per IP per 15 min, then 429 until the window clears."""
    ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
          or (request.client.host if request.client else "?"))
    now = time.time()
    _LOGIN_FAILS[ip] = [t for t in _LOGIN_FAILS.get(ip, []) if now - t < 900]
    if len(_LOGIN_FAILS[ip]) >= 10:
        raise HTTPException(status_code=429, detail="Too many tries. Wait 15 minutes.")
    return ip


@app.post("/api/auth/login")
async def login(body: LoginBody, request: Request):
    ip = _login_guard(request)
    try:
        return await _login(body)
    except HTTPException:
        _LOGIN_FAILS[ip].append(time.time())
        raise


async def _login(body: LoginBody):
    if body.username.strip():
        u = await asyncio.to_thread(users.check_password, body.username, body.password)
        if not u:
            raise HTTPException(status_code=401, detail="Wrong username or password")
        return {"token": users.issue_token(u), "user": u["username"]}
    if body.code.strip() and hmac.compare_digest(body.code.strip().encode(), API_ACCESS_CODE.encode()):
        return {"token": API_ACCESS_CODE, "user": "operator"}
    raise HTTPException(status_code=401, detail="Wrong username or password")


class NewUserBody(BaseModel):
    username: str


@app.post("/api/users")
async def add_user(body: NewUserBody, who: str = Depends(require_auth), authorization: str = Header(default="")):
    """Operator only (the access code): make a login and return its one-time password."""
    if not _is_code(authorization):
        raise HTTPException(403, "only the access code can add users")
    name = body.username.strip().lower()
    if not re.fullmatch(r"[a-z0-9_-]{2,32}", name):
        raise HTTPException(400, "username: 2 to 32 letters, digits, - or _")
    if await asyncio.to_thread(users._get, name):
        raise HTTPException(409, f"{name} already exists")
    pw = users._new_pw()
    await asyncio.to_thread(db.ex, "INSERT INTO users (username, pw_hash, created_at) VALUES (?,?,?)",
                            (name, users._hash(pw), int(time.time())))
    return {"username": name, "password": pw}


@app.get("/api/health")
async def health():
    return {"ok": True, "providers": providers(), "time": int(time.time())}


# --------------------------------------------------------------------------- #
# Catalog + balances
# --------------------------------------------------------------------------- #
@app.get("/api/catalog")
async def catalog(_: str = Depends(require_auth)):
    def pack(m):
        return {p: {f"{plat}:{ot}": sid for (plat, ot), sid in d.items()} for p, d in m.items()}
    return {
        "providers": [{"id": p, "label": provider_label(p), "bot": provider_bot(p),
                       "dripfeed": bool(PROVIDERS[p].get("view_dripfeed"))} for p in providers()],
        "platforms": ENGAGEMENT_PLATFORMS,
        "services": pack(effective_service_map()),   # v2.5: score-switched defaults included
        # live catalog rate for each default service when the catalog is loaded; config otherwise
        "rates": {p: {f"{plat}:{ot}": (panels_mod.cached_rate(p, service_for(p, plat, ot)) or r) if service_for(p, plat, ot) else r
                      for (plat, ot), r in d.items()} for p, d in RATE_MAP.items()},
        # comments are their own panel service per platform + style; absent = this panel cannot comment there
        "comments": {p: {f"{plat}:{st}": {"service": sid, "rate": panels_mod.cached_rate(p, sid) or rate_for(p, plat, "comments")}
                         for (plat, st), sid in d.items()} for p, d in COMMENT_MAP.items()},
    }


@app.get("/api/panels")
async def panels(_: str = Depends(require_auth)):
    async def one(p):
        try:
            bal = await client_for(p).balance()
            return {"id": p, "label": provider_label(p), "bot": provider_bot(p),
                    "balance": float(bal.get("balance", 0) or 0),
                    "currency": bal.get("currency", "USD"), "ok": True}
        except Exception as e:  # noqa: BLE001
            return {"id": p, "label": provider_label(p), "bot": provider_bot(p),
                    "balance": None, "ok": False, "error": str(e)[:80]}
    return {"panels": await asyncio.gather(*[one(p) for p in providers()])}


# --------------------------------------------------------------------------- #
# Single sequenced order
# --------------------------------------------------------------------------- #
class OrderBody(BaseModel):
    provider: str
    platform: str
    link: str
    views: int
    likes: int = 0
    comments: int = 0
    style: str | None = None
    lines: str | None = None
    views_service: str | None = None
    likes_service: str | None = None
    force: bool = False


async def check_service(provider: str, service: str | None) -> str | None:
    """Validate a service override against the live panel catalog."""
    if not service:
        return None
    try:
        info = await panels_mod.service_info(provider, service)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"could not load {provider_label(provider)} catalog: {str(e)[:100]}")
    if not info:
        raise HTTPException(400, f"service {service} not in {provider_label(provider)} catalog")
    return str(service)


@app.post("/api/order")
async def place_order(body: OrderBody, who: str = Depends(require_auth)):
    if body.provider not in PROVIDERS:
        raise HTTPException(400, "unknown provider")
    vs = await check_service(body.provider, body.views_service)
    ls = await check_service(body.provider, body.likes_service)
    for sid in (vs, ls):
        if sid and not await campaigns.override_for(body.provider, sid, body.platform):
            raise HTTPException(400, f"service {sid} is not a {body.platform} service")
    if body.comments:
        if not body.style:
            raise HTTPException(400, "pick a comment style, or set comments to 0")
        if not comment_service(body.provider, body.platform, body.style):
            raise HTTPException(400, f"{provider_label(body.provider)} has no {body.style} comment service for {body.platform}")
        if body.style == "custom" and not (body.lines or "").strip():
            raise HTTPException(400, "custom comments need the comment lines")
    if not (vs or service_for(body.provider, body.platform, "views")):
        raise HTTPException(400, f"{provider_label(body.provider)} has no views service for {body.platform}")
    # warm the likes-min catalog lookup outside the lock
    likes_q = await engine.likes_qty(body.provider, ls or service_for(body.provider, body.platform, "likes"), body.likes)
    bad = await campaigns.qty_problem(body.provider, body.platform, body.views, likes_q, vs, ls)
    if bad:
        raise HTTPException(400, bad)
    try:
        async with campaigns._place_lock:   # same lock as campaign placement: dedupe is race-free
            dup = await asyncio.to_thread(campaigns.duplicate_reason, campaigns.url_key(body.link))
            if dup and not body.force:
                raise HTTPException(409, f"{dup}; send force:true to order anyway")
            res = await engine.place_order(
                body.provider, body.platform, body.link, body.views, body.likes,
                body.comments, body.style, body.lines, created_by=who,
                views_service=vs, likes_service=ls)
    except HTTPException:
        raise
    except PanelError as e:
        raise HTTPException(400, f"Panel rejected: {e}")
    except Exception as e:  # noqa: BLE001  v2.19: never auto-retried; the order may exist
        raise HTTPException(500, f"{e!s}"[:160] + f" ({engine.MAYBE})")
    await _audit(who, "order.place", body.link, {**res, "provider": body.provider, "force": body.force})
    return res


# --------------------------------------------------------------------------- #
# Fast likes (any panel, likes only, full amount at once)
# --------------------------------------------------------------------------- #
_URL = re.compile(r"(?:https?://|(?<![\w.@/])(?=(?:www\.|m\.)?(?:instagram\.com|tiktok\.com|facebook\.com|"
                  r"fb\.watch|youtube\.com|youtu\.be)/))[^\s,;\"'<>()\[\]{}|]+", re.I)


def extract_urls(text: str) -> list[str]:
    """Every URL in pasted text or a CSV dump, in order, once each (by post key).
    Bare instagram.com/... style links get https:// added."""
    out, seen = [], set()
    for m in _URL.finditer(text or ""):
        u = m.group(0).rstrip(".,;:!?)")
        if not u.lower().startswith("http"):
            u = "https://" + u
        k = campaigns.url_key(u)
        if k not in seen:
            seen.add(k); out.append(u)
    return out


def _qty(tok: str) -> int:
    tok = str(tok).strip().lower(); m = 1
    if tok.endswith("k"):
        m, tok = 1000, tok[:-1]
    try:
        return int(round(float(tok) * m))
    except ValueError:
        return -1


def _likes_range(s: str | None):
    """"300" or "300-500" (k suffix ok) -> (lo, hi), or None when empty."""
    if not (s or "").strip():
        return None
    parts = [_qty(x) for x in s.replace(" ", "").split("-")]
    if len(parts) > 2 or min(parts) < 1:
        raise HTTPException(400, "likes per post must be numbers, like 300 or 300-500")
    return (min(parts), max(parts))


class FastPreviewBody(BaseModel):
    provider: str = "justanotherpanel"
    mode: str = "fixed"               # fixed | account
    links: list[str]
    likes_range: str | None = None    # fixed mode: "300" or "300-500"
    likes_cap: int | None = None      # optional max likes per post
    likes_service: str | None = None  # applies to posts on that service's platform


async def _fast_service(provider: str, override: str | None, platform: str) -> str:
    return (await campaigns.override_for(provider, override, platform)) or service_for(provider, platform, "likes")


@app.post("/api/fastlikes/preview")
async def fast_preview(body: FastPreviewBody, _: str = Depends(require_auth)):
    if body.provider not in PROVIDERS:
        raise HTTPException(400, "unknown provider")
    if body.mode not in ("fixed", "account"):
        raise HTTPException(400, "mode must be fixed or account")
    ls = await check_service(body.provider, body.likes_service)
    urls = extract_urls("\n".join(body.links))
    if not urls:
        raise HTTPException(400, "no links found")
    lr = _likes_range(body.likes_range)
    if body.mode == "fixed" and not lr:
        raise HTTPException(400, "enter likes per post, like 300 or 300-500")
    cap = body.likes_cap if body.likes_cap and body.likes_cap > 0 else None
    label = provider_label(body.provider)
    rows = []
    for u in urls:
        p = detect_platform(u)
        r = {"url": u, "platform": p, "views": None, "like_rate": None, "owner": None, "likes": 0,
             "likes_service": None, "est_cost": 0.0, "duplicate": None, "problem": None}
        if p not in ENGAGEMENT_PLATFORMS:
            r["problem"] = "not an Instagram, TikTok, Facebook or YouTube link"
        else:
            r["likes_service"] = await _fast_service(body.provider, ls, p) or None
            if not r["likes_service"]:
                r["problem"] = f"{label} has no likes service for {p}"
        rows.append(r)
    if body.mode == "account":
        todo = [r for r in rows if not r["problem"]]
        got = await engine.fast_account_scrape([(r["url"], r["platform"]) for r in todo]) if todo else []
        for r, g in zip(todo, got):
            r.update(owner=g["owner"], like_rate=g["like_rate"], views=g["views"], problem=g["problem"])
    seen = set()
    for r in rows:
        key = campaigns.url_key(r["url"])
        r["duplicate"] = "duplicate in this list" if key in seen else \
            await asyncio.to_thread(campaigns.duplicate_reason, key)
        seen.add(key)
        if r["problem"]:
            continue
        if body.mode == "account":
            want = engine.fast_likes_for(r["views"], r["like_rate"], cap)
        else:
            want = random.randint(lr[0], lr[1])
            want = min(want, cap) if cap else want
        r["likes"] = await engine.likes_qty(body.provider, r["likes_service"], want)
        if r["likes"] <= 0:
            r["problem"] = "no likes to order"
            continue
        r["problem"] = await campaigns.qty_problem(body.provider, r["platform"], 0, r["likes"], None, r["likes_service"])
        rate = await campaigns.live_rate(body.provider, r["platform"], "likes", r["likes_service"])
        r["est_cost"] = round(r["likes"] / 1000 * rate, 4)
        if r["like_rate"] is not None:
            r["like_rate"] = round(r["like_rate"], 5)
    ok = [r for r in rows if not r["problem"] and not r["duplicate"]]
    return {"provider": body.provider, "mode": body.mode, "orders": rows, "count": len(ok),
            "total_likes": sum(r["likes"] for r in ok), "total_cost": round(sum(r["est_cost"] for r in ok), 4),
            "duplicates": sum(1 for r in rows if r["duplicate"] and not r["problem"]),
            "problems": sum(1 for r in rows if r["problem"])}


class FastPlaceBody(BaseModel):
    provider: str = "justanotherpanel"
    orders: list[dict]                # [{url, platform, likes, likes_service?, views?}]
    force: bool = False


@app.post("/api/fastlikes")
async def fast_place(body: FastPlaceBody, who: str = Depends(require_auth)):
    if body.provider not in PROVIDERS:
        raise HTTPException(400, "unknown provider")
    out = {"placed": [], "failed": [], "skipped": []}
    ready = []
    for o in body.orders:
        url = str(o.get("url") or "").strip()
        p = detect_platform(url)   # the link decides the platform, never the client
        if not url or p not in ENGAGEMENT_PLATFORMS:
            out["skipped"].append({"url": url, "reason": "not an Instagram, TikTok, Facebook or YouTube link"}); continue
        sid = await check_service(body.provider, o.get("likes_service"))
        if sid and not await campaigns.override_for(body.provider, sid, p):
            raise HTTPException(400, f"service {sid} is not a {p} service")
        svc = sid or service_for(body.provider, p, "likes")
        if not svc:
            out["skipped"].append({"url": url, "reason": f"{provider_label(body.provider)} has no likes service for {p}"}); continue
        likes = await engine.likes_qty(body.provider, svc, int(o.get("likes") or 0))
        bad = "no likes to order" if likes <= 0 else \
            await campaigns.qty_problem(body.provider, p, 0, likes, None, svc)
        if bad:
            out["skipped"].append({"url": url, "reason": bad}); continue
        ready.append({"url": url, "platform": p, "likes": likes, "likes_service": svc, "views": o.get("views")})
    if not ready and not out["skipped"]:
        raise HTTPException(400, "no orders")
    for o in ready:   # one at a time under the placement lock: dedupe is race-free
        async with campaigns._place_lock:
            dup = await asyncio.to_thread(campaigns.duplicate_reason, campaigns.url_key(o["url"]))
            if dup and not body.force:
                out["skipped"].append({"url": o["url"], "reason": dup})
                continue
            res = await engine.place_fastlikes(body.provider, [o], created_by=who)
        out["placed"] += res["placed"]; out["failed"] += res["failed"]
    await _audit(who, "fastlikes.place", body.provider, {k: len(v) for k, v in out.items()} | {"force": body.force})
    return out


# --------------------------------------------------------------------------- #
# Bulk engagement
# --------------------------------------------------------------------------- #
class BulkPreviewBody(BaseModel):
    provider: str
    mode: str                 # auto | budget | manual | mancap
    links: list[str]
    views_range: str | None = None   # "5k-25k"
    budget_total: int | None = None
    likes_range: str | None = None   # "30-50"; empty = account like rate
    views_only: bool = False
    views_service: str | None = None
    likes_service: str | None = None


def _range(s: str | None):
    if not s:
        return (SMART_VIEWS_MIN, SMART_VIEWS_MIN)
    parts = s.replace(" ", "").split("-")
    lo = _qty(parts[0]); hi = _qty(parts[1]) if len(parts) > 1 else lo
    lo = max(SMART_VIEWS_MIN, lo); hi = max(lo, hi)
    return (lo, hi)


@app.post("/api/bulk/preview")
async def bulk_preview(body: BulkPreviewBody, _: str = Depends(require_auth)):
    if body.provider not in PROVIDERS:
        raise HTTPException(400, "unknown provider")
    vs = await check_service(body.provider, body.views_service)
    ls = await check_service(body.provider, body.likes_service)
    links = []
    for u in body.links:
        u = u.strip()
        if not u:
            continue
        p = detect_platform(u)
        if p and (service_for(body.provider, p, "views")
                  or await campaigns.override_for(body.provider, vs, p)):
            links.append((u, p))
    if not links:
        raise HTTPException(400, "no supported links for this provider")

    lo, hi = _range(body.views_range)
    if body.mode == "manual":
        scraped = [{"url": u, "platform": p} for u, p in links]
    else:
        scraped = await engine.bulk_scrape(links)
        if not scraped:
            raise HTTPException(422, "could not read any account stats — try Manual range")

    built = engine.bulk_build(body.mode, scraped, views_lo=lo, views_hi=hi,
                              budget_total=body.budget_total or 0)
    out, total_v, total_cost, seen = [], 0, 0.0, set()
    lr = [_qty(x) for x in body.likes_range.replace(" ", "").split("-")] if (body.likes_range or "").strip() else None
    if lr and (len(lr) > 2 or min(lr) < 0):
        raise HTTPException(400, "likes per post must be numbers, like 30 or 30-50")
    for (url, platform, views, likes), m in zip(built["orders"], built["meta"]):
        if lr:
            likes = random.randint(min(lr[0], lr[-1]), max(lr[0], lr[-1]))
        if body.views_only:
            likes = 0
        pvs = await campaigns.override_for(body.provider, vs, platform)
        pls = await campaigns.override_for(body.provider, ls, platform)
        likes = await engine.likes_qty(body.provider, pls or service_for(body.provider, platform, "likes"), likes)
        cost = await campaigns.est_cost(body.provider, platform, views, likes, pvs, pls)
        total_v += views; total_cost += cost
        key = campaigns.url_key(url)
        dup = "duplicate in this list" if key in seen else \
            await asyncio.to_thread(campaigns.duplicate_reason, key)
        seen.add(key)
        out.append({"url": url, "platform": platform, "views": views, "likes": likes,
                    "owner": m.get("owner"), "anchor": m.get("anchor"),
                    "est_cost": round(cost, 4), "views_service": pvs, "likes_service": pls,
                    "duplicate": dup,
                    "problem": await campaigns.qty_problem(body.provider, platform, views, likes, pvs, pls)})
    return {"mode": body.mode, "provider": body.provider, "orders": out,
            "total_views": total_v, "total_likes": sum(o["likes"] for o in out), "total_cost": round(total_cost, 4),
            "leftover": built["leftover"], "count": len(out)}


class BulkPlaceBody(BaseModel):
    provider: str
    orders: list[dict]        # [{url, platform, views, likes, views_service?, likes_service?}]
    views_only: bool = False
    views_service: str | None = None
    likes_service: str | None = None
    force: bool = False


@app.post("/api/bulk/place")
async def bulk_place(body: BulkPlaceBody, who: str = Depends(require_auth)):
    if body.provider not in PROVIDERS:
        raise HTTPException(400, "unknown provider")
    orders = []
    for o in body.orders:
        p = o["platform"]
        vs = await check_service(body.provider, o.get("views_service")) or \
            await campaigns.override_for(body.provider, await check_service(body.provider, body.views_service), p)
        ls = await check_service(body.provider, o.get("likes_service")) or \
            await campaigns.override_for(body.provider, await check_service(body.provider, body.likes_service), p)
        if not (vs or service_for(body.provider, p, "views")):
            raise HTTPException(400, f"{provider_label(body.provider)} has no views service for {p}")
        likes = 0 if body.views_only else int(o.get("likes", 0))
        likes_q = await engine.likes_qty(body.provider, ls or service_for(body.provider, p, "likes"), likes)
        bad = await campaigns.qty_problem(body.provider, p, int(o["views"]), likes_q, vs, ls)
        orders.append((o["url"], p, int(o["views"]), likes, vs, ls, bad))
    out = {"placed": [], "failed": [], "skipped": []}
    for o in [o for o in orders if o[6]]:
        out["skipped"].append({"url": o[0], "reason": o[6]})
    orders = [o[:6] for o in orders if not o[6]]
    for o in orders:   # one at a time under the placement lock: dedupe is race-free
        async with campaigns._place_lock:
            dup = await asyncio.to_thread(campaigns.duplicate_reason, campaigns.url_key(o[0]))
            if dup and not body.force:
                out["skipped"].append({"url": o[0], "reason": dup})
                continue
            res = await engine.place_bulk(body.provider, [o], created_by=who)
        out["placed"] += res["placed"]; out["failed"] += res["failed"]
    await _audit(who, "bulk.place", body.provider, {k: len(v) for k, v in out.items()} | {"force": body.force})
    return out


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #
def _job_view(j: dict) -> dict:
    return {
        "id": j["id"], "kind": j.get("kind", "eng"), "provider": j["provider"],
        "platform": j["platform"], "link": j["link"], "state": j["state"],
        "views_target": j["views_target"], "last_views": j["last_views"] or 0,
        "likes_target": j["likes_target"], "last_likes": j["last_likes"] or 0,
        "likes_ordered": j["likes_ordered"] or 0, "hidden_likes": (j["last_likes"] or 0) < 0,
        "comments_target": j["comments_target"], "owner": j.get("owner_handle"),
        "created_at": j["created_at"], "likes_order_id": j.get("likes_order_id"),
        "views_service": j.get("views_service"), "views_status": j.get("views_status"),
        "views_remaining": j.get("views_remaining"), "completed_at": j.get("completed_at"),
        "minutes_to_complete": (round((j["completed_at"] - j["created_at"]) / 60, 1)
                                if j.get("completed_at") else None),
        "source": j.get("source") or "manual", "note": j.get("note"),
    }


PER_PAGE = 10


@app.get("/api/jobs")
async def jobs(provider: str = "all", state: str = "all", page: int = 1,
               kind: str = "eng", _: str = Depends(require_auth)):
    total = await asyncio.to_thread(db.count_jobs, kind, provider, state)
    pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = max(1, min(page, pages))
    rows = await asyncio.to_thread(db.list_jobs, kind, provider, state, PER_PAGE, (page - 1) * PER_PAGE)
    return {"jobs": [_job_view(r) for r in rows], "page": page, "pages": pages, "total": total}


@app.get("/api/fastjobs")
async def fastjobs(page: int = 1, _: str = Depends(require_auth)):
    total = await asyncio.to_thread(db.count_jobs, "fastlikes", None, None)
    pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = max(1, min(page, pages))
    rows = await asyncio.to_thread(db.list_jobs, "fastlikes", None, None, PER_PAGE, (page - 1) * PER_PAGE)
    statuses: dict = {}
    for prov in {r["provider"] for r in rows if r.get("likes_order_id")}:
        ids = [r["likes_order_id"] for r in rows if r.get("likes_order_id") and r["provider"] == prov]
        try:
            got = await client_for(prov).status(ids)
            if isinstance(got, dict):
                statuses.update({(prov, k): v for k, v in got.items()})
        except Exception as e:  # noqa: BLE001
            log.warning("fastjobs status lookup failed (%s): %s", prov, e)
    out = []
    for r in rows:
        v = _job_view(r)
        st = statuses.get((r["provider"], r.get("likes_order_id")), {})
        st = st if isinstance(st, dict) else {}
        v["order_status"] = st.get("status", "—")
        v["remaining"] = st.get("remaining", "?")
        out.append(v)
    return {"jobs": out, "page": page, "pages": pages, "total": total}


@app.get("/api/jobs/{job_id}")
async def job_detail(job_id: int, _: str = Depends(require_auth)):
    j = await asyncio.to_thread(db.get_job, job_id)
    if not j:
        raise HTTPException(404, "no such job")
    v = _job_view(j)
    v["snapshots"] = await asyncio.to_thread(db.snapshots_for_job, job_id)
    return v


# --------------------------------------------------------------------------- #
# v2: Lumina Clippers campaign links
# --------------------------------------------------------------------------- #
@app.get("/api/clippers/campaigns")
async def clippers_campaigns(_: str = Depends(require_auth)):
    try:
        rows = await clippers.campaigns()
    except clippers.ClippersError as e:
        raise HTTPException(502, str(e))
    linked = {r["campaign_id"] for r in await asyncio.to_thread(db.q, "SELECT campaign_id FROM links")}
    return {"campaigns": [{**c, "linked": c["id"] in linked} for c in rows]}


# --------------------------------------------------------------------------- #
# Source pickers (Plan A): list campaigns/programs for the automation modal's
# dropdown, and pull a chosen one's creators + links (the posts) for preview.
# Read-only; nothing is stored. Persisting links is Plan B (link-centric DB).
# --------------------------------------------------------------------------- #
@app.get("/api/sources/sideshift/programs")
async def sideshift_programs(_: str = Depends(require_auth)):
    import sources
    try:
        data = await sources._ss_get("/campaigns", {"limit": 100})
    except sources.SourceError as e:
        raise HTTPException(502, str(e))
    out = []
    for c in (data or []):
        pid = c.get("id") or c.get("programId")
        if not pid:
            continue
        st = c.get("stats") or {}
        out.append({"id": pid, "name": c.get("name") or "", "company": c.get("companyName") or "",
                    "status": c.get("status") or "",
                    "creators": st.get("activeCreators"),        # realistic "creators uploading"
                    "creators_total": st.get("totalCreators"),   # whole roster (incl. pending/inactive)
                    "posts": st.get("totalPosts"), "views": st.get("totalViews")})
    return {"programs": out}


async def _pull_source(source: str, ref: str, max_pages: int | None = None) -> tuple[list[dict], bool]:
    """tracker.pull_source with upstream errors mapped to HTTP. -> (links, capped)."""
    import sources
    import tracker
    try:
        return await tracker.pull_source(source, ref, max_pages or tracker.PREVIEW_PAGES)
    except ValueError as e:
        raise HTTPException(400, str(e) if "source" in str(e) else "clippers ref must be a campaign id")
    except sources.SourceError as e:
        raise HTTPException(502, str(e))
    except clippers.ClippersError as e:
        raise HTTPException(502, str(e))


@app.get("/api/sources/{source}/preview")
async def source_preview(source: str, ref: str = Query(...), _: str = Depends(require_auth)):
    """Pull a program/campaign's creators + links for the modal preview. Nothing stored."""
    source = source.lower().strip()
    links, capped = await _pull_source(source, ref)
    creators: dict[str, int] = {}
    plats: dict[str, int] = {}
    for l in links:
        c = l["creator"] or "?"
        creators[c] = creators.get(c, 0) + 1
        if l["platform"]:
            plats[l["platform"]] = plats.get(l["platform"], 0) + 1
    return {"source": source, "ref": ref, "capped": capped,
            "totals": {"links": len(links), "creators": len(creators), "platforms": sorted(plats)},
            "creators": [{"name": k, "links": v} for k, v in sorted(creators.items(), key=lambda x: -x[1])][:100],
            "links": links[:200]}


class ImportBody(BaseModel):
    ref: str
    name: str = ""
    company: str = ""
    views_goal: int | None = None


@app.post("/api/sources/{source}/import")
async def source_import(source: str, body: ImportBody, who: str = Depends(require_auth)):
    """Phase 0: Pull & persist. Upserts the campaign, its creators, its links and a first
    snapshot per link into the Postgres tracker store (links = the hub)."""
    import pg
    source = source.lower().strip()
    if not pg.available():
        raise HTTPException(503, "tracker store (Postgres) is not available")
    import tracker
    links, capped = await _pull_source(source, body.ref, tracker.FULL_PAGES)
    r = await pg.import_pull(source, body.ref, body.name, body.company, links, body.views_goal)
    await pg.mark_refreshed(r["campaign_id"], None)
    if source == "sideshift":   # Phase 1: daily history in the background, so charts start with real days
        tracker.spawn(tracker.backfill_history(r["campaign_id"]))
    await _audit(who, "tracker.import", body.ref, {"source": source, **r, "capped": capped})
    return {**r, "capped": capped}


@app.get("/api/tracker/campaigns")
async def tracker_campaigns(_: str = Depends(require_auth)):
    import pg
    if not pg.available():
        raise HTTPException(503, "tracker store (Postgres) is not available")
    return {"campaigns": await pg.list_campaigns()}


@app.get("/api/tracker/campaigns/{cid}/links")
async def tracker_links(cid: int, _: str = Depends(require_auth)):
    import pg
    if not pg.available():
        raise HTTPException(503, "tracker store (Postgres) is not available")
    return {"links": await pg.campaign_links(cid)}


@app.post("/api/tracker/campaigns/{cid}/refresh")
async def tracker_refresh(cid: int, who: str = Depends(require_auth)):
    """Phase 1: snapshot one tracked campaign now (full re-pull + history backfill)."""
    import pg
    import sources
    import tracker
    if not pg.available():
        raise HTTPException(503, "tracker store (Postgres) is not available")
    c = await pg.get_campaign(cid)
    if not c:
        raise HTTPException(404, "no such tracked campaign")
    try:
        r = await tracker.refresh_campaign(c)
    except (sources.SourceError, clippers.ClippersError) as e:
        raise HTTPException(502, str(e))
    await _audit(who, "tracker.refresh", cid, r)
    return r


@app.get("/api/tracker/links/{lid}/snapshots")
async def tracker_snapshots(lid: int, _: str = Depends(require_auth)):
    """Phase 1: one link's views/likes/comments/shares over time."""
    import pg
    if not pg.available():
        raise HTTPException(503, "tracker store (Postgres) is not available")
    return {"snapshots": await pg.link_snapshots(lid)}


@app.get("/api/tracker/status")
async def tracker_status(_: str = Depends(require_auth)):
    import pg
    import tracker
    import persist
    camps = await pg.list_campaigns() if pg.available() else []
    return {"store": pg.available(), "worker": tracker.state, "console_backup": persist.state,
            "campaigns": [{"id": c["id"], "name": c["name"], "links": c["link_count"],
                           "refreshed_at": c.get("refreshed_at"), "error": c.get("refresh_error")} for c in camps]}


class LinkBody(BaseModel):
    campaign_id: int
    campaign_name: str = ""
    provider: str
    mode: str = "approve"
    views: int
    likes: int = 0
    platforms: list[str] = ["instagram", "facebook"]
    views_service: str | None = None
    likes_service: str | None = None
    daily_cap_usd: float | None = None
    services: dict | None = None
    backfill: bool = False


def _norm_services(s) -> dict:
    """{platform: {views, likes}} with str ids or null; platforms with no ids dropped."""
    if not s:
        return {}
    if not isinstance(s, dict):
        raise HTTPException(400, "services must be {platform: {views, likes}}")
    out = {}
    for plat, d in s.items():
        if d is None:
            continue
        if not isinstance(d, dict) or set(d) - {"views", "likes"}:
            raise HTTPException(400, f"services.{plat} must be {{views, likes}}")
        e = {t: str(d[t]).strip() if d.get(t) not in (None, "") else None for t in ("views", "likes")}
        if any(e.values()):
            out[str(plat)] = e
    return out


async def _validate_link(f: dict, catalog: bool = True) -> None:
    """catalog=False skips the live catalog (no service field changed)."""
    if f["provider"] not in PROVIDERS:
        raise HTTPException(400, "unknown provider")
    if f["mode"] not in ("auto", "approve"):
        raise HTTPException(400, "mode must be auto or approve")
    if f["views"] <= 0 or f["likes"] < 0:
        raise HTTPException(400, "views must be > 0 and likes >= 0")
    if f["daily_cap_usd"] is not None and f["daily_cap_usd"] < 0:
        raise HTTPException(400, "daily_cap_usd must be >= 0")
    plats = f["platforms"]
    if not plats or any(p not in ENGAGEMENT_PLATFORMS for p in plats):
        raise HTTPException(400, f"platforms must be a non-empty subset of {ENGAGEMENT_PLATFORMS}")
    f["services"] = _norm_services(f.get("services"))
    if catalog:
        f["views_service"] = await check_service(f["provider"], f["views_service"])
        f["likes_service"] = await check_service(f["provider"], f["likes_service"])
        for d in f["services"].values():
            for sid in d.values():
                await check_service(f["provider"], sid)
    for p in plats:
        if service_for(f["provider"], p, "views") or (f["services"].get(p) or {}).get("views"):
            continue
        if not catalog and f["views_service"]:
            continue
        try:
            ok = await campaigns.override_for(f["provider"], f["views_service"], p)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(502, f"could not load {provider_label(f['provider'])} catalog: {str(e)[:100]}")
        if not ok:
            raise HTTPException(400, f"{provider_label(f['provider'])} has no views service for {p}")


@app.get("/api/links")
async def list_links(_: str = Depends(require_auth)):
    def go():
        return [campaigns.link_view(l) for l in db.q("SELECT * FROM links ORDER BY id DESC")]
    return {"links": await asyncio.to_thread(go)}


@app.post("/api/links")
async def create_link(body: LinkBody, _: str = Depends(require_auth)):
    f = body.model_dump()
    backfill = f.pop("backfill")
    await _validate_link(f)
    f["services"] = json.dumps(f["services"])
    existing = None
    if not backfill:   # snapshot BEFORE the link exists so nothing old can be boosted
        try:
            existing = await clippers.submissions(f["campaign_id"])
        except clippers.ClippersError as e:
            raise HTTPException(502, f"cannot snapshot existing submissions: {e}")
    f["platforms"] = json.dumps(f["platforms"])
    # Insert inactive so the poller cannot queue anything before the snapshot is recorded.
    lid = await asyncio.to_thread(db.insert, "links", **f, active=0, created_at=int(time.time()))
    link = await asyncio.to_thread(campaigns.get_link, lid)
    try:
        if existing is not None:
            await campaigns.record_existing(link, existing)
    except Exception:
        await asyncio.to_thread(db.ex, "DELETE FROM links WHERE id=?", (lid,))
        await asyncio.to_thread(db.ex, "DELETE FROM queue WHERE link_id=?", (lid,))
        raise
    await asyncio.to_thread(db.update, "links", lid, active=1,
                            auto_since=int(time.time()) if f["mode"] == "auto" else None)
    _bg(asyncio.create_task(_safe_poll(lid)))
    await _audit(_, "link.create", lid, body.model_dump())
    return await asyncio.to_thread(campaigns.link_view, await asyncio.to_thread(campaigns.get_link, lid))


_tasks: set = set()


def _bg(t: asyncio.Task) -> asyncio.Task:
    """Keep a reference so a background task is not garbage-collected mid-run."""
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)
    return t


async def _safe_poll(lid: int) -> None:
    try:
        await campaigns.poll_link(lid)
    except Exception as e:  # noqa: BLE001
        log.warning("initial poll link %s failed: %s", lid, e)


_LINK_EDITABLE = ("campaign_name", "provider", "mode", "views", "likes", "platforms",
                  "views_service", "likes_service", "daily_cap_usd", "active", "services")


@app.patch("/api/links/{link_id}")
async def patch_link(link_id: int, body: dict, _: str = Depends(require_auth)):
    cur = await asyncio.to_thread(campaigns.get_link, link_id)
    if not cur:
        raise HTTPException(404, "no such link")
    if "campaign_id" in body and body["campaign_id"] == cur["campaign_id"]:
        body = {k: v for k, v in body.items() if k != "campaign_id"}   # unchanged: ignore
    bad = [k for k in body if k not in _LINK_EDITABLE]
    if bad:
        raise HTTPException(400, f"not editable: {', '.join(bad)}")
    f = {k: cur[k] for k in _LINK_EDITABLE}
    f["platforms"] = json.loads(cur["platforms"] or "[]")
    f["services"] = campaigns.link_services(cur)
    f.update(body)
    if "services" in body:
        f["services"] = _norm_services(f["services"])
    catalog = any(k in body and f[k] != (json.loads(cur[k] or ("{}" if k == "services" else "[]"))
                                         if k in ("platforms", "services") else cur[k])
                  for k in ("provider", "platforms", "views_service", "likes_service", "services"))
    await _validate_link(f, catalog=catalog)
    f["platforms"] = json.dumps(f["platforms"])
    f["services"] = json.dumps(f["services"])
    f["active"] = 1 if f["active"] else 0
    upd = {k: f[k] for k in body}
    # Going auto (or resuming in auto) starts a fresh window: older pending rows stay manual.
    if f["mode"] == "auto" and f["active"] and (cur["mode"] != "auto" or not cur["active"]):
        upd["auto_since"] = int(time.time())
    if upd:
        await asyncio.to_thread(db.update, "links", link_id, **upd)
    await _audit(_, "link.patch", link_id, body)
    return await asyncio.to_thread(campaigns.link_view, await asyncio.to_thread(campaigns.get_link, link_id))


@app.delete("/api/links/{link_id}")
async def delete_link(link_id: int, _: str = Depends(require_auth)):
    def go():
        db.ex("UPDATE queue SET state='skipped', error='link deleted' "
              "WHERE link_id=? AND state IN ('pending','waiting_funds','not_sent')", (link_id,))
        return db.ex("DELETE FROM links WHERE id=?", (link_id,))[0]
    if not await asyncio.to_thread(go):
        raise HTTPException(404, "no such link")
    await _audit(_, "link.delete", link_id)
    return {"ok": True}


@app.post("/api/links/{link_id}/sync")
async def sync_link(link_id: int, _: str = Depends(require_auth)):
    if not await asyncio.to_thread(campaigns.get_link, link_id):
        raise HTTPException(404, "no such link")
    try:
        return {"new": await campaigns.poll_link(link_id)}
    except clippers.ClippersError as e:
        raise HTTPException(502, str(e))


# --------------------------------------------------------------------------- #
# v2: boost queue
# --------------------------------------------------------------------------- #
QUEUE_PER_PAGE = 25


@app.get("/api/queue")
async def queue(state: str = "all", link_id: int | None = None, page: int = 1,
                group: str | None = None, _: str = Depends(require_auth)):
    where, args = [], []
    if group:
        if group not in campaigns.GROUPS:
            raise HTTPException(400, "unknown group")
        where.append(campaigns.GROUPS[group])
    elif state != "all":
        if state not in campaigns.STATES:
            raise HTTPException(400, "unknown state")
        where.append("q.state = ?"); args.append(state)
    if link_id:
        where.append("q.link_id = ?"); args.append(link_id)
    w = f"WHERE {' AND '.join(where)}" if where else ""
    frm = "FROM queue q LEFT JOIN jobs j ON j.id = q.job_id"

    def go(page):
        total = db.q1(f"SELECT COUNT(*) n {frm} {w}", tuple(args))["n"]
        pages = max(1, (total + QUEUE_PER_PAGE - 1) // QUEUE_PER_PAGE)
        page = max(1, min(page, pages))
        rows = db.q(f"SELECT q.*, j.state AS job_state, {campaigns.BUYS} AS buys {frm} {w} "
                    "ORDER BY q.id DESC LIMIT ? OFFSET ?", (*args, QUEUE_PER_PAGE, (page - 1) * QUEUE_PER_PAGE))
        return {"items": [_qitem(r) for r in rows], "page": page, "pages": pages, "total": total,
                "groups": _groups(link_id)}
    return await asyncio.to_thread(go, page)


def _qitem(r: dict) -> dict:
    return {**campaigns.queue_view(r), "job_state": r.get("job_state"), "buys": r.get("buys") or 0,
            "sent": bool(r.get("buys"))}


def _groups(link_id: int | None) -> dict:
    g = "WHERE q.link_id = ?" if link_id else ""
    return dict(db.q1("SELECT " + ", ".join(f"COALESCE(SUM(CASE WHEN {c} THEN 1 ELSE 0 END), 0) \"{k}\""
                                            for k, c in campaigns.GROUPS.items()) +
                      f" FROM queue q LEFT JOIN jobs j ON j.id = q.job_id {g}", (link_id,) if link_id else ()))


class SearchBody(BaseModel):
    link_id: int
    terms: list[str] = []
    sent: str = "all"   # all | yes | no
    page: int = 1


@app.post("/api/queue/search")
async def queue_search(body: SearchBody, _: str = Depends(require_auth)):
    """v2.11 All submissions: every queue row of one link, any state. terms = links (match by url_key) and/or
    emails (case-insensitive exact); a single term also substring-matches url / email / clipper.
    The filter is built once in campaigns.search_filter so the v2.23 CSV export stays identical."""
    try:
        f = campaigns.search_filter(body.link_id, body.terms, body.sent)
    except ValueError as e:
        raise HTTPException(400, str(e))

    def go(page):
        total = db.q1(f"SELECT COUNT(*) n {f.base}{f.tw}{f.sw}", f.args)["n"]
        pages = max(1, (total + QUEUE_PER_PAGE - 1) // QUEUE_PER_PAGE)
        page = max(1, min(page, pages))
        rows = db.q(f"SELECT q.*, j.state AS job_state, {campaigns.BUYS} AS buys {f.base}{f.tw}{f.sw} "
                    "ORDER BY q.id DESC LIMIT ? OFFSET ?", (*f.args, QUEUE_PER_PAGE, (page - 1) * QUEUE_PER_PAGE))
        sc = db.q1(f"SELECT COUNT(*) a, COALESCE(SUM(CASE WHEN {campaigns.SENT} THEN 1 ELSE 0 END), 0) y {f.base}{f.tw}", f.args)
        missing = []
        if f.terms:
            hit = db.q(f"SELECT DISTINCT q.url_key k, LOWER(q.clipper_email) e {f.base}{f.tw}", f.args)
            fk, fe = {h["k"] for h in hit}, {h["e"] for h in hit}
            missing = [t for t in f.terms if not (f.sub and hit) and
                       not (t.lower() in fe if t.lower() in f.emails else f.tkeys[t] & fk)]
        return {"items": [_qitem(r) for r in rows], "page": page, "pages": pages, "total": total,
                "groups": _groups(body.link_id), "sent_counts": {"all": sc["a"], "yes": sc["y"], "no": sc["a"] - sc["y"]},
                "terms": len(f.terms), "missing": missing}
    return await asyncio.to_thread(go, body.page)


class IdsBody(BaseModel):
    ids: list[int]


APPROVE_BUDGET_SEC = 45.0
APPROVABLE = ("pending", "waiting_funds", "not_sent")


@app.post("/api/queue/approve")
async def queue_approve(body: IdsBody, _: str = Depends(require_auth)):
    """Bounded: after APPROVE_BUDGET_SEC an in-flight placement keeps running in the
    background (never cancelled mid-order) and the rest are reported not attempted."""
    placed, failed = [], []
    deadline = time.monotonic() + APPROVE_BUDGET_SEC
    for qid in body.ids:
        left = deadline - time.monotonic()
        if left <= 0:
            cur = await asyncio.to_thread(campaigns.get_item, qid)
            failed.append({"id": qid, "state": cur["state"] if cur else None,
                           "error": "not attempted: approve time limit reached, approve again"})
            continue
        t = asyncio.create_task(campaigns.place_item(qid, APPROVABLE))
        done, _p = await asyncio.wait({t}, timeout=left)
        if not done:
            _bg(t)
            failed.append({"id": qid, "state": "placing",
                           "error": "still placing in the background; refresh the queue"})
            continue
        r = t.result()
        if r is None:
            cur = await asyncio.to_thread(campaigns.get_item, qid)
            failed.append({"id": qid, "state": cur["state"] if cur else None,
                           "error": "not pending, waiting_funds or not_sent" if cur else "no such item"})
        elif r["state"] == "placed":
            placed.append(campaigns.queue_view(r))
        else:
            failed.append({"id": qid, "state": r["state"], "error": r["error"]})
    await _audit(_, "queue.approve", None, {"ids": body.ids, "placed": [x["id"] for x in placed],
                                            "failed": [x["id"] for x in failed]})
    return {"placed": placed, "failed": failed}


class RebuyBody(BaseModel):
    ids: list[int]
    views: int
    likes: int = 0


@app.post("/api/queue/rebuy")
async def queue_rebuy(body: RebuyBody, who: str = Depends(require_auth)):
    """v2.11 Buy again: a second (smaller) order on already-sent rows. Bounded like approve."""
    if body.views <= 0 or body.likes < 0:
        raise HTTPException(400, "views must be > 0 and likes >= 0")
    placed, failed = [], []
    deadline = time.monotonic() + APPROVE_BUDGET_SEC
    for qid in dict.fromkeys(body.ids):   # a repeated id never buys twice
        left = deadline - time.monotonic()
        if left <= 0:
            failed.append({"id": qid, "ok": False, "error": "not attempted: time limit reached, buy again"})
            continue
        t = asyncio.create_task(campaigns.rebuy_item(qid, body.views, body.likes, who))
        done, _p = await asyncio.wait({t}, timeout=left)
        if not done:
            _bg(t)
            failed.append({"id": qid, "ok": False, "error": "still placing in the background; refresh"})
            continue
        r = t.result()
        (placed if r["ok"] else failed).append(r)
    await _audit(who, "queue.rebuy", None, {"ids": body.ids, "views": body.views, "likes": body.likes,
                                            "placed": [{"id": x["id"], "rebuy_id": x["rebuy_id"], "job_id": x["job_id"]}
                                                       for x in placed],
                                            "failed": [{"id": x["id"], "error": x["error"]} for x in failed]})
    return {"placed": placed, "failed": failed}


@app.post("/api/queue/reject")
async def queue_reject(body: IdsBody, _: str = Depends(require_auth)):
    if body.ids:
        await asyncio.to_thread(
            db.ex, f"UPDATE queue SET state='rejected' WHERE id IN ({','.join('?' * len(body.ids))}) "
                   "AND state IN ('pending','waiting_funds','failed','not_sent')", tuple(body.ids))
    await _audit(_, "queue.reject", None, {"ids": body.ids})
    return {"ok": True}


@app.post("/api/queue/{qid}/retry")
async def queue_retry(qid: int, _: str = Depends(require_auth)):
    cur = await asyncio.to_thread(campaigns.get_item, qid)
    if not cur:
        raise HTTPException(404, "no such item")
    r = await campaigns.place_item(qid, ("failed", "waiting_funds"))
    if r is None:
        raise HTTPException(409, f"item is {cur['state']}; only failed or waiting_funds can be retried")
    await _audit(_, "queue.retry", qid, {"state": r["state"], "error": r["error"]})
    return campaigns.queue_view(r)


EXPORT_COLS = ("submission_id", "url", "platform", "clipper", "clipper_email", "state", "provider",
               "views", "likes", "buys", "sent", "job_state", "error", "submitted_at", "created_at",
               "placed_at", "campaign_id", "campaign_name")


@app.get("/api/queue/export")
async def queue_export(link_id: int, sent: str = "all", q: list[str] = Query(default=[]),
                       _: str = Depends(require_auth)):
    """v2.23: read-only CSV of the All-submissions rows for one link, under the same filter as
    POST /api/queue/search (q = repeatable search term; sent = all|yes|no). No panel calls, no DB
    writes. Streams a text/csv attachment whose rows and order match the All-submissions view."""
    try:
        rows = await asyncio.to_thread(campaigns.export_rows, link_id, q, sent)
    except ValueError as e:
        raise HTTPException(400, str(e))

    def cell(r, c):
        if c == "sent":
            return "yes" if r.get(c) else "no"
        v = r.get(c)
        return "" if v is None else v

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(EXPORT_COLS)
    for r in rows:
        w.writerow([cell(r, c) for c in EXPORT_COLS])
    return Response(content=buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="submissions-link-{link_id}.csv"'})


# --------------------------------------------------------------------------- #
# v2: live panel catalog, speed, speedtest
# --------------------------------------------------------------------------- #
@app.get("/api/panels/{provider}/services")
async def panel_services(provider: str, platform: str | None = None, type: str | None = None,
                         _: str = Depends(require_auth)):
    if provider not in PROVIDERS:
        raise HTTPException(404, "unknown provider")
    try:
        rows = await panels_mod.services_cached(provider)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"{provider_label(provider)} catalog: {str(e)[:120]}")
    out = [{"service": str(s["service"]), "name": s.get("name", ""), "rate": s.get("rate"),
            "min": s.get("min"), "max": s.get("max"), "category": s.get("category", "")}
           for s in rows if campaigns.svc_matches(s, platform, type)]
    default = service_for(provider, platform, type) if platform and type else ""
    return {"services": out, "default": default}


@app.get("/api/speed")
async def speed(provider: str | None = None, platform: str | None = None, type: str = "views",
                _: str = Depends(require_auth)):
    """v2.5 Panel service score. Scores are computed over every service (peers = same platform + kind),
    then filtered for the response. Also applies default switches (score.apply_switches)."""
    rows, _made = await score.refresh()
    keep = lambda r: ((not provider or provider == "all" or r["provider"] == provider)  # noqa: E731
                      and (not platform or r["platform"] == platform) and r["type"] == type)
    recs = [x for x in score.recommendations(rows) if keep(x)]   # peers come from every panel
    rows = [r for r in rows if keep(r)]
    for r in rows:
        r["is_default"] = service_for(r["provider"], r["platform"], r["type"]) == r["service"]
        name, rate = "", None
        try:   # cached panel catalog only (10 min); never a fresh panel call per row when warm
            info = await panels_mod.service_info(r["provider"], r["service"]) or {}
            name = info.get("name", "")
            rate = float(info["rate"]) if info.get("rate") is not None else None
        except Exception:  # noqa: BLE001
            pass
        r.update(name=name, avg_rate=rate)   # avg_rate = panel catalog price, USD per 1k
    return {"rows": rows, "formula": score.FORMULA, "min_orders": score.MIN_ORDERS, "poor_below": score.POOR,
            "recommendations": recs,
            "switched": [s for s in await asyncio.to_thread(score.switched) if keep(s)]}


class UndoDefaultBody(BaseModel):
    provider: str
    platform: str
    type: str = "views"


@app.post("/api/speed/defaults/undo")
async def speed_undo(body: UndoDefaultBody, _: str = Depends(require_auth)):
    if body.provider not in PROVIDERS:
        raise HTTPException(400, "unknown provider")
    default = await asyncio.to_thread(score.undo, body.provider, body.platform, body.type)
    await _audit(_, "speed.undo", f"{body.provider}:{body.platform}:{body.type}", {"default": default})
    return {"ok": True, "default": default}


class SpeedPair(BaseModel):
    service: str
    link: str


class SpeedtestBody(BaseModel):
    provider: str
    quantity: int
    pairs: list[SpeedPair]


@app.post("/api/speedtest")
async def speedtest(body: SpeedtestBody, _: str = Depends(require_auth)):
    if body.provider not in PROVIDERS:
        raise HTTPException(400, "unknown provider")
    if body.quantity <= 0 or not body.pairs:
        raise HTTPException(400, "quantity > 0 and at least one pair required")
    keys = [campaigns.url_key(p.link) for p in body.pairs]
    if len(set(keys)) != len(keys):
        raise HTTPException(400, "each service needs its own link (duplicate links)")
    for p in body.pairs:
        await check_service(body.provider, p.service)
        if not detect_platform(p.link):
            raise HTTPException(400, f"unsupported link: {p.link}")
    out = []
    for p in body.pairs:
        try:
            r = await engine.place_order(body.provider, detect_platform(p.link), p.link, body.quantity,
                                         0, created_by="speedtest", views_service=p.service)
            out.append({"service": p.service, "link": p.link, "order_id": r["views_order_id"],
                        "job_id": r["job_id"]})
        except Exception as e:  # noqa: BLE001
            out.append({"service": p.service, "link": p.link, "order_id": None, "job_id": None,
                        "error": engine._why(e)})
    return {"orders": out}


# --------------------------------------------------------------------------- #
# v2: alerts
# --------------------------------------------------------------------------- #
@app.get("/api/alerts")
async def alerts(_: str = Depends(require_auth)):
    now = int(time.time())
    out = []
    if panels_mod.DRY_RUN:
        out.append({"level": "warn", "text": "Dry run: orders are faked, no money is spent", "at": now})

    async def bal(p):
        try:
            return p, await panels_mod.balance_cached(p), None
        except Exception as e:  # noqa: BLE001
            return p, None, str(e)[:80]
    for p, b, err in await asyncio.gather(*[bal(p) for p in providers()]):
        if err:
            out.append({"level": "warn", "text": f"{provider_label(p)} balance unavailable: {err}", "at": now})
        elif b is not None and b < LOW_BALANCE_USD:
            out.append({"level": "crit" if b < 1 else "warn",
                        "text": f"{provider_label(p)} balance low: ${b:.2f}", "at": now})
    cs = clippers.status()
    if cs["error"]:
        out.append({"level": "crit", "text": cs["error"], "at": cs["at"]})
    for text, at in campaigns.panel_alerts(now - 3600):
        out.append({"level": "warn", "text": f"panel error: {text}", "at": at})
    out += automation.alerts(now - 86400)

    def counts():
        return (db.q1("SELECT COUNT(*) n FROM queue WHERE state='waiting_funds'")["n"],
                db.q1("SELECT COUNT(*) n, MAX(created_at) at FROM queue WHERE state='failed' AND created_at>=?",
                      (now - 86400,)),
                db.q("SELECT campaign_name, last_error, last_polled_at FROM links "
                     "WHERE active=1 AND last_error IS NOT NULL"))
    waiting, failed, link_errs = await asyncio.to_thread(counts)
    if waiting:
        out.append({"level": "warn", "text": f"{waiting} boost(s) waiting for funds or daily cap", "at": now})
    if failed["n"]:
        out.append({"level": "warn", "text": f"{failed['n']} boost(s) failed in the last 24h", "at": failed["at"]})
    for l in link_errs:
        out.append({"level": "warn", "text": f"{l['campaign_name']}: {l['last_error']}", "at": l["last_polled_at"]})
    return {"alerts": out}


# --------------------------------------------------------------------------- #
# v2.7: Automation (automation.py). Every POST/PATCH writes one audit_log row.
# --------------------------------------------------------------------------- #
AUTO_PER_PAGE = 25


def _bad(e: Exception):
    raise HTTPException(404 if isinstance(e, LookupError) else 400, str(e))


def _page(sql_from: str, args: tuple, page: int, order: str, per: int = AUTO_PER_PAGE) -> tuple[list[dict], int, int, int]:
    total = db.q1(f"SELECT COUNT(*) n {sql_from}", args)["n"]
    pages = max(1, (total + per - 1) // per)
    page = max(1, min(page, pages))
    return db.q(f"SELECT * {sql_from} ORDER BY {order} LIMIT ? OFFSET ?", (*args, per, (page - 1) * per)), page, pages, total


@app.get("/api/auto/overview")
async def auto_overview(_: str = Depends(require_auth)):
    st = await asyncio.to_thread(automation.get_settings)

    async def bal(p):
        try:
            return p, await panels_mod.balance_cached(p)
        except Exception:  # noqa: BLE001
            return p, None
    balances = dict(await asyncio.gather(*[bal(p) for p in providers()]))
    now = int(time.time())
    alerts_ = automation.alerts(now - 86400) + [
        {"level": "crit" if b < 1 else "warn", "text": f"{provider_label(p)} balance low: ${b:.2f}", "at": now}
        for p, b in balances.items() if b is not None and b < st["low_balance_usd"]]

    def go():
        return (automation.spent(automation.et_midnight(automation.et_day(now))),
                [automation.campaign_view(c, now) for c in db.q("SELECT * FROM auto_campaigns ORDER BY id DESC")],
                [automation.pause_view(p) for p in db.q("SELECT * FROM pauses ORDER BY id DESC")])
    spent, camps, pauses = await asyncio.to_thread(go)
    return {"enabled": automation.enabled(), "dry_run": panels_mod.DRY_RUN, "settings": st, "spent_today_usd": spent,
            "campaigns": camps, "pauses": pauses, "alerts": alerts_, "balances": balances}


@app.get("/api/auto/plan")
async def auto_plan(campaign_id: int | None = None, _: str = Depends(require_auth)):
    return {"rows": await asyncio.to_thread(automation.plan_view, campaign_id)}


class AutoPlanBody(BaseModel):
    rows: list[dict]


@app.post("/api/auto/plan")
async def auto_plan_add(body: AutoPlanBody, who: str = Depends(require_auth)):
    try:
        ins, dup = await asyncio.to_thread(automation.insert_plan, body.rows)
    except (ValueError, TypeError) as e:
        _bad(e)
    await _audit(who, "auto.plan.add", None, {"inserted": ins, "duplicates": dup,
                                              "campaigns": sorted({r.get("campaign_id") for r in body.rows}, key=str)})
    return {"inserted": ins, "duplicates": dup}


@app.get("/api/auto/orders")
async def auto_orders(campaign_id: int | None = None, creator_id: str | None = None, post: str | None = None,
                      state: str | None = None, kind: str | None = None, page: int = 1, since_id: int | None = None,
                      max_id: int | None = None, platform: str | None = None, _: str = Depends(require_auth)):
    where, args = [], []
    if since_id is not None:   # audits: fix max_id to the first page's top id so new orders cannot shift pages
        where.append("o.id > ?"); args.append(since_id)
    if max_id is not None:
        where.append("o.id <= ?"); args.append(max_id)
    for col, v in (("o.campaign_id", campaign_id), ("o.state", state), ("o.kind", kind), ("p.platform", platform),
                   ("o.url_key", campaigns.url_key(post) if post else None)):
        if v not in (None, "", "all"):
            where.append(f"{col} = ?"); args.append(v)
    if creator_id not in (None, "", "all"):   # sheet name or SideShift id: either finds the creator's orders
        w, a = await asyncio.to_thread(automation._creator_where, "p.creator_id", creator_id)
        where.append(w); args += list(a)
    frm = automation.ORDER_SELECT.split(" FROM ", 1)[1]

    def go():
        w = f"WHERE {' AND '.join(where)}" if where else ""
        tot = db.q1(f"SELECT COUNT(*) n, COALESCE(SUM(CASE WHEN {automation.LIVE_SPEND_O} THEN o.est_cost END), 0) s, "
                    f"COALESCE(SUM(CASE WHEN o.state='simulated' THEN o.est_cost END), 0) m FROM {frm} {w}", tuple(args))
        total = tot["n"]
        pages = max(1, (total + AUTO_PER_PAGE - 1) // AUTO_PER_PAGE)
        pg = max(1, min(page, pages))
        rows = db.q(f"{automation.ORDER_SELECT} {w} ORDER BY o.id DESC LIMIT ? OFFSET ?",
                    (*args, AUTO_PER_PAGE, (pg - 1) * AUTO_PER_PAGE))
        return {"items": [automation.order_view(r) for r in rows], "page": pg, "pages": pages, "total": total,
                "spent_usd": round(tot["s"], 2), "simulated_usd": round(tot["m"], 2)}   # v2.15: whole filtered set
    return await asyncio.to_thread(go)


@app.post("/api/auto/campaigns")
async def auto_campaign_add(body: dict, who: str = Depends(require_auth)):
    await automation.prep_comments(body)   # v2.18: Custom Comments tag + default rate 5
    try:
        f = automation.campaign_fields(body, create=True)
    except ValueError as e:
        _bad(e)
    if why := await automation.services_problem(body.get("services")):
        raise HTTPException(400, why)
    f.pop("mode", None)
    if "cap_usd" not in f:   # v2.12: cap default = estimate x 1.1, when the chosen services give an estimate
        try:
            est = await automation.estimate_for(f)
        except Exception:  # noqa: BLE001  bad dates / goal: no default cap (planner reports them later)
            est = None
        if est and est["total_usd"] > 0:
            f["cap_usd"] = est["cap_default_usd"]
    cid = await asyncio.to_thread(db.insert, "auto_campaigns", **f, mode="practice", created_at=int(time.time()))
    await _audit(who, "auto.campaign.create", cid, body)
    return await asyncio.to_thread(lambda: automation.campaign_view(automation._camp(cid)))


@app.get("/api/auto/services")
async def auto_services(platform: str, _: str = Depends(require_auth)):
    """v2.12 calculator dropdowns: views + likes services for one platform, recommended first."""
    if platform not in automation.PLATFORMS:
        raise HTTPException(400, f"platform must be one of {', '.join(automation.PLATFORMS)}")
    return await automation.service_options(platform)


@app.patch("/api/auto/campaigns/{cid}")
async def auto_campaign_patch(cid: int, body: dict, who: str = Depends(require_auth)):
    cur = await asyncio.to_thread(automation._camp, cid)
    if not cur:
        raise HTTPException(404, "no such campaign")
    await automation.prep_comments(body, cur)   # v2.18
    try:
        f = automation.campaign_fields(body, create=False, cur=cur)
    except ValueError as e:
        _bad(e)
    if why := await automation.services_problem(body.get("services")):
        raise HTTPException(400, why)
    cap = f.get("cap_usd", cur["cap_usd"])
    if f.get("mode") == "live" and cur["mode"] != "live" and cap is None:   # v2.12: Go Live needs a campaign cap
        raise HTTPException(400, "set a campaign cap before going live")
    if "cap_usd" in f and f["cap_usd"] is None and f.get("mode", cur["mode"]) == "live":
        raise HTTPException(400, "a live campaign needs a campaign cap; move it back to Practice first")
    if f:
        await asyncio.to_thread(db.update, "auto_campaigns", cid, **f)
    # v2.15 D7: frozen forecast at Go Live (first one wins); dates set later on a live campaign freeze then
    if f.get("mode", cur["mode"]) == "live" and (f.get("mode") == "live" and cur["mode"] != "live"
                                                 or "start_date" in f or "end_date" in f):
        try:
            await asyncio.to_thread(planner.freeze, cid)
        except Exception:  # noqa: BLE001  a compare copy must never block Go Live
            log.exception("forecast freeze failed for campaign %s", cid)
    await _audit(who, "auto.campaign.patch", cid, {**body, "mode_was": cur["mode"]} if "mode" in body else body)
    return await asyncio.to_thread(lambda: automation.campaign_view(automation._camp(cid)))


class AutoPauseBody(BaseModel):
    scope: str
    key: str | int | None = None
    cancel_running: bool = False


@app.post("/api/auto/pause")
async def auto_pause(body: AutoPauseBody, who: str = Depends(require_auth)):
    try:
        scope, key, label = automation.norm_pause(body.scope, body.key)
    except ValueError as e:
        _bad(e)

    def go():
        db.ex("INSERT OR IGNORE INTO pauses (scope, key, label, created_by, created_at) VALUES (?,?,?,?,?)",
              (scope, key, label, who, int(time.time())))
        return db.q1("SELECT * FROM pauses WHERE scope=? AND key=?", (scope, key))
    p = await asyncio.to_thread(go)
    await asyncio.to_thread(automation.drop_carry, scope, key)
    cancelled, refused = [], []
    if body.cancel_running:
        async with automation._lock:   # waits for a running tick, so orders it just placed are cancelled too
            cancelled, refused = await automation.cancel_orders(
                await asyncio.to_thread(automation.orders_in_scope, scope, key))
    await _audit(who, "auto.pause", f"{scope}:{label}", {"cancel_running": body.cancel_running,
                                                       "cancelled": cancelled, "cancel_refused": refused})
    return {"pause": automation.pause_view(p), "cancelled": cancelled, "cancel_refused": refused}


class AutoUnpauseBody(BaseModel):
    scope: str
    key: str | int | None = None


@app.post("/api/auto/unpause")
async def auto_unpause(body: AutoUnpauseBody, who: str = Depends(require_auth)):
    try:
        scope, key, label = automation.norm_pause(body.scope, body.key)
    except ValueError as e:
        _bad(e)
    n = (await asyncio.to_thread(db.ex, "DELETE FROM pauses WHERE scope=? AND key=?", (scope, key)))[0]
    await _audit(who, "auto.unpause", f"{scope}:{label}", {"removed": n})
    return {"ok": True}


@app.post("/api/auto/orders/{oid}/resend")
async def auto_resend(oid: int, who: str = Depends(require_auth)):
    try:
        r = await automation.resend(oid)
    except (LookupError, ValueError) as e:
        _bad(e)
    await _audit(who, "auto.order.resend", oid, {"new_order": r["id"], "state": r["state"], "error": r["error"]})
    return await asyncio.to_thread(automation.order_full, r["id"])


@app.post("/api/auto/orders/{oid}/cancel")
async def auto_cancel(oid: int, who: str = Depends(require_auth)):
    o = await asyncio.to_thread(automation.get_order, oid)
    if not o:
        raise HTTPException(404, "no such order")
    done, refused = await automation.cancel_orders([o])
    out = {"ok": bool(done), "reason": refused[0]["reason"] if refused else None}
    await _audit(who, "auto.order.cancel", oid, out)
    return out


@app.patch("/api/auto/settings")
async def auto_settings(body: dict, who: str = Depends(require_auth)):
    try:
        st = await asyncio.to_thread(automation.set_settings, body)
    except ValueError as e:
        _bad(e)
    await _audit(who, "auto.settings", None, body)
    return st


@app.get("/api/auto/audit")
async def auto_audit(page: int = 1, _: str = Depends(require_auth)):
    rows, pg, pages, total = await asyncio.to_thread(_page, "FROM audit_log", (), page, "id DESC", 50)
    return {"items": [{**r, "detail": json.loads(r["detail"]) if r["detail"] else None} for r in rows],
            "page": pg, "pages": pages, "total": total}


# --------------------------------------------------------------------------- #
# v2.8: Planner (planner.py, sources.py). Every POST writes one audit_log row.
# --------------------------------------------------------------------------- #
import planner  # noqa: E402

_campaign_fields_v27 = automation.campaign_fields


def _campaign_fields_v28(body: dict, create: bool, cur: dict | None = None) -> dict:
    """v2.8: check_times / slot_deadlines must be "HH:MM" (validated before the v2.7 checks, on create and PATCH)."""
    planner.validate_times(body)
    return _campaign_fields_v27(body, create, cur)


automation.campaign_fields = _campaign_fields_v28


@app.get("/api/auto/campaigns/{cid}/dashboard")
async def auto_dashboard(cid: int, _: str = Depends(require_auth)):
    try:
        return await asyncio.to_thread(planner.dashboard, cid)
    except LookupError as e:
        _bad(e)


async def _tracker_match(cid: int) -> tuple[dict, dict, list[dict]]:
    """An automation campaign -> (camp, base payload, tracked tracker campaigns), matched by (source, ref);
    several sources on one campaign are merged. base carries tracked/trackable/missing when nothing matches."""
    import pg
    import sources
    camp = await asyncio.to_thread(automation._camp, cid)
    if not camp:
        raise HTTPException(404, "no such campaign")
    try:
        refs = [(s, str(r)) for s, r in sources.refs(camp) if s in ("sideshift", "clippers")]
    except sources.SourceError:
        refs = []
    base = {"campaign_id": cid, "refs": [{"source": s, "ref": r} for s, r in refs]}
    if not refs:
        return camp, {**base, "tracked": False, "trackable": False}, []
    if not pg.available():
        raise HTTPException(503, "tracker store (Postgres) is not available")
    found = [(s, r, await pg.find_campaign(s, r)) for s, r in refs]
    tracked = [t for _, _, t in found if t]
    base["missing"] = [{"source": s, "ref": r} for s, r, t in found if not t]
    base["tracked"], base["trackable"] = bool(tracked), True
    base["tracker"] = [{"id": t["id"], "name": t["name"], "source": t["source"], "ref": t["source_ref"], "links": t["link_count"],
                        "refreshed_at": t.get("refreshed_at"), "error": t.get("refresh_error")} for t in tracked]
    return camp, base, tracked


@app.get("/api/auto/campaigns/{cid}/tracker")
async def auto_tracker(cid: int, _: str = Depends(require_auth)):
    """Phase 2: this automation campaign's live link analytics (Overview tab tracker panel)."""
    import analytics
    import pg
    camp, base, tracked = await _tracker_match(cid)
    if not tracked:
        return base
    ids = [t["id"] for t in tracked]
    links, snaps = await pg.campaign_rows(ids), await pg.campaign_snapshots(ids)
    return {**base, **analytics.overview(links, snaps, camp["views_goal"], camp["start_date"], camp["end_date"], automation.BUFFER_DAYS)}


STUCK_SEC = int(float(os.getenv("TRACKER_STUCK_H", "24")) * 3600)   # in flight longer than this = stuck
_ORDER_BUCKET = {"completed": "fulfilled", "simulated": "practice", "ambiguous": "stuck", "blocked": "blocked",
                 "failed": "failed", "cancelled": "failed", "retried": "failed"}


def _bucket(o: dict, now: int) -> str:
    st = o.get("state") or ""
    if st in ("placing", "placed"):
        return "stuck" if now - (o.get("created_at") or now) > STUCK_SEC else "in_progress"
    return _ORDER_BUCKET.get(st, st or "unknown")


@app.get("/api/auto/campaigns/{cid}/tracker/links/{lid}")
async def auto_tracker_link(cid: int, lid: int, _: str = Depends(require_auth)):
    """Phase 4 link detail: actual views over time (tracker) + the planner's target and observed views,
    every order placed for this post (bucketed fulfilled / in progress / stuck / failed / practice) and the
    comment lines posted on it. Joined to the console by its canonical post key (config.url_key)."""
    import pg
    from config import url_key as canon_key
    _, base, tracked = await _tracker_match(cid)
    link = await pg.get_link(lid) if tracked else None
    if not link or link["campaign_id"] not in {t["id"] for t in tracked}:
        raise HTTPException(404, "no such link on this campaign")
    ck = canon_key(link["url"])
    now = int(time.time())

    import comments_ai

    def console():
        post = db.q1("SELECT * FROM auto_posts WHERE campaign_id=? AND url_key=? ORDER BY id DESC LIMIT 1", (cid, ck))
        orders = db.q("SELECT id, day, kind, provider, service, qty, est_cost, state, attempt, order_id, error, is_test, "
                      "created_at, comments FROM auto_orders WHERE campaign_id=? AND url_key=? ORDER BY created_at DESC", (cid, ck))
        lines = db.q("SELECT text, at, order_id FROM comment_used WHERE campaign_id=? AND url_key=? ORDER BY at", (cid, ck))
        return post, orders, lines, comments_ai.post_view(cid, ck)
    post, orders, lines, ai = await asyncio.to_thread(console)
    for o in orders:
        o["bucket"] = _bucket(o, now)
        try:
            o["comments"] = json.loads(o["comments"]) if o.get("comments") else None
        except ValueError:
            o["comments"] = None
    summary: dict[str, dict] = {}
    for o in orders:
        b = summary.setdefault(o["bucket"], {"orders": 0, "qty": 0})
        b["orders"] += 1
        b["qty"] += o.get("qty") or 0
    snaps = await pg.link_snapshots(lid)
    last = snaps[-1] if snaps else {}
    return {"campaign_id": cid, "canon_key": ck,
            "link": {k: link.get(k) for k in ("id", "url", "platform", "creator_name", "title", "thumbnail", "uploaded_at")},
            "actual": {k: last.get(k) for k in ("views", "likes", "comments", "shares", "t")},
            "snapshots": snaps,
            "post": None if not post else {k: post.get(k) for k in ("target_total", "observed_views", "observed_likes", "status",
                                                                       "reason", "peak", "checked_at", "viral_state", "day")},
            "orders": orders, "order_summary": summary, "stuck_h": STUCK_SEC // 3600,
            "comments": [{"text": c["text"], "at": c["at"], "order_id": c["order_id"]} for c in lines], "ai": ai}


class BriefBody(BaseModel):
    brief: str = ""
    tone: str = ""


@app.get("/api/auto/campaigns/{cid}/brief")
async def auto_brief(cid: int, _: str = Depends(require_auth)):
    import comments_ai
    return await asyncio.to_thread(comments_ai.get_brief, cid)


@app.put("/api/auto/campaigns/{cid}/brief")
async def auto_brief_set(cid: int, body: BriefBody, who: str = Depends(require_auth)):
    """Phase 5: the campaign brief + tone the AI writes comments from."""
    import comments_ai
    if not await asyncio.to_thread(automation._camp, cid):
        raise HTTPException(404, "no such campaign")
    r = await asyncio.to_thread(comments_ai.set_brief, cid, body.brief, body.tone)
    await _audit(who, "auto.brief.set", cid, {"chars": len(r["brief"]), "tone": r["tone"]})
    return r


class SuggestBody(BaseModel):
    n: int = 10


@app.post("/api/auto/campaigns/{cid}/tracker/links/{lid}/suggest")
async def auto_link_suggest(cid: int, lid: int, body: SuggestBody, who: str = Depends(require_auth)):
    """Phase 5: transcribe (Instagram) or read the caption, then have Gemini write N candidate comments."""
    import comments_ai
    import pg
    _, base, tracked = await _tracker_match(cid)
    link = await pg.get_link(lid) if tracked else None
    if not link or link["campaign_id"] not in {t["id"] for t in tracked}:
        raise HTTPException(404, "no such link on this campaign")
    try:
        r = await comments_ai.suggest(cid, link["url"], link.get("platform") or "", link.get("title") or "", max(1, min(20, body.n)))
    except comments_ai.AIError as e:
        raise HTTPException(502, str(e))
    await _audit(who, "auto.comments.suggest", cid, {"link": lid, **r})
    return r


class DecideBody(BaseModel):
    action: str
    text: str | None = None


@app.post("/api/auto/campaigns/{cid}/suggestions/{sid}")
async def auto_suggestion_decide(cid: int, sid: int, body: DecideBody, who: str = Depends(require_auth)):
    """Phase 5: approve (optionally edited) / reject / restore one AI comment. Approved = this post's own line."""
    import comments_ai
    try:
        r = await asyncio.to_thread(comments_ai.decide, cid, sid, body.action, body.text)
    except LookupError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    await _audit(who, "auto.comments.decide", cid, {"id": sid, "action": body.action, "edited": body.text is not None})
    return r


@app.get("/api/auto/campaigns/{cid}/tracker/accounts")
async def auto_tracker_accounts(cid: int, _: str = Depends(require_auth)):
    """Phase 3: Creators grid — one card per account (creator x platform) with its type + video series."""
    import analytics
    import pg
    _, base, tracked = await _tracker_match(cid)
    if not tracked:
        return base
    return {**base, **analytics.accounts(await pg.campaign_rows([t["id"] for t in tracked]))}


@app.get("/api/auto/campaigns/{cid}/detail")
async def auto_detail(cid: int, _: str = Depends(require_auth)):
    """v2.15 dashboard tabs: per day, per creator, per video, per slot and spend aggregates. Reads only."""
    try:
        return await asyncio.to_thread(planner.detail, cid)
    except LookupError as e:
        _bad(e)


@app.get("/api/auto/campaigns/{cid}/creators")
async def auto_creators(cid: int, _: str = Depends(require_auth)):
    """v2.14 creator include list."""
    try:
        return await asyncio.to_thread(planner.creators, cid)
    except LookupError as e:
        _bad(e)


@app.post("/api/auto/campaigns/{cid}/creators")
async def auto_creators_set(cid: int, body: dict, who: str = Depends(require_auth)):
    """v2.14: bulk tick / un-tick. Not a SideShift cancel."""
    try:
        r = await asyncio.to_thread(planner.set_included, cid, body)
    except (LookupError, ValueError) as e:
        _bad(e)
    ch = r["changed"]
    await _audit(who, "auto.creators.set", cid, {"included": sorted(k for k, v in ch.items() if v),
                                                  "excluded": sorted(k for k, v in ch.items() if not v)})
    return {**await asyncio.to_thread(planner.creators, cid), "changed": ch}


def _need_camp(cid: int) -> None:
    if not automation._camp(cid):
        raise LookupError("no such campaign")


@app.get("/api/auto/campaigns/{cid}/comments")
async def auto_comments(cid: int, _: str = Depends(require_auth)):
    """v2.18 custom comment pool (one list per campaign, every platform)."""
    try:
        await asyncio.to_thread(_need_camp, cid)
    except LookupError as e:
        _bad(e)
    return await asyncio.to_thread(automation.pool_view, cid)


@app.post("/api/auto/campaigns/{cid}/comments")
async def auto_comments_set(cid: int, body: dict, who: str = Depends(require_auth)):
    """v2.18: {text, mode: add | replace, csv: bool}. One comment per line, trimmed, blanks + exact duplicates dropped."""
    try:
        await asyncio.to_thread(_need_camp, cid)
        if not isinstance(body.get("text"), str):
            raise ValueError("text must be a string, one comment per line")
        r = await asyncio.to_thread(automation.pool_set, cid, body["text"], body.get("mode") or "add", bool(body.get("csv")))
    except (LookupError, ValueError) as e:
        _bad(e)
    await _audit(who, "auto.comments.set", cid, {"mode": body.get("mode") or "add", "added": r["added"],
                                                  "removed": r["removed"], "count": r["count"]})
    return r


@app.delete("/api/auto/campaigns/{cid}/comments/{line_id}")
async def auto_comments_remove(cid: int, line_id: int, who: str = Depends(require_auth)):
    try:
        r = await asyncio.to_thread(automation.pool_remove, cid, line_id)
    except LookupError as e:
        _bad(e)
    await _audit(who, "auto.comments.remove", cid, {"line_id": line_id, "count": r["count"]})
    return r


@app.post("/api/auto/campaigns/{cid}/run")
async def auto_planner_run(cid: int, who: str = Depends(require_auth)):
    try:
        await asyncio.to_thread(lambda: planner.runnable(automation._camp(cid)))
    except (LookupError, ValueError) as e:
        _bad(e)
    if not automation.enabled():
        raise HTTPException(400, "automation is off")
    if cid in planner._running:
        raise HTTPException(409, "planner is already running for this campaign")
    planner.spawn(cid)
    await _audit(who, "auto.planner.run", cid)
    return {"started": True}


@app.post("/api/auto/campaigns/{cid}/reset")
async def auto_planner_reset(cid: int, who: str = Depends(require_auth)):
    if cid in planner._running:
        raise HTTPException(409, "planner is running for this campaign")
    planner._running.add(cid)   # no run can start while the reset deletes
    try:
        out = await asyncio.to_thread(planner.reset, cid)
    except (LookupError, ValueError) as e:
        _bad(e)
    finally:
        planner._running.discard(cid)
    await _audit(who, "auto.planner.reset", cid, out)
    return out


@app.get("/api/auto/viral")
async def auto_viral(campaign_id: int | None = None, _: str = Depends(require_auth)):
    return await asyncio.to_thread(planner.viral_lists, campaign_id)


class ViralBody(BaseModel):
    peak: float | None = None


@app.post("/api/auto/viral/{post_id}/approve")
async def auto_viral_approve(post_id: int, body: ViralBody | None = None, who: str = Depends(require_auth)):
    peak = body.peak if body else None
    try:
        p = await asyncio.to_thread(planner.approve, post_id, peak)
    except (LookupError, ValueError) as e:
        _bad(e)
    await _audit(who, "auto.viral.approve", post_id, {"peak": p["peak"], "typed": peak is not None})
    return p


@app.post("/api/auto/viral/{post_id}/skip")
async def auto_viral_skip(post_id: int, who: str = Depends(require_auth)):
    try:
        p = await asyncio.to_thread(planner.skip, post_id)
    except (LookupError, ValueError) as e:
        _bad(e)
    await _audit(who, "auto.viral.skip", post_id)
    return p


class HandpickBody(BaseModel):
    campaign_id: int
    post_url: str
    peak: float | None = None


@app.post("/api/auto/viral/handpick")
async def auto_viral_handpick(body: HandpickBody, who: str = Depends(require_auth)):
    post = await asyncio.to_thread(planner.find_post, body.campaign_id, body.post_url)
    if not post:
        raise HTTPException(404, "post is not in this campaign's posts")
    try:
        p = await asyncio.to_thread(planner.approve, post["id"], body.peak, None, True)
    except (LookupError, ValueError) as e:
        _bad(e)
    await _audit(who, "auto.viral.handpick", post["id"], {"campaign_id": body.campaign_id, "peak": p["peak"],
                                                          "typed": body.peak is not None})
    return p


@app.get("/api/auto/posts")
async def auto_posts(campaign_id: int | None = None, status: str | None = None, reason: str | None = None,
                     page: int = 1, _: str = Depends(require_auth)):
    where, args = [], []
    for col, v in (("campaign_id", campaign_id), ("status", status), ("reason", reason)):
        if v not in (None, "", "all"):
            where.append(f"{col} = ?"); args.append(v)
    frm = "FROM auto_posts" + (f" WHERE {' AND '.join(where)}" if where else "")

    def go():
        rows, pg, pages, total = _page(frm, tuple(args), page, "uploaded_at DESC, id DESC")
        pauses = automation._pause_set()
        return {"items": [planner.post_view(r, pauses) for r in rows], "page": pg, "pages": pages, "total": total}
    return await asyncio.to_thread(go)


@app.patch("/api/auto/planner/settings")
async def auto_planner_settings(body: dict, who: str = Depends(require_auth)):
    try:
        st = await asyncio.to_thread(planner.set_limits, body)
    except ValueError as e:
        _bad(e)
    await _audit(who, "auto.planner.settings", None, body)
    return st
