"""Phase 1: link-tracker snapshot worker.

Every TRACKER_SNAPSHOT_MIN minutes (default 60) each tracked campaign is re-pulled in
full from its source and re-imported: every link gets a fresh snapshot, and new posts
join the tracker on their own. SideShift links are also backfilled once from
metrics-history (daily points), so charts start with real history, not one dot.

All counts are free: SideShift's own analytics and the Clippers app's own scrape.
Nothing here calls Apify or any panel.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

import clippers
import pg
import sources

log = logging.getLogger("tracker")

INTERVAL_MIN = max(5, int(os.getenv("TRACKER_SNAPSHOT_MIN", "60")))
FULL_PAGES = 50          # 100 posts a page: up to 5,000 posts per campaign per sweep
PREVIEW_PAGES = 3        # the modal's Pull preview stays snappy

state = {"interval_min": INTERVAL_MIN, "running": False, "runs": 0,
         "last_run": None, "next_run": None, "last_error": None}
_task = None


async def pull_source(source: str, ref: str, max_pages: int = PREVIEW_PAGES) -> tuple[list[dict], bool]:
    """A program's / campaign's posts as link dicts -> (links, capped).
    ValueError = bad input; sources.SourceError / clippers.ClippersError = upstream failure."""
    links: list[dict] = []
    capped = False
    if source == "sideshift":
        for page in range(1, max_pages + 1):
            d = await sources._ss_get("/analytics/videos", {"programId": ref, "limit": 100, "page": page})
            got = d.get("data") or []
            for it in got:
                if sources.ss_deleted(it):
                    continue
                url = it.get("postPage") or ""
                if not url:
                    continue
                links.append({"url": url, "platform": sources.ss_platform(it) or "",
                              "creator": it.get("contractorName") or it.get("creator") or "",
                              "views": it.get("views"), "likes": it.get("likes"),
                              "comments": it.get("comments"), "shares": it.get("shares"),
                              "ext_post_id": str(it.get("postId") or it.get("id") or "") or None,
                              "title": it.get("title") or "",
                              "uploaded_at": it.get("uploadedAt"), "thumbnail": it.get("thumbnail") or ""})
            tp = d.get("totalPages")
            if not got or (tp is not None and page >= int(tp)):
                break
            if page == max_pages:
                capped = tp is None or int(tp) > max_pages
    elif source == "clippers":
        cid = int(ref)
        page = 1
        while True:
            d = await clippers._get(f"/api/campaigns/{cid}/submissions", {"page": page, "per_page": 100})
            for s in d.get("items", []):
                url = s.get("post_url") or ""
                if not url:
                    continue
                plat = (s.get("platform") or "").lower()
                ok = s.get("scrape_status") == "success"   # only trust a successful Clippers scrape
                links.append({"url": url, "platform": "x" if plat == "twitter" else plat,
                              "creator": s.get("clipper_name") or s.get("clipper_email") or "",
                              "views": s.get("views") if ok else None, "likes": s.get("likes") if ok else None,
                              "comments": s.get("comments") if ok else None, "shares": None,
                              "ext_post_id": str(s["id"]) if s.get("id") is not None else None, "title": "",
                              "uploaded_at": sources._epoch(s.get("posted_at")) or sources._epoch(s.get("created_at")),
                              "thumbnail": clippers.abs_url(s.get("thumbnail_url")) if s.get("thumbnail_url") else ""})
            if page >= int(d.get("pages") or 1):
                break
            if page >= max_pages:
                capped = True
                break
            page += 1
    else:
        raise ValueError("source must be 'sideshift' or 'clippers'")
    return links, capped


async def backfill_history(cid: int) -> int:
    """SideShift only: daily points from metrics-history for links not backfilled yet. -> points added.
    A 429 stops the pass; the rest are picked up on the next sweep."""
    added = 0
    for l in await pg.links_needing_history(cid):
        try:
            h = await sources.ss_history(l["ext_post_id"])   # process-wide 10/s limiter
        except sources.RateLimited:
            break
        except Exception as e:  # noqa: BLE001
            log.info("history for link %s failed: %s", l["id"], e)
            await pg.mark_history(l["id"])   # don't retry a broken post every hour
            continue
        added += await pg.add_history(l["id"], h)
        await pg.mark_history(l["id"])
    return added


async def refresh_campaign(c: dict, backfill: bool = True) -> dict:
    """Full re-pull + re-import of one tracked campaign (+ history backfill)."""
    try:
        links, capped = await pull_source(c["source"], c["source_ref"], FULL_PAGES)
        r = await pg.import_pull(c["source"], c["source_ref"], c["name"], c["company"], links)
        h = await backfill_history(c["id"]) if backfill and c["source"] == "sideshift" else 0
    except Exception as e:  # noqa: BLE001
        await pg.mark_refreshed(c["id"], str(e)[:300])
        raise
    await pg.mark_refreshed(c["id"], None)
    return {**r, "capped": capped, "history_points": h}


async def sweep() -> None:
    if state["running"]:
        return
    state["running"] = True
    try:
        for c in await pg.list_campaigns():
            try:
                r = await refresh_campaign(c)
                log.info("tracker: %s refreshed (%s links, %s history points)", c["name"], r["links"], r["history_points"])
            except Exception as e:  # noqa: BLE001
                log.warning("tracker: %s failed: %s", c["name"], e)
        state["last_run"], state["last_error"] = int(time.time()), None
        state["runs"] += 1
    except Exception as e:  # noqa: BLE001
        state["last_error"] = str(e)[:300]
    finally:
        state["running"] = False


async def run_loop() -> None:
    await asyncio.sleep(30)   # let boot settle
    while True:
        if pg.available():
            await sweep()
        state["next_run"] = int(time.time()) + INTERVAL_MIN * 60
        await asyncio.sleep(INTERVAL_MIN * 60)


def start() -> None:
    global _task
    _task = asyncio.create_task(run_loop())


_bg: set = set()


def spawn(coro) -> None:
    """Fire-and-forget that is not garbage-collected mid-run."""
    t = asyncio.create_task(coro)
    _bg.add(t)
    t.add_done_callback(_bg.discard)
