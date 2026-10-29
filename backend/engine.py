"""Engagement engine: order placement, bulk allocation, and the like-drip model.

Ported from the SMMGlow red-team bot so the console behaves identically:
  * views land first; likes drip toward ratio x views_so_far (Model A, organic-aware)
  * JAP view orders drip natively (runs/interval)
  * bulk sizing: account-aware (p95), budget split (3-tier), manual, manual+cap
  * hidden-like posts (IG -1) complete on views alone
"""
from __future__ import annotations

import asyncio
import logging
import random
import statistics
import time

import apify_client as apify
import db
from config import (
    DRIP_VIEW_INTERVAL_MIN, DRIP_VIEW_RUNS, LIKE_ORDER_MIN, POLL_INTERVAL_MIN,
    POLL_STEPS, SMART_ANCHOR_PCT, SMART_JITTER, SMART_LIKES_MIN, SMART_MIN_LIKE_RATE,
    SMART_SAMPLE, SMART_TIER3_STRETCH, SMART_VIEWS_MAX, SMART_VIEWS_MIN,
    comment_service, provider_view_dripfeed, service_for, url_key,
)
import httpx
import panels
from panels import client_for

log = logging.getLogger("engine")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def view_drip_kwargs(provider: str, quantity: int, svc_min: int = 0) -> dict:
    """runs/interval so a drip-capable provider (JAP) ramps views over time.
    Each run is at least the service's own minimum, so a run is never rejected."""
    if not provider_view_dripfeed(provider):
        return {}
    runs = max(1, min(DRIP_VIEW_RUNS, quantity // max(SMART_VIEWS_MIN, svc_min)))
    return {"runs": runs, "interval": DRIP_VIEW_INTERVAL_MIN} if runs > 1 else {}


async def drip_kwargs(provider: str, service: str | None, quantity: int) -> dict:
    """view_drip_kwargs with the service minimum read from the panel catalog."""
    if not provider_view_dripfeed(provider):
        return {}
    try:
        info = await panels.service_info(provider, service) if service else None
        lo = int(float((info or {}).get("min") or 0))
    except Exception:  # noqa: BLE001
        lo = 0
    return view_drip_kwargs(provider, quantity, lo)


def poll_interval_min(elapsed_sec: int) -> int:
    elapsed_min = elapsed_sec / 60
    for bound, interval in POLL_STEPS:
        if elapsed_min < bound:
            return interval
    return POLL_INTERVAL_MIN


async def likes_floor(provider: str, service: str | None) -> int:
    """Smallest likes order the panel accepts: max(LIKE_ORDER_MIN, catalog min)."""
    try:
        info = await panels.service_info(provider, service) if service else None
        return max(LIKE_ORDER_MIN, int(float((info or {}).get("min") or 0)))
    except Exception:  # noqa: BLE001  catalog unavailable: our own floor
        return LIKE_ORDER_MIN


async def likes_qty(provider: str, service: str | None, requested: int) -> int:
    """Likes to order: 0 when none asked or no likes service, else raised to the floor."""
    if requested <= 0 or not service:
        return 0
    return max(int(requested), await likes_floor(provider, service))


def clamp_views(v) -> int:
    return min(SMART_VIEWS_MAX, max(SMART_VIEWS_MIN, int(v)))


# --------------------------------------------------------------------------- #
# Budget-split distribution (3-tier waterfall)
# --------------------------------------------------------------------------- #
def _waterfall(alloc, caps, eligible, amount: float) -> float:
    active = [i for i in eligible if caps[i] - alloc[i] > 0.5]
    for _ in range(len(active) + 2):
        if amount <= 0.5 or not active:
            break
        roomsum = sum(caps[i] - alloc[i] for i in active)
        if roomsum <= 0:
            break
        placed, capped = 0.0, []
        for i in active:
            room = caps[i] - alloc[i]
            add = amount * room / roomsum
            if add >= room:
                placed += room; alloc[i] = caps[i]; capped.append(i)
            else:
                placed += add; alloc[i] += add
        amount -= placed
        if capped:
            active = [i for i in active if i not in capped]
        else:
            amount = 0.0
            break
    return max(0.0, amount)


def distribute(total: int, p95s: list[int], ceilings: list[int]):
    """Returns (allocations, leftover). Tier 1 fill to p95, Tier 2 headroom to
    ceiling, Tier 3 sprinkle remainder onto bottom-band posts."""
    n = len(p95s)
    p95s = [max(1, int(round(x))) for x in p95s]
    ceilings = [max(p95s[i], int(round(ceilings[i]))) for i in range(n)]
    alloc = [0.0] * n
    sum_p95 = sum(p95s)
    if total <= sum_p95:
        for i in range(n):
            alloc[i] = total * p95s[i] / sum_p95
        ial = [int(round(a)) for a in alloc]
        return ial, max(0, total - sum(ial))
    for i in range(n):
        alloc[i] = float(p95s[i])
    remaining = _waterfall(alloc, ceilings, list(range(n)), total - sum_p95)
    if remaining >= 1:
        med = statistics.median(alloc)
        caps3 = [max(alloc[i], min(med, SMART_TIER3_STRETCH * p95s[i])) for i in range(n)]
        eligible = [i for i in range(n) if alloc[i] <= med + 0.5 and caps3[i] - alloc[i] > 0.5]
        random.shuffle(eligible)
        for i in eligible:
            if remaining < 1:
                break
            give = min(caps3[i] - alloc[i], remaining)
            alloc[i] += give; remaining -= give
    ial = [int(round(a)) for a in alloc]
    return ial, max(0, total - sum(ial))


# --------------------------------------------------------------------------- #
# Single sequenced order
# --------------------------------------------------------------------------- #
async def place_order(provider, platform, link, views, likes=0, comments=0,
                      style=None, lines=None, created_by=None, source="manual",
                      views_service=None, likes_service=None) -> dict:
    views_service = views_service or service_for(provider, platform, "views")
    likes_service = likes_service or service_for(provider, platform, "likes")
    comments_svc = comment_service(provider, platform, style) if comments and style else ""
    if not views_service:
        raise ValueError(f"{provider} has no views service for {platform}")
    likes = await likes_qty(provider, likes_service, likes)
    res = await client_for(provider).add(views_service, link, views,
                                         **await drip_kwargs(provider, views_service, views))
    vo = str(res.get("order", "") or "").strip() if isinstance(res, dict) else ""
    if not vo:
        raise RuntimeError(f"bad panel response, no order id: {str(res)[:120]}")
    now = int(time.time())
    job_id = await asyncio.to_thread(
        db.create_job, kind="eng", provider=provider, platform=platform, link=link,
        views_target=views, likes_target=likes, comments_target=comments,
        comment_style=style, comment_lines=lines, views_service=views_service,
        likes_service=likes_service, comments_service=comments_svc,
        views_order_id=vo, state="running", created_at=now, next_poll_at=now,
        created_by=created_by, source=source)
    return {"job_id": job_id, "views_order_id": vo, "views": views, "likes": likes}


# --------------------------------------------------------------------------- #
# Fast likes (any panel, likes-only, immediate, full amount, no drip)
# --------------------------------------------------------------------------- #
def fast_likes_for(views: int, rate: float, cap: int | None = None) -> int:
    """Account-aware likes: the post's current views x the account's like rate
    (same SMART_MIN_LIKE_RATE floor bulk uses), then the optional per-post cap."""
    likes = round(max(0, int(views)) * max(SMART_MIN_LIKE_RATE, rate or 0))
    return min(likes, int(cap)) if cap else likes


async def fast_account_scrape(links) -> list[dict]:
    """links: list of (url, platform). Per post, in order:
    {url, platform, owner, like_rate, views, problem}. Never guesses: a post whose
    account or current views cannot be read gets a problem and like_rate/views None."""
    sem = asyncio.Semaphore(5)   # bound paid Apify runs on a big paste

    async def lim(coro):
        async with sem:
            return await coro
    owners = await asyncio.gather(*[lim(apify.post_owner(p, u)) for u, p in links],
                                  return_exceptions=True)
    owners = [o if isinstance(o, str) and o else None for o in owners]
    uniq = sorted({(p, o) for (u, p), o in zip(links, owners) if o})
    got = await asyncio.gather(
        *[lim(apify.profile_posts(p, o, SMART_SAMPLE, SMART_ANCHOR_PCT)) for (p, o) in uniq],
        return_exceptions=True)
    accts = {k: g for k, g in zip(uniq, got) if isinstance(g, dict)}

    async def one(u, p, owner):
        row = {"url": u, "platform": p, "owner": owner, "like_rate": None, "views": None, "problem": None}
        acct = accts.get((p, owner)) if owner else None
        if not owner:
            row["problem"] = "could not find the account for this post"
            return row
        if not acct or not acct.get("stats"):
            row["problem"] = f"could not read @{owner}'s recent posts"
            return row
        row["like_rate"] = acct["stats"]["like_rate"]
        views = (acct.get("views") or {}).get(url_key(u))
        if not views:   # target post not in the account scrape: read the post itself
            try:
                counts = await lim(apify.scrape(p, u))
            except Exception:  # noqa: BLE001
                counts = None
            views = (counts or {}).get("views") or None
        if views is None:
            row["problem"] = "could not read this post's current views"
        row["views"] = views
        return row

    return list(await asyncio.gather(*[one(u, p, o) for (u, p), o in zip(links, owners)]))


async def place_fastlikes(provider: str, orders, created_by=None) -> dict:
    """orders: list of {url, platform, likes, likes_service?, views?}. Likes fired
    immediately at full qty (raised to the service floor). No views order, no drip."""
    client = client_for(provider)
    now = int(time.time())
    placed, failed = [], []
    for o in orders:
        link, platform = o["url"], o["platform"]
        svc = o.get("likes_service") or service_for(provider, platform, "likes")
        if not svc:
            failed.append({"url": link, "error": f"{provider} has no {platform} likes service"}); continue
        likes = await likes_qty(provider, svc, int(o.get("likes") or 0))
        if likes <= 0:
            failed.append({"url": link, "error": "no likes to order"}); continue
        try:
            res = await client.add(svc, link, likes)
            lo = str(res.get("order", "") or "").strip() if isinstance(res, dict) else ""
            if not lo:
                failed.append({"url": link, "error": f"bad response: {str(res)[:120]}"}); continue
            job_id = await asyncio.to_thread(
                db.create_job, kind="fastlikes", provider=provider, platform=platform,
                link=link, views_target=int(o.get("views") or 0), likes_target=likes,
                likes_service=svc, likes_order_id=lo, likes_ordered=likes, state="done",
                created_at=now, next_poll_at=None, created_by=created_by)
            placed.append({"job_id": job_id, "url": link, "likes": likes, "order": lo})
        except Exception as e:  # noqa: BLE001
            failed.append({"url": link, "error": _why(e)})
    return {"placed": placed, "failed": failed}


# --------------------------------------------------------------------------- #
# Bulk sizing: scrape accounts, then allocate per mode
# --------------------------------------------------------------------------- #
async def bulk_scrape(links) -> list[dict]:
    """links: list of (url, platform). Returns per-post account stats."""
    owners = await asyncio.gather(*[apify.post_owner(p, u) for u, p in links],
                                  return_exceptions=True)
    owners = [o if isinstance(o, str) else None for o in owners]
    uniq = sorted({(p, o) for (u, p), o in zip(links, owners) if o})
    stat_results = await asyncio.gather(
        *[apify.profile_stats(p, o, SMART_SAMPLE, SMART_ANCHOR_PCT) for (p, o) in uniq],
        return_exceptions=True)
    stats = {key: s for key, s in zip(uniq, stat_results) if isinstance(s, dict)}
    fb: dict[str, dict] = {}
    for plat in {p for _, p in links}:
        good = [s for (pp, _), s in stats.items() if pp == plat]
        if good:
            fb[plat] = {"anchor": int(statistics.median([s["anchor"] for s in good])),
                        "ceiling": int(statistics.median([s["ceiling"] for s in good])),
                        "rate": statistics.median([s["like_rate"] for s in good]),
                        "peak": int(statistics.median([s["peak"] for s in good]))}
    scraped = []
    for (u, p), owner in zip(links, owners):
        s = stats.get((p, owner)); f = fb.get(p)
        anchor = s["anchor"] if s else (f["anchor"] if f else None)
        if anchor is None:
            continue
        ceiling = (s["ceiling"] if s else (f["ceiling"] if f else anchor)) or anchor
        rate = (s["like_rate"] if s else (f["rate"] if f else 0)) or 0
        peak = (s["peak"] if s else (f["peak"] if f else anchor)) or anchor
        scraped.append({"url": u, "platform": p, "owner": owner or "(fallback)",
                        "anchor": anchor, "ceiling": max(anchor, ceiling),
                        "rate": max(SMART_MIN_LIKE_RATE, rate), "peak": peak})
    return scraped


def bulk_build(mode: str, scraped: list[dict], views_lo=0, views_hi=0, budget_total=0):
    """Returns {orders, meta, leftover}. orders: list of (url,platform,views,likes)."""
    orders, meta, leftover = [], [], 0
    if mode == "auto":
        for s in scraped:
            views = clamp_views(round(s["anchor"] * (1 + random.uniform(-SMART_JITTER, SMART_JITTER))))
            likes = max(SMART_LIKES_MIN, round(views * s["rate"]))
            orders.append((s["url"], s["platform"], views, likes))
            meta.append({"owner": s["owner"], "anchor": s["anchor"]})
    elif mode == "budget":
        p95s = [max(1, s["anchor"]) for s in scraped]
        ceilings = [max(SMART_VIEWS_MIN, s["ceiling"]) for s in scraped]
        alloc, leftover = distribute(budget_total, p95s, ceilings)
        for s, v in zip(scraped, alloc):
            if v < SMART_VIEWS_MIN:
                continue
            views = clamp_views(v)
            likes = max(SMART_LIKES_MIN, round(views * s["rate"]))
            orders.append((s["url"], s["platform"], views, likes))
            meta.append({"owner": s["owner"], "anchor": s["anchor"], "ceiling": s["ceiling"]})
    elif mode == "mancap":
        for s in scraped:
            views = clamp_views(min(s["ceiling"], random.randint(views_lo, views_hi)))
            likes = max(SMART_LIKES_MIN, round(views * s["rate"]))
            orders.append((s["url"], s["platform"], views, likes))
            meta.append({"owner": s["owner"], "anchor": s["anchor"], "ceiling": s["ceiling"]})
    else:  # manual (no scrape needed; scraped carries only url/platform)
        for s in scraped:
            views = clamp_views(random.randint(views_lo, views_hi))
            likes = max(SMART_LIKES_MIN, round(views * SMART_MIN_LIKE_RATE))
            orders.append((s["url"], s["platform"], views, likes))
            meta.append({"owner": s.get("owner", "—"), "anchor": s.get("anchor", 0)})
    return {"orders": orders, "meta": meta, "leftover": leftover}


async def place_bulk(provider: str, orders, created_by=None) -> dict:
    """orders: list of (url, platform, views, likes[, views_service, likes_service])."""
    client = client_for(provider)
    now = int(time.time())
    placed, failed = [], []
    for link, platform, views, likes, *ovr in orders:
        vs_o, ls_o = (list(ovr) + [None, None])[:2]
        views_service = vs_o or service_for(provider, platform, "views")
        likes_service = (ls_o or service_for(provider, platform, "likes")) if likes else ""
        likes = await likes_qty(provider, likes_service, likes)
        try:
            res = await client.add(views_service, link, views,
                                   **await drip_kwargs(provider, views_service, views))
            vo = str(res.get("order", "")).strip()
            if not vo:
                failed.append({"url": link, "error": f"bad response: {res}"}); continue
            job_id = await asyncio.to_thread(
                db.create_job, kind="eng", provider=provider, platform=platform, link=link,
                views_target=views, likes_target=likes, views_service=views_service,
                likes_service=likes_service, views_order_id=vo, state="running",
                created_at=now, next_poll_at=now, created_by=created_by)
            placed.append({"job_id": job_id, "url": link, "views": views, "likes": likes, "order": vo})
        except Exception as e:  # noqa: BLE001
            failed.append({"url": link, "error": _why(e)})
    return {"placed": placed, "failed": failed}


# --------------------------------------------------------------------------- #
# Drip advance (Model A: organic-aware like top-up) — the scheduler's unit of work
# --------------------------------------------------------------------------- #
MAYBE = "maybe placed: check the panel before retrying"


def _why(e: Exception) -> str:
    """Error text for a failed add: a panel "no" is sure; anything else may have placed the order (v2.19)."""
    return str(e)[:120] if isinstance(e, panels.PanelError) else f"{e!s}"[:120] + f" ({MAYBE})"


def _add_note(job_id: int, text: str) -> None:
    """Append a warning to the job note (an earlier one is never overwritten)."""
    old = (db.get_job(job_id) or {}).get("note") or ""
    db.update_job(job_id, note=(f"{old} | {text}" if old else text)[-600:])


async def _order_likes(job: dict, qty: int) -> bool:
    try:
        r = await client_for(job["provider"]).add(job["likes_service"], job["link"], qty)
        await asyncio.to_thread(db.update_job, job["id"], likes_order_id=str(r.get("order", "")).strip())
        return True
    except (panels.PanelError, httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as e:
        # the panel said no, or the request never left: nothing placed, a later top-up may try again
        log.warning("likes top-up refused job %s: %s", job["id"], e)
        return False
    except Exception as e:  # noqa: BLE001  may be placed: count it, never re-send (v2.19)
        log.warning("likes top-up unsure job %s: %s", job["id"], e)
        await asyncio.to_thread(_add_note, job["id"], f"likes top-up of {qty}: {_why(e)}")
        return True


async def _order_comments(job: dict) -> None:
    extra = {}
    if job.get("comment_style") == "custom" and job.get("comment_lines"):
        extra["comments"] = job["comment_lines"]
    try:
        r = await client_for(job["provider"]).add(job["comments_service"], job["link"],
                                                  job["comments_target"], **extra)
        await asyncio.to_thread(db.update_job, job["id"], comments_order_id=str(r.get("order", "")).strip())
    except Exception as e:  # noqa: BLE001
        log.warning("comments fire failed job %s: %s", job["id"], e)
        await asyncio.to_thread(_add_note, job["id"], f"comments order: {_why(e)}")


async def advance_job(job: dict) -> None:
    jid = job["id"]; now = int(time.time())
    try:
        counts = await apify.scrape(job["platform"], job["link"])
    except Exception as e:  # noqa: BLE001
        counts = None
        log.warning("scrape failed job %s: %s", jid, e)
    if counts:
        await asyncio.to_thread(db.add_snapshot, jid, now, counts["views"], counts["likes"], counts["comments"])
        await asyncio.to_thread(db.update_job, jid, last_views=counts["views"],
                                last_likes=counts["likes"], last_comments=counts["comments"])
    views = counts["views"] if counts else (job["last_views"] or 0)
    likes = counts["likes"] if counts else (job["last_likes"] or 0)
    hidden = likes == -1

    vt = job["views_target"] or 0
    lt = job["likes_target"] or 0
    # Delivery comes from the panel's own order status, not the post's total views:
    # a post that already had 5,000 views must not count as done for a 100-view order.
    vstat = (job.get("views_status") or "").lower()
    views_done = vstat in ("completed", "partial", "canceled", "cancelled", "refunded")
    remaining = job.get("views_remaining")
    delivered = vt if vstat == "completed" else (max(0, vt - remaining) if remaining is not None else 0)
    ordered = job["likes_ordered"] or 0
    baseline = job["baseline_likes"]
    if baseline is None and counts is not None and likes >= 0:
        baseline = likes
        await asyncio.to_thread(db.update_job, jid, baseline_likes=baseline)

    def like_gap() -> int:
        if (hidden or lt <= 0 or vt <= 0 or not job["likes_service"]
                or ordered >= lt or baseline is None):
            return 0
        desired = round((lt / vt) * delivered)
        effective = max(likes, baseline + ordered)
        return min(desired - effective, lt - ordered)

    gap = like_gap()
    floor = LIKE_ORDER_MIN   # catalog min only looked up when a top-up could fire
    if gap >= floor:
        floor = await likes_floor(job["provider"], job["likes_service"])
    if gap >= floor and await _order_likes(job, gap):
        first = ordered == 0
        ordered += gap
        await asyncio.to_thread(db.update_job, jid, likes_ordered=ordered)
        if first and job["comments_target"] and job["comments_service"]:
            await _order_comments(job)
        gap = like_gap()

    buying_done = lt == 0 or not job["likes_service"] or ordered >= lt or gap < floor
    likes_landed = (hidden or lt == 0 or not job["likes_service"] or ordered == 0
                    or likes >= (baseline or 0) + round(0.9 * ordered))
    if views_done and buying_done and not likes_landed and job.get("likes_order_id"):
        # The panel finished or cancelled the likes order: waiting longer adds nothing.
        try:
            st = (await client_for(job["provider"]).status(job["likes_order_id"])) or {}
            st = st.get(str(job["likes_order_id"]), st)
            likes_landed = str(st.get("status", "")).lower() in (
                "completed", "partial", "canceled", "cancelled", "refunded")
        except Exception as e:  # noqa: BLE001
            log.warning("likes status failed job %s: %s", jid, e)
    if views_done and buying_done and likes_landed:
        await asyncio.to_thread(db.update_job, jid, state="done", next_poll_at=None)
        return
    if now - (job["created_at"] or now) > 48 * 3600:
        await asyncio.to_thread(db.update_job, jid, state="done", next_poll_at=None)
        return
    interval = poll_interval_min(now - (job["created_at"] or now))
    await asyncio.to_thread(db.update_job, jid, next_poll_at=now + interval * 60)
