"""Minimal async Apify scraper for the engagement orchestrator.

Given a post URL + platform, runs the same actors the main app uses and returns
{views, likes, comments}. Used by the 15-min scheduler to track a post's real
on-platform counts. Token from APIFY_TOKEN.
"""
from __future__ import annotations

import asyncio
import os
import re
import statistics

import httpx

APIFY_BASE = "https://api.apify.com/v2"

PLATFORM_ACTORS = {
    "instagram": "apify~instagram-reel-scraper",
    "tiktok": "clockworks~tiktok-scraper",
    "facebook": "apify~facebook-posts-scraper",
    "youtube": "streamers~youtube-scraper",
}

_IG_SHORTCODE = re.compile(r"instagram\.com/(?:reel|p|tv)/([A-Za-z0-9_-]+)", re.IGNORECASE)


class ApifyError(Exception):
    pass


def _to_ig_reel(url: str) -> str:
    m = _IG_SHORTCODE.search(url)
    return f"https://www.instagram.com/reel/{m.group(1)}/" if m else url


def _build_input(platform: str, url: str) -> dict:
    if platform == "tiktok":
        return {
            "postURLs": [url], "resultsPerPage": 1, "excludePinnedPosts": False,
            "proxyCountryCode": "None", "scrapeRelatedVideos": False,
            "shouldDownloadAvatars": False, "shouldDownloadCovers": False,
            "shouldDownloadMusicCovers": False, "shouldDownloadSlideshowImages": False,
            "shouldDownloadVideos": False,
        }
    if platform == "instagram":
        return {"username": [_to_ig_reel(url)], "resultsLimit": 1,
                "dataDetailLevel": "basicData", "skipPinnedPosts": False}
    if platform == "facebook":
        return {"startUrls": [{"url": url}], "resultsLimit": 1}
    if platform == "youtube":
        return {"startUrls": [{"url": url}], "maxResults": 1,
                "maxResultsShorts": 1, "maxResultStreams": 0}
    return {}


def _nn(v) -> int:
    try:
        n = int(v or 0)
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def _extract(platform: str, item: dict) -> dict:
    if platform == "tiktok":
        return {"views": _nn(item.get("playCount")), "likes": _nn(item.get("diggCount")),
                "comments": _nn(item.get("commentCount"))}
    if platform == "instagram":
        raw_likes = item.get("likesCount")
        # -1 is IG's sentinel for "like count hidden on this post" — preserve it
        # so the caller can tell "hidden" apart from a genuine 0.
        likes = -1 if raw_likes == -1 else _nn(raw_likes)
        return {"views": _nn(item.get("videoPlayCount")) or _nn(item.get("videoViewCount")),
                "likes": likes, "comments": _nn(item.get("commentsCount"))}
    if platform == "facebook":
        fb = (item.get("video_view_count_renderer") or {}).get("feedback") or {}
        rl = item.get("likes")
        rl = rl.get("count") if isinstance(rl, dict) else rl
        return {"views": _nn(fb.get("play_count")) or _nn(fb.get("video_view_count")),
                "likes": _nn(rl), "comments": _nn(item.get("total_comment_count"))}
    if platform == "youtube":
        return {"views": _nn(item.get("viewCount")), "likes": _nn(item.get("likes")),
                "comments": _nn(item.get("commentsCount"))}
    return {"views": 0, "likes": 0, "comments": 0}


def _owner(item: dict) -> str | None:
    return (item.get("ownerUsername") or item.get("ownerUserName")
            or (item.get("owner") or {}).get("username"))


async def _run_actor(actor: str, inp: dict, timeout_polls: int = 90) -> list:
    """Run any Apify actor with an input, poll to completion, return its dataset."""
    token = os.getenv("APIFY_TOKEN", "")
    if not token:
        raise ApifyError("missing APIFY_TOKEN")
    params = {"token": token}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{APIFY_BASE}/acts/{actor}/runs", params=params, json=inp)
        if r.status_code not in (200, 201):
            raise ApifyError(f"start HTTP {r.status_code}: {r.text[:160]}")
        run_id = r.json()["data"]["id"]
        for _ in range(timeout_polls):
            await asyncio.sleep(6)
            s = await c.get(f"{APIFY_BASE}/actor-runs/{run_id}", params=params)
            if s.status_code != 200:
                continue
            st = s.json()["data"]["status"]
            if st == "SUCCEEDED":
                break
            if st in ("FAILED", "ABORTED", "TIMED-OUT"):
                raise ApifyError(f"run {st}")
        else:
            raise ApifyError("scrape timed out")
        ds = await c.get(f"{APIFY_BASE}/actor-runs/{run_id}/dataset/items", params=params)
        return ds.json() if ds.status_code == 200 else []


async def _run_ig(inp: dict, timeout_polls: int = 60) -> list:
    return await _run_actor(PLATFORM_ACTORS["instagram"], inp, timeout_polls)


_TT_HANDLE = re.compile(r"tiktok\.com/@([A-Za-z0-9_.\-]+)", re.IGNORECASE)


async def post_owner(platform: str, url: str) -> str | None:
    """Return the account/page/channel that owns a post, or None. Per-platform."""
    if platform == "instagram":
        items = await _run_actor(PLATFORM_ACTORS["instagram"],
                                 {"username": [_to_ig_reel(url)], "resultsLimit": 1,
                                  "dataDetailLevel": "basicData", "skipPinnedPosts": False})
        return _owner(items[0]) if items else None
    if platform == "tiktok":
        m = _TT_HANDLE.search(url)      # handle is in the URL for most TikTok links
        if m:
            return m.group(1)
        items = await _run_actor(PLATFORM_ACTORS["tiktok"], _build_input("tiktok", url))
        it = items[0] if items else {}
        return (it.get("authorMeta") or {}).get("name") or it.get("author")
    if platform == "youtube":
        items = await _run_actor(PLATFORM_ACTORS["youtube"], _build_input("youtube", url))
        it = items[0] if items else {}
        return (it.get("channelUrl") or it.get("channelUsername")
                or it.get("channelId") or it.get("channelName"))
    if platform == "facebook":
        items = await _run_actor(PLATFORM_ACTORS["facebook"], _build_input("facebook", url))
        it = items[0] if items else {}
        pid = it.get("pageName") or it.get("facebookId")
        return str(pid) if pid else None
    return None


async def reel_owner(url: str) -> str | None:   # back-compat IG alias
    return await post_owner("instagram", url)


def _percentile(sorted_asc: list[int], q: float) -> int:
    if not sorted_asc:
        return 0
    i = min(len(sorted_asc) - 1, max(0, round(q * (len(sorted_asc) - 1))))
    return sorted_asc[i]


def _stats_from(rows: list[tuple], anchor_pct: float) -> dict | None:
    """Compute the account profile from (views, likes[, url]) rows of its recent posts.

    Returns {anchor, ceiling, like_rate, median, peak, n}, or None if unusable.
    - anchor  = `anchor_pct` percentile of views (p95 = "a top post"), robust to
                a single freak-viral.
    - ceiling = mean of the top-5 posts EXCLUDING the single max (a genuine strong
                post; the per-post cap for budget-split). Always >= anchor.
    - like_rate = median like/view ratio among the TOP-decile posts.
    """
    rows = [(int(r[0]), int(r[1])) for r in rows if r[0] and int(r[0]) > 0]
    if not rows:
        return None
    views = sorted(v for v, _ in rows)
    ratios = [l / v for v, l in rows]
    anchor = _percentile(views, anchor_pct)
    p90 = _percentile(views, 0.90)
    top = [l / v for v, l in rows if v >= p90] or ratios
    desc = sorted(views, reverse=True)
    strong = desc[1:6] if len(desc) > 5 else (desc[1:] or desc)
    ceiling = max(anchor, int(statistics.mean(strong)))
    return {"anchor": anchor, "ceiling": ceiling,
            "like_rate": float(statistics.median(top)),
            "median": int(statistics.median(views)), "peak": views[-1], "n": len(views)}


async def _rows_instagram(handle: str, limit: int) -> list[tuple[int, int]]:
    items = await _run_actor(PLATFORM_ACTORS["instagram"],
                             {"username": [f"https://www.instagram.com/{handle}/"],
                              "resultsLimit": limit, "dataDetailLevel": "basicData",
                              "skipPinnedPosts": True})
    return [(_nn(p.get("videoPlayCount")) or _nn(p.get("videoViewCount")),
             _nn(p.get("likesCount")),
             p.get("url") or (f"https://www.instagram.com/p/{p['shortCode']}/" if p.get("shortCode") else ""))
            for p in items]


async def _rows_tiktok(handle: str, limit: int) -> list[tuple[int, int]]:
    items = await _run_actor(PLATFORM_ACTORS["tiktok"],
                             {"profiles": [handle], "resultsPerPage": limit,
                              "shouldDownloadVideos": False, "shouldDownloadCovers": False,
                              "shouldDownloadAvatars": False, "shouldDownloadMusicCovers": False,
                              "shouldDownloadSlideshowImages": False})
    return [(_nn(p.get("playCount")), _nn(p.get("diggCount")), p.get("webVideoUrl") or "") for p in items]


async def _rows_youtube(handle: str, limit: int) -> list[tuple[int, int]]:
    if handle.startswith("http"):
        url = handle.rstrip("/") + "/videos"
    elif handle.startswith("@"):
        url = f"https://www.youtube.com/{handle}/videos"
    else:
        url = f"https://www.youtube.com/channel/{handle}/videos"
    items = await _run_actor(PLATFORM_ACTORS["youtube"],
                             {"startUrls": [{"url": url}], "maxResults": limit,
                              "maxResultsShorts": limit, "maxResultStreams": 0})
    return [(_nn(p.get("viewCount")), _nn(p.get("likes")), p.get("url") or "") for p in items]


async def _rows_facebook(handle: str, limit: int) -> list[tuple[int, int]]:
    url = handle if handle.startswith("http") else f"https://www.facebook.com/{handle}"
    items = await _run_actor(PLATFORM_ACTORS["facebook"],
                             {"startUrls": [{"url": url}], "resultsLimit": limit})
    rows: list[tuple[int, int]] = []
    for p in items:
        fb = (p.get("video_view_count_renderer") or {}).get("feedback") or {}
        v = _nn(fb.get("play_count")) or _nn(fb.get("video_view_count"))
        rl = p.get("likes")
        rl = rl.get("count") if isinstance(rl, dict) else rl
        if v > 0:
            rows.append((v, _nn(rl), p.get("url") or p.get("topLevelUrl") or ""))
    return rows


_ROW_FETCHERS = {"instagram": _rows_instagram, "tiktok": _rows_tiktok,
                 "youtube": _rows_youtube, "facebook": _rows_facebook}


async def profile_posts(platform: str, handle: str, limit: int = 120,
                        anchor_pct: float = 0.95) -> dict | None:
    """One account scrape -> {stats: profile or None, views: {url_key: current views}}, or None on failure."""
    from config import url_key   # lazy: keeps this module importable alone
    fetch = _ROW_FETCHERS.get(platform)
    if not fetch or not handle:
        return None
    try:
        rows = await fetch(handle, limit)
    except ApifyError:
        return None
    views = {url_key(r[2]): int(r[0]) for r in rows if len(r) > 2 and r[2] and r[0]}
    return {"stats": _stats_from(rows, anchor_pct), "views": views}


async def profile_stats(platform: str, handle: str, limit: int = 120,
                        anchor_pct: float = 0.95) -> dict | None:
    """Account performance profile for any supported platform, or None on failure."""
    got = await profile_posts(platform, handle, limit, anchor_pct)
    return got["stats"] if got else None


async def scrape(platform: str, url: str, timeout_polls: int = 40) -> dict | None:
    if os.getenv("PANEL_DRY_RUN") == "1":   # dry run: no paid Apify scrapes
        return None
    """Run the actor and return {views,likes,comments}, or None on failure."""
    actor = PLATFORM_ACTORS.get(platform)
    token = os.getenv("APIFY_TOKEN", "")
    if not actor or not token:
        raise ApifyError(f"no actor for {platform} or missing APIFY_TOKEN")
    params = {"token": token}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{APIFY_BASE}/acts/{actor}/runs", params=params,
                         json=_build_input(platform, url))
        if r.status_code not in (200, 201):
            raise ApifyError(f"start HTTP {r.status_code}: {r.text[:160]}")
        run_id = r.json()["data"]["id"]
        for _ in range(timeout_polls):
            await asyncio.sleep(6)
            s = await c.get(f"{APIFY_BASE}/actor-runs/{run_id}", params=params)
            if s.status_code != 200:
                continue
            st = s.json()["data"]["status"]
            if st == "SUCCEEDED":
                break
            if st in ("FAILED", "ABORTED", "TIMED-OUT"):
                raise ApifyError(f"run {st}")
        else:
            raise ApifyError("scrape timed out")
        ds = await c.get(f"{APIFY_BASE}/actor-runs/{run_id}/dataset/items", params=params)
        items = ds.json() if ds.status_code == 200 else []
    if not items:
        return None
    out = _extract(platform, items[0])
    if platform == "facebook" and not out["views"]:
        out["views"] = await fb_play_count(items[0])
    return out


FB_PLAYS_ACTOR = "social_developer~facebook-playcount-scraper"


async def fb_play_count(item: dict) -> int:
    """facebook-posts-scraper gives no view count for reels; this actor reads play_count. 0 on failure."""
    vid = (item.get("video") or {}).get("id") or (((item.get("short_form_video_context") or {}).get("video")) or {}).get("id")
    if not vid:
        return 0
    try:
        rows = await _run_actor(FB_PLAYS_ACTOR, {"startUrls": [{"url": f"https://www.facebook.com/watch/?v={vid}"}]}, 40)
    except ApifyError:
        return 0
    return _nn(rows[0].get("play_count")) if rows else 0
