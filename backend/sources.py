"""v2.8 Planner inputs: post links (sheet / Clippers / SideShift) and post counts.

Every source returns the same link dicts:
  {url, platform, creator_id, account_handle, uploaded_at (epoch), day ("YYYY-MM-DD" ET), slot, source}
`get_counts` = the paid count path (Clippers admin API, then Apify). SideShift posts get free counts from the
program sweep (`ss_items`) + `ss_history` instead (planner.sideshift_counts).
"""
from __future__ import annotations

import asyncio
import csv
import datetime as dt
import io
import json
import logging
import os
import re
import time
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx

import apify_client
import clippers
import db
import config
from config import detect_platform, url_key

ET = ZoneInfo("America/New_York")
log = logging.getLogger("sources")
DEFAULT_TAB = "Daily Posts"
SLOT_RX = re.compile(r"^(IG|FB|TT|YT)\s+(AM|PM)\s+\d+$", re.I)
SLOT_PLATFORM = {"IG": "instagram", "FB": "facebook", "TT": "tiktok", "YT": "youtube"}
URL_RX = re.compile(r"https?://[^\s,;\"']+", re.I)
DELETED = "deleted"
NOT_CANCELLED = ("", "no", "-", "n/a", "na", "none")   # 'Cancelled / account issues' cells that mean nothing
HOSTS = ("instagram.com", "facebook.com", "fb.watch", "fb.com", "tiktok.com", "youtube.com", "youtu.be")
# Apify item `error` text that means the platform says the post is gone (anything else = unreadable).
GONE_WORDS = ("not found", "not_found", "no longer available", "deleted", "removed", "does not exist",
              "doesn't exist", "404")


class SourceError(Exception):
    pass


# --------------------------------------------------------------------------- #
# source_ref
# --------------------------------------------------------------------------- #
def refs(camp: dict) -> list[tuple[str, str]]:
    """-> [(source, ref)]. source_ref is the ref for `source`, or a JSON list
    [{"source": "sheet"|"clippers"|"sideshift", "ref": "..."}] for more than one source."""
    raw = (camp.get("source_ref") or "").strip()
    if raw.startswith("["):
        try:
            return [(str(x["source"]), str(x["ref"])) for x in json.loads(raw)]
        except (ValueError, KeyError, TypeError) as e:
            raise SourceError(f"bad source_ref JSON: {e}") from e
    if not camp.get("source") or not raw:
        raise SourceError("campaign has no source / source_ref")
    return [(camp["source"], raw)]


def sheet_ref(ref: str) -> tuple[str, str]:
    """'<sheet id>' or '<sheet id>:<tab>' or a sheet URL -> (sheet_id, tab). Tab defaults to 'Daily Posts'."""
    m = re.search(r"/d/([A-Za-z0-9_-]+)", ref)
    if m:
        return m.group(1), DEFAULT_TAB
    sid, _, tab = ref.partition(":")
    return sid.strip(), tab.strip() or DEFAULT_TAB


# --------------------------------------------------------------------------- #
# Google sheet (XTO "Daily Posts": one row per creator per ET date)
# --------------------------------------------------------------------------- #
def parse_day(s: str, today: dt.date) -> dt.date | None:
    """'Mon 19 Oct' / '19 Oct' / 'Oct 19' / 'Mon Oct 19' / 'October 19' (year inferred: closest to today),
    or ISO / M/D/YYYY."""
    s = (s or "").strip()
    for f in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return dt.datetime.strptime(s, f).date()
        except ValueError:
            pass
    s2 = re.sub(r"^(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*\.?,?\s+", "", s, flags=re.I)   # drop the weekday
    for f in ("%d %b", "%d %B", "%b %d", "%B %d"):
        try:
            d = dt.datetime.strptime(f"{s2} 2000", f"{f} %Y").date()   # 2000: leap year, so 29 Feb parses
        except ValueError:
            continue
        cands = []
        for y in (today.year - 1, today.year, today.year + 1):
            try:
                cands.append(d.replace(year=y))
            except ValueError:
                pass
        return min(cands, key=lambda c: abs((c - today).days)) if cands else None
    return None


def slot_ts(day: str, slot: str, deadlines: dict | None = None) -> int:
    """Slot deadline (epoch). AM default 12:00 ET, PM (and slots without AM/PM) 23:59 ET."""
    dl = {"AM": "12:00", "PM": "23:59", **(deadlines or {})}
    half = "AM" if re.search(r"\bAM\b", slot, re.I) else "PM"
    h, m = (int(x) for x in dl[half].split(":"))
    return int(dt.datetime.combine(dt.date.fromisoformat(day), dt.time(h, m), ET).timestamp())


def parse_sheet(text: str, today: dt.date, deadlines: dict | None = None) -> dict:
    """CSV -> {links:[link], slots:[{creator_id, day, slot, url|None}], cancelled:{creator: bool}}.
    A slot counts as filled only when its cell has a URL. Cancelled = 'Cancelled / account issues'
    non-empty on the creator's latest dated row."""
    rows = list(csv.reader(io.StringIO(text)))
    hi = next((i for i, r in enumerate(rows) if any(c.strip().lower() == "creator" for c in r)), None)
    if hi is None:
        raise SourceError("sheet has no 'Creator' header")
    head = [c.strip() for c in rows[hi]]
    col = {h.lower(): i for i, h in enumerate(head)}
    ci = col["creator"]
    di = next((i for i, h in enumerate(head) if h.lower().startswith("date")), None)
    xi = next((i for i, h in enumerate(head) if h.lower().startswith("cancelled")), None)
    if di is None:
        raise SourceError("sheet has no 'Date' header")
    slot_cols = [(i, h) for i, h in enumerate(head) if SLOT_RX.match(h)]
    tt = col.get("tiktok")
    links, slots, latest = [], [], {}
    for r in rows[hi + 1:]:
        cell = lambda i: r[i].strip() if i is not None and i < len(r) else ""  # noqa: E731
        creator, d = cell(ci), parse_day(cell(di), today)
        if not creator or not d:
            continue
        day = d.isoformat()
        if day >= latest.get(creator, ("", False))[0]:
            latest[creator] = (day, cell(xi).lower() not in NOT_CANCELLED)
        for i, h in slot_cols + ([(tt, "TikTok")] if tt is not None else []):
            urls = URL_RX.findall(cell(i))
            if i != tt:
                slots.append({"creator_id": creator, "day": day, "slot": h, "url": urls[0] if urls else None})
            for u in urls:
                plat = detect_platform(u) or SLOT_PLATFORM.get(h.split()[0].upper(), "tiktok")
                links.append({"url": u, "platform": plat, "creator_id": creator, "account_handle": creator,
                              "uploaded_at": slot_ts(day, h, deadlines), "day": day, "slot": h, "source": "sheet"})
    return {"links": links, "slots": slots, "cancelled": {k: v[1] for k, v in latest.items()}}


async def fetch_sheet(ref: str) -> str:
    sid, tab = sheet_ref(ref)
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
        r = await c.get(f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq",
                        params={"tqx": "out:csv", "sheet": tab})
    if r.status_code != 200 or "text/csv" not in r.headers.get("content-type", ""):
        raise SourceError(f"sheet {sid}/{tab}: HTTP {r.status_code} (is it shared 'anyone with the link'?)")
    return r.text


VIEWS_TAB = "Views"


def _num(s: str) -> int:
    """'1.547' (Dutch: dot = thousands) / '1,547' / '1547' -> 1547. Blank or anything else -> 0."""
    s = re.sub(r"[\s.,]", "", s or "")
    return int(s) if s.isdigit() else 0


def parse_views(text: str) -> list[dict]:
    """The sheet's "Views" tab (views / likes bought outside the console) -> [{url, views, likes}].
    Views count only when the Status column right after "Views to buy" is Done; likes likewise after "Likes to buy".
    The date column ("ma 19 okt", Dutch) is ignored: the post link is what matters."""
    rows = list(csv.reader(io.StringIO(text)))
    hi = next((i for i, r in enumerate(rows) if any(c.strip().lower() == "post link" for c in r)), None)
    if hi is None:
        raise SourceError("Views tab has no 'Post link' header")
    head = [c.strip().lower() for c in rows[hi]]
    if "views to buy" not in head:
        raise SourceError("Views tab has no 'Views to buy' header")
    li = head.index("post link")
    cols = [(k, head.index(h)) for k, h in (("views", "views to buy"), ("likes", "likes to buy")) if h in head]
    out = []
    for r in rows[hi + 1:]:
        cell = lambda i: r[i].strip() if i < len(r) else ""  # noqa: E731
        urls = URL_RX.findall(cell(li))
        if not urls:
            continue
        got = {k: (_num(cell(i)) if cell(i + 1).lower() == "done" else 0) for k, i in cols}
        if any(got.values()):
            out.append({"url": urls[0], "views": got.get("views", 0), "likes": got.get("likes", 0)})
    return out


async def fetch_views(sheet_id: str) -> str:
    return await fetch_sheet(f"{sheet_id}:{VIEWS_TAB}")


TOP_TAB = "Top Performers"


def _qty(s: str) -> int | None:
    """'200k' / '~200k+' / '1.5 M views' / '1,5m' / '1,500k' -> 200000 / 200000 / 1500000 / 1500000 / 1500000;
    bare '20,000' / '1.547' (Dutch) / '1547' -> 20000 / 1547 / 1547. Blank -> 0. Anything else (incl. '1.5', 0) -> None."""
    s = re.sub(r"views?|\s", "", (s or "").lower()).lstrip("~").rstrip("+")
    if not s:
        return 0
    m = re.fullmatch(r"(\d+)(?:([.,])(\d+))?([km])", s)
    if m:   # comma + 3 digits before k/m = thousands ('1,500k'); otherwise the separator is a decimal point
        n = float(m[1] + m[3]) if m[2] == "," and len(m[3]) == 3 else float(f"{m[1]}.{m[3] or 0}")
        n = round(n * (1000 if m[4] == "k" else 1_000_000))
    else:
        n = int(re.sub(r"[.,]", "", s)) if re.fullmatch(r"\d+|\d{1,3}(?:[.,]\d{3})+", s) else 0
    return n or None


def parse_top(text: str) -> tuple[list[dict], list[str]]:
    """The sheet's "Top Performers" tab -> ([{url, views, likes: 0}], unreadable "Views to buy" cells). No status
    column: every row with a URL in "Full URL" and a readable "Views to buy" was placed; an unreadable one is skipped
    and returned. No such header = no such tab (gviz answers an unknown tab name with the first tab) -> ([], [])."""
    rows = list(csv.reader(io.StringIO(text)))
    hi = next((i for i, r in enumerate(rows) if {"full url", "views to buy"} <= {c.strip().lower() for c in r}), None)
    if hi is None:
        return [], []
    head = [c.strip().lower() for c in rows[hi]]
    ui, vi = head.index("full url"), head.index("views to buy")
    out, bad = [], []
    for r in rows[hi + 1:]:
        urls = URL_RX.findall(r[ui]) if ui < len(r) else []
        n = _qty(r[vi]) if vi < len(r) else 0
        if urls and n is None:
            bad.append(r[vi].strip())
        elif urls and n:
            out.append({"url": urls[0], "views": n, "likes": 0})
    return out, bad


async def fetch_top(sheet_id: str) -> str:
    return await fetch_sheet(f"{sheet_id}:{TOP_TAB}")


# --------------------------------------------------------------------------- #
# Clippers submissions / SideShift
# --------------------------------------------------------------------------- #
def _epoch(s) -> int | None:
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return int((d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)).timestamp())


async def fetch_clippers(ref: str) -> list[dict]:
    out = []
    for s in await clippers.submissions(int(ref)):
        ts = _epoch(s["submitted_at"])
        if not s["url"] or ts is None:
            continue
        out.append({"url": s["url"], "platform": s["platform"], "creator_id": s["clipper"],
                    "account_handle": s["clipper"], "uploaded_at": ts,
                    "day": dt.datetime.fromtimestamp(ts, ET).date().isoformat(), "slot": None, "source": "clippers"})
    return out


SS_BASE = "https://app.sideshift.app/api/oauth/v1"
SS_TTL = 600        # one sweep per program per run: the links pass and the counts pass share it
SS_MAX_PAGES = 1000
SS_GAP = 0.1        # metrics-history: one call per SS_GAP s across the whole process (10/s, limit 600/min)
SS_BACKOFF = 60     # after a 429: no history calls for this long (callers fall back)
SS_DELETED = ("deleted", "removed", "true", "1")
SS_ALIAS = {"ig": "instagram", "fb": "facebook", "tt": "tiktok", "yt": "youtube", "youtube_shorts": "youtube"}
_ss_cache: dict[str, tuple[int, object]] = {}   # program id -> (fetched at, items | the exception)
ss_notes: set[str] = set()   # sweep oddities for the planner to alert once (page cap hit, no totalPages)
_ss_seen_del: set[str] = set()
_ss_next = 0.0     # next free metrics-history slot (monotonic)
_ss_wait = 0.0     # no metrics-history calls before this (monotonic), set by a 429


class RateLimited(SourceError):
    pass


async def _ss_get(path: str, params: dict | None = None):
    """GET only (the key also has write scopes: never POST/PUT/DELETE). -> the response's `data`."""
    key = os.getenv("SIDESHIFT_PLATFORM_KEY", "")
    if not key:
        raise SourceError("SIDESHIFT_PLATFORM_KEY not set")
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{SS_BASE}{path}", params=params, headers={"x-api-key": key})
    if r.status_code == 429:
        raise RateLimited(f"SideShift {path.split('?')[0]}: HTTP 429")
    if r.status_code != 200:
        raise SourceError(f"SideShift {path.split('?')[0]}: HTTP {r.status_code}")
    return r.json()["data"]


async def ss_items(program_id: str) -> tuple[int, list[dict]]:
    """Every video of a program (all pages, 100 per page) -> (fetched at, items). Cached SS_TTL s, failures too."""
    hit = _ss_cache.get(program_id)
    if hit and time.time() - hit[0] < SS_TTL:
        if isinstance(hit[1], Exception):
            raise hit[1]
        return hit
    try:
        items, page = [], 1
        while True:
            d = await _ss_get("/analytics/videos", {"programId": program_id, "limit": 100, "page": page})
            got = d.get("data") or []
            items += got
            tp = d.get("totalPages")
            if tp is None:
                ss_notes.add(f"SideShift program {program_id}: no totalPages, paged until an empty page")
            if (int(tp) if tp is not None else page + 1) <= page or not got:
                break
            if page >= SS_MAX_PAGES:
                ss_notes.add(f"SideShift program {program_id}: stopped at the {SS_MAX_PAGES}-page cap, later posts missing")
                break
            page += 1
    except Exception as e:  # noqa: BLE001
        _ss_cache[program_id] = (int(time.time()), e)
        raise
    _ss_cache[program_id] = (int(time.time()), items)
    return _ss_cache[program_id]


def ss_platform(it: dict) -> str | None:
    p = str(it.get("platform") or "").strip().lower()
    p = SS_ALIAS.get(p, p)
    return p if p in SLOT_PLATFORM.values() else detect_platform(it.get("postPage") or "")


def ss_deleted(it: dict) -> bool:
    """Deleted only for explicit values (deleted / removed / true / 1); an unknown value is logged once, not deleted."""
    v = it.get("deletionStatus")
    if v is None:
        return False
    t = str(v).strip().lower()
    if t in SS_DELETED:
        return True
    if t and t not in _ss_seen_del:
        _ss_seen_del.add(t)
        log.warning("SideShift deletionStatus %r not understood: post kept", v)
    return False


def ss_link(it: dict) -> dict | None:
    url, ts, plat = it.get("postPage"), it.get("uploadedAt"), ss_platform(it)
    if not url or not plat or ts is None:
        return None
    ts = int(ts)
    return {"url": url, "platform": plat, "creator_id": it.get("contractorId") or it.get("creator"),
            "account_handle": it.get("creator") or it.get("contractorId"), "uploaded_at": ts,
            "day": dt.datetime.fromtimestamp(ts, ET).date().isoformat(), "slot": None, "source": "sideshift",
            "name": (it.get("contractorName") or "").strip()}


async def fetch_sideshift(ref: str) -> list[dict]:
    return [x for x in (ss_link(it) for it in (await ss_items(ref.strip()))[1]) if x]


async def ss_history(post_id: str) -> list[dict]:
    """Daily snapshots [{timestamp, views, likes, ...}] of one post (SideShift refreshes about once a day).
    Process-wide limit: one call per SS_GAP s. A 429 = RateLimited, and calls fail fast for SS_BACKOFF s."""
    global _ss_next, _ss_wait
    t = time.monotonic()
    if t < _ss_wait:
        raise RateLimited("SideShift metrics-history: backing off after a 429")
    slot, _ss_next = max(t, _ss_next), max(t, _ss_next) + SS_GAP
    if slot > t:
        await asyncio.sleep(slot - t)
    try:
        d = await _ss_get(f"/posts/{post_id}/metrics-history")
    except RateLimited:
        _ss_wait = time.monotonic() + SS_BACKOFF
        raise
    return [h for h in (d.get("history") or []) if h.get("timestamp") is not None]


async def collect(camp: dict, today: dt.date) -> dict:
    """All sources of a campaign -> {links, slots, cancelled, errors}. A failing source is reported, not fatal."""
    out = {"links": [], "slots": [], "cancelled": {}, "errors": []}
    try:
        rs = refs(camp)
    except SourceError as e:
        out["errors"].append(str(e))
        return out
    dl = json.loads(camp["slot_deadlines"]) if camp.get("slot_deadlines") else None
    for src, ref in rs:
        try:
            if src == "sheet":
                got = parse_sheet(await fetch_sheet(ref), today, dl)
                out["links"] += got["links"]; out["slots"] += got["slots"]; out["cancelled"].update(got["cancelled"])
            elif src == "clippers":
                out["links"] += await fetch_clippers(ref)
            elif src == "sideshift":
                out["links"] += await fetch_sideshift(ref)
            else:
                raise SourceError(f"unknown source {src}")
        except Exception as e:  # noqa: BLE001  one bad source must not stop the others
            out["errors"].append(f"{src}: {e!s}"[:200])
    return out


# --------------------------------------------------------------------------- #
# post_key (+ one-time redirect resolve for share / short links)
# --------------------------------------------------------------------------- #
_TT_ID = re.compile(r"/(?:video|photo)/(\d+)")


def post_id(platform: str, url: str) -> str | None:
    k = url_key(url)
    if k[:3] in ("ig:", "fb:", "yt:"):
        return k[3:]
    m = _TT_ID.search(url) if platform == "tiktok" else None
    return m.group(1) if m else None


def post_key(platform: str, url: str) -> str | None:
    pid = post_id(platform, url)
    return f"{platform}:{pid}" if pid else None


async def _follow(url: str) -> str:
    async with httpx.AsyncClient(timeout=20, follow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0"}) as c:
        r = await c.get(url)
    return str(r.url)


async def resolve(platform: str, url: str) -> str | None:
    """URL whose post id can be read: as is, or after following the redirect once (cached in settings)."""
    if post_id(platform, url):
        return url
    try:
        host = (urlsplit(url).hostname or "").lower()
    except ValueError:
        host = ""
    if not any(host == h or host.endswith("." + h) for h in HOSTS):
        return None   # only platform links are followed
    k = f"planner:url:{url}"
    hit = db.q1("SELECT v FROM settings WHERE k=?", (k,))
    if hit:
        return hit["v"] or None   # "" = failed before: not followed again
    try:
        final = await _follow(url)
    except Exception:  # noqa: BLE001
        final = ""
    if not post_id(platform, final):
        final = ""
    db.ex("INSERT OR IGNORE INTO settings (k, v) VALUES (?, ?)", (k, final))
    return final or None


# --------------------------------------------------------------------------- #
# Counts: the ONE function. SideShift later = replace the body.
# --------------------------------------------------------------------------- #
CLIPPERS_FRESH_SEC = 26 * 3600   # a Clippers scrape older than this is not trusted


async def get_counts(platform: str, url: str):
    """-> (views, likes[, comments]) | DELETED (platform says not found) | None (unreadable: skip, never guess).
    likes is None when the platform hides it. Lumina Clippers' own scrape first (free) when the post is a
    Clippers submission with a fresh successful count, else Apify. No calls in PANEL_DRY_RUN. Any exception = None."""
    try:
        return await _clippers_counts(url) or await _counts(platform, url)
    except Exception:  # noqa: BLE001
        return None


async def _clippers_counts(url: str):
    """(views, likes) from the Clippers admin API, or None (not a submission there, stale, failed or 0 views)."""
    if os.getenv("PANEL_DRY_RUN") == "1" or not config.CLIPPERS_API_BASE:
        return None
    key = url_key(url)
    term = key.split(":", 1)[-1].rsplit("/", 1)[-1]
    if len(term) < 6:
        return None
    for s in await clippers._get("/api/chat/submissions-search", {"search": term}):
        if url_key(s.get("post_url") or "") != key:
            continue
        d = await clippers._get(f"/api/submissions/{int(s['id'])}")
        try:
            ran = dt.datetime.fromisoformat(str(d.get("scrape_last_run"))).replace(tzinfo=dt.timezone.utc).timestamp()
        except ValueError:
            continue
        views = int(d.get("views") or 0)
        if d.get("scrape_status") == "success" and views > 0 and dt.datetime.now(dt.timezone.utc).timestamp() - ran < CLIPPERS_FRESH_SEC:
            likes = d.get("likes")
            return views, (int(likes) if likes else None)
    return None


async def _counts(platform: str, url: str):
    if os.getenv("PANEL_DRY_RUN") == "1" or platform not in apify_client.PLATFORM_ACTORS:
        return None
    items = await apify_client._run_actor(apify_client.PLATFORM_ACTORS[platform],
                                          apify_client._build_input(platform, url), 40)
    if not items or not isinstance(items[0], dict):
        return None
    it = items[0]
    err = str(it.get("error") or it.get("errorDescription") or "").lower()
    if err:
        return DELETED if any(w in err for w in GONE_WORDS) else None
    c = apify_client._extract(platform, it)
    if platform == "facebook" and not c["views"]:
        c["views"] = await apify_client.fb_play_count(it)
        if not c["views"]:
            return None   # fb_play_count gives 0 on failure: unreadable, never 0
    return c["views"], (None if c["likes"] < 0 else c["likes"]), c["comments"]   # v2.16: comments from the same read
