"""Campaign links: Lumina Clippers submissions -> boost queue -> panel orders.

Safety model (every order costs real money):
  * one queue row per (link, submission), plus a global dedupe on the normalized
    post URL so the same post is never boosted twice across links;
  * slow panel lookups (catalog, balance) run before the lock and fail fast for
    60s once a provider is down; every placement then runs under one asyncio lock:
    guards (dedupe, daily cap, balance) -> claim row as `placing` (committed) ->
    panel call -> placed/failed;
  * auto mode only places rows queued since `links.auto_since`; older pending
    rows are backlog for manual approval;
  * a row found in `placing` at startup is marked failed, never re-placed;
  * `not_sent` rows (on the campaign before the link) are never auto-placed, only approved;
  * a post that already has a manual job is never boosted by a campaign (and vice versa,
    see `duplicate_reason`);
  * v2.11 Buy again (`rebuy_item`) is the one way to order an already-placed post twice:
    manual only, same guards, recorded in `rebuys`, the queue row is never changed.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import re
import time
from typing import NamedTuple

import clippers
import db
import engine
import panels
from config import (
    CAMPAIGN_POLL_SEC, PLATFORM_HINTS, TYPE_HINTS, provider_label, rate_for, service_for,
    url_key,
)
from panels import PanelError

log = logging.getLogger("campaigns")

STATES = ("pending", "placing", "placed", "failed", "rejected", "skipped", "waiting_funds", "not_sent")
# Rows that hold (or will hold) a boost for their URL. skipped/rejected/not_sent free the URL.
CLAIMING = ("pending", "placing", "placed", "failed", "waiting_funds")

_place_lock = asyncio.Lock()
_link_locks: dict[int, asyncio.Lock] = {}
_panel_alerts: dict[str, tuple[str, int]] = {}   # provider -> (text, at)
# Failed rows whose order may exist on the panel. Counted as spent (conservative).
AMBIGUOUS = "check panel before retry"
REBUY_GAP_SEC = 600   # v2.11: min gap between two Buy again orders on one post


# --------------------------------------------------------------------------- #
# Catalog text matching (used by /services and to scope service overrides)
# --------------------------------------------------------------------------- #
def svc_text(s: dict) -> str:
    return f" {s.get('name', '')} {s.get('category', '')} ".lower()


def svc_matches(s: dict, platform: str | None, otype: str | None) -> bool:
    t = svc_text(s)
    if platform and not any(h in t for h in PLATFORM_HINTS.get(platform, (platform,))):
        return False
    if otype and not any(h in t for h in TYPE_HINTS.get(otype, (otype,))):
        return False
    return True


def svc_platform(s: dict | None) -> str | None:
    if not s:
        return None
    hits = [p for p in PLATFORM_HINTS if svc_matches(s, p, None)]
    return hits[0] if len(hits) == 1 else None


async def override_for(provider: str, service: str | None, platform: str) -> str | None:
    """Use an override only on the platform its catalog row names (or any
    platform when the name does not say)."""
    if not service:
        return None
    info = await panels.service_info(provider, service)
    sp = svc_platform(info)
    return str(service) if sp in (None, platform) else None


async def live_rate(provider: str, platform: str, otype: str, sid: str | None = None) -> float:
    """Live catalog rate per 1k for sid (or the panel default); config rate if the catalog has none."""
    sid = sid or service_for(provider, platform, otype)
    if sid:
        try:
            info = await panels.service_info(provider, sid)
            if info and info.get("rate") is not None:
                return float(info["rate"])
        except Exception:  # noqa: BLE001
            pass
    return rate_for(provider, platform, otype)


async def qty_problem(provider: str, platform: str, views: int, likes: int,
                      vs: str | None, ls: str | None) -> str | None:
    """Why the panel would reject these amounts (service min/max), or None.
    Views are never raised silently; JAP drips, so its check is per run."""
    from engine import view_drip_kwargs   # lazy: engine imports campaigns

    async def info(sid):
        try:
            return await panels.service_info(provider, sid) if sid else None
        except Exception:  # noqa: BLE001
            return None   # catalog down: let the panel decide

    def num(x):
        try:
            return int(float(x))
        except (TypeError, ValueError):
            return 0

    vsid = vs or service_for(provider, platform, "views")
    vi = await info(vsid)
    if vi and views > 0:
        lo, hi = num(vi.get("min")), num(vi.get("max"))
        per_run = views // (view_drip_kwargs(provider, views, lo).get("runs") or 1)
        if lo and per_run < lo:
            return f"Views {views:,} are below service {vsid}'s minimum of {lo:,}" + \
                (f" per drip run ({per_run:,} per run)" if per_run != views else "")
        if hi and per_run > hi:
            return f"Views {views:,} are above service {vsid}'s maximum of {hi:,}"
    lsid = ls or service_for(provider, platform, "likes")
    li = await info(lsid) if likes > 0 else None
    if li and num(li.get("max")) and likes > num(li["max"]):
        return f"Likes {likes:,} are above service {lsid}'s maximum of {num(li['max']):,}"
    return None


async def est_cost(provider: str, platform: str, views: int, likes: int,
                   vs: str | None, ls: str | None) -> float:
    return round(views / 1000 * await live_rate(provider, platform, "views", vs)
                 + likes / 1000 * await live_rate(provider, platform, "likes", ls), 4)


# --------------------------------------------------------------------------- #
# Views
# --------------------------------------------------------------------------- #
def _utc_midnight() -> int:
    now = dt.datetime.now(dt.timezone.utc)
    return int(dt.datetime(now.year, now.month, now.day, tzinfo=dt.timezone.utc).timestamp())


def spent_today(link_id: int) -> float:
    r = db.q1("SELECT COALESCE(SUM(est_cost),0) s FROM queue WHERE link_id=? "
              "AND (state IN ('placed','placing') OR (state='failed' AND error LIKE ?)) "
              "AND COALESCE(placed_at, created_at) >= ?",
              (link_id, f"%{AMBIGUOUS}%", _utc_midnight()))
    rb = db.q1("SELECT COALESCE(SUM(est_cost),0) s FROM rebuys WHERE link_id=? "   # v2.11 Buy again
               "AND (state IN ('placed','placing') OR (state='failed' AND error LIKE ?)) AND created_at >= ?",
               (link_id, f"%{AMBIGUOUS}%", _utc_midnight()))
    return round(r["s"] + rb["s"], 4)


def link_view(l: dict) -> dict:
    counts = {s: 0 for s in ("pending", "placed", "failed", "skipped", "rejected", "not_sent")}
    for r in db.q("SELECT state, COUNT(*) n FROM queue WHERE link_id=? GROUP BY state", (l["id"],)):
        st = r["state"]
        st = "pending" if st == "waiting_funds" else ("placed" if st == "placing" else st)
        counts[st] = counts.get(st, 0) + r["n"]
    backlog = 0
    if l["mode"] == "auto":
        backlog = db.q1("SELECT COUNT(*) n FROM queue WHERE link_id=? AND state='pending' "
                        "AND created_at < ?", (l["id"], l["auto_since"] or 0))["n"]
    return {
        "id": l["id"], "campaign_id": l["campaign_id"], "campaign_name": l["campaign_name"],
        "provider": l["provider"], "mode": l["mode"], "views": l["views"], "likes": l["likes"],
        "platforms": json.loads(l["platforms"] or "[]"), "views_service": l["views_service"],
        "likes_service": l["likes_service"], "services": link_services(l), "daily_cap_usd": l["daily_cap_usd"],
        "active": bool(l["active"]), "last_polled_at": l["last_polled_at"],
        "last_error": l["last_error"], "counts": counts, "spent_today_usd": spent_today(l["id"]),
        "auto_since": l["auto_since"], "backlog_pending": backlog,
        "thumbnail_url": clippers.thumbnail_for(l["campaign_id"]),
    }


_QCOLS = ("id", "link_id", "campaign_id", "campaign_name", "submission_id", "url", "platform",
          "clipper", "submitted_at", "state", "provider", "views", "likes", "job_id", "error",
          "created_at", "placed_at", "clipper_email")


def queue_view(r: dict) -> dict:
    return {k: r.get(k) for k in _QCOLS}


# Campaign page tabs (v2.6). Keys are SQL over `queue q LEFT JOIN jobs j ON j.id = q.job_id`.
GROUPS = {
    "pending": "q.state IN ('pending','waiting_funds','not_sent')",
    "placed": "(q.state = 'placing' OR (q.state = 'placed' AND COALESCE(j.state, '') <> 'done'))",
    "done": "(q.state = 'placed' AND j.state = 'done')",
    "closed": "q.state IN ('skipped','rejected','failed')",
    "all": "1 = 1",   # v2.11 All submissions
}
# v2.11: ordered from this console at least once (queue order or a Buy again), and how many times.
_REBUYS = "(SELECT COUNT(*) FROM rebuys r WHERE r.queue_id = q.id AND r.state IN ('placing','placed'))"
SENT = f"(q.state IN ('placing','placed') OR {_REBUYS} > 0)"
BUYS = f"((CASE WHEN q.state IN ('placing','placed') THEN 1 ELSE 0 END) + {_REBUYS})"


# --------------------------------------------------------------------------- #
# v2.11 All-submissions search filter (one source of truth for /api/queue/search
# and the v2.23 CSV export, so the CSV always matches what the UI shows).
# --------------------------------------------------------------------------- #
SEARCH_MAX_TERMS = 5000
_EMAIL_RX = re.compile(r"^[^@\s,;<>/]+@[^@\s,;<>/]+\.[^@\s,;<>/]+$")
_ID_RX = re.compile(r"^[A-Za-z0-9_-]{5,}$")   # v2.17: a bare post id / shortcode (De23gxEqfJP) pasted without the URL


def _term_keys(t: str) -> set:
    """url_key of a link term; a bare id also matches the ig/yt/fb keys url_key builds from a full URL."""
    k = {url_key(t)}
    if _ID_RX.match(t):
        k |= {f"ig:{t}", f"yt:{t}", f"fb:{t}"}
    return k


class SearchFilter(NamedTuple):
    base: str      # FROM queue q LEFT JOIN jobs j ... WHERE q.link_id = ?   (first arg)
    tw: str        # " AND (<term/email/substring ORs>)"  — "" when no terms match anything
    sw: str        # " AND <sent clause>"                 — "" when sent == 'all'
    args: tuple    # params for base and base+tw (sw carries no params)
    terms: list    # cleaned, de-duped search terms
    emails: set    # lower-cased terms that are exact emails
    tkeys: dict     # non-email term -> its url_key set
    sub: bool      # a single short term also substring-matches url / email / clipper


def search_filter(link_id: int, terms: list, sent: str = "all") -> SearchFilter:
    """Build the All-submissions WHERE for one link. terms = links (matched by url_key)
    and/or emails (case-insensitive exact); a single short term also substring-matches
    url / email / clipper. Raises ValueError on a bad `sent` or too many terms (the route
    maps that to HTTP 400). This is the only place the v2.11 filter is built."""
    if sent not in ("all", "yes", "no"):
        raise ValueError("sent must be all, yes or no")
    terms = list(dict.fromkeys(t.strip() for t in terms if t and t.strip()))
    if len(terms) > SEARCH_MAX_TERMS:
        raise ValueError(f"too many search terms (max {SEARCH_MAX_TERMS:,})")
    emails = {t.lower() for t in terms if _EMAIL_RX.match(t)}
    tkeys = {t: _term_keys(t) for t in terms if t.lower() not in emails}
    keys = set().union(*tkeys.values()) if tkeys else set()
    ors, targs = [], []
    if keys:
        ors.append(f"q.url_key IN ({','.join('?' * len(keys))})"); targs += list(keys)
    if emails:
        ors.append(f"LOWER(q.clipper_email) IN ({','.join('?' * len(emails))})"); targs += list(emails)
    sub = len(terms) == 1 and len(terms[0]) <= 200
    if sub:
        like = "%" + re.sub(r"([\\%_])", r"\\\1", terms[0]) + "%"
        ors.append("(q.url LIKE ? ESCAPE '\\' OR q.clipper_email LIKE ? ESCAPE '\\' OR q.clipper LIKE ? ESCAPE '\\')")
        targs += [like] * 3
    tw = f" AND ({' OR '.join(ors)})" if ors else ""
    sw = {"all": "", "yes": f" AND {SENT}", "no": f" AND NOT {SENT}"}[sent]
    base = "FROM queue q LEFT JOIN jobs j ON j.id = q.job_id WHERE q.link_id = ?"
    return SearchFilter(base, tw, sw, (link_id, *targs), terms, emails, tkeys, sub)


def export_rows(link_id: int, terms: list, sent: str = "all") -> list[dict]:
    """Every All-submissions row for one link under the same filter as the v2.11 search,
    newest first, no pagination. Read-only (no panel calls, no writes). Each row is a
    queue_view plus job_state, buys and sent — exactly the fields the queue items carry."""
    f = search_filter(link_id, terms, sent)
    rows = db.q(f"SELECT q.*, j.state AS job_state, {BUYS} AS buys {f.base}{f.tw}{f.sw} "
                "ORDER BY q.id DESC", f.args)
    return [{**queue_view(r), "job_state": r.get("job_state"), "buys": r.get("buys") or 0,
             "sent": bool(r.get("buys"))} for r in rows]


def link_services(l: dict) -> dict:
    return json.loads(l.get("services") or "{}")


def duplicate_reason(key: str, exclude_qid: int | None = None) -> str | None:
    """Why this post must not be boosted again: a placing/placed campaign row or a
    manual (non-campaign) job on the same url_key. Indexed lookups only."""
    r = db.q1("SELECT id FROM queue WHERE url_key=? AND id<>? AND state IN ('placing','placed') LIMIT 1",
              (key, exclude_qid or 0))
    if r:
        return f"duplicate post URL (queue #{r['id']})"
    j = db.q1("SELECT id FROM jobs WHERE url_key=? AND COALESCE(source,'manual')<>'campaign' LIMIT 1", (key,))
    if j:
        return f"post already has a manual job (#{j['id']})"
    return None


def get_link(link_id: int) -> dict | None:
    return db.q1("SELECT * FROM links WHERE id=?", (link_id,))


def get_item(qid: int) -> dict | None:
    return db.q1("SELECT * FROM queue WHERE id=?", (qid,))


# --------------------------------------------------------------------------- #
# Polling: Clippers submissions -> queue rows
# --------------------------------------------------------------------------- #
def _insert_submission(link: dict, s: dict, state: str, error: str | None = None) -> bool:
    key = url_key(s["url"])
    if state == "pending":
        dup = db.q1(f"SELECT id FROM queue WHERE url_key=? AND state IN ({','.join('?' * len(CLAIMING))}) "
                    "LIMIT 1", (key, *CLAIMING))
        if dup:
            state, error = "skipped", f"duplicate post URL (queue #{dup['id']})"
    n, _ = db.ex(
        "INSERT OR IGNORE INTO queue (link_id, campaign_id, campaign_name, submission_id, url, "
        "url_key, platform, clipper, submitted_at, state, provider, views, likes, error, created_at, clipper_email) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (link["id"], link["campaign_id"], link["campaign_name"], s["id"], s["url"], key,
         s["platform"], s["clipper"], s["submitted_at"], state, link["provider"], link["views"],
         link["likes"], error, int(time.time()), s.get("email")))
    if not n and s.get("email"):   # v2.11: rows queued before emails were stored
        db.ex("UPDATE queue SET clipper_email=? WHERE link_id=? AND submission_id=? AND clipper_email IS NULL",
              (s["email"], link["id"], s["id"]))
    return n > 0


async def record_existing(link: dict, subs: list[dict]) -> None:
    """backfill:false -> everything already on the campaign is `not_sent`: never
    auto-placed, only by a manual approve."""
    def go():
        for s in subs:
            _insert_submission(link, s, "not_sent")
    await asyncio.to_thread(go)


async def poll_link(link_id: int) -> int:
    """Fetch the campaign's submissions and queue the new ones. Returns #new."""
    lock = _link_locks.setdefault(link_id, asyncio.Lock())
    async with lock:
        link = await asyncio.to_thread(get_link, link_id)
        if not link:
            return 0
        try:
            subs = await clippers.submissions(link["campaign_id"])
        except Exception as e:  # noqa: BLE001
            await asyncio.to_thread(db.update, "links", link_id, last_polled_at=int(time.time()),
                                    last_error=str(e)[:200])
            raise
        plats = set(json.loads(link["platforms"] or "[]"))

        def go():
            new = 0
            for s in sorted(subs, key=lambda x: x["id"]):
                if s["platform"] in plats and s["url"]:
                    new += _insert_submission(link, s, "pending")
            db.update("links", link_id, last_polled_at=int(time.time()), last_error=None)
            return new
        new = await asyncio.to_thread(go)
    if new:
        log.info("link %s: %d new submission(s)", link_id, new)
    if link["mode"] == "auto" and link["active"]:
        await process_link_auto(link_id)
    return new


async def process_link_auto(link_id: int) -> None:
    """Auto-place only rows queued since the link went auto/active; older pending
    rows are backlog and wait for a manual approve."""
    rows = await asyncio.to_thread(
        db.q, "SELECT q.id, l.provider FROM queue q JOIN links l ON l.id=q.link_id "
              "WHERE q.link_id=? AND q.state='pending' AND q.created_at >= COALESCE(l.auto_since, 0) "
              "ORDER BY q.id", (link_id,))
    for r in rows:
        if panels.down(r["provider"]):
            break
        await place_item(r["id"], ("pending",))


# --------------------------------------------------------------------------- #
# Placement
# --------------------------------------------------------------------------- #
def _set(qid: int, **f) -> None:
    db.update("queue", qid, **f)


def _set_if(qid: int, from_states: tuple[str, ...], **f) -> dict | None:
    """Update only while the row is still in from_states (used outside the lock)."""
    n, _ = db.ex(f"UPDATE queue SET {', '.join(f'{k} = ?' for k in f)} WHERE id=? "
                 f"AND state IN ({','.join('?' * len(from_states))})",
                 (*f.values(), qid, *from_states))
    return get_item(qid) if n else None


async def _services(link: dict, platform: str) -> tuple[str | None, str | None]:
    """services[platform][type] > legacy override (platform-name check) > default."""
    per, provider = link_services(link).get(platform) or {}, link["provider"]
    vs = per.get("views") or await override_for(provider, link["views_service"], platform) or \
        service_for(provider, platform, "views")
    ls = per.get("likes") or await override_for(provider, link["likes_service"], platform) or \
        service_for(provider, platform, "likes")
    return vs, ls


async def place_item(qid: int, from_states: tuple[str, ...]) -> dict | None:
    """Place one queue row if it is in `from_states`. Returns the updated row, or
    None when the row was not in an allowed state (someone else has it).

    Slow panel lookups (catalog, balance) run BEFORE taking _place_lock; the lock
    covers only the re-check, guards, claim and the order call."""
    row = await asyncio.to_thread(get_item, qid)
    if not row or row["state"] not in from_states:
        return None
    link = await asyncio.to_thread(get_link, row["link_id"])
    if not link:
        return await asyncio.to_thread(_set_if, qid, from_states, state="failed", error="link deleted")
    provider, platform = link["provider"], row["platform"]
    views = int(link["views"])

    try:
        vs, ls = await _services(link, platform)
        likes = await engine.likes_qty(provider, ls, int(link["likes"]))   # no likes service -> 0
        bal = await panels.balance_cached(provider)
    except Exception as e:  # noqa: BLE001  panel lookup failed: retry next tick
        msg = str(e) if isinstance(e, panels.PanelUnavailable) else f"panel unavailable: {e!s}"
        return await asyncio.to_thread(_set_if, qid, from_states, state="waiting_funds", error=msg[:200])
    if not vs:
        return await asyncio.to_thread(
            _set_if, qid, from_states, state="failed",
            error=f"{provider_label(provider)} has no views service for {platform}")
    cost = await est_cost(provider, platform, views, likes, vs, ls)

    async with _place_lock:
        row = await asyncio.to_thread(get_item, qid)
        if not row or row["state"] not in from_states:
            return None

        # 1. never boost the same post twice (campaign row or manual job)
        dup = await asyncio.to_thread(duplicate_reason, row["url_key"], qid)
        if dup:
            await asyncio.to_thread(_set, qid, state="skipped", error=dup)
            return await asyncio.to_thread(get_item, qid)
        import automation   # lazy: automation imports campaigns
        if await asyncio.to_thread(automation.in_automation, row["url_key"]):   # v2.7: one flow per post
            await asyncio.to_thread(_set, qid, state="skipped", error="in automation")
            return await asyncio.to_thread(get_item, qid)

        # 2. daily cap (UTC day, estimated cost)
        cap = link["daily_cap_usd"]
        if cap is not None:
            spent = await asyncio.to_thread(spent_today, link["id"])
            if spent + cost > cap:
                await asyncio.to_thread(_set, qid, state="waiting_funds",
                                        error=f"daily cap ${cap:.2f} reached (spent ${spent:.2f})")
                return await asyncio.to_thread(get_item, qid)

        # 3. balance guard (fetched above; peek sees spends made since, under this lock)
        peek = panels.balance_peek(provider)
        bal = peek if peek is not None else bal
        if cost > bal:
            await asyncio.to_thread(_set, qid, state="waiting_funds",
                                    error=f"low balance ${bal:.2f} < est ${cost:.4f}")
            return await asyncio.to_thread(get_item, qid)

        # 4. claim (committed) BEFORE the panel call
        states = ",".join("?" * len(from_states))
        n, _ = await asyncio.to_thread(
            db.ex, f"UPDATE queue SET state='placing', provider=?, views=?, likes=?, est_cost=?, "
                   f"error=NULL, placed_at=? WHERE id=? AND state IN ({states})",
            (provider, views, likes, cost, int(time.time()), qid, *from_states))
        if n != 1:
            return None

        # 5. place
        try:
            res = await engine.place_order(provider, platform, row["url"], views, likes,
                                           created_by=f"link:{link['id']}", source="campaign",
                                           views_service=vs, likes_service=ls)
        except PanelError as e:
            _panel_alerts[provider] = (f"{provider_label(provider)}: {e!s}"[:160], int(time.time()))
            await asyncio.to_thread(_set, qid, state="failed", error=f"panel: {e!s}"[:300])
            return await asyncio.to_thread(get_item, qid)
        except Exception as e:  # noqa: BLE001  order may exist: count it as spent
            panels.balance_spend(provider, cost)
            panels.mark_down(provider, f"{type(e).__name__}: {e}")
            await asyncio.to_thread(_set, qid, state="failed",
                                    error=f"{type(e).__name__}: {e!s}"[:240] + f" ({AMBIGUOUS})")
            return await asyncio.to_thread(get_item, qid)
        panels.balance_spend(provider, cost)
        await asyncio.to_thread(_set, qid, state="placed", job_id=res["job_id"],
                                placed_at=int(time.time()), error=None)
        return await asyncio.to_thread(get_item, qid)


def recover_stuck() -> int:
    n, _ = db.ex("UPDATE queue SET state='failed', error=? WHERE state='placing'",
                 (f"interrupted, {AMBIGUOUS}",))
    m, _ = db.ex("UPDATE rebuys SET state='failed', error=? WHERE state='placing'",
                 (f"interrupted, {AMBIGUOUS}",))
    return n + m


async def rebuy_item(qid: int, views: int, likes: int, who: str) -> dict:
    """v2.11 Buy again: a second, smaller order on a post this console already ordered (queue row `placed`).
    Manual only, never auto. Same guards as place_item, but the queue row is never changed: each attempt
    that reaches the panel is a `rebuys` row. Returns {"id", "ok", "rebuy_id"?, "job_id"?, "error"?}."""
    def no(err, **kw):
        return {"id": qid, "ok": False, "error": err, **kw}
    row = await asyncio.to_thread(get_item, qid)
    if not row:
        return no("no such item")
    if row["state"] != "placed":
        return no("still placing" if row["state"] == "placing" else f"not sent ({row['state']}): use Approve")
    link = await asyncio.to_thread(get_link, row["link_id"])
    if not link:
        return no("link deleted")
    mv, ml = int(link["views"]), int(link["likes"] or 0)
    if not 0 < views <= mv or not 0 <= likes <= ml:
        return no(f"views must be 1 to {mv:,} and likes 0 to {ml:,} (the link's per-post amounts)")
    provider, platform = link["provider"], row["platform"]
    try:
        vs, ls = await _services(link, platform)
        likes = await engine.likes_qty(provider, ls, likes)   # no likes service -> 0
        bal = await panels.balance_cached(provider)
    except Exception as e:  # noqa: BLE001
        return no(str(e)[:200] if isinstance(e, panels.PanelUnavailable) else f"panel unavailable: {e!s}"[:200])
    if not vs:
        return no(f"{provider_label(provider)} has no views service for {platform}")
    bad = await qty_problem(provider, platform, views, likes, vs, ls)
    if bad:
        return no(bad)
    cost = await est_cost(provider, platform, views, likes, vs, ls)

    async with _place_lock:
        cur = await asyncio.to_thread(get_item, qid)
        if not cur or cur["state"] != "placed":
            return no("queue row changed, refresh")
        # a double click / retry / second tab must not buy twice: one Buy again per post per REBUY_GAP_SEC
        recent = await asyncio.to_thread(
            db.q1, "SELECT id FROM rebuys WHERE queue_id=? AND (state='placing' OR created_at >= ?) "
                   "AND NOT (state='failed' AND COALESCE(error,'') NOT LIKE ?) LIMIT 1",
            (qid, int(time.time()) - REBUY_GAP_SEC, f"%{AMBIGUOUS}%"))
        if recent:
            return no(f"a Buy again on this post ran in the last {REBUY_GAP_SEC // 60} min (rebuy #{recent['id']})")
        dup = await asyncio.to_thread(duplicate_reason, row["url_key"], qid)
        if dup:
            return no(dup)
        import automation   # lazy: automation imports campaigns
        if await asyncio.to_thread(automation.in_automation, row["url_key"]):
            return no("in automation")
        cap = link["daily_cap_usd"]
        if cap is not None:
            spent = await asyncio.to_thread(spent_today, link["id"])
            if spent + cost > cap:
                return no(f"daily cap ${cap:.2f} reached (spent ${spent:.2f})")
        peek = panels.balance_peek(provider)
        bal = peek if peek is not None else bal
        if cost > bal:
            return no(f"low balance ${bal:.2f} < est ${cost:.4f}")
        rid = await asyncio.to_thread(   # claim (committed) BEFORE the panel call
            db.insert, "rebuys", queue_id=qid, link_id=link["id"], url_key=row["url_key"], state="placing",
            provider=provider, views=views, likes=likes, est_cost=cost, created_by=who, created_at=int(time.time()))
        try:
            res = await engine.place_order(provider, platform, row["url"], views, likes,
                                           created_by=f"link:{link['id']}", source="campaign",
                                           views_service=vs, likes_service=ls)
        except PanelError as e:
            _panel_alerts[provider] = (f"{provider_label(provider)}: {e!s}"[:160], int(time.time()))
            err = f"panel: {e!s}"[:300]
            await asyncio.to_thread(db.update, "rebuys", rid, state="failed", error=err)
            return no(err, rebuy_id=rid)
        except Exception as e:  # noqa: BLE001  order may exist: count it as spent
            panels.balance_spend(provider, cost)
            panels.mark_down(provider, f"{type(e).__name__}: {e}")
            err = f"{type(e).__name__}: {e!s}"[:240] + f" ({AMBIGUOUS})"
            await asyncio.to_thread(db.update, "rebuys", rid, state="failed", error=err)
            return no(err, rebuy_id=rid)
        panels.balance_spend(provider, cost)
        await asyncio.to_thread(db.update, "rebuys", rid, state="placed", job_id=res["job_id"])
        return {"id": qid, "ok": True, "rebuy_id": rid, "job_id": res["job_id"], "views": views, "likes": likes,
                "est_cost": cost}


async def retry_waiting() -> None:
    rows = await asyncio.to_thread(
        db.q, "SELECT q.id, l.provider FROM queue q JOIN links l ON l.id=q.link_id "
              "WHERE q.state='waiting_funds' AND l.active=1 ORDER BY q.id")
    failed: set[str] = set()   # providers that failed this tick: skip their other rows
    for r in rows:
        p = r["provider"]
        if p in failed or panels.down(p):
            continue
        res = await place_item(r["id"], ("waiting_funds",))
        if res and (res["error"] or "").startswith("panel unavailable") or panels.down(p):
            failed.add(p)


# --------------------------------------------------------------------------- #
# Poller loop (own task; never waits on Apify scrapes)
# --------------------------------------------------------------------------- #
async def poller_tick() -> None:
    now = int(time.time())
    links = await asyncio.to_thread(
        db.q, "SELECT id FROM links WHERE active=1 AND (last_polled_at IS NULL OR last_polled_at <= ?)",
        (now - CAMPAIGN_POLL_SEC,))
    for l in links:
        try:
            await poll_link(l["id"])
        except Exception as e:  # noqa: BLE001
            log.warning("poll link %s failed: %s", l["id"], e)
    await retry_waiting()


def panel_alerts(since: int) -> list[tuple[str, int]]:
    return [v for v in _panel_alerts.values() if v[1] >= since]
