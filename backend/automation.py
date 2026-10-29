"""v2.7 Automation: planner rows (`plan_rows`) -> panel orders (`auto_orders`).

Safety model (every live order costs real money):
  * the executor runs only with env AUTO_ENABLED=1; PANEL_DRY_RUN still fakes panel calls;
  * one executor at a time: an asyncio lock in-process plus a DB lease row (settings `auto:lock`);
  * a campaign starts in practice mode: `simulated` rows with est cost, no panel call;
  * live orders need a campaign cap (v2.12; an old campaign's weekly budget still counts) and a global ET
    daily cap (null = nothing placed); the cap is total spend over the campaign's life;
  * the order row is committed as `placing` before the panel call; a `placing` row found later is
    `ambiguous`; ambiguous orders (add timed out, maybe placed) are never re-sent automatically;
  * a clear failure (panel reject, partial/canceled/refunded, open > 24 h) gets ONE re-send of the
    missing qty on the next best service, then stops with an alert;
  * every live order is also a `jobs` row (source 'auto'), so the status sweep + service score cover it.
All days are America/New_York (ET): plan day, order day, budget week (Monday 00:00), global daily cap.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import math
import os
import random
import re
import time
import uuid
from zoneinfo import ZoneInfo

import campaigns
import db
import engine
import panels
import score
from config import (COMMENT_DEFAULTS, COMMENT_MAX_LEN, COMMENT_MIN, COMMENT_RATE_DEFAULT, PROVIDERS, detect_platform,
                    provider_label, service_for, url_key)
from panels import PanelError, client_for

log = logging.getLogger("automation")

ET = ZoneInfo("America/New_York")
PLATFORMS = ("instagram", "facebook", "tiktok", "youtube")
SCOPES = ("all", "campaign", "creator", "post", "panel")
MIN_SCORE = score.POOR          # 60
LEASE_SEC = 1800
CARRY_DAYS = 3                  # carried views older than this are dropped
RETRY_DAYS = 2                  # a failed order older than this is not auto re-sent
# Catalog rows that match "<platform> views" but are not post views (story, live, profile ...).
NOT_POST_VIEWS = ("story", "stories", "live", "profile", "impression", "reach", "like", "follow",
                  "comment", "save", "share", "visit", "subscriber", "watch time", "watchtime", "hours",
                  "review", "photo")   # "review" contains "view"; photo views do not count on a video
# Est cost of these rows counts as spent (refunds are not tracked: conservative).
LIVE_SPEND = ("(state IN ('placing','placed','completed','ambiguous') "
              "OR (state IN ('failed','retried','cancelled') AND job_id IS NOT NULL))")
LIVE_SPEND_O = LIVE_SPEND.replace("state", "o.state").replace("job_id", "o.job_id")   # v2.15: same, for joins
SETTINGS = {"global_daily_cap_usd": None, "low_balance_usd": 10.0}

_lock = asyncio.Lock()
_owner = uuid.uuid4().hex
_alerts: dict[str, tuple[str, int]] = {}   # text -> (level, first at)


def _now() -> int:
    return int(time.time())


def enabled() -> bool:
    return os.getenv("AUTO_ENABLED") == "1"


# --------------------------------------------------------------------------- #
# ET dates
# --------------------------------------------------------------------------- #
def et_day(ts: int | None = None) -> str:
    return dt.datetime.fromtimestamp(_now() if ts is None else ts, ET).date().isoformat()


def et_midnight(day: str) -> int:
    return int(dt.datetime.combine(dt.date.fromisoformat(day), dt.time(), ET).timestamp())


def week_start(ts: int) -> int:
    d = dt.datetime.fromtimestamp(ts, ET).date()
    return et_midnight((d - dt.timedelta(days=d.weekday())).isoformat())


# --------------------------------------------------------------------------- #
# Settings, alerts, audit
# --------------------------------------------------------------------------- #
def get_settings() -> dict:
    out = dict(SETTINGS)
    for r in db.q("SELECT k, v FROM settings WHERE k LIKE 'auto:%'"):
        k = r["k"][5:]
        if k in out:
            out[k] = json.loads(r["v"])
    return out


def set_settings(body: dict) -> dict:
    bad = [k for k in body if k not in SETTINGS]
    if bad:
        raise ValueError(f"not editable: {', '.join(bad)}")
    for k, v in body.items():
        if v is not None and (not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0):
            raise ValueError(f"{k} must be a number >= 0" + (" or null" if k == "global_daily_cap_usd" else ""))
        if v is None and k == "low_balance_usd":
            raise ValueError("low_balance_usd must be a number >= 0")
        db.ex("INSERT INTO settings (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
              (f"auto:{k}", json.dumps(v)))
    return get_settings()


def alert(level: str, text: str) -> None:
    if text not in _alerts:
        _alerts[text] = (level, _now())
        log.warning("auto alert: %s", text)


def alerts(since: int) -> list[dict]:
    return [{"level": lv, "text": t, "at": at} for t, (lv, at) in _alerts.items() if at >= since]


def audit(username: str, action: str, target, detail=None) -> None:
    db.insert("audit_log", at=_now(), username=username, action=action,
              target=None if target is None else str(target), detail=json.dumps(detail, default=str))


# --------------------------------------------------------------------------- #
# Pauses
# --------------------------------------------------------------------------- #
def norm_pause(scope: str, key) -> tuple[str, str, str]:
    """-> (scope, stored key, label). post keys are stored as url_key, label keeps the URL."""
    if scope not in SCOPES:
        raise ValueError(f"scope must be one of {', '.join(SCOPES)}")
    key = str(key if key is not None else "").strip()
    if scope == "all":
        return scope, "*", "*"
    if not key:
        raise ValueError("key is required")
    if scope == "post":
        return scope, url_key(key), key
    if scope == "panel" and key not in PROVIDERS:
        raise ValueError("unknown provider")
    return scope, key, key


def pause_view(p: dict) -> dict:
    return {"id": p["id"], "scope": p["scope"], "key": p["label"] or p["key"],
            "created_by": p["created_by"], "created_at": p["created_at"]}


def creator_keys(key) -> set[str]:
    """A creator key plus its aliases through auto_creator_map (sheet name <-> SideShift id, both ways, every name of
    that id), all lower-cased and trimmed."""
    k = str(key or "").strip().lower()
    ids = {r["c"] for r in db.q("SELECT LOWER(TRIM(creator_id)) c FROM auto_creator_map "
                                "WHERE name_key=? OR LOWER(TRIM(creator_id))=?", (k, k))}
    names = {r["name_key"] for r in db.q(f"SELECT name_key FROM auto_creator_map WHERE LOWER(TRIM(creator_id)) IN "
                                         f"({','.join('?' * len(ids))})", tuple(ids))} if ids else set()
    return {k} | ids | names


def _creator_where(col: str, key) -> tuple[str, tuple]:
    ks = sorted(creator_keys(key))
    return f"LOWER(TRIM(CAST({col} AS TEXT))) IN ({','.join('?' * len(ks))})", tuple(ks)


def _pause_set() -> set[tuple[str, str]]:
    """Creator pauses are matched case-insensitively and through the sheet name <-> SideShift id map (v2.9.2)."""
    out = set()
    for r in db.q("SELECT scope, key FROM pauses"):
        if r["scope"] == "creator":
            out |= {("creator", k) for k in creator_keys(r["key"])}
        else:
            out.add((r["scope"], r["key"]))
    for r in db.q("SELECT campaign_id, creator_id FROM auto_creators WHERE included=0"):   # v2.14: un-ticked = no orders
        out |= {("excluded", f"{r['campaign_id']}:{k}") for k in creator_keys(r["creator_id"])}
    return out


def _paused(pauses: set, plan: dict, provider: str | None = None) -> bool:
    keys = {("all", "*"), ("campaign", str(plan["campaign_id"])), ("post", plan["url_key"])}
    if plan.get("creator_id"):
        keys.add(("creator", str(plan["creator_id"]).strip().lower()))
        keys.add(("excluded", f"{plan['campaign_id']}:{str(plan['creator_id']).strip().lower()}"))
    if provider:
        keys.add(("panel", provider))
    return bool(keys & pauses)


# --------------------------------------------------------------------------- #
# Money
# --------------------------------------------------------------------------- #
def spent(since: int, campaign_id: int | None = None, simulated: bool = False, real: bool = True) -> float:
    states = [s for s, on in ((LIVE_SPEND, real), ("state = 'simulated'", simulated)) if on]
    where = f"({' OR '.join(states)}) AND created_at >= ?"
    args: list = [since]
    if campaign_id is not None:
        where += " AND campaign_id = ?"; args.append(campaign_id)
    return round(db.q1(f"SELECT COALESCE(SUM(est_cost), 0) s FROM auto_orders WHERE {where}", tuple(args))["s"], 4)


def budget_problem(camp: dict, cost: float, now: int) -> str | None:
    """Live: campaign cap (v2.12) + global cap both required; an old live campaign with only a weekly budget
    keeps running on it. A weekly budget, when set, is still honoured next to the cap.
    Practice: limits apply only when set (simulated spend counts), so the full plan cost is shown before anything is set."""
    live = camp["mode"] == "live"
    cap_c, budget = camp.get("cap_usd"), camp["weekly_budget_usd"]
    if cap_c is not None:
        s = spent(0, camp["id"], simulated=not live, real=live)
        if s + cost > cap_c:
            return f"campaign cap ${cap_c:.2f} reached (spent ${s:.2f})"
    elif budget is None and live:
        return "no campaign cap set"
    if budget is not None:
        s = spent(week_start(now), camp["id"], simulated=not live, real=live)
        if s + cost > budget:
            return f"weekly budget ${budget:.2f} reached (spent ${s:.2f})"
    cap = get_settings()["global_daily_cap_usd"]
    if cap is None:
        if live:
            return "no global daily cap set"
    else:
        s = spent(et_midnight(et_day(now)), None, simulated=not live)
        if s + cost > cap:
            return f"global daily cap ${cap:.2f} reached (spent ${s:.2f})"
    return None


# --------------------------------------------------------------------------- #
# Service pick
# --------------------------------------------------------------------------- #
def _num(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _scores() -> dict[tuple[str, str, str], int | None]:
    return {(r["provider"], r["service"], r["platform"]): r["score"] for r in score.compute(score.score_jobs())}


def _tests() -> dict[tuple[str, str], str]:
    """(provider, service) -> 'completed' | 'open' | 'failed' for auto test orders."""
    rank = {"failed": 0, "open": 1, "completed": 2}
    out: dict = {}
    for r in db.q("SELECT provider, service, state FROM auto_orders WHERE is_test=1 AND kind='views' "
                  "AND state NOT IN ('simulated','blocked')"):
        st = "completed" if r["state"] == "completed" else ("open" if r["state"] in ("placing", "placed") else "failed")
        k = (r["provider"], r["service"])
        if rank[st] > rank.get(out.get(k), -1):
            out[k] = st
    return out


async def context() -> dict:
    return {"scores": await asyncio.to_thread(_scores), "tests": await asyncio.to_thread(_tests),
            "pauses": await asyncio.to_thread(_pause_set)}


async def candidates(platform: str, ctx: dict, exclude=()) -> list[dict]:
    """Eligible views services on every unpaused panel, cheapest first. Score >= 60 or no score;
    an unscored service is a test (`test`) until one auto test order on it completed."""
    memo = ctx.setdefault("cands", {})
    mk = (platform, frozenset(exclude))
    if mk in memo:
        return memo[mk]
    out = []
    for p in PROVIDERS:
        if ("panel", p) in ctx["pauses"] or panels.down(p):
            continue
        try:
            cat = await panels.services_cached(p)
        except Exception:  # noqa: BLE001  panel down or no key: skip it
            continue
        for s in cat:
            sid = str(s["service"])
            if str(s.get("type") or "Default").strip().lower() != "default":
                continue   # Subscription / Package / Custom ... need extra fields or behave differently
            if (p, sid) in exclude or not campaigns.svc_matches(s, platform, "views") \
                    or any(w in campaigns.svc_text(s) for w in NOT_POST_VIEWS):
                continue
            rate, lo, hi = _num(s.get("rate")), int(_num(s.get("min")) or 0), int(_num(s.get("max")) or 0)
            if rate is None or rate <= 0:
                continue
            sc = ctx["scores"].get((p, sid, platform))
            if sc is not None and sc < MIN_SCORE:
                continue
            test = False
            if sc is None:
                t = ctx["tests"].get((p, sid))
                if t in ("open", "failed"):
                    continue
                test = t != "completed"
            out.append({"provider": p, "service": sid, "rate": rate, "min": max(1, lo), "max": hi, "test": test})
    out.sort(key=lambda c: (c["rate"], c["provider"], c["service"]))
    memo[mk] = out
    return out


async def _balance_ok(provider: str, cost: float) -> str | None:
    try:
        bal = await panels.balance_cached(provider)
    except Exception as e:  # noqa: BLE001
        return f"{provider_label(provider)} balance unavailable ({str(e)[:60]})"
    return None if bal >= cost else f"{provider_label(provider)} balance ${bal:.2f} < est ${cost:.4f}"


def chosen(camp: dict | None, platform: str, kind: str) -> tuple[str, str] | None:
    """(provider, service) the campaign picked for this platform + kind in the calculator, or None."""
    try:
        x = json.loads((camp or {}).get("services") or "{}").get(platform, {}).get(kind) or {}
        return (x["provider"], str(x["service"])) if x.get("provider") and x.get("service") else None
    except (ValueError, AttributeError, TypeError):
        return None


EXTRA = ("comments", "shares")   # v2.16: bought only with a chosen service AND a ratio > 0 (extra_ok)


def extra_ok(camp: dict | None, platform: str, kind: str) -> bool:
    """v2.16 hard rule: a comments / shares order exists only when the campaign chose a service for this platform
    + kind AND set its per-1k ratio above 0. Planner, executor and pick() all check it."""
    return kind in EXTRA and chosen(camp, platform, kind) is not None and (_num((camp or {}).get(f"{kind}_per_1k")) or 0) > 0


def qty_only(s: dict) -> bool:
    """Catalog type Default = quantity only (Custom Comments / Package / Subscription need extra fields)."""
    return str(s.get("type") or "Default").strip().lower() == "default"


def custom_comments(s: dict) -> bool:
    """v2.18: catalog type Custom Comments = the order carries our own lines (one comment per line)."""
    return str(s.get("type") or "").strip().lower() == "custom comments"


# v2.16.1 bug 18: rows that are not "this many comments / shares on this one post" (wrong kind, profile / page /
# story / channel targets, multi-post splits, separators). Over-rejecting is safe: it only shortens the dropdown.
EXTRA_NOT = ("follower", "visit", "threads", "like", "---", "story", "channel", "rating", "page", "profile", "latest",
             "split", "suggestion", "live stream", "live video", "minute", "note", "engagement", " + ",
             "custom", "least")


# v2.16.2: fixed packages ("[Random 5 Comments]", "[300]", "[100] [INDONESIA]"): qty may mean packages, not comments
EXTRA_PACK = re.compile(r"\[\s*(random\s+)?\d+(\s+(comments?|shares?))?\s*\]")


def extra_svc_ok(s: dict, platform: str, kind: str) -> bool:
    """A comments / shares service for this platform that takes a quantity only. Judged on the NAME alone (bug 18: a
    category such as "Instagram Shares" holds profile visits, Threads shares and separators): the name must say this
    platform only and the kind, and nothing in EXTRA_NOT (followers, visits, Threads, likes, separator rows)."""
    name = f" {s.get('name', '')} ".lower()
    plats = [p for p, hs in campaigns.PLATFORM_HINTS.items() if any(h in name for h in hs)]
    custom = kind == "comments" and custom_comments(s)   # v2.18: Custom Comments allowed for comments (not shares)
    return plats == [platform] and any(h in name for h in campaigns.TYPE_HINTS[kind]) and (qty_only(s) or custom) \
        and not any(w in name for w in EXTRA_NOT if not (custom and w == "custom")) and not EXTRA_PACK.search(name) \
        and campaigns.svc_matches(s, platform, kind)


# --------------------------------------------------------------------------- #
# v2.18 Custom comment pool (one per campaign) + per-post used lines
# --------------------------------------------------------------------------- #
USED_STATES = "('placing','placed','completed','ambiguous','cancelled','simulated')"   # lines that may be on the post


def parse_lines(text: str, csv: bool = False) -> list[str]:
    """One comment per line, trimmed, blanks dropped, exact duplicates dropped (first kept). csv: a line wrapped in
    double quotes is unwrapped ("" -> "). A line over COMMENT_MAX_LEN rejects the whole paste."""
    out, seen, long_ = [], set(), []
    for i, ln in enumerate(str(text or "").splitlines(), 1):
        t = ln.strip()
        if csv and len(t) >= 2 and t[0] == t[-1] == '"':
            t = t[1:-1].replace('""', '"').strip()
        if not t:
            continue
        if len(t) > COMMENT_MAX_LEN:
            long_.append(i)
        elif t not in seen:
            seen.add(t)
            out.append(t)
    if long_:
        raise ValueError(f"comments must be {COMMENT_MAX_LEN} characters or less: line(s) "
                         + ", ".join(map(str, long_[:10])) + (" ..." if len(long_) > 10 else ""))
    return out


def pool_view(cid: int) -> dict:
    rows = db.q("SELECT id, text, times_used FROM comment_pool WHERE campaign_id=? ORDER BY id", (cid,))
    return {"campaign_id": cid, "count": len(rows), "items": rows}


POOL_MAX, POOL_TOTAL_MAX = 5000, 20000   # review fix: lines per paste / per campaign


def pool_set(cid: int, text: str, mode: str = "add", csv: bool = False) -> dict:
    """mode add: new lines appended (already in the list = skipped). replace: the list becomes exactly these lines
    (lines kept keep their use counts)."""
    if mode not in ("add", "replace"):
        raise ValueError("mode must be add or replace")
    lines, now = parse_lines(text, csv), _now()
    if len(lines) > POOL_MAX:
        raise ValueError(f"at most {POOL_MAX} comment lines per paste")
    if mode == "add" and (db.q1("SELECT COUNT(*) n FROM comment_pool WHERE campaign_id=?", (cid,))["n"] + len(lines)
                          > POOL_TOTAL_MAX):
        raise ValueError(f"a campaign list holds at most {POOL_TOTAL_MAX} comments")
    if mode == "add" and not lines:
        raise ValueError("no comment lines (one comment per line)")
    with db._conn() as c:
        removed = 0
        if mode == "replace":
            keep = set(lines)
            have = [r["id"] for r in c.execute("SELECT id, text FROM comment_pool WHERE campaign_id=?", (cid,)).fetchall()
                    if r["text"] not in keep]
            removed = c.execute(f"DELETE FROM comment_pool WHERE id IN ({','.join('?' * len(have))})", have).rowcount \
                if have else 0
        added = sum(c.execute("INSERT OR IGNORE INTO comment_pool (campaign_id, text, created_at) VALUES (?,?,?)",
                              (cid, t, now)).rowcount for t in lines)
    return {**pool_view(cid), "added": added, "skipped": len(lines) - added, "removed": removed}


def pool_remove(cid: int, line_id: int) -> dict:
    if not db.ex("DELETE FROM comment_pool WHERE id=? AND campaign_id=?", (line_id, cid))[0]:
        raise LookupError("no such comment line")
    return pool_view(cid)


def used_texts(key: str) -> set[str]:
    """Lines that are (or may be) on this post: every recorded use + the lines of any order that may have reached the
    panel (placing / ambiguous included, so a maybe-placed order is never followed by the same lines)."""
    out = {r["text"] for r in db.q("SELECT text FROM comment_used WHERE url_key=?", (key,))}
    for r in db.q(f"SELECT comments FROM auto_orders WHERE url_key=? AND kind='comments' AND comments IS NOT NULL "
                  f"AND (state IN {USED_STATES} OR (state='retried' AND COALESCE(error,'') NOT LIKE 'panel:%'))",
                  (key,)):   # retried: only a clear panel reject ("panel: ...") means nothing was placed
        out |= set(json.loads(r["comments"]))
    return out


def pick_lines(cid: int, key: str, n: int) -> tuple[list[tuple[int, str]], int]:
    """-> (up to n (id, text) lines not yet on this post, least used first, random tie-break; pool size)."""
    rows = db.q("SELECT id, text, times_used FROM comment_pool WHERE campaign_id=?", (cid,))
    used = used_texts(key)
    cands = [r for r in rows if r["text"] not in used]
    random.shuffle(cands)
    cands.sort(key=lambda r: r["times_used"])   # stable: equal counts stay shuffled
    return [(r["id"], r["text"]) for r in cands[:max(0, n)]], len(rows)


def mark_used(cid: int, key: str, order_id: int, lines: list[tuple[int, str]]) -> None:
    """Only after the panel accepted the order (or it was simulated in Practice)."""
    now = _now()
    with db._conn() as c:
        c.executemany("INSERT INTO comment_used (campaign_id, url_key, line_id, text, order_id, at) VALUES (?,?,?,?,?,?)",
                      [(cid, key, i, t, order_id, now) for i, t in lines])
        c.execute(f"UPDATE comment_pool SET times_used = times_used + 1 WHERE id IN ({','.join('?' * len(lines))})",
                  [i for i, _ in lines])


def comments_ordered(practice: bool) -> dict[str, int]:
    """url_key -> comments already ordered (live: not failed / cancelled / blocked; Practice: simulated)."""
    st = "('simulated')" if practice else "('placing','placed','completed','ambiguous')"
    return {r["url_key"]: r["n"] for r in db.q(f"SELECT url_key, SUM(qty) n FROM auto_orders WHERE kind='comments' "
                                               f"AND state IN {st} GROUP BY url_key")}


def custom_pick(camp: dict | None, platform: str) -> dict | None:
    """The campaign's comments pick for this platform when it was saved as a Custom Comments service (prep_comments)."""
    try:
        x = json.loads((camp or {}).get("services") or "{}").get(platform, {}).get("comments") or {}
    except (ValueError, AttributeError, TypeError):
        return None
    return x if x.get("custom") else None


async def prep_comments(body: dict, cur: dict | None = None) -> None:
    """v2.18, before campaign_fields on create / edit: tag each comments pick with its catalog type (custom, min, max;
    the planner reads it) and default comments_per_1k to 5 when a Custom Comments service is picked and no rate is
    typed (empty on create, or empty / never set on edit). A client-sent tag is always replaced."""
    sv, custom = body.get("services"), False
    for by in (sv.values() if isinstance(sv, dict) else ()):
        x = by.get("comments") if isinstance(by, dict) else None
        if not isinstance(x, dict):
            continue
        for k in ("custom", "min", "max"):
            x.pop(k, None)
        try:
            info = await panels.service_info(x["provider"], str(x["service"]))
        except Exception:  # noqa: BLE001  bad pick / catalog down: campaign_fields + services_problem answer it
            info = None
        if info and custom_comments(info):
            x.update(custom=True, min=int(_num(info.get("min")) or 0), max=int(_num(info.get("max")) or 0))
            custom = True
    had = bool(cur) and any(custom_pick(cur, pl) for pl in PLATFORMS)
    if "services" not in body and cur:   # edit without services: the stored picks decide
        custom = had
    # default 5 only on create, on an explicit empty rate, or when an edit newly picks Custom Comments. Never on an
    # unrelated PATCH (Go Live, rename): a rate the user set to 0 to turn comments off stays 0 (review fix).
    if custom and body.get("comments_per_1k") in (None, "") and ("comments_per_1k" in body or not cur or not had):
        body["comments_per_1k"] = COMMENT_RATE_DEFAULT


async def pick(plan: dict, kind: str, qty: int, ctx: dict, live: bool, exclude=(),
               provider: str | None = None, camp: dict | None = None) -> tuple[dict | None, str | None]:
    """-> ({provider, service, qty, est_cost, is_test}, None) or (None, why)."""
    if kind in EXTRA:   # v2.16: only the campaign's own pick, re-checked against the live catalog
        if not extra_ok(camp, plan["platform"], kind):
            return None, f"no {kind} service or ratio set"
        p, sid = chosen(camp, plan["platform"], kind)
        if ("panel", p) in ctx["pauses"]:
            return None, f"{provider_label(p)} is paused"
        try:
            info = await panels.service_info(p, sid)
        except Exception:  # noqa: BLE001  catalog down: never buy unchecked
            info = None
        if not info or not extra_svc_ok(info, plan["platform"], kind) or not _num(info.get("rate")):
            return None, (f"{kind} service {sid} is not a quantity-only {kind} service"
                          + (" or Custom Comments service" if kind == "comments" else "") + f" for {plan['platform']} now")
        if kind == "comments" and custom_comments(info):   # v2.18: our own lines, quantity = number of lines
            lo, hi = max(COMMENT_MIN, int(_num(info.get("min")) or 0)), int(_num(info.get("max")) or 0) or int(qty)
            if qty < lo:
                return None, f"{qty} comments is below the service minimum {lo}"
            lines, size = await asyncio.to_thread(pick_lines, plan["campaign_id"], plan["url_key"], min(int(qty), hi))
            if len(lines) < lo:
                return None, ("comment list is empty" if not size else
                              f"comment list has {len(lines)} unused lines for this video, the service minimum is {lo}")
            n = len(lines)
            cost = round(n / 1000 * _num(info["rate"]), 4)
            why = await _balance_ok(p, cost) if live else None
            return (None, why) if why else ({"provider": p, "service": sid, "qty": n, "est_cost": cost, "is_test": 0,
                                             "comments": json.dumps([t for _, t in lines]), "_lines": lines}, None)
        floor = await engine.likes_floor(p, sid)   # never raised to the floor: buy what the ratio says or nothing
        if qty < floor:
            return None, f"{qty} {kind} is below the service minimum {floor}"
        n = int(qty)
        cost = round(n / 1000 * _num(info["rate"]), 4)
        why = await _balance_ok(p, cost) if live else None
        return (None, why) if why else ({"provider": p, "service": sid, "qty": n, "est_cost": cost, "is_test": 0}, None)
    if kind == "likes":
        if ("panel", provider) in ctx["pauses"]:
            return None, f"{provider_label(provider)} is paused"
        want = chosen(camp, plan["platform"], "likes")   # v2.12: the campaign's pick when it is on this panel
        svc = service_for(provider, plan["platform"], "likes")
        if want and want[0] == provider:   # only a real likes service for this platform
            try:
                info = await panels.service_info(provider, want[1])
            except Exception:  # noqa: BLE001  catalog down: panel default
                info = None
            if info and campaigns.svc_matches(info, plan["platform"], "likes"):
                svc = want[1]
        n = await engine.likes_qty(provider, svc, qty)
        cost = round(n / 1000 * await campaigns.live_rate(provider, plan["platform"], "likes", svc), 4)
        why = await _balance_ok(provider, cost) if live else None
        return (None, why) if why else ({"provider": provider, "service": svc, "qty": n, "est_cost": cost,
                                         "is_test": 0}, None)
    fits = [c for c in await candidates(plan["platform"], ctx, exclude) if c["min"] <= qty]
    if not fits:
        return None, "no eligible views service for this qty"
    want = chosen(camp, plan["platform"], "views")   # v2.12: the campaign's pick first, if it is eligible
    fits.sort(key=lambda c: (c["provider"], c["service"]) != want)
    low = []
    for c in fits:
        n = c["min"] if c["test"] else min(qty, c["max"] or qty)
        cost = round(n / 1000 * c["rate"], 4)
        why = await _balance_ok(c["provider"], cost) if live else None
        if why:
            low.append(why)
            continue
        return {"provider": c["provider"], "service": c["service"], "qty": n, "est_cost": cost,
                "is_test": int(c["test"])}, None
    alert("warn", "Automation: no panel has enough balance for an order")
    return None, "no panel with enough balance: " + "; ".join(low)[:200]


# --------------------------------------------------------------------------- #
# Orders
# --------------------------------------------------------------------------- #
def get_order(oid: int) -> dict | None:
    return db.q1("SELECT * FROM auto_orders WHERE id=?", (oid,))


def _save(f: dict) -> dict:
    """Insert an order row, or reuse this slot's `blocked` row (so a blocked order is one row, not one per tick)."""
    if f.get("retry_of"):
        old = db.q1("SELECT id FROM auto_orders WHERE retry_of=? AND state='blocked'", (f["retry_of"],))
    else:
        old = db.q1("SELECT id FROM auto_orders WHERE plan_id=? AND day=? AND kind=? AND attempt=0 "
                    "AND retry_of IS NULL AND state='blocked'", (f["plan_id"], f["day"], f["kind"]))
    if old:
        db.update("auto_orders", old["id"], **{**f, "created_at": _now()})
        return get_order(old["id"])
    return get_order(db.insert("auto_orders", **{**f, "created_at": _now()}))


async def place(plan: dict, camp: dict, kind: str, qty: int, ctx: dict, *, attempt: int = 0,
                retry_of: int | None = None, exclude=(), provider: str | None = None) -> dict:
    """Pick, check balance + budget, then simulate (practice) or place (live). Returns the order row:
    simulated | placed | failed (panel rejected) | ambiguous (maybe placed) | blocked (nothing sent)."""
    live = camp["mode"] == "live"
    now = _now()
    f = {"plan_id": plan["plan_id"], "campaign_id": plan["campaign_id"], "url_key": plan["url_key"],
         "day": et_day(now), "kind": kind, "attempt": attempt, "retry_of": retry_of, "error": None}
    if detect_platform(plan["post_url"]) != plan["platform"]:
        return await asyncio.to_thread(_save, {**f, "state": "blocked", "qty": qty,
                                               "error": f"post URL is not a {plan['platform']} link"})
    got, why = await pick(plan, kind, qty, ctx, live, exclude, provider, camp)
    if not got:
        return await asyncio.to_thread(_save, {**f, "state": "blocked", "error": why, "qty": qty})
    lines = got.pop("_lines", None)   # v2.18 custom comments: marked used only once accepted / simulated
    f.update(got)
    if got["is_test"]:   # one test per service at a time, also within this tick
        ctx["tests"][(got["provider"], got["service"])] = "open"
        ctx.pop("cands", None)
    why = await asyncio.to_thread(budget_problem, camp, got["est_cost"], now)
    if why:
        alert("warn", f"Automation: {camp['name']}: {why.split(' (')[0]}")
        return await asyncio.to_thread(_save, {**f, "state": "blocked", "error": why})
    if not live:
        row = await asyncio.to_thread(_save, {**f, "state": "simulated"})
        if lines:
            await asyncio.to_thread(mark_used, plan["campaign_id"], plan["url_key"], row["id"], lines)
        return row
    if _paused(await asyncio.to_thread(_pause_set), plan, got["provider"]):   # paused during this tick
        return await asyncio.to_thread(_save, {**f, "state": "blocked", "error": "paused"})
    row = await asyncio.to_thread(_save, {**f, "state": "placing"})   # committed BEFORE the panel call
    p, svc, n = got["provider"], got["service"], got["qty"]
    try:
        if kind == "views":
            res = await engine.place_order(p, plan["platform"], plan["post_url"], n, 0,
                                           created_by=f"auto:{plan['campaign_id']}", source="auto",
                                           views_service=svc)
            job_id, order_id = res["job_id"], res["views_order_id"]
        else:
            res = await client_for(p).add(svc, plan["post_url"], n,
                                          **({"comments": "\n".join(t for _, t in lines)} if lines else {}))
            order_id = str(res.get("order", "") or "").strip() if isinstance(res, dict) else ""
            if not order_id:
                raise RuntimeError(f"bad panel response, no order id: {str(res)[:120]}")
            job_id = await asyncio.to_thread(
                db.create_job, kind="eng", provider=p, platform=plan["platform"], link=plan["post_url"],
                likes_target=n, likes_service=svc, likes_order_id=order_id, likes_ordered=n, state="done",
                created_at=_now(), next_poll_at=None, created_by=f"auto:{plan['campaign_id']}",
                source="auto") if kind == "likes" else None   # v2.16: comments / shares: auto_orders row only
    except PanelError as e:   # the panel answered no: nothing placed
        alert("warn", f"Automation: {provider_label(p)} rejected an order: {e!s}"[:200])
        await asyncio.to_thread(db.update, "auto_orders", row["id"], state="failed", error=f"panel: {e!s}"[:300])
    except Exception as e:  # noqa: BLE001  may exist on the panel: count it, never auto re-send
        panels.balance_spend(p, got["est_cost"])
        panels.mark_down(p, f"{type(e).__name__}: {e}")
        alert("crit", f"Automation: order #{row['id']} result unclear, check {provider_label(p)} before resending")
        await asyncio.to_thread(db.update, "auto_orders", row["id"], state="ambiguous",
                                error=f"{type(e).__name__}: {e!s}"[:240] + " (check panel before resend)")
    else:
        panels.balance_spend(p, got["est_cost"])
        await asyncio.to_thread(db.update, "auto_orders", row["id"], state="placed", job_id=job_id,
                                order_id=str(order_id))
        if lines:
            await asyncio.to_thread(mark_used, plan["campaign_id"], plan["url_key"], row["id"], lines)
    return await asyncio.to_thread(get_order, row["id"])


def missing_qty(o: dict) -> int:
    """Views still owed by a failed/cancelled/ambiguous order: the panel's remaining, else the full qty."""
    j = db.get_job(o["job_id"]) if o["job_id"] else None
    rem = j.get("views_remaining") if j else None
    return int(rem) if rem is not None and 0 < rem <= o["qty"] else int(o["qty"])


# --------------------------------------------------------------------------- #
# Executor
# --------------------------------------------------------------------------- #
def _lease() -> bool:
    now = _now()
    n, _ = db.ex("INSERT INTO settings (k, v) VALUES ('auto:lock', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v "
                 "WHERE settings.v LIKE ? OR CAST(substr(settings.v, instr(settings.v, ':') + 1) AS INTEGER) < ?",
                 (f"{_owner}:{now}", f"{_owner}:%", now - LEASE_SEC))
    return n == 1


def _release() -> None:
    db.ex("DELETE FROM settings WHERE k='auto:lock' AND v LIKE ?", (f"{_owner}:%",))


async def tick() -> None:
    """Scheduler entry (every 10 min). Does nothing unless AUTO_ENABLED=1."""
    if enabled():
        await run_tick()


async def run_tick() -> None:
    async with _lock:
        if not await asyncio.to_thread(_lease):
            log.info("auto tick skipped: another executor holds the lease")
            return
        try:
            await asyncio.to_thread(db.ex, "UPDATE auto_orders SET state='ambiguous', "
                                           "error='interrupted while placing (check panel before resend)' "
                                           "WHERE state='placing'")
            await cancel_stale(await asyncio.to_thread(sync_states))
            ctx = await context()
            await _retries(ctx)
            await _views(ctx)
            await _likes(ctx)
            for kind in EXTRA:
                await _likes(ctx, kind)
            await _custom_comments(ctx)
            low = (await asyncio.to_thread(get_settings))["low_balance_usd"]
            for p in PROVIDERS:
                b = panels.balance_peek(p)
                if b is not None and b < low:
                    alert("crit" if b < 1 else "warn", f"{provider_label(p)} balance low: ${b:.2f}")
        finally:
            await asyncio.to_thread(_release)


def sync_states() -> list[dict]:
    """Placed views orders follow their job (status sweep): completed, or a clear failure.
    Returns the stale ones (open > 24 h after a status check): they are cancelled on the panel first."""
    stale = []
    now = _now()
    rows = db.q("SELECT o.id, o.error, j.views_status, j.completed_at, j.status_checked_at, j.created_at "
                "FROM auto_orders o JOIN jobs j ON j.id = o.job_id WHERE o.state='placed' AND o.kind='views'")
    for r in rows:
        st = (r["views_status"] or "").lower()
        if r["completed_at"]:
            db.ex("UPDATE auto_orders SET state='completed' WHERE id=? AND state='placed'", (r["id"],))
        elif st in score.FAILED:
            db.ex("UPDATE auto_orders SET state='failed', error=? WHERE id=? AND state='placed'",
                  (f"panel status {r['views_status']}", r["id"]))
        elif r["status_checked_at"] and now - r["created_at"] > score.STALE_HOURS * 3600 \
                and not (r["error"] or "").startswith("stale"):
            stale.append(get_order(r["id"]))
    return stale


async def cancel_stale(rows: list[dict]) -> None:
    """Stale order: re-send only once the panel accepted the cancel; refused = stays placed + alert."""
    for o in rows:
        done, refused = await cancel_orders([o])
        if done:
            await asyncio.to_thread(db.ex, "UPDATE auto_orders SET state='failed', error=? WHERE id=? AND state='cancelled'",
                                    (f"open over {score.STALE_HOURS} h, cancelled on the panel", o["id"]))
        else:
            why = refused[0]["reason"] if refused else "no answer"
            await asyncio.to_thread(db.update, "auto_orders", o["id"], error=f"stale, cancel refused: {why}"[:300])
            alert("crit", f"Automation: order #{o['id']} open over {score.STALE_HOURS} h and the panel refused to cancel; "
                          f"not re-sent, check {provider_label(o['provider'])}")


def _plan(plan_id: str) -> dict | None:
    return db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (plan_id,))


def _camp(cid: int) -> dict | None:
    return db.q1("SELECT * FROM auto_campaigns WHERE id=?", (cid,))


async def _retries(ctx: dict) -> None:
    """One re-send of the missing qty for a clear failure, on the next best service; a failed re-send stops + alerts."""
    for o in await asyncio.to_thread(
            db.q, "SELECT * FROM auto_orders o WHERE kind='views' AND state='failed' AND attempt=1 "
                  "AND COALESCE(error,'') NOT LIKE '%stopped%'"):
        alert("crit", f"Automation: order #{o['id']} failed after its one re-send; stopped ({o['error']})")
        await asyncio.to_thread(db.update, "auto_orders", o["id"], error=f"{o['error']}; stopped"[:300])
    rows = await asyncio.to_thread(
        db.q, "SELECT * FROM auto_orders o WHERE kind='views' AND state='failed' AND attempt=0 "
              "AND COALESCE(error,'') NOT LIKE '%not re-sent%' AND created_at >= ? "
              "AND NOT EXISTS (SELECT 1 FROM auto_orders r WHERE r.retry_of=o.id AND r.state<>'blocked')",
        (_now() - RETRY_DAYS * 86400,))
    for o in rows:
        plan, camp = await asyncio.to_thread(_plan, o["plan_id"]), await asyncio.to_thread(_camp, o["campaign_id"])
        if not plan or not camp or _paused(ctx["pauses"], plan):
            continue
        need = await asyncio.to_thread(missing_qty, o)
        r = await place(plan, camp, "views", need, ctx, attempt=1, retry_of=o["id"],
                        exclude={(o["provider"], o["service"])})
        if r["state"] != "blocked":
            await asyncio.to_thread(db.update, "auto_orders", o["id"], state="retried")
        elif r["error"] == "no eligible views service for this qty":
            await asyncio.to_thread(db.ex, "DELETE FROM auto_orders WHERE id=?", (r["id"],))
            await asyncio.to_thread(db.update, "auto_orders", o["id"],
                                    error=f"{o['error']}; not re-sent: no other service fits {need}"[:300])
            alert("crit", f"Automation: order #{o['id']} failed; no other service fits {need} views, not re-sent")


def _inflight(key: str, campaign_id: int) -> int:
    """Views still owed by OPEN manual jobs and other automation campaigns' jobs on this post."""
    fails = ",".join(f"'{s}'" for s in score.FAILED)
    return db.q1(f"SELECT COALESCE(SUM(COALESCE(views_remaining, views_target)), 0) n FROM jobs WHERE url_key=? "
                 f"AND COALESCE(source,'manual') <> 'campaign' "
                 f"AND NOT (COALESCE(source,'') = 'auto' AND COALESCE(created_by,'') = ?) AND state='running' "
                 f"AND completed_at IS NULL AND views_order_id IS NOT NULL AND views_order_id <> '' "
                 f"AND LOWER(COALESCE(views_status,'')) NOT IN ({fails})", (key, f"auto:{campaign_id}"))["n"]


def _open_queue(key: str) -> bool:
    fails = ",".join(f"'{s}'" for s in score.FAILED)
    return any(db.q1(f"SELECT q.id FROM {t} q LEFT JOIN jobs j ON j.id = q.job_id WHERE q.url_key=? AND "
                     f"(q.state='placing' OR (q.state='placed' AND j.completed_at IS NULL "
                     f"AND COALESCE(j.state,'running')='running' "
                     f"AND LOWER(COALESCE(j.views_status,'')) NOT IN ({fails}))) LIMIT 1", (key,))
               for t in ("queue", "rebuys"))   # v2.11: an open Buy again order counts too


def _consume(rows: list[dict], n: int) -> None:
    """Take n views off the post's rows, oldest first; the rest (`carry`, views still owed) carries to tomorrow."""
    for r in rows:
        avail = r["views_add"] if r["carry"] is None else r["carry"]
        take = min(avail, max(0, n))
        n -= take
        db.update("plan_rows", r["id"], carry=avail - take)


async def _views(ctx: dict) -> None:
    today = et_day()
    since = (dt.date.fromisoformat(today) - dt.timedelta(days=CARRY_DAYS)).isoformat()
    posts = await asyncio.to_thread(
        db.q, "SELECT DISTINCT campaign_id, url_key FROM plan_rows WHERE day=? OR (day>=? AND day<? AND carry>0)",
        (today, since, today))
    for p in posts:
        camp = await asyncio.to_thread(_camp, p["campaign_id"])
        rows = await asyncio.to_thread(
            db.q, "SELECT * FROM plan_rows WHERE campaign_id=? AND url_key=? "
                  "AND (day=? OR (day>=? AND day<? AND carry>0)) ORDER BY day, id",
            (p["campaign_id"], p["url_key"], today, since, today))
        if not camp or not rows:
            continue
        plan = rows[-1]   # today's row, else the newest carried one
        if _paused(ctx["pauses"], plan):
            await asyncio.to_thread(db.ex, "UPDATE plan_rows SET carry=0 WHERE campaign_id=? AND url_key=? "
                                           "AND carry>0", (camp["id"], plan["url_key"]))   # never carry across a pause
            continue
        if await asyncio.to_thread(db.q1, "SELECT id FROM auto_orders WHERE url_key=? AND day=? "
                                          "AND kind='views' AND attempt=0 AND state<>'blocked'",
                                   (plan["url_key"], today)):
            continue   # one views order per post per day, across campaigns
        if await asyncio.to_thread(_open_queue, plan["url_key"]):
            await asyncio.to_thread(_save, {"plan_id": plan["plan_id"], "campaign_id": camp["id"],
                                            "url_key": plan["url_key"], "day": today, "kind": "views",
                                            "attempt": 0, "retry_of": None, "qty": 0, "state": "blocked",
                                            "error": "post has an open campaign-queue order"})
            continue
        want = sum(r["views_add"] if r["carry"] is None else r["carry"] for r in rows)
        inflight = min(want, await asyncio.to_thread(_inflight, plan["url_key"], camp["id"]))
        qty = want - inflight
        if qty <= 0:
            continue
        cands = await candidates(plan["platform"], ctx)
        if cands and qty < min(c["min"] for c in cands):
            # below every service minimum: carry (today's row is marked so tomorrow picks it up)
            if plan["day"] == today and plan["carry"] is None:
                await asyncio.to_thread(db.update, "plan_rows", plan["id"], carry=plan["views_add"])
            continue
        o = await place(plan, camp, "views", qty, ctx)
        if o["state"] != "blocked":
            await asyncio.to_thread(_consume, rows, o["qty"] + inflight)


def _delivered(plan_id: str) -> tuple[int, int]:
    """(views delivered, first order qty) over a plan row's views orders."""
    tot = first = 0
    for o in db.q("SELECT o.state, o.qty, o.attempt, j.views_remaining, j.completed_at FROM auto_orders o "
                  "LEFT JOIN jobs j ON j.id = o.job_id WHERE o.plan_id=? AND o.kind='views' "
                  "AND o.state IN ('simulated','completed','failed','retried','cancelled')", (plan_id,)):
        if o["attempt"] == 0:
            first = max(first, o["qty"])
        if o["state"] in ("simulated", "completed") or o["completed_at"]:
            tot += o["qty"]
        elif o["views_remaining"] is not None:
            tot += max(0, o["qty"] - o["views_remaining"])
    return tot, first


async def _likes_for(v: dict, plan: dict, camp: dict, ctx: dict) -> dict | None:
    """Likes = likes_add x delivered / ordered views (ordered = max(first order qty, planned views)),
    raised to the panel's likes floor. Same panel as the views order."""
    if not service_for(v["provider"], plan["platform"], "likes"):
        return None   # no likes service on this panel: views only
    got, first = await asyncio.to_thread(_delivered, plan["plan_id"])
    want = round(plan["likes_add"] * min(1.0, got / max(1, first, plan["views_add"])))
    if want <= 0:
        return None
    return await place(plan, camp, "likes", want, ctx, provider=v["provider"])


async def _extra_for(kind: str, plan: dict, camp: dict, ctx: dict) -> dict | None:
    """v2.16 comments / shares: same scaling as likes, on the campaign's chosen service. Hard rule: nothing without
    a chosen service AND a ratio > 0 (extra_ok), whatever the plan row says."""
    if not extra_ok(camp, plan["platform"], kind):
        return None
    if kind == "comments" and await _is_custom(camp, plan["platform"]):
        return None   # v2.18: Custom Comments follow the views on the post (_custom_comments), not the bought views
    got, first = await asyncio.to_thread(_delivered, plan["plan_id"])
    want = round(plan[f"{kind}_add"] * min(1.0, got / max(1, first, plan["views_add"])))
    if want <= 0 or want < await engine.likes_floor(*chosen(camp, plan["platform"], kind)):
        return None   # below the service minimum: not bought (never raised), no blocked row every tick
    return await place(plan, camp, kind, want, ctx)


async def _is_custom(camp: dict, platform: str) -> bool | None:
    """Live catalog says the campaign's comments service is Custom Comments (None = catalog unreadable)."""
    try:
        info = await panels.service_info(*chosen(camp, platform, "comments"))
    except Exception:  # noqa: BLE001
        return None
    return bool(info) and custom_comments(info)


def _post_live(plan: dict) -> bool:
    """No comments on a deleted / link-down post or a cancelled creator's post (un-ticked = a pause, see _paused)."""
    p = db.q1("SELECT status FROM auto_posts WHERE campaign_id=? AND url_key=?", (plan["campaign_id"], plan["url_key"]))
    gone = db.q1("SELECT 1 FROM auto_creators WHERE campaign_id=? AND creator_id=? AND cancelled=1",
                 (plan["campaign_id"], plan["creator_id"])) if plan.get("creator_id") else None
    return not gone and (p is None or p["status"] == "active")


async def _custom_comments(ctx: dict) -> None:
    """v2.18: today's plan rows with comments owed (planner: views on the post x rate / 1000 - already ordered) on a
    Custom Comments service: one order per row, lines from the campaign's pool. No views order needed first."""
    rows = await asyncio.to_thread(
        db.q, "SELECT * FROM plan_rows p WHERE p.day=? AND p.comments_add > 0 AND NOT EXISTS (SELECT 1 FROM auto_orders l "
              "WHERE l.plan_id=p.plan_id AND l.kind='comments' AND l.state<>'blocked') ORDER BY p.id", (et_day(),))
    for plan in rows:
        camp = await asyncio.to_thread(_camp, plan["campaign_id"])
        if not camp or not extra_ok(camp, plan["platform"], "comments") or not await _is_custom(camp, plan["platform"]):
            continue   # off, or quantity-only (the v2.16 path), or catalog unreadable (never buy unchecked)
        if _paused(ctx["pauses"], plan, chosen(camp, plan["platform"], "comments")[0]) \
                or not await asyncio.to_thread(_post_live, plan):
            continue
        await place(plan, camp, "comments", await asyncio.to_thread(_comments_left, plan, camp), ctx)


def _comments_left(plan: dict, camp: dict) -> int:
    """comments_add minus what OTHER plan rows (the same post in another campaign) ordered after this campaign's
    planner run computed it (review fix: never over-order one post across campaigns)."""
    r = db.q1("SELECT v FROM settings WHERE k=?", (f"planner:run:{camp['id']}",))
    at = (json.loads(r["v"]).get("at") or 0) if r else 0
    st = "('simulated')" if camp["mode"] != "live" else "('placing','placed','completed','ambiguous')"
    got = db.q1(f"SELECT COALESCE(SUM(qty),0) n FROM auto_orders WHERE kind='comments' AND url_key=? AND plan_id<>? "
                f"AND state IN {st} AND created_at >= ?", (plan["url_key"], plan["plan_id"], at))["n"]
    return max(0, plan["comments_add"] - got)


async def _likes(ctx: dict, kind: str = "likes") -> None:
    """Likes (and v2.16 comments / shares) follow a completed / simulated views order, once per plan row."""
    col = {"likes": "likes_add", "comments": "comments_add", "shares": "shares_add"}[kind]
    rows = await asyncio.to_thread(
        db.q, "SELECT o.* FROM auto_orders o JOIN plan_rows p ON p.plan_id = o.plan_id "
              f"WHERE o.kind='views' AND o.state IN ('completed','simulated') AND p.{col} > 0 AND NOT EXISTS "
              "(SELECT 1 FROM auto_orders l WHERE l.plan_id=o.plan_id AND l.kind=? AND l.state<>'blocked') "
              "ORDER BY o.id", (kind,))
    done = set()
    for v in rows:
        if v["plan_id"] in done:
            continue
        done.add(v["plan_id"])
        plan, camp = await asyncio.to_thread(_plan, v["plan_id"]), await asyncio.to_thread(_camp, v["campaign_id"])
        if v["state"] == "simulated" and camp and camp["mode"] == "live":
            continue   # practice views never become real likes
        if kind != "likes":
            if plan and camp and not _paused(ctx["pauses"], plan, (chosen(camp, plan["platform"], kind) or [None])[0]):
                await _extra_for(kind, plan, camp, ctx)
        elif plan and camp and not _paused(ctx["pauses"], plan, v["provider"]):
            await _likes_for(v, plan, camp, ctx)


# --------------------------------------------------------------------------- #
# Controls (called by the routes in main.py)
# --------------------------------------------------------------------------- #
async def resend(oid: int) -> dict:
    """Manual re-send of the missing qty now (same pick + budget checks). attempt 1, retry_of = oid."""
    async with _lock:
        o = await asyncio.to_thread(get_order, oid)
        if not o:
            raise LookupError("no such order")
        if o["state"] not in ("failed", "ambiguous", "cancelled"):
            raise ValueError(f"order is {o['state']}; only failed, ambiguous or cancelled can be resent")
        plan, camp = await asyncio.to_thread(_plan, o["plan_id"]), await asyncio.to_thread(_camp, o["campaign_id"])
        if not plan or not camp:
            raise ValueError("plan row or campaign is gone")
        ctx = await context()
        need = await asyncio.to_thread(missing_qty, o) if o["kind"] == "views" else o["qty"]
        r = await place(plan, camp, o["kind"], need, ctx, attempt=1, retry_of=oid,
                        provider=o["provider"] if o["kind"] == "likes" else None)
        if r["state"] != "blocked":
            await asyncio.to_thread(db.update, "auto_orders", oid, state="retried")
        return r


async def cancel_orders(rows: list[dict]) -> tuple[list[int], list[dict]]:
    """Panel cancel per provider. Accepted -> state cancelled; anything else is refused with the panel's reason."""
    done, refused = [], []
    by: dict[str, list[dict]] = {}
    for o in rows:
        if o["state"] != "placed" or not o["order_id"]:
            refused.append({"id": o["id"], "reason": f"order is {o['state']}, not running on the panel"})
        else:
            by.setdefault(o["provider"], []).append(o)
    for p, os_ in by.items():
        try:
            res = await client_for(p).cancel([o["order_id"] for o in os_])
        except Exception as e:  # noqa: BLE001
            res = {o["order_id"]: f"panel error: {e!s}"[:160] for o in os_}
        for o in os_:
            why = res.get(str(o["order_id"]), "panel gave no answer for this order")
            if why is None:
                await asyncio.to_thread(db.ex, "UPDATE auto_orders SET state='cancelled' WHERE id=? AND state='placed'",
                                        (o["id"],))
                done.append(o["id"])
            else:
                refused.append({"id": o["id"], "reason": why})
    return done, refused


def drop_carry(scope: str, key: str) -> None:
    """A pause drops the views carried by every plan row in its scope (never carried across a pause)."""
    col = {"all": "1", "campaign": "campaign_id", "creator": "creator_id", "post": "url_key"}.get(scope)
    if scope == "creator":
        w, a = _creator_where(col, key)
        db.ex(f"UPDATE plan_rows SET carry=0 WHERE carry>0 AND {w}", a)
    elif col:
        db.ex(f"UPDATE plan_rows SET carry=0 WHERE carry>0 AND " + ("1=1" if scope == "all" else f"CAST({col} AS TEXT) = ?"),
              () if scope == "all" else (key,))


def orders_in_scope(scope: str, key: str) -> list[dict]:
    col = {"all": "1", "campaign": "o.campaign_id", "creator": "p.creator_id", "post": "o.url_key",
           "panel": "o.provider"}[scope]
    where, args = ("1=1", ()) if scope == "all" else _creator_where(col, key) if scope == "creator" \
        else (f"CAST({col} AS TEXT) = ?", (key,))
    return db.q(f"SELECT o.* FROM auto_orders o LEFT JOIN plan_rows p ON p.plan_id = o.plan_id "
                f"WHERE o.state IN ('placed','placing') AND {where}", args)


def insert_plan(rows: list[dict]) -> tuple[int, int]:
    """Validate every row first, then insert all in one transaction (all or nothing). Duplicate plan_id = ignored."""
    camps = {r["id"] for r in db.q("SELECT id FROM auto_campaigns")}

    def opt_int(v):
        return None if v in (None, "") else int(v)
    vals, now = [], _now()
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            raise ValueError(f"rows[{i}]: must be an object")
        miss = [k for k in ("plan_id", "campaign_id", "post_url", "platform", "window_start", "window_end")
                if r.get(k) in (None, "")]
        if miss:
            raise ValueError(f"rows[{i}]: missing {', '.join(miss)}")
        try:
            cid, ws, we = int(r["campaign_id"]), int(r["window_start"]), int(r["window_end"])
            va, la = int(r.get("views_add") or 0), int(r.get("likes_add") or 0)
            ca, sa = int(r.get("comments_add") or 0), int(r.get("shares_add") or 0)   # v2.16
            ttv, prio = opt_int(r.get("target_total_views")), opt_int(r.get("priority"))
        except (TypeError, ValueError):
            raise ValueError(f"rows[{i}]: campaign_id, window_start, window_end, views_add, likes_add, "
                             f"target_total_views and priority must be integers") from None
        if cid not in camps:
            raise ValueError(f"rows[{i}]: unknown campaign_id {r['campaign_id']}")
        if r["platform"] not in PLATFORMS:
            raise ValueError(f"rows[{i}]: platform must be one of {', '.join(PLATFORMS)}")
        if detect_platform(str(r["post_url"])) != r["platform"]:
            raise ValueError(f"rows[{i}]: post_url is not a {r['platform']} link")
        if r.get("reason") not in (None, "base", "viral", "decay"):
            raise ValueError(f"rows[{i}]: reason must be base, viral or decay")
        if min(va, la, ca, sa) < 0 or we < ws:
            raise ValueError(f"rows[{i}]: views_add, likes_add, comments_add and shares_add must be >= 0 "
                             f"and window_end >= window_start")
        for k in ("creator_id", "account_handle"):
            if r.get(k) is not None and not isinstance(r[k], (str, int)):
                raise ValueError(f"rows[{i}]: {k} must be a string")
        vals.append((str(r["plan_id"]), cid, None if r.get("creator_id") is None else str(r["creator_id"]),
                     str(r["post_url"]), url_key(str(r["post_url"])), r["platform"],
                     None if r.get("account_handle") is None else str(r["account_handle"]),
                     ws, we, et_day(ws), va, la, ttv, r.get("reason") or "base", prio, now, ca, sa))
    n = 0
    with db._conn() as c:   # one transaction: an error rolls back every row
        for v in vals:
            n += c.execute("INSERT OR IGNORE INTO plan_rows (plan_id, campaign_id, creator_id, post_url, url_key, "
                           "platform, account_handle, window_start, window_end, day, views_add, likes_add, "
                           "target_total_views, reason, priority, created_at, comments_add, shares_add) "
                           "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           v).rowcount
    return n, len(vals) - n


def in_automation(key: str) -> bool:
    """Old campaign-queue placement skips posts planned in the last 14 days by a LIVE campaign while the executor is on."""
    return enabled() and bool(db.q1(
        "SELECT p.id FROM plan_rows p JOIN auto_campaigns c ON c.id = p.campaign_id "
        "WHERE p.url_key=? AND p.window_end >= ? AND c.mode='live' LIMIT 1", (key, _now() - 14 * 86400)))


# --------------------------------------------------------------------------- #
# Views (API shapes, see API-CONTRACT.md v2.7)
# --------------------------------------------------------------------------- #
CAMP_FIELDS = ("name", "weekly_budget_usd", "mode", "views_goal", "start_date", "end_date", "creators_planned",
               "posts_per_day", "platforms", "ramp_weeks", "check_times", "slot_deadlines", "source",
               "source_ref", "approved", "cap_usd", "posts_per_creator", "platform_split", "margin_pct", "services",
               "comments_per_1k", "shares_per_1k")
_JSON_FIELDS = ("platforms", "check_times", "slot_deadlines", "posts_per_creator", "platform_split", "services")
KINDS = ("views", "likes", "comments", "shares")
DELIVERED = ("CASE WHEN o.state IN ('simulated','completed') OR j.completed_at IS NOT NULL THEN o.qty "
             "WHEN j.views_remaining IS NOT NULL THEN MAX(0, o.qty - j.views_remaining) ELSE 0 END")
ORDERED_STATES = "('simulated','placing','placed','completed','ambiguous','failed','retried','cancelled')"


MONEY_MAX = 1e7   # v2.12: sanity ceiling for caps / budgets / counts (inf or 1e18 would mean no cap)


def campaign_fields(body: dict, create: bool, cur: dict | None = None) -> dict:
    """cur: the stored campaign on a patch (start <= end is checked against its other date)."""
    bad = [k for k in body if k not in CAMP_FIELDS]
    if bad:
        raise ValueError(f"not editable: {', '.join(bad)}")
    f = dict(body)
    if create and not str(f.get("name") or "").strip():
        raise ValueError("name is required")
    if "name" in f and not str(f["name"] or "").strip():
        raise ValueError("name is required")
    if create and f.get("mode", "practice") != "practice":
        raise ValueError("a new campaign starts in practice mode")
    if "mode" in f and f["mode"] not in ("practice", "live"):
        raise ValueError("mode must be practice or live")
    def money(v):
        return v is None or (isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
                             and 0 <= v <= MONEY_MAX)
    for k in ("weekly_budget_usd", "cap_usd"):
        if not money(f.get(k)):
            raise ValueError(f"{k} must be a number >= 0 or null")
    for k in ("comments_per_1k", "shares_per_1k"):   # v2.16: per 1k bought views, 0 = never bought
        if k in f and (f[k] is None or not money(f[k]) or f[k] > 1000):
            raise ValueError(f"{k} must be a number from 0 to 1000")
    if "margin_pct" in f and (f["margin_pct"] is None or not money(f["margin_pct"]) or f["margin_pct"] > 100):
        raise ValueError("margin_pct must be a number from 0 to 100")
    for k in ("posts_per_creator", "platform_split"):
        v = f.get(k)
        if v is not None and (not isinstance(v, dict) or any(p not in PLATFORMS or v[p] is None or not money(v[p])
                                                             for p in v)):
            raise ValueError(f"{k} must be {{platform: number >= 0}} or null")
    if f.get("platform_split") and abs(sum(f["platform_split"].values()) - 100) > 0.5:
        raise ValueError("platform_split must add up to 100")
    if f.get("posts_per_creator") is not None:   # the planner still reads posts_per_day (= the sum)
        f["posts_per_day"] = round(sum(f["posts_per_creator"].values()), 4)
    sv = f.get("services")
    if sv is not None and (not isinstance(sv, dict) or any(
            p not in PLATFORMS or not isinstance(by, dict) or any(
                k not in KINDS or (x is not None and (not isinstance(x, dict) or x.get("provider") not in PROVIDERS
                                                      or not str(x.get("service") or "").strip()))
                for k, x in by.items()) for p, by in sv.items())):
        raise ValueError("services must be {platform: {views|likes|comments|shares: {provider, service} or null}}")
    for k in ("start_date", "end_date"):   # v2.13: ISO dates only (the planner compares them as strings)
        v = f.get(k)
        try:
            ok = v is None or dt.date.fromisoformat(v).isoformat() == v
        except (TypeError, ValueError):
            ok = False
        if not ok:
            raise ValueError(f"{k} must be a date YYYY-MM-DD or null")
    lo, hi = (f[k] if k in f else (cur or {}).get(k) for k in ("start_date", "end_date"))
    if ("start_date" in f or "end_date" in f) and lo and hi and lo > hi:   # an old bad row still takes other edits
        raise ValueError("start_date must be on or before end_date")
    if f.get("source") not in (None, "sheet", "sideshift", "clippers"):
        raise ValueError("source must be sheet, sideshift or clippers")
    if any(p not in PLATFORMS for p in f.get("platforms") or []):
        raise ValueError(f"platforms must be in {', '.join(PLATFORMS)}")
    for k in _JSON_FIELDS:
        if k in f:
            f[k] = None if f[k] is None else json.dumps(f[k])
    if "approved" in f:
        f["approved"] = 1 if f["approved"] else 0
    return f


def campaign_view(c: dict, now: int | None = None) -> dict:
    now = now or _now()
    wk = week_start(now)
    first, last = et_day(wk), et_day(wk + 6 * 86400 + 7200)
    plan = db.q1("SELECT COUNT(DISTINCT url_key) posts, COALESCE(SUM(views_add), 0) v FROM plan_rows "
                 "WHERE campaign_id=? AND day BETWEEN ? AND ?", (c["id"], first, last))
    o = db.q1(f"SELECT COALESCE(SUM(o.qty), 0) ordered, COALESCE(SUM({DELIVERED}), 0) delivered "
              f"FROM auto_orders o LEFT JOIN jobs j ON j.id = o.job_id WHERE o.campaign_id=? AND o.kind='views' "
              f"AND o.state IN {ORDERED_STATES} AND o.created_at >= ?", (c["id"], wk))
    pauses = _pause_set()
    out = {"id": c["id"], "name": c["name"], "mode": c["mode"], "weekly_budget_usd": c["weekly_budget_usd"],
           "spent_week_usd": spent(wk, c["id"]), "simulated_week_usd": spent(wk, c["id"], simulated=True, real=False),
           "posts": plan["posts"], "views_planned_week": plan["v"], "views_ordered_week": o["ordered"],
           "views_delivered_week": o["delivered"],
           "spent_total_usd": spent(0, c["id"]), "simulated_total_usd": spent(0, c["id"], simulated=True, real=False),
           "paused": bool({("all", "*"), ("campaign", str(c["id"]))} & pauses), "created_at": c["created_at"]}
    for k in CAMP_FIELDS[3:]:
        out[k] = json.loads(c[k]) if k in _JSON_FIELDS and c[k] else c[k]
    out["approved"] = bool(c["approved"])
    return out


def plan_view(campaign_id: int | None) -> list[dict]:
    where, args = ("WHERE p.campaign_id=?", (campaign_id,)) if campaign_id else ("", ())
    rows = db.q(f"SELECT p.*, COALESCE((SELECT SUM(o.qty) FROM auto_orders o WHERE o.plan_id=p.plan_id "
                f"AND o.kind='views' AND o.state IN {ORDERED_STATES}), 0) ordered_views, "
                f"COALESCE((SELECT SUM({DELIVERED}) FROM auto_orders o LEFT JOIN jobs j ON j.id=o.job_id "
                f"WHERE o.plan_id=p.plan_id AND o.kind='views' AND o.state IN {ORDERED_STATES}), 0) delivered_views "
                f"FROM plan_rows p {where} ORDER BY p.day DESC, p.priority IS NULL, p.priority, p.id", args)
    pauses = _pause_set()
    keys = ("plan_id", "campaign_id", "creator_id", "post_url", "platform", "account_handle", "window_start",
            "window_end", "views_add", "likes_add", "comments_add", "shares_add", "target_total_views", "reason", "priority",
            "ordered_views", "delivered_views", "carry")
    return [{**{k: r[k] for k in keys}, "paused": _paused(pauses, r)} for r in rows]


def _service_name(provider: str | None, service: str | None) -> str:
    hit = panels._services.get(provider or "")
    s = next((x for x in hit[1] if str(x["service"]) == str(service)), None) if hit else None
    return (s or {}).get("name", "")


def order_view(r: dict) -> dict:
    return {"id": r["id"], "plan_id": r["plan_id"], "campaign_id": r["campaign_id"], "creator_id": r.get("creator_id"),
            "account_handle": r.get("account_handle"), "post_url": r.get("post_url"), "platform": r.get("platform"),
            "day": r["day"], "kind": r["kind"], "provider": r["provider"], "service": r["service"],
            "service_name": _service_name(r["provider"], r["service"]), "qty": r["qty"], "est_cost": r["est_cost"],
            "state": r["state"], "attempt": r["attempt"], "retry_of": r["retry_of"], "is_test": bool(r["is_test"]),
            "job_id": r["job_id"], "panel_status": r.get("views_status"), "remaining": r.get("views_remaining"),
            "error": r["error"], "can_cancel": r["state"] == "placed" and bool(r["order_id"]),
            "comment_lines": json.loads(r["comments"]) if r.get("comments") else None, "created_at": r["created_at"]}


def order_full(oid: int) -> dict:
    return order_view(db.q1(ORDER_SELECT + " WHERE o.id=?", (oid,)))


ORDER_SELECT = ("SELECT o.*, p.creator_id, p.account_handle, p.post_url, p.platform, j.views_status, j.views_remaining "
                "FROM auto_orders o LEFT JOIN plan_rows p ON p.plan_id = o.plan_id LEFT JOIN jobs j ON j.id = o.job_id")


# --------------------------------------------------------------------------- #
# v2.12 Calculator: service dropdowns + cost estimate (API-CONTRACT.md v2.12)
# --------------------------------------------------------------------------- #
LIKES_PER_VIEW = 0.01   # planner: a post's likes = its target views x p_likes, p_likes uniform 0.8% to 1.2%
BUFFER_DAYS = 2


async def service_options(platform: str) -> dict:
    """Dropdown rows for one platform. views: the services the executor may use (candidates()), best score first,
    then cheapest; likes: each unpaused panel's likes service (likes go to the views order's panel). [0] = recommended."""
    ctx = await context()
    sc = lambda c: ctx["scores"].get((c["provider"], c["service"], platform))  # noqa: E731
    views = [{**c, "name": (await panels.service_info(c["provider"], c["service"]) or {}).get("name", ""), "score": sc(c)}
             for c in sorted(await candidates(platform, ctx), key=lambda c: (sc(c) is None, -(sc(c) or 0), c["rate"]))]
    likes = []
    for p in PROVIDERS:
        sid = service_for(p, platform, "likes")
        if not sid or ("panel", p) in ctx["pauses"] or panels.down(p):
            continue
        try:
            info = await panels.service_info(p, sid)
        except Exception:  # noqa: BLE001  panel down or no key
            continue
        if info and _num(info.get("rate")):
            likes.append({"provider": p, "service": sid, "name": info.get("name", ""), "rate": _num(info["rate"])})
    top = views[0]["provider"] if views else None
    likes.sort(key=lambda c: (c["provider"] != top, c["rate"]))
    for rows in (views, likes):
        for i, r in enumerate(rows):
            r["recommended"] = i == 0
    extra = {k: [] for k in EXTRA}   # v2.16: every quantity-only comments / shares service, cheapest first, none preset
    for p in PROVIDERS:
        if ("panel", p) in ctx["pauses"] or panels.down(p):
            continue
        try:
            cat = await panels.services_cached(p)
        except Exception:  # noqa: BLE001  panel down or no key
            continue
        for s in cat:
            for k in EXTRA:
                if _num(s.get("rate")) and extra_svc_ok(s, platform, k):
                    cu = k == "comments" and custom_comments(s)
                    extra[k].append({"provider": p, "service": str(s["service"]), "name": s.get("name", ""),
                                     "rate": _num(s["rate"]), "recommended": False, "custom": cu,
                                     "star": cu and (p, str(s["service"])) == COMMENT_DEFAULTS.get(platform),
                                     "min": int(_num(s.get("min")) or 0), "max": int(_num(s.get("max")) or 0)})
    for rows in extra.values():   # v2.18: the starred default first
        rows.sort(key=lambda c: (not c.get("star"), c["rate"], c["provider"], c["service"]))
    return {"platform": platform, "views": views, "likes": likes, **extra}


def estimate(c: dict, rates: dict) -> dict:
    """Calculator math (the popup does the same in JS). c: campaign fields, JSON decoded. rates: {(platform, kind):
    USD per 1k}. Views bought = goal x (1 + margin) split by platform share; likes = those views x LIKES_PER_VIEW.
    Upper bound: organic views lower the real spend."""
    ppc = {p: n for p, n in (c.get("posts_per_creator") or {}).items() if n}
    days = (dt.date.fromisoformat(c["end_date"]) - dt.date.fromisoformat(c["start_date"])).days + 1 \
        if c.get("start_date") and c.get("end_date") else 0
    goal, creators = c.get("views_goal") or 0, c.get("creators_planned") or 0
    margin = 12.5 if c.get("margin_pct") is None else c["margin_pct"]
    target = goal * (1 + margin / 100)
    split = {p: (c.get("platform_split") or {}).get(p, 0) for p in ppc} if c.get("platform_split") \
        else {p: 100 / len(ppc) for p in ppc}
    videos = creators * sum(ppc.values()) * max(0, days)
    cost = {"views": 0.0, "likes": 0.0, "comments": 0.0, "shares": 0.0}
    for p, pct in split.items():
        v = target * pct / 100
        cost["views"] += v / 1000 * rates.get((p, "views"), 0)
        cost["likes"] += v * LIKES_PER_VIEW / 1000 * rates.get((p, "likes"), 0)
        for k in EXTRA:   # v2.16: ratio per 1k views x rate per 1k; no service chosen = no rate = 0
            cost[k] += v / 1000 * (_num(c.get(f"{k}_per_1k")) or 0) / 1000 * rates.get((p, k), 0)
    total = round(sum(cost.values()), 2)
    return {"days": max(0, days), "buffer_days": BUFFER_DAYS, "total_videos": round(videos),
            "views_target": round(target), "avg_views_per_video": round(target / videos) if videos else None,
            "cost": {k: round(v, 2) for k, v in cost.items()}, "total_usd": total,
            "per_1k_views_usd": round(total / goal * 1000, 4) if goal else None,
            "cap_default_usd": round(total * 1.1, 2)}


async def services_problem(sv: dict | None) -> str | None:
    """v2.13: a views / likes pick must be that kind of service for that platform in its panel's catalog.
    Catalog not readable = accepted (the executor falls back to the panel default). v2.16: comments / shares too, and
    they must take a quantity only (a Custom Comments service needs comment text: rejected). An unreadable catalog
    is accepted here; the executor re-checks before every comments / shares order and blocks it if unverified."""
    for p, by in (sv or {}).items():
        for kind in KINDS:
            x = (by or {}).get(kind)
            if not x:
                continue
            try:
                info = await panels.service_info(x["provider"], str(x["service"]))
            except Exception:  # noqa: BLE001
                continue
            if info is None:
                return f"services: {p} {kind} service {x['service']} is not in the {x['provider']} catalog"
            if not campaigns.svc_matches(info, p, kind):
                return f"services: {p} {kind} service {x['service']} ({info.get('name', '')}) is not a {kind} service for {p}"
            if kind in EXTRA and not qty_only(info) and not (kind == "comments" and custom_comments(info)):
                t = str(info.get("type") or "")
                return (f"services: {p} {kind} service {x['service']} ({info.get('name', '')}) is type {t}: "
                        + ("only quantity-only (Default type) or Custom Comments services can be used" if kind == "comments"
                           else "only quantity-only (Default type) services can be used"))
            if kind in EXTRA and not extra_svc_ok(info, p, kind):
                return (f"services: {p} {kind} service {x['service']} ({info.get('name', '')}) is not a plain {p} {kind} "
                        "service (its name must say the platform and the kind; no followers, visits, Threads, likes)")
    return None


async def estimate_for(f: dict) -> dict | None:
    """estimate() with the live catalog rates of the campaign's chosen services; None if a views pick is missing."""
    c = {k: json.loads(v) if k in _JSON_FIELDS and isinstance(v, str) else v for k, v in f.items()}
    rates = {}
    for p, n in (c.get("posts_per_creator") or {}).items():
        for kind in KINDS:
            x = ((c.get("services") or {}).get(p) or {}).get(kind)
            if not x:
                if kind == "views" and n:
                    return None
                continue
            rates[(p, kind)] = await campaigns.live_rate(x["provider"], p, kind, str(x["service"]))
    return estimate(c, rates)
