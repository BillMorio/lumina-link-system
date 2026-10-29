"""v2.8 Planner: per post per ET day views/likes -> `plan_rows` (spec specs/automation-algorithm-spec.md).

Never calls a panel. Reads links (sources.py), post counts (sources.get_counts), our orders
(jobs + auto_orders + plan_rows carry), and writes plan rows through automation.insert_plan
(INSERT OR IGNORE on plan_id `campaign_id:post_key:YYYY-MM-DD`; a later batch or run the same day
replaces a row's numbers only while no auto order exists for it, see replace_rows). Runs at each campaign's check_times (default 13:00 and
01:00 ET) from the scheduler, only with AUTO_ENABLED=1, or on demand (POST .../run).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import math
import random
import statistics
import time

import automation
import db
import score
import sources
from automation import ET, et_day, et_midnight
from config import COMMENT_MIN, detect_platform, url_key

log = logging.getLogger("planner")

rng = random.Random()
CHECK_TIMES = ["13:00", "01:00"]
PRIORITY = {"viral": 1, "decay": 2, "base": 3}
DECAY = ((0.60, 0.70), (0.40, 0.50), (0.08, 0.12))
CAP_X = 5
RAMP_DAYS = 21      # normal / decay ramp (v2.13: fixed, squeezed to end_date + buffer)
MARGIN_PCT = 12.5   # default safety margin when a campaign has none
SCRAPES = 5   # parallel count checks
BATCH = 50    # rows are written after every batch of count checks, not after the full sweep
LIKES_ZERO_VIEWS = 500   # 0 likes on a post with this many views = unreadable likes
MAX_MISSES = 3
MAX_PEAK = 10_000_000
LIMITS = {"max_scrapes_per_run": 300, "max_scrapes_per_day": 1000}   # settings keys auto:<name>
_running: set[int] = set()
_EXT: dict[int, dict] = {}   # campaign id -> {url_key: {views, likes}} bought outside the console (sheet "Views" + "Top Performers" tabs, last good read)
_TOP: dict[int, dict] = {}   # campaign id -> {url_key: {views, likes}} of the "Top Performers" tab alone (last good read)


def _now() -> int:
    return int(time.time())


def _d(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def _days(day: str, n: int) -> str:
    return (_d(day) + dt.timedelta(days=n)).isoformat()


# --------------------------------------------------------------------------- #
# Curve (spec 5)
# --------------------------------------------------------------------------- #
def weights(D: int, viral: bool = False) -> list[float]:
    """Day weights over D days, sum 1. Days 1-3 = 1 : 1.5 : 2 (50% normal / 70% viral), days 4..D
    geometric 0.85 (the rest). D <= 3: 1 : 1.5 : 2 over D days = 100%."""
    D = max(1, D)
    head = [1, 1.5, 2][:min(D, 3)]
    if D <= 3:
        return [w / sum(head) for w in head]
    hs = 0.7 if viral else 0.5
    tail = [0.85 ** (k - 4) for k in range(4, D + 1)]
    return [w / 4.5 * hs for w in head] + [w / sum(tail) * (1 - hs) for w in tail]


def buy_end(camp: dict) -> str:
    """Last buying day (v2.13): end_date + BUFFER_DAYS. No posts are expected in the buffer days."""
    return _days(camp["end_date"], automation.BUFFER_DAYS)


def ramp_len(p: dict, camp: dict) -> int:
    """Normal/decay: 21 days from upload; viral: 10 from approval. Squeezed to buy_end (min 1). ramp_weeks is
    no longer read (v2.13)."""
    full = 10 if p["reason"] == "viral" else RAMP_DAYS
    return max(1, min(full, (_d(buy_end(camp)) - _d(p["ramp_start"])).days + 1))


def ramp_day(p: dict, day: str) -> int:
    return (_d(day) - _d(p["ramp_start"])).days + 1


def in_ramp(p: dict, camp: dict, day: str) -> bool:
    return 1 <= ramp_day(p, day) <= ramp_len(p, camp)


def F(p: dict, camp: dict, day: str) -> float:
    return sum(weights(ramp_len(p, camp), p["reason"] == "viral")[:max(0, ramp_day(p, day))])


def goal(p: dict, camp: dict, day: str, target: float) -> float:
    """Cumulative views goal by `day`. Normal/decay ramp from upload; first read on ramp day k > 1 (base_day) ->
    min(target, base_views + target x (F(d) - F(k-1))): past days' shares dropped, never caught up.
    k = 1 -> target x F(d). Viral (ramps from approval, target >= peak): min(target, viral_base + (target - viral_base)
    x F(d)), viral_base = views at approval, so the approval day already spikes."""
    if p["reason"] == "viral":
        b = p["viral_base"] or 0
        return min(target, b + max(0, target - b) * F(p, camp, day))
    if not p["base_day"] or ramp_day(p, p["base_day"]) <= 1:
        return target * F(p, camp, day)
    return min(target, (p["base_views"] or 0) + target * (F(p, camp, day) - F(p, camp, _days(p["base_day"], -1))))


# --------------------------------------------------------------------------- #
# Our orders per post (url_key)
# --------------------------------------------------------------------------- #
def order_facts(today: str, now: int) -> dict:
    """url_key -> {delivered, inflight, sim, inflight_likes, sim_likes, ext, ext_likes, carry:{campaign_id: views}}.
    delivered/inflight = every jobs row (manual, campaign queue, auto live); a `placing` auto order (no job
    yet), an `ambiguous` one under 48 h old (maybe placed) and a failed one awaiting its re-send = in flight. sim = practice (`simulated`) orders. carry = views the executor still owes from
    the last CARRY_DAYS days' plan rows (it adds them to today's order, so the planner must not plan them again).
    ext / ext_likes = views / likes bought outside the console ("Done" rows of the sheet's "Views" tab + every
    "Top Performers" row, see load_external)."""
    out: dict = {}

    def f(k):
        return out.setdefault(k, {"delivered": 0, "inflight": 0, "sim": 0, "inflight_likes": 0, "sim_likes": 0,
                                  "ext": 0, "ext_likes": 0, "carry": {}})
    fails = ",".join(f"'{s}'" for s in score.FAILED)
    for r in db.q(
            "SELECT url_key, SUM(CASE WHEN completed_at IS NOT NULL THEN views_target "
            "WHEN views_remaining IS NOT NULL THEN MAX(0, views_target - views_remaining) ELSE 0 END) d, "
            f"SUM(CASE WHEN completed_at IS NULL AND LOWER(COALESCE(views_status,'')) NOT IN ({fails}) "
            "AND created_at >= ? THEN COALESCE(views_remaining, views_target) ELSE 0 END) i "
            "FROM jobs WHERE views_order_id IS NOT NULL AND views_order_id <> '' AND url_key IS NOT NULL "
            "GROUP BY url_key", (now - 7 * 86400,)):
        f(r["url_key"]).update(delivered=r["d"] or 0, inflight=r["i"] or 0)
    for r in db.q("SELECT url_key, kind, state, SUM(qty) n FROM auto_orders WHERE url_key IS NOT NULL "
                  "AND kind IN ('views','likes') AND "   # v2.16: comments / shares are not likes
                  "(state IN ('placing','simulated') OR (kind='likes' AND state='placed' AND created_at >= ?) "
                  "OR (state='ambiguous' AND job_id IS NULL AND created_at >= ?)) "
                  "GROUP BY url_key, kind, state", (now - 86400, now - 2 * 86400)):
        x = f(r["url_key"])
        if r["state"] == "simulated":
            x["sim" if r["kind"] == "views" else "sim_likes"] += r["n"]
        else:
            x["inflight" if r["kind"] == "views" else "inflight_likes"] += r["n"]
    for r in db.q("SELECT campaign_id, url_key, SUM(carry) n FROM plan_rows WHERE day >= ? AND day < ? AND carry > 0 "
                  "GROUP BY campaign_id, url_key", (_days(today, -automation.CARRY_DAYS), today)):
        f(r["url_key"])["carry"][r["campaign_id"]] = r["n"]
    for o in db.q("SELECT * FROM auto_orders o WHERE kind='views' AND state='failed' AND attempt=0 AND created_at >= ? "
                  "AND COALESCE(error,'') NOT LIKE '%not re-sent%' AND url_key IS NOT NULL "
                  "AND NOT EXISTS (SELECT 1 FROM auto_orders r WHERE r.retry_of=o.id AND r.state<>'blocked')",
                  (now - automation.RETRY_DAYS * 86400,)):
        f(o["url_key"])["inflight"] += automation.missing_qty(o)   # the executor re-sends it once
    for m in list(_EXT.values()):   # two campaigns on one sheet: same rows, so max, not sum
        for k, v in m.items():
            x = f(k)
            x["ext"], x["ext_likes"] = max(x["ext"], v["views"]), max(x["ext_likes"], v["likes"])
    return out


_EMPTY = {"delivered": 0, "inflight": 0, "sim": 0, "inflight_likes": 0, "sim_likes": 0, "ext": 0, "ext_likes": 0,
          "carry": {}}


def natural(p: dict, facts: dict) -> int:
    """Observed minus every bought view (our orders + the sheet's "Views" / "Top Performers" tabs)."""
    f = facts.get(p["url_key"], _EMPTY)
    return max(0, (p["observed_views"] or 0) - f["delivered"] - f["ext"])


async def _add_ext(got: dict, rows: list[dict]) -> None:
    for x in rows:
        u = await sources.resolve(detect_platform(x["url"]) or "", x["url"]) or x["url"]
        g = got.setdefault(url_key(u), {"views": 0, "likes": 0})
        g["views"] += x["views"]
        g["likes"] += x["likes"]


async def load_external(camp: dict) -> None:
    """Views / likes bought outside the console: each sheet source's "Views" tab plus its "Top Performers" tab (summed
    per post), read once per run (cached in _EXT; Top Performers also in _TOP). A failed read = log + one alert, the run
    goes on with that tab's last good read (none yet = nothing subtracted). No Top Performers tab = nothing from it."""
    try:
        sids = sorted({sources.sheet_ref(r)[0] for s, r in sources.refs(camp) if s == "sheet"})
    except sources.SourceError:
        return
    if not sids:
        return
    try:
        top: dict = {}
        bad: list = []
        for sid in sids:
            rows, b = sources.parse_top(await sources.fetch_top(sid))
            await _add_ext(top, rows)
            bad += b
        _TOP[camp["id"]] = top
        if bad:   # skipped rows under-count bought views (natural looks higher): say so (one alert per run; same text = shown once)
            automation.alert("warn", f"Automation: {camp['name']}: sheet \"Top Performers\": {len(bad)} unreadable "
                                     f"\"Views to buy\" cell(s) skipped ({', '.join(repr(x) for x in bad[:3])})")
    except Exception as e:  # noqa: BLE001  never stops the run nor the Views tab
        log.warning("planner %s: Top Performers tab not read: %s", camp["id"], e)
        automation.alert("warn", f"Automation: {camp['name']}: sheet \"Top Performers\" tab not read; views bought "
                                 "there are not subtracted from natural views")
    try:
        got: dict = {}
        for sid in sids:
            await _add_ext(got, sources.parse_views(await sources.fetch_views(sid)))
        for k, v in _TOP.get(camp["id"], {}).items():   # a separate buy from the Views tab's: summed
            g = got.setdefault(k, {"views": 0, "likes": 0})
            g["views"] += v["views"]
        _EXT[camp["id"]] = got
    except Exception as e:  # noqa: BLE001  never stops the run
        log.warning("planner %s: Views tab not read: %s", camp["id"], e)
        automation.alert("warn", f"Automation: {camp['name']}: sheet \"Views\" tab not read; views bought there are "
                                 "not subtracted from natural views")


ACCT = "COALESCE(creator_id, account_handle)"   # account = (creator, platform): one creator can post under several handles


def _acct(p: dict) -> str:
    return p["creator_id"] or p["account_handle"] or ""


def handle(creator_id, platform: str, fallback=None):
    """Display handle of an account (creator, platform): the SideShift handle, else the most recent post's."""
    r = db.q1(f"SELECT account_handle FROM auto_posts WHERE {ACCT}=? AND platform=? AND account_handle IS NOT NULL "
              "ORDER BY source='sideshift' DESC, uploaded_at DESC, id DESC LIMIT 1", (creator_id, platform))
    return r["account_handle"] if r else fallback


def normal_views(p: dict, facts: dict, now: int) -> float | None:
    """Median natural views of the account's last 10 posts on that platform >= 3 days old (one viral post does not
    inflate it); fewer than 3 -> the campaign median for that platform (posts >= 3 days old; none -> checked posts >= 24 h old,
    so a fresh campaign has a baseline on its first run); nothing -> None."""
    old = now - 3 * 86400
    mine = db.q(f"SELECT url_key, observed_views FROM auto_posts WHERE {ACCT}=? "
                "AND platform=? AND uploaded_at <= ? AND observed_views IS NOT NULL "
                "ORDER BY uploaded_at DESC LIMIT 10", (_acct(p), p["platform"], old))
    if len(mine) < 3:
        mine = db.q("SELECT url_key, observed_views FROM auto_posts WHERE campaign_id=? AND platform=? "
                    "AND uploaded_at <= ? AND observed_views IS NOT NULL", (p["campaign_id"], p["platform"], old))
    if not mine:
        mine = db.q("SELECT url_key, observed_views FROM auto_posts WHERE campaign_id=? AND platform=? "
                    "AND uploaded_at <= ? AND observed_views IS NOT NULL", (p["campaign_id"], p["platform"], now - 86400))
    return statistics.median(natural(m, facts) for m in mine) if mine else None


# --------------------------------------------------------------------------- #
# Ingest
# --------------------------------------------------------------------------- #
def active_count(camp: dict, out_: set) -> int:
    """Creators expected to post: planned minus ignored, where ignored creators beyond the planned number (extras the
    campaign knows) do not reduce it. Known = creators in auto_creators, posts or slots. v2.16 (Agent A bug 17)."""
    planned, cid = camp["creators_planned"] or 0, camp["id"]
    known = db.q1("SELECT COUNT(*) n FROM (SELECT creator_id FROM auto_creators WHERE campaign_id=? AND creator_id IS NOT NULL UNION "
                  "SELECT creator_id FROM auto_posts WHERE campaign_id=? AND creator_id IS NOT NULL UNION "
                  "SELECT creator_id FROM auto_slots WHERE campaign_id=? AND creator_id IS NOT NULL)", (cid, cid, cid))["n"]
    return max(0, min(planned, max(known, planned) - len(out_)))


def ignored(cid: int) -> tuple[set, set]:
    """-> (cancelled, excluded) creator ids of a campaign. v2.14: neither counts for expected posts or missed slots;
    cancelled creators' views still count toward the goal, excluded creators' views do not; excluded (admin un-ticked, included=0) also gets no new posts, no count checks, no rows, no orders."""
    rows = db.q("SELECT creator_id, cancelled, included FROM auto_creators WHERE campaign_id=?", (cid,))
    return {r["creator_id"] for r in rows if r["cancelled"]}, {r["creator_id"] for r in rows if not r["included"]}


def ingest(camp: dict, links: list[dict], slots: list[dict], cancelled: dict, now: int) -> int:
    """New posts -> auto_posts (post_key unique across all campaigns: first seen wins). Returns # new.
    ramp_start = upload day (v2.10): a post first read later plans only the rest of its curve (see goal).
    v2.14: posts of an un-ticked creator are not taken in."""
    plats = set(json.loads(camp["platforms"]) if camp["platforms"] else automation.PLATFORMS)
    lo, hi = camp["start_date"], camp["end_date"]
    excluded = ignored(camp["id"])[1]
    new = 0
    for ln in links:
        if ln["platform"] not in plats or not (lo <= ln["day"] <= hi) or not ln.get("post_key") \
                or ln["creator_id"] in excluded:
            continue
        new += db.ex("INSERT OR IGNORE INTO auto_posts (post_key, url_key, campaign_id, creator_id, account_handle, "
                     "platform, url, source, slot, day, uploaded_at, w_random, p_likes, ramp_start, created_at) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     (ln["post_key"], url_key(ln["url"]), camp["id"], ln["creator_id"], ln["account_handle"],
                      ln["platform"], ln["url"], ln["source"], ln["slot"], ln["day"], ln["uploaded_at"],
                      rng.uniform(0.85, 1.15), rng.uniform(0.008, 0.012), ln["day"], now))[0]
    for s in slots:
        if lo <= s["day"] <= hi:
            db.ex("INSERT OR REPLACE INTO auto_slots (campaign_id, creator_id, day, slot, url) VALUES (?,?,?,?,?)",
                  (camp["id"], s["creator_id"], s["day"], s["slot"], s["url"]))
    for creator, x in cancelled.items():
        db.ex("INSERT INTO auto_creators (campaign_id, creator_id, cancelled) VALUES (?,?,?) "
              "ON CONFLICT(campaign_id, creator_id) DO UPDATE SET cancelled=excluded.cancelled",
              (camp["id"], creator, int(bool(x))))
    return new


def apply_count(p: dict, got, facts: dict, now: int, at: int | None = None, t24_views: int | None = None) -> str:
    """-> 'ok' | 'ok_nolikes' (views read, likes not) | 'unreadable' | 'not_found' (first of 2) | 'deleted'.
    0 views on a post > 24 h old or that had views before = unreadable. Counts never go down.
    0 likes with >= LIKES_ZERO_VIEWS views = likes unreadable (kept as before, no likes this run).
    Unreadable: sit out the next run; MAX_MISSES in a row = stop checking. t24 = natural views at the
    first check 24-36 h after upload; no check in that window = t24 stays NULL (weight 1, not viral-eligible).
    at: when the counts were read (SideShift snapshot / sweep time; default now). t24_views: SideShift views of the
    snapshot nearest 24 h after upload (already inside 24-36 h) -> t24 = those views net of bought views."""
    at = at or now
    if got == sources.DELETED:
        nf = (p["not_found"] or 0) + 1
        if nf >= 2:
            db.ex("UPDATE auto_posts SET status='deleted', not_found=?, checked_at=? WHERE id=?", (nf, now, p["id"]))
            return "deleted"
        db.ex("UPDATE auto_posts SET not_found=? WHERE id=?", (nf, p["id"]))
        return "not_found"
    prior = p["observed_views"] or 0
    if not got or not isinstance(got, tuple) or got[0] is None \
            or (got[0] == 0 and (at - p["uploaded_at"] > 86400 or prior > 0)):
        db.ex("UPDATE auto_posts SET misses=misses+1, skip_next=1, not_found=0 WHERE id=?", (p["id"],))
        return "unreadable"
    views, likes = max(int(got[0]), prior), got[1]
    likes_ok = likes is not None and not (likes == 0 and views >= LIKES_ZERO_VIEWS)
    likes = max(int(likes), p["observed_likes"] or 0) if likes_ok else p["observed_likes"]
    com = got[2] if len(got) > 2 else None   # v2.16: comments when the source returns them (Apify), never guessed
    com = max(int(com), p.get("observed_comments") or 0) if com is not None else p.get("observed_comments")
    p.update(observed_views=views, observed_likes=likes, observed_comments=com, checked_at=at)
    if p.get("base_views") is None:   # v2.10: first views read = the base, set once
        p.update(base_views=views, base_day=max(p["day"], et_day(at)))
    t24 = p["t24"]
    if t24 is None and t24_views is not None:
        t24 = p["t24"] = natural({**p, "observed_views": int(t24_views)}, facts)
    elif t24 is None and 86400 <= at - p["uploaded_at"] <= 36 * 3600:
        t24 = p["t24"] = natural(p, facts)
    db.ex("UPDATE auto_posts SET observed_views=?, observed_likes=?, observed_comments=?, checked_at=?, t24=?, "
          "misses=0, not_found=0, base_views=COALESCE(base_views, ?), base_day=COALESCE(base_day, ?) WHERE id=?",
          (views, likes, com, at, t24, p["base_views"], p["base_day"], p["id"]))
    return "ok" if likes_ok else "ok_nolikes"


# --------------------------------------------------------------------------- #
# SideShift (v2.9): creator ids, free counts from the program sweep + metrics history
# --------------------------------------------------------------------------- #
SS_FRESH = 20 * 3600   # a SideShift snapshot older than this (first 3 days / viral) = stale: paid path instead


def merge_creators(cid: int, got: dict) -> None:
    """Campaign with SideShift links: canonical creator_id = SideShift contractorId. Sheet Creator names map to it by
    contractorName (exact, case-insensitive, trimmed; a contractorName two contractors share maps nobody), else by a post
    in both (post_key) only when that post has no contractorName. A name maps to one id. Mapping stored in
    auto_creator_map; links, slots and cancelled flags are rewritten to the canonical id, and rows already stored
    under the name move to it. Sheet links matched to a SideShift post take its handle and upload time; sheet links go
    first so the sheet's slot info is kept. Unmatched names stay as they are."""
    ss = [ln for ln in got["links"] if ln["source"] == "sideshift"]
    m = {r["name_key"]: r["creator_id"] for r in db.q("SELECT * FROM auto_creator_map WHERE campaign_id=?", (cid,))}
    if not ss and not m:   # SideShift outage with a stored map still maps (canon below)
        return
    ids_by_name: dict = {}
    by_post = {ln["post_key"]: ln for ln in ss if ln.get("post_key")}
    for ln in ss:
        if ln.get("name"):
            ids_by_name.setdefault(ln["name"].strip().lower(), set()).add(ln["creator_id"])
    ambiguous = {k for k, v in ids_by_name.items() if len(v) > 1}
    by_name = {k: next(iter(v)) for k, v in ids_by_name.items() if len(v) == 1}
    names = {}
    sheet = [ln for ln in got["links"] if ln["source"] == "sheet"]
    for nm, pk in [(ln["creator_id"], ln.get("post_key")) for ln in sheet] + \
                  [(x["creator_id"], None) for x in got["slots"]] + [(k, None) for k in got["cancelled"]]:
        k = str(nm or "").strip().lower()
        if not k:
            continue
        names.setdefault(k, str(nm).strip())
        if k in ambiguous:
            m.pop(k, None)
        elif k in by_name:
            m[k] = by_name[k]
        elif k not in m and pk in by_post and not by_post[pk].get("name"):
            m[k] = by_post[pk]["creator_id"]

    def canon(c):
        return m.get(str(c or "").strip().lower(), c)
    for ln in sheet:
        x = by_post.get(ln.get("post_key"))
        ln["creator_id"] = canon(ln["creator_id"])
        if x:
            ln["account_handle"], ln["uploaded_at"] = x["account_handle"], x["uploaded_at"]
    for x in got["slots"]:
        x["creator_id"] = canon(x["creator_id"])
    canc: dict = {}
    for k, v in got["cancelled"].items():
        canc[canon(k)] = canc.get(canon(k), False) or v
    got["cancelled"] = canc
    got["links"].sort(key=lambda ln: ln["source"] != "sheet")
    with db._conn() as c:
        for k in ambiguous:
            c.execute("DELETE FROM auto_creator_map WHERE campaign_id=? AND name_key=?", (cid, k))
        for k, v in m.items():
            c.execute("INSERT INTO auto_creator_map (campaign_id, name_key, name, creator_id) VALUES (?,?,?,?) "
                      "ON CONFLICT(campaign_id, name_key) DO UPDATE SET creator_id=excluded.creator_id, "
                      "name=CASE WHEN ? <> '' THEN ? ELSE name END",
                      (cid, k, names.get(k, k), v, names.get(k, ""), names.get(k, "")))
            for t in ("auto_posts", "auto_slots", "auto_creators"):
                c.execute(f"{'UPDATE OR IGNORE' if t != 'auto_posts' else 'UPDATE'} {t} SET creator_id=? "
                          "WHERE campaign_id=? AND LOWER(TRIM(creator_id))=? AND creator_id<>?", (v, cid, k, v))
            c.execute("UPDATE auto_creators SET included=0 WHERE campaign_id=? AND creator_id=? AND EXISTS (SELECT 1 "
                      "FROM auto_creators n WHERE n.campaign_id=? AND LOWER(TRIM(n.creator_id))=? AND n.creator_id<>? "
                      "AND n.included=0)", (cid, v, cid, k, v))   # v2.14: an un-tick under the name is kept
            for t in ("auto_slots", "auto_creators"):   # name rows a newer canonical row already replaced
                c.execute(f"DELETE FROM {t} WHERE campaign_id=? AND LOWER(TRIM(creator_id))=? AND creator_id<>?",
                          (cid, k, v))


async def sweep(camp: dict) -> dict:
    """post_key -> SideShift item (+ `_at` = sweep time) over the campaign's sideshift programs (one sweep per run,
    shared with the links pass). A failed sweep = log + one alert; those posts use the paid path."""
    try:
        pids = [r.strip() for src, r in sources.refs(camp) if src == "sideshift"]
    except sources.SourceError:
        return {}
    out = {}
    for pid in pids:
        try:
            at, items = await sources.ss_items(pid)
        except Exception as e:  # noqa: BLE001  never stops the run
            log.warning("planner %s: SideShift sweep %s failed: %s", camp["id"], pid, e)
            automation.alert("warn", f"Automation: {camp['name']}: SideShift sweep failed; counts use the paid "
                                     "path (Clippers, Apify)")
            continue
        for note in sorted(sources.ss_notes):
            automation.alert("warn", f"Automation: {note}")
        for it in items:
            plat = sources.ss_platform(it)
            k = sources.post_key(plat, it.get("postPage") or "") if plat else None
            if k:
                out[k] = {**it, "_at": at}
    return out


def mark_deleted(cid: int, sw: dict) -> int:
    """SideShift deletionStatus set = the post is deleted (no second read needed)."""
    return sum(db.ex("UPDATE auto_posts SET status='deleted' WHERE campaign_id=? AND post_key=? AND status='active'",
                     (cid, k))[0] for k, it in sw.items() if sources.ss_deleted(it))


SS_OLD = 72 * 3600   # other ramp posts: a snapshot older than this, on a post still short of its target = paid re-read


async def sideshift_counts(camp: dict, posts: list[dict], sw: dict, facts: dict, now: int) -> tuple[dict, list, list]:
    """Free counts for posts in the sweep (never toward the scrape caps). Every post in ramp: metrics-history gives the
    snapshot time (= checked_at; our orders created after it count as in flight in plan) and t24. Posts in their first
    3 days and viral posts in ramp: latest snapshot older than SS_FRESH or history failed / rate limited = stale
    (returned for the paid path); other ramp posts with no history: snapshot time = sweep time - 24 h. Posts out of
    ramp: sweep values, checked_at = sweep time. No views, or 0 views on a post > 24 h old (or that had views) = stale
    too, never a miss: SideShift has not read it. Other ramp posts whose snapshot is older than SS_OLD and that still need
    views (target_total > observed + in flight) keep the SideShift counts AND are returned for a paid re-read at the
    lowest priority (v2.9.3). -> ({post id: apply_count result}, [stale posts], [re-read posts])."""
    refresh = []
    today = et_day(now)

    async def one(p):
        it = sw[p["post_key"]]
        got, at, t24v = (it.get("views"), it.get("likes")), it["_at"], None
        strict = now - p["uploaded_at"] <= 3 * 86400 or (p["reason"] == "viral" and in_ramp(p, camp, today))
        if strict or in_ramp(p, camp, today):
            try:
                h = await sources.ss_history(str(it["postId"]))   # process-wide 10/s limiter, 429 back-off
            except Exception:  # noqa: BLE001
                h = []
            last = max(h, key=lambda x: int(x["timestamp"]), default=None)
            if strict and (not last or now - int(last["timestamp"]) > SS_FRESH):
                return None
            if last:
                got, at = (last.get("views"), last.get("likes", it.get("likes"))), int(last["timestamp"])
                win = [x for x in h if x.get("views") is not None
                       and 86400 <= int(x["timestamp"]) - p["uploaded_at"] <= 36 * 3600]
                if win:
                    t24v = min(win, key=lambda x: abs(int(x["timestamp"]) - p["uploaded_at"] - 86400))["views"]
            else:
                at = it["_at"] - 86400   # snapshot time unknown: assume the sweep's numbers are a day old
        if got[0] is None or (got[0] == 0 and (now - p["uploaded_at"] > 86400 or (p["observed_views"] or 0) > 0)):
            return None   # SideShift has no usable views: the paid path reads it (and applies its misses rule)
        r = await asyncio.to_thread(apply_count, p, got, facts, now, at, t24v)
        if not strict and now - at > SS_OLD and p["target_total"] is not None and r in ("ok", "ok_nolikes") \
                and p["target_total"] > (p["observed_views"] or 0) + facts.get(p["url_key"], _EMPTY)["inflight"]:
            refresh.append({**p, "_ss": r})
        return r
    out = await asyncio.gather(*(one(p) for p in posts), return_exceptions=True)
    res = {p["id"]: r for p, r in zip(posts, out) if isinstance(r, str)}
    return res, [p for p, r in zip(posts, out) if not isinstance(r, str)], refresh


def setting(name: str) -> int:
    r = db.q1("SELECT v FROM settings WHERE k=?", (f"auto:{name}",))
    return int(json.loads(r["v"])) if r else LIMITS[name]


def set_limits(body: dict) -> dict:
    bad = [k for k in body if k not in LIMITS]
    if bad:
        raise ValueError(f"not editable: {', '.join(bad)}")
    for k, v in body.items():
        if isinstance(v, bool) or not isinstance(v, int) or v < 0:
            raise ValueError(f"{k} must be an integer >= 0")
    for k, v in body.items():
        db.ex("INSERT INTO settings (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (f"auto:{k}", json.dumps(v)))
    return {k: setting(k) for k in LIMITS}


def scrapes_today(now: int) -> int:
    r = db.q1("SELECT v FROM settings WHERE k=?", (f"planner:scrapes:{et_day(now)}",))
    return int(r["v"]) if r else 0


def _count_scrapes(now: int, n: int) -> None:
    db.ex("INSERT INTO settings (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = CAST(v AS INTEGER) + ?",
          (f"planner:scrapes:{et_day(now)}", str(n), n))


async def check_counts(posts: list[dict], facts: dict, now: int) -> dict[int, str]:
    """Count checks (each one is a paid scrape, counted toward max_scrapes_per_day). Any error = unreadable."""
    if not posts:
        return {}
    await asyncio.to_thread(_count_scrapes, now, len(posts))
    sem = asyncio.Semaphore(SCRAPES)

    async def one(p):
        async with sem:
            try:
                got = await sources.get_counts(p["platform"], p["url"])
            except Exception:  # noqa: BLE001  unreadable (counts as a miss)
                got = None
        return await asyncio.to_thread(apply_count, p, got, facts, now)
    res = await asyncio.gather(*(one(p) for p in posts), return_exceptions=True)
    return {p["id"]: ("unreadable" if isinstance(r, BaseException) else r) for p, r in zip(posts, res)}


def scrape_order(p: dict) -> tuple:
    """Never checked first (newest upload first: days 1-3 carry the most views), then viral, then the oldest check."""
    if p["checked_at"] is None:
        return 0, -(p["uploaded_at"] or 0), p["id"]
    return (1 if p["reason"] == "viral" else 2), p["checked_at"], p["id"]


# --------------------------------------------------------------------------- #
# Viral (spec 7)
# --------------------------------------------------------------------------- #
def _q75(xs: list[int]) -> float:
    xs = sorted(xs)
    return xs[int(0.75 * (len(xs) - 1))] if xs else math.inf


def _has_viral(p: dict, today: str) -> bool:
    return bool(db.q1(f"SELECT id FROM auto_posts WHERE {ACCT}=? AND platform=? AND "
                      "((reason='viral' AND ramp_start >= ?) OR viral_state='picked') LIMIT 1",
                      (_acct(p), p["platform"], _days(today, -14))))


def pick_viral(camp: dict, posts: list[dict], ok, now: int) -> list[int]:
    """Each newly eligible post is tested once, picked with probability 0.02 / eligible_fraction."""
    today = et_day(now)
    cands = [p for p in posts if ok(p) and p["reason"] == "base" and p["t24"] is not None
             and 86400 <= now - p["uploaded_at"] <= 3 * 86400]
    if not cands:
        return []
    q = {pl: _q75([p["t24"] for p in posts if p["platform"] == pl and p["t24"] is not None])
         for pl in {p["platform"] for p in cands}}
    n_acct = {}

    def posts_of(p):
        k = (_acct(p), p["platform"])
        if k not in n_acct:
            n_acct[k] = db.q1(f"SELECT COUNT(*) n FROM auto_posts WHERE {ACCT}=? "
                              "AND platform=?", k)["n"]
        return n_acct[k]
    elig = [p for p in cands if p["t24"] >= q[p["platform"]] and posts_of(p) >= 3
            and (p["viral_state"] is not None or not _has_viral(p, today))]
    if not elig:
        return []
    prob = min(1.0, 0.02 / (len(elig) / len(cands)))
    picked = []
    for p in elig:
        if p["viral_state"] is not None or _has_viral(p, today):
            continue
        st = "picked" if rng.random() < prob else "tested"
        p["viral_state"] = st
        db.ex("UPDATE auto_posts SET viral_state=? WHERE id=?", (st, p["id"]))
        if st == "picked":
            picked.append(p["id"])
    return picked


def assign_decay(cid: int) -> None:
    """The next 3 posts of a viral post's account, same platform, by upload time, become decay posts
    with peak x U(.60,.70), U(.40,.50), U(.08,.12) (stored in `peak`). Filled as those posts arrive."""
    for v in db.q("SELECT * FROM auto_posts WHERE campaign_id=? AND reason='viral'", (cid,)):
        have = db.q1("SELECT COUNT(*) n FROM auto_posts WHERE viral_parent=?", (v["id"],))["n"]
        if have >= 3:
            continue
        nxt = db.q(f"SELECT id FROM auto_posts WHERE campaign_id=? AND {ACCT}=? "
                   "AND platform=? AND uploaded_at > ? AND reason='base' AND status='active' "
                   "ORDER BY uploaded_at, id LIMIT ?", (cid, _acct(v), v["platform"], v["uploaded_at"], 3 - have))
        for i, r in enumerate(nxt, start=have):
            db.ex("UPDATE auto_posts SET reason='decay', viral_parent=?, peak=? WHERE id=?",
                  (v["id"], round(v["peak"] * rng.uniform(*DECAY[i])), r["id"]))


def approve(post_id: int, peak=None, now: int | None = None, handpick: bool = False) -> dict:
    now = now or _now()
    p = db.q1("SELECT * FROM auto_posts WHERE id=?", (post_id,))
    if not p:
        raise LookupError("no such post")
    if handpick:
        if p["reason"] != "base" or p["status"] != "active":
            raise ValueError(f"post is {p['reason']}/{p['status']}; only an active normal post can be hand-picked")
    elif p["viral_state"] != "picked":
        raise ValueError(f"post is not waiting for approval (viral_state {p['viral_state']})")
    if peak is not None:
        if isinstance(peak, bool) or not isinstance(peak, (int, float)) or not math.isfinite(peak) \
                or not 0 < peak <= MAX_PEAK:
            raise ValueError(f"peak must be a number > 0 and <= {MAX_PEAK:,}")
        peak = round(peak)
    else:
        base = normal_views(p, order_facts(et_day(now), now), now)
        if not base:
            raise ValueError("no normal views for this account yet: type a peak")
        peak = round(base * rng.uniform(20, 35))
    db.ex("UPDATE auto_posts SET reason='viral', viral_state='approved', peak=?, target_total=?, ramp_start=?, "
          "viral_base=? WHERE id=?", (peak, peak, et_day(now), p["observed_views"] or 0, post_id))
    assign_decay(p["campaign_id"])
    return post_view(db.q1("SELECT * FROM auto_posts WHERE id=?", (post_id,)))


def skip(post_id: int) -> dict:
    p = db.q1("SELECT * FROM auto_posts WHERE id=?", (post_id,))
    if not p:
        raise LookupError("no such post")
    if p["viral_state"] != "picked":
        raise ValueError(f"post is not waiting for approval (viral_state {p['viral_state']})")
    db.ex("UPDATE auto_posts SET viral_state='skipped' WHERE id=?", (post_id,))
    return post_view({**p, "viral_state": "skipped"})


def find_post(campaign_id: int, url: str) -> dict | None:
    plat = detect_platform(url) or ""
    k = sources.post_key(plat, url)
    return db.q1("SELECT * FROM auto_posts WHERE campaign_id=? AND (post_key=? OR url_key=?)",
                 (campaign_id, k, url_key(url)))


# --------------------------------------------------------------------------- #
# Targets + rows (spec 4, 5, 6, 8)
# --------------------------------------------------------------------------- #
def plan(camp: dict, now: int, skip_ids=frozenset(), nolike_ids=frozenset(), alert: bool = True) -> dict:
    """Recompute every target, store it, write today's rows (INSERT OR IGNORE). Returns run numbers.
    skip_ids: no row this run (unreadable / not checked yet). nolike_ids: likes unreadable this run (likes_add 0).
    alert: raise the shortfall alert (a run calls plan once per batch and alerts once, after the last).
    A base post gets no row until a baseline (account or campaign normal views) exists."""
    cid, today = camp["id"], et_day(now)
    posts = db.q("SELECT * FROM auto_posts WHERE campaign_id=?", (cid,))
    facts = order_facts(today, now)
    pauses = automation._pause_set()
    cancelled, excluded = ignored(cid)
    out_ = cancelled | excluded
    practice = camp["mode"] != "live"

    def ok(p):   # may get rows
        return p["status"] == "active" and p["creator_id"] not in out_ and not automation._paused(
            pauses, {"campaign_id": cid, "url_key": p["url_key"], "creator_id": p["creator_id"]})

    def fx(p):
        return facts.get(p["url_key"], _EMPTY)

    def obs(p):
        return (p["observed_views"] or 0) + (fx(p)["sim"] if practice else 0)

    picked = pick_viral(camp, posts, ok, now)
    assign_decay(cid)
    posts = db.q("SELECT * FROM auto_posts WHERE campaign_id=?", (cid,))
    ramp = [p for p in posts if ok(p) and in_ramp(p, camp, today)]
    ids = {p["id"] for p in ramp}
    counted = [p for p in posts if p["creator_id"] not in out_]   # v2.14: posting rate skips cancelled / un-ticked creators
    live = [p for p in posts if p["creator_id"] not in excluded and p["status"] != "deleted"]   # views: only un-ticked out   # v2.13: deleted / link-down posts count for nothing
    done = [p for p in live if p["id"] not in ids]
    done_views = sum(obs(p) for p in done)
    special = sum(p["peak"] or 0 for p in ramp if p["reason"] in ("viral", "decay"))
    normal = [p for p in ramp if p["reason"] == "base"]
    days_left = max(0, (_d(camp["end_date"]) - _d(today)).days)   # posting days: buffer days expect no posts
    aim = (camp["views_goal"] or 0) * (1 + (MARGIN_PCT if camp["margin_pct"] is None else camp["margin_pct"]) / 100)
    plats = json.loads(camp["platforms"]) if camp["platforms"] else sorted({p["platform"] for p in posts}) \
        or list(automation.PLATFORMS)
    ppc = json.loads(camp["posts_per_creator"]) if camp["posts_per_creator"] else None
    early = (_d(today) - _d(camp["start_date"])).days < 3
    denom = {}   # platform -> normal posts in ramp + expected future posts
    for pl in set(plats) | {p["platform"] for p in normal}:
        if early:
            r = active_count(camp, out_) * (ppc.get(pl, 0) if ppc else (camp["posts_per_day"] or 0) / len(plats)
                                                  if pl in plats else 0)
        else:
            r = sum(1 for p in counted if p["platform"] == pl and p["uploaded_at"] >= now - 3 * 86400) / 3
        denom[pl] = (sum(1 for p in normal if p["platform"] == pl) + r * days_left, r)
    rate = sum(r for _, r in denom.values())
    if camp["platform_split"]:
        split = json.loads(camp["platform_split"])
    else:   # null = even over the platforms that have posts to fill (a platform nobody posts on takes no share)
        on = [pl for pl, (d, _) in denom.items() if d] or plats
        split = {pl: 100 / len(on) for pl in on}
    def rem_of(pl):
        return max(0, aim * (split.get(pl) or 0) / 100 - sum(obs(p) for p in done if p["platform"] == pl)
                   - sum(p["peak"] or 0 for p in ramp if p["platform"] == pl and p["reason"] in ("viral", "decay")))
    avg_by, rem_all, denom_all = {}, 0.0, 0.0
    for pl, (d, _) in denom.items():
        rem = rem_of(pl)
        avg_by[pl] = rem / d if d else 0
        rem_all, denom_all = rem_all + (rem if d else 0), denom_all + d
    avg = rem_all / denom_all if denom_all else 0
    # v2.14 split gap: a platform the split gives a share but with no posts to fill it (denominator 0): its views are
    # not bought and never moved to the other platforms. Reported (run summary + one alert per campaign per day).
    split_gap = {pl: g for pl, sh in split.items()
                 if sh and days_left and counted and not denom.get(pl, (0, 0))[0] and (g := round(rem_of(pl))) > 0}

    med = {}
    for pl in {p["platform"] for p in ramp}:
        t = [p["t24"] for p in posts if p["platform"] == pl and p["t24"] is not None]
        med[pl] = statistics.median(t) if t else None

    def w(p):
        m = med.get(p["platform"])
        if p["t24"] is None or not m:
            return 1.0
        return min(2.0, max(0.5, math.sqrt(p["t24"] / m))) * p["w_random"]
    mean_w = {pl: statistics.mean(w(p) for p in normal if p["platform"] == pl) for pl in {p["platform"] for p in normal}}
    shortfall = 0
    held = set()

    def normal_target(p):
        nonlocal shortfall
        t = avg_by.get(p["platform"], 0) * w(p) / mean_w.get(p["platform"], 1.0)
        nv = normal_views(p, facts, now)
        if nv is None:   # no baseline yet: base post held, decay post keeps its decay target
            held.add(p["id"])
            return obs(p)
        out = max(min(t, CAP_X * nv), obs(p))
        if p["reason"] == "base":
            shortfall += max(0, t - out)
        return out

    late: dict = {}   # url_key -> [(created_at, delivered views)] of our jobs: delivered after a read = not in observed
    for r in db.q("SELECT url_key, created_at, CASE WHEN completed_at IS NOT NULL THEN views_target "
                  "WHEN views_remaining IS NOT NULL THEN MAX(0, views_target - views_remaining) ELSE 0 END d "
                  "FROM jobs WHERE views_order_id IS NOT NULL AND views_order_id <> '' AND url_key IS NOT NULL "
                  "AND created_at >= ?", (now - 10 * 86400,)):
        late.setdefault(r["url_key"], []).append((r["created_at"], r["d"] or 0))

    def after_read(p):
        return sum(d for c, d in late.get(p["url_key"], ()) if c > (p["checked_at"] or 0))

    rows, planned = [], {r["plan_id"] for r in db.q("SELECT plan_id FROM plan_rows WHERE campaign_id=? AND day=?",
                                                    (cid, today))}
    # v2.18 Custom Comments: owed = floor(views on the post x rate / 1000) - already ordered for the post, only when
    # >= the order minimum, one order <= the service max, lifetime per post <= the pool size.
    cc = {pl: automation.custom_pick(camp, pl) for pl in set(plats) | {p["platform"] for p in ramp}
          if automation.extra_ok(camp, pl, "comments")}
    cc = {pl: x for pl, x in cc.items() if x}
    pool_n = db.q1("SELECT COUNT(*) n FROM comment_pool WHERE campaign_id=?", (cid,))["n"] if cc else 0
    c_done = automation.comments_ordered(practice) if cc else {}
    c_short = 0

    def owed_comments(p) -> int:
        nonlocal c_short
        x, done_ = cc[p["platform"]], c_done.get(p["url_key"], 0)
        lo = max(COMMENT_MIN, x.get("min") or 0)
        want = math.floor(obs(p) * camp["comments_per_1k"] / 1000) - done_
        want = min(want, x.get("max") or want)
        room = pool_n - done_
        if want >= lo and room < want and pool_n:   # an empty list has its own alert
            c_short += 1
        n = min(want, room)
        return n if n >= lo else 0
    for p in ramp:
        if p["reason"] == "viral":
            target = max(p["peak"], obs(p))
            if p["viral_base"] is None:   # approved before v2.10.2: views now = its base, stored once
                p["viral_base"] = p["observed_views"] or 0
                db.ex("UPDATE auto_posts SET viral_base=? WHERE id=? AND viral_base IS NULL", (p["viral_base"], p["id"]))
        elif p["reason"] == "decay":
            target = max(p["peak"], normal_target(p))
        else:
            target = normal_target(p)
        target = round(target)
        db.ex("UPDATE auto_posts SET target_total=? WHERE id=?", (target, p["id"]))
        if p["id"] in skip_ids or p["checked_at"] is None or p["base_views"] is None \
                or (p["reason"] == "base" and p["id"] in held):
            continue
        f = fx(p)
        desired = goal(p, camp, today, target)
        views = max(0, round(desired - obs(p) - f["inflight"] - f["carry"].get(cid, 0) - after_read(p)))
        likes = 0
        if p["observed_likes"] is not None and p["id"] not in nolike_ids:   # hidden / unreadable likes: never guess
            likes = max(0, round(desired * p["p_likes"]) - p["observed_likes"]
                        - (f["sim_likes"] if practice else 0) - f["inflight_likes"])
        # v2.16: comments / shares = ratio per 1k of the views bought today; 0 unless a service AND a ratio are set
        extra = {k: round(views * camp[f"{k}_per_1k"] / 1000) if automation.extra_ok(camp, p["platform"], k) else 0
                 for k in automation.EXTRA}
        if p["platform"] in cc:
            extra["comments"] = owed_comments(p)
        rows.append({"plan_id": f"{cid}:{p['post_key']}:{today}", "campaign_id": cid, "creator_id": p["creator_id"],
                     "post_url": p["url"], "platform": p["platform"],
                     "account_handle": handle(_acct(p), p["platform"], p["account_handle"]),
                     "window_start": et_midnight(today), "window_end": et_midnight(today), "views_add": views,
                     "likes_add": likes, "comments_add": extra["comments"], "shares_add": extra["shares"],
                     "target_total_views": target, "reason": p["reason"],
                     "priority": PRIORITY[p["reason"]]})
    new = [r for r in rows if r["plan_id"] not in planned]
    inserted = automation.insert_plan(new)[0] if new else 0
    replaced = replace_rows([r for r in rows if r["plan_id"] in planned])
    shortfall = round(shortfall)
    c_info = {"comments_short": c_short, "comments_list_empty": bool(cc) and not pool_n}
    if alert:
        shortfall_alert(camp, shortfall)
        split_gap_alert(camp, split_gap, today)
        comments_alert(camp, c_info, today)
    return {**c_info, "rows": inserted, "replaced": replaced, "in_ramp": len(ramp), "held_no_baseline": len(held), "avg": round(avg), "rate": round(rate, 2),
            "avg_by_platform": {k: round(v) for k, v in avg_by.items()}, "views_aim": round(aim),
            "days_left": days_left, "buy_end": buy_end(camp), "done_views": done_views, "special_targets": special,
            "shortfall": shortfall, "split_gap": split_gap, "viral_picked": picked}


def replace_rows(rows: list[dict]) -> int:
    """Today's rows already written: new numbers replace the old ones, atomically per row, only while no auto order
    (any state) exists for that plan_id (rows with an order stay frozen). A carry mark set before any order (below every
    service minimum) follows the new views_add; carry 0 (dropped by a pause) stays 0. -> rows changed."""
    n = 0
    with db._conn() as c:
        for r in rows:
            n += c.execute(
                "UPDATE plan_rows SET views_add=?, likes_add=?, target_total_views=?, reason=?, priority=?, "
                "comments_add=?, shares_add=?, "
                "carry=CASE WHEN carry > 0 THEN ? ELSE carry END WHERE plan_id=? "
                "AND (views_add<>? OR likes_add<>? OR COALESCE(target_total_views,-1)<>? OR COALESCE(reason,'')<>? "
                "OR comments_add<>? OR shares_add<>?) "
                "AND NOT EXISTS (SELECT 1 FROM auto_orders WHERE plan_id=?)",
                (r["views_add"], r["likes_add"], r["target_total_views"], r["reason"], r["priority"],
                 r.get("comments_add", 0), r.get("shares_add", 0), r["views_add"],
                 r["plan_id"], r["views_add"], r["likes_add"], r["target_total_views"], r["reason"],
                 r.get("comments_add", 0), r.get("shares_add", 0), r["plan_id"])).rowcount
    return n


def shortfall_alert(camp: dict, n: int) -> None:
    if n > 0:
        automation.alert("crit", f"Automation: {camp['name']}: " + shortfall_text(n))


def split_gap_text(gap: dict) -> str:
    return ("Platform split: no posts on " + ", ".join(f"{pl} ({n:,} views not bought)" for pl, n in sorted(gap.items()))
            + ". Options: change the platform split, get posts on that platform, accept the gap.")


def split_gap_alert(camp: dict, gap: dict, today: str) -> None:
    """One alert per campaign per ET day (the first run that day that sees a gap, with its numbers)."""
    k = f"planner:splitgap:{camp['id']}"
    if not gap or (db.q1("SELECT v FROM settings WHERE k=?", (k,)) or {}).get("v") == today:
        return
    db.ex("INSERT INTO settings (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, today))
    automation.alert("crit", f"Automation: {camp['name']}: {today}: " + split_gap_text(gap))


def comments_alert(camp: dict, s: dict, today: str) -> None:
    """v2.18: one warning per campaign per ET day each: comments on but the list is empty / too small for N videos."""
    for k, on, text in (("cempty", s.get("comments_list_empty"),
                         "Comments on but list empty: add comment lines on the campaign page, nothing is bought until then."),
                        ("cshort", s.get("comments_short"),
                         f"Comment list smaller than needed on {s.get('comments_short')} videos: add more comment lines.")):
        key = f"planner:{k}:{camp['id']}"
        if not on or (db.q1("SELECT v FROM settings WHERE k=?", (key,)) or {}).get("v") == today:
            continue
        db.ex("INSERT INTO settings (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, today))
        automation.alert("warn", f"Automation: {camp['name']}: {today}: {text}")


def shortfall_text(n: int) -> str:
    return (f"Goal not reachable under the 5x cap: short {n:,} views. "
            "Options: more creators, more viral picks, accept the gap.")


# --------------------------------------------------------------------------- #
# Run
# --------------------------------------------------------------------------- #
def runnable(camp: dict | None) -> dict:
    if not camp:
        raise LookupError("no such campaign")
    if not camp["approved"]:
        raise ValueError("campaign is not approved")
    miss = [k for k in ("views_goal", "start_date", "end_date") if not camp[k]]
    if miss:
        raise ValueError(f"set {', '.join(miss)} first")
    return camp


def _save_run(cid: int, s: dict) -> None:
    db.ex("INSERT INTO settings (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
          (f"planner:run:{cid}", json.dumps(s)))


def last_run(cid: int) -> dict | None:
    r = db.q1("SELECT v FROM settings WHERE k=?", (f"planner:run:{cid}",))
    return json.loads(r["v"]) if r else None


async def run_campaign(cid: int, now: int | None = None) -> dict:
    """One planner run: sources -> new posts -> counts -> viral/decay -> targets -> today's rows."""
    now = now or _now()
    camp = runnable(await asyncio.to_thread(automation._camp, cid))
    today = et_day(now)
    s = {"at": now, "day": today, "new_posts": 0, "checked": 0, "unreadable": 0, "deleted": 0, "rows": 0,
         "errors": []}
    if not (camp["start_date"] <= today <= buy_end(camp)):
        s["errors"].append("outside start_date..end_date + buffer days: nothing planned")
        await asyncio.to_thread(_save_run, cid, s)
        return s
    if cid in _running:
        raise ValueError("planner is already running for this campaign")
    _running.add(cid)
    try:
        got = await sources.collect(camp, _d(today))
        s["errors"] += got["errors"]
        for ln in got["links"]:
            u = await sources.resolve(ln["platform"], ln["url"])
            ln["post_key"] = sources.post_key(ln["platform"], u) if u else None
            if u:
                ln["url"] = u
        await asyncio.to_thread(merge_creators, cid, got)
        s["new_posts"] = await asyncio.to_thread(ingest, camp, got["links"], got["slots"], got["cancelled"], now)
        sw = await sweep(camp)
        ss_deleted = await asyncio.to_thread(mark_deleted, cid, sw)
        await load_external(camp)
        facts = await asyncio.to_thread(order_facts, today, now)
        sel, held = await asyncio.to_thread(pick_scrapes, camp, now)
        res, stale, refresh = await sideshift_counts(camp, [p for p in sel if p["post_key"] in sw], sw, facts, now)
        s["sideshift_checked"] = len(res)
        ss_res = {p["id"]: p for p in refresh}   # old-snapshot re-reads: last in line, a failed read changes nothing
        sel = sorted([p for p in sel if p["post_key"] not in sw] + stale, key=scrape_order) + refresh
        s["capped"] = max(0, len(sel) - (budget := await asyncio.to_thread(scrape_budget, now)))
        sel = budgeted(sel, budget, now)
        pending, skip = {p["id"] for p in sel}, set(held) | {k for k, v in res.items() if v in ("unreadable", "not_found")}
        nolikes = {k for k, v in res.items() if v == "ok_nolikes"}
        # rows for posts already checked go out now; the rest follow batch by batch
        s.update(await asyncio.to_thread(plan, camp, now, frozenset(pending | skip), frozenset(), False))
        rows, replaced = s["rows"], s["replaced"]
        for i in range(0, len(sel), BATCH):
            batch = sel[i:i + BATCH]
            r = await check_counts(batch, facts, now)
            for k, v in list(r.items()):
                if k in ss_res and v in ("unreadable", "not_found"):
                    await asyncio.to_thread(db.ex, "UPDATE auto_posts SET misses=0, skip_next=0, not_found=0 WHERE id=?",
                                            (k,))   # back to the SideShift read's state
                    r[k] = ss_res[k]["_ss"]
            res.update(r)
            pending -= {p["id"] for p in batch}
            skip |= {k for k, v in r.items() if v in ("unreadable", "not_found")}
            nolikes |= {k for k, v in r.items() if v == "ok_nolikes"}
            s.update(await asyncio.to_thread(plan, camp, now, frozenset(pending | skip), frozenset(nolikes), False))
            rows += s["rows"]
            replaced += s["replaced"]
        s["rows"], s["replaced"] = rows, replaced
        shortfall_alert(camp, s["shortfall"])   # once per run, final number
        split_gap_alert(camp, s.get("split_gap") or {}, today)
        comments_alert(camp, s, today)
        s["checked"] = len(res)
        s["unreadable"] = sum(1 for v in res.values() if v == "unreadable")
        s["deleted"] = sum(1 for v in res.values() if v == "deleted") + ss_deleted
        s["backoff"] = len(held)
    except Exception as e:  # noqa: BLE001
        log.exception("planner run %s failed", cid)
        s["errors"].append(f"{type(e).__name__}: {e!s}"[:200])
    finally:
        _running.discard(cid)
        await asyncio.to_thread(_save_run, cid, s)
    return s


def reset(cid: int) -> dict:
    """Practice campaign only: delete its posts, plan rows, simulated/blocked orders, creators, slots and run state
    in one transaction (campaign row and inputs kept). Any other order state (it reached a panel) = refused."""
    camp = automation._camp(cid)
    if not camp:
        raise LookupError("no such campaign")
    if camp["mode"] != "practice":
        raise ValueError("only practice campaigns can be reset")
    orders = "FROM auto_orders WHERE (campaign_id=? OR plan_id IN (SELECT plan_id FROM plan_rows WHERE campaign_id=?))"
    with db._conn() as c:
        c.execute("BEGIN IMMEDIATE")
        real = c.execute(f"SELECT COUNT(*) {orders} AND state NOT IN ('simulated','blocked')", (cid, cid)).fetchone()[0]
        if real:
            raise ValueError(f"campaign has {real} order(s) that reached a panel: not reset")
        n = {"orders": c.execute(f"DELETE {orders}", (cid, cid)).rowcount,
             "plan_rows": c.execute("DELETE FROM plan_rows WHERE campaign_id=?", (cid,)).rowcount,
             "posts": c.execute("DELETE FROM auto_posts WHERE campaign_id=?", (cid,)).rowcount}
        for t in ("auto_creators", "auto_slots", "auto_creator_map", "comment_used"):
            c.execute(f"DELETE FROM {t} WHERE campaign_id=?", (cid,))
        c.execute("UPDATE comment_pool SET times_used=0 WHERE campaign_id=?", (cid,))   # v2.18: list kept, uses reset
        c.execute("DELETE FROM settings WHERE k IN (?, ?, ?, ?, ?)", (f"planner:run:{cid}", f"planner:splitgap:{cid}",
                                                                       f"planner:frozen:{cid}", f"planner:cempty:{cid}",
                                                                       f"planner:cshort:{cid}"))
    _EXT.pop(cid, None)
    return {"deleted": n}


_tasks: set = set()


def spawn(cid: int) -> None:
    """Background run (manual route): scrapes can take minutes; progress shows in the dashboard."""
    async def go():
        try:
            await run_campaign(cid)
        except (LookupError, ValueError) as e:
            log.info("planner run %s not started: %s", cid, e)
    t = asyncio.create_task(go())
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


RESERVE = 0.2   # share of a run's checks kept for never-checked posts >= 3 days old (baselines early)


def budgeted(sel: list[dict], budget: int, now: int) -> list[dict]:
    """sel (in scrape_order) cut to budget, RESERVE of it first for never-checked posts >= 3 days old (newest first);
    an unused reserve goes to the rest in scrape_order."""
    old = [p for p in sel if p["checked_at"] is None and p["uploaded_at"] <= now - 3 * 86400][:int(budget * RESERVE)]
    ids = {p["id"] for p in old}
    return old + [p for p in sel if p["id"] not in ids][:budget - len(old)]


def scrape_budget(now: int) -> int:
    return max(0, min(setting("max_scrapes_per_run"), setting("max_scrapes_per_day") - scrapes_today(now)))


def pick_scrapes(camp: dict, now: int) -> tuple[list[dict], list[int]]:
    """-> (posts to check, in scrape_order; ids sitting out this run). Checked: never-checked posts and posts in
    ramp of creators not cancelled; never an un-ticked creator's posts (v2.14). Backoff: a post read as unreadable sits out the next run (its flag is
    cleared here); MAX_MISSES in a row = not checked again (no rows) until its ramp ends."""
    today = et_day(now)
    cancelled, excluded = ignored(camp["id"])
    sel, held = [], []
    for p in db.q("SELECT * FROM auto_posts WHERE campaign_id=? AND status='active'", (camp["id"],)):
        if p["creator_id"] in excluded or not (p["checked_at"] is None or (in_ramp(p, camp, today)
                                                                           and p["creator_id"] not in cancelled)):
            continue
        if p["misses"] >= MAX_MISSES:
            held.append(p["id"])
        elif p["skip_next"]:
            held.append(p["id"])
            db.ex("UPDATE auto_posts SET skip_next=0 WHERE id=?", (p["id"],))
        else:
            sel.append(p)
    return sorted(sel, key=scrape_order), held


def validate_times(body: dict) -> None:
    """check_times ["HH:MM"...] and slot_deadlines {AM?, PM?: "HH:MM"}; raises ValueError."""
    def ok(t):
        return isinstance(t, str) and len(t) == 5 and t[2] == ":" and t[:2].isdigit() and t[3:].isdigit() \
            and int(t[:2]) < 24 and int(t[3:]) < 60
    ct = body.get("check_times")
    if ct is not None and (not isinstance(ct, list) or not ct or not all(ok(t) for t in ct)):
        raise ValueError('check_times must be a non-empty list of "HH:MM" (ET) or null')
    sd = body.get("slot_deadlines")
    if sd is not None and (not isinstance(sd, dict) or set(sd) - {"AM", "PM"} or not all(ok(t) for t in sd.values())):
        raise ValueError('slot_deadlines must be {"AM": "HH:MM", "PM": "HH:MM"} or null')


def due(camp: dict, now: int) -> bool:
    """A check time (ET, today or yesterday) passed since the last run."""
    last = (last_run(camp["id"]) or {}).get("at", 0)
    times = json.loads(camp["check_times"]) if camp["check_times"] else CHECK_TIMES
    today = et_day(now)
    for day in (_days(today, -1), today):
        for t in times:
            h, m = (int(x) for x in t.split(":"))
            ts = int(dt.datetime.combine(_d(day), dt.time(h, m), ET).timestamp())
            if last < ts <= now:
                return True
    return False


async def refresh_viral(now: int) -> None:
    """Viral posts in their first 3 days: counts every 6 h (between the twice-daily runs)."""
    rows = await asyncio.to_thread(
        db.q, "SELECT * FROM auto_posts WHERE reason='viral' AND status='active' AND ramp_start >= ? "
              "AND COALESCE(checked_at, 0) < ? AND misses < ? AND NOT EXISTS (SELECT 1 FROM auto_creators a WHERE "
              "a.campaign_id=auto_posts.campaign_id AND a.creator_id=auto_posts.creator_id AND a.included=0) "
              "ORDER BY checked_at", (_days(et_day(now), -2),
                                                                                    now - 6 * 3600, MAX_MISSES))
    rows = rows[:await asyncio.to_thread(scrape_budget, now)]
    if rows:
        await check_counts(rows, await asyncio.to_thread(order_facts, et_day(now), now), now)


async def tick() -> None:
    """Scheduler entry. Does nothing unless AUTO_ENABLED=1."""
    if not automation.enabled():
        return
    now = _now()
    for c in await asyncio.to_thread(db.q, "SELECT * FROM auto_campaigns WHERE approved=1"):
        try:
            if c["id"] not in _running and await asyncio.to_thread(due, c, now):
                await run_campaign(c["id"], now)
        except Exception as e:  # noqa: BLE001  one bad campaign (eg bad check_times) must not stop the others
            log.warning("planner skipped campaign %s: %s", c["id"], e)
    await refresh_viral(now)


# --------------------------------------------------------------------------- #
# Views (API-CONTRACT v2.8)
# --------------------------------------------------------------------------- #
POST_KEYS = ("id", "post_key", "campaign_id", "creator_id", "account_handle", "platform", "url", "source", "slot",
             "day", "uploaded_at", "t24", "reason", "peak", "target_total", "ramp_start", "viral_state",
             "viral_parent", "status", "observed_views", "observed_likes", "checked_at", "base_views", "base_day",
             "viral_base")


def post_view(p: dict, pauses: set | None = None) -> dict:
    pauses = automation._pause_set() if pauses is None else pauses
    return {**{k: p[k] for k in POST_KEYS}, "account_handle": handle(_acct(p), p["platform"], p["account_handle"]),
            "paused": automation._paused(
        pauses, {"campaign_id": p["campaign_id"], "url_key": p["url_key"], "creator_id": p["creator_id"]})}


def viral_lists(campaign_id: int | None, now: int | None = None) -> dict:
    now = now or _now()
    today = et_day(now)
    where, args = ("AND campaign_id=?", (campaign_id,)) if campaign_id else ("", ())
    facts = order_facts(today, now)
    camps = {c["id"]: c for c in db.q("SELECT * FROM auto_campaigns")}
    pauses = automation._pause_set()
    picks = []
    for p in db.q(f"SELECT * FROM auto_posts WHERE viral_state='picked' {where} ORDER BY uploaded_at", args):
        nv = normal_views(p, facts, now)
        picks.append({**post_view(p, pauses), "normal_views": None if nv is None else round(nv),
                      "peak_range": None if not nv else [round(nv * 20), round(nv * 35)]})
    prog = []
    for p in db.q(f"SELECT * FROM auto_posts WHERE reason='viral' {where} ORDER BY ramp_start DESC", args):
        c = camps.get(p["campaign_id"])
        if not c or not c["end_date"] or not in_ramp(p, c, today):
            continue
        f = facts.get(p["url_key"], _EMPTY)
        decay = [{"post_id": d["id"], "url": d["url"], "target": d["peak"]}
                 for d in db.q("SELECT id, url, peak FROM auto_posts WHERE viral_parent=? ORDER BY uploaded_at",
                               (p["id"],))]
        prog.append({**post_view(p, pauses), "ramp_day": ramp_day(p, today), "ramp_days": ramp_len(p, c),
                     "delivered": f["delivered"] + (f["sim"] if c["mode"] != "live" else 0), "decay": decay})
    return {"picks": picks, "in_progress": prog}


def dashboard(cid: int, now: int | None = None) -> dict:
    now = now or _now()
    camp = automation._camp(cid)
    if not camp:
        raise LookupError("no such campaign")
    today = et_day(now)
    posts = db.q("SELECT * FROM auto_posts WHERE campaign_id=?", (cid,))
    cancelled, excluded = ignored(cid)
    out_ = cancelled | excluded
    counted = [p for p in posts if p["creator_id"] not in out_]   # v2.14: posting rate skips cancelled / un-ticked creators
    goal = camp["views_goal"] or 0
    achieved = sum(p["observed_views"] or 0 for p in posts   # views: cancelled still count, un-ticked do not (v2.14)
                   if p["creator_id"] not in excluded and p["status"] != "deleted")   # v2.13: removed posts drop out
    recent = [p for p in counted if p["uploaded_at"] >= now - 3 * 86400]
    plan_rate = active_count(camp, out_) * (camp["posts_per_day"] or 0)   # v2.16: minus ignored within planned
    real_rate = round(len(recent) / 3, 2)
    run = last_run(cid)
    dl = json.loads(camp["slot_deadlines"]) if camp["slot_deadlines"] else None
    missed = [{"creator_id": s["creator_id"], "day": s["day"], "slot": s["slot"]}
              for s in db.q("SELECT * FROM auto_slots WHERE campaign_id=? AND url IS NULL AND day >= ? "
                            "ORDER BY day DESC, creator_id, slot", (cid, _days(today, -2)))
              if s["creator_id"] not in out_ and sources.slot_ts(s["day"], s["slot"], dl) <= now]
    by: dict[str, list] = {}
    for m in missed:
        by.setdefault(m["creator_id"], []).append(m)
    cmap = db.q("SELECT name, creator_id FROM auto_creator_map WHERE campaign_id=?", (cid,))
    shown = {m["creator_id"]: m["name"] for m in cmap}
    try:
        has_ss = any(src == "sideshift" for src, _ in sources.refs(camp))
    except sources.SourceError:
        has_ss = False
    unmatched = sorted({r["creator_id"] for r in db.q("SELECT DISTINCT creator_id FROM auto_slots WHERE campaign_id=?",
                                                       (cid,))} - set(shown) - excluded) if has_ss else []
    slacking = sorted(({"creator_id": k, "missed": len(v), "slots": v, "message": slack_message(shown.get(k, k), v)}
                       for k, v in by.items()), key=lambda x: (-x["missed"], x["creator_id"]))
    alerts = []
    if run and run.get("shortfall"):
        alerts.append({"level": "crit", "text": shortfall_text(run["shortfall"])})
    if run and run.get("split_gap"):
        alerts.append({"level": "crit", "text": split_gap_text(run["split_gap"])})
    if plan_rate and real_rate < 0.8 * plan_rate:
        alerts.append({"level": "warn", "text": f"Posting rate {real_rate}/day is below 80% of plan ({plan_rate}/day)"})
    if run and run.get("comments_list_empty"):   # v2.20: comment-pool warnings also on the campaign dashboard
        alerts.append({"level": "warn", "text": "Comments on but list empty: add comment lines, nothing is bought until then."})
    if run and run.get("comments_short"):
        alerts.append({"level": "warn", "text": f"Comment list smaller than needed on {run['comments_short']} videos: "
                                                "add more comment lines."})
    days_left = max(0, (_d(camp["end_date"]) - _d(today)).days) if camp["end_date"] else None
    return {"campaign_id": cid, "goal": goal, "achieved": achieved, "gap": max(0, goal - achieved),
            "pct_done": round(100 * achieved / goal, 1) if goal else None, "days_left": days_left,
            "buffer_days": automation.BUFFER_DAYS, "buy_end": buy_end(camp) if camp["end_date"] else None,
            "shortfall": (run or {}).get("shortfall", 0),
            "creators": {"planned": camp["creators_planned"],
                         "active": len({p["creator_id"] for p in recent}),
                         "cancelled": sorted(cancelled), "excluded": sorted(excluded)},
            "posting_rate": {"plan": plan_rate, "real": real_rate, "low": bool(plan_rate and real_rate < 0.8 * plan_rate)},
            "submissions": {d: sum(1 for p in posts if p["day"] == _days(today, -i))
                            for i, d in enumerate(("today", "yesterday", "day_before"))},
            "missed_slots": missed, "slacking": slacking, "unmatched_creators": unmatched,
            "viral": viral_lists(cid, now),
            "posts": {"total": len(posts), "deleted": sum(1 for p in posts if p["status"] == "deleted")},
            "scrapes": {"today": scrapes_today(now), "max_per_run": setting("max_scrapes_per_run"),
                        "max_per_day": setting("max_scrapes_per_day")},
            "last_run": run, "running": cid in _running, "alerts": alerts}


def creators(cid: int) -> dict:
    """v2.14 include list: every creator the campaign knows (posts, slots, auto_creators rows; SideShift contractors and
    sheet names, by canonical id) with included / cancelled. No row = included."""
    if not automation._camp(cid):
        raise LookupError("no such campaign")
    flags = {r["creator_id"]: r for r in db.q("SELECT * FROM auto_creators WHERE campaign_id=?", (cid,))}
    names = {r["creator_id"]: r["name"] for r in db.q("SELECT name, creator_id FROM auto_creator_map WHERE campaign_id=?",
                                                      (cid,))}
    stats = {r["c"]: r for r in db.q("SELECT creator_id c, COUNT(*) n, GROUP_CONCAT(DISTINCT platform) pl, "
                                     "MAX(source='sideshift') ss FROM auto_posts WHERE campaign_id=? GROUP BY creator_id",
                                     (cid,))}
    ids = set(flags) | set(stats) | {r["creator_id"] for r in db.q("SELECT DISTINCT creator_id FROM auto_slots "
                                                                    "WHERE campaign_id=?", (cid,))}
    out = []
    for k in ids:
        st, f = stats.get(k), flags.get(k)
        pls = sorted((st["pl"] or "").split(",")) if st else []
        out.append({"creator_id": k, "name": names.get(k), "handle": handle(k, pls[0]) if pls else None,
                    "platforms": pls, "posts": st["n"] if st else 0,
                    "source": "sideshift" if st and st["ss"] else "sheet",
                    "included": not f or bool(f["included"]), "cancelled": bool(f and f["cancelled"])})
    out.sort(key=lambda x: str(x["name"] or x["handle"] or x["creator_id"]).lower())
    return {"campaign_id": cid, "items": out}


def set_included(cid: int, body: dict) -> dict:
    """{"included": {creator_id: bool}} -> {"changed": {creator_id: bool}}. Unknown creator / non-bool = ValueError."""
    want = body.get("included") if isinstance(body, dict) else None
    if not isinstance(want, dict) or not want:
        raise ValueError('body must be {"included": {"<creator_id>": true|false, ...}}')
    cur = {x["creator_id"]: x["included"] for x in creators(cid)["items"]}
    bad = [k for k in want if k not in cur]
    if bad:
        raise ValueError(f"not a creator of this campaign: {', '.join(map(str, bad[:5]))}")
    if any(not isinstance(v, bool) for v in want.values()):
        raise ValueError("each value must be true or false")
    changed = {k: v for k, v in want.items() if cur[k] != v}
    with db._conn() as c:
        for k, v in changed.items():
            c.execute("INSERT INTO auto_creators (campaign_id, creator_id, included) VALUES (?,?,?) "
                      "ON CONFLICT(campaign_id, creator_id) DO UPDATE SET included=excluded.included", (cid, k, int(v)))
    return {"changed": changed}


def slack_message(creator: str, slots: list[dict]) -> str:
    days: dict[str, list[str]] = {}
    for s in slots:
        days.setdefault(s["day"], []).append(s["slot"])
    lines = "; ".join(f"{dt.date.fromisoformat(d).strftime('%a %d %b')}: {', '.join(v)}" for d, v in sorted(days.items()))
    return (f"Hi {creator}, we are missing {len(slots)} post{'s' if len(slots) != 1 else ''} from you ({lines}). "
            "Please post them and add the links to the sheet as soon as you can. Thanks.")


# --------------------------------------------------------------------------- #
# v2.15 Dashboard detail (Milan spec section 5, D7). Reads only, except freeze() (called from Go Live).
# --------------------------------------------------------------------------- #
def _ppc(camp: dict) -> dict:
    """Posts per creator per day by platform (v2.12 field; older campaigns: posts_per_day spread over platforms)."""
    if camp["posts_per_creator"]:
        return {pl: n for pl, n in json.loads(camp["posts_per_creator"]).items() if n}
    plats = json.loads(camp["platforms"]) if camp["platforms"] else []
    return {pl: (camp["posts_per_day"] or 0) / len(plats) for pl in plats} if plats and camp["posts_per_day"] else {}


def _span(camp: dict) -> list[str]:
    """start_date .. end_date + buffer days."""
    if not (camp["start_date"] and camp["end_date"]):
        return []
    return [_days(camp["start_date"], i) for i in range((_d(buy_end(camp)) - _d(camp["start_date"])).days + 1)]


def _aim(camp: dict) -> float:
    return (camp["views_goal"] or 0) * (1 + (MARGIN_PCT if camp["margin_pct"] is None else camp["margin_pct"]) / 100)


def _slots(ppc: float, i: int) -> int:
    """Expected posts on posting day i (0-based) for ppc posts per day: fractions spread over days (0.5 = 0, 1, 0, 1)."""
    return math.floor(ppc * (i + 1) + 1e-9) - math.floor(ppc * i + 1e-9)


def forecast(camp: dict, now: int) -> dict:
    """D7: what the planner aims each post at, recomputed at every run. posts: {post id: target views} (the planner's
    target_total; a post without one yet = its platform's avg). Future slots (today .. end_date not posted yet) =
    avg_by_platform of the last run (no run yet: views aim x split / expected videos on that platform). days:
    {day: {platform: views}} by upload day. Counted posts only (not deleted, creator not un-ticked)."""
    cid, today = camp["id"], et_day(now)
    cancelled, excluded = ignored(cid)
    ppc, span = _ppc(camp), _span(camp)
    posting = [d for d in span if d <= (camp["end_date"] or "")]
    split = json.loads(camp["platform_split"]) if camp["platform_split"] else {pl: 100 / len(ppc) for pl in ppc}
    creators_ = active_count(camp, cancelled | excluded)
    avg_by = dict((last_run(cid) or {}).get("avg_by_platform") or {})
    for pl, n in ppc.items():
        if pl not in avg_by:
            videos = creators_ * n * len(posting)
            avg_by[pl] = round(_aim(camp) * (split.get(pl) or 0) / 100 / videos) if videos else 0
    posts = [p for p in db.q("SELECT * FROM auto_posts WHERE campaign_id=?", (cid,))
             if p["creator_id"] not in excluded and p["status"] != "deleted"]
    fp = {p["id"]: p["target_total"] if p["target_total"] is not None else avg_by.get(p["platform"], 0) for p in posts}
    days = {d: {pl: 0 for pl in ppc} for d in span}
    seen: dict = {}
    for p in posts:
        if p["day"] in days:
            days[p["day"]][p["platform"]] = days[p["day"]].get(p["platform"], 0) + fp[p["id"]]
            seen[(p["day"], p["platform"])] = seen.get((p["day"], p["platform"]), 0) + 1
    for i, d in enumerate(posting):
        if d >= today:
            for pl, n in ppc.items():
                left = max(0, creators_ * n - seen.get((d, pl), 0))
                days[d][pl] = days[d].get(pl, 0) + left * avg_by.get(pl, 0)
    return {"avg_by_platform": avg_by, "posts": fp, "days": {d: {k: round(v) for k, v in x.items()} for d, x in days.items()}}


def freeze(cid: int, now: int | None = None) -> bool:
    """Go Live: keep the forecast as it is now, for compare (first Go Live wins; practice reset clears it)."""
    camp = automation._camp(cid)
    if not camp or not camp["start_date"] or not camp["end_date"]:
        return False
    now = now or _now()
    f = forecast(camp, now)
    v = json.dumps({"at": now, "avg_by_platform": f["avg_by_platform"], "posts": f["posts"], "days": f["days"]})
    return db.ex("INSERT OR IGNORE INTO settings (k, v) VALUES (?, ?)", (f"planner:frozen:{cid}", v))[0] == 1


def frozen(cid: int) -> dict | None:
    r = db.q1("SELECT v FROM settings WHERE k=?", (f"planner:frozen:{cid}",))
    return json.loads(r["v"]) if r else None


def detail(cid: int, now: int | None = None) -> dict:
    """GET .../detail: per day, per creator, per video, per slot and spend aggregates for the dashboard tabs.
    Counting rules as the dashboard: deleted posts and un-ticked creators' views do not count, cancelled creators' do."""
    now = now or _now()
    camp = automation._camp(cid)
    if not camp:
        raise LookupError("no such campaign")
    today, live = et_day(now), camp["mode"] == "live"
    cancelled, excluded = ignored(cid)
    ppc, span = _ppc(camp), _span(camp)
    end = camp["end_date"] or ""
    posting = [d for d in span if d <= end]
    aim = _aim(camp)
    total_videos = round((camp["creators_planned"] or 0) * sum(ppc.values()) * len(posting))
    fc = forecast(camp, now) if span else {"avg_by_platform": {}, "posts": {}, "days": {}}
    fz = frozen(cid)
    posts = [p for p in db.q("SELECT * FROM auto_posts WHERE campaign_id=? ORDER BY uploaded_at, id", (cid,))
             if p["creator_id"] not in excluded]
    counted = [p for p in posts if p["status"] != "deleted"]
    achieved = sum(p["observed_views"] or 0 for p in counted)

    # spend: real orders on a live campaign, simulated ones in practice (as the campaign header)
    sw = automation.LIVE_SPEND_O if live else "o.state = 'simulated'"
    orders = db.q(f"SELECT o.day, o.kind, o.qty, o.est_cost, o.url_key, p.platform, p.creator_id FROM auto_orders o "
                  f"LEFT JOIN plan_rows p ON p.plan_id = o.plan_id WHERE o.campaign_id=? AND {sw}", (cid,))
    spent = round(sum(o["est_cost"] or 0 for o in orders), 2)
    # cap left is always real money: a campaign moved back to Practice keeps its real spend against the cap
    real_spent = spent if live else (db.q1(f"SELECT COALESCE(SUM(est_cost),0) s FROM auto_orders WHERE campaign_id=? AND "
                                          f"{automation.LIVE_SPEND}", (cid,))["s"] or 0)
    by_key, by_cr, by_pl, by_day = {}, {}, {}, {}
    for o in orders:
        c = o["est_cost"] or 0
        by_key[o["url_key"]] = by_key.get(o["url_key"], 0) + c
        by_cr[o["creator_id"]] = by_cr.get(o["creator_id"], 0) + c
        by_pl[o["platform"]] = by_pl.get(o["platform"], 0) + c
        by_day[o["day"]] = by_day.get(o["day"], 0) + c
    bought = sum(o["qty"] for o in orders if o["kind"] == "views")
    projected = round(spent + max(0, aim - achieved) * spent / bought, 2) if bought else None
    future = [d for d in span if d >= today]
    sched = (projected - spent) / len(future) if projected is not None and future else 0
    cap = camp["cap_usd"]
    by_kind: dict = {}
    for o in orders:
        by_kind[o["kind"]] = by_kind.get(o["kind"], 0) + (o["est_cost"] or 0)
    lines: dict = {}   # v2.18: the custom comment lines that went on each video
    for r in db.q("SELECT url_key, text FROM comment_used WHERE campaign_id=? ORDER BY id", (cid,)):
        lines.setdefault(r["url_key"], []).append(r["text"])

    videos = [{"id": p["id"], "url": p["url"], "platform": p["platform"], "creator_id": p["creator_id"],
               "handle": p["account_handle"], "day": p["day"], "uploaded_at": p["uploaded_at"],
               "views": p["observed_views"], "likes": p["observed_likes"], "comments": p.get("observed_comments"), "shares": None,
               "status": p["status"], "reason": p["reason"], "forecast": fc["posts"].get(p["id"]),
               "forecast_frozen": (fz or {}).get("posts", {}).get(str(p["id"])),
               "spend": round(by_key.get(p["url_key"], 0), 4), "comment_lines": lines.get(p["url_key"], [])} for p in posts]

    plats = sorted(set(ppc) | {p["platform"] for p in posts})
    actual = {d: {pl: 0 for pl in plats} for d in span}
    for p in counted:
        if p["day"] in actual:
            actual[p["day"]][p["platform"]] += p["observed_views"] or 0
    flat = aim / len(posting) if posting else 0
    target = {d: (sum(fz["days"].get(d, {}).values()) if fz else (flat if d <= end else 0)) for d in span}

    crs = creators(cid)["items"]
    per: dict = {}
    for p in posts:
        x = per.setdefault(p["creator_id"], {"videos": 0, "views": 0})
        if p["status"] != "deleted":
            x["videos"] += 1
            x["views"] += p["observed_views"] or 0
    creators_out = []
    for c in crs:
        x = per.get(c["creator_id"], {"videos": 0, "views": 0})
        creators_out.append({**c, "videos": x["videos"], "views": x["views"],
                             "avg_views": round(x["views"] / x["videos"]) if x["videos"] else None,
                             "spend": round(by_cr.get(c["creator_id"], 0), 2)})

    # timeline: rows = included creators, one square per expected slot per platform per day (+ extra posts)
    by_slot: dict = {}
    for p in posts:
        by_slot.setdefault((p["creator_id"], p["day"], p["platform"]), []).append(p)
    rows, counts = [], {k: 0 for k in ("posted", "pending", "missed", "down", "cancelled", "buffer")}
    for c in creators_out:
        if not c["included"]:
            continue
        cells, issues = {}, False
        for i, d in enumerate(span):
            cells[d] = {}
            for pl in plats:
                n = _slots(ppc.get(pl, 0), i) if d <= end else 0
                sq = [{"s": "down" if p["status"] == "deleted" else "posted", "id": p["id"]}
                      for p in by_slot.get((c["creator_id"], d, pl), [])]
                if d > end:
                    sq += [{"s": "buffer"}] * _slots(ppc.get(pl, 0), i)
                else:
                    sq += [{"s": "cancelled" if c["cancelled"] else "missed" if d < today else "pending"}] * max(0, n - len(sq))
                for q in sq:
                    counts[q["s"]] += 1
                    issues = issues or q["s"] in ("missed", "down", "cancelled")
                if sq:
                    cells[d][pl] = sq
        rows.append({"creator_id": c["creator_id"], "name": c["name"], "handle": c["handle"],
                     "cancelled": c["cancelled"], "issues": issues, "cells": cells})

    known = [c for c in crs if c["included"] and not c["cancelled"]]
    return {
        "campaign_id": cid, "mode": camp["mode"], "today": today, "days": span, "posting_days": len(posting),
        "buffer_days": automation.BUFFER_DAYS, "platforms": plats,
        "kpi": {"goal": camp["views_goal"] or 0, "achieved": achieved, "pending": max(0, (camp["views_goal"] or 0) - achieved),
                "posting_days_left": max(0, (_d(end) - _d(today)).days) if end else None,
                "spent": spent, "cap": cap, "avg_views": round(achieved / len(counted)) if counted else None,
                "avg_needed": round(aim / total_videos) if total_videos else None},
        "setup": {"creators_planned": camp["creators_planned"], "creators_active": len(known),
                  "posts_per_creator": ppc, "posting_days": len(posting), "buffer_days": automation.BUFFER_DAYS,
                  "total_videos": total_videos, "views_aim": round(aim),
                  "avg_needed": round(aim / total_videos) if total_videos else None},
        "per_day": [{"day": d, "buffer": d > end, "actual": actual[d], "forecast": fc["days"].get(d, {}),
                     "target": round(target[d])} for d in span],
        "forecast": {"avg_by_platform": fc["avg_by_platform"], "frozen_at": (fz or {}).get("at")},
        "creators": creators_out, "videos": videos,
        "timeline": {"rows": rows, "counts": counts},
        "spend": {"simulated": not live, "spent": spent, "cap": cap,
                  "cap_left": round(cap - real_spent, 2) if cap is not None else None, "projected": projected,
                  "by_kind": {k: round(by_kind.get(k, 0), 2) for k in automation.KINDS},
                  "videos_paid": sum(1 for v in by_key.values() if v > 0),
                  "avg_per_video": round(spent / sum(1 for v in by_key.values() if v > 0), 2) if spent else None,
                  "per_1k_views": round(spent / achieved * 1000, 4) if achieved and spent else None,
                  "by_day": [{"day": d, "spent": round(by_day.get(d, 0), 2), "scheduled": round(sched, 2) if d >= today else 0}
                             for d in span],
                  "unattributed": round(spent - sum(round(by_cr.get(c["creator_id"], 0), 2) for c in crs), 2),
                  "by_platform": {pl: {"spent": round(by_pl.get(pl, 0), 2), "views": sum(actual[d][pl] for d in span),
                                       "per_1k_views": round(by_pl.get(pl, 0) / v * 1000, 4) if (v := sum(
                                           actual[d][pl] for d in span)) and by_pl.get(pl) else None} for pl in plats}}}
