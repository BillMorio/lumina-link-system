"""Phase 2: campaign analytics over the link tracker (pure functions, no I/O).

Input rows come from pg: links with their latest snapshot, plus every snapshot.
Days are ET calendar days, like the planner.
"""
from __future__ import annotations

import datetime as dt
import os
import statistics
import time
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
VIRAL_X = float(os.getenv("TRACKER_VIRAL_X", "6"))   # x the creator's normal reach
VIRAL_MIN_PRIOR = 3                                   # earlier videos needed to judge "normal"
VIRAL_WINDOW = 8                                      # normal reach = median of up to this many earlier videos


def et_day(ts: int | None) -> str | None:
    return dt.datetime.fromtimestamp(int(ts), ET).date().isoformat() if ts else None


def _days(a: str, b: str) -> list[str]:
    d0, d1 = dt.date.fromisoformat(a), dt.date.fromisoformat(b)
    return [(d0 + dt.timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)] if d1 >= d0 else []


def acct_key(l: dict) -> str:
    """An account = one creator on one platform (IG and FB reach differ, so each gets its own normal + type)."""
    return f"{l.get('creator_name') or '?'}|{l.get('platform') or 'other'}"


MATURE_SEC = int(float(os.getenv("TRACKER_MATURE_H", "48")) * 3600)   # younger videos are still collecting views


def is_mature(l: dict, now: float) -> bool:
    up = l.get("uploaded_at")
    return up is None or now - up >= MATURE_SEC


def mark_viral(links: list[dict], now: float | None = None) -> None:
    """Sets l["viral"], l["normal"] (the account's normal reach for that video) and l["mature"].
    normal = the higher of (a) the earlier videos' form: max(median of up to 8, median of last 3) — so a
    steady ramp is not a spike — and (b) the median of the account's OTHER videos — so a spike early in a
    short series still stands out. Only mature videos (>= MATURE_SEC old) shape normal; a video is judged
    once the account has 2+ other mature videos (3 in all)."""
    now = time.time() if now is None else now
    by: dict[str, list[dict]] = {}
    for l in links:
        by.setdefault(acct_key(l), []).append(l)
    for vids in by.values():
        vids.sort(key=lambda l: (l.get("uploaded_at") is None, l.get("uploaded_at") or 0))
        for l in vids:
            l["mature"] = is_mature(l, now)
        ripe = [(i, l["views"]) for i, l in enumerate(vids) if l.get("views") is not None and l["mature"]]
        for i, l in enumerate(vids):
            v = l.get("views")
            others = [x for j, x in ripe if j != i]
            prior = [x for j, x in ripe if j < i]
            cand = []
            if len(prior) >= 2:
                cand.append(max(statistics.median(prior[-VIRAL_WINDOW:]), statistics.median(prior[-3:])))
            if len(others) >= VIRAL_MIN_PRIOR - 1:
                cand.append(statistics.median(others))
            normal = max(cand) if cand else None
            l["normal"] = round(normal) if normal is not None else None
            l["viral"] = bool(v is not None and normal and v >= VIRAL_X * normal)


# Account types (Phase 3) — names + blurbs from the client's forecast mock; rules classify REAL series.
TYPES = {
    "hot":         ("Multi-viral", "Hits often. Several breakouts stack on top of each other."),
    "breakout":    ("Single breakout", "Quiet, then one video blows up and lifts everything after it."),
    "established": ("Established mid-size", "Already has an audience. High, steady numbers with the odd viral."),
    "consistent":  ("Consistent", "Same range every day. Very few surprises up or down."),
    "climber":     ("Steady climber", "Starts small and grows a bit with every video."),
    "slow":        ("Slow starter", "Tiny reach at first, then builds fast later on."),
    "under":       ("Underperformer", "Low and flat. Content or account isn't landing."),
    "early":       ("Too early", "Fewer than 3 videos with counts: not enough to judge yet."),
}
TYPE_ORDER = ["established", "consistent", "climber", "slow", "breakout", "hot", "under", "early"]
CLIMB_X = 1.5        # late normal reach >= 1.5x early -> climbing
SLOW_X = 3.0         # ... >= 3x and it started small -> slow starter
SMALL_REL = 0.5      # "small" = below half the campaign's typical video
UNDER_REL = 0.25     # "low" = below a quarter of the campaign's typical video
ESTAB_REL = 1.5      # "high" = 1.5x the campaign's typical video
STEADY_CV = 0.5      # spread (stdev / mean of non-viral views) at or under this = steady


def _med(a: list[int]) -> float | None:
    return statistics.median(a) if a else None


def classify(views: list[int], viral: list[bool], camp_median: float | None) -> str:
    """One account's videos (upload order, counted only) -> a TYPES key."""
    n = len(views)
    if n < VIRAL_MIN_PRIOR:
        return "early"
    nv = sum(viral)
    if nv >= 2:
        return "hot"
    if nv == 1:
        return "breakout"
    base = camp_median or _med(views) or 1
    q = max(1, min(4, n // 2))   # early / late reach over half the series (max 4): one odd video can't decide it
    start, end = _med(views[:q]) or 0, _med(views[-q:]) or 0
    growth = end / start if start else (CLIMB_X if end else 1.0)
    rel = (_med(views) or 0) / base
    mean = sum(views) / n
    cv = statistics.pstdev(views) / mean if mean else 0
    if rel < UNDER_REL and growth < CLIMB_X:
        return "under"
    if growth >= SLOW_X and start / base < SMALL_REL:
        return "slow"
    if growth >= CLIMB_X:
        return "climber"
    if rel >= ESTAB_REL:
        return "established"
    if cv <= STEADY_CV:
        return "consistent"
    return "under" if rel < SMALL_REL else "consistent"


def accounts(links: list[dict], now: float | None = None) -> dict:
    """Creators grid: one card per account with its type, totals and per-video series.
    Type and normal reach use mature videos only (young ones are still collecting views)."""
    mark_viral(links, now)
    counted = [l["views"] for l in links if l.get("views") is not None and l["mature"]]
    camp_median = _med(counted) or _med([l["views"] for l in links if l.get("views") is not None])
    by: dict[str, list[dict]] = {}
    for l in links:
        by.setdefault(acct_key(l), []).append(l)
    out = []
    for key, vids in by.items():
        vids.sort(key=lambda l: (l.get("uploaded_at") is None, l.get("uploaded_at") or 0))
        c = [l for l in vids if l.get("views") is not None]
        m = [l for l in c if l["mature"]]
        mv, mf = [l["views"] for l in m], [bool(l.get("viral")) for l in m]
        t = classify(mv, mf, camp_median)
        vs, vf = [l["views"] for l in c], [bool(l.get("viral")) for l in c]
        q = max(1, min(4, len(mv) // 2)) if mv else 1
        nonv = [v for v, f in zip(mv, mf) if not f]
        s = lambda k: sum(l.get(k) or 0 for l in vids)   # noqa: E731
        out.append({"key": key, "creator": vids[0].get("creator_name") or "?", "platform": vids[0].get("platform") or "other",
                    "type": t, "type_name": TYPES[t][0], "videos": len(vids), "counted": len(c),
                    "views": sum(vs), "viral": sum(vf), "best": max(vs) if vs else None,
                    "avg": round(sum(vs) / len(vs)) if vs else None,
                    "start_reach": round(_med(nonv[:q]) or 0) if nonv else None,
                    "end_reach": round(_med(nonv[-q:]) or 0) if nonv else None,
                    "likes": s("likes"), "comments": s("comments"), "shares": s("shares"),
                    "mature": len(m), "young": len(c) - len(m),
                    "series": [{"id": l["id"], "views": l["views"], "viral": bool(l.get("viral")), "mature": l["mature"],
                                "day": et_day(l.get("uploaded_at")), "uploaded_at": l.get("uploaded_at"),
                                "url": l.get("url"), "normal": l.get("normal"), "likes": l.get("likes"),
                                "comments": l.get("comments"), "shares": l.get("shares")} for l in c]})
    out.sort(key=lambda a: -a["views"])
    counts = {k: 0 for k in TYPE_ORDER}
    for a in out:
        counts[a["type"]] += 1
    return {"accounts": out, "campaign_median": round(camp_median) if camp_median else None,
            "types": [{"key": k, "name": TYPES[k][0], "short": TYPES[k][1], "count": counts[k]} for k in TYPE_ORDER],
            "platforms": sorted({a["platform"] for a in out})}


def gained_by_day(snaps: list[dict], uploaded: dict[int, int | None]) -> dict[str, int]:
    """Views gained per ET day, from each link's end-of-day value minus its previous one.
    A link's first point counts in full only if it lands on (or the day after) the upload day;
    otherwise its earlier growth is unknown and is not dumped onto one day."""
    last: dict[int, dict[str, int]] = {}
    for s in snaps:
        if s.get("views") is None:
            continue
        d = et_day(s["t"])
        last.setdefault(s["link_id"], {})
        last[s["link_id"]][d] = s["views"]           # snaps arrive ordered by t: later overwrites earlier
    out: dict[str, int] = {}
    for lid, per in last.items():
        prev = None
        up = et_day(uploaded.get(lid))
        for d in sorted(per):
            v = per[d]
            if prev is None:
                if up and (dt.date.fromisoformat(d) - dt.date.fromisoformat(up)).days <= 1:
                    out[d] = out.get(d, 0) + v
            else:
                out[d] = out.get(d, 0) + max(0, v - prev)
            prev = v
    return out


def overview(links: list[dict], snaps: list[dict], goal: int | None, start: str | None,
             end: str | None, buffer_days: int = 0, today: str | None = None) -> dict:
    today = today or dt.datetime.now(ET).date().isoformat()
    mark_viral(links)
    counted = [l for l in links if l.get("views") is not None]
    views = [l["views"] for l in counted]
    s = lambda k: sum(l.get(k) or 0 for l in links)   # noqa: E731
    platforms = sorted({l.get("platform") or "other" for l in links})

    up_days = [d for d in (et_day(l.get("uploaded_at")) for l in links) if d]
    if start and end:
        posting = _days(start, end)
        window = posting + _days((dt.date.fromisoformat(end) + dt.timedelta(days=1)).isoformat(),
                                 (dt.date.fromisoformat(end) + dt.timedelta(days=buffer_days)).isoformat()) if buffer_days else posting
    else:
        lo = min(up_days) if up_days else today
        window = _days(max(lo, (dt.date.fromisoformat(today) - dt.timedelta(days=29)).isoformat()), today)
        posting = window

    by_post: dict[str, dict[str, int]] = {d: {} for d in window}
    for l in counted:
        d = et_day(l.get("uploaded_at"))
        if d in by_post:
            p = l.get("platform") or "other"
            by_post[d][p] = by_post[d].get(p, 0) + l["views"]
    gained = gained_by_day(snaps, {l["id"]: l.get("uploaded_at") for l in links})

    n_post = len(posting)
    days, cum = [], 0
    for i, d in enumerate(window):
        tot = sum(by_post[d].values())
        cum += tot
        ramp = None
        if goal:
            ramp = round(goal * min(1.0, (i + 1) / n_post)) if n_post else goal
        days.append({"day": d, "by_platform": by_post[d], "views": tot, "gained": gained.get(d, 0),
                     "cumulative": cum if d <= today else None, "goal_line": ramp,
                     "buffer": d not in posting, "today": d == today})

    virals = sorted((l for l in links if l.get("viral")), key=lambda l: -(l.get("views") or 0))
    return {
        "tiles": {"views": sum(views), "goal": goal, "pct": round(100 * sum(views) / goal, 1) if goal else None,
                  "videos": len(links), "counted": len(counted),
                  "avg": round(sum(views) / len(views)) if views else None,
                  "median": round(statistics.median(views)) if views else None,
                  "viral": len(virals), "viral_pct": round(100 * len(virals) / len(counted), 1) if counted else None,
                  "likes": s("likes"), "comments": s("comments"), "shares": s("shares"),
                  "creators": len({l.get("creator_name") for l in links if l.get("creator_name")})},
        "platforms": platforms, "days": days, "today": today, "viral_x": VIRAL_X,
        "viral_links": [{"id": l["id"], "url": l["url"], "creator": l.get("creator_name"), "platform": l.get("platform"),
                         "views": l.get("views"), "normal": l.get("normal")} for l in virals[:10]],
    }
