"""Phase 2: campaign analytics over the link tracker (pure functions, no I/O).

Input rows come from pg: links with their latest snapshot, plus every snapshot.
Days are ET calendar days, like the planner.
"""
from __future__ import annotations

import datetime as dt
import os
import statistics
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


def mark_viral(links: list[dict]) -> None:
    """Sets l["viral"] and l["normal"] (the creator's normal reach before that video)."""
    by: dict[str, list[dict]] = {}
    for l in links:
        by.setdefault(l.get("creator_name") or "?", []).append(l)
    for vids in by.values():
        vids.sort(key=lambda l: (l.get("uploaded_at") is None, l.get("uploaded_at") or 0))
        prior: list[int] = []
        for l in vids:
            v = l.get("views")
            normal = statistics.median(prior[-VIRAL_WINDOW:]) if len(prior) >= VIRAL_MIN_PRIOR else None
            l["normal"] = round(normal) if normal is not None else None
            l["viral"] = bool(v is not None and normal and v >= VIRAL_X * normal)
            if v is not None:
                prior.append(v)


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
