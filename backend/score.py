"""Panel service score (v2.5). Ranks every panel service from the orders this console
placed, and switches a poor panel default to a clearly better one. SQL plus the cached panel
catalogs only (panels.services_cached, 10 min TTL); never places or changes a panel order.

Only views orders are status-tracked (scheduler.status_sweep), so scores are views only.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time

import config
import db

MIN_ORDERS = 3          # judged orders needed before a service gets a score
POOR = 60               # score below this = poor
STALE_HOURS = 24        # an open order older than this counts against the service
SWITCH_MIN_RATE = 0.8   # a replacement default needs this success rate
SWITCH_MARGIN = 10      # ...and must score this many points above the current default
SWITCH_MAX_PRICE = 1.5  # ...and cost at most this times the current default's catalog rate
SWITCH_COOLDOWN = 7 * 86400   # no new switch for a key within this long of its last switch
FAILED = ("partial", "canceled", "cancelled", "refunded")
FORMULA = ("Score = 70 x success rate + 30 x speed. Success rate = completed / (completed + cancelled, "
           "partial, refunded or open over 24 h). Speed = the platform's median minutes / this service's "
           "median, capped at 1. Under 3 such orders: not enough data. Under 60 = poor.")
_lock = threading.Lock()   # apply_switches / undo read-modify-write the settings rows


def _median(v: list[float]) -> float | None:
    v = sorted(v)
    if not v:
        return None
    m = len(v) // 2
    return v[m] if len(v) % 2 else (v[m - 1] + v[m]) / 2


def _pct(v: list[float], p: float) -> float | None:
    v = sorted(v)
    return round(v[min(len(v) - 1, int(round(p * (len(v) - 1))))], 1) if v else None


def _num(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def compute(jobs: list[dict], now: int | None = None, mismatch: set | None = None) -> list[dict]:
    """jobs: rows with provider, platform, views_service, views_status, created_at, completed_at,
    status_checked_at. One row per (provider, service, platform), views only.
    mismatch: {(provider, service, platform)} whose catalog row names another platform: listed, never scored."""
    now = now or int(time.time())
    mismatch = mismatch or set()
    groups: dict[tuple, list[dict]] = {}
    for j in jobs:
        groups.setdefault((j["provider"], str(j["views_service"]), j["platform"]), []).append(j)
    mins_of = lambda js: [(j["completed_at"] - j["created_at"]) / 60 for j in js if j["completed_at"]]  # noqa: E731
    peer: dict[str, list[float]] = {}
    for key, js in groups.items():
        if key not in mismatch:
            peer.setdefault(key[2], []).extend(mins_of(js))
    peer_med = {plat: _median(peer.get(plat, [])) for (_, _, plat) in groups}
    rows = []
    for (prov, svc, plat), js in groups.items():
        mins = mins_of(js)
        completed = len(mins)
        failed = sum(1 for j in js if not j["completed_at"] and (j.get("views_status") or "").lower() in FAILED)
        open_ = len(js) - completed - failed
        # open too long: checked by the status sweep at least once and still not done after STALE_HOURS
        stale = sum(1 for j in js if not j["completed_at"] and (j.get("views_status") or "").lower() not in FAILED
                    and j.get("status_checked_at") and now - j["created_at"] > STALE_HOURS * 3600)
        judged = completed + failed + stale
        rate = completed / judged if judged else None
        med = _median(mins)
        speed = (min(1.0, (peer_med[plat] or 0) / med) if med else 1.0) if completed else 0.0
        bad_plat = (prov, svc, plat) in mismatch
        score = round(70 * rate + 30 * speed) if judged >= MIN_ORDERS and not bad_plat else None
        rows.append({"provider": prov, "service": svc, "platform": plat, "type": "views",
                     "orders": len(js), "completed": completed, "open": open_, "failed": failed,
                     "stale": stale, "judged": judged,
                     "completion_rate": round(completed / len(js), 3),
                     "success_rate": round(rate, 3) if rate is not None else None,
                     "median_minutes": round(med, 1) if med is not None else None,
                     "p90_minutes": _pct(mins, 0.9),
                     "platform_median_minutes": round(peer_med[plat], 1) if peer_med[plat] is not None else None,
                     "score": score, "poor": score is not None and score < POOR, "platform_mismatch": bad_plat})
    rows.sort(key=lambda r: (r["score"] is None, -(r["score"] or 0), -r["orders"]))
    return rows


def recommendations(rows: list[dict]) -> list[dict]:
    """Poor service -> best scored service on the same platform + kind (any panel)."""
    out = []
    for r in rows:
        if not r["poor"]:
            continue
        better = [c for c in rows if c["platform"] == r["platform"] and c["type"] == r["type"]
                  and c["score"] is not None and c["score"] >= max(POOR, r["score"] + SWITCH_MARGIN)]
        if better:
            b = max(better, key=lambda c: (c["score"], c["orders"]))
            out.append({"platform": r["platform"], "type": r["type"],
                        "provider": r["provider"], "service": r["service"], "score": r["score"],
                        "better_provider": b["provider"], "better_service": b["service"], "better_score": b["score"],
                        "text": f"{config.PLATFORM_LABEL.get(r['platform'], r['platform'])} {r['type']}: "
                                f"{config.provider_label(r['provider'])} {r['service']} scores {r['score']}. "
                                f"Use {config.provider_label(b['provider'])} {b['service']} (score {b['score']})."})
    return out


# --------------------------------------------------------------------------- #
# Default overrides: settings row "default:<provider>:<platform>:<type>" =
# {"service": str|null, "from": str, "reason": str, "at": int, "switched_at": int, "undone": [str]}
# --------------------------------------------------------------------------- #
def _key(provider: str, platform: str, otype: str) -> str:
    return f"default:{provider}:{platform}:{otype}"


def _all_overrides() -> dict[tuple, dict]:
    out = {}
    for r in db.q("SELECT k, v FROM settings WHERE k LIKE 'default:%'"):
        _, prov, plat, ot = r["k"].split(":", 3)
        out[(prov, plat, ot)] = json.loads(r["v"])
    return out


def _save(provider: str, platform: str, otype: str, st: dict) -> None:
    db.ex("INSERT INTO settings (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
          (_key(provider, platform, otype), json.dumps(st)))


def load_overrides() -> dict[tuple, str]:
    """Push active overrides from the DB into config.service_for."""
    config.DEFAULT_OVERRIDES = {k: st["service"] for k, st in _all_overrides().items() if st.get("service")}
    return config.DEFAULT_OVERRIDES


def apply_switches(rows: list[dict], catalog: dict[str, dict | None], now: int | None = None) -> list[dict]:
    """Switch a poor default (>= MIN_ORDERS judged) to the best same-panel, same-platform, same-kind
    service that has >= MIN_ORDERS judged, success >= SWITCH_MIN_RATE, is not poor, scores >= current +
    SWITCH_MARGIN, is in the live catalog with min <= SMART_VIEWS_MIN <= max, and costs <= SWITCH_MAX_PRICE x
    the current default's catalog rate. catalog: provider -> {service_id: catalog row}; None or missing =
    lookup failed, no switch for that panel. rows should exclude speedtest jobs. No switch within
    SWITCH_COOLDOWN of the key's last switch; an undone service is never switched back to."""
    now = now or int(time.time())
    by = {(r["provider"], r["service"], r["platform"], r["type"]): r for r in rows}
    made = []
    with _lock:
        state = _all_overrides()
        for prov, plat, ot in sorted({(r["provider"], r["platform"], r["type"]) for r in rows}):
            base = config.SERVICE_MAP.get(prov, {}).get((plat, ot))
            cat = catalog.get(prov)
            if not base or not cat:
                continue   # no config default to replace, or catalog unknown
            st = state.get((prov, plat, ot)) or {}
            if now - (st.get("switched_at") or 0) < SWITCH_COOLDOWN:
                continue
            cur = st.get("service") or base
            cr = by.get((prov, cur, plat, ot))
            cur_rate = _num((cat.get(cur) or {}).get("rate"))
            if not cr or cr["score"] is None or not cr["poor"] or cur_rate is None:
                continue

            def fits(c, cat=cat, cur_rate=cur_rate):
                info = cat.get(c["service"])
                if not info:
                    return False
                lo, hi, rate = _num(info.get("min")) or 0, _num(info.get("max")), _num(info.get("rate"))
                return (lo <= config.SMART_VIEWS_MIN and (not hi or config.SMART_VIEWS_MIN <= hi)
                        and rate is not None and rate <= cur_rate * SWITCH_MAX_PRICE)
            cands = [c for c in rows if c["provider"] == prov and c["platform"] == plat and c["type"] == ot
                     and c["service"] != cur and c["service"] not in (st.get("undone") or [])
                     and c["judged"] >= MIN_ORDERS and c["score"] is not None and not c["poor"]
                     and (c["success_rate"] or 0) >= SWITCH_MIN_RATE
                     and c["score"] >= cr["score"] + SWITCH_MARGIN and fits(c)]
            if not cands:
                continue
            b = max(cands, key=lambda c: (c["score"], c["orders"]))
            reason = (f"{cur} scored {cr['score']} ({round((cr['success_rate'] or 0) * 100)}% success over "
                      f"{cr['judged']} orders); {b['service']} scored {b['score']} "
                      f"({round(b['success_rate'] * 100)}% over {b['judged']}).")
            _save(prov, plat, ot, {"service": None if b["service"] == base else b["service"], "from": cur,
                                   "reason": reason, "at": now, "switched_at": now,
                                   "undone": st.get("undone") or []})
            made.append({"provider": prov, "platform": plat, "type": ot, "service": b["service"], "from": cur})
        if made:
            load_overrides()
    return made


def switched() -> list[dict]:
    """Active overrides for the dashboard."""
    return [{"provider": p, "platform": pl, "type": ot, "service": st["service"], "from": st.get("from"),
             "config_default": config.SERVICE_MAP.get(p, {}).get((pl, ot), ""), "reason": st.get("reason", ""),
             "at": st.get("at")}
            for (p, pl, ot), st in sorted(_all_overrides().items()) if st.get("service")]


def undo(provider: str, platform: str, otype: str) -> str:
    """Restore the config default and remember the undone service so it is not switched back."""
    with _lock:
        st = _all_overrides().get((provider, platform, otype))
        if st and st.get("service"):
            st["undone"] = sorted(set((st.get("undone") or []) + [st["service"]]))
            st.update(service=None, reason="undone", at=int(time.time()))
            _save(provider, platform, otype, st)
        load_overrides()
        return config.service_for(provider, platform, otype)


def score_jobs() -> list[dict]:
    return db.q("SELECT provider, platform, views_service, views_status, created_at, completed_at, "
                "status_checked_at, created_by FROM jobs WHERE views_order_id IS NOT NULL AND views_order_id <> '' "
                "AND views_service IS NOT NULL AND views_service <> ''")


async def catalogs(providers) -> dict[str, dict | None]:
    """Cached panel catalogs (panels.services_cached). None = lookup failed."""
    import panels
    out: dict[str, dict | None] = {}
    for p in providers:
        try:
            out[p] = {str(s["service"]): s for s in await panels.services_cached(p)}
        except Exception:  # noqa: BLE001
            out[p] = None
    return out


async def refresh() -> tuple[list[dict], list[dict]]:
    """Score every service and apply default switches. Returns (rows for display, switches made now).
    A service whose catalog row names another platform than the job is listed but not scored (same rule
    as campaigns.override_for). Speedtest jobs show in the rows but do not count toward switching."""
    from campaigns import svc_platform
    jobs = await asyncio.to_thread(score_jobs)
    cat = await catalogs(sorted({j["provider"] for j in jobs}))
    mismatch = {(j["provider"], str(j["views_service"]), j["platform"]) for j in jobs
                if cat.get(j["provider"]) and svc_platform(cat[j["provider"]].get(str(j["views_service"])))
                not in (None, j["platform"])}
    rows = compute(jobs, mismatch=mismatch)
    elig = compute([j for j in jobs if j.get("created_by") != "speedtest"], mismatch=mismatch)
    made = await asyncio.to_thread(apply_switches, elig, cat)
    return rows, made
