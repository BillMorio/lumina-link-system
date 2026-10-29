"""In-process background tasks.

  * drip loop (60s): advance every due job, up to ADVANCE_CONCURRENCY at once.
    A job already in flight is never picked up again by a later tick, so one
    slow Apify scrape no longer starves the rest.
  * campaign poller (20s tick, each link every 2 min): its own task, so it
    never waits on scrapes.
  * status sweep (60s): panel `status` for views orders, for /api/speed.
  * score (10 min): panel service score + default switches (score.py).
  * automation (10 min): v2.7 executor (automation.py), only with AUTO_ENABLED=1.
  * planner (5 min): v2.8 planner (planner.py) at each campaign's check_times, only with AUTO_ENABLED=1.
"""
from __future__ import annotations

import asyncio
import logging
import time

import automation
import planner
import campaigns
import db
import engine
import score
from config import ADVANCE_CONCURRENCY, CAMPAIGN_TICK_SEC, STATUS_POLL_SEC, STATUS_TRACK_DAYS
from panels import client_for

log = logging.getLogger("scheduler")

_inflight: dict[int, asyncio.Task] = {}
_sem = asyncio.Semaphore(ADVANCE_CONCURRENCY)


async def _advance(job: dict) -> None:
    try:
        async with _sem:
            await engine.advance_job(job)
    except Exception as e:  # noqa: BLE001
        log.warning("advance error job %s: %s", job.get("id"), e)
    finally:
        _inflight.pop(job["id"], None)


async def drip_tick() -> None:
    due = await asyncio.to_thread(db.due_jobs, int(time.time()))
    for job in due:
        if job["id"] not in _inflight:
            _inflight[job["id"]] = asyncio.create_task(_advance(job))


def _int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


async def status_sweep() -> None:
    since = int(time.time()) - STATUS_TRACK_DAYS * 86400
    rows = await asyncio.to_thread(
        db.q, "SELECT id, provider, source, views_order_id, views_target, completed_at, first_progress_at "
              "FROM jobs WHERE views_order_id IS NOT NULL AND views_order_id <> '' "
              "AND created_at >= ? AND completed_at IS NULL "
              "AND LOWER(COALESCE(views_status,'')) NOT IN ('partial','canceled','cancelled','refunded')",
        (since,))
    by_provider: dict[str, list[dict]] = {}
    for r in rows:
        by_provider.setdefault(r["provider"], []).append(r)
    for provider, jobs in by_provider.items():
        for i in range(0, len(jobs), 100):
            chunk = jobs[i:i + 100]
            try:
                res = await client_for(provider).status([j["views_order_id"] for j in chunk])
            except Exception as e:  # noqa: BLE001
                log.warning("status sweep %s failed: %s", provider, e)
                continue
            now = int(time.time())
            for j in chunk:
                st = res.get(str(j["views_order_id"])) if isinstance(res, dict) else None
                if not isinstance(st, dict) or st.get("error"):
                    continue
                status, remaining = st.get("status"), _int(st.get("remains", st.get("remaining")))
                f = {"views_status": status, "views_remaining": remaining,
                     "views_start_count": _int(st.get("start_count")), "status_checked_at": now}
                if (status or "").lower() == "completed":
                    f["completed_at"] = now
                    if j["source"] == "auto":   # auto jobs are not drip-polled; the panel status ends them
                        f["state"] = "done"
                if (j["first_progress_at"] is None and remaining is not None
                        and remaining < (j["views_target"] or 0)):
                    f["first_progress_at"] = now
                await asyncio.to_thread(db.update_job, j["id"], **f)


async def score_tick() -> None:
    """v2.5: re-score services and apply default switches (SQL + cached panel catalogs)."""
    await score.refresh()


async def _loop(name: str, every: int, fn) -> None:
    while True:
        try:
            await fn()
        except Exception as e:  # noqa: BLE001
            log.warning("%s tick error: %s", name, e)
        await asyncio.sleep(every)


async def run_scheduler() -> None:
    log.info("scheduler started: drip 60s x%d, campaigns %ds, status %ds",
             ADVANCE_CONCURRENCY, CAMPAIGN_TICK_SEC, STATUS_POLL_SEC)
    await asyncio.gather(
        _loop("drip", 60, drip_tick),
        _loop("campaigns", CAMPAIGN_TICK_SEC, campaigns.poller_tick),
        _loop("status", STATUS_POLL_SEC, status_sweep),
        _loop("score", 600, score_tick),
        _loop("auto", 600, automation.tick),
        _loop("planner", 300, planner.tick),
    )
