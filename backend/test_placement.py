"""Placement safety tests. No network, no real panel: PanelClient talks to an
httpx MockTransport. Run: python test_placement.py"""
import os; os.environ["PANEL_DRY_RUN"] = "0"  # tests use their own mock panel
import asyncio
import json
import os
import tempfile
import time

os.environ["CONSOLE_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "t.db")
os.environ["SMMGLOW_API_KEY"] = "test-key"
os.environ["API_ACCESS_CODE"] = "test-code"

import httpx  # noqa: E402

import campaigns  # noqa: E402
import clippers  # noqa: E402
import db  # noqa: E402
import engine  # noqa: E402
import panels  # noqa: E402

REAL_ADVANCE = engine.advance_job   # main() swaps it for a stub
REAL_SUBMISSIONS = clippers.submissions   # tests swap it for fakes

panels.BACKOFF_SEC = 0
P = "smmglow"
STATE = {"balance": 100.0, "add_plan": [], "adds": 0, "down": False, "lookups": 0}
_RealClient = httpx.AsyncClient


def handler(req: httpx.Request) -> httpx.Response:
    form = dict(x.split("=", 1) for x in req.content.decode().split("&"))
    action = form["action"]
    if action in ("balance", "services"):
        STATE["lookups"] += 1
        if STATE["down"]:
            raise httpx.ConnectError("refused", request=req)
    if action == "balance":
        return httpx.Response(200, json={"balance": str(STATE["balance"]), "currency": "USD"})
    if action == "services":
        return httpx.Response(200, json=[{"service": 26146, "name": "Instagram Views", "rate": "0.04",
                                          "min": "100", "max": "1000000", "category": "Instagram"},
                                         {"service": 26330, "name": "Instagram Likes", "rate": "0.08",
                                          "min": "30", "max": "100000", "category": "Instagram"},
                                         {"service": 777, "name": "Instagram Views Alt", "rate": "0.05",
                                          "min": "100", "max": "1000000", "category": "Instagram"},
                                         {"service": 5227, "name": "Facebook Views", "rate": "0.06",
                                          "min": "100", "max": "1000000", "category": "Facebook"},
                                         {"service": 25855, "name": "Instagram Views Big", "rate": "0.1",
                                          "min": "100000", "max": "1000000", "category": "Instagram"}])
    if action == "add":
        STATE["adds"] += 1
        STATE.setdefault("add_log", []).append((form["service"], int(form["quantity"])))
        STATE.setdefault("add_forms", []).append(dict(__import__("urllib.parse").parse.parse_qsl(req.content.decode())))
        step = STATE["add_plan"].pop(0) if STATE["add_plan"] else "ok"
        if step == "503":
            return httpx.Response(503, text="busy")
        if step == "timeout":
            raise httpx.ReadTimeout("read timeout", request=req)
        if step == "error":
            return httpx.Response(200, json={"error": "Not enough funds on balance"})
        if step == "noorder":
            return httpx.Response(200, json={})
        return httpx.Response(200, json={"order": 1000 + STATE["adds"]})
    if action == "cancel":
        ids = form["orders"].replace("%2C", ",").split(",")
        if STATE.get("cancel_ok"):
            return httpx.Response(200, json=[{"order": int(i), "cancel": 1} for i in ids])
        return httpx.Response(200, json={"error": "Incorrect request"})
    if action == "status":
        ids = form["orders"].replace("%2C", ",").split(",")
        STATE["status_calls"] = STATE.get("status_calls", 0) + 1
        return httpx.Response(200, json={i: ({"status": "Completed", "remaining": "0", "start_count": "50"}
                                             if int(i) % 2 else {"status": "In progress", "remaining": "400",
                                                                 "start_count": "50"}) for i in ids})
    return httpx.Response(200, json={})


panels.httpx.AsyncClient = lambda **kw: _RealClient(transport=httpx.MockTransport(handler), **kw)


def reset(balance=100.0, plan=(), down=False):
    STATE.update(balance=balance, add_plan=list(plan), adds=0, down=down, lookups=0, add_log=[])
    panels._balance.clear()
    panels._down.clear()


def mk_link(**kw):
    f = dict(campaign_id=1, campaign_name="T", provider=P, mode="approve", views=1000, likes=0,
             platforms=json.dumps(["instagram", "facebook"]), active=1, created_at=int(time.time()))
    f.update(kw)
    return db.insert("links", **f)


_sid = [0]


def mk_item(link_id, url, state="pending"):
    _sid[0] += 1
    link = campaigns.get_link(link_id)
    s = {"id": _sid[0], "url": url, "platform": "instagram", "clipper": "c", "submitted_at": None}
    campaigns._insert_submission(link, s, state)
    return db.q1("SELECT * FROM queue WHERE link_id=? AND submission_id=?", (link_id, _sid[0]))


async def place(qid, frm=("pending",)):
    return await campaigns.place_item(qid, frm)


def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if not cond else ""))
    if not cond:
        raise SystemExit(1)


async def campaign_page_tests():
    """v2.6: campaign banner passthrough + queue groups for the campaign page."""
    import config
    import main
    from fastapi import HTTPException

    real_get, real_base = clippers._get, config.CLIPPERS_API_BASE
    config.CLIPPERS_API_BASE = "https://clip.test"

    async def fake_get(path, params=None):
        assert path == "/api/campaigns"
        return [{"id": 601, "name": "Abs", "accepted_platforms": "instagram", "thumbnail_url": "https://cdn.x/a.jpg"},
                {"id": 602, "name": "Rel", "accepted_platforms": "instagram", "thumbnail_url": "/uploads/b.png"},
                {"id": 603, "name": "Bare", "accepted_platforms": "instagram", "thumbnail_url": "uploads/c.png"},
                {"id": 604, "name": "Empty", "accepted_platforms": "instagram", "thumbnail_url": ""},
                {"id": 605, "name": "Missing", "accepted_platforms": "instagram"},
                {"id": 606, "name": "Proto", "accepted_platforms": "instagram", "thumbnail_url": "//cdn.y/d.jpg"}]
    clippers._get = fake_get
    try:
        th = {c["id"]: c["thumbnail_url"] for c in (await main.clippers_campaigns(_="t"))["campaigns"]}
    finally:
        clippers._get, config.CLIPPERS_API_BASE = real_get, real_base
    check("clippers campaigns: thumbnail_url passed through, relative resolved on CLIPPERS_API_BASE",
          th == {601: "https://cdn.x/a.jpg", 602: "https://clip.test/uploads/b.png", 603: "https://clip.test/uploads/c.png",
                 604: "", 605: "", 606: "https://cdn.y/d.jpg"}, th)
    L6, L7 = mk_link(campaign_id=601), mk_link(campaign_id=699)
    links = {x["id"]: x for x in (await main.list_links(_="t"))["links"]}
    check("links: thumbnail_url from the last campaigns call, '' when unknown",
          links[L6]["thumbnail_url"] == "https://cdn.x/a.jpg" and links[L7]["thumbnail_url"] == "", links[L6])

    # queue groups: pending | placed | done | closed, done = placed row whose job is done
    LG = mk_link(campaign_id=610)
    ids = {}
    for st_ in ("pending", "waiting_funds", "not_sent", "skipped", "rejected", "failed"):
        ids[st_] = mk_item(LG, f"https://instagram.com/reel/GRP{st_}")["id"]
        db.update("queue", ids[st_], state=st_)
    for name, jstate in (("run", "running"), ("done", "done"), ("nojob", None), ("placing", None)):
        it = mk_item(LG, f"https://instagram.com/reel/GRPJ{name}")
        jid = db.create_job(provider=P, platform="instagram", link=it["url"], state=jstate, created_at=1) if jstate else None
        db.update("queue", it["id"], state="placing" if name == "placing" else "placed", job_id=jid)
        ids[name] = it["id"]

    async def grp(g):
        d = await main.queue(state="all", link_id=LG, page=1, group=g, _="t")
        return d, {x["id"] for x in d["items"]}
    d, got = await grp("pending")
    check("group pending = pending + waiting_funds + not_sent",
          got == {ids["pending"], ids["waiting_funds"], ids["not_sent"]} and d["total"] == 3, got)
    check("groups counts per tab", d["groups"] == {"pending": 3, "placed": 3, "done": 1, "closed": 3, "all": 10}, d["groups"])
    d, got = await grp("placed")
    check("group placed = placing + placed whose job is not done",
          got == {ids["run"], ids["nojob"], ids["placing"]}, got)
    d, got = await grp("done")
    check("group done = placed + job done, job_state returned",
          got == {ids["done"]} and d["items"][0]["job_state"] == "done", d["items"])
    d, got = await grp("closed")
    check("group closed = skipped + rejected + failed", got == {ids["skipped"], ids["rejected"], ids["failed"]}, got)
    old = await main.queue(state="placed", link_id=LG, page=1, _="t")
    check("state= filter unchanged; items gain job_state",
          {x["id"] for x in old["items"]} == {ids["run"], ids["done"], ids["nojob"]}
          and all("job_state" in x for x in old["items"]), old["items"])
    try:
        await main.queue(state="all", link_id=None, page=1, group="nope", _="t")
        check("unknown group -> 400", False)
    except HTTPException as e:
        check("unknown group -> 400", e.status_code == 400)


async def main():
    db.init_db()
    # url normalization
    k = campaigns.url_key
    check("ig reel/p/tv/username forms share a key",
          k("https://www.instagram.com/reel/ABC123/?igsh=x") == k("instagram.com/p/ABC123")
          == k("https://instagram.com/someone/reel/ABC123/") == k("https://instagram.com/tv/ABC123") == "ig:ABC123")
    check("fb reel/watch/videos share a key",
          k("https://www.facebook.com/reel/1234567890") == k("https://m.facebook.com/watch/?v=1234567890")
          == k("https://facebook.com/page/videos/1234567890/") == "fb:1234567890")

    L = mk_link()

    reset()
    r = await place(mk_item(L, "https://instagram.com/reel/OK1")["id"])
    job = db.get_job(r["job_id"]) if r["job_id"] else {}
    check("success -> placed + campaign job", r["state"] == "placed" and job.get("source") == "campaign"
          and job.get("views_order_id") == "1001", r)

    reset(plan=["error"])
    r = await place(mk_item(L, "https://instagram.com/reel/ERR1")["id"])
    check("panel {error} -> failed, not retried", r["state"] == "failed" and "Not enough funds" in r["error"]
          and STATE["adds"] == 1, (r, STATE))

    reset(plan=["503", "503"])
    r = await place(mk_item(L, "https://instagram.com/reel/TR1")["id"])
    check("v2.19: 503 on add -> failed, NOT retried, marked check panel", r["state"] == "failed" and STATE["adds"] == 1
          and "check panel before retry" in r["error"], (r, STATE))

    reset(plan=["503"] * 4)
    r = await place(mk_item(L, "https://instagram.com/reel/TR2")["id"])
    check("v2.19: 503 x4 on add -> one try only", r["state"] == "failed" and STATE["adds"] == 1, (r, STATE))

    reset(plan=["timeout"])
    r = await place(mk_item(L, "https://instagram.com/reel/TO1")["id"])
    check("read timeout on add -> failed, NOT retried (double-spend)", r["state"] == "failed"
          and STATE["adds"] == 1 and "check panel" in r["error"], (r, STATE))

    reset(balance=0.0)
    it = mk_item(L, "https://instagram.com/reel/WF1")
    r = await place(it["id"])
    check("low balance -> waiting_funds, no panel call", r["state"] == "waiting_funds" and STATE["adds"] == 0, r)
    reset(balance=50.0)
    await campaigns.retry_waiting()
    check("waiting_funds auto-retried once funds exist", campaigns.get_item(it["id"])["state"] == "placed")

    L2 = mk_link(campaign_id=2, daily_cap_usd=0.05)   # 1000 IG views @0.04 = 0.04 each
    reset()
    a = await place(mk_item(L2, "https://instagram.com/reel/CAP1")["id"])
    b = await place(mk_item(L2, "https://instagram.com/reel/CAP2")["id"])
    check("daily cap: first placed, second waiting_funds", a["state"] == "placed" and b["state"] == "waiting_funds"
          and "daily cap" in b["error"] and STATE["adds"] == 1, (a, b))

    L3 = mk_link(campaign_id=3)
    reset()
    d = mk_item(L3, "https://www.instagram.com/someone/reel/OK1/?utm=1")
    check("duplicate post URL across links -> skipped at intake", d["state"] == "skipped"
          and "duplicate" in d["error"], d)
    # race path: a pending row whose twin got placed meanwhile
    x = mk_item(L3, "https://instagram.com/reel/RACE1")
    y = mk_item(mk_link(campaign_id=4), "https://instagram.com/reel/RACE2")
    db.update("queue", y["id"], url_key=x["url_key"])
    await place(x["id"])
    r = await place(y["id"])
    check("duplicate at placement time -> skipped, no 2nd order", r["state"] == "skipped" and STATE["adds"] == 1, r)

    check("only allowed source states are placed", await place(x["id"]) is None and STATE["adds"] == 1)

    s = mk_item(L, "https://instagram.com/reel/STUCK1")
    db.update("queue", s["id"], state="placing")
    n = campaigns.recover_stuck()
    s = campaigns.get_item(s["id"])
    check("stuck placing on restart -> failed, never re-placed", n == 1 and s["state"] == "failed"
          and s["error"] == "interrupted, check panel before retry" and STATE["adds"] == 1, s)

    # backfill:false + auto-mode polling, Clippers mocked
    L5 = mk_link(campaign_id=5, mode="auto")
    link5 = campaigns.get_link(L5)
    old = [{"id": 9001, "url": "https://instagram.com/reel/OLD1", "platform": "instagram",
            "clipper": "a", "submitted_at": None, "status": "approved"}]
    await campaigns.record_existing(link5, old)
    new = old + [{"id": 9002, "url": "https://instagram.com/reel/NEW1", "platform": "instagram",
                  "clipper": "b", "submitted_at": None, "status": "approved"},
                 {"id": 9003, "url": "https://tiktok.com/@x/video/1", "platform": "tiktok",
                  "clipper": "b", "submitted_at": None, "status": "approved"}]

    async def fake_subs(cid):
        return new
    clippers.submissions = fake_subs
    reset()
    got = await campaigns.poll_link(L5)
    rows = {r["submission_id"]: r for r in db.q("SELECT * FROM queue WHERE link_id=?", (L5,))}
    check("backfill:false old -> not_sent; new auto-placed; other platform ignored",
          got == 1 and rows[9001]["state"] == "not_sent" and rows[9001]["error"] is None
          and rows[9002]["state"] == "placed" and 9003 not in rows and STATE["adds"] == 1, rows)
    got = await campaigns.poll_link(L5)
    check("re-poll is idempotent", got == 0 and STATE["adds"] == 1)

    # drip loop: concurrent, bounded, never two tasks on one job
    import engine
    import scheduler
    for i in range(8):
        db.create_job(provider=P, platform="instagram", link=f"l{i}", state="running", created_at=1)
    calls, live, peak = [], [0], [0]

    async def slow(job):
        calls.append(job["id"]); live[0] += 1; peak[0] = max(peak[0], live[0])
        await asyncio.sleep(0.2); live[0] -= 1
    engine.advance_job = slow
    await scheduler.drip_tick()
    await scheduler.drip_tick()          # second tick while all are in flight
    await asyncio.gather(*list(scheduler._inflight.values()))
    check("drip: each due job advanced once, <= ADVANCE_CONCURRENCY at a time",
          len(calls) == len(set(calls)) == len(db.due_jobs(int(time.time()))) and peak[0] == 5, (calls, peak))
    await scheduler.status_sweep()
    js = db.q("SELECT views_order_id, views_status, views_remaining, completed_at, first_progress_at "
              "FROM jobs WHERE views_order_id IS NOT NULL")
    odd = [j for j in js if int(j["views_order_id"]) % 2]
    even = [j for j in js if not int(j["views_order_id"]) % 2]
    check("status sweep: Completed -> completed_at; remaining<qty -> first_progress_at",
          js and all(j["completed_at"] and j["views_status"] == "Completed" for j in odd)
          and all(j["completed_at"] is None and j["first_progress_at"] and j["views_remaining"] == 400 for j in even)
          and STATE["status_calls"] == 1, js)
    import main
    sp = (await main.speed(provider="smmglow", platform=None, type="views", _="t"))["rows"]
    check("speed aggregates by (provider, service)", len(sp) == 1 and sp[0]["service"] == "26146"
          and sp[0]["name"] == "Instagram Views" and sp[0]["completed"] == len(odd)
          and sp[0]["orders"] == len(js) and sp[0]["median_minutes"] is not None, sp)
    await new_tests()
    await score_tests()
    await campaign_page_tests()
    await auto_tests()
    print("ALL PASS")


async def new_tests():
    import main
    from fastapi import HTTPException

    # --- 8. ambiguous failures count as spent; no order id = failure
    L8 = mk_link(campaign_id=80, daily_cap_usd=0.05)
    reset(balance=10.0, plan=["noorder"])
    r = await place(mk_item(L8, "https://instagram.com/reel/NOID1")["id"])
    check("add with no order id -> failed ambiguous, no job", r["state"] == "failed" and r["job_id"] is None
          and campaigns.AMBIGUOUS in r["error"], r)
    check("ambiguous failure counts toward spent_today", campaigns.spent_today(L8) == 0.04,
          campaigns.spent_today(L8))
    check("ambiguous failure deducted from cached balance", abs(panels.balance_peek(P) - 9.96) < 1e-9,
          panels.balance_peek(P))
    panels._down.clear()
    r = await place(mk_item(L8, "https://instagram.com/reel/NOID2")["id"])
    check("daily cap honors ambiguous spend", r["state"] == "waiting_funds" and "daily cap" in r["error"], r)
    reset(plan=["error"])
    L8b = mk_link(campaign_id=81)
    await place(mk_item(L8b, "https://instagram.com/reel/DEF1")["id"])
    check("definite panel {error} does NOT count as spent", campaigns.spent_today(L8b) == 0)

    # --- 2. outage: failure cache, retry_waiting skips provider, approve bounded
    L2o = mk_link(campaign_id=20)
    rows = [mk_item(L2o, f"https://instagram.com/reel/OUT{i}") for i in range(5)]
    for x in rows:
        db.update("queue", x["id"], state="waiting_funds")
    reset(down=True)
    panels._services.clear()
    t0 = time.monotonic()
    await campaigns.retry_waiting()
    check("outage: one provider failure -> other rows skipped this tick",
          STATE["lookups"] == 4 and STATE["adds"] == 0 and time.monotonic() - t0 < 5, STATE)  # 1 + 3 retries
    n = STATE["lookups"]
    r = await place(rows[1]["id"], ("waiting_funds",))
    check("outage: cached failure -> fail fast, no network", STATE["lookups"] == n
          and r["state"] == "waiting_funds" and r["error"].startswith("panel unavailable"), r)
    STATE["down"] = False
    panels._down.clear()
    await campaigns.retry_waiting()
    check("outage over: waiting rows placed", all(campaigns.get_item(x["id"])["state"] == "placed" for x in rows))

    # lookups happen outside _place_lock
    reset()
    async def hold():
        async with campaigns._place_lock:
            await asyncio.sleep(0.3)
    h = asyncio.create_task(hold()); await asyncio.sleep(0)
    panels._balance.clear()
    bt = asyncio.create_task(panels.balance_cached(P))
    await asyncio.sleep(0.05)
    check("balance lookup does not wait on _place_lock", bt.done())
    await h

    # approve is bounded and never cancels an in-flight order
    import engine
    real_po = engine.place_order

    async def slow_po(*a, **kw):
        await asyncio.sleep(0.5)
        return await real_po(*a, **kw)
    engine.place_order = slow_po
    main.APPROVE_BUDGET_SEC = 0.2
    reset()
    a1, a2 = mk_item(L2o, "https://instagram.com/reel/SLOW1"), mk_item(L2o, "https://instagram.com/reel/SLOW2")
    t0 = time.monotonic()
    out = await main.queue_approve(main.IdsBody(ids=[a1["id"], a2["id"]]), _="t")
    took = time.monotonic() - t0
    errs = {f["id"]: f["error"] for f in out["failed"]}
    check("approve returns within budget", took < 0.45 and not out["placed"]
          and "background" in errs[a1["id"]] and "not attempted" in errs[a2["id"]], (took, out))
    await asyncio.sleep(0.6)
    check("in-flight approve finishes in background", campaigns.get_item(a1["id"])["state"] == "placed"
          and campaigns.get_item(a2["id"])["state"] == "pending")
    engine.place_order = real_po
    main.APPROVE_BUDGET_SEC = 45.0

    # --- 6. approve accepts waiting_funds
    reset()
    w = mk_item(L2o, "https://instagram.com/reel/WFAPP1")
    db.update("queue", w["id"], state="waiting_funds")
    out = await main.queue_approve(main.IdsBody(ids=[w["id"]]), _="t")
    check("approve places a waiting_funds row", len(out["placed"]) == 1 and not out["failed"], out)

    # --- 3. approve -> auto does not auto-place the old backlog
    L3a = mk_link(campaign_id=30, mode="approve")
    old = mk_item(L3a, "https://instagram.com/reel/BACKLOG1")
    db.update("queue", old["id"], created_at=int(time.time()) - 100)
    reset()
    v = await main.patch_link(L3a, {"mode": "auto"}, _="t")
    check("switch to auto sets auto_since + backlog_pending", v["auto_since"] and v["backlog_pending"] == 1, v)
    new = mk_item(L3a, "https://instagram.com/reel/FRESH1")
    await campaigns.process_link_auto(L3a)
    check("auto places only rows since auto_since", campaigns.get_item(new["id"])["state"] == "placed"
          and campaigns.get_item(old["id"])["state"] == "pending" and STATE["adds"] == 1)
    await main.patch_link(L3a, {"active": False}, _="t")
    db.update("links", L3a, auto_since=int(time.time()) - 1000)   # pretend paused long ago
    late = mk_item(L3a, "https://instagram.com/reel/PAUSED1")
    db.update("queue", late["id"], created_at=int(time.time()) - 50)
    v = await main.patch_link(L3a, {"active": True}, _="t")
    await campaigns.process_link_auto(L3a)
    check("resume in auto resets auto_since; paused backlog not placed",
          campaigns.get_item(late["id"])["state"] == "pending" and v["backlog_pending"] == 2
          and STATE["adds"] == 1, v)

    # --- 1 + 4. PATCH tolerates unchanged campaign_id; no catalog call unless a service field changes
    L4 = mk_link(campaign_id=40, views_service="26146")
    reset(down=True)
    panels._services.clear()
    v = await main.patch_link(L4, {"active": False, "campaign_id": 40, "campaign_name": "T", "views": 2000},
                              _="t")
    check("patch active/views + unchanged campaign_id: ok, no catalog call", v["active"] is False
          and v["views"] == 2000 and STATE["lookups"] == 0, (v, STATE))
    try:
        await main.patch_link(L4, {"campaign_id": 41}, _="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 400
    check("patch changing campaign_id -> 400", ok)

    # --- 5. create_link backfill:false: link inactive until the snapshot is recorded
    snap = [{"id": 7001, "url": "https://instagram.com/reel/PRE1", "platform": "instagram",
             "clipper": "a", "submitted_at": None, "status": "approved"}]

    async def subs5(cid):
        return snap
    clippers.submissions = subs5
    real_rec = campaigns.record_existing
    seen = {}

    async def spy_rec(link, subs):
        seen["active"] = campaigns.get_link(link["id"])["active"]
        seen["polled"] = [r["id"] for r in db.q("SELECT id FROM links WHERE active=1 AND id=?", (link["id"],))]
        await real_rec(link, subs)
    campaigns.record_existing = spy_rec
    reset()
    v = await main.create_link(main.LinkBody(campaign_id=50, campaign_name="C5", provider=P, mode="auto",
                                             views=1000), _="t")
    await asyncio.gather(*list(main._tasks))
    q5 = db.q("SELECT state FROM queue WHERE link_id=?", (v["id"],))
    check("create backfill:false: inactive (poller-invisible) during snapshot, then active",
          seen == {"active": 0, "polled": []} and v["active"] is True and v["auto_since"]
          and [r["state"] for r in q5] == ["not_sent"] and STATE["adds"] == 0, (seen, v, q5))
    campaigns.record_existing = real_rec
    await v22_tests()
    await v24_tests()


async def v22_tests():
    import main
    import scheduler
    from fastapi import HTTPException

    async def status_of(coro):
        try:
            await coro
            return 200
        except HTTPException as e:
            return e.status_code

    # --- v2.2 item 1: not_sent
    L = mk_link(campaign_id=100, mode="auto", likes=0)
    link = campaigns.get_link(L)
    await campaigns.record_existing(link, [{"id": 10001, "url": "https://instagram.com/reel/NS1",
                                            "platform": "instagram", "clipper": "a", "submitted_at": None},
                                           {"id": 10002, "url": "https://instagram.com/reel/NS2",
                                            "platform": "instagram", "clipper": "a", "submitted_at": None}])
    ns = db.q("SELECT * FROM queue WHERE link_id=? ORDER BY submission_id", (L,))
    reset()
    await campaigns.process_link_auto(L)
    await campaigns.retry_waiting()
    check("not_sent: stored with no error, never auto-placed", [r["state"] for r in ns] == ["not_sent"] * 2
          and all(r["error"] is None for r in ns) and STATE["adds"] == 0, ns)
    other = mk_item(mk_link(campaign_id=101), "https://instagram.com/reel/NS1")
    check("not_sent does not claim its URL", other["state"] == "pending", other)
    db.update("queue", other["id"], state="rejected")
    q = await main.queue(state="not_sent", link_id=L, page=1, _="t")
    check("GET /api/queue?state=not_sent", q["total"] == 2 and {i["state"] for i in q["items"]} == {"not_sent"}, q)
    check("Link.counts.not_sent", campaigns.link_view(campaigns.get_link(L))["counts"]["not_sent"] == 2)
    out = await main.queue_approve(main.IdsBody(ids=[ns[0]["id"]]), _="t")
    check("approve places a not_sent row", len(out["placed"]) == 1 and STATE["adds"] == 1, out)
    await main.queue_reject(main.IdsBody(ids=[ns[1]["id"]]), _="t")
    check("reject a not_sent row", campaigns.get_item(ns[1]["id"])["state"] == "rejected")
    mig = mk_item(L, "https://instagram.com/reel/MIG1")
    db.update("queue", mig["id"], state="skipped", error="existed before link")
    dupe = mk_item(L, "https://instagram.com/reel/MIG2")
    db.update("queue", dupe["id"], state="skipped", error="duplicate post URL (queue #1)")
    db.init_db()
    check("startup migration: skipped/'existed before link' -> not_sent, error NULL; others untouched",
          campaigns.get_item(mig["id"])["state"] == "not_sent" and campaigns.get_item(mig["id"])["error"] is None
          and campaigns.get_item(dupe["id"])["state"] == "skipped")

    # --- v2.2 item 2: per-platform services on a link
    async def subs_none(cid):
        return []
    clippers.submissions = subs_none
    reset()
    v = await main.create_link(main.LinkBody(campaign_id=110, provider=P, views=1000, views_service="26146",
                                             services={"instagram": {"views": "777", "likes": None},
                                                       "facebook": {"views": None, "likes": None}},
                                             backfill=True), _="t")
    await asyncio.gather(*list(main._tasks))
    check("Link.services normalized (null platforms dropped)",
          v["services"] == {"instagram": {"views": "777", "likes": None}}, v["services"])
    v0 = campaigns.link_view(campaigns.get_link(L))
    check("Link.services is {} when none", v0["services"] == {}, v0)
    r = await place(mk_item(v["id"], "https://instagram.com/reel/SVC1")["id"])
    check("services[platform][type] beats legacy views_service", r["state"] == "placed"
          and STATE["add_log"] == [("777", 1000)], STATE["add_log"])
    v = await main.patch_link(v["id"], {"services": {}}, _="t")
    reset()
    r = await place(mk_item(v["id"], "https://instagram.com/reel/SVC2")["id"])
    check("PATCH services:{} -> legacy views_service used", v["services"] == {}
          and STATE["add_log"] == [("26146", 1000)], STATE["add_log"])
    check("unknown service id in services -> 400", await status_of(main.patch_link(
        v["id"], {"services": {"instagram": {"views": "999999"}}}, _="t")) == 400)

    # --- v2.2 item 3: likes minimum (SMMGlow IG likes 26330 has catalog min 30)
    reset()
    Lk = mk_link(campaign_id=120, likes=10)
    r = await place(mk_item(Lk, "https://instagram.com/reel/LK1")["id"])
    job = db.get_job(r["job_id"])
    row = campaigns.get_item(r["id"])
    check("campaign likes raised to catalog min; est_cost uses raised qty",
          job["likes_target"] == 30 and row["likes"] == 30 and abs(row["est_cost"] - 0.0424) < 1e-9, (job, row))
    import config
    fb_likes = config.SERVICE_MAP["smmglow"].pop(("facebook", "likes"))   # simulate a panel with no likes service
    fbi = mk_item(Lk, "https://www.facebook.com/reel/1234567001")
    db.update("queue", fbi["id"], platform="facebook")
    r = await place(fbi["id"])
    config.SERVICE_MAP["smmglow"][("facebook", "likes")] = fb_likes
    job = db.get_job(r["job_id"])
    check("no likes service (SMMGlow FB): views placed, likes skipped", r["state"] == "placed"
          and job["likes_target"] == 0 and r["likes"] == 0 and STATE["add_log"][-1] == ("5227", 1000), (r, job))
    reset(down=True)
    panels._services.clear()
    check("catalog lookup fails -> floor falls back to LIKE_ORDER_MIN",
          await engine.likes_floor(P, "26330") == engine.LIKE_ORDER_MIN)
    reset()
    check("floor = catalog min when known", await engine.likes_floor(P, "26330") == 30)
    # drip top-up only when gap >= floor
    jid = db.create_job(provider=P, platform="instagram", link="https://instagram.com/reel/DRIP1",
                        views_target=1000, likes_target=100, likes_service="26330", baseline_likes=0,
                        state="running", created_at=int(time.time()))
    seen_views = [200]
    db.update_job(jid, views_status="In progress", views_remaining=800)   # panel: 200 of 1000 delivered

    async def scrape(platform, link):
        return {"views": seen_views[0], "likes": 0, "comments": 0}
    engine.apify.scrape = scrape
    await REAL_ADVANCE(db.get_job(jid))
    check("top-up gap 20 < floor 30 -> no likes order", STATE["adds"] == 0
          and db.get_job(jid)["likes_ordered"] == 0 and db.get_job(jid)["state"] == "running")
    seen_views[0] = 400
    db.update_job(jid, views_remaining=600)
    await REAL_ADVANCE(db.get_job(jid))
    check("top-up gap 40 >= floor -> ordered 40", STATE["add_log"] == [("26330", 40)]
          and db.get_job(jid)["likes_ordered"] == 40, STATE["add_log"])
    # done follows the panel's order status, not the post's total views
    jid2 = db.create_job(provider=P, platform="instagram", link="https://instagram.com/reel/OLD1",
                         views_target=100, likes_target=10, likes_service="26330", likes_ordered=10,
                         baseline_likes=170, state="running", created_at=int(time.time()), views_status="Pending")

    async def scrape_old(platform, link):
        return {"views": 5716, "likes": 170, "comments": 0}
    engine.apify.scrape = scrape_old
    await REAL_ADVANCE(db.get_job(jid2))
    check("post already over target, panel Pending -> still running", db.get_job(jid2)["state"] == "running")
    db.update_job(jid2, views_status="Completed")
    await REAL_ADVANCE(db.get_job(jid2))
    check("panel Completed but 10 ordered likes not landed -> still running", db.get_job(jid2)["state"] == "running")

    async def scrape_landed(platform, link):
        return {"views": 5816, "likes": 180, "comments": 0}
    engine.apify.scrape = scrape_landed
    await REAL_ADVANCE(db.get_job(jid2))
    check("panel Completed + likes landed -> done", db.get_job(jid2)["state"] == "done")
    engine.apify.scrape = scrape
    reset()
    res = await main.place_order(main.OrderBody(provider=P, platform="instagram",
                                                link="https://instagram.com/reel/ORD1", views=1000, likes=5), who="t")
    check("/api/order likes raised to floor", res["likes"] == 30
          and db.get_job(res["job_id"])["likes_target"] == 30, res)
    pv = await main.bulk_preview(main.BulkPreviewBody(provider=P, mode="manual", views_range="1000-1000",
                                                      links=["https://instagram.com/reel/BLK1"]), _="t")
    check("bulk preview likes raised + est_cost uses it", pv["orders"][0]["likes"] == 30
          and abs(pv["orders"][0]["est_cost"] - 0.0424) < 1e-9, pv)
    reset()
    out = await main.bulk_place(main.BulkPlaceBody(provider=P, orders=[
        {"url": "https://instagram.com/reel/BLK1", "platform": "instagram", "views": 1000, "likes": 5}]), who="t")
    check("bulk place likes raised", out["placed"][0]["likes"] == 30
          and db.get_job(out["placed"][0]["job_id"])["likes_target"] == 30, out)

    # --- service min/max guard: no panel call when views are below the service minimum
    reset()
    check("/api/order views below service min -> 400, no panel call", await status_of(main.place_order(
        main.OrderBody(provider=P, platform="instagram", link="https://instagram.com/reel/MIN1", views=100,
                       views_service="25855"), who="t")) == 400 and STATE["adds"] == 0)
    check("/api/order likes above service max -> 400, no panel call", await status_of(main.place_order(
        main.OrderBody(provider=P, platform="instagram", link="https://instagram.com/reel/MAX1", views=1000,
                       likes=200000), who="t")) == 400 and STATE["adds"] == 0)
    out = await main.bulk_place(main.BulkPlaceBody(provider=P, views_service="25855", orders=[
        {"url": "https://instagram.com/reel/MIN2", "platform": "instagram", "views": 100, "likes": 0}]), who="t")
    check("bulk place skips a row below service min, no panel call",
          not out["placed"] and "minimum" in out["skipped"][0]["reason"] and STATE["adds"] == 0, out)

    # --- v2.2 item 4
    from config import STATUS_POLL_SEC
    check("status sweep every 60s", STATUS_POLL_SEC == 60 == scheduler.STATUS_POLL_SEC)

    # --- v2.2 item 5: manual/bulk dedupe
    reset()
    body = main.OrderBody(provider=P, platform="instagram", link="https://instagram.com/reel/ORD1/?igsh=x",
                          views=1000)
    check("/api/order on a post with a manual job -> 409, no panel call",
          await status_of(main.place_order(body, who="t")) == 409 and STATE["adds"] == 0)
    body.force = True
    check("/api/order force:true overrides", await status_of(main.place_order(body, who="t")) == 200
          and STATE["adds"] == 1)
    check("/api/order on a placed campaign post -> 409", await status_of(main.place_order(
        main.OrderBody(provider=P, platform="instagram", link="https://instagram.com/reel/LK1", views=1000),
        who="t")) == 409)
    pv = await main.bulk_preview(main.BulkPreviewBody(
        provider=P, mode="manual", views_range="1000-1000",
        links=["https://instagram.com/reel/ORD1", "https://instagram.com/reel/NEWB1",
               "https://www.instagram.com/p/NEWB1/"]), _="t")
    d = [o["duplicate"] for o in pv["orders"]]
    check("bulk preview rows carry duplicate reason", d[0] and "manual job" in d[0] and d[1] is None
          and d[2] == "duplicate in this list", d)
    reset()
    rows = [{"url": u, "platform": "instagram", "views": 1000, "likes": 0}
            for u in ("https://instagram.com/reel/ORD1", "https://instagram.com/reel/NEWB1",
                      "https://www.instagram.com/p/NEWB1/")]
    out = await main.bulk_place(main.BulkPlaceBody(provider=P, orders=rows), who="t")
    check("bulk place skips duplicates (db + in-list) and reports them",
          len(out["placed"]) == 1 and STATE["adds"] == 1
          and [s["url"] for s in out["skipped"]] == [rows[0]["url"], rows[2]["url"]], out)
    reset()
    out = await main.bulk_place(main.BulkPlaceBody(provider=P, orders=rows[:1], force=True), who="t")
    check("bulk place force:true places duplicates", len(out["placed"]) == 1 and not out["skipped"], out)
    reset()
    r = await place(mk_item(mk_link(campaign_id=130), "https://instagram.com/reel/NEWB1")["id"])
    check("campaign placement skips a post with a manual job", r["state"] == "skipped"
          and "manual job" in r["error"] and STATE["adds"] == 0, r)
    old = db.create_job(provider=P, platform="instagram", link="https://instagram.com/reel/OLDJOB1",
                        state="done", created_at=1)
    db.ex("UPDATE jobs SET url_key=NULL WHERE id=?", (old,))
    db.init_db()
    check("startup backfills jobs.url_key", db.get_job(old)["url_key"] == "ig:OLDJOB1")
    plan = db.q("EXPLAIN QUERY PLAN SELECT id FROM jobs WHERE url_key=? "
                "AND COALESCE(source,'manual')<>'campaign' LIMIT 1", ("x",))
    check("manual-job dedupe uses the jobs_url_key index", any("jobs_url_key" in p["detail"] for p in plan), plan)



async def v24_tests():
    """v2.4 fast likes: URL extraction, account-aware likes math, guards. Apify is mocked."""
    import main
    import apify_client
    from fastapi import HTTPException

    async def status_of(coro):
        try:
            await coro
            return 200
        except HTTPException as e:
            return e.status_code

    # --- URL extraction (paste or CSV dump)
    txt = ('url,views\n"https://www.instagram.com/reel/FA1/?igsh=x",123\n'
           'instagram.com/p/FA1, dup of the first\nhttps://www.tiktok.com/@a/video/7000000000000000001.\n'
           'note: see https://youtu.be/abcdefghijk) and youtube.com/shorts/abcdefghijk;\nwww.facebook.com/reel/1234567890 junk words')
    got = main.extract_urls(txt)
    check("extract_urls: any column, bare domains get https, dedupe by post, trailing punctuation dropped",
          got == ["https://www.instagram.com/reel/FA1/?igsh=x", "https://www.tiktok.com/@a/video/7000000000000000001",
                  "https://youtu.be/abcdefghijk", "https://www.facebook.com/reel/1234567890"], got)
    k = campaigns.url_key
    check("youtube watch/youtu.be/shorts share a key, different ids differ",
          k("https://www.youtube.com/watch?v=abcdefghijk&t=3") == k("https://youtu.be/abcdefghijk")
          == k("https://youtube.com/shorts/abcdefghijk") == "yt:abcdefghijk"
          and k("https://www.youtube.com/watch?v=zzzzzzzzzzz") != "yt:abcdefghijk")

    # --- account-aware math
    check("fast_likes_for: views x like rate", engine.fast_likes_for(10000, 0.05) == 500)
    check("fast_likes_for: like rate floored like bulk", engine.fast_likes_for(10000, 0.001) == 200)
    check("fast_likes_for: optional cap", engine.fast_likes_for(10000, 0.05, 300) == 300
          and engine.fast_likes_for(10000, 0.05, None) == 500)

    # --- account scrape carries per-post views keyed by post
    real_fetch = apify_client._ROW_FETCHERS["instagram"]

    async def fake_rows(handle, limit):
        return [(20000, 1000, "https://www.instagram.com/p/INACCT1/"), (5000, 100, ""),
                (8000, 400, "https://www.instagram.com/reel/OTHER1/")]
    apify_client._ROW_FETCHERS["instagram"] = fake_rows
    pp = await apify_client.profile_posts("instagram", "acct")
    apify_client._ROW_FETCHERS["instagram"] = real_fetch
    check("profile_posts: stats + views by url_key", pp["stats"]["n"] == 3
          and pp["views"] == {"ig:INACCT1": 20000, "ig:OTHER1": 8000}, pp)

    # --- fast_account_scrape with mocked Apify
    calls = {"scrape": []}
    owners = {"INACCT1": "acct", "NOTIN1": "acct", "NOVIEWS1": "acct", "NOOWNER1": None, "DUPF1": "acct",
              "ZEROV1": "acct"}

    async def f_owner(platform, url):
        return owners[url.rstrip("/").split("/")[-1]]

    async def f_posts(platform, handle, limit=120, anchor_pct=0.95):
        return {"stats": {"like_rate": 0.05}, "views": {"ig:INACCT1": 20000}}

    async def f_scrape(platform, url):
        calls["scrape"].append(url)
        if "NOTIN1" in url or "DUPF1" in url:
            return {"views": 6000, "likes": 1, "comments": 0}
        if "ZEROV1" in url:
            return {"views": 0, "likes": 0, "comments": 0}
        return None
    saved = (apify_client.post_owner, apify_client.profile_posts, apify_client.scrape)
    apify_client.post_owner, apify_client.profile_posts, apify_client.scrape = f_owner, f_posts, f_scrape
    try:
        def U(c):
            return f"https://www.instagram.com/reel/{c}/"
        rows = await engine.fast_account_scrape([(U(c), "instagram") for c in ("INACCT1", "NOTIN1", "NOVIEWS1", "NOOWNER1")])
        check("account scrape: target post found in account scrape -> its views, no extra scrape",
              rows[0]["views"] == 20000 and rows[0]["like_rate"] == 0.05 and U("INACCT1") not in calls["scrape"], rows[0])
        check("account scrape: post not in account scrape -> scrape(platform, url)",
              rows[1]["views"] == 6000 and U("NOTIN1") in calls["scrape"] and not rows[1]["problem"], rows[1])
        check("account scrape: views unreadable -> problem, no guess",
              rows[2]["views"] is None and "current views" in rows[2]["problem"], rows[2])
        check("account scrape: owner unknown -> problem", rows[3]["problem"] and rows[3]["like_rate"] is None, rows[3])

        # --- preview (account mode); DUPF1 has a manual job already
        db.create_job(provider=P, platform="instagram", link=U("DUPF1"), state="done", created_at=1)
        pv = await main.fast_preview(main.FastPreviewBody(provider=P, mode="account", likes_cap=900, links=[
            U("INACCT1") + " " + U("NOTIN1") + "\n" + U("NOVIEWS1"), U("DUPF1"), "https://x.com/a/status/1",
            U("ZEROV1")]), _="t")
        o = {r["url"].split("/")[-2]: r for r in pv["orders"] if "instagram" in r["url"]}
        check("account preview: likes = current views x like rate, capped",
              o["INACCT1"]["likes"] == 900 and o["NOTIN1"]["likes"] == 300 and o["INACCT1"]["views"] == 20000
              and o["NOTIN1"]["like_rate"] == 0.05 and o["NOTIN1"]["likes_service"] == "26330", o)
        check("account preview: unreadable views and 0 views are problems, no likes",
              o["NOVIEWS1"]["problem"] and o["NOVIEWS1"]["likes"] == 0 and o["ZEROV1"]["problem"]
              and o["ZEROV1"]["likes"] == 0, (o["NOVIEWS1"], o["ZEROV1"]))
        check("account preview: non-engagement link is a problem row",
              any(r["platform"] == "x" and r["problem"] for r in pv["orders"]), pv["orders"])
        check("preview totals skip duplicates and problems",
              o["DUPF1"]["duplicate"] and "manual job" in o["DUPF1"]["duplicate"]
              and pv["count"] == 2 and pv["total_likes"] == 1200 and pv["duplicates"] == 1
              and abs(pv["total_cost"] - 1200 / 1000 * 0.08) < 1e-9,
              {k2: v for k2, v in pv.items() if k2 != "orders"})
    finally:
        apify_client.post_owner, apify_client.profile_posts, apify_client.scrape = saved

    # --- preview (fixed mode)
    pv = await main.fast_preview(main.FastPreviewBody(provider=P, mode="fixed", likes_range="10", links=[
        "https://instagram.com/reel/FX1"]), _="t")
    check("fixed preview: below the service min is raised to it (26330 min 30)",
          pv["orders"][0]["likes"] == 30 and pv["orders"][0]["views"] is None and not pv["orders"][0]["problem"], pv)
    pv = await main.fast_preview(main.FastPreviewBody(provider=P, mode="fixed", likes_range="200000", links=[
        "https://instagram.com/reel/FX1"]), _="t")
    check("fixed preview: above the service max is a problem", "maximum" in (pv["orders"][0]["problem"] or ""), pv)
    pv = await main.fast_preview(main.FastPreviewBody(provider=P, mode="fixed", likes_range="300-500",
                                                      likes_service="26330",
                                                      links=["https://instagram.com/reel/FX2"]), _="t")
    check("fixed preview: range + picked service", 300 <= pv["orders"][0]["likes"] <= 500
          and pv["orders"][0]["likes_service"] == "26330", pv)
    check("fixed preview without likes -> 400", await status_of(main.fast_preview(main.FastPreviewBody(
        provider=P, mode="fixed", links=["https://instagram.com/reel/FX1"]), _="t")) == 400)
    check("unknown provider -> 400", await status_of(main.fast_preview(main.FastPreviewBody(
        provider="nope", mode="fixed", likes_range="50", links=["https://instagram.com/reel/FX1"]), _="t")) == 400)
    check("instagram likes service on a facebook post -> 400", await status_of(main.fast_place(main.FastPlaceBody(
        provider=P, orders=[{"url": "https://facebook.com/reel/1234567", "platform": "facebook", "likes": 50,
                             "likes_service": "26330"}]), who="t")) == 400)

    # --- place: guards + dedupe, mocked panel
    reset()
    out = await main.fast_place(main.FastPlaceBody(provider=P, orders=[
        {"url": "https://instagram.com/reel/FP1", "platform": "instagram", "likes": 12},
        {"url": "https://instagram.com/reel/FP2", "platform": "instagram", "likes": 200000},
        {"url": "https://instagram.com/reel/DUPF1", "platform": "instagram", "likes": 100},
        {"url": "https://x.com/a/status/1", "platform": "x", "likes": 100},
        {"url": "https://www.instagram.com/p/FP1/", "platform": "instagram", "likes": 100}]), who="t")
    reasons = {s_["url"]: s_["reason"] for s_ in out["skipped"]}
    check("fast place: likes raised to floor, one panel add, likes only",
          len(out["placed"]) == 1 and STATE["add_log"] == [("26330", 30)], (out, STATE["add_log"]))
    check("fast place: above max, duplicate, non-engagement and in-list repeat are skipped",
          "maximum" in reasons["https://instagram.com/reel/FP2"]
          and "manual job" in reasons["https://instagram.com/reel/DUPF1"]
          and "Instagram" in reasons["https://x.com/a/status/1"]
          and "manual job" in reasons["https://www.instagram.com/p/FP1/"] and len(out["skipped"]) == 4, out)
    j = db.get_job(out["placed"][0]["job_id"])
    check("fast place: job kind=fastlikes, provider/platform/service recorded, no views order",
          j["kind"] == "fastlikes" and j["provider"] == P and j["platform"] == "instagram"
          and j["likes_service"] == "26330" and not j["views_order_id"] and j["likes_order_id"] == "1001"
          and j["state"] == "done", j)
    reset()
    out = await main.fast_place(main.FastPlaceBody(provider=P, force=True, orders=[
        {"url": "https://instagram.com/reel/DUPF1", "platform": "instagram", "likes": 100}]), who="t")
    check("fast place force:true sends a duplicate", len(out["placed"]) == 1 and STATE["adds"] == 1, out)
    fj = await main.fastjobs(page=1, _="t")
    check("fastjobs: status looked up on each job's own panel",
          fj["jobs"] and all(x["order_status"] in ("Completed", "In progress") for x in fj["jobs"]), fj)


async def score_tests():
    """v2.5 panel service score + default override guardrails. Mock panel only."""
    import config
    import main
    import score
    now = int(time.time())
    H = 3600

    def J(prov, svc, plat, mins=None, status=None, age_h=48, checked=True):
        return {"provider": prov, "views_service": svc, "platform": plat, "created_at": now - age_h * H,
                "completed_at": (now - age_h * H + mins * 60) if mins is not None else None,
                "views_status": "Completed" if mins is not None else status,
                "status_checked_at": now if checked else None}

    # --- pure score
    jobs = ([J(P, "9733", "tiktok", 60), J(P, "9733", "tiktok", status="Canceled"),
             J(P, "9733", "tiktok", status="Partial"), J(P, "9733", "tiktok", status="In progress"),
             J(P, "9733", "tiktok", status="Pending", age_h=1),            # young open: not judged
             J(P, "9733", "tiktok", status=None, checked=False)]          # never status-checked: not judged
            + [J(P, "9999", "tiktok", 10) for _ in range(4)]
            + [J(P, "8888", "tiktok", 5) for _ in range(2)]
            + [J("morethanpanel", "4444", "tiktok", 20) for _ in range(3)])
    rows = {r["service"]: r for r in score.compute(jobs, now)}
    a, b, c = rows["9733"], rows["9999"], rows["8888"]
    check("score: completed/open/failed/stale counted; young + never-checked open not judged",
          a["orders"] == 6 and a["completed"] == 1 and a["failed"] == 2 and a["open"] == 3
          and a["stale"] == 1 and a["judged"] == 4 and a["success_rate"] == 0.25, a)
    check("score: platform median = median of all completed minutes on that platform",
          a["platform_median_minutes"] == 10, a)
    check("score: 70 x success + 30 x speed (speed = platform median / own median, max 1)",
          a["score"] == round(70 * 0.25 + 30 * (10 / 60)) and b["score"] == 100 and rows["4444"]["score"] == 85
          and a["poor"] and not b["poor"], (a["score"], b["score"], rows["4444"]["score"]))
    check("score: under 3 judged orders -> no score (not enough data)", c["score"] is None and not c["poor"], c)
    recs = score.recommendations(list(rows.values()))
    check("recommendation: poor service -> best scored same platform+kind",
          len(recs) == 1 and recs[0]["service"] == "9733" and recs[0]["better_service"] == "9999"
          and "9999" in recs[0]["text"], recs)

    # --- overrides through the DB
    def put(j):
        jid = db.create_job(provider=j["provider"], platform=j["platform"], link=f"https://t/{os.urandom(4).hex()}",
                            views_service=j["views_service"], views_order_id="9" + str(os.urandom(2).hex()),
                            state="done", created_at=j["created_at"])
        db.update_job(jid, views_status=j["views_status"], completed_at=j["completed_at"],
                      status_checked_at=j["status_checked_at"])
    for j in jobs:
        put(j)
    # facebook: default 5227 has only 2 judged orders -> cannot be judged, even with a perfect candidate
    for j in [J(P, "5227", "facebook", status="Canceled") for _ in range(2)] + [J(P, "6666", "facebook", 5) for _ in range(4)]:
        put(j)
    # youtube: default 24575 poor, but candidate 1234 has 67% success (< 80%) -> no switch
    for j in ([J(P, "24575", "youtube", status="Canceled") for _ in range(3)]
              + [J(P, "1234", "youtube", 5), J(P, "1234", "youtube", 5), J(P, "1234", "youtube", status="Refunded")]):
        put(j)
    # an Instagram catalog service used on Facebook posts: listed, never scored
    for j in [J(P, "26146", "facebook", 5) for _ in range(3)]:
        put(j)
    # the mock catalog gains the tiktok/youtube/facebook services above (switch needs them in the catalog)
    cat_rows = await panels.services_cached(P)
    extra = [{"service": sid, "name": nm, "rate": rt, "min": "100", "max": "1000000", "category": nm.split()[0]}
             for sid, nm, rt in (("9733", "TikTok Views", "0.06"), ("9999", "TikTok Views Fast", "0.07"),
                                 ("8888", "TikTok Views Slow", "0.05"), ("24575", "YouTube Views", "0.8"),
                                 ("1234", "YouTube Views HQ", "0.8"), ("6666", "Facebook Views HQ", "0.06"))]
    panels._services[P] = (time.time(), [s_ for s_ in cat_rows if str(s_["service"]) not in {e["service"] for e in extra}] + extra)
    config.DEFAULT_OVERRIDES = {}
    sp = await main.speed(provider=None, platform="tiktok", type="views", _="t")
    check("switch: poor default (>=3 judged) -> same-panel candidate with >=3 orders, >=80%, clearly better",
          config.service_for(P, "tiktok", "views") == "9999" and config.SERVICE_MAP[P][("tiktok", "views")] == "9733"
          and len(sp["switched"]) == 1 and sp["switched"][0]["service"] == "9999"
          and sp["switched"][0]["config_default"] == "9733" and "9733 scored" in sp["switched"][0]["reason"], sp["switched"])
    check("switch: other panels' services and under-3-order services never become the default",
          config.service_for("morethanpanel", "tiktok", "views") == config.SERVICE_MAP["morethanpanel"][("tiktok", "views")]
          and sp["recommendations"] and sp["formula"].startswith("Score ="), sp["recommendations"])
    check("guardrail: default with < 3 judged orders is not judged", config.service_for(P, "facebook", "views") == "5227")
    check("guardrail: candidate under 80% success is not used", config.service_for(P, "youtube", "views") == "24575")
    cat = await main.catalog(_="t")
    check("override: /api/catalog defaults include the switch", cat["services"][P]["tiktok:views"] == "9999", cat["services"][P])
    reset()
    await engine.place_order(P, "tiktok", "https://www.tiktok.com/@a/video/1", 1000, 0, created_by="test")
    check("override: engine default placement uses the switched service", STATE["add_log"][0][0] == "9999", STATE["add_log"])
    config.DEFAULT_OVERRIDES = {}
    score.load_overrides()
    check("override: stored in the DB settings table, survives a restart",
          config.service_for(P, "tiktok", "views") == "9999"
          and db.q1("SELECT v FROM settings WHERE k='default:smmglow:tiktok:views'"))
    u = await main.speed_undo(main.UndoDefaultBody(provider=P, platform="tiktok", type="views"), _="t")
    sp = await main.speed(provider=None, platform="tiktok", type="views", _="t")
    check("undo: config default restored and not switched back on the next score",
          u["default"] == "9733" and config.service_for(P, "tiktok", "views") == "9733" and not sp["switched"], (u, sp["switched"]))
    check("speed rows: is_default + score fields", any(r["service"] == "9733" and r["is_default"] and r["poor"] for r in sp["rows"]),
          sp["rows"])
    later = int(time.time()) + 8 * 86400
    elig = score.compute(score.score_jobs())
    check("undo: undone service is never switched back, even after the cooldown",
          not score.apply_switches(elig, await score.catalogs([P]), now=later)
          and config.service_for(P, "tiktok", "views") == "9733")
    fb = (await main.speed(provider=P, platform="facebook", type="views", _="t"))["rows"]
    check("platform mismatch: Instagram catalog service on Facebook jobs is listed but not scored",
          any(r["service"] == "26146" and r["platform_mismatch"] and r["score"] is None for r in fb), fb)

    # --- switch guardrails 1-3, pure apply_switches on smmglow X views (config default 22143)
    def R(svc, sc, judged=5, success=1.0):
        return {"provider": P, "service": svc, "platform": "x", "type": "views", "orders": judged, "judged": judged,
                "success_rate": success, "score": sc, "poor": sc is not None and sc < score.POOR}

    def C(**over):
        base = {"22143": {"service": "22143", "rate": "0.02", "min": "10", "max": "1000000"},
                "5001": {"service": "5001", "rate": "0.02", "min": "10", "max": "1000000"},
                "5002": {"service": "5002", "rate": "0.02", "min": "10", "max": "1000000"}}
        for sid, f in over.items():
            if f is None:
                base.pop(sid)
            else:
                base[sid].update(f)
        return {P: base}

    def clear_x():
        db.ex("DELETE FROM settings WHERE k='default:smmglow:x:views'")
        score.load_overrides()
    rows_x = [R("22143", 40), R("5001", 95)]
    clear_x()
    check("guardrail sanity: poor default -> catalog-valid, priced, good candidate switches",
          score.apply_switches(rows_x, C()) and config.service_for(P, "x", "views") == "5001")
    clear_x()
    check("guardrail: catalog lookup failed -> no switch",
          not score.apply_switches(rows_x, {P: None}) and not score.apply_switches(rows_x, {}))
    check("guardrail: candidate missing from the live catalog -> no switch", not score.apply_switches(rows_x, C(**{"5001": None})))
    check("guardrail: candidate min above SMART_VIEWS_MIN -> no switch",
          not score.apply_switches(rows_x, C(**{"5001": {"min": "500"}})))
    check("guardrail: candidate max below SMART_VIEWS_MIN -> no switch",
          not score.apply_switches(rows_x, C(**{"5001": {"max": "50"}})))
    pricey = C(**{"5001": {"rate": "0.031"}})
    check("guardrail: candidate over 1.5x the default's price -> no switch, still a suggestion",
          not score.apply_switches(rows_x, pricey)
          and any(r_["better_service"] == "5001" for r_ in score.recommendations(rows_x))
          and score.apply_switches(rows_x, C(**{"5001": {"rate": "0.03"}})))
    clear_x()
    check("guardrail: poor candidate (score < 60) is never switched to, even 10+ points better",
          not score.apply_switches([R("22143", 40), R("5001", 56, success=0.8)], C()))
    t0 = int(time.time())
    score.apply_switches(rows_x, C(), now=t0)
    rows_x2 = [R("22143", 40), R("5001", 40), R("5002", 95)]
    check("guardrail: no re-switch within 7 days of the last switch",
          not score.apply_switches(rows_x2, C(), now=t0 + 86400) and config.service_for(P, "x", "views") == "5001")
    check("guardrail: after the 7 day cooldown a poor switched default can switch again",
          score.apply_switches(rows_x2, C(), now=t0 + 8 * 86400) and config.service_for(P, "x", "views") == "5002")
    clear_x()


async def auto_tests():
    """v2.7 Automation executor. Panels mocked: fake catalogs + balances, adds hit the MockTransport."""
    import automation
    import main
    from fastapi import HTTPException
    os.environ["MORETHANPANEL_API_KEY"] = "test-key"
    panels._clients.pop("morethanpanel", None)
    M = "morethanpanel"
    aj = db.create_job(provider=P, platform="tiktok", link="https://tiktok.com/@a/video/1", state="running",
                       created_at=1, next_poll_at=None, source="auto")
    check("auto: auto jobs are never drip/scrape polled", aj not in [j["id"] for j in db.due_jobs(int(time.time()))])
    CAT = {P: [{"service": 9101, "name": "TikTok Views Cheap", "rate": "0.01", "min": "100", "max": "1000"},
               {"service": 9102, "name": "TikTok Views Good", "rate": "0.02", "min": "50", "max": "100000"},
               {"service": 9199, "name": "TikTok Story Views", "rate": "0.001", "min": "10", "max": "100000"},
               {"service": 16580, "name": "TikTok Likes", "rate": "1.0", "min": "20", "max": "10000"}],
           M: [{"service": 9201, "name": "TikTok Views MTP", "rate": "0.015", "min": "100", "max": "100000"},
               {"service": 9301, "name": "TikTok Views Sub", "type": "Subscription", "rate": "0.001", "min": "10",
                "max": "100000"},
               {"service": 1126, "name": "TikTok Likes MTP", "rate": "0.5", "min": "20", "max": "10000"}]}
    BAL = {P: 100.0, M: 100.0}
    SC = {(P, "9101", "tiktok"): 50, (P, "9102", "tiktok"): 90, (M, "9201", "tiktok"): 80, (M, "9301", "tiktok"): 95}
    real = (panels.services_cached, panels.balance_cached, automation._scores)

    async def f_services(p):
        if p not in CAT:
            raise panels.PanelUnavailable("no key")
        return CAT[p]

    async def f_balance(p):
        return BAL[p]
    panels.services_cached, panels.balance_cached = f_services, f_balance
    automation._scores = lambda: dict(SC)
    today = automation.et_day()
    yday = (automation.dt.date.fromisoformat(today) - automation.dt.timedelta(days=1)).isoformat()
    n = [0]

    def fresh(cap=100.0):
        for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns"):
            db.ex(f"DELETE FROM {t}")
        automation._alerts.clear()
        panels._down.clear()
        BAL.update({P: 100.0, M: 100.0})
        reset()
        db.ex("DELETE FROM settings WHERE k LIKE 'auto:%'")
        if cap is not None:
            automation.set_settings({"global_daily_cap_usd": cap})

    async def camp(mode="live", budget=100.0, cap_usd=1000.0):
        c = await main.auto_campaign_add({"name": f"C{n[0]}", "weekly_budget_usd": budget, "cap_usd": cap_usd}, who="t")
        if mode == "live":
            c = await main.auto_campaign_patch(c["id"], {"mode": "live"}, who="t")
        return c

    def plan(cid, views, likes=0, day=None, carry=None, creator="cr1"):
        n[0] += 1
        day = day or today
        url = f"https://www.tiktok.com/@u/video/{70000 + n[0]}"
        pid = f"{cid}:{campaigns.url_key(url)}:{day}"
        automation.insert_plan([{"plan_id": pid, "campaign_id": cid, "creator_id": creator, "post_url": url,
                                 "platform": "tiktok", "account_handle": "u", "window_start": automation.et_midnight(day),
                                 "window_end": automation.et_midnight(day), "views_add": views, "likes_add": likes,
                                 "reason": "base"}])
        if carry is not None:
            db.ex("UPDATE plan_rows SET carry=? WHERE plan_id=?", (carry, pid))
        return url, pid

    def orders(**w):
        sql = "SELECT * FROM auto_orders" + (" WHERE " + " AND ".join(f"{k}=?" for k in w) if w else "") + " ORDER BY id"
        return db.q(sql, tuple(w.values()))

    tick = automation.run_tick

    # --- off unless AUTO_ENABLED=1
    fresh()
    c = await camp()
    plan(c["id"], 1000)
    os.environ.pop("AUTO_ENABLED", None)
    await automation.tick()
    check("auto: executor does nothing without AUTO_ENABLED=1", not orders() and STATE["adds"] == 0)

    # --- plan_id dedupe
    fresh()
    c = await camp()
    row = {"plan_id": "x:1", "campaign_id": c["id"], "post_url": "https://www.tiktok.com/@u/video/1", "platform": "tiktok",
           "window_start": automation.et_midnight(today), "window_end": automation.et_midnight(today), "views_add": 500}
    a = await main.auto_plan_add(main.AutoPlanBody(rows=[row]), who="t")
    b = await main.auto_plan_add(main.AutoPlanBody(rows=[row, {**row, "plan_id": "x:2"}]), who="t")
    check("auto: same plan_id twice is a no-op (inserted/duplicates)",
          a == {"inserted": 1, "duplicates": 0} and b == {"inserted": 1, "duplicates": 1}
          and db.q1("SELECT COUNT(*) n FROM plan_rows WHERE plan_id='x:1'")["n"] == 1, (a, b))
    try:
        await main.auto_plan_add(main.AutoPlanBody(rows=[{**row, "plan_id": "x:3", "campaign_id": 999999}]), who="t")
        check("auto: plan row with unknown campaign -> 400", False)
    except HTTPException as e:
        check("auto: plan row with unknown campaign -> 400", e.status_code == 400)

    # --- practice mode: simulated rows with cost, no panel call
    fresh()
    c = await camp(mode="practice", budget=None)
    plan(c["id"], 1000, likes=5)
    await tick()
    o = orders()
    check("auto: practice mode simulates views + likes, sends nothing",
          [x["state"] for x in o] == ["simulated", "simulated"] and STATE["adds"] == 0
          and o[0]["provider"] == M and o[0]["est_cost"] == 0.015 and o[1]["kind"] == "likes" and o[1]["qty"] == 20, o)
    await tick()
    check("auto: practice tick again adds nothing (one order per post per day)", len(orders()) == 2)

    # --- budget caps block (live)
    fresh(cap=None)
    c = await camp(budget=None)
    db.update("auto_campaigns", c["id"], cap_usd=None)   # v2.12: an old live campaign with neither cap nor budget
    plan(c["id"], 1000)
    await tick()
    check("auto: live with no campaign cap and no weekly budget -> blocked, no panel call",
          orders()[0]["state"] == "blocked" and "no campaign cap" in orders()[0]["error"] and STATE["adds"] == 0, orders())
    await main.auto_campaign_patch(c["id"], {"weekly_budget_usd": 0.01}, who="t")
    await tick()
    check("auto: weekly budget below est cost -> blocked (same row reused)",
          len(orders()) == 1 and "weekly budget" in orders()[0]["error"] and STATE["adds"] == 0, orders())
    await main.auto_campaign_patch(c["id"], {"weekly_budget_usd": 100}, who="t")
    await tick()
    check("auto: no global daily cap -> blocked", "no global daily cap" in orders()[0]["error"] and STATE["adds"] == 0)
    await main.auto_settings({"global_daily_cap_usd": 0.01}, who="t")
    await tick()
    check("auto: global daily cap below est cost -> blocked", "global daily cap" in orders()[0]["error"] and STATE["adds"] == 0)
    await main.auto_settings({"global_daily_cap_usd": 50}, who="t")
    await tick()
    o = orders()
    check("auto: caps raised -> the blocked row is placed once", len(o) == 1 and o[0]["state"] == "placed"
          and o[0]["job_id"] and STATE["adds"] == 1 and db.get_job(o[0]["job_id"])["source"] == "auto", o)
    await tick()
    check("auto: placed order is not placed again the same day", STATE["adds"] == 1 and len(orders()) == 1)

    # --- pause blocks (post), unpause resumes
    fresh()
    c = await camp()
    url, _pid = plan(c["id"], 1000)
    r = await main.auto_pause(main.AutoPauseBody(scope="post", key=url), who="t")
    check("auto: pause returns the original URL as key", r["pause"]["key"] == url and r["pause"]["scope"] == "post", r)
    await tick()
    check("auto: paused post -> no order", not orders() and STATE["adds"] == 0)
    await main.auto_pause(main.AutoPauseBody(scope="all", key=None), who="t")
    await main.auto_unpause(main.AutoUnpauseBody(scope="post", key=url), who="t")
    await tick()
    check("auto: pause all -> no order", not orders() and STATE["adds"] == 0)
    await main.auto_unpause(main.AutoUnpauseBody(scope="all"), who="t")
    await tick()
    check("auto: unpaused -> placed", len(orders(state="placed")) == 1 and STATE["adds"] == 1)

    # --- carry below min, then picked up once it reaches the min
    fresh()
    c = await camp()
    _u, pid = plan(c["id"], 30)
    await tick()
    check("auto: qty below every service min -> nothing placed, carried",
          not orders() and db.q1("SELECT carry FROM plan_rows WHERE plan_id=?", (pid,))["carry"] == 30 and STATE["adds"] == 0)
    fresh()
    c = await camp()
    url, old = plan(c["id"], 30, day=yday, carry=30)
    db.ex("UPDATE plan_rows SET post_url=?, url_key=? WHERE plan_id=?", (url, campaigns.url_key(url), old))
    n[0] -= 1   # same post, today
    _u, pid = plan(c["id"], 30)
    await tick()
    o = orders()
    check("auto: carried 30 + today 30 = 60 placed on the service whose min fits",
          len(o) == 1 and o[0]["qty"] == 60 and o[0]["service"] == "9102" and STATE["add_log"] == [("9102", 60)]
          and db.q1("SELECT carry FROM plan_rows WHERE plan_id=?", (old,))["carry"] == 0, (o, STATE["add_log"]))

    # --- cap at service max, carry the rest; story views never picked; poor score skipped
    fresh(cap=1000)
    c = await camp(budget=1000)
    _u, pid = plan(c["id"], 150000)
    await tick()
    o = orders()
    check("auto: qty above service max -> max placed now, rest carried",
          o[0]["qty"] == 100000 and o[0]["service"] == "9201" and STATE["add_log"] == [("9201", 100000)]
          and db.q1("SELECT carry FROM plan_rows WHERE plan_id=?", (pid,))["carry"] == 50000, (o, STATE["add_log"]))

    # --- in-flight dedupe: open manual job on the post is subtracted
    fresh()
    c = await camp()
    url, _pid = plan(c["id"], 1000)
    jid = db.create_job(provider=P, platform="tiktok", link=url, views_target=500, views_order_id="m1",
                        state="running", created_at=int(time.time()), source="manual")
    db.update_job(jid, views_remaining=400)
    await tick()
    check("auto: in-flight manual views (remaining 400) subtracted -> 600 ordered",
          orders()[0]["qty"] == 600 and STATE["add_log"] == [("9201", 600)], (orders(), STATE["add_log"]))
    db.update_job(jid, state="done")

    # --- low balance on the cheapest panel -> next panel
    fresh()
    c = await camp()
    plan(c["id"], 1000)
    BAL[M] = 0.001
    await tick()
    check("auto: cheapest panel low on balance -> order moves to the next panel",
          orders()[0]["provider"] == P and orders()[0]["service"] == "9102" and STATE["add_log"] == [("9102", 1000)], orders())

    # --- unscored cheapest service -> test order at service min
    fresh()
    del SC[(M, "9201", "tiktok")]
    c = await camp()
    plan(c["id"], 1000)
    await tick()
    o = orders()
    check("auto: unscored cheapest service -> test order at its min", o[0]["is_test"] == 1 and o[0]["qty"] == 100
          and o[0]["service"] == "9201" and STATE["add_log"] == [("9201", 100)], o)
    SC[(M, "9201", "tiktok")] = 80

    # --- clear failure: one re-send of the missing qty on another service, then stop
    fresh()
    c = await camp()
    plan(c["id"], 1000)
    await tick()
    first = orders()[0]
    db.update_job(first["job_id"], views_status="Partial", views_remaining=300)
    await tick()
    o = orders()
    check("auto: partial -> one re-send of the missing 300 on the next best service",
          o[0]["state"] == "retried" and o[1]["attempt"] == 1 and o[1]["retry_of"] == first["id"] and o[1]["qty"] == 300
          and o[1]["service"] == "9102" and STATE["add_log"] == [("9201", 1000), ("9102", 300)], o)
    db.update_job(o[1]["job_id"], views_status="Canceled")
    await tick()
    await tick()
    o = orders()
    check("auto: re-send failed -> stopped + crit alert, no third order",
          len(o) == 2 and o[1]["state"] == "failed" and "stopped" in o[1]["error"] and STATE["adds"] == 2
          and any(a["level"] == "crit" and f"#{o[1]['id']}" in a["text"] for a in automation.alerts(0)), o)

    # --- panel rejects at add -> failed -> re-sent once elsewhere
    fresh()
    c = await camp()
    plan(c["id"], 1000)
    reset(plan=["error"])
    await tick()
    check("auto: panel {error} on add -> failed (nothing placed)", orders()[0]["state"] == "failed")
    await tick()
    check("auto: rejected order re-sent once on another service",
          [x["state"] for x in orders()] == ["retried", "placed"] and orders()[1]["service"] == "9102")

    # --- ambiguous (add timed out) never re-sent
    fresh()
    c = await camp()
    plan(c["id"], 1000)
    reset(plan=["timeout"])
    await tick()
    panels._down.clear()
    await tick()
    await tick()
    o = orders()
    check("auto: add timeout -> ambiguous, never auto re-sent", len(o) == 1 and o[0]["state"] == "ambiguous"
          and STATE["adds"] == 1 and automation.spent(0) == o[0]["est_cost"], o)
    r = await main.auto_resend(o[0]["id"], who="t")
    check("auto: manual resend of an ambiguous order places a new one (attempt 1)",
          r["state"] == "placed" and r["retry_of"] == o[0]["id"] and r["attempt"] == 1 and STATE["adds"] == 2
          and orders()[0]["state"] == "retried", r)
    try:
        await main.auto_resend(r["id"], who="t")
        check("auto: resend of a placed order -> 400", False)
    except HTTPException as e:
        check("auto: resend of a placed order -> 400", e.status_code == 400)

    # --- likes only after the day's views job completed, scaled + raised to the floor
    fresh()
    c = await camp()
    plan(c["id"], 1000, likes=50)
    await tick()
    check("auto: no likes while views are still running", not orders(kind="likes") and STATE["adds"] == 1)
    db.update_job(orders()[0]["job_id"], views_status="Completed", completed_at=int(time.time()))
    await tick()
    lk = orders(kind="likes")
    check("auto: views completed -> likes placed on the same panel (likes_add x delivered/ordered)",
          orders(kind="views")[0]["state"] == "completed" and len(lk) == 1 and lk[0]["qty"] == 50
          and lk[0]["state"] == "placed" and lk[0]["provider"] == M and STATE["add_log"][-1] == ("1126", 50), lk)
    await tick()
    check("auto: likes placed once", len(orders(kind="likes")) == 1 and STATE["adds"] == 2)
    fresh()
    c = await camp()
    plan(c["id"], 1000, likes=5)
    await tick()
    db.update_job(orders()[0]["job_id"], views_status="Completed", completed_at=int(time.time()))
    await tick()
    check("auto: small likes raised to the panel floor (20)", orders(kind="likes")[0]["qty"] == 20)

    # --- old campaign queue skips automation posts; executor skips posts with an open queue order
    fresh()
    c = await camp()
    url, _pid = plan(c["id"], 1000)
    LQ = mk_link(campaign_id=770)
    os.environ.pop("AUTO_ENABLED", None)
    check("auto: executor off -> campaign queue does not skip planned posts", not automation.in_automation(campaigns.url_key(url)))
    os.environ["AUTO_ENABLED"] = "1"
    cp = await camp(mode="practice")
    urlp, _p = plan(cp["id"], 1000)
    check("auto: practice campaign posts are not skipped by the campaign queue", not automation.in_automation(campaigns.url_key(urlp)))
    r = await place(mk_item(LQ, url)["id"])
    check("auto: campaign-queue placement skips a post planned by automation",
          r["state"] == "skipped" and r["error"] == "in automation" and STATE["adds"] == 0, r)
    fresh()
    c = await camp()
    url2 = "https://www.tiktok.com/@u/video/88888"
    it = mk_item(LQ, url2)
    jq = db.create_job(provider=P, platform="tiktok", link=url2, views_target=1000, views_order_id="q1",
                       state="running", created_at=int(time.time()), source="campaign")
    db.update("queue", it["id"], state="placed", job_id=jq)
    automation.insert_plan([{"plan_id": "q:1", "campaign_id": c["id"], "post_url": url2, "platform": "tiktok",
                             "window_start": automation.et_midnight(today), "window_end": automation.et_midnight(today),
                             "views_add": 1000}])
    await tick()
    check("auto: executor skips a post with an open campaign-queue order",
          orders()[0]["state"] == "blocked" and "campaign-queue" in orders()[0]["error"] and STATE["adds"] == 0, orders())
    db.update_job(jq, state="done")
    os.environ.pop("AUTO_ENABLED", None)

    # --- cancel: refused (unsupported / error) vs accepted; pause with cancel_running
    fresh()
    c = await camp()
    url, _pid = plan(c["id"], 1000)
    await tick()
    oid = orders()[0]["id"]
    STATE["cancel_ok"] = False
    r = await main.auto_cancel(oid, who="t")
    check("auto: panel refuses cancel -> ok false with the panel's reason, order still placed",
          r["ok"] is False and "Incorrect request" in r["reason"] and orders()[0]["state"] == "placed", r)
    STATE["cancel_ok"] = True
    r = await main.auto_pause(main.AutoPauseBody(scope="campaign", key=c["id"], cancel_running=True), who="t")
    check("auto: pause + cancel_running cancels running orders in scope",
          r["cancelled"] == [oid] and not r["cancel_refused"] and orders()[0]["state"] == "cancelled", r)
    STATE["cancel_ok"] = False

    # --- single executor: another owner's fresh lease -> tick does nothing
    fresh()
    c = await camp()
    plan(c["id"], 1000)
    db.ex("INSERT INTO settings (k, v) VALUES ('auto:lock', ?)", (f"other:{int(time.time())}",))
    await tick()
    check("auto: another executor holds the lease -> nothing placed", not orders() and STATE["adds"] == 0)
    db.ex("DELETE FROM settings WHERE k='auto:lock'")
    await tick()
    check("auto: lease released after a tick", STATE["adds"] == 1 and not db.q1("SELECT k FROM settings WHERE k='auto:lock'"))


    # --- review fixes (v2.7.1)
    # 1. platform must match the post URL (insert + place)
    fresh()
    c = await camp()
    bad = {"plan_id": "pf:1", "campaign_id": c["id"], "post_url": "https://www.instagram.com/reel/PF1", "platform": "tiktok",
           "window_start": automation.et_midnight(today), "window_end": automation.et_midnight(today), "views_add": 500}
    try:
        await main.auto_plan_add(main.AutoPlanBody(rows=[bad]), who="t")
        check("fix1: plan row whose URL is another platform -> 400", False)
    except HTTPException as e:
        check("fix1: plan row whose URL is another platform -> 400", e.status_code == 400 and "tiktok" in e.detail)
    _u, pid = plan(c["id"], 1000)
    db.ex("UPDATE plan_rows SET post_url='https://www.instagram.com/reel/PF2' WHERE plan_id=?", (pid,))
    await tick()
    check("fix1: place() re-checks the URL platform -> blocked, no panel call",
          orders()[0]["state"] == "blocked" and "not a tiktok link" in orders()[0]["error"] and STATE["adds"] == 0, orders())

    # 2a. pause created mid-tick (after the pick) -> no panel call
    fresh()
    c = await camp()
    url, _pid = plan(c["id"], 1000)
    real_pick = automation.pick

    async def pick_then_pause(*a, **kw):
        got = await real_pick(*a, **kw)
        db.ex("INSERT OR IGNORE INTO pauses (scope, key, label, created_at) VALUES ('post', ?, ?, 1)",
              (campaigns.url_key(url), url))
        return got
    automation.pick = pick_then_pause
    try:
        await tick()
    finally:
        automation.pick = real_pick
    check("fix2: pause added during the tick is honoured before the panel call",
          orders()[0]["state"] == "blocked" and orders()[0]["error"] == "paused" and STATE["adds"] == 0, orders())
    # 2b. pause + cancel_running waits for a running tick, then cancels what it placed
    fresh()
    c = await camp()
    _u, pid = plan(c["id"], 1000)
    await automation._lock.acquire()
    task = asyncio.create_task(main.auto_pause(main.AutoPauseBody(scope="campaign", key=c["id"], cancel_running=True), who="t"))
    await asyncio.sleep(0.05)
    waiting = not task.done()
    late = db.insert("auto_orders", plan_id=pid, campaign_id=c["id"], url_key="k", day=today, kind="views", provider=M,
                     service="9201", qty=1000, est_cost=0.015, state="placed", order_id="1999", created_at=int(time.time()))
    STATE["cancel_ok"] = True
    automation._lock.release()
    r = await task
    STATE["cancel_ok"] = False
    check("fix2: pause + cancel_running waits for the tick and cancels orders it placed",
          waiting and r["cancelled"] == [late] and automation.get_order(late)["state"] == "cancelled", r)

    # 3. stale > 24 h: cancel first; re-send only if the panel accepted
    for ok in (False, True):
        fresh()
        c = await camp()
        plan(c["id"], 1000)
        await tick()
        o = orders()[0]
        db.update_job(o["job_id"], status_checked_at=int(time.time()), created_at=int(time.time()) - 25 * 3600)
        STATE["cancel_ok"] = ok
        await tick()
        await tick()
        STATE["cancel_ok"] = False
        o2 = orders()
        if not ok:
            check("fix3: stale order, cancel refused -> stays placed, marked, crit alert, no re-send",
                  len(o2) == 1 and o2[0]["state"] == "placed" and o2[0]["error"].startswith("stale, cancel refused")
                  and STATE["adds"] == 1 and any("refused to cancel" in a["text"] for a in automation.alerts(0)), o2)
        else:
            check("fix3: stale order, cancel accepted -> one re-send of the missing qty elsewhere",
                  [x["state"] for x in o2] == ["retried", "placed"] and o2[1]["service"] == "9102" and STATE["adds"] == 2, o2)

    # 5. one views order per post per day across campaigns; other campaigns' open auto jobs are in flight
    fresh()
    c1, c2 = await camp(), await camp()
    url, _pid = plan(c1["id"], 1000)
    n[0] -= 1
    plan(c2["id"], 1000)
    await tick()
    check("fix5: two campaigns on one post -> one views order that day", len(orders(kind="views")) == 1 and STATE["adds"] == 1)
    fresh()
    c1, c2 = await camp(), await camp()
    url, _pid = plan(c2["id"], 1000)
    jo = db.create_job(provider=M, platform="tiktok", link=url, views_target=1000, views_order_id="a1", state="running",
                       created_at=int(time.time()), source="auto", created_by=f"auto:{c1['id']}")
    db.update_job(jo, views_remaining=400)
    await tick()
    check("fix5: another campaign's open auto job is subtracted as in flight", orders()[0]["qty"] == 600, orders())
    db.update_job(jo, state="done")

    # 6. every field validated first, all or nothing
    fresh()
    c = await camp()
    good = {"plan_id": "aon:1", "campaign_id": c["id"], "post_url": "https://www.tiktok.com/@u/video/5", "platform": "tiktok",
            "window_start": automation.et_midnight(today), "window_end": automation.et_midnight(today), "views_add": 500}
    try:
        await main.auto_plan_add(main.AutoPlanBody(rows=[good, {**good, "plan_id": "aon:2", "window_start": "soon"}]), who="t")
        check("fix6: a bad window int -> 400 and no row inserted", False)
    except HTTPException as e:
        check("fix6: a bad window int -> 400 and no row inserted",
              e.status_code == 400 and not db.q1("SELECT id FROM plan_rows WHERE plan_id LIKE 'aon:%'"))

    # 7. carry expires after 3 days and never crosses a pause; retries expire after 2 days
    fresh()
    c = await camp()
    old_day = (automation.dt.date.fromisoformat(today) - automation.dt.timedelta(days=4)).isoformat()
    url, old = plan(c["id"], 30, day=old_day, carry=30)
    n[0] -= 1
    plan(c["id"], 30)
    await tick()
    check("fix7: carry older than 3 days is not used", not orders() and STATE["adds"] == 0)
    fresh()
    c = await camp()
    url, old = plan(c["id"], 30, day=yday, carry=30)
    await main.auto_pause(main.AutoPauseBody(scope="post", key=url), who="t")
    await main.auto_unpause(main.AutoUnpauseBody(scope="post", key=url), who="t")
    n[0] -= 1
    plan(c["id"], 30)
    await tick()
    check("fix7: a pause drops carried views", not orders() and db.q1("SELECT carry FROM plan_rows WHERE plan_id=?", (old,))["carry"] == 0)
    fresh()
    c = await camp()
    _u, pid = plan(c["id"], 1000)
    reset(plan=["error"])
    await tick()
    db.ex("UPDATE auto_orders SET created_at=?", (int(time.time()) - 3 * 86400,))
    await tick()
    check("fix7: a failed order older than 2 days is not auto re-sent", len(orders()) == 1 and STATE["adds"] == 1, orders())

    # 8. only Default-type catalog services
    check("fix8: Subscription-type service (cheapest, scored 95) is never a candidate",
          all(x["service"] != "9301" for x in await automation.candidates("tiktok", await automation.context())))

    for name in ("Facebook Page Random Review | Brazil", "Instagram Photo Views | Max 10M"):
        check(f"views filter drops '{name}'",
              any(w in campaigns.svc_text({"name": name, "category": ""}) for w in automation.NOT_POST_VIEWS))
    check("views filter keeps 'Facebook Reels Views'",
          not any(w in campaigns.svc_text({"name": "Facebook Reels Views", "category": ""}) for w in automation.NOT_POST_VIEWS))

    # --- routes: overview / orders / plan shapes, audit rows
    fresh()
    c = await camp()
    plan(c["id"], 1000)
    await tick()
    ov = await main.auto_overview(_="t")
    ac = ov["campaigns"][0]
    check("auto: overview shape", set(ov) == {"enabled", "dry_run", "settings", "spent_today_usd", "campaigns", "pauses",
                                              "alerts", "balances"} and ac["spent_week_usd"] == 0.015
          and ac["views_ordered_week"] == 1000 and ac["posts"] == 1 and ac["mode"] == "live", ov)
    panels._services[M] = (time.time(), CAT[M])   # service_name reads the warm catalog cache only
    lst = await main.auto_orders(campaign_id=c["id"], creator_id=None, post=None, state=None, kind=None, page=1, _="t")
    it = lst["items"][0]
    check("auto: orders list shape", lst["total"] == 1 and it["post_url"] and it["platform"] == "tiktok"
          and it["service_name"] == "TikTok Views MTP" and it["can_cancel"] is True, lst)
    pr = (await main.auto_plan(campaign_id=c["id"], _="t"))["rows"][0]
    check("auto: plan rows carry ordered/delivered/paused", pr["ordered_views"] == 1000 and pr["delivered_views"] == 0
          and pr["paused"] is False, pr)
    aud = await main.auto_audit(page=1, _="t")
    acts = {x["action"] for x in db.q("SELECT action FROM audit_log WHERE username='t'")}
    check("auto: every POST/PATCH wrote an audit row with the caller",
          {"auto.campaign.create", "auto.campaign.patch", "auto.plan.add", "auto.pause", "auto.unpause",
           "auto.settings", "auto.order.resend", "auto.order.cancel"} <= acts and aud["total"] > 0, acts)
    check("auto: money routes are audited too", {"order.place", "bulk.place"} <= acts, acts)

    panels._services.pop(M, None)
    panels.services_cached, panels.balance_cached, automation._scores = real


asyncio.run(main())


async def planner_tests():
    """v2.8 Planner. No network: sheet text, redirects, Clippers and post counts are all mocked."""
    import datetime as dt
    import automation
    import main
    import planner
    import sources
    from fastapi import HTTPException
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots",
              "jobs"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%'")
    automation._alerts.clear()
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"

    def day(n):
        return (dt.date.fromisoformat(TODAY) + dt.timedelta(days=n)).isoformat()

    def camp(**kw):
        f = {"name": "P", "mode": "live", "views_goal": 100000, "start_date": day(-5), "end_date": day(9),
             "creators_planned": 1, "posts_per_day": 1, "ramp_weeks": 3, "approved": 1, "created_at": NOW, **kw}
        return db.insert("auto_campaigns", **f)
    n = [0]

    def post(cid, d, acct, observed=None, likes=0, t24=None, platform="instagram", up=None, **kw):
        n[0] += 1
        url = f"https://www.instagram.com/reel/P{n[0]}/" if platform == "instagram" else \
            f"https://www.facebook.com/reel/{9000000 + n[0]}"
        f = {"post_key": sources.post_key(platform, url), "url_key": campaigns.url_key(url), "campaign_id": cid,
             "creator_id": acct, "account_handle": acct, "platform": platform, "url": url, "source": "sheet",
             "day": d, "uploaded_at": up if up is not None else automation.et_midnight(d) + 12 * 3600, "t24": t24,
             "w_random": 1.0, "p_likes": 0.01, "ramp_start": d, "observed_views": observed, "base_views": observed, "base_day": d,
             "observed_likes": likes if observed is not None else None,
             "checked_at": NOW if observed is not None else None, **kw}
        f.setdefault("created_at", automation.et_midnight(d))   # seen on its day: the whole curve (v2.10 share)
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (db.insert("auto_posts", **f),))

    def G(pid):
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (pid,))

    def row(cid, p):
        return db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (f"{cid}:{p['post_key']}:{TODAY}",))

    # --- sheet parsing
    csv_text = ('"XTO daily posts","","",""\n'
                '"Date (ET)","Creator","IG AM 1","IG AM 2","IG PM 1","IG PM 2","FB AM 1","FB AM 2","FB PM 1","FB PM 2",'
                '"Done (x/8)","Views","Status","Remarks","Bio link","Cancelled / account issues","TikTok"\n'
                '"Mon 19 Oct","alice","https://www.instagram.com/reel/AAA1/?igsh=x","","https://instagram.com/p/AAA2/",'
                '"x","https://www.facebook.com/share/r/SHR1/","","","","2/8","","","","","",'
                '"https://www.tiktok.com/@al/video/7333"\n'
                '"Tue 20 Oct","alice","https://www.instagram.com/reel/AAA3/","","","","","","","","","","","","","",""\n'
                '"Mon 19 Oct","bob","","","","","","","","","","","","","","banned",""\n'
                '"Thu 1 Jan","carol","https://www.instagram.com/reel/CCC1/","","","","","","","","","","","","","",""\n')
    sh = sources.parse_sheet(csv_text, dt.date(2026, 10, 24))
    urls = [(x["platform"], x["day"], x["slot"]) for x in sh["links"]]
    check("sheet: links per slot cell with platform, ET date (year inferred), slot; non-URL cell ignored",
          urls[:5] == [("instagram", "2026-10-19", "IG AM 1"), ("instagram", "2026-10-19", "IG PM 1"),
                       ("facebook", "2026-10-19", "FB AM 1"), ("tiktok", "2026-10-19", "TikTok"),
                       ("instagram", "2026-10-20", "IG AM 1")] and urls[5][1] == "2027-01-01", urls)
    s19 = [s for s in sh["slots"] if s["creator_id"] == "alice" and s["day"] == "2026-10-19"]
    check("sheet: 8 slots per creator-day, empty / non-URL = None", len(s19) == 8
          and [s["slot"] for s in s19 if s["url"] is None] == ["IG AM 2", "IG PM 2", "FB AM 2", "FB PM 1", "FB PM 2"], s19)
    check("sheet: cancelled = 'Cancelled / account issues' non-empty", sh["cancelled"] == {"alice": False, "bob": True,
                                                                                          "carol": False}, sh["cancelled"])
    am = [x for x in sh["links"] if x["slot"] == "IG AM 1"][0]["uploaded_at"]
    check("sheet: uploaded_at = slot deadline (AM 12:00 ET)",
          am == int(dt.datetime(2026, 10, 19, 12, 0, tzinfo=ET).timestamp()), am)
    check("sheet: source_ref forms", sources.sheet_ref("abc:Daily Posts") == ("abc", "Daily Posts")
          and sources.sheet_ref("https://docs.google.com/spreadsheets/d/abc/edit#gid=0") == ("abc", "Daily Posts")
          and sources.refs({"source": "sheet", "source_ref": "[{\"source\":\"clippers\",\"ref\":\"7\"}]"}) == [("clippers", "7")])

    # --- post_key + one-time redirect resolve
    follows = []

    async def f_follow(u):
        follows.append(u)
        return "https://www.facebook.com/reel/1234567890/?s=1"
    sources._follow = f_follow
    check("post_key: IG reel and /p/ forms are one post",
          sources.post_key("instagram", "https://instagram.com/p/AbC/") == sources.post_key(
              "instagram", "https://www.instagram.com/reel/AbC/?igsh=1") == "instagram:AbC")
    check("post_key: tiktok video id", sources.post_key("tiktok", "https://www.tiktok.com/@a/video/123?x") == "tiktok:123")
    r1 = await sources.resolve("facebook", "https://www.facebook.com/share/r/SHR1/")
    r2 = await sources.resolve("facebook", "https://www.facebook.com/share/r/SHR1/")
    check("post_key: FB share link resolved once, then read from the store",
          r1 == r2 and sources.post_key("facebook", r1) == "facebook:1234567890" and len(follows) == 1, (r1, follows))

    # --- curve
    w21, w2, w5v = planner.weights(21), planner.weights(2), planner.weights(10, viral=True)
    check("curve: weights sum to 1; days 1-3 = 50% (normal) / 70% (viral); 1:1.5:2",
          abs(sum(w21) - 1) < 1e-9 and abs(sum(w21[:3]) - 0.5) < 1e-9 and abs(sum(w5v) - 1) < 1e-9
          and abs(sum(w5v[:3]) - 0.7) < 1e-9 and abs(w21[1] / w21[0] - 1.5) < 1e-9
          and abs(w21[4] / w21[3] - 0.85) < 1e-9 and abs(sum(w2) - 1) < 1e-9 and abs(w2[1] / w2[0] - 1.5) < 1e-9,
          (w21[:5], w2, w5v[:4]))
    c0 = {"ramp_weeks": 3, "end_date": day(3)}
    check("curve: ramp squeezed to end_date + 2 buffer days (min 1) (v2.13)",
          planner.ramp_len({"reason": "base", "ramp_start": day(-1)}, c0) == 7
          and planner.ramp_len({"reason": "base", "ramp_start": day(3)}, c0) == 3
          and planner.ramp_len({"reason": "base", "ramp_start": day(5)}, c0) == 1
          and planner.ramp_len({"reason": "viral", "ramp_start": day(-20)}, {"ramp_weeks": 3, "end_date": day(30)}) == 10)

    # --- targets: avg, 5x cap + shortfall, views_add subtracts in-flight only, likes
    A, B = camp(), camp(name="other", approved=0)
    for i in range(3):
        post(B, day(-10 - i), "acct_a", observed=1000)      # acct_a normal = 1000 -> cap 5000
    pa = post(A, day(-1), "acct_a", observed=100, likes=5, up=NOW - 3600)   # < 24 h: no v2.8.3 campaign fallback yet
    pb = post(A, day(-1), "acct_b", observed=0, up=NOW - 3600)
    j_done = db.create_job(provider=P, platform="instagram", link=pa["url"], views_target=200, views_order_id="1",
                           state="done", created_at=NOW - 3600)
    db.update_job(j_done, completed_at=NOW - 1800)
    db.create_job(provider=P, platform="instagram", link=pa["url"], views_target=500, views_order_id="2",
                  state="running", created_at=NOW - 3600)
    db.update_job(db.q1("SELECT MAX(id) i FROM jobs")["i"], views_remaining=300)
    s = planner.plan(automation._camp(A), NOW)
    # rate = 2 posts in 3 days / 3, days_left 9 -> avg = 100000 x 1.125 (v2.13 margin) / (2 + 6) = 14062.5
    check("target: avg = remaining goal x (1 + margin) / (normal in ramp + expected future)",
          s["avg"] == 14062 and s["views_aim"] == 112500, s)
    check("target: 5x normal(account) cap + shortfall",
          G(pa["id"])["target_total"] == 5000 and s["shortfall"] == 9062, s)
    check("no baseline (account or campaign mean): base post held, no row",
          s["held_no_baseline"] == 1 and row(A, pb) is None and row(A, pa) is not None, s)
    check("target: shortfall raises the red alert",
          any("short 9,062 views" in a["text"] and a["level"] == "crit" for a in automation.alerts(0)), automation.alerts(0))
    for i in range(3):
        post(B, day(-10 - i), "acct_b", observed=4000)      # acct_b / acct_c baseline from now on
        post(B, day(-10 - i), "acct_c", observed=4000)
    ra = row(A, pa)
    F2 = (1 + 1.5) / 4.5 * 0.5            # day 2 of an 11-day (squeezed) ramp
    check("views_add = target x F(d) - observed - in-flight (delivered not subtracted again)",
          ra["views_add"] == round(5000 * F2 - 100 - 300) and ra["reason"] == "base" and ra["priority"] == 3
          and ra["target_total_views"] == 5000 and ra["window_start"] == automation.et_midnight(TODAY), dict(ra))
    check("likes_add = round(desired x p) - observed likes - in-flight likes",
          ra["likes_add"] == round(5000 * F2 * 0.01) - 5 and ra["comments_add"] == 0, dict(ra))
    db.ex("UPDATE plan_rows SET carry=40 WHERE plan_id=?", (ra["plan_id"],))
    db.ex("UPDATE plan_rows SET day=? WHERE plan_id=?", (day(-1), ra["plan_id"]))
    check("in-flight also counts the executor's carried views", planner.order_facts(TODAY, NOW)[pa["url_key"]]["carry"]
          == {A: 40})
    db.ex("DELETE FROM plan_rows")

    # --- plan_id idempotent across two runs a day
    planner.plan(automation._camp(A), NOW)
    before = db.q("SELECT plan_id, views_add FROM plan_rows ORDER BY plan_id")
    db.insert("auto_orders", plan_id=row(A, pa)["plan_id"], campaign_id=A, url_key=pa["url_key"], day=TODAY,
              kind="views", qty=10, state="simulated", attempt=0, created_at=NOW)
    db.ex("UPDATE auto_posts SET observed_views=0 WHERE id=?", (pa["id"],))
    pc = post(A, TODAY, "acct_c", observed=0)
    s2 = planner.plan(automation._camp(A), NOW + 6 * 3600)
    after = db.q("SELECT plan_id, views_add FROM plan_rows ORDER BY plan_id")
    check("second run of the day: new post added, no duplicate plan_id, a row with an order stays frozen (v2.8.3)",
          s2["rows"] == 1 and len(after) == len(before) + 1 and len({r["plan_id"] for r in after}) == len(after)
          and row(A, pc) is not None and before[0] in after and before[0]["plan_id"] == row(A, pa)["plan_id"],
          (before, after))
    db.ex("DELETE FROM auto_orders")

    # --- paused / cancelled / unapproved
    db.ex("DELETE FROM plan_rows")
    db.insert("pauses", scope="creator", key="acct_b", label="acct_b", created_by="t", created_at=NOW)
    db.insert("auto_creators", campaign_id=A, creator_id="acct_c", cancelled=1)
    planner.plan(automation._camp(A), NOW)
    check("paused creator and cancelled creator get no rows", row(A, pb) is None and row(A, pc) is None
          and row(A, pa) is not None)
    db.ex("DELETE FROM pauses"); db.ex("DELETE FROM auto_creators")
    try:
        await planner.run_campaign(B, NOW)
        ok = False
    except ValueError as e:
        ok = "not approved" in str(e)
    check("unapproved campaign: run refused", ok)
    os.environ["AUTO_ENABLED"] = "0"
    await planner.tick()
    check("tick does nothing without AUTO_ENABLED=1", planner.last_run(A) is None)

    # --- viral: eligibility, one test per post, approve, decay
    db.ex("DELETE FROM plan_rows")
    V = camp(name="viral", views_goal=1000000)
    for i in range(3):
        post(V, day(-8 - i), "vv", observed=2000, t24=100)
    for i in range(4):
        post(V, day(-1), f"x{i}", t24=50, observed=60, up=NOW - 30 * 3600)
    hot = post(V, day(-1), "vv", observed=5000, t24=900, up=NOW - 30 * 3600)
    young = post(V, TODAY, "vv", observed=10, t24=None, up=NOW - 3600)
    thin = post(V, day(-1), "solo", observed=5000, t24=900, up=NOW - 30 * 3600)   # account has 1 post
    real_random = planner.rng.random
    planner.rng.random = lambda: 0.0
    picked = planner.pick_viral(automation._camp(V), db.q("SELECT * FROM auto_posts WHERE campaign_id=?", (V,)),
                                lambda p: True, NOW)
    check("viral: top-25% t24, 24-72 h old, account >= 3 posts -> picked; others not",
          picked == [hot["id"]] and G(thin["id"])["viral_state"] is None and G(young["id"])["viral_state"] is None,
          (picked, G(thin["id"])["viral_state"]))
    again = planner.pick_viral(automation._camp(V), db.q("SELECT * FROM auto_posts WHERE campaign_id=?", (V,)),
                               lambda p: True, NOW)
    check("viral: a post is tested once", again == [] and G(hot["id"])["viral_state"] == "picked")
    planner.rng.random = real_random
    d1 = post(V, TODAY, "vv", observed=0, up=NOW - 1000)
    d2 = post(V, TODAY, "vv", observed=0, up=NOW - 900)
    d3 = post(V, TODAY, "vv", observed=0, up=NOW - 800)
    d4 = post(V, TODAY, "vv", observed=0, up=NOW - 700)
    fb = post(V, TODAY, "vv", observed=0, up=NOW - 600, platform="facebook")
    pv = planner.approve(hot["id"], 100000, NOW)
    check("viral approve: typed peak, ramp starts today", pv["reason"] == "viral" and pv["peak"] == 100000
          and pv["ramp_start"] == TODAY and pv["viral_state"] == "approved", pv)
    dec = [G(x["id"]) for x in (young, d1, d2, d3, d4, fb)]
    check("decay: next 3 posts of that account, same platform, peak x .60-.70 / .40-.50 / .08-.12",
          [d["reason"] for d in dec] == ["decay", "decay", "decay", "base", "base", "base"]
          and 60000 <= dec[0]["peak"] <= 70000 and 40000 <= dec[1]["peak"] <= 50000 and 8000 <= dec[2]["peak"] <= 12000
          and all(d["viral_parent"] == hot["id"] for d in dec[:3]), [(d["reason"], d["peak"]) for d in dec])
    try:
        await main.auto_viral_approve(hot["id"], None, who="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 400
    check("viral approve twice -> 400", ok)
    try:
        await main.auto_viral_approve(d1["id"], main.ViralBody(peak=None), who="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 400 and "not waiting" in e.detail
    check("viral approve of a post that was never picked -> 400", ok)
    sv = planner.plan(automation._camp(V), NOW)
    rv, rd = row(V, hot), row(V, young)
    check("viral row: priority 1, 70% in days 1-3, ignores the cap",
          rv["reason"] == "viral" and rv["priority"] == 1 and rv["target_total_views"] == 100000
          and rv["views_add"] == round(5000 + (100000 - 5000) * (1 / 4.5 * 0.7) - 5000), dict(rv))   # v2.10.2 base 5000
    check("decay row: priority 2, target = max(decay target, normal target)",
          rd["reason"] == "decay" and rd["priority"] == 2 and rd["target_total_views"] >= dec[0]["peak"], dict(rd))
    check("special targets = viral + decay peaks in ramp", sv["special_targets"] == 100000 + sum(d["peak"] for d in dec[:3]), sv)
    planner.rng.random = lambda: 0.0
    late = post(V, day(-1), "vv", observed=9000, t24=950, up=NOW - 30 * 3600)
    check("viral: no new pick for an account with a viral post in the last 14 days",
          planner.pick_viral(automation._camp(V), db.q("SELECT * FROM auto_posts WHERE campaign_id=?", (V,)),
                             lambda p: True, NOW) == [] and G(late["id"])["viral_state"] is None)
    planner.rng.random = real_random
    db.ex("UPDATE auto_posts SET viral_state='picked' WHERE id=?", (thin["id"],))
    await main.auto_viral_skip(thin["id"], who="t")
    check("viral skip: never re-picked", G(thin["id"])["viral_state"] == "skipped")
    hp = planner.approve(planner.find_post(V, d4["url"])["id"], 50000, NOW, handpick=True)
    check("hand-pick any post in scope as viral", hp["reason"] == "viral" and hp["peak"] == 50000)
    try:
        await main.auto_viral_handpick(main.HandpickBody(campaign_id=V, post_url="https://instagram.com/reel/NOPE/"),
                                       who="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 404
    check("hand-pick of a post not in the campaign -> 404", ok)

    # --- full run from the sheet: dedupe across campaigns, deleted never re-checked, unreadable skipped
    calls = {}
    COUNTS = {}

    async def f_counts(platform, url):
        k = sources.post_key(platform, url)
        calls[k] = calls.get(k, 0) + 1
        return COUNTS.get(k, (1000, 10))
    real_gc0 = sources.get_counts
    sources.get_counts = f_counts
    d0 = dt.date.fromisoformat(day(-1)).strftime("%a %d %b")
    sheet = ('"Date (ET)","Creator","IG AM 1","IG AM 2","FB AM 1","Cancelled / account issues"\n'
             f'"{d0}","zed","https://www.instagram.com/reel/S1/","https://www.instagram.com/reel/S2/",'
             '"https://www.facebook.com/share/r/SHR1/",""\n'
             f'"{d0}","yan","https://www.instagram.com/reel/S3/","",""\n')

    async def f_sheet(ref):
        return sheet
    sources.fetch_sheet = f_sheet

    async def f_subs(cid):
        return [{"id": 1, "url": "https://www.instagram.com/reel/S1/", "platform": "instagram", "clipper": "zz",
                 "submitted_at": "2026-10-19T15:00:00Z", "status": "approved"}]
    clippers.submissions = f_subs
    S = camp(name="sheet", source="sheet", source_ref="sheetid:Daily Posts")
    C2 = camp(name="clip", source="clippers", source_ref="5")
    for i in range(3):   # baselines, so base rows are not held
        for acct, plat in (("zed", "instagram"), ("zed", "facebook"), ("yan", "instagram")):
            post(B, day(-10 - i), acct, observed=3000, platform=plat)
    COUNTS["instagram:S2"] = sources.DELETED
    COUNTS["instagram:S3"] = None
    r = await planner.run_campaign(S, NOW)
    check("run: sheet posts in, FB share resolved, counts checked",
          r["new_posts"] == 4 and r["deleted"] == 0 and r["unreadable"] == 1 and not r["errors"]
          and db.q1("SELECT id FROM auto_posts WHERE post_key='facebook:1234567890' AND campaign_id=?", (S,)), r)
    keys = {x["url_key"] for x in db.q("SELECT url_key FROM plan_rows WHERE campaign_id=?", (S,))}
    check("run: not-found (1st read) and unreadable posts get no row; others do",
          keys == {"ig:S1", "fb:1234567890"}, keys)
    r2 = await planner.run_campaign(C2, NOW)
    check("dedupe across sources/campaigns: first seen wins", r2["new_posts"] == 0
          and db.q1("SELECT campaign_id FROM auto_posts WHERE post_key='instagram:S1'")["campaign_id"] == S, r2)
    r3 = await planner.run_campaign(S, NOW + 60)
    s2_ = db.q1("SELECT status FROM auto_posts WHERE post_key='instagram:S2'")["status"]
    check("deleted only after 2 not-found reads in a row; unreadable post sits out the next run",
          r3["deleted"] == 1 and s2_ == "deleted" and r3["backoff"] == 1 and calls["instagram:S3"] == 1, (r3, calls))
    await planner.run_campaign(S, NOW + 120)
    check("deleted post never re-checked (scraper 404 is billed); backed-off post checked again",
          calls["instagram:S2"] == 2 and calls["instagram:S3"] == 2, calls)
    check("run summary stored", planner.last_run(S)["at"] == NOW + 120)
    check("scheduler: due at a check time not yet run",
          planner.due(automation._camp(S), NOW + 120) is False
          and planner.due(automation._camp(S), int(dt.datetime(2026, 10, 21, 1, 1, tzinfo=ET).timestamp())) is True)

    # --- dashboard + routes
    db.ex("INSERT OR REPLACE INTO auto_slots (campaign_id, creator_id, day, slot, url) VALUES (?,?,?,?,NULL)",
          (S, "yan", day(-1), "IG AM 2"))
    dash = planner.dashboard(S, NOW)
    check("dashboard shape", {"goal", "achieved", "gap", "pct_done", "days_left", "shortfall", "creators",
                              "posting_rate", "submissions", "missed_slots", "slacking", "viral", "last_run",
                              "running", "alerts", "posts", "campaign_id"} <= set(dash)
          and dash["submissions"]["yesterday"] == 4 and dash["achieved"] == 2000, dash)
    sl = {x["creator_id"]: x for x in dash["slacking"]}
    check("dashboard: missed slots by name, slacking ranked with a ready message",
          sl["yan"]["missed"] == 2 and "IG AM 2" in sl["yan"]["message"] and "FB AM 1" in sl["yan"]["message"]
          and dash["slacking"][0]["creator_id"] == "yan", dash["slacking"])
    check("dashboard: posting rate below 80% of plan -> alert",
          dash["posting_rate"]["low"] is False and dash["posting_rate"]["plan"] == 1
          and (db.ex("UPDATE auto_campaigns SET creators_planned=10 WHERE id=?", (S,)) and False
               or planner.dashboard(S, NOW)["posting_rate"]["low"] is True)
          and any("below 80%" in a["text"] for a in planner.dashboard(S, NOW)["alerts"]))
    vl = planner.viral_lists(V, NOW)
    check("viral route", set(await main.auto_viral(campaign_id=V, _="t")) == {"picks", "in_progress"})
    check("viral lists: picks + in progress with day / delivered / decay",
          vl["picks"] == [] and {x["id"] for x in vl["in_progress"]} == {hot["id"], d4["id"]}
          and [x for x in vl["in_progress"] if x["id"] == hot["id"]][0]["ramp_day"] == 1
          and len([x for x in vl["in_progress"] if x["id"] == hot["id"]][0]["decay"]) == 3, vl)
    pl = await main.auto_posts(campaign_id=S, status=None, reason=None, page=1, _="t")
    check("posts route", pl["total"] == 4 and {"post_key", "observed_views", "paused", "status"} <= set(pl["items"][0]), pl)
    db.ex("UPDATE auto_campaigns SET approved=0 WHERE id=?", (S,))
    try:
        await main.auto_planner_run(S, who="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 400
    check("run route: unapproved -> 400", ok)
    db.ex("UPDATE auto_posts SET viral_state='picked' WHERE id=?", (late["id"],))
    await main.auto_viral_approve(late["id"], main.ViralBody(peak=7000), who="t")
    await main.auto_viral_handpick(main.HandpickBody(campaign_id=V, post_url=fb["url"].replace("www.", ""), peak=1000), who="t")
    acts = {x["action"] for x in db.q("SELECT action FROM audit_log WHERE action LIKE 'auto.viral%'")}
    check("planner POSTs are audited", acts == {"auto.viral.approve", "auto.viral.skip", "auto.viral.handpick"}, acts)
    sources.get_counts = real_gc0



async def planner_review_tests():
    """v2.8 review fixes 1-13 (one check each)."""
    import datetime as dt
    import apify_client
    import automation
    import main
    import planner
    import sources
    from fastapi import HTTPException
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots", "jobs"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%' OR k LIKE 'auto:max_%'")
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"

    def day(n):
        return (dt.date.fromisoformat(TODAY) + dt.timedelta(days=n)).isoformat()

    def camp(**kw):
        return db.insert("auto_campaigns", **{"name": "R", "mode": "live", "views_goal": 100000, "start_date": day(-5),
                                              "end_date": day(9), "creators_planned": 1, "posts_per_day": 1,
                                              "ramp_weeks": 3, "approved": 1, "created_at": NOW, **kw})
    n = [0]

    def post(cid, d, acct, observed=None, likes=0, up=None, checked=None, **kw):
        n[0] += 1
        url = f"https://www.instagram.com/reel/R{n[0]}/"
        f = {"post_key": f"instagram:R{n[0]}", "url_key": f"ig:R{n[0]}", "campaign_id": cid, "creator_id": acct,
             "account_handle": acct, "platform": "instagram", "url": url, "source": "sheet", "day": d,
             "uploaded_at": up if up is not None else automation.et_midnight(d) + 12 * 3600, "w_random": 1.0,
             "p_likes": 0.01, "ramp_start": d, "observed_views": observed, "base_views": observed, "base_day": d,
             "observed_likes": likes if observed is not None else None,
             "checked_at": checked if checked is not None else (NOW - 3600 if observed is not None else None),
             "created_at": automation.et_midnight(d), **kw}   # seen on its day: the whole curve (v2.10 share)
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (db.insert("auto_posts", **f),))

    def G(pid):
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (pid,))

    def row(cid, p):
        return db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (f"{cid}:{p['post_key']}:{TODAY}",))
    BASE = camp(name="base", approved=0)
    for a in ("a", "b", "c", "d", "e", "f"):
        for i in range(3):
            post(BASE, day(-10 - i), a, observed=1000)

    # 1. unreadable counts
    A = camp()
    old = post(A, day(-2), "a", observed=500, likes=20)
    young = post(A, TODAY, "b", up=NOW - 3600)
    r = [planner.apply_count(G(old["id"]), (0, 0), {}, NOW), planner.apply_count(G(young["id"]), (0, 0), {}, NOW)]
    check("1: 0 views on a post > 24 h old / with views before = unreadable; a young post may read 0",
          r == ["unreadable", "ok"] and G(old["id"])["observed_views"] == 500, r)
    planner.apply_count(G(old["id"]), (300, 25), {}, NOW)
    check("1: observed_views never decreases", G(old["id"])["observed_views"] == 500
          and G(old["id"])["observed_likes"] == 25)
    st = planner.apply_count(G(old["id"]), (900, 0), {}, NOW)
    check("1/6: 0 likes with views >= threshold = likes unreadable, kept as before",
          st == "ok_nolikes" and G(old["id"])["observed_likes"] == 25 and G(old["id"])["observed_views"] == 900, st)
    real_run, real_fb = apify_client._run_actor, apify_client.fb_play_count
    import config
    import datetime
    real_cget, real_cbase = clippers._get, config.CLIPPERS_API_BASE
    fresh = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)).replace(tzinfo=None).isoformat()
    old_run = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=30)).replace(tzinfo=None).isoformat()
    SUBS = {7: {"views": 4321, "likes": 55, "scrape_status": "success", "scrape_last_run": fresh},
            8: {"views": 999, "likes": 9, "scrape_status": "success", "scrape_last_run": old_run}}

    async def cget(path, params=None):
        if path.endswith("submissions-search"):
            return [{"id": 6, "post_url": "https://www.instagram.com/reel/OTHERPOST1/"},
                    {"id": 7, "post_url": "https://instagram.com/reel/CLIPFRESH1/?igsh=x"},
                    {"id": 8, "post_url": "https://www.instagram.com/reel/CLIPSTALE1/"}]
        return SUBS[int(path.rsplit("/", 1)[-1])]

    async def no_apify(actor, inp, polls=0):
        return []
    clippers._get, config.CLIPPERS_API_BASE, apify_client._run_actor = cget, "https://clip.test", no_apify
    c_fresh = await sources.get_counts("instagram", "https://www.instagram.com/reel/CLIPFRESH1/")
    c_stale = await sources.get_counts("instagram", "https://www.instagram.com/reel/CLIPSTALE1/")
    c_none = await sources.get_counts("instagram", "https://www.instagram.com/reel/NOTTHERE01/")
    clippers._get, config.CLIPPERS_API_BASE, apify_client._run_actor = real_cget, "", real_run
    check("counts: Clippers submission with a fresh scrape is used first; stale or no match falls back to Apify",
          c_fresh == (4321, 55) and c_stale is None and c_none is None, (c_fresh, c_stale, c_none))

    async def fb_item(actor, inp, polls=0):
        return [{"likes": 5, "video": {"id": "1"}}]

    async def fb_zero(item):
        return 0
    apify_client._run_actor, apify_client.fb_play_count = fb_item, fb_zero
    got = await sources.get_counts("facebook", "https://www.facebook.com/reel/123456789")

    async def boom(actor, inp, polls=0):
        raise RuntimeError("network")
    apify_client._run_actor = boom
    got2 = await sources.get_counts("instagram", "https://www.instagram.com/reel/X/")
    apify_client._run_actor, apify_client.fb_play_count = real_run, real_fb
    check("1/3: fb_play_count failure = unreadable (never 0); any get_counts exception = unreadable",
          got is None and got2 is None, (got, got2))
    config.CLIPPERS_API_BASE = real_cbase
    db.ex("UPDATE auto_posts SET misses=0, skip_next=0 WHERE id=?", (old["id"],))
    nl = post(A, day(-1), "c", observed=900, likes=1)
    planner.plan(automation._camp(A), NOW, nolike_ids=frozenset({nl["id"]}))
    check("6: likes unreadable this run -> likes_add 0", row(A, nl)["likes_add"] == 0 and row(A, nl)["views_add"] > 0)

    # 3. gather(return_exceptions=True)
    async def raising(platform, url):
        raise RuntimeError("x")
    real_gc = sources.get_counts
    sources.get_counts = raising
    res = await planner.check_counts([G(nl["id"])], {}, NOW)
    check("3: an exception in one count check = unreadable, run goes on", res == {nl["id"]: "unreadable"}, res)

    # 2. scrape caps, priority, backoff, early rows, dashboard
    db.ex("DELETE FROM plan_rows")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:scrapes:%'")
    C = camp(name="cap")
    checked_old = post(C, day(-1), "d", observed=100, checked=NOW - 9 * 3600)
    checked_new = post(C, day(-1), "e", observed=100, checked=NOW - 3600)
    viral = post(C, day(-1), "f", observed=100, checked=NOW - 1800, reason="viral", peak=50000, viral_state="approved")
    fresh = post(C, TODAY, "d", up=NOW - 600)
    seen = []

    async def counting(platform, url):
        seen.append(url)
        if len(seen) == 1:   # rows of posts already checked are out before the first scrape
            seen.append(("rows_at_first_scrape", db.q1("SELECT COUNT(*) n FROM plan_rows WHERE campaign_id=?", (C,))["n"]))
        return (200, 3)
    sources.get_counts = counting
    planner.set_limits({"max_scrapes_per_run": 3})
    rr = await planner.run_campaign(C, NOW)
    urls = [u for u in seen if isinstance(u, str)]
    check("2: per-run cap; order = never checked, viral, oldest check",
          urls == [fresh["url"], viral["url"], checked_old["url"]] and rr["capped"] == 1 and rr["checked"] == 3, (urls, rr))
    check("2: rows for already-checked posts written before the sweep; unscraped checked post still gets a row",
          ("rows_at_first_scrape", 1) in seen and row(C, checked_new) is not None, seen)
    dash = planner.dashboard(C, NOW)
    check("2: dashboard shows scrapes used today", dash["scrapes"] == {"today": 3, "max_per_run": 3, "max_per_day": 1000},
          dash["scrapes"])
    planner.set_limits({"max_scrapes_per_run": 300, "max_scrapes_per_day": 4})
    seen.clear()
    await planner.run_campaign(C, NOW + 60)
    check("2: per-day cap", len([u for u in seen if isinstance(u, str)]) == 1 and planner.scrapes_today(NOW) == 4, seen)
    planner.set_limits({"max_scrapes_per_day": 1000})
    try:
        await main.auto_planner_settings({"max_scrapes_per_run": -1}, who="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 400
    check("2: planner settings validated", ok)
    sources.get_counts = raising
    db.ex("UPDATE auto_posts SET misses=0, skip_next=0 WHERE campaign_id=?", (C,))
    target = G(checked_new["id"])
    for k in range(3):
        db.ex("UPDATE auto_posts SET skip_next=0 WHERE id=?", (target["id"],))
        await planner.check_counts([G(target["id"])], {}, NOW)
    sel, held = planner.pick_scrapes(automation._camp(C), NOW)
    check("2: backoff: unreadable sits out next run; 3 misses = not checked again",
          G(target["id"])["misses"] == 3 and target["id"] in held and target["id"] not in {p["id"] for p in sel}, held)
    sources.get_counts = real_gc

    # 5. no back-fill (v2.10): the ramp starts at upload; a post first seen later plans only days k..D
    links = [{"url": "https://www.instagram.com/reel/LATE1/", "platform": "instagram", "creator_id": "a",
              "account_handle": "a", "uploaded_at": NOW - 3 * 86400, "day": day(-3), "slot": "IG AM 1",
              "source": "sheet", "post_key": "instagram:LATE1"}]
    planner.ingest(automation._camp(A), links, [], {}, NOW)
    late = db.q1("SELECT * FROM auto_posts WHERE post_key='instagram:LATE1'")
    cA = automation._camp(A)
    check("5 (v2.10): post first seen 3 days after upload: ramp_start = upload day, no base until its first read",
          late["ramp_start"] == day(-3) and late["day"] == day(-3) and late["base_views"] is None
          and planner.in_ramp(late, cA, TODAY), dict(late))

    # 7. check_times validated on save
    for bad in ({"check_times": ["25:00"]}, {"check_times": "13:00"}, {"slot_deadlines": {"AM": "9am"}}):
        try:
            await main.auto_campaign_add({"name": "x", "weekly_budget_usd": 1, **bad}, who="t")
            ok = False
        except HTTPException as e:
            ok = e.status_code == 400 and "HH:MM" in e.detail
        check(f"7: bad {list(bad)[0]} rejected on save", ok)
    good = await main.auto_campaign_add({"name": "ok", "weekly_budget_usd": 1, "check_times": ["09:30", "21:00"]}, who="t")
    check("7: good check_times saved", good["check_times"] == ["09:30", "21:00"])

    # 8. month-first dates
    t = dt.date(2026, 10, 24)
    check("8: month-first dates", sources.parse_day("Oct 19", t) == sources.parse_day("October 19", t)
          == sources.parse_day("Mon Oct 19", t) == sources.parse_day("Mon, 19 Oct", t) == dt.date(2026, 10, 19))

    # 9. resolve only platform hosts; failures cached
    fol = []

    async def fail_follow(u):
        fol.append(u)
        raise RuntimeError("blocked")
    sources._follow = fail_follow
    a1 = await sources.resolve("facebook", "https://evil.example.com/share/r/X/")
    a2 = await sources.resolve("tiktok", "https://vm.tiktok.com/ZZZ/")
    a3 = await sources.resolve("tiktok", "https://vm.tiktok.com/ZZZ/")
    check("9: non-platform host never followed; a failed resolve is cached", a1 is None and a2 is None and a3 is None
          and fol == ["https://vm.tiktok.com/ZZZ/"], fol)

    # 10. carry only last CARRY_DAYS; failed order awaiting retry = in flight
    cp = post(A, day(-6), "a", observed=10)
    for d, c in ((day(-1), 70), (day(-5), 999)):
        automation.insert_plan([{"plan_id": f"{A}:{cp['post_key']}:{d}", "campaign_id": A, "post_url": cp["url"],
                                 "platform": "instagram", "window_start": automation.et_midnight(d),
                                 "window_end": automation.et_midnight(d), "views_add": c}])
        db.ex("UPDATE plan_rows SET carry=? WHERE plan_id=?", (c, f"{A}:{cp['post_key']}:{d}"))
    db.insert("auto_orders", plan_id="x", campaign_id=A, url_key=cp["url_key"], day=day(-1), kind="views", qty=400,
              state="failed", attempt=0, created_at=NOW - 86400)
    db.insert("auto_orders", plan_id="y", campaign_id=A, url_key=cp["url_key"], day=day(-4), kind="views", qty=800,
              state="failed", attempt=0, created_at=NOW - 4 * 86400)
    fx = planner.order_facts(TODAY, NOW)[cp["url_key"]]
    check("10: carry only from the last 3 days; failed attempt-0 order < 2 days = in flight",
          fx["carry"] == {A: 70} and fx["inflight"] == 400, fx)

    # 11. cancelled cell values that mean no; peak bounds
    sh = sources.parse_sheet('"Date (ET)","Creator","IG AM 1","Cancelled / account issues"\n'
                             '"19 Oct","p","","No"\n"19 Oct","q","","-"\n"19 Oct","r","","N/A"\n"19 Oct","s","","none"\n'
                             '"19 Oct","t","","account banned"\n', t)
    check("11: cancelled cell 'no' / '-' / 'n/a' / 'none' ignored",
          sh["cancelled"] == {"p": False, "q": False, "r": False, "s": False, "t": True}, sh["cancelled"])
    pk = post(A, day(-1), "a", observed=10, viral_state="picked")
    for bad in (float("inf"), 10_000_001):
        try:
            await main.auto_viral_approve(pk["id"], main.ViralBody(peak=bad), who="t")
            ok = False
        except HTTPException as e:
            ok = e.status_code == 400
        check(f"11: peak {bad} rejected", ok)

    # 12. run route needs AUTO_ENABLED=1
    os.environ["AUTO_ENABLED"] = "0"
    try:
        await main.auto_planner_run(A, who="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 400 and e.detail == "automation is off"
    check("12: POST run with automation off -> 400", ok)

    # 13. simulated counts as delivered in practice only
    db.ex("DELETE FROM plan_rows")
    Pc = camp(name="practice", mode="practice")
    sp = post(Pc, day(-1), "a", observed=100)
    db.insert("auto_orders", plan_id="s", campaign_id=Pc, url_key=sp["url_key"], day=day(-1), kind="views", qty=300,
              state="simulated", attempt=0, created_at=NOW - 86400)
    planner.plan(automation._camp(Pc), NOW)
    v_practice = row(Pc, sp)["views_add"]
    db.ex("DELETE FROM plan_rows")
    db.ex("UPDATE auto_campaigns SET mode='live' WHERE id=?", (Pc,))
    planner.plan(automation._camp(Pc), NOW)
    v_live = row(Pc, sp)["views_add"]
    check("13: practice -> live: simulated qty no longer counts as delivered / in flight",
          v_live == v_practice + 300 and planner.natural(G(sp["id"]), planner.order_facts(TODAY, NOW)) == 100,
          (v_practice, v_live))

    # 7. a campaign with bad stored check_times does not stop the tick
    db.ex("UPDATE auto_campaigns SET approved=0")
    bad_c = camp(name="bad", check_times='["xx"]')
    ok_c = camp(name="good", start_date=automation.et_day(), end_date=automation.et_day())
    os.environ["AUTO_ENABLED"] = "1"
    sources.get_counts = raising   # never a real scrape from the tick (it runs on the real clock)
    await planner.tick()
    sources.get_counts = real_gc
    os.environ["AUTO_ENABLED"] = "0"
    check("7: bad stored check_times skipped, other campaigns still run",
          planner.last_run(bad_c) is None and planner.last_run(ok_c) is not None)


async def practice_fix_tests():
    """v2.8.2 Practice-run fixes: scrape order, sheet Views tab, late t24, X-LEC-User, one shortfall alert, reset."""
    import datetime as dt
    import automation
    import main
    import planner
    import sources
    import users
    from fastapi import HTTPException
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots", "jobs"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%' OR k LIKE 'auto:max_%'")
    automation._alerts.clear()
    planner._EXT.clear()
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"

    def day(n):
        return (dt.date.fromisoformat(TODAY) + dt.timedelta(days=n)).isoformat()

    def camp(**kw):
        return db.insert("auto_campaigns", **{"name": "X", "mode": "practice", "views_goal": 100000, "start_date": day(-5),
                                              "end_date": day(9), "creators_planned": 1, "posts_per_day": 1,
                                              "ramp_weeks": 3, "approved": 1, "created_at": NOW, **kw})
    n = [0]

    def post(cid, acct, up, observed=None, checked=None, **kw):
        n[0] += 1
        f = {"post_key": f"instagram:Q{n[0]}", "url_key": f"ig:Q{n[0]}", "campaign_id": cid, "creator_id": acct,
             "account_handle": acct, "platform": "instagram", "url": f"https://www.instagram.com/reel/Q{n[0]}/",
             "source": "sheet", "day": automation.et_day(up), "uploaded_at": up, "w_random": 1.0, "p_likes": 0.01,
             "ramp_start": automation.et_day(up), "observed_views": observed, "base_views": observed,
             "base_day": automation.et_day(up),
             "observed_likes": 0 if observed is not None else None, "checked_at": checked, "created_at": up, **kw}
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (db.insert("auto_posts", **f),))

    def G(pid):
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (pid,))

    # 1. scrape order: never checked newest upload first, then viral, then oldest check
    A = camp(name="order")
    old_new = post(A, "a", NOW - 4 * 86400)
    new_new = post(A, "a", NOW - 3600)
    mid_new = post(A, "a", NOW - 86400)
    chk = post(A, "a", NOW - 5 * 86400, observed=10, checked=NOW - 100)
    vir = post(A, "a", NOW - 5 * 86400, observed=10, checked=NOW - 50, reason="viral")
    got = [p["id"] for p in sorted([G(x["id"]) for x in (chk, old_new, vir, mid_new, new_new)], key=planner.scrape_order)]
    check("v2.8.2: scrape order = never checked newest first, viral, oldest check",
          got == [new_new["id"], mid_new["id"], old_new["id"], vir["id"], chk["id"]], got)

    # 2. Views tab parse: Dutch numbers, Done filter, likes
    views_csv = ('"fv","Day #","Creator","Platform","Slot","Post link","Views to buy","Status","Likes to buy","Status",'
                 '"Shares to buy","Status","Comments to buy","Status","Total Views Update Daily"\n'
                 '"ma 19 okt","1","jae","FB","AM 1","https://www.instagram.com/reel/Q1/","1.547","Done","300","Done",'
                 '"","","","","9.999"\n'
                 '"di 20 okt","2","jae","FB","AM 1","https://www.instagram.com/reel/Q1/","1,200","Done","50","Pending",'
                 '"","","","",""\n'
                 '"di 20 okt","2","bo","IG","AM 1","https://www.instagram.com/reel/Q2/","5000","In progress","","",'
                 '"","","","",""\n'
                 '"","","","","","","","","","","","","","",""\n')
    pv = sources.parse_views(views_csv)
    check("v2.8.2: Views tab parse ('1.547' = 1547, '1,200' = 1200, only Done counts)",
          pv == [{"url": "https://www.instagram.com/reel/Q1/", "views": 1547, "likes": 300},
                 {"url": "https://www.instagram.com/reel/Q1/", "views": 1200, "likes": 0}], pv)
    try:
        sources.parse_views('"a","b"\n"1","2"\n')
        ok = False
    except sources.SourceError:
        ok = True
    check("v2.8.2: Views tab without its header = SourceError", ok)

    # 3. bought views from the sheet change natural views and t24; a failed read alerts once, keeps the last good read
    S = camp(name="sheetx", source="sheet", source_ref="sid1:Daily Posts")
    sp = post(S, "jae", NOW - 30 * 3600, observed=5000, checked=NOW - 20 * 3600)
    fetched = []

    async def f_views(sid):
        fetched.append(sid)
        return views_csv.replace("/Q1/", f"/{sp['post_key'].split(':')[1]}/")   # the Done rows = this post
    sources_fetch_views = sources.fetch_views
    sources.fetch_views = f_views
    await planner.load_external(automation._camp(S))
    fx = planner.order_facts(TODAY, NOW)
    check("v2.8.2: sheet bought views subtracted from natural views",
          fetched == ["sid1"] and fx[sp["url_key"]]["ext"] == 2747 and fx[sp["url_key"]]["ext_likes"] == 300
          and planner.natural(G(sp["id"]), fx) == 5000 - 2747, (fetched, fx.get(sp["url_key"])))
    planner.apply_count(G(sp["id"]), (6000, 50), fx, NOW)
    check("v2.8.2: t24 uses natural views net of sheet buys", G(sp["id"])["t24"] == 6000 - 2747, G(sp["id"]))

    async def f_broken(sid):
        raise sources.SourceError("HTTP 500")
    sources.fetch_views = f_broken
    automation._alerts.clear()
    await planner.load_external(automation._camp(S))
    await planner.load_external(automation._camp(S))
    al = [t for t in automation._alerts if '"Views" tab' in t]
    check("v2.8.2: Views tab read failure = one alert, last good read kept",
          len(al) == 1 and planner.order_facts(TODAY, NOW)[sp["url_key"]]["ext"] == 2747, (al, planner._EXT))
    sources.fetch_views = f_views
    planner._EXT.clear()

    # 4. late first check: > 36 h after upload -> t24 stays NULL
    late = post(A, "a", NOW - 40 * 3600)
    planner.apply_count(G(late["id"]), (900, 9), {}, NOW)
    ontime = post(A, "a", NOW - 30 * 3600)
    planner.apply_count(G(ontime["id"]), (900, 9), {}, NOW)
    check("v2.8.2: first check > 36 h after upload -> t24 NULL; 24-36 h -> set",
          G(late["id"])["t24"] is None and G(late["id"])["observed_views"] == 900 and G(ontime["id"])["t24"] == 900)

    # 5. X-LEC-User only with the access code, sanitized
    users._ensure()
    db.ex("DELETE FROM users WHERE username='lisa'")
    db.insert("users", username="lisa", pw_hash=users._hash("pw"), active=1, created_at=NOW)
    tok = users.issue_token(users._get("lisa"))
    check("v2.8.2: X-LEC-User names an access-code caller (sanitized, max 40); user logins keep their name",
          main.require_auth("Bearer test-code", "  Rhys M<script>!  ") == "RhysMscript"
          and main.require_auth("Bearer test-code", "bill@lumina.co") == "bill@lumina.co"
          and main.require_auth("Bearer test-code", "x" * 50) == "x" * 40
          and main.require_auth("Bearer test-code", " !! ") == "operator"
          and main.require_auth("Bearer test-code", "") == "operator"
          and main.require_auth(f"Bearer {tok}", "mallory") == "lisa")
    try:
        await main.add_user(main.NewUserBody(username="zz"), who="rhys", authorization=f"Bearer {tok}")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 403
    check("v2.8.2: add user still needs the access code itself (a user login is refused)", ok)

    # 6. shortfall alert once per run, final number
    automation._alerts.clear()
    SF = camp(name="shorty", views_goal=50_000_000)
    for i in range(3):
        post(SF, "s", NOW - (5 + i) * 86400, observed=100, checked=NOW - 3600)
    for i in range(4):
        post(SF, "s", NOW - (i + 1) * 3600)
    real_gc, real_batch = sources.get_counts, planner.BATCH

    seen_sf, real_plan = [], planner.plan

    def spy_plan(*a, **k):
        r = real_plan(*a, **k)
        seen_sf.append(r["shortfall"])
        return r

    async def f_counts(platform, url):   # counts above the 5x cap: each batch lowers the shortfall
        return (600 + 1000 * len(seen_sf), 1)
    sources.get_counts, planner.BATCH, planner.plan = f_counts, 1, spy_plan
    rs = await planner.run_campaign(SF, NOW)
    sources.get_counts, planner.BATCH, planner.plan = real_gc, real_batch, real_plan
    al = [t for t in automation._alerts if "shorty" in t and "Goal not reachable" in t]
    check("v2.8.2: shortfall alert once per run (batches differ), with the final number",
          len(set(seen_sf)) > 1 and rs["shortfall"] > 0 and len(al) == 1
          and f"short {rs['shortfall']:,} views" in al[0], (seen_sf, rs, al))

    # 7. reset: practice only, not while running, refused when an order reached a panel
    R = camp(name="reset")
    rp = post(R, "r", NOW - 3600, observed=10, checked=NOW)
    db.insert("plan_rows", plan_id=f"{R}:{rp['post_key']}:{TODAY}", campaign_id=R, creator_id="r", post_url=rp["url"],
              url_key=rp["url_key"], platform="instagram", window_start=NOW, window_end=NOW, day=TODAY, views_add=10, likes_add=0,
              target_total_views=100, reason="base", priority=3, created_at=NOW)
    for st in ("simulated", "blocked"):
        db.insert("auto_orders", plan_id=f"{R}:{rp['post_key']}:{TODAY}", campaign_id=R, url_key=rp["url_key"],
                  day=TODAY, kind="views", qty=10, state=st, attempt=0, created_at=NOW)
    db.ex("INSERT INTO auto_creators (campaign_id, creator_id, cancelled) VALUES (?, 'r', 0)", (R,))
    planner._save_run(R, {"at": NOW})
    planner._running.add(R)
    try:
        await main.auto_planner_reset(R, who="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 409
    planner._running.discard(R)
    check("v2.8.2: reset refused (409) while the planner runs", ok)
    out = await main.auto_planner_reset(R, who="t")
    check("v2.8.2: practice reset deletes posts, rows, simulated/blocked orders, creators, run state; keeps campaign",
          out == {"deleted": {"orders": 2, "plan_rows": 1, "posts": 1}}
          and not db.q1("SELECT 1 FROM auto_creators WHERE campaign_id=?", (R,)) and planner.last_run(R) is None
          and automation._camp(R) is not None
          and db.q1("SELECT username FROM audit_log WHERE action='auto.planner.reset' ORDER BY id DESC")["username"] == "t",
          out)
    L = camp(name="live", mode="live")
    try:
        await main.auto_planner_reset(L, who="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 400 and e.detail == "only practice campaigns can be reset"
    check("v2.8.2: live campaign reset -> 400", ok)
    R2 = camp(name="real")
    rp2 = post(R2, "r", NOW - 3600, observed=10, checked=NOW)
    db.insert("auto_orders", plan_id=f"{R2}:{rp2['post_key']}:{TODAY}", campaign_id=R2, url_key=rp2["url_key"],
              day=TODAY, kind="views", qty=10, state="placed", attempt=0, created_at=NOW)
    try:
        await main.auto_planner_reset(R2, who="t")
        ok = False
    except HTTPException as e:
        ok = e.status_code == 400
    check("v2.8.2: reset refused when an order reached a panel, nothing deleted",
          ok and G(rp2["id"]) is not None and R2 not in planner._running)
    sources.fetch_views = sources_fetch_views


asyncio.run(planner_tests())
asyncio.run(planner_review_tests())
async def practice_fix2_tests():
    """v2.8.3: first run gets rows (24 h campaign fallback, 20% reserve), same-day row replace unless ordered."""
    import datetime as dt
    import automation
    import planner
    import sources
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots", "jobs"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%' OR k LIKE 'auto:max_%'")
    planner._EXT.clear()
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"

    def camp(**kw):
        return db.insert("auto_campaigns", **{"name": "F", "mode": "practice", "views_goal": 1_000_000,
                                              "start_date": "2026-10-15", "end_date": "2026-10-29",
                                              "creators_planned": 5, "posts_per_day": 2, "ramp_weeks": 3,
                                              "approved": 1, "created_at": NOW, **kw})
    n = [0]

    def post(cid, acct, up, observed=None, checked=None, **kw):
        n[0] += 1
        f = {"post_key": f"instagram:F{n[0]}", "url_key": f"ig:F{n[0]}", "campaign_id": cid, "creator_id": acct,
             "account_handle": acct, "platform": "instagram", "url": f"https://www.instagram.com/reel/F{n[0]}/",
             "source": "sheet", "day": automation.et_day(up), "uploaded_at": up, "w_random": 1.0, "p_likes": 0.01,
             "ramp_start": automation.et_day(up), "observed_views": observed, "base_views": observed,
             "base_day": automation.et_day(up),
             "observed_likes": 0 if observed is not None else None, "checked_at": checked, "created_at": up, **kw}
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (db.insert("auto_posts", **f),))

    def G(pid):
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (pid,))

    # 12a. fresh campaign, every post < 3 days old and never checked: run 1 writes rows (campaign fallback >= 24 h)
    F = camp()
    fresh = [post(F, f"fresh{i}", NOW - (26 + 6 * i) * 3600) for i in range(6)] + [post(F, "fresh9", NOW - 3600)]
    real_gc = sources.get_counts

    async def f_counts(platform, url):
        return (2000, 20)
    sources.get_counts = f_counts
    r = await planner.run_campaign(F, NOW)
    sources.get_counts = real_gc
    nrows = db.q1("SELECT COUNT(*) n FROM plan_rows WHERE campaign_id=?", (F,))["n"]
    check("v2.8.3: fresh campaign gets rows on run 1 (campaign fallback = checked posts >= 24 h old)",
          r["checked"] == 7 and nrows == 7 and r["held_no_baseline"] == 0, (r, nrows))
    check("v2.8.3: account normal still needs posts >= 3 days old (campaign fallback used)",
          planner.normal_views(G(fresh[0]["id"]), {}, NOW) == 2000
          and planner.normal_views(G(fresh[0]["id"]), {}, NOW - 30 * 86400) is None)

    # 12b. 20% of the budget for never-checked posts >= 3 days old, newest of those first
    R = camp(name="reserve")
    new_ = [post(R, "n", NOW - i * 3600) for i in range(1, 11)]
    old_ = [post(R, "o", NOW - d * 86400) for d in (5, 4, 6)]
    sel, _ = planner.pick_scrapes(automation._camp(R), NOW)
    b5 = [p["id"] for p in planner.budgeted(sel, 5, NOW)]
    b10 = [p["id"] for p in planner.budgeted(sel, 10, NOW)]
    b30 = [p["id"] for p in planner.budgeted(sel, 30, NOW)]
    check("v2.8.3: 20% reserve picks old never-checked posts first (newest of them), rest in scrape order",
          b5 == [old_[1]["id"]] + [p["id"] for p in new_[:4]]
          and b10 == [old_[1]["id"], old_[0]["id"]] + [p["id"] for p in new_[:8]]
          and len(b30) == 13 and len(set(b30)) == 13, (b5, b10))

    # 13. same-day replace: a row without an order follows new numbers; a row with an order never changes
    db.ex("DELETE FROM plan_rows")
    planner.plan(automation._camp(F), NOW)
    a, b, c = (f"{F}:{p['post_key']}:{TODAY}" for p in fresh[:3])
    v1 = {x: db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (x,)) for x in (a, b, c)}
    db.insert("auto_orders", plan_id=b, campaign_id=F, url_key=fresh[1]["url_key"], day=TODAY, kind="views", qty=10,
              state="blocked", attempt=0, created_at=NOW)
    db.ex("UPDATE plan_rows SET carry=views_add WHERE plan_id=?", (c,))   # below-minimum mark, no order
    db.ex("UPDATE auto_posts SET observed_views=500 WHERE campaign_id=?", (F,))   # targets move
    s2 = planner.plan(automation._camp(F), NOW + 3600)
    v2 = {x: db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (x,)) for x in (a, b, c)}
    check("v2.8.3: same-day replace updates a row with no order (carry mark follows), never one with an order",
          v2[a]["views_add"] != v1[a]["views_add"] and v2[a]["id"] == v1[a]["id"]
          and v2[b] == v1[b] and v2[c]["carry"] == v2[c]["views_add"] != v1[c]["views_add"] and s2["replaced"] >= 2
          and s2["rows"] == 0 and db.q1("SELECT COUNT(*) - COUNT(DISTINCT plan_id) x FROM plan_rows")["x"] == 0,
          (v1, v2, s2))
    db.ex("UPDATE plan_rows SET carry=0 WHERE plan_id=?", (c,))   # dropped by a pause
    db.ex("UPDATE auto_posts SET observed_views=100 WHERE campaign_id=?", (F,))
    planner.plan(automation._camp(F), NOW + 7200)
    check("v2.8.3: carry 0 (pause) stays 0 on replace",
          db.q1("SELECT carry FROM plan_rows WHERE plan_id=?", (c,))["carry"] == 0)


asyncio.run(practice_fix_tests())
async def sideshift_tests():
    """v2.9 SideShift source: paging, mapping, deleted, free counts (no cap, no Apify), creator ids, sweep failure."""
    import datetime as dt
    import automation
    import planner
    import sources
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots", "jobs",
              "auto_creator_map"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%' OR k LIKE 'auto:max_%'")
    automation._alerts.clear()
    planner._EXT.clear()
    sources._ss_cache.clear()
    os.environ["SIDESHIFT_PLATFORM_KEY"] = "test-ss"
    sources.SS_GAP = 0
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"
    H = 3600

    def item(pid, url, plat, cid, handle, name, up, views, likes=10, deleted=None):
        return {"postId": pid, "postPage": url, "platform": plat, "creator": handle, "contractorName": name,
                "contractorId": cid, "views": views, "likes": likes, "uploadedAt": up, "deletionStatus": deleted}
    ITEMS = [   # program P1
        item("A1", "https://instagram.com/reel/SSA1/", "Instagram", "cid_sam", "climbwithdavis", "Sam Davis", NOW - 5 * 86400, 4000),
        item("A2", "https://www.instagram.com/reel/SSA2/", "instagram", "cid_sam", "climbwithdavis", "Sam Davis", NOW - 30 * H, 900),
        item("A3", "https://www.instagram.com/reel/SSA3/", "ig", "cid_bo", "bo_clips", "", NOW - 40 * H, 700),
        item("777001", "https://www.facebook.com/reel/777001", "fb", "cid_bo", "Bo Page", "Robert B", NOW - 50 * H, 500),
        item("A5", "https://www.instagram.com/reel/SSA5/", "instagram", "cid_sam", "climbwithdavis", "Sam Davis", NOW - 60 * H, 300,
             deleted="deleted"),
        item("A6", "https://www.instagram.com/reel/SSA6/", "instagram", "cid_sam", "climbwithdavis", "Sam Davis", NOW - 4 * 86400, 0),
    ]
    HIST = {"A2": [{"timestamp": NOW - 30 * H + 25 * H, "views": 800, "likes": 9},     # 25 h after upload -> t24
                   {"timestamp": NOW - 2 * H, "views": 950, "likes": 12}],            # fresh -> checked_at
            "A3": [{"timestamp": NOW - 21 * H, "views": 690, "likes": 5}],            # stale (> 20 h) -> paid path
            "777001": [{"timestamp": NOW - H, "views": 520, "likes": 0}]}
    reqs, mode = [], {"fail": False}

    def h(req):
        reqs.append(req)
        if mode["fail"]:
            return httpx.Response(500, json={"error": "down"})
        if "/metrics-history" in req.url.path:
            pid = req.url.path.split("/posts/")[1].split("/")[0]
            return httpx.Response(200, json={"data": {"postId": pid, "history": HIST.get(pid, [])}})
        page, lim = int(req.url.params["page"]), int(req.url.params["limit"])
        per = 2
        return httpx.Response(200, json={"data": {"data": ITEMS[(page - 1) * per:page * per], "page": page, "limit": lim,
                                                  "total": len(ITEMS), "totalPages": 3}})
    prev_client = httpx.AsyncClient
    httpx.AsyncClient = lambda **kw: _RealClient(transport=httpx.MockTransport(h), **kw)
    try:
        # paging + field mapping
        links = await sources.fetch_sideshift("P1")
        pages = [r.url.params.get("page") for r in reqs]
        check("v2.9: sweep reads every page (GET only, x-api-key, limit 100, programId)",
              pages == ["1", "2", "3"] and all(r.method == "GET" and r.headers.get("x-api-key") == "test-ss"
                                               and r.url.params["limit"] == "100" and r.url.params["programId"] == "P1"
                                               for r in reqs), pages)
        await sources.fetch_sideshift("P1")
        check("v2.9: second read in the same run comes from the cache", len(reqs) == 3, len(reqs))
        l0, l3 = links[0], links[3]
        check("v2.9: field mapping (url, platform normalized, contractorId, handle, upload time, ET day, no slot)",
              len(links) == 6 and l0["url"] == ITEMS[0]["postPage"] and l0["platform"] == "instagram"
              and l0["creator_id"] == "cid_sam" and l0["account_handle"] == "climbwithdavis"
              and l0["uploaded_at"] == NOW - 5 * 86400 and l0["day"] == "2026-10-15" and l0["slot"] is None
              and l0["source"] == "sideshift" and l3["platform"] == "facebook" and links[2]["platform"] == "instagram", links)

        # sheet + sideshift campaign: dedupe, creator ids, counts
        d1 = "Mon 19 Oct"
        sheet = ('"Date (ET)","Creator","IG AM 1","IG AM 2","FB AM 1","Cancelled / account issues"\n'
                 f'"{d1}","Sam Davis","https://www.instagram.com/reel/SSA2/?igsh=1","",""," "\n'
                 f'"{d1}","Bo","","https://www.instagram.com/reel/SSA3/","",""\n'
                 f'"{d1}","Zed","https://www.instagram.com/reel/ZED1/","","",""\n')

        async def f_sheet(ref):
            return sheet
        real_sheet, real_views = sources.fetch_sheet, sources.fetch_views
        sources.fetch_sheet = f_sheet

        async def f_views(sid):
            raise sources.SourceError("no Views tab")
        sources.fetch_views = f_views
        C = db.insert("auto_campaigns", name="SS", mode="practice", views_goal=1_000_000, start_date="2026-10-10",
                      end_date="2026-10-30", creators_planned=3, posts_per_day=2, ramp_weeks=3, approved=1,
                      created_at=NOW, source="sheet",
                      source_ref=json.dumps([{"source": "sheet", "ref": "sid:Daily Posts"}, {"source": "sideshift", "ref": "P1"}]))
        old = db.insert("auto_posts", post_key="instagram:SSA1", url_key="ig:SSA1", campaign_id=C, creator_id="Sam Davis",
                        account_handle="Sam Davis", platform="instagram", url=ITEMS[0]["postPage"], source="sheet",
                        slot="IG AM 1", day="2026-10-15", uploaded_at=NOW - 5 * 86400, w_random=1.0, p_likes=0.01,
                        ramp_start="2026-10-15", created_at=NOW)
        db.insert("pauses", scope="creator", key="Sam Davis", label="Sam Davis", created_by="t", created_at=NOW)
        paid = []

        async def f_counts(platform, url):
            paid.append(sources.post_key(platform, url))
            return (1234, 11)
        real_gc = sources.get_counts
        sources.get_counts = f_counts
        before = planner.scrapes_today(NOW)
        r = await planner.run_campaign(C, NOW)
        posts = {p["post_key"]: p for p in db.q("SELECT * FROM auto_posts WHERE campaign_id=?", (C,))}
        a2, a3 = posts["instagram:SSA2"], posts["instagram:SSA3"]
        at = sources._ss_cache["P1"][0]
        check("v2.9: sheet + sideshift dedupe by post_key: one row, sheet slot kept, SideShift id/handle/upload time",
              len([k for k in posts if k == "instagram:SSA2"]) == 1 and a2["slot"] == "IG AM 1" and a2["source"] == "sheet"
              and a2["creator_id"] == "cid_sam" and a2["account_handle"] == "climbwithdavis"
              and a2["uploaded_at"] == NOW - 30 * H, a2)
        cmap = {m["name"]: m["creator_id"] for m in db.q("SELECT * FROM auto_creator_map WHERE campaign_id=?", (C,))}
        check("v2.9: sheet names map to contractorId by contractorName, else by a shared post (no contractorName); unmatched kept",
              cmap == {"Sam Davis": "cid_sam", "Bo": "cid_bo"} and a3["creator_id"] == "cid_bo"
              and posts["instagram:ZED1"]["creator_id"] == "Zed" and G_(old)["creator_id"] == "cid_sam", cmap)
        slots = {x["creator_id"] for x in db.q("SELECT creator_id FROM auto_slots WHERE campaign_id=?", (C,))}
        canc = {x["creator_id"] for x in db.q("SELECT creator_id FROM auto_creators WHERE campaign_id=?", (C,))}
        dash = planner.dashboard(C, NOW)
        check("v2.9: slots / cancelled keyed by the canonical id; dashboard lists unmatched_creators",
              slots == {"cid_sam", "cid_bo", "Zed"} and canc == {"cid_sam", "cid_bo", "Zed"}
              and dash["unmatched_creators"] == ["Zed"], (slots, canc, dash["unmatched_creators"]))
        check("v2.9: a creator pause keyed by the sheet name also pauses the mapped SideShift id",
              automation._paused(automation._pause_set(), {"campaign_id": C, "url_key": "ig:x", "creator_id": "cid_sam"}))
        check("v2.9: SideShift deletionStatus -> post deleted", posts["instagram:SSA5"]["status"] == "deleted")
        a1 = posts["instagram:SSA1"]
        check("v2.9.2: older post in ramp: sweep counts; no snapshot in history -> checked_at = sweep time - 24 h",
              a1["observed_views"] == 4000 and a1["checked_at"] == at - 86400
              and any("/posts/A1/" in r_.url.path for r_ in reqs), a1)
        check("v2.9: post in first 3 days: fresh history snapshot = counts + checked_at, t24 from the ~24 h snapshot",
              a2["observed_views"] == 950 and a2["checked_at"] == NOW - 2 * H and a2["t24"] == 800, a2)
        check("v2.9: stale snapshot (> 20 h) or not in SideShift -> paid path (counts toward caps); others never paid",
              sorted(paid) == ["instagram:SSA3", "instagram:SSA6", "instagram:ZED1"] and planner.scrapes_today(NOW) - before == 3
              and a3["observed_views"] == 1234 and posts["facebook:777001"]["observed_views"] == 520, (paid, r))
        check("v2.9.1: SideShift views 0 on a post > 24 h old = paid path (no miss burnt on a SideShift gap)",
              posts["instagram:SSA6"]["observed_views"] == 1234 and posts["instagram:SSA6"]["misses"] == 0,
              posts["instagram:SSA6"])
        check("v2.9: run summary", r["sideshift_checked"] == 3 and not [e for e in r["errors"] if "sideshift" in e], r)

        # sweep failure: alert once, posts fall back to the paid path, run goes on
        sources._ss_cache.clear()
        mode["fail"] = True
        paid.clear()
        db.ex("UPDATE auto_posts SET skip_next=0, misses=0, checked_at=NULL WHERE campaign_id=?", (C,))
        automation._alerts.clear()
        r2 = await planner.run_campaign(C, NOW + 60)
        r3 = await planner.run_campaign(C, NOW + 120)
        al = [t for t in automation._alerts if "SideShift sweep failed" in t]
        check("v2.9: sweep failure = one alert, paid path for every post, run not crashed",
              len(al) == 1 and "instagram:SSA1" in paid and r2["sideshift_checked"] == 0
              and not [e for e in r2["errors"] if "Error:" in e] and "at" in r3, (al, paid, r2))
        sources.get_counts, sources.fetch_sheet, sources.fetch_views = real_gc, real_sheet, real_views
    finally:
        httpx.AsyncClient = prev_client


def G_(pid):
    return db.q1("SELECT * FROM auto_posts WHERE id=?", (pid,))


asyncio.run(practice_fix2_tests())
async def account_key_tests():
    """v2.9.1: account = (creator_id, platform); mixed handles under one creator act as one account."""
    import datetime as dt
    import automation
    import planner
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots", "jobs",
              "auto_creator_map"):
        db.ex(f"DELETE FROM {t}")
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"
    C = db.insert("auto_campaigns", name="K", mode="practice", views_goal=100000, start_date="2026-10-01",
                  end_date="2026-10-30", creators_planned=1, posts_per_day=1, ramp_weeks=3, approved=1, created_at=NOW)
    n = [0]

    def post(hnd, up, observed=None, source="sheet", platform="instagram", **kw):
        n[0] += 1
        return db.insert("auto_posts", **{
            "post_key": f"instagram:K{n[0]}", "url_key": f"ig:K{n[0]}", "campaign_id": C, "creator_id": "SJq9",
            "account_handle": hnd, "platform": platform, "url": f"https://www.instagram.com/reel/K{n[0]}/",
            "source": source, "day": automation.et_day(up), "uploaded_at": up, "w_random": 1.0, "p_likes": 0.01,
            "ramp_start": automation.et_day(up), "observed_views": observed, "base_views": observed,
            "base_day": automation.et_day(up),
            "observed_likes": 0 if observed is not None else None,
            "checked_at": NOW if observed is not None else None, "created_at": NOW, **kw})
    G = lambda i: db.q1("SELECT * FROM auto_posts WHERE id=?", (i,))  # noqa: E731
    post("CazKc", NOW - 10 * 86400, 1000)
    post("caz.kc", NOW - 9 * 86400, 2000, source="sideshift")
    post("Kaheim Corbin", NOW - 8 * 86400, 3000)
    fresh = post("Kaheim Corbin", NOW - 30 * 3600, 50000, t24=50000)
    check("v2.9.1: normal(account) pools every handle of the creator on that platform",
          planner.normal_views(G(fresh), {}, NOW) == 2000)
    check("v2.9.1: display handle = the SideShift one", planner.post_view(G(fresh))["account_handle"] == "caz.kc")
    v = post("CazKc", NOW - 5 * 86400, 900, reason="viral", peak=40000, ramp_start="2026-10-15")
    check("v2.9.1: 14-day viral cooldown sees a viral post under another handle", planner._has_viral(G(fresh), TODAY))
    db.ex("UPDATE auto_posts SET reason='base', peak=NULL WHERE id=?", (v,))
    real = planner.rng.random
    planner.rng.random = lambda: 0.0
    for i in range(3):   # other creators (1 post each): campaign q75 below fresh
        post(f"o{i}", NOW - 40 * 3600, 100, t24=100, creator_id=f"o{i}")
    picked = planner.pick_viral(automation._camp(C), db.q("SELECT * FROM auto_posts WHERE campaign_id=?", (C,)),
                                lambda p: True, NOW)
    planner.rng.random = real
    check("v2.9.1: >= 3 posts on that platform counts every handle of the creator (fresh post picked)",
          picked == [fresh], picked)
    planner.approve(fresh, 60000, NOW)
    d = [post(hh, NOW - 20 * 3600 + i * 60) for i, hh in enumerate(("caz.kc", "CazKc", "Kaheim Corbin"))]
    planner.assign_decay(C)
    check("v2.9.1: decay = the next 3 posts of the creator on that platform, any handle",
          [G(x)["reason"] for x in d] == ["decay"] * 3 and all(G(x)["viral_parent"] == fresh for x in d))


asyncio.run(sideshift_tests())
async def ss_review_tests():
    """v2.9.2 review fixes 1-7 (mocks only)."""
    import datetime as dt
    import automation
    import planner
    import sources
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots", "jobs",
              "auto_creator_map"):
        db.ex(f"DELETE FROM {t}")
    automation._alerts.clear()
    planner._EXT.clear()
    sources._ss_cache.clear()
    sources.ss_notes.clear()
    sources.SS_GAP = 0
    os.environ["SIDESHIFT_PLATFORM_KEY"] = "test-ss"
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"
    H = 3600
    C = db.insert("auto_campaigns", name="RV", mode="live", views_goal=10_000_000, start_date="2026-10-01",
                  end_date="2026-10-30", creators_planned=1, posts_per_day=1, ramp_weeks=3, approved=1, created_at=NOW,
                  source="sideshift", source_ref="PR")
    n = [0]

    def post(acct, up, observed=None, checked=None, **kw):
        n[0] += 1
        return db.insert("auto_posts", **{
            "post_key": f"instagram:RV{n[0]}", "url_key": f"ig:RV{n[0]}", "campaign_id": C, "creator_id": acct,
            "account_handle": acct, "platform": "instagram", "url": f"https://www.instagram.com/reel/RV{n[0]}/",
            "source": "sideshift", "day": automation.et_day(up), "uploaded_at": up, "w_random": 1.0, "p_likes": 0.01,
            "ramp_start": automation.et_day(up), "observed_views": observed, "base_views": observed,
            "base_day": automation.et_day(up),
            "observed_likes": 0 if observed is not None else None, "checked_at": checked, "created_at": NOW, **kw})
    G = lambda i: db.q1("SELECT * FROM auto_posts WHERE id=?", (i,))  # noqa: E731
    for i in range(3):
        post("base", NOW - (10 + i) * 86400, 2000, NOW - H)
        post("x", NOW - (10 + i) * 86400, 1_000_000, NOW - H)   # x's own baseline: the 5x cap never binds below

    # 1. our views delivered after the SideShift snapshot count as in flight (no double order)
    snap = NOW - 20 * H
    p1 = post("x", NOW - 6 * 86400, 1000, snap)
    url = G(p1)["url"]
    planner.plan(automation._camp(C), NOW)
    base_v = db.q1("SELECT views_add FROM plan_rows WHERE url_key=?", (G(p1)["url_key"],))["views_add"]
    db.ex("DELETE FROM plan_rows")
    j1 = db.create_job(provider=P, platform="instagram", link=url, views_target=500, views_order_id="9", state="done",
                       created_at=snap + H)
    db.update_job(j1, completed_at=snap + 2 * H)
    j2 = db.create_job(provider=P, platform="instagram", link=url, views_target=400, views_order_id="10",
                       state="running", created_at=snap + 3 * H)
    db.update_job(j2, views_remaining=100)
    j3 = db.create_job(provider=P, platform="instagram", link=url, views_target=700, views_order_id="11", state="done",
                       created_at=snap - 5 * H)
    db.update_job(j3, completed_at=snap - H)
    planner.plan(automation._camp(C), NOW)
    v = db.q1("SELECT views_add FROM plan_rows WHERE url_key=?", (G(p1)["url_key"],))["views_add"]
    check("v2.9.2 #1: orders created after the snapshot (delivered or not) are subtracted; older ones are not",
          base_v - v == 900, (base_v, v))
    db.ex("DELETE FROM plan_rows")
    db.ex("DELETE FROM jobs")

    # 1 + 7a/c: ramp posts get snapshot time from history; fresh null views -> paid; 429 -> fallback
    old_ramp = post("y", NOW - 8 * 86400)
    fresh_null = post("z", NOW - 5 * H)
    sw = {G(old_ramp)["post_key"]: {"postId": "OLD", "views": 3000, "likes": 30, "_at": NOW},
          G(fresh_null)["post_key"]: {"postId": "FN", "views": 5, "likes": 0, "_at": NOW}}
    HIST = {"OLD": [{"timestamp": NOW - 15 * H, "views": 2900, "likes": 29}],
            "FN": [{"timestamp": NOW - H, "views": None, "likes": 0}]}

    async def f_hist(pid):
        return HIST[pid]
    real_hist = sources.ss_history
    sources.ss_history = f_hist
    res, stale, _ = await planner.sideshift_counts(automation._camp(C), [G(old_ramp), G(fresh_null)], sw, {}, NOW)
    check("v2.9.2 #1: any ramp post takes checked_at = snapshot time from history",
          G(old_ramp)["checked_at"] == NOW - 15 * H and G(old_ramp)["observed_views"] == 2900, G(old_ramp))
    check("v2.9.2 #7a: fresh post with null snapshot views -> paid path, not a miss",
          [p["id"] for p in stale] == [fresh_null] and G(fresh_null)["misses"] == 0, (res, stale))
    sources.ss_history = real_hist
    calls = []

    def h429(req):
        calls.append(req)
        return httpx.Response(429, json={})
    prev_client = httpx.AsyncClient
    httpx.AsyncClient = lambda **kw: _RealClient(transport=httpx.MockTransport(h429), **kw)
    try:
        res, stale, _ = await planner.sideshift_counts(automation._camp(C), [G(fresh_null), G(old_ramp)], sw, {}, NOW)
        check("v2.9.2 #7c: 429 -> back off (one call, then fail fast); fresh post -> paid, older -> sweep - 24 h",
              len(calls) == 1 and [p["id"] for p in stale] == [fresh_null] and old_ramp in res
              and G(old_ramp)["checked_at"] == NOW - 24 * H, (len(calls), res, stale))
        sources._ss_wait = 0.0
    finally:
        httpx.AsyncClient = prev_client

    # 2 + 6. creator keys through the map, both ways, case-insensitive
    db.insert("auto_creator_map", campaign_id=C, name_key="sam davis", name="Sam Davis", creator_id="cid_SAM")
    db.insert("auto_creator_map", campaign_id=C, name_key="sammy", name="Sammy", creator_id="cid_SAM")
    for i, (cid_, carry) in enumerate((("cid_SAM", 50), ("Sam Davis", 60), ("other", 70))):
        db.insert("plan_rows", plan_id=f"pr{i}", campaign_id=C, creator_id=cid_, post_url=f"https://www.instagram.com/reel/PR{i}/",
                  url_key=f"ig:PR{i}", platform="instagram", window_start=NOW, window_end=NOW, day=TODAY,
                  views_add=100, likes_add=0, carry=carry, created_at=NOW)
        db.insert("auto_orders", plan_id=f"pr{i}", campaign_id=C, url_key=f"ig:PR{i}", day=TODAY, kind="views",
                  qty=100, state="placed", attempt=0, created_at=NOW)
    got_o = {o["plan_id"] for o in automation.orders_in_scope("creator", "  SAM davis ")}
    automation.drop_carry("creator", "sammy")
    carries = {r["plan_id"]: r["carry"] for r in db.q("SELECT plan_id, carry FROM plan_rows")}
    check("v2.9.2 #2: cancel_running / drop_carry find the creator under its sheet names and SideShift id",
          got_o == {"pr0", "pr1"} and carries == {"pr0": 0, "pr1": 0, "pr2": 70}, (got_o, carries))
    db.insert("pauses", scope="creator", key=" sam DAVIS", label="x", created_by="t", created_at=NOW)
    ps = automation._pause_set()
    check("v2.9.2 #6: creator pause matched case-insensitively and through the map",
          all(automation._paused(ps, {"campaign_id": C, "url_key": "u", "creator_id": c_})
              for c_ in ("cid_SAM", "Sam Davis", "Sammy")) and not automation._paused(
              ps, {"campaign_id": C, "url_key": "u", "creator_id": "other"}))
    db.ex("DELETE FROM pauses")

    # 4 + 5. merge_creators: outage keeps the stored map; shared contractorName maps nobody; post match needs no name
    got = {"links": [{"source": "sheet", "creator_id": "SAM DAVIS", "post_key": "instagram:Q1", "account_handle": "S",
                      "uploaded_at": 1}], "slots": [{"creator_id": "Sam Davis"}], "cancelled": {"Sam Davis": False}}
    planner.merge_creators(C, got)
    check("v2.9.2 #4: SideShift outage (no links) still maps sheet names through the stored map",
          got["links"][0]["creator_id"] == "cid_SAM" and got["slots"][0]["creator_id"] == "cid_SAM")
    ss = lambda pk, cid_, name: {"source": "sideshift", "creator_id": cid_, "post_key": pk, "name": name,  # noqa: E731
                                 "account_handle": cid_, "uploaded_at": 2}
    got = {"links": [{"source": "sheet", "creator_id": "Sam Davis", "post_key": "instagram:Z1", "account_handle": "S",
                      "uploaded_at": 1},
                     {"source": "sheet", "creator_id": "Bo", "post_key": "instagram:Z2", "account_handle": "B",
                      "uploaded_at": 1},
                     ss("instagram:Z9", "cid_A", "Sam Davis"), ss("instagram:Z8", "cid_B", "sam davis "),
                     ss("instagram:Z2", "cid_bo", "Robert B")],
           "slots": [], "cancelled": {}}
    planner.merge_creators(C, got)
    cm = {r["name_key"]: r["creator_id"] for r in db.q("SELECT * FROM auto_creator_map WHERE campaign_id=?", (C,))}
    check("v2.9.2 #5: a contractorName two contractors share maps nobody (stored entry dropped); a post match with a "
          "contractorName set creates no mapping",
          "sam davis" not in cm and "bo" not in cm and got["links"][0]["creator_id"] == "Sam Davis"
          and got["links"][1]["creator_id"] == "Bo", cm)

    # 3. sweep: page cap and missing totalPages -> alert once, keep paging until an empty page
    pages_seen = []

    def hp(req):
        pg = int(req.url.params["page"])
        pages_seen.append(pg)
        data = [{"postId": f"S{pg}", "postPage": f"https://www.instagram.com/reel/SW{pg}/", "platform": "instagram",
                 "views": 1}] if pg <= 3 else []
        return httpx.Response(200, json={"data": {"data": data, "page": pg}})   # no totalPages
    httpx.AsyncClient = lambda **kw: _RealClient(transport=httpx.MockTransport(hp), **kw)
    try:
        sources._ss_cache.clear()
        sources.ss_notes.clear()
        automation._alerts.clear()
        sw1 = await planner.sweep(automation._camp(C))
        no_tp = pages_seen == [1, 2, 3, 4] and len(sw1) == 3
        sources._ss_cache.clear()
        real_cap, sources.SS_MAX_PAGES = sources.SS_MAX_PAGES, 2
        pages_seen.clear()
        sw2 = await planner.sweep(automation._camp(C))
        sources.SS_MAX_PAGES = real_cap
        await planner.sweep(automation._camp(C))
        al = sorted(t for t in automation._alerts if "SideShift program PR" in t)
        check("v2.9.2 #3: no totalPages -> pages until empty; page cap stops; each oddity alerts once",
              no_tp and pages_seen == [1, 2] and len(sw2) == 2 and len(al) == 2
              and any("cap" in t for t in al) and any("totalPages" in t for t in al), (pages_seen, al))
    finally:
        httpx.AsyncClient = prev_client
        sources._ss_cache.clear()
        sources.ss_notes.clear()

    # 7b. deletionStatus: explicit values only
    check("v2.9.2 #7b: deleted only for deleted / removed / true / 1",
          [sources.ss_deleted({"deletionStatus": v}) for v in ("deleted", "Removed", True, 1, "1", None, "pending",
                                                               "active", False, "")]
          == [True, True, True, True, True, False, False, False, False, False])


asyncio.run(account_key_tests())
async def stale72_tests():
    """v2.9.3: older ramp posts with a SideShift snapshot > 72 h old that still need views get a paid re-read, last."""
    import datetime as dt
    import automation
    import planner
    import sources
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots", "jobs",
              "auto_creator_map"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%' OR k LIKE 'auto:max_%'")
    automation._alerts.clear()
    planner._EXT.clear()
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    H = 3600
    C = db.insert("auto_campaigns", name="S72", mode="practice", views_goal=5_000_000, start_date="2026-10-01",
                  end_date="2026-10-30", creators_planned=1, posts_per_day=1, ramp_weeks=3, approved=1, created_at=NOW,
                  source="sideshift", source_ref="P72")
    items, hist = [], {}

    def post(key, up, views, snap_age_h, target=None, in_sw=True, observed=None):
        url = f"https://www.instagram.com/reel/{key}/"
        pid = db.insert("auto_posts", post_key=f"instagram:{key}", url_key=f"ig:{key}", campaign_id=C, creator_id="c",
                        account_handle="c", platform="instagram", url=url, source="sideshift", day=automation.et_day(up),
                        uploaded_at=up, w_random=1.0, p_likes=0.01, ramp_start=automation.et_day(up),
                        target_total=target, observed_views=observed, observed_likes=0 if observed else None,
                        checked_at=NOW - 30 * H if observed else None, created_at=NOW)
        if in_sw:
            items.append({"postId": key, "postPage": url, "platform": "instagram", "creator": "c", "contractorId": "c",
                          "contractorName": "", "views": views, "likes": 5, "uploadedAt": up, "deletionStatus": None})
            hist[key] = [{"timestamp": NOW - snap_age_h * H, "views": views, "likes": 5}]
        return pid
    for i in range(3):   # baseline
        post(f"B{i}", NOW - (12 + i) * 86400, 2000, 30, observed=2000)
    fresh = post("F1", NOW - 10 * H, 50, 25)                         # first 3 days, snapshot > 20 h -> paid (strict)
    o1 = post("O1", NOW - 8 * 86400, 3000, 100, target=10000)        # old snapshot, still short -> paid re-read, last
    o2 = post("O2", NOW - 8 * 86400, 3000, 100, target=100)          # old snapshot, target already met -> SideShift only
    o3 = post("O3", NOW - 8 * 86400, 3000, 30, target=10000)         # snapshot 30 h -> SideShift only
    nsw = post("N1", NOW - 2 * 86400, 0, 0, in_sw=False)             # not in SideShift -> paid
    G = lambda i: db.q1("SELECT * FROM auto_posts WHERE id=?", (i,))  # noqa: E731

    async def f_items(pid):
        return NOW, items

    async def f_hist(pid):
        return hist[pid]
    paid, answer = [], {}

    async def f_counts(platform, url):
        k = sources.post_key(platform, url)
        paid.append(k.split(":")[1])
        return answer.get(k, (7777, 50))
    real = (sources.ss_items, sources.ss_history, sources.get_counts)
    sources.ss_items, sources.ss_history, sources.get_counts = f_items, f_hist, f_counts
    try:
        r = await planner.run_campaign(C, NOW)
        check("v2.9.3: >72 h snapshot + still short -> paid re-read after every other paid check; others SideShift only",
              paid[-1] == "O1" and set(paid) == {"F1", "N1", "O1"} and G(o1)["observed_views"] == 7777
              and G(o2)["observed_views"] == 3000 and G(o3)["observed_views"] == 3000 and not r["errors"], (paid, r))

        db.ex("UPDATE auto_posts SET target_total=10000, observed_views=NULL, checked_at=NULL WHERE id=?", (o1,))
        db.ex("UPDATE auto_posts SET target_total=100 WHERE id=?", (o2,))
        db.ex("DELETE FROM settings WHERE k LIKE 'planner:%'")
        paid.clear()
        planner.set_limits({"max_scrapes_per_run": 2})
        await planner.run_campaign(C, NOW + 60)
        check("v2.9.3: cap reached -> re-read dropped first, SideShift values kept",
              "O1" not in paid and len(paid) == 2 and G(o1)["observed_views"] == 3000
              and G(o1)["checked_at"] == NOW - 100 * H, (paid, G(o1)))
        planner.set_limits({"max_scrapes_per_run": 300})

        db.ex("UPDATE auto_posts SET target_total=10000, observed_views=NULL, checked_at=NULL WHERE id=?", (o1,))
        db.ex("DELETE FROM settings WHERE k LIKE 'planner:%'")
        answer["instagram:O1"] = None   # paid read fails
        paid.clear()
        await planner.run_campaign(C, NOW + 120)
        p1 = G(o1)
        check("v2.9.3: a failed re-read changes nothing (no miss, no sit-out, SideShift counts kept)",
              "O1" in paid and p1["misses"] == 0 and p1["skip_next"] == 0 and p1["observed_views"] == 3000, p1)
    finally:
        sources.ss_items, sources.ss_history, sources.get_counts = real
        db.ex("DELETE FROM settings WHERE k LIKE 'auto:max_%'")
    del fresh, nsw


async def v210_tests():
    """v2.10: normal(account) = median, sheet "Top Performers" tab = bought views, ramp from the upload day."""
    import datetime as dt
    import automation
    import planner
    import sources
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots", "jobs",
              "auto_creator_map"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%'")
    automation._alerts.clear()
    planner._EXT.clear()
    planner._TOP.clear()
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    D = 86400
    C = db.insert("auto_campaigns", name="V", mode="practice", views_goal=50_000_000, start_date="2026-10-01",
                  end_date="2026-10-30", creators_planned=1, posts_per_day=1, ramp_weeks=3, approved=1, created_at=NOW,
                  source="sheet", source_ref="sidV:Daily Posts")
    n = [0]

    def post(up, observed=None, seen=None, acct="a", cid=C, **kw):
        n[0] += 1
        return db.insert("auto_posts", **{
            "post_key": f"instagram:V{n[0]}", "url_key": f"ig:V{n[0]}", "campaign_id": cid, "creator_id": acct,
            "account_handle": acct, "platform": "instagram", "url": f"https://www.instagram.com/reel/V{n[0]}/",
            "source": "sheet", "day": automation.et_day(up), "uploaded_at": up, "w_random": 1.0, "p_likes": 0.01,
            "ramp_start": automation.et_day(up), "observed_views": observed,
            "observed_likes": 0 if observed is not None else None, "base_views": observed,
            "base_day": automation.et_day(seen or up) if observed is not None else None,
            "checked_at": NOW if observed is not None else None, "created_at": NOW, **kw})
    G = lambda i: db.q1("SELECT * FROM auto_posts WHERE id=?", (i,))  # noqa: E731

    # 1. median: one viral-sized post does not inflate normal(account)
    for i, v in enumerate((1000, 1000, 1000, 50000)):
        post(NOW - (10 + i) * D, v)
    late = post(NOW - 4 * D, 0, base_views=None, base_day=None)   # uploaded 16 Oct (ramp day 1), first read 20 Oct (day 5)
    check("v2.10 #1: normal(account) = median of the last 10 posts (1000, not the 13,250 mean)",
          planner.normal_views(G(late), {}, NOW) == 1000)
    camp_mean = post(NOW - 5 * D, 10, acct="solo")   # < 3 own posts -> campaign median
    check("v2.10 #1: campaign fallback = median too", planner.normal_views(G(camp_mean), {}, NOW) == 1000)
    db.ex("DELETE FROM auto_posts WHERE id=?", (camp_mean,))

    # 2. Top Performers tab
    top_csv = ('"Rank","Creator","Platform","Day","Slot","Views (Lumina, 22 Oct 13:55 UK)","Full URL","Views to buy",""\n'
               '"1","Cole Pash","Instagram","1","Video 2 PM repost","61263","https://www.instagram.com/reel/V1/","200k","61.8K"\n'
               '"2","Terrence Zhu","Instagram","1","Video 2 PM repost","55622","https://www.instagram.com/reel/V2/","1.5m",""\n'
               '"3","x","Instagram","1","AM","5","https://www.instagram.com/reel/V3/","20,000",""\n'
               '"4","y","Instagram","1","AM","5","https://www.instagram.com/reel/V4/","1.547",""\n'
               '"5","z","Instagram","1","AM","5","","300k",""\n'
               '"6","z","Instagram","1","AM","5","https://www.instagram.com/reel/V5/","",""\n')
    pt, bad0 = sources.parse_top(top_csv)
    check("v2.10 #2: Top Performers parse (200k / 1.5m / 20,000 / 1.547; no URL or no amount = skipped)",
          [(x["url"].split("/")[-2], x["views"], x["likes"]) for x in pt]
          == [("V1", 200000, 0), ("V2", 1500000, 0), ("V3", 20000, 0), ("V4", 1547, 0)], pt)
    daily = '"XTO daily posts","",""\n"Date (ET)","Creator","IG AM 1"\n"20 Oct","a","https://www.instagram.com/reel/V1/"\n'
    check("v2.10 #2: gviz unknown tab answers the first tab -> header guard -> []",
          sources.parse_top(daily) == ([], []) and bad0 == [])
    cells = ["200k", "200k+", "~200k", "1.5 M views", "1,5m", "1,500k", "20,000", "1.547", "1547", "", "  ",
             "1.5", "0", "abc", "1.5.5k", "12,34"]
    got = [sources._qty(c) for c in cells]
    check("v2.10 #2: Views to buy cells (+ / ~ / 'views' stripped; '1,500k' = thousands; '1.5' / 0 / junk = None)",
          got == [200000, 200000, 200000, 1500000, 1500000, 1500000, 20000, 1547, 1547, 0, 0,
                  None, None, None, None, None], got)
    odd = top_csv + ''.join(f'"9","q","Instagram","1","AM","5","https://www.instagram.com/reel/W{i}/","{c}",""\n'
                            for i, c in enumerate(("1.5", "200k?", "0", "abc")))
    pt2, bad2 = sources.parse_top(odd)
    check("v2.10 #2: unreadable cells with a URL skipped and returned", len(pt2) == 4 and bad2 == ["1.5", "200k?", "0", "abc"],
          (pt2, bad2))
    views_csv = ('"fv","Post link","Views to buy","Status"\n'
                 '"ma 19 okt","https://www.instagram.com/reel/V1/","1.547","Done"\n')
    tops = [top_csv]

    async def f_views(sid):
        return views_csv

    async def f_top(sid):
        if isinstance(tops[0], Exception):
            raise tops[0]
        return tops[0]
    real = sources.fetch_views, sources.fetch_top
    sources.fetch_views, sources.fetch_top = f_views, f_top
    try:
        await planner.load_external(automation._camp(C))
        fx = planner.order_facts("2026-10-20", NOW)
        check("v2.10 #2: Top Performers buys summed with the Views tab per post, subtracted from natural",
              fx["ig:V1"]["ext"] == 201547 and fx["ig:V2"]["ext"] == 1500000 and fx["ig:V1"]["ext_likes"] == 0,
              {k: fx[k]["ext"] for k in ("ig:V1", "ig:V2")})
        tops[0] = odd
        automation._alerts.clear()
        await planner.load_external(automation._camp(C))
        al = [t for t in automation._alerts if "unreadable" in t]
        check("v2.10 #2: unreadable Views to buy cells -> rows skipped, one alert listing up to 3 cells",
              len(al) == 1 and "4 unreadable" in al[0] and "'1.5', '200k?', '0')" in al[0] and "abc" not in al[0]
              and planner.order_facts("2026-10-20", NOW)["ig:V1"]["ext"] == 201547, al)
        tops[0] = sources.SourceError("HTTP 500")
        automation._alerts.clear()
        await planner.load_external(automation._camp(C))
        await planner.load_external(automation._camp(C))
        al = [t for t in automation._alerts if "Top Performers" in t]
        fx = planner.order_facts("2026-10-20", NOW)
        check("v2.10 #2: Top Performers read failure = one alert, Views tab still read, last good Top read kept",
              len(al) == 1 and not [t for t in automation._alerts if '"Views" tab' in t]
              and fx["ig:V1"]["ext"] == 201547, (al, fx["ig:V1"]))
        tops[0] = daily
        automation._alerts.clear()
        await planner.load_external(automation._camp(C))
        check("v2.10 #2: no Top Performers tab = nothing from it, no alert",
              planner.order_facts("2026-10-20", NOW)["ig:V1"]["ext"] == 1547 and not automation._alerts,
              automation._alerts)
    finally:
        sources.fetch_views, sources.fetch_top = real
        planner._EXT.clear()
        planner._TOP.clear()

    # 3. ramp from the upload day; first read on day k -> base + only days k..D, no catch-up
    db.ex("UPDATE auto_posts SET url_key='ig:base'||id WHERE id<>?", (late,))   # keep the sheet facts off them
    cV = automation._camp(C)
    planner.plan(cV, NOW)
    check("v2.10 #3: checked but no base yet -> no row",
          not db.q1("SELECT 1 FROM plan_rows WHERE plan_id=?", (f"{C}:{G(late)['post_key']}:2026-10-20",)))
    planner.apply_count(G(late), (1000, 10), {}, NOW)
    check("v2.10 #3: first views read sets base_views / base_day",
          (G(late)["base_views"], G(late)["base_day"]) == (1000, "2026-10-20"), G(late))
    w = planner.weights(planner.ramp_len(G(late), cV))
    planner.plan(cV, NOW)
    r5 = db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (f"{C}:{G(late)['post_key']}:2026-10-20",))
    check("v2.10 #3: first read day 5 with 1,000 views -> views_add = target x w5 (base + share - observed)",
          G(late)["target_total"] == 5000 and r5["views_add"] == round(5000 * w[4]) > 0, dict(r5))
    planner.apply_count(G(late), (1200, 12), {}, NOW + D)
    planner.apply_count(G(late), (1300, 13), {}, NOW + D + 60)
    check("v2.10 #3: later reads never move the base",
          (G(late)["base_views"], G(late)["base_day"], G(late)["observed_views"]) == (1000, "2026-10-20", 1300), G(late))
    planner.plan(cV, NOW + D)
    r6 = db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (f"{C}:{G(late)['post_key']}:2026-10-21",))
    check("v2.10 #3: day 6 cumulative = 1,000 + target x (w5 + w6) - observed",
          r6["views_add"] == round(1000 + 5000 * (w[4] + w[5]) - 1300), dict(r6))
    capp = post(NOW - 2 * D, 4990, seen=NOW)       # < 3 days old: not in normal(account); k = 3
    planner.plan(cV, NOW)
    rc = db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (f"{C}:{G(capp)['post_key']}:2026-10-20",))
    check("v2.10 #3: goal capped at target (base 4,990 + w3 share > 5,000 -> views_add 10)",
          G(capp)["target_total"] == 5000 and rc["views_add"] == 10, dict(rc))
    same = post(NOW - 5 * 3600, 50)                # first read on its upload day: unchanged (target x F)
    planner.plan(cV, NOW)
    rs = db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (f"{C}:{G(same)['post_key']}:2026-10-20",))
    ws = planner.weights(planner.ramp_len(G(same), cV))
    check("v2.10 #3: k = 1 post = target x F(d) as before",
          planner.goal(G(same), cV, "2026-10-20", 777) == 777 * planner.F(G(same), cV, "2026-10-20")
          and rs["views_add"] == round(G(same)["target_total"] * ws[0] - 50), dict(rs))
    vr = post(NOW - 8 * D, 900, seen=NOW, reason="viral", peak=40000, ramp_start="2026-10-15")
    check("v2.10 #3: viral = target x F(d), base ignored",
          planner.goal(G(vr), cV, "2026-10-20", 40000) == 40000 * planner.F(G(vr), cV, "2026-10-20") > 0)
    C1 = db.insert("auto_campaigns", name="V1", mode="practice", views_goal=100000,
                   start_date="2026-09-01", end_date="2026-10-30", creators_planned=1, posts_per_day=1, ramp_weeks=1,
                   approved=1, created_at=NOW)   # v2.13: ramp_weeks no longer read, the ramp is 21 days
    for i in range(3):
        post(NOW - (30 + i) * D, 1000, acct="e", cid=C1)
    post(NOW - 23 * D, 100, seen=NOW, acct="e", cid=C1)    # 21-day ramp from upload ended 2 days ago
    s = planner.plan(automation._camp(C1), NOW)
    check("v2.10 #3: upload-day ramp already over when first read -> not in ramp, no row, no crash",
          s["in_ramp"] == 0 and not db.q("SELECT 1 FROM plan_rows WHERE campaign_id=?", (C1,)), s)

    # 3b. startup fix: ramp_start back to the upload day for base/decay (viral untouched); base from the last read
    b0 = post(NOW - 4 * D, 10, ramp_start="2026-10-20", base_views=None, base_day=None,
              checked_at=int(dt.datetime(2026, 10, 19, 23, 30, tzinfo=ET).timestamp()))   # 03:30 UTC = 19 Oct ET
    d0 = post(NOW - 4 * D, 10, ramp_start="2026-10-20", reason="decay", peak=900)
    v0 = post(NOW - 8 * D, 10, ramp_start="2026-10-19", reason="viral", peak=9000)
    k0 = post(NOW - 4 * D, 10, seen=NOW - D, base_views=7)        # base already set: kept
    db.init_db()
    db.init_db()
    check("v2.10 #3: startup fix: ramp_start = day for base/decay, viral keeps approval day; base = last read, "
          "base_day = ET day of checked_at; set bases kept (idempotent)",
          [G(x)["ramp_start"] for x in (b0, d0, v0)] == ["2026-10-16", "2026-10-16", "2026-10-19"]
          and (G(b0)["base_views"], G(b0)["base_day"]) == (10, "2026-10-19")
          and (G(k0)["base_views"], G(k0)["base_day"]) == (7, "2026-10-19"),
          [dict(G(x)) for x in (b0, k0)])


asyncio.run(ss_review_tests())
asyncio.run(stale72_tests())
asyncio.run(v210_tests())


async def v2102_tests():
    """v2.10.2: viral goal starts from viral_base (views at approval), so the approval day already spikes."""
    import datetime as dt
    import automation
    import planner
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots", "jobs",
              "auto_creator_map"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%'")
    planner._EXT.clear()
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    D = 86400
    C = db.insert("auto_campaigns", name="VB", mode="practice", views_goal=1_000_000, start_date="2026-10-01",
                  end_date="2026-10-30", creators_planned=1, posts_per_day=1, ramp_weeks=3, approved=1, created_at=NOW)
    n = [0]

    def post(up, observed, acct="a", **kw):
        n[0] += 1
        return db.insert("auto_posts", **{
            "post_key": f"instagram:VB{n[0]}", "url_key": f"ig:VB{n[0]}", "campaign_id": C, "creator_id": acct,
            "account_handle": acct, "platform": "instagram", "url": f"https://www.instagram.com/reel/VB{n[0]}/",
            "source": "sheet", "day": automation.et_day(up), "uploaded_at": up, "w_random": 1.0, "p_likes": 0.01,
            "ramp_start": automation.et_day(up), "observed_views": observed, "observed_likes": 0,
            "base_views": observed, "base_day": automation.et_day(up), "checked_at": NOW - 60, "created_at": up, **kw})
    G = lambda i: db.q1("SELECT * FROM auto_posts WHERE id=?", (i,))  # noqa: E731
    row = lambda p, d: db.q1("SELECT * FROM plan_rows WHERE plan_id=?", (f"{C}:{p['post_key']}:{d}",))  # noqa: E731
    for i in range(3):
        post(NOW - (10 + i) * D, 1000)
    v = post(NOW - 30 * 3600, 2065, viral_state="picked")
    planner.approve(v, 7336, NOW)
    cV = automation._camp(C)
    w = planner.weights(planner.ramp_len(G(v), cV), True)
    planner.plan(cV, NOW)
    r1 = row(G(v), "2026-10-20")
    check("v2.10.2: approval day: viral_base = views at approval, views_add = base + (peak - base) x w1 - observed > 0",
          G(v)["viral_base"] == 2065 and r1["target_total_views"] == 7336
          and r1["views_add"] == round(2065 + (7336 - 2065) * w[0] - 2065) > 0, (dict(r1), w[0]))
    planner.apply_count(G(v), (2900, 30), {}, NOW + D)
    planner.plan(cV, NOW + D)
    r2 = row(G(v), "2026-10-21")
    check("v2.10.2: day 2 cumulative = base + (peak - base) x (w1 + w2) - observed; base never moves",
          G(v)["viral_base"] == 2065 and r2["views_add"] == round(2065 + 5271 * (w[0] + w[1]) - 2900), dict(r2))
    check("v2.10.2: likes use the same goal",
          r2["likes_add"] == round((2065 + 5271 * (w[0] + w[1])) * 0.01) - 30, dict(r2))
    p = G(v)
    check("v2.10.2: goal reaches the peak on the last ramp day and never passes it",
          abs(planner.goal(p, cV, "2026-10-29", 7336) - 7336) < 1e-6
          and all(planner.goal(p, cV, f"2026-10-{d}", 7336) <= 7336 for d in range(20, 31)))
    hi = post(NOW - 30 * 3600, 9000, acct="h", reason="viral", viral_state="approved", peak=7336, viral_base=9000,
              ramp_start="2026-10-20")
    planner.plan(cV, NOW)
    rh = row(G(hi), "2026-10-20")
    check("v2.10.2: peak < viral_base -> target = observed, goal = base, views_add 0 (never negative)",
          rh["target_total_views"] == 9000 and rh["views_add"] == 0 and rh["likes_add"] >= 0
          and planner.goal({**G(hi), "viral_base": 9000}, cV, "2026-10-21", 7336) == 7336, dict(rh))
    old = post(NOW - 30 * 3600, 3000, acct="o", reason="viral", viral_state="approved", peak=20000,
               ramp_start="2026-10-19")
    planner.plan(cV, NOW)
    db.ex("UPDATE auto_posts SET observed_views=3500 WHERE id=?", (old,))
    planner.plan(cV, NOW + 60)
    check("v2.10.2: viral approved before the deploy: viral_base = observed at the first plan, stored once",
          G(old)["viral_base"] == 3000, G(old))
    b = post(NOW - 5 * 3600, 50, acct="b")
    check("v2.10.2: base k = 1 goal unchanged (target x F)",
          planner.goal(G(b), cV, "2026-10-20", 5000) == 5000 * planner.F(G(b), cV, "2026-10-20")
          and planner.post_view(G(v))["viral_base"] == 2065)


asyncio.run(v2102_tests())


async def v211_tests():
    """v2.11: All submissions (clipper email, all group, sent filter, bulk search) + Buy again."""
    import automation
    import main
    from fastapi import HTTPException
    reset()
    # clippers.submissions passes clipper_email through as `email`
    real_get = clippers._get

    async def fake_get(path, params=None):
        return {"items": [{"id": 1, "post_url": "u", "platform": "Instagram", "clipper_name": "n",
                           "clipper_email": " e@x.com ", "created_at": None},
                          {"id": 2, "post_url": "v", "platform": "Instagram", "clipper_email": "f@x.com"}], "pages": 1}
    clippers._get = fake_get
    try:
        subs_ = await REAL_SUBMISSIONS(1)
    finally:
        clippers._get = real_get
    check("v2.11: submissions() returns clipper_email as email (name fallback kept)",
          [s["email"] for s in subs_] == ["e@x.com", "f@x.com"] and subs_[1]["clipper"] == "f@x.com", subs_)

    L = mk_link(campaign_id=1101, views=1000, likes=100)
    link = campaigns.get_link(L)

    def sub(sid, code, email):
        return {"id": sid, "url": f"https://www.instagram.com/reel/{code}/", "platform": "instagram", "clipper": "c" + code,
                "submitted_at": None, "email": email}

    def row(sid):
        return db.q1("SELECT * FROM queue WHERE link_id=? AND submission_id=?", (L, sid))
    campaigns._insert_submission(link, sub(91001, "V11A", "Ann@X.com"), "not_sent")
    campaigns._insert_submission(link, sub(91002, "V11B", None), "not_sent")
    check("v2.11: email stored on insert", row(91001)["clipper_email"] == "Ann@X.com" and row(91002)["clipper_email"] is None)
    campaigns._insert_submission(link, sub(91002, "V11B", "bob@y.com"), "pending")
    campaigns._insert_submission(link, sub(91001, "V11A", "other@z.com"), "pending")
    check("v2.11: poll backfills a NULL email by (link, submission); never overwrites; state untouched",
          row(91002)["clipper_email"] == "bob@y.com" and row(91001)["clipper_email"] == "Ann@X.com"
          and row(91002)["state"] == "not_sent", (row(91001), row(91002)))
    A, B = row(91001)["id"], row(91002)["id"]
    C, D = mk_item(L, "https://instagram.com/reel/V11C")["id"], mk_item(L, "https://instagram.com/reel/V11D")["id"]
    db.update("queue", C, state="skipped")
    r = await place(A, ("not_sent",))
    check("v2.11: setup A placed", r["state"] == "placed", r)

    d = await main.queue(state="all", link_id=L, page=1, group="all", _="t")
    check("v2.11: group all = every row of the link, any state; items carry clipper_email + buys",
          {x["id"] for x in d["items"]} == {A, B, C, D} and d["groups"]["all"] == 4
          and {x["id"]: x["buys"] for x in d["items"]} == {A: 1, B: 0, C: 0, D: 0}
          and next(x for x in d["items"] if x["id"] == A)["clipper_email"] == "Ann@X.com", d)

    async def srch(terms=(), sent="all", page=1):
        return await main.queue_search(main.SearchBody(link_id=L, terms=list(terms), sent=sent, page=page), _="t")

    def ids(d):
        return {x["id"] for x in d["items"]}
    y, n = await srch(sent="yes"), await srch(sent="no")
    check("v2.11: sent filter: yes = ordered, no = never ordered; sent_counts",
          ids(y) == {A} and ids(n) == {B, C, D} and y["sent_counts"] == {"all": 4, "yes": 1, "no": 3}
          and y["items"][0]["sent"] is True, (y, n))
    d = await srch(["instagram.com/reel/V11A", "https://instagram.com/p/V11D?x=1", "https://instagram.com/reel/NOPE"])
    check("v2.11: bulk links match by url_key; missing listed",
          ids(d) == {A, D} and d["terms"] == 3 and d["missing"] == ["https://instagram.com/reel/NOPE"], d)
    d = await srch(["ANN@x.com", "bob@y.com", "zed@q.com"])
    check("v2.11: bulk emails match case-insensitive exact; missing listed",
          ids(d) == {A, B} and d["missing"] == ["zed@q.com"], d)
    d = await srch(["ANN@x.com", "bob@y.com"], sent="no")
    check("v2.11: terms + sent filter combine", ids(d) == {B} and d["sent_counts"] == {"all": 2, "yes": 1, "no": 1}, d)
    d, d2 = await srch(["V11C"]), await srch(["ann@"])
    check("v2.11: a single short term substring-matches url / email", ids(d) == {C} and ids(d2) == {A}
          and d["missing"] == [] and d2["missing"] == [], (d, d2))
    d = await srch(["%"])
    check("v2.11: LIKE wildcards in the term are literal", ids(d) == set(), d)
    try:
        await srch(sent="maybe")
        check("v2.11: bad sent -> 400", False)
    except HTTPException as e:
        check("v2.11: bad sent -> 400", e.status_code == 400)

    async def rebuy(ids_, views, likes=0):
        return await main.queue_rebuy(main.RebuyBody(ids=ids_, views=views, likes=likes), who="tester")
    before, adds = row(91001), STATE["adds"]
    r1, r2, r3 = await rebuy([A], 1001), await rebuy([A], 500, 101), await rebuy([B], 500)
    check("v2.11: rebuy rejects views/likes above the link's per-post amounts and not-sent rows, no panel call",
          not r1["placed"] and not r2["placed"] and "1,000" in r1["failed"][0]["error"]
          and "not sent" in r3["failed"][0]["error"] and STATE["adds"] == adds
          and not db.q1("SELECT id FROM rebuys"), (r1, r2, r3))
    r = await rebuy([A], 50)
    check("v2.11: rebuy runs qty_problem (below service minimum)", "minimum" in r["failed"][0]["error"]
          and STATE["adds"] == adds, r)
    spent0 = campaigns.spent_today(L)
    r = await rebuy([A, A], 500, 50)
    rb = db.q("SELECT * FROM rebuys")
    job = db.get_job(r["placed"][0]["job_id"]) if r["placed"] else {}
    check("v2.11: rebuy places once per id, records job/views/likes/est_cost/created_by",
          len(r["placed"]) == 1 and STATE["adds"] == adds + 1 and len(rb) == 1 and rb[0]["state"] == "placed"
          and rb[0]["job_id"] == job.get("id") and rb[0]["views"] == 500 and rb[0]["likes"] == 50
          and rb[0]["created_by"] == "tester" and rb[0]["est_cost"] == 0.024 and job.get("source") == "campaign", (r, rb))
    check("v2.11: rebuy does not change the queue row", row(91001) == before, (before, row(91001)))
    check("v2.11: rebuy counts toward spent_today", campaigns.spent_today(L) == round(spent0 + 0.024, 4))
    check("v2.11: rebuy job stays a campaign job for dedupe (no 'manual job' reason)",
          campaigns.duplicate_reason(before["url_key"], A) is None)
    d = await srch(["V11A"])
    check("v2.11: buy count after rebuy", d["items"][0]["buys"] == 2, d)
    check("v2.11: audit log entry", db.q1("SELECT * FROM audit_log WHERE action='queue.rebuy' ORDER BY id DESC")["username"] == "tester")
    adds = STATE["adds"]
    r = await rebuy([A], 500)
    check("v2.11: second Buy again within REBUY_GAP_SEC refused, no panel call (double click / retry)",
          "last 10 min" in r["failed"][0]["error"] and STATE["adds"] == adds
          and len(db.q("SELECT id FROM rebuys")) == 1, r)
    pid = db.insert("rebuys", queue_id=A, link_id=L, url_key=before["url_key"], state="placing", views=1,
                    created_at=int(time.time()) - 3600)
    r = await rebuy([A], 500)
    check("v2.11: an old rebuy still placing also blocks", "last 10 min" in r["failed"][0]["error"]
          and STATE["adds"] == adds, r)
    db.ex("DELETE FROM rebuys WHERE id=?", (pid,))
    db.ex("UPDATE rebuys SET created_at = created_at - ?", (campaigns.REBUY_GAP_SEC + 1,))

    adds = STATE["adds"]
    db.update("links", L, daily_cap_usd=campaigns.spent_today(L) + 0.001)
    r = await rebuy([A], 500)
    check("v2.11: rebuy respects the daily cap", "daily cap" in r["failed"][0]["error"] and STATE["adds"] == adds, r)
    db.update("links", L, daily_cap_usd=None)
    reset(balance=0.001)
    r = await rebuy([A], 500)
    check("v2.11: rebuy respects the balance guard", "low balance" in r["failed"][0]["error"] and STATE["adds"] == 0, r)
    reset()
    env = os.environ.get("AUTO_ENABLED")
    os.environ["AUTO_ENABLED"] = "1"
    ac = db.insert("auto_campaigns", name="RB", mode="live", approved=1, created_at=int(time.time()))
    db.insert("plan_rows", plan_id="rb:1", campaign_id=ac, post_url=before["url"], url_key=before["url_key"],
              platform="instagram", window_start=int(time.time()), window_end=int(time.time()) + 3600, day="2026-10-25",
              created_at=int(time.time()))
    try:
        r = await rebuy([A], 500)
    finally:
        db.ex("DELETE FROM plan_rows WHERE plan_id='rb:1'")
        if env is None:
            os.environ.pop("AUTO_ENABLED", None)
        else:
            os.environ["AUTO_ENABLED"] = env
    check("v2.11: rebuy skips a post in automation, queue row untouched",
          r["failed"][0]["error"] == "in automation" and STATE["adds"] == 0 and row(91001) == before, r)
    reset(plan=["timeout"])
    spent0 = campaigns.spent_today(L)
    r = await rebuy([A], 500)
    amb = db.q1("SELECT * FROM rebuys ORDER BY id DESC")
    check("v2.11: ambiguous rebuy failure kept as failed + counted as spent",
          campaigns.AMBIGUOUS in r["failed"][0]["error"] and amb["state"] == "failed"
          and campaigns.AMBIGUOUS in amb["error"] and row(91001) == before
          and campaigns.spent_today(L) == round(spent0 + 0.02, 4), (r, amb))
    reset()
    dkey = db.q1("SELECT url_key FROM queue WHERE id=?", (D,))["url_key"]
    check("v2.11: pending row alone is not an open queue order", not automation._open_queue(dkey))
    rid = db.insert("rebuys", queue_id=D, link_id=L, url_key=dkey, state="placing", views=1, created_at=int(time.time()))
    check("v2.11: an open rebuy blocks the executor (_open_queue)", automation._open_queue(dkey))
    campaigns.recover_stuck()
    check("v2.11: recover_stuck fails a rebuy stuck in placing",
          db.q1("SELECT state FROM rebuys WHERE id=?", (rid,))["state"] == "failed")


asyncio.run(v211_tests())


async def v212_tests():
    """v2.12 calculator fields + campaign cap (Milan spec sections 1, 2)."""
    import automation
    import main
    from fastapi import HTTPException
    CAT = [{"service": 26146, "name": "Instagram Views", "rate": "0.04", "min": "100", "max": "1000000"},
           {"service": 777, "name": "Instagram Views Alt", "rate": "0.05", "min": "100", "max": "1000000"},
           {"service": 5227, "name": "Facebook Views", "rate": "0.06", "min": "100", "max": "1000000"},
           {"service": 26330, "name": "Instagram Likes", "rate": "0.08", "min": "20", "max": "100000"},
           {"service": 26999, "name": "Instagram Likes Cheap", "rate": "0.02", "min": "20", "max": "100000"},
           {"service": 22457, "name": "Facebook Likes", "rate": "0.1", "min": "20", "max": "100000"}]

    async def f_services(p):
        if p != P:
            raise panels.PanelUnavailable("no key")
        return CAT

    async def f_balance(p):
        return 100.0
    real = (panels.services_cached, panels.balance_cached, automation._scores)
    panels.services_cached, panels.balance_cached = f_services, f_balance
    automation._scores = lambda: {(P, "26146", "instagram"): 70, (P, "777", "instagram"): 90, (P, "5227", "facebook"): 80}
    import config
    saved_over = dict(config.DEFAULT_OVERRIDES)
    config.DEFAULT_OVERRIDES.clear()
    try:
        for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns"):
            db.ex(f"DELETE FROM {t}")
        automation._alerts.clear()
        db.ex("DELETE FROM settings WHERE k LIKE 'auto:%'")
        automation.set_settings({"global_daily_cap_usd": 100})
        reset()
        svc = {"instagram": {"views": {"provider": P, "service": "777"}, "likes": {"provider": P, "service": "26999"},
                             "comments": None, "shares": None},
               "facebook": {"views": {"provider": P, "service": "5227"}, "likes": {"provider": P, "service": "22457"}}}
        body = {"name": "Calc", "views_goal": 4_000_000, "start_date": "2026-11-01", "end_date": "2026-11-07",
                "creators_planned": 10, "posts_per_creator": {"instagram": 2, "facebook": 1}, "platforms": ["instagram", "facebook"],
                "platform_split": {"instagram": 60, "facebook": 40}, "services": svc}

        # --- create / read: new fields stored, posts_per_day = sum, margin default 12.5, cap default = estimate x 1.1
        c = await main.auto_campaign_add(body, who="t")
        est = automation.estimate({**body, "margin_pct": 12.5}, {("instagram", "views"): 0.05, ("instagram", "likes"): 0.02,
                                                                 ("facebook", "views"): 0.06, ("facebook", "likes"): 0.1})
        check("v2.12: estimate math (videos, avg/video, cost split, cap default)",
              est["days"] == 7 and est["total_videos"] == 210 and est["views_target"] == 4_500_000
              and est["avg_views_per_video"] == round(4_500_000 / 210)
              and est["cost"]["views"] == round(2_700_000 / 1000 * 0.05 + 1_800_000 / 1000 * 0.06, 2)
              and est["cost"]["likes"] == round(27_000 / 1000 * 0.02 + 18_000 / 1000 * 0.1, 2)
              and est["total_usd"] == round(est["cost"]["views"] + est["cost"]["likes"], 2)
              and est["cap_default_usd"] == round(est["total_usd"] * 1.1, 2)
              and est["per_1k_views_usd"] == round(est["total_usd"] / 4_000_000 * 1000, 4), est)
        check("v2.12: create stores the calculator fields, posts_per_day = sum, margin default 12.5",
              c["posts_per_creator"] == {"instagram": 2, "facebook": 1} and c["posts_per_day"] == 3
              and c["platform_split"] == {"instagram": 60, "facebook": 40} and c["margin_pct"] == 12.5
              and c["services"]["instagram"]["views"] == {"provider": P, "service": "777"} and c["mode"] == "practice", c)
        check("v2.12: cap default on create = estimate x 1.1", c["cap_usd"] == est["cap_default_usd"] and c["cap_usd"] > 0, c)
        c2 = await main.auto_campaign_add({**body, "cap_usd": 12.5}, who="t")
        check("v2.12: a sent cap is kept", c2["cap_usd"] == 12.5)
        c3 = await main.auto_campaign_add({"name": "Bare"}, who="t")
        check("v2.12: no services -> no cap default (cap stays null)", c3["cap_usd"] is None and c3["margin_pct"] == 12.5, c3)
        e = await main.auto_campaign_patch(c["id"], {"posts_per_creator": {"instagram": 1.5}, "margin_pct": 20,
                                                     "platform_split": {"instagram": 100}}, who="t")
        check("v2.12: edit updates per platform posts (posts_per_day follows), margin, split",
              e["posts_per_creator"] == {"instagram": 1.5} and e["posts_per_day"] == 1.5 and e["margin_pct"] == 20
              and e["platform_split"] == {"instagram": 100} and e["ramp_weeks"] == 3, e)
        for bad in ({"platform_split": {"instagram": 50, "facebook": 40}}, {"posts_per_creator": {"myspace": 1}},
                    {"margin_pct": 150}, {"margin_pct": None}, {"cap_usd": -1}, {"cap_usd": float("inf")},
                    {"cap_usd": float("nan")}, {"cap_usd": 1e18}, {"weekly_budget_usd": float("inf")},
                    {"services": {"instagram": {"views": {"provider": "nope", "service": "1"}}}},
                    {"services": {"instagram": {"saves": None}}}):
            try:
                await main.auto_campaign_patch(c["id"], bad, who="t")
                check(f"v2.12: bad field rejected {bad}", False)
            except HTTPException as x:
                check(f"v2.12: bad field rejected {list(bad)}", x.status_code == 400, x.detail)

        # --- Go Live needs the cap, not a weekly budget
        nb = await main.auto_campaign_add({"name": "NoCap", "weekly_budget_usd": 50}, who="t")
        try:
            await main.auto_campaign_patch(nb["id"], {"mode": "live"}, who="t")
            check("v2.12: Go Live without a cap (weekly budget only) -> 400", False)
        except HTTPException as x:
            check("v2.12: Go Live without a cap (weekly budget only) -> 400", x.status_code == 400 and "cap" in x.detail, x.detail)
        lv = await main.auto_campaign_patch(nb["id"], {"mode": "live", "cap_usd": 0.05}, who="t")
        check("v2.12: Go Live with a cap in the same PATCH", lv["mode"] == "live" and lv["cap_usd"] == 0.05)
        try:
            await main.auto_campaign_patch(nb["id"], {"cap_usd": None}, who="t")
            check("v2.12: removing the cap of a live campaign -> 400", False)
        except HTTPException as x:
            check("v2.12: removing the cap of a live campaign -> 400", x.status_code == 400)

        # --- executor stops at the cap + one alert (cap $0.05, each order 1000 views x $0.05/1k = $0.05)
        db.update("auto_campaigns", nb["id"], weekly_budget_usd=None,
                  services=json.dumps({"instagram": {"views": {"provider": P, "service": "777"}}}))
        today = automation.et_day()

        def plan(cid, i, likes=0):
            url = f"https://www.instagram.com/reel/V212x{cid}x{i}"
            automation.insert_plan([{"plan_id": f"{cid}:{i}:{today}", "campaign_id": cid, "creator_id": "cr", "post_url": url,
                                     "platform": "instagram", "window_start": automation.et_midnight(today),
                                     "window_end": automation.et_midnight(today), "views_add": 1000, "likes_add": likes}])
        for i in range(3):
            plan(nb["id"], i)
        await automation.run_tick()
        await automation.run_tick()
        o = db.q("SELECT * FROM auto_orders WHERE campaign_id=? ORDER BY id", (nb["id"],))
        capped = [a for a in automation.alerts(0) if "campaign cap" in a["text"]]
        check("v2.12: chosen views service is used first (777 over the cheaper 26146)",
              o[0]["state"] == "placed" and o[0]["service"] == "777" and o[0]["est_cost"] == 0.05, o)
        check("v2.12: executor stops buying at the campaign cap",
              [x["state"] for x in o] == ["placed", "blocked", "blocked"] and STATE["adds"] == 1
              and all("campaign cap $0.05 reached" in x["error"] for x in o[1:]), o)
        check("v2.12: one cap alert over two ticks", len(capped) == 1, capped)
        v = automation.campaign_view(automation._camp(nb["id"]))
        check("v2.12: campaign view shows total spent against the cap", v["spent_total_usd"] == 0.05, v)

        # --- old campaigns keep working: live + weekly budget, no cap; practice with neither
        old = db.insert("auto_campaigns", name="Old", mode="live", weekly_budget_usd=100, created_at=int(time.time()))
        prac = db.insert("auto_campaigns", name="Practice 1", mode="practice", created_at=int(time.time()))
        plan(old, 0)
        plan(prac, 0, likes=50)
        await automation.run_tick()
        await automation.run_tick()
        oo = db.q("SELECT * FROM auto_orders WHERE campaign_id=? ORDER BY id", (old,))
        po = db.q("SELECT * FROM auto_orders WHERE campaign_id=? ORDER BY id", (prac,))
        check("v2.12: old live campaign (weekly budget, no cap) still places", [x["state"] for x in oo] == ["placed"], oo)
        check("v2.12: old practice campaign (no cap, no budget) still simulates",
              [x["state"] for x in po] == ["simulated", "simulated"] and po[1]["kind"] == "likes"
              and po[1]["service"] == automation.service_for(P, "instagram", "likes"), po)
        db.update("auto_campaigns", prac, services=json.dumps(svc))
        db.ex("DELETE FROM auto_orders WHERE campaign_id=? AND kind='likes'", (prac,))
        await automation.run_tick()
        lk = db.q1("SELECT * FROM auto_orders WHERE campaign_id=? AND kind='likes'", (prac,))
        check("v2.12: chosen likes service is used on the views order's panel", lk and lk["service"] == "26999", lk)
        db.update("auto_campaigns", prac, services=json.dumps({"instagram": {**svc["instagram"],
                                                                     "likes": {"provider": P, "service": "26146"}}}))
        db.ex("DELETE FROM auto_orders WHERE campaign_id=? AND kind='likes'", (prac,))
        await automation.run_tick()
        lk = db.q1("SELECT * FROM auto_orders WHERE campaign_id=? AND kind='likes'", (prac,))
        check("v2.12: chosen 'likes' service that is really a views service falls back to the panel default",
              lk and lk["service"] == automation.service_for(P, "instagram", "likes"), lk)
        try:   # v2.13: a bad date is now a 400 (was: stored, no default cap)
            await main.auto_campaign_add({"name": "Bad date", "start_date": "2026-1-5", "end_date": "2026-01-09",
                                          "views_goal": 1000, "creators_planned": 1,
                                          "posts_per_creator": {"instagram": 1}, "services": svc}, who="t")
            check("v2.12: create with a bad date does not 500 (v2.13: 400)", False)
        except HTTPException as x:
            check("v2.12: create with a bad date does not 500 (v2.13: 400)", x.status_code == 400, x.detail)

        # --- v2.13 coordinator fixes: service pick kind checked on the catalog; ISO dates, start <= end
        for bad, why in (({"instagram": {"likes": {"provider": P, "service": "26146"}}}, "not a likes service for instagram"),
                         ({"instagram": {"views": {"provider": P, "service": "26330"}}}, "not a views service for instagram"),
                         ({"facebook": {"views": {"provider": P, "service": "26146"}}}, "not a views service for facebook"),
                         ({"instagram": {"views": {"provider": P, "service": "424242"}}}, "not in the")):
            for go in (lambda: main.auto_campaign_add({"name": "S", "services": bad}, who="t"),
                       lambda: main.auto_campaign_patch(c["id"], {"services": bad}, who="t")):
                try:
                    await go()
                    check(f"v2.13: wrong-kind service pick rejected {bad}", False)
                except HTTPException as x:
                    check(f"v2.13: wrong-kind service pick rejected ({why})", x.status_code == 400 and why in x.detail,
                          x.detail)
        other = next(p for p in config.PROVIDERS if p != P)
        ok_down = await main.auto_campaign_patch(c["id"], {"services": {"instagram": {
            "views": {"provider": other, "service": "1"}, "likes": {"provider": P, "service": "26999"}}}}, who="t")
        check("v2.13: catalog down = pick accepted; a real likes pick accepted",
              ok_down["services"]["instagram"]["views"]["provider"] == other, ok_down)
        for bad in ({"start_date": "20261105"}, {"end_date": "2026-11-31"}, {"start_date": 5},
                    {"start_date": "2026-11-09", "end_date": "2026-11-08"}):
            try:
                await main.auto_campaign_add({"name": "D", **bad}, who="t")
                check(f"v2.13: bad dates rejected on create {bad}", False)
            except HTTPException as x:
                check(f"v2.13: bad dates rejected on create {bad}", x.status_code == 400, x.detail)
        try:   # c runs 2026-11-01..07: a start after the stored end is refused
            await main.auto_campaign_patch(c["id"], {"start_date": "2026-11-08"}, who="t")
            check("v2.13: patch start after the stored end_date -> 400", False)
        except HTTPException as x:
            check("v2.13: patch start after the stored end_date -> 400", x.status_code == 400 and "before" in x.detail)
        ok_d = await main.auto_campaign_patch(c["id"], {"end_date": "2026-11-10"}, who="t")
        check("v2.13: good patch date accepted", ok_d["end_date"] == "2026-11-10", ok_d)
        db.update("auto_campaigns", c["id"], start_date="2026-11-20")   # an old row stored before v2.13 checks
        e2 = await main.auto_campaign_patch(c["id"], {"cap_usd": 50}, who="t")
        check("v2.13: stored start > end does not block an unrelated edit", e2["cap_usd"] == 50, e2)
        db.update("auto_campaigns", c["id"], start_date="2026-11-01")

        # --- dropdown rows: recommended first (best score), likes on that panel first
        so = await main.auto_services("instagram", "t")
        check("v2.12: /api/auto/services views best score first, recommended flag",
              [x["service"] for x in so["views"]] == ["777", "26146"] and so["views"][0]["recommended"]
              and not so["views"][1]["recommended"] and so["views"][0]["name"] == "Instagram Views Alt"
              and so["likes"][0]["provider"] == P and so["likes"][0]["rate"] == 0.08, so)
        try:
            await main.auto_services("myspace", "t")
            check("v2.12: unknown platform -> 400", False)
        except HTTPException as x:
            check("v2.12: unknown platform -> 400", x.status_code == 400)
    finally:
        panels.services_cached, panels.balance_cached, automation._scores = real
        config.DEFAULT_OVERRIDES.clear()
        config.DEFAULT_OVERRIDES.update(saved_over)


asyncio.run(v212_tests())


async def v213_tests():
    """v2.13 Milan step 2: margin, platform split, buffer days, auto ramp length, removed posts."""
    import datetime as dt
    import automation
    import planner
    import sources
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots",
              "jobs"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%'")
    automation._alerts.clear()
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"

    def day(n):
        return (dt.date.fromisoformat(TODAY) + dt.timedelta(days=n)).isoformat()

    def camp(**kw):
        f = {"name": "M", "mode": "practice", "views_goal": 100000, "start_date": day(-5), "end_date": day(9),
             "creators_planned": 1, "posts_per_day": 1, "ramp_weeks": 3, "approved": 1, "created_at": NOW,
             "platforms": json.dumps(["instagram", "facebook"]), **kw}
        return db.insert("auto_campaigns", **f)
    n = [0]

    def post(cid, d, acct, observed=0, platform="instagram", up=None, **kw):
        n[0] += 1
        url = f"https://www.instagram.com/reel/M{n[0]}/" if platform == "instagram" else \
            f"https://www.facebook.com/reel/{8000000 + n[0]}"
        f = {"post_key": sources.post_key(platform, url), "url_key": campaigns.url_key(url), "campaign_id": cid,
             "creator_id": acct, "account_handle": acct, "platform": platform, "url": url, "source": "sheet",
             "day": d, "uploaded_at": up if up is not None else automation.et_midnight(d) + 12 * 3600,
             "w_random": 1.0, "p_likes": 0.01, "ramp_start": d, "observed_views": observed, "base_views": observed,
             "base_day": d, "observed_likes": 0, "checked_at": NOW, "created_at": automation.et_midnight(d), **kw}
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (db.insert("auto_posts", **f),))

    def G(pid):
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (pid,))
    B = camp(name="baselines", approved=0)
    for i in range(3):   # big account baselines: the 5x cap never binds below
        for acct, plat in (("ig1", "instagram"), ("ig2", "instagram"), ("fb1", "facebook"), ("fb2", "facebook")):
            post(B, day(-40 - i), acct, observed=1_000_000, platform=plat)

    # 1. margin: aim = goal x (1 + margin_pct / 100); null -> 12.5; 0 = the old formula
    A = camp(margin_pct=20, platforms=json.dumps(["instagram"]))
    ps = [post(A, day(-1), f"ig{1 + i % 2}", up=NOW - 3600) for i in range(3)]
    s = planner.plan(automation._camp(A), NOW)
    # rate = 3 posts in 3 days / 3 = 1, days_left 9 -> avg = 120000 / (3 + 9) = 10000
    check("v2.13 #1: margin 20% -> aim 120000, avg = aim / (normal in ramp + expected future)",
          s["views_aim"] == 120000 and s["avg"] == 10000 and G(ps[0]["id"])["target_total"] == 10000, s)
    s = planner.plan({**automation._camp(A), "margin_pct": None}, NOW)
    check("v2.13 #1: margin null -> 12.5% (aim 112500, per-post target up 12.5%)",
          s["views_aim"] == 112500 and s["avg"] == 9375 and G(ps[0]["id"])["target_total"] == 9375, s)
    s = planner.plan({**automation._camp(A), "margin_pct": 0}, NOW)
    check("v2.13 #1: margin 0 = the pre-v2.13 avg (goal / denominator)", s["avg"] == round(100000 / 12), s)
    db.ex("DELETE FROM plan_rows")
    planner.plan(automation._camp(A), NOW)
    r = db.q1("SELECT * FROM plan_rows WHERE campaign_id=? AND url_key=?", (A, ps[0]["url_key"]))
    F2 = (1 + 1.5) / 4.5 * 0.5
    check("v2.13 #1: views_add formula unchanged (target x F(d) - observed)", r["views_add"] == round(10000 * F2), dict(r))

    # 2. platform split: per platform goal = aim x split; each platform's normal posts share it
    S = camp(margin_pct=0, platform_split=json.dumps({"instagram": 75, "facebook": 25}))
    si = [post(S, day(-1), f"ig{1 + i}", up=NOW - 3600) for i in range(2)]
    sf = [post(S, day(-1), f"fb{1 + i}", platform="facebook", up=NOW - 3600) for i in range(2)]
    s = planner.plan(automation._camp(S), NOW)
    # per platform: 2 in ramp + (2/3 x 9) expected = 8 -> IG 75000 / 8, FB 25000 / 8
    check("v2.13 #2: split 75/25 -> avg per platform, targets per platform",
          s["avg_by_platform"] == {"instagram": 9375, "facebook": 3125}
          and G(si[0]["id"])["target_total"] == 9375 and G(sf[0]["id"])["target_total"] == 3125, s)
    db.ex("UPDATE auto_campaigns SET platform_split=NULL WHERE id=?", (S,))
    s = planner.plan(automation._camp(S), NOW)
    check("v2.13 #2: split null -> even over the campaign's platforms",
          s["avg_by_platform"] == {"instagram": 6250, "facebook": 6250}, s)
    E = camp(margin_pct=0)   # platforms ig + fb, null split, nobody posts on fb
    ei = [post(E, day(-1), f"ig{1 + i}", up=NOW - 3600) for i in range(2)]
    s = planner.plan(automation._camp(E), NOW)
    check("v2.13 #2: split null, a platform with no posts takes no share (all goal on the platform used)",
          s["avg_by_platform"] == {"instagram": 12500, "facebook": 0} and G(ei[0]["id"])["target_total"] == 12500, s)
    db.ex("UPDATE auto_posts SET reason='viral', peak=30000, viral_state='approved', viral_base=0 WHERE id=?",
          (si[1]["id"],))
    db.ex("UPDATE auto_campaigns SET platform_split=? WHERE id=?", (json.dumps({"instagram": 75, "facebook": 25}), S))
    s = planner.plan(automation._camp(S), NOW)
    check("v2.13 #2: viral peak comes off its own platform's goal only; viral target = its peak",
          s["avg_by_platform"] == {"instagram": round((75000 - 30000) / 7), "facebook": 3125}
          and G(si[1]["id"])["target_total"] == 30000, s)

    # 3. buffer days: buying runs to end_date + 2, no posts / slots expected in the buffer days
    Bf = camp(margin_pct=0, platforms=json.dumps(["instagram"]), start_date=day(-20), end_date=day(-1))
    for i in range(2):
        post(Bf, day(-2), f"ig{1 + i}", up=NOW - 2 * 86400)
    s = planner.plan(automation._camp(Bf), NOW)
    check("v2.13 #3: buffer day: days_left 0 (no expected posts), goal spread over posts in ramp, rows written",
          s["days_left"] == 0 and s["buy_end"] == day(1) and s["in_ramp"] == 2 and s["avg"] == 50000
          and s["rows"] == 2, s)
    new = planner.ingest(automation._camp(Bf), [{"platform": "instagram", "day": TODAY, "post_key": "instagram:BUF1",
                                                 "url": "https://www.instagram.com/reel/BUF1/", "creator_id": "ig1",
                                                 "account_handle": "ig1", "source": "sheet", "slot": "IG AM 1",
                                                 "uploaded_at": NOW}],
                         [{"creator_id": "ig1", "day": TODAY, "slot": "IG AM 1", "url": None}], {}, NOW)
    check("v2.13 #3: posts and slots dated in buffer days are not taken (no missed slots there)",
          new == 0 and not db.q("SELECT 1 FROM auto_slots WHERE campaign_id=?", (Bf,)), new)
    dsh = planner.dashboard(Bf, NOW)
    check("v2.13 #3: dashboard says buffer_days 2 + buy_end", dsh["buffer_days"] == 2 and dsh["buy_end"] == day(1)
          and dsh["days_left"] == 0, dsh)
    late = await planner.run_campaign(Bf, NOW + 2 * 86400)
    check("v2.13 #3: run after end_date + 2 -> nothing planned",
          any("outside start_date..end_date + buffer days" in e for e in late["errors"]), late)
    real_collect = sources.collect

    async def no_collect(c, d):
        raise RuntimeError("stop here")
    sources.collect = no_collect
    inb = await planner.run_campaign(Bf, NOW)
    sources.collect = real_collect
    check("v2.13 #3: run on a buffer day is not refused", not any("outside" in e for e in inb["errors"])
          and any("stop here" in e for e in inb["errors"]), inb)

    # 4. ramp length = min(21, days to end_date + 2), ramp_weeks ignored
    short = {"ramp_weeks": 1, "end_date": day(3)}
    long_ = {"ramp_weeks": 1, "end_date": day(60)}
    check("v2.13 #4: ramp = min(21 days, days left + 2 buffer); ramp_weeks not read",
          planner.ramp_len({"reason": "base", "ramp_start": TODAY}, short) == 6
          and planner.ramp_len({"reason": "base", "ramp_start": TODAY}, long_) == 21
          and planner.ramp_len({"reason": "decay", "ramp_start": TODAY}, {**long_, "ramp_weeks": 5}) == 21
          and planner.ramp_len({"reason": "viral", "ramp_start": TODAY}, long_) == 10
          and planner.ramp_len({"reason": "viral", "ramp_start": TODAY}, short) == 6)

    # 5. removed posts: deleted / link-down drop out of done_views and achieved; the gap goes to live posts
    R = camp(margin_pct=0, platforms=json.dumps(["instagram"]), start_date=day(-40))
    post(R, day(-30), "ig1", observed=5000)               # ramp over: done
    gone = post(R, day(-30), "ig2", observed=7000)        # ramp over: done, then deleted
    gone2 = post(R, day(-1), "ig1", observed=3000, up=NOW - 3600)   # in ramp, then link down (2 not_found reads)
    rp = post(R, day(-1), "ig2", up=NOW - 3600)
    s0 = planner.plan(automation._camp(R), NOW)
    d0 = planner.dashboard(R, NOW)["achieved"]
    db.ex("UPDATE auto_posts SET status='deleted' WHERE id IN (?, ?)", (gone["id"], gone2["id"]))
    s1 = planner.plan(automation._camp(R), NOW)
    d1 = planner.dashboard(R, NOW)
    check("v2.13 #5: deleted posts out of done_views (were counted before) and out of achieved",
          s0["done_views"] == 12000 and s1["done_views"] == 5000 and d0 == 15000 and d1["achieved"] == 5000
          and d1["posts"]["deleted"] == 2, (s0, s1, d0, d1["achieved"]))
    # before: (100000 - 12000) / (2 in ramp + 2/3 x 9); after: (100000 - 5000) / (1 + 2/3 x 9)
    check("v2.13 #5: the gap is re-spread over live posts at the next run",
          s0["avg"] == 11000 and s1["in_ramp"] == 1 and s1["avg"] == round(95000 / 7)
          and G(rp["id"])["target_total"] == s1["avg"], (s0, s1))


asyncio.run(v213_tests())


async def v214_tests():
    """v2.14 Milan step 3: creator include list, split-gap alert."""
    import datetime as dt
    import automation
    import main
    import planner
    import sources
    from fastapi import HTTPException
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots",
              "auto_creator_map", "jobs"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%'")
    automation._alerts.clear()
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"

    def day(n):
        return (dt.date.fromisoformat(TODAY) + dt.timedelta(days=n)).isoformat()

    def camp(**kw):
        f = {"name": "N", "mode": "practice", "views_goal": 100000, "start_date": day(-5), "end_date": day(9),
             "creators_planned": 1, "posts_per_day": 1, "ramp_weeks": 3, "approved": 1, "created_at": NOW,
             "margin_pct": 0, "platforms": json.dumps(["instagram"]), **kw}
        return db.insert("auto_campaigns", **f)
    n = [0]

    def post(cid, d, acct, observed=0, platform="instagram", up=None, **kw):
        n[0] += 1
        url = f"https://www.instagram.com/reel/N{n[0]}/" if platform == "instagram" else \
            f"https://www.facebook.com/reel/{9000000 + n[0]}"
        f = {"post_key": sources.post_key(platform, url), "url_key": campaigns.url_key(url), "campaign_id": cid,
             "creator_id": acct, "account_handle": acct, "platform": platform, "url": url, "source": "sheet",
             "day": d, "uploaded_at": up if up is not None else automation.et_midnight(d) + 12 * 3600,
             "w_random": 1.0, "p_likes": 0.01, "ramp_start": d, "observed_views": observed, "base_views": observed,
             "base_day": d, "observed_likes": 0, "checked_at": NOW, "created_at": automation.et_midnight(d), **kw}
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (db.insert("auto_posts", **f),))

    def row(cid, p):
        return db.q1("SELECT * FROM plan_rows WHERE campaign_id=? AND url_key=?", (cid, p["url_key"]))

    async def set_(cid, inc):
        return await main.auto_creators_set(cid, {"included": inc}, who="t")
    X = camp()
    post(X, day(-30), "a", observed=5000)   # ramp over: done
    post(X, day(-30), "b", observed=7000)
    pa, pb = post(X, day(-1), "a", up=NOW - 3600), post(X, day(-1), "b", up=NOW - 3600)
    for c in ("a", "b"):
        db.insert("auto_slots", campaign_id=X, creator_id=c, day=day(-1), slot="IG AM 1", url=None)

    # 1. default: no auto_creators row = included; numbers as before
    lst = await main.auto_creators(X, _="t")
    s0, d0 = planner.plan(automation._camp(X), NOW), planner.dashboard(X, NOW)
    check("v2.14 #1: every known creator listed, included by default (no row needed)",
          [(x["creator_id"], x["included"], x["cancelled"], x["posts"]) for x in lst["items"]]
          == [("a", True, False, 2), ("b", True, False, 2)], lst)
    check("v2.14 #1: default = old numbers (both creators' views, rows, expected posts, missed slots)",
          s0["done_views"] == 12000 and s0["in_ramp"] == 2 and s0["rate"] == 0.67 and row(X, pb) is not None
          and d0["achieved"] == 12000 and d0["creators"]["excluded"] == [] and len(d0["missed_slots"]) == 2, (s0, d0))

    # 2. un-tick b: no ingest, no count checks, no rows, no orders, views / expected posts / slots out; audited
    r = await set_(X, {"b": False})
    au = db.q1("SELECT * FROM audit_log WHERE action='auto.creators.set' ORDER BY id DESC LIMIT 1")
    check("v2.14 #2: bulk set returns the list + changed; one audit row with excluded",
          r["changed"] == {"b": False} and [x["included"] for x in r["items"]] == [True, False]
          and au["target"] == str(X) and json.loads(au["detail"]) == {"included": [], "excluded": ["b"]}, (r, dict(au)))
    db.ex("DELETE FROM plan_rows")
    s1, d1 = planner.plan(automation._camp(X), NOW), planner.dashboard(X, NOW)
    check("v2.14 #2: un-ticked: no plan row, out of done_views / achieved / expected posts / missed slots / slacking",
          s1["done_views"] == 5000 and s1["in_ramp"] == 1 and s1["rate"] == 0.33 and row(X, pb) is None
          and row(X, pa) is not None and d1["achieved"] == 5000 and d1["creators"]["excluded"] == ["b"]
          and [m["creator_id"] for m in d1["missed_slots"]] == ["a"] and [x["creator_id"] for x in d1["slacking"]] == ["a"],
          (s1, d1))
    lk = [{"platform": "instagram", "day": TODAY, "post_key": f"instagram:NEW{c}", "url": f"https://www.instagram.com/reel/NEW{c}/",
           "creator_id": c, "account_handle": c, "source": "sheet", "slot": None, "uploaded_at": NOW} for c in ("a", "b")]
    check("v2.14 #2: ingest skips the un-ticked creator's new posts", planner.ingest(automation._camp(X), lk, [], {}, NOW) == 1
          and db.q1("SELECT 1 FROM auto_posts WHERE post_key='instagram:NEWb'") is None)
    db.ex("DELETE FROM auto_posts WHERE post_key='instagram:NEWa'")
    db.ex("UPDATE auto_posts SET checked_at=NULL WHERE id IN (?, ?)", (pa["id"], pb["id"]))
    sel, held = planner.pick_scrapes(automation._camp(X), NOW)
    db.ex("UPDATE auto_posts SET checked_at=? WHERE id IN (?, ?)", (NOW, pa["id"], pb["id"]))
    check("v2.14 #2: no count check (post fetch) for the un-ticked creator, even never-checked",
          [p["id"] for p in sel] == [pa["id"]] and not held, sel)
    pz = automation._pause_set()
    check("v2.14 #2: executor skips the un-ticked creator's existing rows (this campaign only)",
          automation._paused(pz, {"campaign_id": X, "url_key": "k", "creator_id": "B "})
          and not automation._paused(pz, {"campaign_id": X, "url_key": "k", "creator_id": "a"})
          and not automation._paused(pz, {"campaign_id": X + 1000, "url_key": "k", "creator_id": "b"}))

    # 3. re-tick: back in, its existing posts count again
    r = await set_(X, {"b": True, "a": True})
    s2, d2 = planner.plan(automation._camp(X), NOW), planner.dashboard(X, NOW)
    check("v2.14 #3: re-tick restores views, rows, expected posts; only real changes reported",
          r["changed"] == {"b": True} and s2["done_views"] == 12000 and s2["in_ramp"] == 2 and row(X, pb) is not None
          and d2["achieved"] == 12000 and d2["creators"]["excluded"] == [] and len(d2["missed_slots"]) == 2, (r, s2))

    # 4. cancelled (sheet) creators' views still count (only un-ticked ones drop out)
    db.ex("UPDATE auto_creators SET cancelled=1 WHERE campaign_id=? AND creator_id='b'", (X,))
    s3, d3 = planner.plan(automation._camp(X), NOW), planner.dashboard(X, NOW)
    lst = await main.auto_creators(X, _="t")
    check("v2.14 #4: cancelled creator's views still in done_views and achieved; still listed as cancelled + included",
          s3["done_views"] == 12000 and d3["achieved"] == 12000 and d3["creators"]["cancelled"] == ["b"]
          and [(x["included"], x["cancelled"]) for x in lst["items"]] == [(True, False), (True, True)], (s3, d3, lst))

    # 5. bad bodies / unknown creator / unknown campaign
    errs = []
    for cid, body in ((X, {"included": {"zzz": False}}), (X, {"included": {"a": "no"}}), (X, {}), (X, {"included": []})):
        try:
            await main.auto_creators_set(cid, body, who="t")
            errs.append(None)
        except HTTPException as e:
            errs.append(e.status_code)
    try:
        await main.auto_creators(99999, _="t")
        errs.append(None)
    except HTTPException as e:
        errs.append(e.status_code)
    check("v2.14 #5: unknown creator / non-bool / bad body = 400, unknown campaign = 404", errs == [400, 400, 400, 400, 404],
          errs)

    # 6. an un-tick stored under a sheet name survives the name -> SideShift id merge
    M = camp(name="merge")
    db.insert("auto_creators", campaign_id=M, creator_id="Sam", cancelled=0, included=0)
    db.insert("auto_creators", campaign_id=M, creator_id="ss_1", cancelled=1)
    planner.merge_creators(M, {"links": [{"source": "sideshift", "creator_id": "ss_1", "name": "Sam",
                                          "post_key": "instagram:MQ1", "account_handle": "sam", "uploaded_at": 1},
                                         {"source": "sheet", "creator_id": "Sam", "post_key": "instagram:MQ2",
                                          "account_handle": "Sam", "uploaded_at": 1}], "slots": [], "cancelled": {}})
    check("v2.14 #6: un-tick under a sheet name moves to the canonical SideShift id",
          [dict(x) for x in db.q("SELECT creator_id, cancelled, included FROM auto_creators WHERE campaign_id=?", (M,))]
          == [{"creator_id": "ss_1", "cancelled": 1, "included": 0}])

    # 7. split gap: a share on a platform with no posts -> run summary + one alert per campaign per day, not redistributed
    SG = camp(name="SG", platforms=json.dumps(["instagram", "facebook"]),
              platform_split=json.dumps({"instagram": 50, "facebook": 50}))
    for i in range(2):
        post(SG, day(-1), f"g{i}", up=NOW - 3600)

    def gap_alerts():
        return [t for t in automation._alerts if t.startswith("Automation: SG:") and "Platform split" in t]
    s = planner.plan(automation._camp(SG), NOW)
    check("v2.14 #7: split gap in the plan result; facebook share not moved to instagram",
          s["split_gap"] == {"facebook": 50000} and s["avg_by_platform"]["instagram"] == round(50000 / (2 + 2 / 3 * 9))
          and gap_alerts() == [f"Automation: SG: {TODAY}: Platform split: no posts on facebook (50,000 views not bought). "
                               "Options: change the platform split, get posts on that platform, accept the gap."], (s, gap_alerts()))
    post(SG, day(-30), "g9", observed=1000, platform="facebook")   # gap now 49,000: new text, same day
    s = planner.plan(automation._camp(SG), NOW + 3600)
    check("v2.14 #7: same day, new numbers -> no second alert", s["split_gap"] == {"facebook": 49000}
          and len(gap_alerts()) == 1, gap_alerts())
    planner._save_run(SG, s)
    check("v2.14 #7: dashboard alert from the last run summary",
          any("no posts on facebook (49,000" in a["text"] for a in planner.dashboard(SG, NOW)["alerts"]))
    planner.plan(automation._camp(SG), NOW + 86400)
    check("v2.14 #7: next day -> one new alert", len(gap_alerts()) == 2, gap_alerts())
    end = db.q1("SELECT end_date FROM auto_campaigns WHERE id=?", (SG,))["end_date"]
    db.ex("UPDATE auto_campaigns SET end_date=? WHERE id=?", (TODAY, SG))
    check("v2.14 #7: no split gap on the last posting day / buffer days (no posts expected)",
          planner.plan(automation._camp(SG), NOW)["split_gap"] == {})
    db.ex("UPDATE auto_campaigns SET end_date=? WHERE id=?", (end, SG))
    E = camp(name="E0", platform_split=json.dumps({"instagram": 50, "facebook": 50}),
             platforms=json.dumps(["instagram", "facebook"]))
    check("v2.14 #7: no split gap before any post is taken in", planner.plan(automation._camp(E), NOW)["split_gap"] == {})
    db.ex("UPDATE auto_campaigns SET platform_split=NULL WHERE id=?", (SG,))
    check("v2.14 #7: split null -> no gap (even over platforms with posts)",
          planner.plan(automation._camp(SG), NOW)["split_gap"] == {})


asyncio.run(v214_tests())

async def v215_tests():
    """v2.15 Milan step 4: dashboard detail (per day / creator / video / slot / spend), forecast + Go Live freeze,
    orders platform filter + spent total."""
    import datetime as dt
    import automation
    import main
    import planner
    import sources
    from fastapi import HTTPException
    ET = automation.ET
    for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots",
              "auto_creator_map", "jobs"):
        db.ex(f"DELETE FROM {t}")
    db.ex("DELETE FROM settings WHERE k LIKE 'planner:%'")
    NOW = int(dt.datetime(2026, 10, 20, 13, 5, tzinfo=ET).timestamp())
    TODAY = "2026-10-20"

    def day(n):
        return (dt.date.fromisoformat(TODAY) + dt.timedelta(days=n)).isoformat()
    X = db.insert("auto_campaigns", name="D", mode="practice", views_goal=60000, start_date=day(-2), end_date=day(1),
                  creators_planned=3, posts_per_day=2, approved=1, created_at=NOW, margin_pct=0, cap_usd=50,
                  platforms=json.dumps(["instagram", "facebook"]),
                  posts_per_creator=json.dumps({"instagram": 1, "facebook": 1}))
    n = [0]

    def post(d, acct, observed, platform="instagram", **kw):
        n[0] += 1
        url = f"https://www.instagram.com/reel/V{n[0]}/" if platform == "instagram" else \
            f"https://www.facebook.com/reel/{8000000 + n[0]}"
        f = {"post_key": sources.post_key(platform, url), "url_key": campaigns.url_key(url), "campaign_id": X,
             "creator_id": acct, "account_handle": acct, "platform": platform, "url": url, "source": "sheet",
             "day": d, "uploaded_at": automation.et_midnight(d) + 12 * 3600, "w_random": 1.0, "p_likes": 0.01,
             "ramp_start": d, "observed_views": observed, "observed_likes": observed // 100, "checked_at": NOW,
             "created_at": NOW, **kw}
        return db.q1("SELECT * FROM auto_posts WHERE id=?", (db.insert("auto_posts", **f),))
    a1 = post(day(-2), "a", 1000, target_total=3000)
    a2 = post(day(-2), "a", 500, platform="facebook", status="deleted")
    a3 = post(day(-1), "a", 2000)
    b1 = post(day(-2), "b", 4000)
    post(day(-2), "c", 9999)
    db.insert("auto_creators", campaign_id=X, creator_id="b", cancelled=1)
    db.insert("auto_creators", campaign_id=X, creator_id="c", cancelled=0, included=0)
    db.insert("plan_rows", plan_id=f"{X}:a1", campaign_id=X, creator_id="a", post_url=a1["url"], url_key=a1["url_key"],
              platform="instagram", window_start=0, window_end=0, day=day(-1), created_at=NOW)
    for st, cost in (("simulated", 1.5), ("placed", 9.0), ("blocked", 99.0)):
        db.insert("auto_orders", plan_id=f"{X}:a1", campaign_id=X, url_key=a1["url_key"], day=day(-1), kind="views",
                  qty=1000, est_cost=cost, state=st, created_at=NOW)

    d = planner.detail(X, NOW)
    tl = {r["creator_id"]: r for r in d["timeline"]["rows"]}
    S = lambda c, dd, pl: [q["s"] for q in tl[c]["cells"][day(dd)].get(pl, [])]  # noqa: E731
    check("v2.15 #1: KPI: achieved = live posts of ticked creators (cancelled count, deleted / un-ticked not)",
          d["kpi"]["achieved"] == 7000 and d["kpi"]["pending"] == 53000 and d["kpi"]["posting_days_left"] == 1
          and d["kpi"]["avg_views"] == round(7000 / 3) and d["kpi"]["avg_needed"] == 2500, d["kpi"])
    check("v2.15 #1: setup: days + buffer, total videos, active creators (ticked, not cancelled)",
          d["days"] == [day(i) for i in range(-2, 4)] and d["posting_days"] == 4 and d["buffer_days"] == 2
          and d["setup"]["total_videos"] == 24 and d["setup"]["creators_active"] == 1
          and d["setup"]["posts_per_creator"] == {"instagram": 1, "facebook": 1}, d["setup"])
    check("v2.15 #2: timeline rows = ticked creators; states posted / link down / missed / pending / cancelled / buffer",
          sorted(tl) == ["a", "b"] and S("a", -2, "instagram") == ["posted"] and S("a", -2, "facebook") == ["down"]
          and S("a", -1, "facebook") == ["missed"] and S("a", 0, "instagram") == ["pending"]
          and S("a", 2, "facebook") == ["buffer"] and S("b", -2, "instagram") == ["posted"]
          and S("b", -2, "facebook") == ["cancelled"] and S("b", 1, "instagram") == ["cancelled"]
          and tl["a"]["cells"][day(-2)]["instagram"][0]["id"] == a1["id"] and tl["a"]["issues"] and tl["b"]["issues"]
          and d["timeline"]["counts"] == {"posted": 3, "pending": 4, "missed": 1, "down": 1, "cancelled": 7, "buffer": 8},
          d["timeline"])
    pd = {x["day"]: x for x in d["per_day"]}
    check("v2.15 #3: per day actual by upload day + platform; buffer days flagged; flat target before Go Live",
          pd[day(-2)]["actual"] == {"facebook": 0, "instagram": 5000} and pd[day(-1)]["actual"]["instagram"] == 2000
          and pd[day(2)]["buffer"] and not pd[day(1)]["buffer"] and pd[day(0)]["target"] == 15000
          and pd[day(2)]["target"] == 0, d["per_day"])
    v = {x["id"]: x for x in d["videos"]}
    check("v2.15 #4: forecast (no run yet): target_total, else aim x split / expected videos of active creators (v2.16); future slots filled",
          d["forecast"]["avg_by_platform"] == {"instagram": 7500, "facebook": 7500} and v[a1["id"]]["forecast"] == 3000
          and v[a3["id"]]["forecast"] == 7500 and v[b1["id"]]["forecast"] == 7500 and v[a2["id"]]["forecast"] is None
          and pd[day(-2)]["forecast"] == {"instagram": 10500, "facebook": 0}
          and pd[day(0)]["forecast"] == {"instagram": 7500, "facebook": 7500} and pd[day(-1)]["forecast"]["facebook"] == 0
          and pd[day(2)]["forecast"] == {"instagram": 0, "facebook": 0} and d["forecast"]["frozen_at"] is None, (d["forecast"], pd))
    check("v2.15 #4: videos: un-ticked creator out; deleted listed with its status; comments / shares blank; spend",
          len(d["videos"]) == 4 and v[a2["id"]]["status"] == "deleted" and v[a1["id"]]["likes"] == 10
          and v[a1["id"]]["comments"] is None and v[a1["id"]]["shares"] is None and v[a1["id"]]["spend"] == 1.5)
    planner._save_run(X, {"at": NOW, "avg_by_platform": {"instagram": 4000}})
    pd2 = {x["day"]: x for x in planner.detail(X, NOW)["per_day"]}
    check("v2.15 #4: forecast follows the last run's avg_by_platform (recomputed each run)",
          pd2[day(0)]["forecast"] == {"instagram": 4000, "facebook": 7500}, pd2[day(0)])
    sp = d["spend"]
    check("v2.15 #5: spend in practice = simulated orders, cap left = cap minus real spend; projected, per 1k, by day / platform / creator",
          sp["simulated"] and sp["spent"] == 1.5 and sp["cap_left"] == 41.0 and sp["projected"] == 81.0
          and sp["videos_paid"] == 1 and sp["avg_per_video"] == 1.5 and sp["per_1k_views"] == round(1.5 / 7, 4)
          and [x["spent"] for x in sp["by_day"]][:2] == [0, 1.5] and sp["by_day"][0]["scheduled"] == 0
          and sp["by_day"][2]["scheduled"] == round(79.5 / 4, 2)
          and sp["by_platform"]["instagram"]["spent"] == 1.5 and sp["by_platform"]["facebook"]["per_1k_views"] is None
          and {c["creator_id"]: c["spend"] for c in d["creators"]} == {"a": 1.5, "b": 0, "c": 0}, sp)
    cr = {c["creator_id"]: c for c in d["creators"]}
    check("v2.15 #5: creators: videos / views / avg (deleted out) + include flags",
          (cr["a"]["videos"], cr["a"]["views"], cr["a"]["avg_views"]) == (2, 3000, 1500) and cr["b"]["cancelled"]
          and not cr["c"]["included"] and cr["c"]["videos"] == 0, cr)

    # 6. Go Live freezes the forecast once (first Go Live wins); target line = frozen; live spend = real orders
    await main.auto_campaign_patch(X, {"mode": "live"}, who="t")
    fz = planner.frozen(X)
    planner._save_run(X, {"at": NOW, "avg_by_platform": {"instagram": 100, "facebook": 100}})
    await main.auto_campaign_patch(X, {"mode": "practice"}, who="t")
    await main.auto_campaign_patch(X, {"mode": "live"}, who="t")
    d = planner.detail(X, NOW)
    pd = {x["day"]: x for x in d["per_day"]}
    check("v2.15 #6: Go Live keeps a frozen copy; a second Go Live does not overwrite it",
          fz and planner.frozen(X) == fz and fz["avg_by_platform"]["instagram"] == 4000
          and d["forecast"]["frozen_at"] == fz["at"] and pd[day(-2)]["target"] == 7000 == sum(fz["days"][day(-2)].values())
          and pd[day(0)]["forecast"] == {"instagram": 100, "facebook": 100}
          and {x["id"]: x["forecast_frozen"] for x in d["videos"]}[a1["id"]] == 3000, (fz, pd[day(0)]))
    check("v2.15 #6: live campaign: spend = real orders (simulated / blocked out)",
          not d["spend"]["simulated"] and d["spend"]["spent"] == 9.0, d["spend"])
    db.ex("UPDATE auto_campaigns SET mode='practice' WHERE id=?", (X,))
    db.ex("DELETE FROM auto_orders WHERE state<>'simulated'")
    planner.reset(X)
    check("v2.15 #6: practice reset clears the frozen copy", planner.frozen(X) is None)

    # 7. orders: platform filter + spent total of the filtered set
    for st, cost, pl in (("simulated", 1.5, "instagram"), ("placed", 9.0, "instagram"), ("completed", 2.0, "facebook")):
        db.insert("plan_rows", plan_id=f"{X}:{st}", campaign_id=X, creator_id="a", post_url="u", url_key=f"k{st}",
                  platform=pl, window_start=0, window_end=0, day=TODAY, created_at=NOW)
        db.insert("auto_orders", plan_id=f"{X}:{st}", campaign_id=X, url_key=f"k{st}", day=TODAY, kind="views",
                  qty=1000, est_cost=cost, state=st, job_id=1, created_at=NOW)
    o = lambda **kw: main.auto_orders(campaign_id=X, creator_id=None, post=None, state=None, kind=None, page=1, _="t", **kw)  # noqa: E731
    r_all, r_ig, r_fb = await o(), await o(platform="instagram"), await o(platform="facebook")
    check("v2.15 #7: orders platform filter; spent_usd / simulated_usd over the whole filtered set",
          (r_all["total"], r_all["spent_usd"], r_all["simulated_usd"]) == (3, 11.0, 1.5)
          and (r_ig["total"], r_ig["spent_usd"], r_ig["simulated_usd"]) == (2, 9.0, 1.5)
          and (r_fb["total"], r_fb["spent_usd"]) == (1, 2.0), (r_all, r_ig, r_fb))
    try:
        await main.auto_detail(99999, _="t")
        nf = None
    except HTTPException as e:
        nf = e.status_code
    E = db.insert("auto_campaigns", name="E", mode="practice", approved=0, created_at=NOW)
    e = planner.detail(E, NOW)
    check("v2.15 #8: unknown campaign = 404; campaign without dates / goal = empty detail, no error",
          nf == 404 and e["days"] == [] and e["timeline"]["rows"] == [] and e["kpi"]["avg_needed"] is None, e)


asyncio.run(v215_tests())


async def v216_tests():
    """v2.16 Milan step 5: comments + shares as kinds (service pick + ratio per 1k views, both needed to buy)."""
    import datetime as dt
    import apify_client
    import automation
    import config
    import main
    import planner
    import sources
    from fastapi import HTTPException
    CAT = [{"service": 26146, "name": "Instagram Views", "rate": "0.04", "min": "100", "max": "1000000"},
           {"service": 26330, "name": "Instagram Likes", "rate": "0.08", "min": "20", "max": "100000"},
           {"service": 900, "name": "Instagram Comments Random", "rate": "2.0", "min": "10", "max": "5000", "type": "Default"},
           {"service": 901, "name": "Instagram Custom Comments", "rate": "1.5", "min": "10", "max": "5000",
            "type": "Custom Comments"},
           {"service": 902, "name": "Instagram Shares", "rate": "0.5", "min": "10", "max": "50000"},
           {"service": 903, "name": "Instagram Comment Likes", "rate": "0.3", "min": "10", "max": "5000"},
           {"service": 904, "name": "Facebook Shares", "rate": "0.4", "min": "10", "max": "50000"},
           {"service": 905, "name": "Instagram Shares Package", "rate": "0.2", "min": "10", "max": "50000", "type": "Package"}]

    async def f_services(p):
        if p != P:
            raise panels.PanelUnavailable("no key")
        return CAT

    async def f_balance(p):
        return 100.0
    real = (panels.services_cached, panels.balance_cached, automation._scores)
    panels.services_cached, panels.balance_cached = f_services, f_balance
    automation._scores = lambda: {}
    saved_over = dict(config.DEFAULT_OVERRIDES)
    config.DEFAULT_OVERRIDES.clear()
    S = lambda sid: {"provider": P, "service": sid}  # noqa: E731
    try:
        for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots",
                  "auto_creator_map", "jobs"):
            db.ex(f"DELETE FROM {t}")
        db.ex("DELETE FROM settings WHERE k LIKE 'planner:%' OR k LIKE 'auto:%'")
        automation._alerts.clear()
        automation.set_settings({"global_daily_cap_usd": 1000})
        reset()

        # --- 1. service picks: kind + quantity-only checked on the catalog
        c = await main.auto_campaign_add({"name": "CS"}, who="t")
        check("v2.16 #1: new campaign: comments / shares ratios default 0",
              c["comments_per_1k"] == 0 and c["shares_per_1k"] == 0, c)
        for bad, why in (({"instagram": {"comments": S("903")}}, "no followers, visits, Threads, likes"),
                         ({"instagram": {"comments": S("26146")}}, "not a comments service for instagram"),
                         ({"instagram": {"shares": S("904")}}, "not a shares service for instagram"),
                         ({"instagram": {"shares": S("905")}}, "only quantity-only"),
                         ({"instagram": {"shares": S("424242")}}, "not in the")):
            try:
                await main.auto_campaign_patch(c["id"], {"services": bad}, who="t")
                check(f"v2.16 #1: bad comments / shares pick rejected {bad}", False)
            except HTTPException as x:
                check(f"v2.16 #1: bad comments / shares pick rejected ({why})", x.status_code == 400 and why in x.detail,
                      x.detail)
        ok = await main.auto_campaign_patch(c["id"], {"services": {"instagram": {"comments": S("900"), "shares": S("902")}},
                                                      "comments_per_1k": 5, "shares_per_1k": 2.5}, who="t")
        check("v2.16 #1: quantity-only comments / shares picks + ratios accepted",
              ok["services"]["instagram"]["comments"] == S("900") and ok["comments_per_1k"] == 5
              and ok["shares_per_1k"] == 2.5, ok)
        for bad in ({"comments_per_1k": -1}, {"comments_per_1k": None}, {"shares_per_1k": "5"}, {"shares_per_1k": 5000}):
            try:
                await main.auto_campaign_patch(c["id"], bad, who="t")
                check(f"v2.16 #1: bad ratio rejected {bad}", False)
            except HTTPException as x:
                check(f"v2.16 #1: bad ratio rejected {bad}", x.status_code == 400, x.detail)

        # --- 2. dropdown rows: quantity-only, right platform + kind, cheapest first, nothing preset
        so = await main.auto_services("instagram", "t")
        check("v2.16 #2: /api/auto/services lists comments + shares (custom / package / comment likes / other platform out)",
              [x["service"] for x in so["comments"]] == ["901", "900"] and [x["service"] for x in so["shares"]] == ["902"]
              and not any(x["recommended"] for x in so["comments"] + so["shares"])
              and [x["custom"] for x in so["comments"]] == [True, False]   # v2.18: Custom Comments listed for comments
              and so["comments"][1]["rate"] == 2.0 and [x["service"] for x in so["views"]] == ["26146"], so)

        # --- 3. estimate: comments / shares priced only when ratio AND rate are set
        body = {"views_goal": 1_000_000, "start_date": "2026-11-01", "end_date": "2026-11-10", "creators_planned": 10,
                "posts_per_creator": {"instagram": 1}, "margin_pct": 0}
        r = {("instagram", "views"): 0.04, ("instagram", "likes"): 0.08}
        e0 = automation.estimate(body, r)
        e1 = automation.estimate({**body, "comments_per_1k": 5, "shares_per_1k": 2}, r)   # ratio, no service
        e2 = automation.estimate({**body, "comments_per_1k": 5, "shares_per_1k": 2},
                                 {**r, ("instagram", "comments"): 2.0, ("instagram", "shares"): 0.5})
        e3 = automation.estimate(body, {**r, ("instagram", "comments"): 2.0})                  # service, ratio 0
        check("v2.16 #3: estimate: nothing set = same total as before; ratio x rate added when both are set",
              e0["total_usd"] == e1["total_usd"] == e3["total_usd"] == round(40 + 0.8, 2)
              and e0["cost"]["comments"] == e0["cost"]["shares"] == 0
              and e2["cost"]["comments"] == 10.0 and e2["cost"]["shares"] == 1.0
              and e2["total_usd"] == round(40.8 + 11, 2) and e2["cap_default_usd"] == round(e2["total_usd"] * 1.1, 2),
              (e0, e2))
        ef = await automation.estimate_for({**body, "comments_per_1k": 5, "services": {"instagram": {
            "views": S("26146"), "likes": S("26330"), "comments": S("900")}}})
        check("v2.16 #3: estimate_for prices the chosen comments service (cap default follows)",
              ef["cost"]["comments"] == 10.0 and ef["total_usd"] == 50.8, ef)

        # --- 4. HARD RULE: planner + executor on a campaign without both settings buy no comments / shares,
        #        and views / likes orders are identical with or without the new settings
        NOW = int(time.time())
        TODAY = automation.et_day(NOW)

        def day(n):
            return (dt.date.fromisoformat(TODAY) + dt.timedelta(days=n)).isoformat()
        base_svc = {"instagram": {"views": S("26146"), "likes": S("26330")}}

        def run_case(**camp_kw):
            for t in ("plan_rows", "auto_orders", "auto_campaigns", "auto_posts", "jobs"):
                db.ex(f"DELETE FROM {t}")
            reset()
            cid = db.insert("auto_campaigns", name="H", mode="practice", views_goal=200000, start_date=day(-2),
                            end_date=day(5), creators_planned=1, posts_per_day=1, approved=1, margin_pct=0,
                            platforms=json.dumps(["instagram"]), posts_per_creator=json.dumps({"instagram": 1}),
                            created_at=NOW, **{"services": json.dumps(base_svc), **camp_kw})
            for i, (d, v) in enumerate(((-2, 3000), (-1, 2000))):
                url = f"https://www.instagram.com/reel/H216x{i}/"
                db.insert("auto_posts", post_key=sources.post_key("instagram", url), url_key=campaigns.url_key(url),
                          campaign_id=cid, creator_id="a", account_handle="a", platform="instagram", url=url,
                          source="sheet", day=day(d), uploaded_at=automation.et_midnight(day(d)) + 12 * 3600,
                          w_random=1.0, p_likes=0.01, ramp_start=day(d), observed_views=v, base_views=v, base_day=day(d),
                          observed_likes=v // 200, checked_at=NOW, created_at=automation.et_midnight(day(d)))
            planner.plan(automation._camp(cid), NOW)
            return cid
        sig = lambda cid: sorted((o["kind"], o["url_key"], o["service"], o["qty"], o["est_cost"], o["state"])  # noqa: E731
                                 for o in db.q("SELECT * FROM auto_orders WHERE campaign_id=?", (cid,)))
        rows = lambda cid: db.q("SELECT * FROM plan_rows WHERE campaign_id=? ORDER BY plan_id", (cid,))  # noqa: E731
        results = {}
        for name, kw in (("none", {}),
                         ("service_only", {"services": json.dumps({"instagram": {**base_svc["instagram"],
                                                                                 "comments": S("900"), "shares": S("902")}})}),
                         ("ratio_only", {"comments_per_1k": 50, "shares_per_1k": 20}),
                         ("both", {"services": json.dumps({"instagram": {**base_svc["instagram"],
                                                                         "comments": S("900"), "shares": S("902")}}),
                                   "comments_per_1k": 300, "shares_per_1k": 150})):
            cid = run_case(**kw)
            await automation.run_tick()
            await automation.run_tick()
            results[name] = (cid, sig(cid), rows(cid))
        vl = lambda name: [x[:1] + x[2:] for x in results[name][1] if x[0] in ("views", "likes")]  # noqa: E731
        ex_ = lambda name: [x for x in results[name][1] if x[0] in ("comments", "shares")]  # noqa: E731
        check("v2.16 #4: baseline plans + simulates views and likes",
              any(x[0] == "views" for x in results["none"][1]) and any(x[0] == "likes" for x in results["none"][1])
              and all(r_["views_add"] > 0 for r_ in results["none"][2]), results["none"][1])
        for name in ("none", "service_only", "ratio_only"):
            check(f"v2.16 #4 HARD RULE ({name}): zero comments / shares planned or ordered",
                  not ex_(name) and all(r_["comments_add"] == 0 and r_["shares_add"] == 0 for r_ in results[name][2]),
                  (results[name][1], [dict(r_) for r_ in results[name][2]]))
        check("v2.16 #4: views / likes orders identical with or without comments / shares settings",
              vl("none") == vl("service_only") == vl("ratio_only") == vl("both"),
              (vl("none"), vl("both")))
        cid_b, _, rows_b = results["both"]
        check("v2.16 #4: service AND ratio set: plan rows carry comments / shares = ratio per 1k bought views",
              all(r_["comments_add"] == round(r_["views_add"] * 300 / 1000) and r_["shares_add"] == round(r_["views_add"] * 150 / 1000)
                  for r_ in rows_b) and any(r_["comments_add"] > 0 for r_ in rows_b), [dict(r_) for r_ in rows_b])
        cs = ex_("both")
        check("v2.16 #4: service AND ratio set: comments / shares simulated on the chosen service, qty = plan (never raised)",
              sorted({(x[0], x[2]) for x in cs}) == [("comments", "900"), ("shares", "902")]
              and all(x[5] == "simulated" and x[3] >= 10 for x in cs)
              and all(x[4] == round(x[3] / 1000 * (2.0 if x[0] == "comments" else 0.5), 4) for x in cs), (cs, [(r_["comments_add"], r_["shares_add"]) for r_ in rows_b]))
        f = planner.order_facts(TODAY, int(time.time()))
        lk = sum(x[3] for x in results["both"][1] if x[0] == "likes")
        check("v2.16 #4: simulated comments / shares are not counted as likes by the planner",
              sum(v["sim_likes"] for v in f.values()) == lk, (f, lk))
        sp = planner.detail(cid_b, NOW)["spend"]["spent"]
        check("v2.16 #4: spend includes comments / shares", sp == round(sum(x[4] for x in results["both"][1]), 2), sp)

        # --- 5. executor gate holds even when a plan row asks for comments / shares (external plan API)
        for t in ("plan_rows", "auto_orders", "auto_campaigns", "auto_posts", "jobs"):
            db.ex(f"DELETE FROM {t}")
        reset()
        cid_s = run_case(services=json.dumps({"instagram": {**base_svc["instagram"], "comments": S("900"), "shares": S("902")}}),
                         comments_per_1k=50, shares_per_1k=20)
        await automation.run_tick()
        await automation.run_tick()
        check("v2.16 review: below the service minimum -> not bought, never raised, no blocked rows",
              not [x for x in sig(cid_s) if x[0] in ("comments", "shares")], sig(cid_s))
        lv = db.insert("auto_campaigns", name="L", mode="live", cap_usd=100, created_at=NOW, services=json.dumps(base_svc))
        url = "https://www.instagram.com/reel/G216x1/"
        automation.insert_plan([{"plan_id": f"{lv}:g:{TODAY}", "campaign_id": lv, "creator_id": "a", "post_url": url,
                                 "platform": "instagram", "window_start": automation.et_midnight(TODAY),
                                 "window_end": automation.et_midnight(TODAY), "views_add": 1000, "likes_add": 0,
                                 "comments_add": 500, "shares_add": 500}])
        await automation.run_tick()
        db.ex("UPDATE auto_orders SET state='completed' WHERE campaign_id=? AND kind='views'", (lv,))
        await automation.run_tick()
        await automation.run_tick()
        got = db.q("SELECT kind, state FROM auto_orders WHERE campaign_id=?", (lv,))
        check("v2.16 #5 HARD RULE (live): plan row with comments / shares but no service + ratio: none placed",
              got == [{"kind": "views", "state": "completed"}] and [s_ for s_, _ in STATE["add_log"]] == ["26146"],
              (got, STATE["add_log"]))
        got, why = await automation.pick(automation._plan(f"{lv}:g:{TODAY}"), "comments", 100, await automation.context(),
                                         True, camp=automation._camp(lv))
        check("v2.16 #5: pick() refuses comments without service + ratio", got is None and "no comments service" in why, why)
        # a custom comments service stored behind the API's back is still refused at order time
        db.update("auto_campaigns", lv, comments_per_1k=10,
                  services=json.dumps({"instagram": {**base_svc["instagram"], "comments": S("901")}}))
        await automation.run_tick()
        o = db.q1("SELECT * FROM auto_orders WHERE campaign_id=? AND kind='comments'", (lv,))
        check("v2.16 #5 (v2.18): custom comments service with an empty comment list: blocked, nothing sent",
              o and o["state"] == "blocked" and "comment list is empty" in o["error"] and len(STATE["add_log"]) == 1, (o, STATE["add_log"]))
        db.update("auto_campaigns", lv, services=json.dumps({"instagram": {**base_svc["instagram"], "comments": S("900")}}))
        await automation.run_tick()
        o = db.q1("SELECT * FROM auto_orders WHERE campaign_id=? AND kind='comments'", (lv,))
        # the views order was a 100-view test order: 100 of 1000 planned delivered -> 500 x 0.1 = 50 comments
        check("v2.16 #5: live comments order placed on the chosen service, scaled like likes (no jobs row); shares none",
              o["state"] == "placed" and o["service"] == "900" and o["qty"] == 50 and o["job_id"] is None
              and o["order_id"] and STATE["add_log"][-1] == ("900", 50)
              and not db.q("SELECT 1 FROM auto_orders WHERE campaign_id=? AND kind='shares'", (lv,)), (o, STATE["add_log"]))
        r_ = await main.auto_orders(campaign_id=lv, creator_id=None, post=None, state=None, kind="comments", page=1, _="t")
        check("v2.16 #5: orders tab filters by kind comments", r_["total"] == 1 and r_["items"][0]["kind"] == "comments"
              and r_["spent_usd"] == 0.1, r_)

        # --- 6. observed comments: stored from a source that returns them (Apify 3-tuple), shown on videos; never down
        X = db.insert("auto_campaigns", name="V", mode="practice", start_date=day(-1), end_date=day(1), created_at=NOW)
        url = "https://www.instagram.com/reel/O216x1/"
        pid = db.insert("auto_posts", post_key=sources.post_key("instagram", url), url_key=campaigns.url_key(url),
                        campaign_id=X, creator_id="a", account_handle="a", platform="instagram", url=url, source="sheet",
                        day=day(-1), uploaded_at=NOW - 3600, w_random=1.0, p_likes=0.01, ramp_start=day(-1), created_at=NOW)
        G = lambda: db.q1("SELECT * FROM auto_posts WHERE id=?", (pid,))  # noqa: E731
        planner.apply_count(G(), (1000, 20, 7), {}, NOW)
        c1 = G()["observed_comments"]
        planner.apply_count(G(), (1100, 21), {}, NOW)        # Clippers / SideShift: no comments field
        c2 = G()["observed_comments"]
        planner.apply_count(G(), (1200, 22, 3), {}, NOW)     # counts never go down
        v = {x["id"]: x for x in planner.detail(X, NOW)["videos"]}[pid]
        check("v2.16 #6: observed comments stored when the source has them, kept otherwise, never down; shares blank",
              (c1, c2, G()["observed_comments"]) == (7, 7, 7) and v["comments"] == 7 and v["shares"] is None, (c1, c2, v))

        async def apify_item(actor, inp, polls=0):
            return [{"videoPlayCount": 900, "likesCount": 9, "commentsCount": 4}]
        real_run = apify_client._run_actor
        apify_client._run_actor = apify_item
        try:
            gc = await sources.get_counts("instagram", "https://www.instagram.com/reel/APF216/")
        finally:
            apify_client._run_actor = real_run
        check("v2.16 #6: Apify read returns comments from the same response", gc == (900, 9, 4), gc)
    finally:
        panels.services_cached, panels.balance_cached, automation._scores = real
        config.DEFAULT_OVERRIDES.clear()
        config.DEFAULT_OVERRIDES.update(saved_over)


asyncio.run(v216_tests())


def v216_active_tests():
    """v2.16 bug 17: cancelled extras beyond the planned number do not shrink expected creators."""
    import automation
    import planner
    A = db.insert("auto_campaigns", name="act", mode="practice", creators_planned=20, created_at=1)
    for i in range(38):
        db.insert("auto_creators", campaign_id=A, creator_id=f"c{i}", cancelled=1 if i < 18 else 0)
    camp = automation._camp(A)
    c, e = planner.ignored(A)
    check("v2.16 bug17: 38 known, 18 cancelled, 20 planned -> 20 active", planner.active_count(camp, c | e) == 20,
          planner.active_count(camp, c | e))
    B = db.insert("auto_campaigns", name="act2", mode="practice", creators_planned=20, created_at=1)
    for i in range(10):
        db.insert("auto_creators", campaign_id=B, creator_id=f"d{i}", cancelled=1 if i < 2 else 0)
    camp = automation._camp(B)
    c, e = planner.ignored(B)
    check("v2.16 bug17: 10 known (fewer than planned), 2 cancelled -> 18 active", planner.active_count(camp, c | e) == 18,
          planner.active_count(camp, c | e))
    db.ex("UPDATE auto_creators SET included=0 WHERE campaign_id=? AND creator_id IN ('c18','c19','c20')", (A,))
    camp = automation._camp(A)
    c, e = planner.ignored(A)
    check("v2.16 bug17: 3 more un-ticked -> 17 active", planner.active_count(camp, c | e) == 17, planner.active_count(camp, c | e))


v216_active_tests()

def bug18_tests():
    """v2.16.1 bug 18: comments / shares lists and pick() judge the service NAME, not its category."""
    import automation
    ok = automation.extra_svc_ok
    bad = [("shares", "facebook", {"name": "Facebook Page Followers", "category": "Facebook Shares"}),
           ("shares", "facebook", {"name": "Facebook Profile Followers", "category": "Facebook Shares"}),
           ("shares", "instagram", {"name": "Instagram Profile Visits", "category": "Instagram Shares"}),
           ("comments", "instagram", {"name": "--- EGYPT ---", "category": "Instagram Comments"}),
           ("comments", "instagram", {"name": "Threads Shares", "category": "Instagram Comments"}),
           ("comments", "instagram", {"name": "Threads Reshare", "category": "Instagram Comments"}),
           ("comments", "facebook", {"name": "FACEBOOK POST SHARE", "category": "Facebook Comments"}),
           ("shares", "instagram", {"name": "Threads Shares", "category": "Instagram"}),
           ("comments", "instagram", {"name": "Instagram Comment Likes", "category": "Instagram Comments"}),
           ("shares", "instagram", {"name": "Instagram + Facebook Shares", "category": "Shares"}),
           ("shares", "instagram", {"name": "Instagram Custom Shares", "type": "Custom Comments"}),
           ("comments", "instagram", {"name": "Instagram Custom Comments", "type": "Custom Comments Package"}),
           ("comments", "instagram", {"name": "Instagram FEMALE Comments [Random 5 Comments] [Max: 40]"}),
           ("comments", "instagram", {"name": "Instagram Comments [TOP TIER] [300]"}),
           ("comments", "instagram", {"name": "Instagram Comments [100] [INDONESIA]"}),
           ("comments", "instagram", {"name": "Instagram Comments [RANDOM - USA] [150] [Start: 0-1H]"})]
    good = [("shares", "facebook", {"name": "Facebook Post Shares", "category": "Facebook Shares"}),
            ("shares", "instagram", {"name": "Instagram Shares [Real]", "category": "Instagram Shares"}),
            ("comments", "instagram", {"name": "Instagram Random Comments", "category": "Instagram Comments", "type": "Default"}),
            ("comments", "facebook", {"name": "Facebook Comments Emoji", "category": "Facebook"}),
            ("comments", "instagram", {"name": "Instagram Comments [RANDOM] [Max: 10K] [Refill: 30 Days]"}),
            ("comments", "instagram", {"name": "Instagram Verified Random Comments  [10/Day]"}),
            ("comments", "instagram", {"name": "Instagram Custom Comments", "type": "Custom Comments"}),   # v2.18
            ("comments", "instagram", {"name": "Instagram VIP Comments | USA | Custom", "type": "Custom Comments"}),
            ("comments", "facebook", {"name": "Facebook Comments | Custom | USA | 30 Days Refill", "type": "Custom Comments"})]
    check("v2.16.1 bug18: wrong kinds rejected (followers, visits, separators, Threads, share-as-comment, comment likes)",
          not [x for k, pl, x in bad if ok(x, pl, k)], [x["name"] for k, pl, x in bad if ok(x, pl, k)])
    check("v2.16.1 bug18: plain platform comments / shares services still accepted",
          all(ok(x, pl, k) for k, pl, x in good), [x["name"] for k, pl, x in good if not ok(x, pl, k)])


bug18_tests()


async def v217_tests():
    """v2.17 playtest round 1: bulk search by bare post id; Clippers errors readable and scoped."""
    import config
    import httpx
    import main
    L = mk_link(campaign_id=1701)
    a = mk_item(L, "https://www.instagram.com/reel/De23gxEqfJP")["id"]
    b = mk_item(L, "https://www.facebook.com/reel/1432199114964558")["id"]
    db.update("queue", a, clipper_email=None)
    db.update("queue", b, clipper_email="val@x.com")

    async def srch(terms):
        return await main.queue_search(main.SearchBody(link_id=L, terms=terms), _="t")
    d = await srch(["De23gxEqfJP", "val@x.com", "not-here@zzz.com"])
    check("v2.17 bug1: bulk terms match a bare IG shortcode; only the fake email missing",
          {x["id"] for x in d["items"]} == {a, b} and d["missing"] == ["not-here@zzz.com"], d)
    d = await srch(["De23gxEqfJP\r", " 1432199114964558 ", "nope12345"])
    check("v2.17 bug1: bare FB id matches, CR / spaces trimmed, unknown id missing",
          {x["id"] for x in d["items"]} == {a, b} and d["missing"] == ["nope12345"], d)
    d = await srch(["https://instagram.com/reel/De23gxEqfJP/?igsh=x", "val@x.com"])
    check("v2.17 bug1: full links still match by url_key", {x["id"] for x in d["items"]} == {a, b} and d["missing"] == [], d)

    check("v2.17 bug9: 5xx HTML page -> short message, no HTML",
          clippers.http_error("/api/campaigns/163/submissions", 502, "<!DOCTYPE html><html>")
          == "Clippers source unavailable (HTTP 502), retrying")
    check("v2.17 bug9: 4xx keeps a short plain body, drops HTML",
          clippers.http_error("/x", 404, "not  found\n") == "Clippers API /x: HTTP 404 not found"
          and clippers.http_error("/x", 404, " <html>") == "Clippers API /x: HTTP 404")
    real_client, real_base, real_tok = httpx.AsyncClient, config.CLIPPERS_API_BASE, clippers._token
    codes = [502, 200]

    def h(req):
        c = codes.pop(0)
        return httpx.Response(c, text="<!DOCTYPE html><html>bad gateway</html>") if c != 200 else httpx.Response(200, json=[])
    clippers.httpx.AsyncClient = lambda **kw: _RealClient(transport=httpx.MockTransport(h), **kw)
    config.CLIPPERS_API_BASE, clippers._token = "https://clip.test", "tok"
    clippers._fail("Clippers API unreachable: old")
    try:
        try:
            await clippers._get("/api/campaigns/163/submissions")
            err = None
        except clippers.ClippersError as e:
            err = str(e)
        st1 = clippers.status()["error"]
        await clippers._get("/api/campaigns")
        st2 = clippers.status()
    finally:
        clippers.httpx.AsyncClient, config.CLIPPERS_API_BASE, clippers._token = real_client, real_base, real_tok
    check("v2.17 bug9: a per-path 502 raises the clean text and does not set the global banner",
          err == "Clippers source unavailable (HTTP 502), retrying" and st1 == "Clippers API unreachable: old", (err, st1))
    check("v2.17 bug9: a good read clears a stale global error", st2["error"] is None and st2["at"] is None, st2)


asyncio.run(v217_tests())


async def v218_tests():
    """v2.18 custom comment pool: per campaign list, owed from views on the post, own lines on a Custom Comments service."""
    import datetime as dt
    import automation
    import config
    import main
    import planner
    import sources
    from fastapi import HTTPException
    VIP = {"service": 950, "name": "Instagram VIP Comments | USA | Custom", "rate": "158.368", "min": "5", "max": "300",
           "type": "Custom Comments"}
    CAT = {P: [{"service": 26146, "name": "Instagram Views", "rate": "0.04", "min": "100", "max": "1000000"},
               {"service": 26330, "name": "Instagram Likes", "rate": "0.08", "min": "20", "max": "100000"},
               {"service": 900, "name": "Instagram Comments Random", "rate": "2.0", "min": "10", "max": "5000", "type": "Default"},
               {"service": 902, "name": "Instagram Shares", "rate": "0.5", "min": "10", "max": "50000"}, VIP],
           "morethanpanel": [{**VIP, "service": 8793},
                             {"service": 8800, "name": "Instagram Comments Custom Cheap", "rate": "9", "min": "5", "max": "100",
                              "type": "Custom Comments"},
                             {"service": 8439, "name": "Facebook Comments | Custom | USA | 30 Days Refill", "rate": "13.23",
                              "min": "5", "max": "10000", "type": "Custom Comments"}]}

    async def f_services(p):
        if p not in CAT:
            raise panels.PanelUnavailable("no key")
        return CAT[p]

    async def f_balance(p):
        return 100.0
    real = (panels.services_cached, panels.balance_cached, automation._scores)
    panels.services_cached, panels.balance_cached = f_services, f_balance
    automation._scores = lambda: {}
    saved_over = dict(config.DEFAULT_OVERRIDES)
    config.DEFAULT_OVERRIDES.clear()
    S = lambda sid, p=P: {"provider": p, "service": sid}  # noqa: E731
    CS = {**S("950"), "custom": True, "min": 5, "max": 300}   # a pick as prep_comments stores it
    NOW = int(time.time())
    TODAY = automation.et_day(NOW)

    def day(n):
        return (dt.date.fromisoformat(TODAY) + dt.timedelta(days=n)).isoformat()

    def wipe():
        for t in ("plan_rows", "auto_orders", "pauses", "auto_campaigns", "auto_posts", "auto_creators", "auto_slots",
                  "auto_creator_map", "jobs", "comment_pool", "comment_used"):
            db.ex(f"DELETE FROM {t}")
        db.ex("DELETE FROM settings WHERE k LIKE 'planner:%' OR k LIKE 'auto:%'")
        automation._alerts.clear()
        automation.set_settings({"global_daily_cap_usd": 1000})
        reset()
        STATE["add_forms"] = []

    def pool(cid, n, tag="c"):
        return automation.pool_set(cid, "\n".join(f"{tag} line {i}" for i in range(n)))

    def post(cid, i, views, creator="a", status="active", d=-2):
        url = f"https://www.instagram.com/reel/V218x{cid}x{i}/"
        db.insert("auto_posts", post_key=sources.post_key("instagram", url), url_key=campaigns.url_key(url),
                  campaign_id=cid, creator_id=creator, account_handle=creator, platform="instagram", url=url,
                  source="sheet", day=day(d), uploaded_at=automation.et_midnight(day(d)) + 12 * 3600, w_random=1.0,
                  p_likes=0.01, ramp_start=day(d), observed_views=views, base_views=views, base_day=day(d),
                  observed_likes=views // 200, checked_at=NOW, created_at=automation.et_midnight(day(d)), status=status)
        return url

    def camp(mode="practice", comments=None, rate=0, **kw):
        sv = {"views": S("26146"), "likes": S("26330")}
        if comments:
            sv["comments"] = comments
        return db.insert("auto_campaigns", **{"name": f"C218{mode}", "mode": mode, "views_goal": 200000, "start_date": day(-2),
                         "end_date": day(5), "creators_planned": 1, "posts_per_day": 1, "approved": 1, "margin_pct": 0,
                         "platforms": json.dumps(["instagram"]), "posts_per_creator": json.dumps({"instagram": 1}),
                         "created_at": NOW, "services": json.dumps({"instagram": sv}), "comments_per_1k": rate, **kw})

    def ca(cid):   # url -> comments_add of today's rows
        return {r["post_url"]: r["comments_add"] for r in db.q("SELECT * FROM plan_rows WHERE campaign_id=? AND day=?",
                                                               (cid, TODAY))}

    def prow(cid, url, n, pid, creator="a"):
        automation.insert_plan([{"plan_id": f"{cid}:{pid}:{TODAY}", "campaign_id": cid, "creator_id": creator, "post_url": url,
                                 "platform": "instagram", "window_start": automation.et_midnight(TODAY),
                                 "window_end": automation.et_midnight(TODAY), "views_add": 0, "likes_add": 0,
                                 "comments_add": n}])
        # a hand-made row stands for a planner run that already counted every earlier order (v2.18 review fix
        # _comments_left subtracts only orders after the run); the cross-campaign test below sets its own run time
        planner._save_run(cid, {"at": 10 ** 10})
    try:
        wipe()
        # --- 1. API: Custom Comments accepted for comments, tagged, rate empty = 5; quantity-only unchanged
        c = await main.auto_campaign_add({"name": "A", "services": {"instagram": {"comments": S("950")}}}, who="t")
        check("v2.18 #1: Custom Comments pick accepted on create, tagged custom + min/max, empty rate = 5",
              c["services"]["instagram"]["comments"] == CS and c["comments_per_1k"] == 5, c)
        q_ = await main.auto_campaign_add({"name": "B", "services": {"instagram": {"comments": S("900")}}}, who="t")
        check("v2.18 #1: quantity-only comments pick: rate still defaults to 0, no tag",
              q_["comments_per_1k"] == 0 and q_["services"]["instagram"]["comments"] == S("900"), q_)
        x = await main.auto_campaign_patch(q_["id"], {"services": {"instagram": {"comments": {**S("900"), "custom": True}}}},
                                           who="t")
        check("v2.18 #1: a client-sent custom tag on a quantity-only service is dropped",
              x["services"]["instagram"]["comments"] == S("900"), x)
        x = await main.auto_campaign_patch(q_["id"], {"services": {"instagram": {"comments": S("950")}}}, who="t")
        y = await main.auto_campaign_patch(q_["id"], {"comments_per_1k": 12}, who="t")
        z = await main.auto_campaign_patch(q_["id"], {"comments_per_1k": None}, who="t")
        check("v2.18 #1: edit: switch to Custom Comments with rate 0 -> 5; typed rate kept; emptied -> 5",
              (x["comments_per_1k"], y["comments_per_1k"]) == (5, 12) and z["comments_per_1k"] == 5, (x, y, z))
        try:
            await main.auto_campaign_patch(q_["id"], {"services": {"instagram": {"shares": S("950")}}}, who="t")
            check("v2.18 #1: Custom Comments refused for shares", False)
        except HTTPException as e:
            check("v2.18 #1: Custom Comments refused for shares", e.status_code == 400, e.detail)
        ef = await automation.estimate_for({"views_goal": 1_000_000, "start_date": "2026-11-01", "end_date": "2026-11-10",
                                            "creators_planned": 10, "posts_per_creator": {"instagram": 1}, "margin_pct": 0,
                                            "comments_per_1k": 5, "services": {"instagram": {"views": S("26146"),
                                                                                             "comments": S("950")}}})
        check("v2.18 #1: calculator comments line = views x 5 / 1k x price / 1k (inside the one cap)",
              ef["cost"]["comments"] == round(1_000_000 * 5 / 1000 * 158.368 / 1000, 2)
              and ef["cap_default_usd"] == round(ef["total_usd"] * 1.1, 2), ef)
        so = await main.auto_services("instagram", "t")
        fb = await main.auto_services("facebook", "t")
        check("v2.18 #2: dropdown: starred default first (MoreThanPanel 8793 IG, 8439 FB), others not starred",
              (so["comments"][0]["provider"], so["comments"][0]["service"], so["comments"][0]["star"]) == ("morethanpanel", "8793", True)
              and sum(1 for r_ in so["comments"] if r_["star"]) == 1 and fb["comments"][0]["service"] == "8439"
              and fb["comments"][0]["star"] and {"950", "900", "8800"} <= {r_["service"] for r_ in so["comments"]}, so["comments"])

        # --- 3. pool API: paste / csv / dedup / 300 chars / remove / replace
        cid = c["id"]
        r_ = await main.auto_comments_set(cid, {"text": "  Whats the app?\r\n\n getpicks is actually goated \nWhats the app?\n"
                                                        "It's not available in my state 💀\n", "mode": "add"}, who="t")
        check("v2.18 #3: paste: trimmed, blanks + exact duplicates dropped, count shown",
              r_["count"] == 3 and r_["added"] == 3 and [i["text"] for i in r_["items"]][1] == "getpicks is actually goated", r_)
        r_ = await main.auto_comments_set(cid, {"text": '"Hello, ""world"""\nWhats the app?\nnew one', "csv": True}, who="t")
        check("v2.18 #3: add (csv file): quotes unwrapped, existing line skipped",
              (r_["count"], r_["added"], r_["skipped"]) == (5, 2, 1) and 'Hello, "world"' in [i["text"] for i in r_["items"]], r_)
        for bad in ({"text": "ok\n" + "x" * 301}, {"text": "\n \n"}, {"text": 5}, {"text": "a", "mode": "merge"}):
            try:
                await main.auto_comments_set(cid, bad, who="t")
                check(f"v2.18 #3: bad pool input rejected {str(bad)[:40]}", False)
            except HTTPException as e:
                check(f"v2.18 #3: bad pool input rejected {str(bad)[:40]}", e.status_code == 400 and automation.pool_view(cid)["count"] == 5,
                      e.detail)
        check("v2.18 #3: a 300-character line is accepted", automation.pool_set(cid, "y" * 300)["count"] == 6)
        db.ex("UPDATE comment_pool SET times_used=4 WHERE campaign_id=? AND text='new one'", (cid,))
        r_ = await main.auto_comments_set(cid, {"text": "new one\nfresh", "mode": "replace"}, who="t")
        check("v2.18 #3: replace: list = exactly the new lines, kept line keeps its use count",
              {(i["text"], i["times_used"]) for i in r_["items"]} == {("new one", 4), ("fresh", 0)} and r_["removed"] == 5, r_)
        r_ = await main.auto_comments_remove(cid, r_["items"][0]["id"], who="t")
        g = await main.auto_comments(cid, "t")
        check("v2.18 #3: remove one line; GET lists the pool", r_["count"] == 1 == g["count"], g)
        try:
            await main.auto_comments(424242, "t")
            check("v2.18 #3: unknown campaign 404", False)
        except HTTPException as e:
            check("v2.18 #3: unknown campaign 404", e.status_code == 404)

        # --- 4. seam 1: planner rows
        wipe()
        A = camp(comments=CS, rate=5)
        pool(A, 1000)
        u = [post(A, 0, 3000), post(A, 1, 900), post(A, 2, 100_000), post(A, 3, 9000, creator="x"),
             post(A, 4, 9000, status="deleted"), post(A, 5, 4000)]
        db.insert("auto_creators", campaign_id=A, creator_id="x", included=0)
        db.insert("auto_orders", plan_id="old", campaign_id=A, url_key=campaigns.url_key(u[5]), day=day(-1), kind="comments",
                  attempt=0, qty=12, state="simulated", created_at=NOW - 86400)
        planner.plan(automation._camp(A), NOW)
        got = ca(A)
        check("v2.18 #4 seam1: rate 5: 3000 views -> 15; 900 -> 4 = below min -> 0; 100k -> 500 capped at max 300; "
              "4000 views with 12 already ordered -> 8",
              (got.get(u[0]), got.get(u[1]), got.get(u[2]), got.get(u[5])) == (15, 0, 300, 8), got)
        check("v2.18 #4 seam1: un-ticked creator and deleted post get no comments",
              not got.get(u[3]) and not got.get(u[4]), got)
        B = camp(comments=CS, rate=12)
        pool(B, 1000)
        ub = post(B, 0, 3000)
        planner.plan(automation._camp(B), NOW)
        check("v2.18 #4 seam1: custom rate 12: 3000 views -> 36", ca(B).get(ub) == 36, ca(B))
        Cc = camp(comments=CS, rate=5)
        pool(Cc, 20)
        uc = [post(Cc, 0, 10_000), post(Cc, 1, 3000)]
        planner.plan(automation._camp(Cc), NOW)
        check("v2.18 #4 seam1: lifetime per video <= pool size (50 owed, list of 20 -> 20); small video unaffected",
              (ca(Cc).get(uc[0]), ca(Cc).get(uc[1])) == (20, 15), ca(Cc))
        al = [a["text"] for a in automation.alerts(0)]
        check("v2.18 #4: alert 'Comment list smaller than needed on 1 videos'",
              any("C218practice" in t and "Comment list smaller than needed on 1 videos" in t for t in al), al)
        D = camp(comments=CS, rate=5)
        ud = post(D, 0, 3000)
        planner.plan(automation._camp(D), NOW)
        planner.plan(automation._camp(D), NOW)
        al = [a["text"] for a in automation.alerts(0) if "Comments on but list empty" in a["text"]]
        check("v2.18 #4: alert 'Comments on but list empty' (one per campaign per day, no 'smaller' alert); nothing owed",
              len(al) == 1 and ca(D).get(ud) == 0
              and not any("C218practice" in a["text"] and "smaller" in a["text"] and "on 1 videos" not in a["text"]
                          for a in automation.alerts(0))
              and db.q1("SELECT v FROM settings WHERE k=?", (f"planner:cempty:{D}",))["v"] == TODAY, al)
        E = camp(comments=CS, rate=0)
        pool(E, 50)
        ue = post(E, 0, 3000)
        planner.plan(automation._camp(E), NOW)
        check("v2.18 #4 HARD RULE: Custom Comments service but rate 0 -> no comments", ca(E).get(ue) == 0, ca(E))

        # --- 5. seam 2: live order payloads
        wipe()
        L = camp(mode="live", comments=CS, rate=5, cap_usd=100)
        pool(L, 12)
        ul = post(L, 0, 3000)
        prow(L, ul, 6, "a")
        await automation.run_tick()
        f1 = [f_ for f_ in STATE["add_forms"] if f_["service"] == "950"]
        prow(L, ul, 6, "b")
        await automation.run_tick()
        f2 = [f_ for f_ in STATE["add_forms"] if f_["service"] == "950"]
        l1, l2 = (f2[0]["comments"].split("\n") if f2 else []), (f2[1]["comments"].split("\n") if len(f2) > 1 else [])
        pl = {i["text"] for i in automation.pool_view(L)["items"]}
        check("v2.18 #5 seam2: order payload: quantity == number of lines, lines from the pool, no repeat across 2 orders",
              len(f1) == 1 and len(f2) == 2 and [int(f_["quantity"]) for f_ in f2] == [6, 6] and len(set(l1)) == 6
              and len(set(l2)) == 6 and not set(l1) & set(l2) and set(l1) | set(l2) <= pl, f2)
        o = db.q("SELECT * FROM auto_orders WHERE campaign_id=? AND kind='comments' ORDER BY id", (L,))
        check("v2.18 #5: orders placed on the chosen service, est cost = lines x rate, lines kept on the order",
              [x_["state"] for x_ in o] == ["placed", "placed"] and o[0]["est_cost"] == round(6 / 1000 * 158.368, 4)
              and json.loads(o[0]["comments"]) == l1 and automation.order_full(o[0]["id"])["comment_lines"] == l1, o)
        check("v2.18 #5: every line used once, recorded per post",
              {i["times_used"] for i in automation.pool_view(L)["items"]} == {1}
              and db.q1("SELECT COUNT(*) n FROM comment_used WHERE url_key=?", (campaigns.url_key(ul),))["n"] == 12)
        n0 = len(STATE["add_forms"])
        prow(L, ul, 6, "c")
        await automation.run_tick()
        o = db.q1("SELECT * FROM auto_orders WHERE plan_id=?", (f"{L}:c:{TODAY}",))
        check("v2.18 #5: all lines already on the post -> blocked, nothing sent",
              o["state"] == "blocked" and "0 unused lines" in o["error"] and len(STATE["add_forms"]) == n0, o)
        v = {x_["url"]: x_ for x_ in planner.detail(L, NOW)["videos"]}[ul]
        check("v2.18 #5: Videos tab shows the lines that went on the video", sorted(v["comment_lines"]) == sorted(l1 + l2), v)
        sp = planner.detail(L, NOW)["spend"]
        check("v2.18 #5: Spend shows comment spend apart", sp["by_kind"]["comments"] == round(2 * round(6 / 1000 * 158.368, 4), 2)
              and sp["by_kind"]["views"] == 0, sp["by_kind"])
        # fewer unused lines than asked but >= min: send what exists
        u2 = post(L, 1, 3000)
        prow(L, u2, 300, "d")
        await automation.run_tick()
        o = db.q1("SELECT * FROM auto_orders WHERE plan_id=?", (f"{L}:d:{TODAY}",))
        check("v2.18 #5: owed 300, list of 12 -> one order of 12 lines (quantity 12)",
              o["state"] == "placed" and o["qty"] == 12 and int(STATE["add_forms"][-1]["quantity"]) == 12
              and len(STATE["add_forms"][-1]["comments"].split("\n")) == 12, o)
        # empty pool never sends
        L2 = camp(mode="live", comments=CS, rate=5, cap_usd=100)
        u3 = post(L2, 0, 3000)
        n0 = len(STATE["add_forms"])
        prow(L2, u3, 10, "e")
        await automation.run_tick()
        o = db.q1("SELECT * FROM auto_orders WHERE plan_id=?", (f"{L2}:e:{TODAY}",))
        check("v2.18 #5: empty pool: blocked 'comment list is empty', nothing sent",
              o["state"] == "blocked" and "comment list is empty" in o["error"] and len(STATE["add_forms"]) == n0, o)
        # deleted post / cancelled creator / un-ticked creator: no order at all
        pool(L2, 30)
        u4, u5, u6 = post(L2, 1, 3000, status="deleted"), post(L2, 2, 3000, creator="k"), post(L2, 3, 3000, creator="z")
        db.insert("auto_creators", campaign_id=L2, creator_id="k", cancelled=1)
        db.insert("auto_creators", campaign_id=L2, creator_id="z", included=0)
        prow(L2, u4, 10, "f")
        prow(L2, u5, 10, "g", creator="k")
        prow(L2, u6, 10, "h", creator="z")
        await automation.run_tick()
        check("v2.18 #5: no comments for a deleted post, a cancelled or an un-ticked creator",
              not db.q("SELECT 1 FROM auto_orders WHERE plan_id IN (?,?,?) AND state<>'blocked'",
                       (f"{L2}:f:{TODAY}", f"{L2}:g:{TODAY}", f"{L2}:h:{TODAY}")) and len(STATE["add_forms"]) - n0 == 1)
        # failed order: nothing marked; ambiguous: nothing marked, but its lines never go on that post again
        L3 = camp(mode="live", comments=CS, rate=5, cap_usd=100)
        pool(L3, 15)
        u7 = post(L3, 0, 3000)
        STATE["add_plan"] = ["error"]
        prow(L3, u7, 5, "i")
        await automation.run_tick()
        o = db.q1("SELECT * FROM auto_orders WHERE plan_id=?", (f"{L3}:i:{TODAY}",))
        check("v2.18 #6: panel rejects: order failed, no line marked used",
              o["state"] == "failed" and not db.q("SELECT 1 FROM comment_used WHERE campaign_id=?", (L3,))
              and {i["times_used"] for i in automation.pool_view(L3)["items"]} == {0}, o)
        await automation.run_tick()
        check("v2.18 #6: a failed comments order is not auto re-sent the same day",
              db.q1("SELECT COUNT(*) n FROM auto_orders WHERE campaign_id=? AND kind='comments'", (L3,))["n"] == 1)
        r_ = await automation.resend(o["id"])
        check("v2.18 #6: manual resend of the failed order: new lines placed, now marked",
              r_["state"] == "placed" and r_["qty"] == 5 and sum(i["times_used"] for i in automation.pool_view(L3)["items"]) == 5, r_)
        STATE["add_plan"] = ["timeout"]
        prow(L3, u7, 5, "j")
        await automation.run_tick()
        panels._down.clear()
        o = db.q1("SELECT * FROM auto_orders WHERE plan_id=?", (f"{L3}:j:{TODAY}",))
        amb = set(json.loads(o["comments"]))
        check("v2.18 #6: add timed out: ambiguous, its lines not marked used",
              o["state"] == "ambiguous" and sum(i["times_used"] for i in automation.pool_view(L3)["items"]) == 5, o)
        r_ = await automation.resend(o["id"])
        sent = set(STATE["add_forms"][-1]["comments"].split("\n"))
        check("v2.18 #6: resend after an ambiguous order never repeats its lines on that post",
              r_["state"] == "placed" and r_["qty"] == 5 and not sent & amb
              and len(sent | amb | set(json.loads(db.q1("SELECT comments FROM auto_orders WHERE plan_id=? AND state='retried'",
                                                         (f"{L3}:i:{TODAY}",))["comments"]))) >= 10, (sent, amb))
        prow(L3, u7, 5, "k")
        await automation.run_tick()
        o = db.q1("SELECT * FROM auto_orders WHERE plan_id=?", (f"{L3}:k:{TODAY}",))
        check("v2.18 #6: list of 15, 15 lines placed or maybe placed on the post -> blocked, nothing sent",
              o["state"] == "blocked" and "0 unused" in o["error"], o)
        # budget: the campaign cap covers comments
        L4 = camp(mode="live", comments=CS, rate=5, cap_usd=0.5)
        pool(L4, 30)
        u8 = post(L4, 0, 3000)
        n0 = len(STATE["add_forms"])
        prow(L4, u8, 10, "l")
        await automation.run_tick()
        o = db.q1("SELECT * FROM auto_orders WHERE plan_id=?", (f"{L4}:l:{TODAY}",))
        check("v2.18 #6: campaign cap stops comments (10 lines = $1.58 > $0.50 cap), nothing sent, nothing marked",
              o["state"] == "blocked" and "campaign cap" in o["error"] and len(STATE["add_forms"]) == n0
              and not db.q("SELECT 1 FROM comment_used WHERE campaign_id=?", (L4,)), o)

        # review fix (HIGH): a rate set to 0 stays 0 on an unrelated PATCH (rename / Go Live path)
        z = await main.auto_campaign_add({"name": "Z0", "services": {"instagram": {"comments": S("950")}}}, who="t")
        await main.auto_campaign_patch(z["id"], {"comments_per_1k": 0}, who="t")
        await main.auto_campaign_patch(z["id"], {"name": "Z0 renamed"}, who="t")
        check("v2.18 review: comments rate 0 survives an unrelated PATCH (no silent default 5)",
              automation._camp(z["id"])["comments_per_1k"] == 0, automation._camp(z["id"])["comments_per_1k"])

        # review fix: one post in two live campaigns, both rows computed before either order -> second is capped
        wipe()
        X1 = camp(mode="live", comments=CS, rate=5, cap_usd=100)
        X2 = camp(mode="live", comments=CS, rate=5, cap_usd=100)
        pool(X1, 20)
        pool(X2, 20)
        ux = post(X1, 0, 3000)
        prow(X1, ux, 6, "x1")
        prow(X2, ux, 6, "x2")
        planner._save_run(X1, {"at": 0})
        planner._save_run(X2, {"at": 0})
        nf = len(STATE["add_forms"])
        await automation.run_tick()
        sent = [f_ for f_ in STATE["add_forms"][nf:] if f_["service"] == "950"]
        check("v2.18 review: same post in 2 campaigns, rows from one run -> only one order goes out",
              len(sent) == 1 and int(sent[0]["quantity"]) == 6, sent)

        # --- 7. seam 2 spread: 50 practice posts, least used first
        wipe()
        Pc = camp(comments=CS, rate=5)
        pool(Pc, 20)
        for i in range(50):
            prow(Pc, f"https://www.instagram.com/reel/S218x{i}/", 5, f"s{i}")
        await automation.run_tick()
        o = db.q("SELECT * FROM auto_orders WHERE campaign_id=? AND kind='comments'", (Pc,))
        tu = [i["times_used"] for i in automation.pool_view(Pc)["items"]]
        check("v2.18 #7 seam2: practice: 50 simulated orders, qty = lines, $0 sent; spread max - min used <= 1",
              len(o) == 50 and all(x_["state"] == "simulated" and len(json.loads(x_["comments"])) == x_["qty"] == 5 for x_ in o)
              and not STATE["add_forms"] and sum(tu) == 250 and max(tu) - min(tu) <= 1, tu)
        r_ = planner.reset(Pc)
        check("v2.18 #7: practice reset: list kept, uses cleared",
              automation.pool_view(Pc)["count"] == 20 and {i["times_used"] for i in automation.pool_view(Pc)["items"]} == {0}
              and not db.q("SELECT 1 FROM comment_used WHERE campaign_id=?", (Pc,)), r_)

        # --- 8. views / likes / shares unchanged; quantity-only comments path unchanged
        def case(**kw):
            for t in ("plan_rows", "auto_orders", "auto_campaigns", "auto_posts", "jobs", "comment_pool", "comment_used"):
                db.ex(f"DELETE FROM {t}")
            reset()
            STATE["add_forms"] = []
            cid_ = camp(**kw)
            for i, (d, v_) in enumerate(((-2, 3000), (-1, 2000))):
                post(cid_, i, v_, d=d)
            pool(cid_, 100)
            planner.plan(automation._camp(cid_), NOW)
            return cid_
        sig = lambda cid_: sorted((o_["kind"], o_["url_key"].split("x", 1)[-1][-2:], o_["service"], o_["qty"], o_["est_cost"], o_["state"])  # noqa: E731
                                  for o_ in db.q("SELECT * FROM auto_orders WHERE campaign_id=?", (cid_,)))
        res = {}
        for name, kw in (("none", {}), ("custom", {"comments": CS, "rate": 5}),
                         ("qty", {"comments": S("900"), "rate": 300})):
            cid_ = case(**kw)
            for _ in range(3):
                await automation.run_tick()
            res[name] = (cid_, sig(cid_), db.q("SELECT * FROM plan_rows WHERE campaign_id=?", (cid_,)))
        vls = lambda n: [x_ for x_ in res[n][1] if x_[0] in ("views", "likes", "shares")]  # noqa: E731
        check("v2.18 #8: views / likes / shares orders identical with and without custom comments",
              vls("none") == vls("custom") == vls("qty") and any(x_[0] == "views" for x_ in vls("none")), (vls("none"), vls("custom")))
        cc_ = [x_ for x_ in res["custom"][1] if x_[0] == "comments"]
        check("v2.18 #8: custom comments simulated on 950: owed from views on the post (3000 -> 15, 2000 -> 10)",
              sorted(x_[3] for x_ in cc_) == [10, 15] and all(x_[2] == "950" for x_ in cc_), cc_)
        qc = [x_ for x_ in res["qty"][1] if x_[0] == "comments"]
        check("v2.18 #8: quantity-only comments path unchanged (ratio of bought views, service 900, no lines attached)",
              all(r_["comments_add"] == round(r_["views_add"] * 300 / 1000) for r_ in res["qty"][2]) and qc
              and all(x_[2] == "900" for x_ in qc)
              and not db.q("SELECT 1 FROM auto_orders WHERE campaign_id=? AND comments IS NOT NULL", (res["qty"][0],))
              and not db.q("SELECT 1 FROM comment_used WHERE campaign_id=?", (res["qty"][0],)), qc)
    finally:
        panels.services_cached, panels.balance_cached, automation._scores = real
        panels._down.clear()
        config.DEFAULT_OVERRIDES.clear()
        config.DEFAULT_OVERRIDES.update(saved_over)


asyncio.run(v218_tests())

async def v219_no_add_retry_tests():
    """v2.19: "add" is never retried (a 5xx after the panel took it would double-spend); unsure = marked, not re-sent."""
    import httpx
    import engine
    req = httpx.Request("POST", "https://p.test")
    for code in (500, 502, 503, 504):
        e = httpx.HTTPStatusError("x", request=req, response=httpx.Response(code, request=req))
        check(f"v2.19: add on HTTP {code} not retried; services is", not panels._transient(e, "add")
              and (code == 504 or panels._transient(e, "services")) and panels._transient(e, "status"))
    check("v2.19: add on connect error / timeout not retried",
          not panels._transient(httpx.ConnectError("x"), "add") and not panels._transient(httpx.ReadTimeout("x"), "add"))
    calls = []

    class C:
        async def add(self, *a, **k):
            calls.append(a)
            raise httpx.HTTPStatusError("bad gateway", request=req, response=httpx.Response(502, request=req))
    real = engine.client_for
    engine.client_for = lambda p: C()
    try:
        jid = db.create_job(kind="eng", provider="smmglow", platform="instagram", link="https://www.instagram.com/reel/N219/",
                            likes_service="1", state="running", created_at=1)
        ok = await engine._order_likes(db.get_job(jid), 50)
        j = db.get_job(jid)
        check("v2.19: unsure likes top-up counts as ordered (no re-send) and notes 'check the panel'",
              ok is True and len(calls) == 1 and "check the panel" in (j["note"] or ""), j["note"])
        import main
        check("v2.19 review: job note reaches the API (Jobs row warning)", "check the panel" in (main._job_view(j)["note"] or ""))
        await engine._order_likes(db.get_job(jid), 30)
        check("v2.19 review: a second unsure top-up appends, never overwrites",
              db.get_job(jid)["note"].count("check the panel") == 2, db.get_job(jid)["note"])

        class D:
            async def add(self, *a, **k):
                raise httpx.ConnectError("refused")
        engine.client_for = lambda p: D()
        jid2 = db.create_job(kind="eng", provider="smmglow", platform="instagram", link="https://www.instagram.com/reel/N219b/",
                             likes_service="1", state="running", created_at=1)
        check("v2.19 review: connect error = never sent -> not counted, no note",
              await engine._order_likes(db.get_job(jid2), 50) is False and not db.get_job(jid2)["note"])
    finally:
        engine.client_for = real


asyncio.run(v219_no_add_retry_tests())

def v220_dash_alert_tests():
    """v2.20: the last run's comment-pool warnings show in the campaign dashboard alerts."""
    import planner
    c = db.insert("auto_campaigns", name="DA", mode="practice", views_goal=1000, start_date="2026-10-01",
                  end_date="2026-10-30", created_at=1)
    planner._save_run(c, {"at": 1, "comments_short": 7})
    t = [a["text"] for a in planner.dashboard(c, 1793000000)["alerts"]]
    check("v2.20: dashboard shows 'Comment list smaller than needed on 7 videos'", any("on 7 videos" in x for x in t), t)


v220_dash_alert_tests()


async def v223_export_tests():
    """v2.23: All-submissions CSV export reuses the v2.11 search filter (same rows + order,
    clipper_email column), is read-only (no panel call) and sits behind require_auth."""
    import csv as _csv
    import inspect
    import io as _io
    import main
    from fastapi import HTTPException
    reset()
    L = mk_link(campaign_id=2301, views=1000, likes=100)
    link = campaigns.get_link(L)

    def sub(sid, code, email):
        return {"id": sid, "url": f"https://www.instagram.com/reel/{code}/", "platform": "instagram",
                "clipper": "c" + code, "submitted_at": None, "email": email}
    campaigns._insert_submission(link, sub(92301, "X23A", "Ann@X.com"), "pending")
    campaigns._insert_submission(link, sub(92302, "X23B", "bob@y.com"), "pending")
    campaigns._insert_submission(link, sub(92303, "X23C", None), "pending")
    A = db.q1("SELECT id FROM queue WHERE link_id=? AND submission_id=?", (L, 92301))["id"]
    r = await place(A, ("pending",))
    check("v2.23: setup A placed (sent)", r["state"] == "placed", r)

    def csv_rows(resp):
        return [row for row in _csv.reader(_io.StringIO(resp.body.decode("utf-8"))) if row]

    async def export(sent="all", q=()):
        return csv_rows(await main.queue_export(link_id=L, sent=sent, q=list(q), _="t"))

    async def srch(sent="all", q=()):
        d = await main.queue_search(main.SearchBody(link_id=L, terms=list(q), sent=sent), _="t")
        return {str(x["submission_id"]) for x in d["items"]}

    def sids(rows):
        return {row[0] for row in rows[1:]}

    adds0 = STATE["adds"]
    rows = await export()
    check("v2.23: CSV header row is the documented columns", tuple(rows[0]) == main.EXPORT_COLS, rows[0])
    check("v2.23: one data row per submission, newest first (q.id DESC)",
          [row[0] for row in rows[1:]] == ["92303", "92302", "92301"], rows)
    check("v2.23: export is read-only (no panel order placed)", STATE["adds"] == adds0)

    ec = rows[0].index("clipper_email")
    check("v2.23: clipper_email column present and populated (blank when unknown)",
          {row[0]: row[ec] for row in rows[1:]} == {"92301": "Ann@X.com", "92302": "bob@y.com", "92303": ""},
          {row[0]: row[ec] for row in rows[1:]})
    scol = rows[0].index("sent")
    check("v2.23: sent column = yes only for the ordered post",
          {row[0]: row[scol] for row in rows[1:]} == {"92301": "yes", "92302": "no", "92303": "no"},
          {row[0]: row[scol] for row in rows[1:]})

    for sent, q in [("all", ()), ("yes", ()), ("no", ()), ("all", ["bob@y.com"]),
                    ("all", ["ann@x.com", "bob@y.com"]), ("no", ["ann@x.com", "bob@y.com"]), ("all", ["X23C"])]:
        exp, got = sids(await export(sent, q)), await srch(sent, q)
        check(f"v2.23: export rows match /api/queue/search (sent={sent} q={list(q)})", exp == got, (exp, got))

    try:
        await main.queue_export(link_id=L, sent="maybe", q=[], _="t")
        check("v2.23: bad sent -> 400", False)
    except HTTPException as e:
        check("v2.23: bad sent -> 400", e.status_code == 400)

    dep = inspect.signature(main.queue_export).parameters["_"].default
    check("v2.23: export route sits behind require_auth", getattr(dep, "dependency", None) is main.require_auth)


asyncio.run(v223_export_tests())
print("ALL PASS")
