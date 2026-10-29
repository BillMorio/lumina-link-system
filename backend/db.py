"""SQLite job log for the engagement console.

Every job is a labeled data point for the bot-detection research: target +
platform + provider + targets + delivery snapshots over time. Low volume, internal
tool -> sync sqlite3 (callers in async code wrap these in asyncio.to_thread).
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import time
from contextlib import contextmanager
from zoneinfo import ZoneInfo

DB_PATH = os.getenv("CONSOLE_DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "console.db"))

ACTIVE_JOB_STATES = ("running",)


@contextmanager
def _conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _seed() -> None:
    """One-time move of an existing database onto a fresh disk: CONSOLE_DB_SEED is a
    base64 gzip of console.db. Used only when no database file exists yet."""
    seed = os.getenv("CONSOLE_DB_SEED", "").strip()
    if seed and not os.path.exists(DB_PATH):
        import base64, gzip
        with open(DB_PATH, "wb") as f:
            f.write(gzip.decompress(base64.b64decode(seed)))


def init_db() -> None:
    _seed()
    with _conn() as c:
        c.execute("PRAGMA journal_mode = WAL")
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                kind              TEXT NOT NULL DEFAULT 'eng',
                provider          TEXT NOT NULL,
                platform          TEXT NOT NULL,
                link              TEXT NOT NULL,
                views_target      INTEGER NOT NULL DEFAULT 0,
                likes_target      INTEGER NOT NULL DEFAULT 0,
                comments_target   INTEGER NOT NULL DEFAULT 0,
                comment_style     TEXT,
                comment_lines     TEXT,
                views_service     TEXT,
                likes_service     TEXT,
                comments_service  TEXT,
                views_order_id    TEXT,
                likes_order_id    TEXT,
                comments_order_id TEXT,
                likes_ordered     INTEGER DEFAULT 0,
                baseline_likes    INTEGER,
                last_views        INTEGER DEFAULT 0,
                last_likes        INTEGER DEFAULT 0,
                last_comments     INTEGER DEFAULT 0,
                state             TEXT NOT NULL DEFAULT 'running',
                owner_handle      TEXT,
                created_at        INTEGER NOT NULL,
                next_poll_at      INTEGER,
                created_by        TEXT,
                note              TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id   INTEGER NOT NULL,
                t        INTEGER NOT NULL,
                views    INTEGER, likes INTEGER, comments INTEGER
            )
            """
        )
        # v2: completion tracking + source. Guarded so existing DBs upgrade in place.
        have = {r["name"] for r in c.execute("PRAGMA table_info(jobs)")}
        for col, typ in (("source", "TEXT DEFAULT 'manual'"), ("views_status", "TEXT"),
                         ("views_remaining", "INTEGER"), ("views_start_count", "INTEGER"),
                         ("completed_at", "INTEGER"), ("first_progress_at", "INTEGER"),
                         ("status_checked_at", "INTEGER"), ("url_key", "TEXT")):
            if col not in have:
                c.execute(f"ALTER TABLE jobs ADD COLUMN {col} {typ}")
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id    INTEGER NOT NULL,
                campaign_name  TEXT,
                provider       TEXT NOT NULL,
                mode           TEXT NOT NULL DEFAULT 'approve',
                views          INTEGER NOT NULL,
                likes          INTEGER NOT NULL DEFAULT 0,
                platforms      TEXT NOT NULL,
                views_service  TEXT,
                likes_service  TEXT,
                daily_cap_usd  REAL,
                active         INTEGER NOT NULL DEFAULT 1,
                last_polled_at INTEGER,
                last_error     TEXT,
                created_at     INTEGER NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS queue (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                link_id       INTEGER NOT NULL,
                campaign_id   INTEGER,
                campaign_name TEXT,
                submission_id INTEGER NOT NULL,
                url           TEXT NOT NULL,
                url_key       TEXT NOT NULL,
                platform      TEXT,
                clipper       TEXT,
                submitted_at  TEXT,
                state         TEXT NOT NULL,
                provider      TEXT,
                views         INTEGER,
                likes         INTEGER,
                est_cost      REAL,
                job_id        INTEGER,
                error         TEXT,
                created_at    INTEGER NOT NULL,
                placed_at     INTEGER,
                UNIQUE(link_id, submission_id)
            )
            """
        )
        have = {r["name"] for r in c.execute("PRAGMA table_info(links)")}
        for col, typ in (("auto_since", "INTEGER"), ("services", "TEXT")):
            if col not in have:
                c.execute(f"ALTER TABLE links ADD COLUMN {col} {typ}")
        # v2.2: backfill:false rows are `not_sent` (approvable), no longer `skipped`.
        c.execute("UPDATE queue SET state='not_sent', error=NULL "
                  "WHERE state='skipped' AND error='existed before link'")
        # v2.5: small key/value store (panel default overrides from score.py)
        c.execute("CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        c.execute("CREATE INDEX IF NOT EXISTS jobs_url_key ON jobs(url_key)")
        c.execute("CREATE INDEX IF NOT EXISTS queue_url_key ON queue(url_key)")
        c.execute("CREATE INDEX IF NOT EXISTS queue_state ON queue(state)")
        # v2.7 Automation (automation.py). Settings keys: auto:global_daily_cap_usd, auto:low_balance_usd, auto:lock
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                mode TEXT NOT NULL DEFAULT 'practice', weekly_budget_usd REAL,
                views_goal INTEGER, start_date TEXT, end_date TEXT, creators_planned INTEGER,
                posts_per_day INTEGER, platforms TEXT, ramp_weeks INTEGER DEFAULT 3, check_times TEXT,
                slot_deadlines TEXT, source TEXT, source_ref TEXT, approved INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS plan_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id TEXT NOT NULL UNIQUE,
                campaign_id INTEGER NOT NULL, creator_id TEXT, post_url TEXT NOT NULL, url_key TEXT NOT NULL,
                platform TEXT NOT NULL, account_handle TEXT, window_start INTEGER NOT NULL,
                window_end INTEGER NOT NULL, day TEXT NOT NULL, views_add INTEGER NOT NULL DEFAULT 0,
                likes_add INTEGER NOT NULL DEFAULT 0, target_total_views INTEGER, reason TEXT, priority INTEGER,
                carry INTEGER, created_at INTEGER NOT NULL   -- carry: views still owed (NULL = untouched)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id TEXT NOT NULL, campaign_id INTEGER,
                url_key TEXT, day TEXT NOT NULL, kind TEXT NOT NULL, provider TEXT, service TEXT,
                qty INTEGER NOT NULL DEFAULT 0, est_cost REAL NOT NULL DEFAULT 0, state TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0, retry_of INTEGER, is_test INTEGER NOT NULL DEFAULT 0,
                job_id INTEGER, order_id TEXT, error TEXT, created_at INTEGER NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS pauses (
                id INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL, key TEXT NOT NULL, label TEXT,
                created_by TEXT, created_at INTEGER NOT NULL, UNIQUE(scope, key)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, username TEXT,
                action TEXT NOT NULL, target TEXT, detail TEXT
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS plan_rows_day ON plan_rows(day)")
        c.execute("CREATE INDEX IF NOT EXISTS plan_rows_post ON plan_rows(campaign_id, url_key)")
        c.execute("CREATE INDEX IF NOT EXISTS plan_rows_key ON plan_rows(url_key)")
        c.execute("CREATE INDEX IF NOT EXISTS auto_orders_plan ON auto_orders(plan_id)")
        c.execute("CREATE INDEX IF NOT EXISTS auto_orders_state ON auto_orders(state)")
    backfill_job_keys()


_JOB_COLS = ("kind", "provider", "platform", "link", "views_target", "likes_target",
             "comments_target", "comment_style", "comment_lines", "views_service",
             "likes_service", "comments_service", "views_order_id", "likes_order_id",
             "comments_order_id", "likes_ordered", "baseline_likes", "state",
             "owner_handle", "created_at", "next_poll_at", "created_by", "source", "url_key")


def create_job(**f) -> int:
    from config import url_key   # lazy: keeps db importable without config
    f.setdefault("url_key", url_key(f["link"]))
    cols = [c for c in _JOB_COLS if c in f]
    with _conn() as c:
        cur = c.execute(
            f"INSERT INTO jobs ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
            tuple(f[k] for k in cols),
        )
        return cur.lastrowid


def backfill_job_keys() -> int:
    """v2.2: give pre-existing jobs a url_key so manual dedupe is an indexed lookup."""
    from config import url_key
    with _conn() as c:
        rows = c.execute("SELECT id, link FROM jobs WHERE url_key IS NULL").fetchall()
        c.executemany("UPDATE jobs SET url_key=? WHERE id=?", [(url_key(r["link"]), r["id"]) for r in rows])
        # v2.4: YouTube posts key on the video id (yt:<id>). Re-key older YouTube rows once.
        yt = c.execute("SELECT id, link FROM jobs WHERE link LIKE '%youtu%' AND url_key NOT LIKE 'yt:%'").fetchall()
        c.executemany("UPDATE jobs SET url_key=? WHERE id=?", [(url_key(r["link"]), r["id"]) for r in yt])
        yq = c.execute("SELECT id, url FROM queue WHERE url LIKE '%youtu%' AND url_key NOT LIKE 'yt:%'").fetchall()
        c.executemany("UPDATE queue SET url_key=? WHERE id=?", [(url_key(r["url"]), r["id"]) for r in yq])
        return len(rows)


def get_job(job_id: int) -> dict | None:
    with _conn() as c:
        row = c.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row else None


def update_job(job_id: int, **fields) -> None:
    if not fields:
        return
    sets = ", ".join(f"{k} = ?" for k in fields)
    with _conn() as c:
        c.execute(f"UPDATE jobs SET {sets} WHERE id = ?", (*fields.values(), job_id))


def list_jobs(kind: str = "eng", provider: str | None = None, state: str | None = None,
              limit: int = 20, offset: int = 0) -> list[dict]:
    where = ["COALESCE(kind,'eng') = ?"]
    args: list = [kind]
    if provider and provider != "all":
        where.append("provider = ?"); args.append(provider)
    if state and state != "all":
        where.append("state = ?"); args.append(state)
    with _conn() as c:
        rows = c.execute(
            f"SELECT * FROM jobs WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT ? OFFSET ?",
            (*args, limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]


def count_jobs(kind: str = "eng", provider: str | None = None, state: str | None = None) -> int:
    where = ["COALESCE(kind,'eng') = ?"]
    args: list = [kind]
    if provider and provider != "all":
        where.append("provider = ?"); args.append(provider)
    if state and state != "all":
        where.append("state = ?"); args.append(state)
    with _conn() as c:
        row = c.execute(f"SELECT COUNT(*) n FROM jobs WHERE {' AND '.join(where)}", tuple(args)).fetchone()
        return row["n"] if row else 0


def due_jobs(now_ts: int) -> list[dict]:
    qmarks = ",".join("?" * len(ACTIVE_JOB_STATES))
    with _conn() as c:
        rows = c.execute(
            f"SELECT * FROM jobs WHERE state IN ({qmarks}) "
            f"AND (next_poll_at IS NULL OR next_poll_at <= ?) "
            f"AND COALESCE(source, '') <> 'auto' ORDER BY id",  # auto jobs: panel status only, no scrape polling
            (*ACTIVE_JOB_STATES, now_ts),
        ).fetchall()
        return [dict(r) for r in rows]


def add_snapshot(job_id: int, t: int, views: int, likes: int, comments: int) -> None:
    with _conn() as c:
        c.execute("INSERT INTO snapshots (job_id, t, views, likes, comments) VALUES (?,?,?,?,?)",
                  (job_id, t, views, likes, comments))


def snapshots_for_job(job_id: int) -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT t, views, likes, comments FROM snapshots WHERE job_id = ? ORDER BY t",
                         (job_id,)).fetchall()
        return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# Generic helpers (v2: links, queue, speed)
# --------------------------------------------------------------------------- #
def q(sql: str, args: tuple = ()) -> list[dict]:
    with _conn() as c:
        return [dict(r) for r in c.execute(sql, args).fetchall()]


def q1(sql: str, args: tuple = ()) -> dict | None:
    rows = q(sql, args)
    return rows[0] if rows else None


def ex(sql: str, args: tuple = ()) -> tuple[int, int]:
    """Execute a write; returns (rowcount, lastrowid)."""
    with _conn() as c:
        cur = c.execute(sql, args)
        return cur.rowcount, cur.lastrowid


def insert(table: str, **f) -> int:
    return ex(f"INSERT INTO {table} ({','.join(f)}) VALUES ({','.join('?' * len(f))})",
              tuple(f.values()))[1]


def update(table: str, row_id: int, **f) -> None:
    if f:
        ex(f"UPDATE {table} SET {', '.join(f'{k} = ?' for k in f)} WHERE id = ?",
           (*f.values(), row_id))


# --------------------------------------------------------------------------- #
# v2.8 Planner tables (planner.py). Appended as a wrapper so the v2.7 init above stays untouched.
# --------------------------------------------------------------------------- #
_init_v27 = init_db


def init_db() -> None:
    _init_v27()
    with _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT, post_key TEXT NOT NULL UNIQUE, url_key TEXT NOT NULL,
                campaign_id INTEGER NOT NULL, creator_id TEXT, account_handle TEXT, platform TEXT NOT NULL,
                url TEXT NOT NULL, source TEXT, slot TEXT, day TEXT NOT NULL, uploaded_at INTEGER NOT NULL,
                t24 INTEGER, w_random REAL NOT NULL, p_likes REAL NOT NULL,
                reason TEXT NOT NULL DEFAULT 'base',      -- base | viral | decay
                peak INTEGER, target_total INTEGER, ramp_start TEXT NOT NULL,
                viral_state TEXT,                          -- NULL | tested | picked | approved | skipped
                viral_parent INTEGER,                      -- decay posts: the viral post id
                status TEXT NOT NULL DEFAULT 'active',     -- active | deleted
                observed_views INTEGER, observed_likes INTEGER, checked_at INTEGER,
                misses INTEGER NOT NULL DEFAULT 0,         -- unreadable reads in a row (3 = stop checking)
                skip_next INTEGER NOT NULL DEFAULT 0,      -- 1 = sit out the next run (backoff)
                not_found INTEGER NOT NULL DEFAULT 0,      -- not-found reads in a row (2 = deleted)
                created_at INTEGER NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_creators (
                campaign_id INTEGER NOT NULL, creator_id TEXT NOT NULL, cancelled INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (campaign_id, creator_id)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_slots (
                campaign_id INTEGER NOT NULL, creator_id TEXT NOT NULL, day TEXT NOT NULL, slot TEXT NOT NULL,
                url TEXT, PRIMARY KEY (campaign_id, creator_id, day, slot)
            )
            """
        )
        c.execute(   # v2.9: sheet Creator name -> SideShift contractorId (canonical creator_id when a campaign has SideShift)
            """
            CREATE TABLE IF NOT EXISTS auto_creator_map (
                campaign_id INTEGER NOT NULL, name_key TEXT NOT NULL, name TEXT NOT NULL, creator_id TEXT NOT NULL,
                PRIMARY KEY (campaign_id, name_key)
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS auto_posts_camp ON auto_posts(campaign_id, platform)")
        c.execute("CREATE INDEX IF NOT EXISTS auto_posts_acct ON auto_posts(account_handle, platform)")
        c.execute("CREATE INDEX IF NOT EXISTS auto_posts_creator ON auto_posts(creator_id, platform)")
        if "comments_add" not in {r["name"] for r in c.execute("PRAGMA table_info(plan_rows)")}:
            c.execute("ALTER TABLE plan_rows ADD COLUMN comments_add INTEGER NOT NULL DEFAULT 0")
        # v2.10: normal / decay ramps start at the upload day (was: first-seen day). Viral keeps its approval day.
        c.execute("UPDATE auto_posts SET ramp_start=day WHERE reason IN ('base','decay') AND ramp_start > day")
        have = {r["name"] for r in c.execute("PRAGMA table_info(auto_posts)")}
        for col, typ in (("base_views", "INTEGER"), ("base_day", "TEXT"),   # v2.10: first views read + its ET day
                         ("viral_base", "INTEGER")):                          # v2.10.2: views at viral approval
            if col not in have:
                c.execute(f"ALTER TABLE auto_posts ADD COLUMN {col} {typ}")
        et = ZoneInfo("America/New_York")
        c.executemany("UPDATE auto_posts SET base_views=observed_views, base_day=? WHERE id=?",
                      [(max(r["day"], dt.datetime.fromtimestamp(r["checked_at"], et).date().isoformat()), r["id"])
                       for r in c.execute("SELECT id, day, checked_at FROM auto_posts WHERE base_views IS NULL "
                                          "AND checked_at IS NOT NULL AND observed_views IS NOT NULL").fetchall()])
        # v2.11 All submissions + Buy again: clipper email on queue rows, one rebuys row per manual 2nd buy.
        if "clipper_email" not in {r["name"] for r in c.execute("PRAGMA table_info(queue)")}:
            c.execute("ALTER TABLE queue ADD COLUMN clipper_email TEXT")
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS rebuys (
                id INTEGER PRIMARY KEY AUTOINCREMENT, queue_id INTEGER NOT NULL, link_id INTEGER NOT NULL,
                url_key TEXT NOT NULL, state TEXT NOT NULL,   -- placing | placed | failed
                provider TEXT, views INTEGER NOT NULL, likes INTEGER NOT NULL DEFAULT 0, est_cost REAL,
                job_id INTEGER, error TEXT, created_by TEXT, created_at INTEGER NOT NULL
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS rebuys_queue ON rebuys(queue_id)")
        c.execute("CREATE INDEX IF NOT EXISTS rebuys_key ON rebuys(url_key)")
        # v2.12 calculator + campaign cap: per platform posts per creator per day, split, margin, chosen services.
        # weekly_budget_usd stays (old campaigns still honour it); ramp_weeks stays (planner reads it).
        have = {r["name"] for r in c.execute("PRAGMA table_info(auto_campaigns)")}
        for col, typ in (("cap_usd", "REAL"), ("posts_per_creator", "TEXT"), ("platform_split", "TEXT"),
                         ("margin_pct", "REAL NOT NULL DEFAULT 12.5"), ("services", "TEXT")):
            if col not in have:
                c.execute(f"ALTER TABLE auto_campaigns ADD COLUMN {col} {typ}")
        # v2.14 creator include list: 0 = admin un-ticked the creator (ignored). No row / 1 = included.
        if "included" not in {r["name"] for r in c.execute("PRAGMA table_info(auto_creators)")}:
            c.execute("ALTER TABLE auto_creators ADD COLUMN included INTEGER NOT NULL DEFAULT 1")
        # v2.16 comments + shares: per campaign ratio per 1k bought views (0 = never bought), plan amounts, observed comments.
        for t, col, typ in (("auto_campaigns", "comments_per_1k", "REAL NOT NULL DEFAULT 0"),
                            ("auto_campaigns", "shares_per_1k", "REAL NOT NULL DEFAULT 0"),
                            ("plan_rows", "shares_add", "INTEGER NOT NULL DEFAULT 0"),
                            ("auto_posts", "observed_comments", "INTEGER")):
            if col not in {r["name"] for r in c.execute(f"PRAGMA table_info({t})")}:
                c.execute(f"ALTER TABLE {t} ADD COLUMN {col} {typ}")
        # v2.18 custom comment pool: one list per campaign (shared by every platform) + which line went on which post.
        c.execute("""CREATE TABLE IF NOT EXISTS comment_pool (
                id INTEGER PRIMARY KEY AUTOINCREMENT, campaign_id INTEGER NOT NULL, text TEXT NOT NULL,
                times_used INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, UNIQUE (campaign_id, text))""")
        c.execute("""CREATE TABLE IF NOT EXISTS comment_used (
                id INTEGER PRIMARY KEY AUTOINCREMENT, campaign_id INTEGER NOT NULL, url_key TEXT NOT NULL,
                line_id INTEGER, text TEXT NOT NULL, order_id INTEGER NOT NULL, at INTEGER NOT NULL)""")
        c.execute("CREATE INDEX IF NOT EXISTS comment_used_key ON comment_used(url_key)")
        if "comments" not in {r["name"] for r in c.execute("PRAGMA table_info(auto_orders)")}:
            c.execute("ALTER TABLE auto_orders ADD COLUMN comments TEXT")   # JSON list of the lines sent (custom comments)
