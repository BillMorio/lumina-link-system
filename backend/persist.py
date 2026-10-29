"""Console SQLite durability.

The console keeps its own state (automation campaigns, plans, orders, users, settings)
in SQLite, and on Render that file lives on the ephemeral /tmp disk, so every deploy
used to wipe it. This module keeps the file durable without touching any SQL:

- boot: if no database file exists yet, restore the latest backup from Postgres;
- every CONSOLE_BACKUP_SEC (30 s) the file is copied (sqlite backup API: consistent
  even mid-write), gzipped and stored in Postgres when its content changed;
- shutdown (Render stops the old instance on every deploy): one final backup.

Safety: if Postgres was unreachable at boot, backups stay OFF, so an empty fresh
database can never overwrite the real backup. The last KEEP versions are kept.
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import logging
import os
import sqlite3
import tempfile
import time

import db
import pg

log = logging.getLogger("persist")

BACKUP_SEC = max(10, int(os.getenv("CONSOLE_BACKUP_SEC", "30")))
KEEP = 96
SCHEMA = """CREATE TABLE IF NOT EXISTS console_backups (
    id       BIGSERIAL PRIMARY KEY,
    saved_at BIGINT NOT NULL,
    reason   TEXT NOT NULL DEFAULT '',
    size     BIGINT NOT NULL,
    sha      TEXT NOT NULL,
    data     BYTEA NOT NULL)"""

state = {"enabled": False, "restored": None, "last_backup": None, "last_reason": None,
         "backups": 0, "size": None, "last_error": None, "interval_sec": BACKUP_SEC}
_sha: str | None = None
_task = None


def _dump() -> tuple[bytes, str]:
    """-> (raw database bytes, sha256). A consistent copy even while the app writes (WAL)."""
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        src, dst = sqlite3.connect(db.DB_PATH, timeout=30), sqlite3.connect(tmp)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        with open(tmp, "rb") as f:
            raw = f.read()
        return raw, hashlib.sha256(raw).hexdigest()
    finally:
        os.remove(tmp)


async def restore() -> None:
    """Call after pg.init() and BEFORE db.init_db()."""
    global _sha
    if not pg.available():
        state["restored"] = "Postgres unavailable at boot: backups disabled"
        log.warning("console backup disabled: Postgres unavailable at boot")
        return
    async with pg._pool.acquire() as c:
        await c.execute(SCHEMA)
        row = await c.fetchrow("SELECT id, saved_at, sha, data FROM console_backups ORDER BY id DESC LIMIT 1")
    if os.path.exists(db.DB_PATH) and os.path.getsize(db.DB_PATH) > 0:
        state["restored"] = "kept the database already on disk"   # warm restart of the same container
    elif row:
        os.makedirs(os.path.dirname(db.DB_PATH) or ".", exist_ok=True)
        for ext in ("-wal", "-shm"):   # stale journal from a different database must not be replayed
            if os.path.exists(db.DB_PATH + ext):
                os.remove(db.DB_PATH + ext)
        raw = gzip.decompress(row["data"])
        with open(db.DB_PATH, "wb") as f:
            f.write(raw)
        _sha = row["sha"]
        state["restored"] = f"backup #{row['id']} saved {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(row['saved_at']))} UTC"
        log.info("console database restored from %s (%d bytes)", state["restored"], len(raw))
    else:
        state["restored"] = "no backup yet: fresh database"
    state["enabled"] = True


async def backup(reason: str = "interval") -> bool:
    """-> True when a new version was stored."""
    global _sha
    if not state["enabled"] or not pg.available() or not os.path.exists(db.DB_PATH):
        return False
    raw, sha = await asyncio.to_thread(_dump)
    if sha == _sha:
        return False
    data = gzip.compress(raw, 6)
    async with pg._pool.acquire() as c:
        await c.execute("INSERT INTO console_backups (saved_at, reason, size, sha, data) VALUES ($1,$2,$3,$4,$5)",
                        int(time.time()), reason, len(raw), sha, data)
        await c.execute("DELETE FROM console_backups WHERE id NOT IN "
                        "(SELECT id FROM console_backups ORDER BY id DESC LIMIT $1)", KEEP)
    _sha = sha
    state.update(last_backup=int(time.time()), last_reason=reason, size=len(raw), last_error=None)
    state["backups"] += 1
    return True


async def _loop() -> None:
    while True:
        await asyncio.sleep(BACKUP_SEC)
        try:
            await backup()
        except Exception as e:  # noqa: BLE001
            state["last_error"] = str(e)[:200]
            log.warning("console backup failed: %s", e)


def start() -> None:
    global _task
    if state["enabled"]:
        _task = asyncio.create_task(_loop())


async def shutdown() -> None:
    try:
        if await backup("shutdown"):
            log.info("console database backed up on shutdown")
    except Exception as e:  # noqa: BLE001
        log.warning("shutdown backup failed: %s", e)
