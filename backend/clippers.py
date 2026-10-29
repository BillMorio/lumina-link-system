"""Read-only client for the Lumina Clippers admin API.

Only GETs (plus the login POST). Token is cached; a 401 triggers one re-login.
Repeated login failures back off exponentially (30s .. 10min) and are surfaced
via `status()` for /api/alerts.
"""
from __future__ import annotations

import asyncio
import re
import time

import httpx

import config

TIMEOUT = 90.0          # staging cold-starts are slow
PER_PAGE = 200          # API caps per_page at 200


class ClippersError(Exception):
    pass


_token: str | None = None
_fail_count = 0
_next_try_at = 0.0
_last_error: str | None = None
_last_error_at: int | None = None
_lock = asyncio.Lock()


def status() -> dict:
    return {"error": _last_error, "at": _last_error_at, "fail_count": _fail_count}


def http_error(path: str, code: int, body: str) -> str:
    """Short readable text, never a raw HTML error page."""
    if code >= 500:
        return f"Clippers source unavailable (HTTP {code}), retrying"
    b = "" if re.match(r"\s*<", body or "") else " " + " ".join((body or "").split())[:80]
    return f"Clippers API {path}: HTTP {code}{b}"


def _fail(msg: str | None) -> None:
    global _last_error, _last_error_at
    _last_error, _last_error_at = msg, int(time.time()) if msg else None


async def _login() -> str:
    global _token, _fail_count, _next_try_at, _last_error
    async with _lock:
        if _token:
            return _token
        if not config.CLIPPERS_API_BASE or not config.CLIPPERS_ADMIN_EMAIL:
            _fail("Clippers API not configured (CLIPPERS_API_BASE / CLIPPERS_ADMIN_EMAIL)")
            raise ClippersError(_last_error)
        if time.time() < _next_try_at:
            raise ClippersError(f"Clippers login backing off: {_last_error}")
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT) as c:
                r = await c.post(f"{config.CLIPPERS_API_BASE}/api/auth/login",
                                 json={"email": config.CLIPPERS_ADMIN_EMAIL,
                                       "password": config.CLIPPERS_ADMIN_PASSWORD})
            if r.status_code != 200:
                raise ClippersError("Clippers login failed: " + http_error("/api/auth/login", r.status_code, r.text).split(": ", 1)[-1])
            _token = r.json()["access_token"]
        except Exception as e:  # noqa: BLE001
            _fail_count += 1
            _next_try_at = time.time() + min(600, 30 * 2 ** (_fail_count - 1))
            _fail(str(e)[:160] if isinstance(e, ClippersError) else f"Clippers login error: {e!s}"[:160])
            raise ClippersError(_last_error) from e
        _fail_count, _last_error = 0, None
        return _token


async def _get(path: str, params: dict | None = None):
    global _token
    for attempt in (0, 1):
        tok = await _login()
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT) as c:
                r = await c.get(f"{config.CLIPPERS_API_BASE}{path}", params=params,
                                headers={"Authorization": f"Bearer {tok}"})
        except httpx.HTTPError as e:
            _fail(f"Clippers API unreachable: {e!s}"[:160])
            raise ClippersError(_last_error) from e
        if r.status_code == 401 and attempt == 0:
            _token = None          # expired -> re-login once
            continue
        if r.status_code != 200:   # v2.17: per-path error on the caller (link last_error), not the global banner
            raise ClippersError(http_error(path, r.status_code, r.text))
        if _last_error and not _last_error.startswith("Clippers API not configured"):
            _fail(None)   # a good read clears a stale global error (unreachable, login)
        return r.json()
    raise ClippersError("Clippers API: unauthorized after re-login")


def _platforms(s: str | None) -> list[str]:
    out = [p.strip().lower() for p in (s or "").split(",") if p.strip()]
    return ["x" if p == "twitter" else p for p in out]


def abs_url(u: str | None) -> str:
    """Clippers may return a relative upload path: resolve it against CLIPPERS_API_BASE."""
    u = (u or "").strip()
    if u.startswith("http://"):
        return "https://" + u[7:]   # page is https: avoid mixed content
    if not u or u.startswith("https://"):
        return u
    if u.startswith("//"):
        return "https:" + u
    return abs_url(f"{config.CLIPPERS_API_BASE}/{u.lstrip('/')}")


_thumbs: dict[int, str] = {}   # campaign id -> banner url, from the last campaigns() call


def thumbnail_for(campaign_id: int) -> str:
    return _thumbs.get(campaign_id, "")


async def campaigns() -> list[dict]:
    rows = await _get("/api/campaigns")
    out = [{"id": c["id"], "name": c.get("name", ""), "status": c.get("status", ""),
            "platforms": _platforms(c.get("accepted_platforms")),
            "submission_count": c.get("submission_count"),
            "thumbnail_url": abs_url(c.get("thumbnail_url"))} for c in rows]
    _thumbs.update({c["id"]: c["thumbnail_url"] for c in out})
    return out


async def submissions(campaign_id: int) -> list[dict]:
    """Every visible submission on the campaign (all pages). The admin listing
    already hides rejected + suspicious rows."""
    out, page = [], 1
    while True:
        d = await _get(f"/api/campaigns/{campaign_id}/submissions",
                       {"page": page, "per_page": PER_PAGE})
        for s in d.get("items", []):
            plat = (s.get("platform") or "").lower()
            out.append({"id": int(s["id"]), "url": s.get("post_url") or "",
                        "platform": "x" if plat == "twitter" else plat,
                        "clipper": s.get("clipper_name") or s.get("clipper_email") or "",
                        "email": (s.get("clipper_email") or "").strip() or None,
                        "submitted_at": s.get("created_at"), "status": s.get("status")})
        if page >= int(d.get("pages") or 1):
            return out
        page += 1
