"""Phase 5: AI comment suggestions per post.

context:  Instagram -> real transcript (video downloader, audio only -> Deepgram);
          everything else (Facebook downloads are broken upstream) -> the post's caption.
          Falls back to the caption when a download / transcript fails or the audio has no speech.
          One transcript per post, cached in post_transcripts.
generate: Gemini writes N candidate comments from campaign brief + context; a person approves them.
          Approved lines become that post's own lines (automation.pick_lines uses them first).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time

import httpx

import automation
import db
from config import url_key as canon_key

log = logging.getLogger("comments_ai")

GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-flash-latest")
DEEPGRAM_KEY = os.getenv("DEEPGRAM_API_KEY", "")
DL_BASE = os.getenv("VIDEO_DL_BASE", "https://lumina-video-downloader-api.onrender.com").rstrip("/")
DL_TIMEOUT = 120          # s: queue + download + audio extract on the downloader
MAX_AUDIO = 40 * 1024 * 1024
MAX_LINE = 140
_BAD = re.compile(r"https?://|www\.|@\w|#\w", re.I)


class AIError(RuntimeError):
    pass


# ---------------------------------------------------------------- brief
def get_brief(cid: int) -> dict:
    r = db.q1("SELECT brief, tone, updated_at FROM comment_briefs WHERE campaign_id=?", (cid,))
    return dict(r) if r else {"brief": "", "tone": "", "updated_at": None}


def set_brief(cid: int, brief: str, tone: str) -> dict:
    db.ex("INSERT INTO comment_briefs (campaign_id, brief, tone, updated_at) VALUES (?,?,?,?) "
          "ON CONFLICT(campaign_id) DO UPDATE SET brief=excluded.brief, tone=excluded.tone, updated_at=excluded.updated_at",
          (cid, brief.strip()[:4000], tone.strip()[:300], int(time.time())))
    return get_brief(cid)


# ---------------------------------------------------------------- context
async def _download_audio(url: str) -> tuple[bytes, str]:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(f"{DL_BASE}/api/download", json={"url": url, "audio_only": True})
        if r.status_code != 200:
            raise AIError(f"downloader: HTTP {r.status_code} {r.text[:120]}")
        job = r.json()["job_id"]
        end = time.monotonic() + DL_TIMEOUT
        while True:
            s = (await c.get(f"{DL_BASE}/api/status/{job}")).json()
            if s.get("status") == "ready":
                break
            if s.get("status") == "error":
                raise AIError(f"downloader: {s.get('error') or 'failed'}")
            if time.monotonic() > end:
                raise AIError("downloader: timed out")
            await asyncio.sleep(2)
        f = await c.get(f"{DL_BASE}/api/file/{job}", timeout=120)
        if f.status_code != 200:
            raise AIError(f"downloader file: HTTP {f.status_code}")
        if len(f.content) > MAX_AUDIO:
            raise AIError("downloader: audio too large")
        return f.content, f.headers.get("content-type") or "audio/mpeg"


async def _deepgram(audio: bytes, ctype: str) -> tuple[str, str | None]:
    if not DEEPGRAM_KEY:
        raise AIError("DEEPGRAM_API_KEY not set")
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.post("https://api.deepgram.com/v1/listen",
                         params={"model": "nova-2", "smart_format": "true", "detect_language": "true"},
                         headers={"Authorization": f"Token {DEEPGRAM_KEY}", "Content-Type": ctype}, content=audio)
    if r.status_code != 200:
        raise AIError(f"deepgram: HTTP {r.status_code} {r.text[:120]}")
    ch = (r.json().get("results") or {}).get("channels") or [{}]
    alt = (ch[0].get("alternatives") or [{}])[0]
    return (alt.get("transcript") or "").strip(), ch[0].get("detected_language")


async def context(url: str, platform: str, caption: str) -> dict:
    """-> {source: transcript | caption | none, text, lang, error}. Cached per post."""
    key = canon_key(url)
    row = await asyncio.to_thread(db.q1, "SELECT * FROM post_transcripts WHERE url_key=?", (key,))
    if row and (row["source"] == "transcript" or platform != "instagram"):
        return dict(row)
    text, lang, err, src = "", None, None, "none"
    if platform == "instagram":
        try:
            audio, ct = await _download_audio(url)
            text, lang = await _deepgram(audio, ct)
            src = "transcript" if text else "none"
            if not text:
                err = "no speech in the audio"
        except Exception as e:  # noqa: BLE001
            err = str(e)[:200]
            log.info("transcript for %s failed: %s", key, err)
    if not text and caption:
        text, src = caption.strip(), "caption"
    rec = {"url_key": key, "source": src, "text": text[:8000], "lang": lang, "error": err, "at": int(time.time())}
    await asyncio.to_thread(db.ex, "INSERT INTO post_transcripts (url_key, source, text, lang, error, at) VALUES (?,?,?,?,?,?) "
                            "ON CONFLICT(url_key) DO UPDATE SET source=excluded.source, text=excluded.text, lang=excluded.lang, "
                            "error=excluded.error, at=excluded.at", tuple(rec.values()))
    return rec


# ---------------------------------------------------------------- generation
def _prompt(platform: str, brief: dict, ctx: dict, avoid: list[str], n: int) -> str:
    what = {"transcript": "Transcript of the video's audio", "caption": "The post's caption",
            "none": "No transcript or caption is available"}[ctx["source"]]
    return f"""You write comments that real viewers leave under short-form videos on {platform or 'social media'}.

Campaign brief (follow it; it overrides the style notes below where they conflict):
{brief.get('brief') or '(no brief given: react naturally to the video)'}
Tone: {brief.get('tone') or 'casual, warm, playful'}

{what}:
{ctx.get('text') or '(none)'}

Write {n} different comments for THIS video.
Style:
- Sound like different real people scrolling: casual, mostly lowercase, short (most 3-12 words).
- About a third are emoji-only reactions (2-4 emojis that fit the video); the rest are short reactions, ideally
  pointing at a specific moment, line or detail from the video.
- Vary openings, length and emoji; no two comments should read alike.
- Never: hashtags, links, @mentions, prices, "buy/sign up/check out", follow-for-follow, questions asking the
  creator to do something, anything that sounds like an ad, a bot or an AI. No profanity.
- Do not repeat or lightly reword any of these existing comments: {json.dumps(avoid[-60:], ensure_ascii=False)}

Return only a JSON array of {n} strings."""


async def _gemini(prompt: str) -> list[str]:
    if not GEMINI_KEY:
        raise AIError("GEMINI_API_KEY not set")
    body = {"contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 1.0, "responseMimeType": "application/json",
                                 "responseSchema": {"type": "ARRAY", "items": {"type": "STRING"}}}}
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
                         params={"key": GEMINI_KEY}, json=body)
    if r.status_code != 200:
        raise AIError(f"gemini: HTTP {r.status_code} {r.text[:160]}")
    try:
        txt = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        out = json.loads(txt)
    except (KeyError, IndexError, ValueError) as e:
        raise AIError(f"gemini: unreadable answer ({e})") from e
    return [str(x) for x in out if isinstance(x, (str, int, float))]


def clean(lines: list[str], avoid: set[str]) -> list[str]:
    out, seen = [], {a.strip().lower() for a in avoid}
    for t in lines:
        t = re.sub(r"\s+", " ", str(t)).strip().strip('"').strip()
        if not t or len(t) > MAX_LINE or _BAD.search(t) or t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t)
    return out


async def suggest(cid: int, url: str, platform: str, caption: str, n: int = 10) -> dict:
    key = canon_key(url)
    ctx = await context(url, platform, caption)
    brief = await asyncio.to_thread(get_brief, cid)

    def existing():
        s = {r["text"] for r in db.q("SELECT text FROM comment_suggestions WHERE campaign_id=? AND url_key=?", (cid, key))}
        return s | automation.used_texts(key)
    avoid = await asyncio.to_thread(existing)
    lines = clean(await _gemini(_prompt(platform, brief, ctx, sorted(avoid), n)), avoid)
    now = int(time.time())

    def store():
        return sum(db.ex("INSERT OR IGNORE INTO comment_suggestions (campaign_id, url_key, text, status, model, created_at) "
                         "VALUES (?,?,?,?,?,?)", (cid, key, t, "suggested", GEMINI_MODEL, now))[0] for t in lines)
    added = await asyncio.to_thread(store)
    return {"added": added, "model": GEMINI_MODEL, "context": {k: ctx.get(k) for k in ("source", "lang", "error")}}


# ---------------------------------------------------------------- review
def post_view(cid: int, key: str) -> dict:
    t = db.q1("SELECT source, text, lang, error, at FROM post_transcripts WHERE url_key=?", (key,))
    s = db.q("SELECT id, text, status, model, created_at, decided_at FROM comment_suggestions "
             "WHERE campaign_id=? AND url_key=? ORDER BY status='rejected', id", (cid, key))
    b = get_brief(cid)
    return {"context": dict(t) if t else None, "suggestions": s, "brief_set": bool(b.get("brief")), "brief": b,
            "ready": bool(GEMINI_KEY), "transcribe": bool(DEEPGRAM_KEY)}


def decide(cid: int, sid: int, action: str, text: str | None = None) -> dict:
    r = db.q1("SELECT * FROM comment_suggestions WHERE id=? AND campaign_id=?", (sid, cid))
    if not r:
        raise LookupError("no such suggestion")
    if action not in ("approve", "reject", "restore"):
        raise ValueError("action must be approve, reject or restore")
    new = r["text"]
    if text is not None:
        c = clean([text], set())
        if not c:
            raise ValueError("comment is empty, too long or has a link / @ / #")
        new = c[0]
    status = {"approve": "approved", "reject": "rejected", "restore": "suggested"}[action]
    try:
        db.ex("UPDATE comment_suggestions SET text=?, status=?, decided_at=? WHERE id=?", (new, status, int(time.time()), sid))
    except Exception as e:  # noqa: BLE001  UNIQUE(campaign, post, text)
        raise ValueError("that exact comment already exists for this post") from e
    return dict(db.q1("SELECT id, text, status FROM comment_suggestions WHERE id=?", (sid,)))
