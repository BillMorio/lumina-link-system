"""Static config: providers, per-provider service maps, rates, detection.

Ported from the SMMGlow red-team bot so the console places against the exact same
PerfectPanel catalog. Service IDs are public catalog numbers (hardcoded per panel);
only the API keys live in env.
"""
from __future__ import annotations

import logging
import os
import re

from dotenv import load_dotenv

load_dotenv()

PLATFORM_LABEL = {
    "instagram": "Instagram", "tiktok": "TikTok", "youtube": "YouTube",
    "x": "X / Twitter", "facebook": "Facebook",
}
TYPE_LABEL = {"views": "Views", "likes": "Likes", "followers": "Followers"}

# --------------------------------------------------------------------------- #
# Providers (all PerfectPanel v2). Keys from env; service IDs hardcoded below.
# --------------------------------------------------------------------------- #
PROVIDERS = {
    "smmglow": {"label": "SMMGlow", "base_url": "https://smmglow.com/api/v2",
                "key_env": "SMMGLOW_API_KEY", "bot": "@luminagrowth_bot"},
    "morethanpanel": {"label": "MoreThanPanel", "base_url": "https://morethanpanel.com/api/v2",
                      "key_env": "MORETHANPANEL_API_KEY", "bot": "@luminareach_bot"},
    "justanotherpanel": {"label": "JustAnotherPanel", "base_url": "https://justanotherpanel.com/api/v2",
                         "key_env": "JAP_API_KEY", "view_dripfeed": True, "bot": "@lumina_jap_bot"},
}
DEFAULT_PROVIDER = "smmglow"

# Native panel drip-feed (runs/interval) for view orders on providers that support it (JAP).
DRIP_VIEW_RUNS = 12
DRIP_VIEW_INTERVAL_MIN = 15


def provider_view_dripfeed(provider: str) -> bool:
    return bool(PROVIDERS.get(provider, {}).get("view_dripfeed"))


SERVICE_MAP: dict[str, dict[tuple[str, str], str]] = {
    "smmglow": {
        ("instagram", "views"): "26146", ("tiktok", "views"): "9733",
        ("youtube", "views"): "24575", ("x", "views"): "22143",
        ("facebook", "views"): "5227",
        ("instagram", "likes"): "26330", ("tiktok", "likes"): "16580",
        ("youtube", "likes"): "24492", ("facebook", "likes"): "22457",
    },
    "morethanpanel": {
        ("instagram", "views"): "7662", ("tiktok", "views"): "9856",
        ("instagram", "likes"): "2499", ("tiktok", "likes"): "1126",
        ("facebook", "views"): "7654", ("facebook", "likes"): "5133",
        ("youtube", "views"): "5714", ("youtube", "likes"): "1857",
    },
    "justanotherpanel": {
        ("instagram", "views"): "3526", ("instagram", "likes"): "5714",
        ("tiktok", "views"): "7513", ("tiktok", "likes"): "9398",
        ("facebook", "views"): "6160", ("facebook", "likes"): "8703",
        ("youtube", "views"): "4152", ("youtube", "likes"): "2250",
    },
}

COMMENT_MAP: dict[str, dict[tuple[str, str], str]] = {
    "smmglow": {("instagram", "custom"): "26977", ("instagram", "emoji"): "26976",
                ("tiktok", "custom"): "12128", ("tiktok", "emoji"): "6393"},
    "morethanpanel": {("instagram", "custom"): "4641", ("instagram", "emoji"): "6227",
                      ("tiktok", "custom"): "9848", ("tiktok", "emoji"): "9848"},
    "justanotherpanel": {("instagram", "custom"): "5898"},
}

# Per-1000 USD fallback when the live catalog is unreachable (synced to live 2026-10-22).
RATE_MAP: dict[str, dict[tuple[str, str], float]] = {
    "smmglow": {("instagram", "views"): 0.04, ("tiktok", "views"): 0.06,
                ("youtube", "views"): 0.83, ("x", "views"): 0.02, ("facebook", "views"): 0.06,
                ("instagram", "likes"): 0.25, ("tiktok", "likes"): 1.05,
                ("youtube", "likes"): 0.88, ("facebook", "likes"): 0.27,
                ("instagram", "comments"): 1.50, ("tiktok", "comments"): 3.75},
    "morethanpanel": {("instagram", "views"): 0.0686, ("tiktok", "views"): 0.0294,
                      ("instagram", "likes"): 0.1176, ("tiktok", "likes"): 0.1862,
                      ("facebook", "views"): 0.1176, ("facebook", "likes"): 0.1568,
                      ("youtube", "views"): 0.9212, ("youtube", "likes"): 0.5194,
                      ("instagram", "comments"): 0.8428, ("tiktok", "comments"): 6.125},
    "justanotherpanel": {("instagram", "views"): 0.26, ("instagram", "likes"): 2.08,
                         ("tiktok", "views"): 0.054, ("tiktok", "likes"): 0.3375,
                         ("facebook", "views"): 0.364, ("facebook", "likes"): 0.3558,
                         ("youtube", "views"): 0.8125, ("youtube", "likes"): 0.8125,
                         ("instagram", "comments"): 0.75},
}

ENGAGEMENT_PLATFORMS = ["instagram", "tiktok", "facebook", "youtube"]

# Drip scheduler cadence.
POLL_INTERVAL_MIN = 15
POLL_STEPS = [(30, 5), (90, 10)]   # <30m -> 5m, <90m -> 10m, else POLL_INTERVAL_MIN
LIKE_ORDER_MIN = 10
# v2.18 custom comments: starred defaults (first in the dropdown, pre-selected on new campaigns only)
COMMENT_DEFAULTS = {"instagram": ("morethanpanel", "8793"), "facebook": ("morethanpanel", "8439")}
COMMENT_MIN = 5            # a custom comments order needs at least this many lines (or the service min if higher)
COMMENT_RATE_DEFAULT = 5   # comments per 1,000 views when a Custom Comments service is picked and no rate typed
COMMENT_MAX_LEN = 300      # longest comment line accepted into a pool

# Account-aware ("smart") bulk targeting.
SMART_SAMPLE = 120
SMART_ANCHOR_PCT = 0.95
SMART_JITTER = 0.20
SMART_VIEWS_MIN = 100
SMART_VIEWS_MAX = 250000
SMART_LIKES_MIN = 10
SMART_MIN_LIKE_RATE = 0.02
SMART_TIER3_STRETCH = 1.5

# Simple shared access code for the console API (mirrors the bot's /login).
# Required: refuse to start without a real value (the old public default is rejected).
_OLD_DEFAULT_CODE = "lumina-glow-2026"
API_ACCESS_CODE = os.getenv("API_ACCESS_CODE", "").strip()
if not API_ACCESS_CODE or API_ACCESS_CODE == _OLD_DEFAULT_CODE:
    logging.getLogger("config").error(
        "API_ACCESS_CODE is %s; set a private value in the environment. Refusing to start.",
        "unset" if not API_ACCESS_CODE else "the old public default")
    raise RuntimeError("API_ACCESS_CODE must be set to a private value")


# Signs login tokens. Falls back to API_ACCESS_CODE; set it so rotating the code does not log everyone out.
SESSION_SECRET = os.getenv("SESSION_SECRET", "").strip()


def providers() -> list[str]:
    return list(PROVIDERS)


def provider_label(provider: str) -> str:
    return PROVIDERS.get(provider, {}).get("label", provider)


def provider_bot(provider: str) -> str:
    return PROVIDERS.get(provider, {}).get("bot", "")


# v2.5: (provider, platform, type) -> service, switched by score.py and stored in the DB settings
# table. Loaded at startup; every default lookup goes through service_for.
DEFAULT_OVERRIDES: dict[tuple[str, str, str], str] = {}


def service_for(provider: str, platform: str, otype: str) -> str:
    return DEFAULT_OVERRIDES.get((provider, platform, otype)) or SERVICE_MAP.get(provider, {}).get((platform, otype), "")


def effective_service_map() -> dict[str, dict[tuple[str, str], str]]:
    return {p: {k: service_for(p, *k) for k in d} for p, d in SERVICE_MAP.items()}


def comment_service(provider: str, platform: str, style: str) -> str:
    return COMMENT_MAP.get(provider, {}).get((platform, style), "")


def rate_for(provider: str, platform: str, otype: str) -> float:
    return RATE_MAP.get(provider, {}).get((platform, otype), 0.0)


def detect_platform(url: str) -> str | None:
    u = (url or "").lower()
    if "instagram.com" in u:
        return "instagram"
    if "tiktok.com" in u:
        return "tiktok"
    if "youtube.com" in u or "youtu.be" in u:
        return "youtube"
    if "twitter.com" in u or "x.com" in u:
        return "x"
    if "facebook.com" in u or "fb.watch" in u or "fb.com" in u:
        return "facebook"
    return None


# Post URL normalization (dedupe key for queue rows and jobs).
_IG = re.compile(r"instagram\.com/(?:[^/?#]+/)?(?:reels?|p|tv)/([A-Za-z0-9_-]+)", re.I)
_FB_ID = [
    re.compile(r"facebook\.com/(?:[^?#]*/)?(?:reel|videos|posts)/(?:[^/?#]*/)?(\d{6,})", re.I),
    re.compile(r"[?&](?:v|story_fbid|fbid|video_id)=(\d{6,})", re.I),
]
_YT = re.compile(r"(?:youtube\.com/(?:watch\?(?:[^#]*&)?v=|shorts/|embed/|live/)|youtu\.be/)([A-Za-z0-9_-]{6,})", re.I)


def url_key(url: str) -> str:
    u = (url or "").strip()
    m = _IG.search(u)
    if m:
        return f"ig:{m.group(1)}"
    m = _YT.search(u)   # watch?v= ids live in the query string the fallback drops
    if m:
        return f"yt:{m.group(1)}"
    if re.search(r"facebook\.com|fb\.watch|fb\.com", u, re.I):
        for rx in _FB_ID:
            m = rx.search(u)
            if m:
                return f"fb:{m.group(1)}"
    # Fallback: scheme/www/m./query/fragment/trailing-slash insensitive.
    u = re.sub(r"^https?://", "", u, flags=re.I).split("#")[0].split("?")[0]
    u = re.sub(r"^(www\.|m\.|web\.)", "", u, flags=re.I).rstrip("/")
    return u.lower()


# --------------------------------------------------------------------------- #
# v2: Lumina Clippers campaign links + scheduler scale
# --------------------------------------------------------------------------- #
CLIPPERS_API_BASE = os.getenv("CLIPPERS_API_BASE", "").rstrip("/")
CLIPPERS_ADMIN_EMAIL = os.getenv("CLIPPERS_ADMIN_EMAIL", "")
CLIPPERS_ADMIN_PASSWORD = os.getenv("CLIPPERS_ADMIN_PASSWORD", "")

ADVANCE_CONCURRENCY = int(os.getenv("ADVANCE_CONCURRENCY", "5"))
CAMPAIGN_POLL_SEC = 120          # per active link
CAMPAIGN_TICK_SEC = 20           # poller loop + waiting_funds retry cadence
STATUS_POLL_SEC = 60             # panel status sweep for completion tracking
STATUS_TRACK_DAYS = 7
LOW_BALANCE_USD = 5.0

# Text hints used to match live panel catalog rows to a platform / order type.
PLATFORM_HINTS = {
    "instagram": ("instagram", " ig "), "facebook": ("facebook", " fb "),
    "tiktok": ("tiktok", "tik tok"), "youtube": ("youtube", " yt "),
    "x": ("twitter", "x.com", " x "),
}
TYPE_HINTS = {"views": ("view", "plays"), "likes": ("like",),
              "followers": ("follower",), "comments": ("comment",), "shares": ("share",)}
