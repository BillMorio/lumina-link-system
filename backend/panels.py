"""Async client for PerfectPanel v2 panels (SMMGlow / MoreThanPanel / JAP).

Single POST endpoint, key auth, action-based dispatch. Errors come back as
{"error": "..."} with HTTP 200, so we raise on that explicitly. One client is
created lazily per provider from its env key.
"""
from __future__ import annotations

import asyncio
import os
import time

import httpx

from config import PROVIDERS


class PanelError(Exception):
    """Raised when a panel returns an {"error": ...} payload. Never retried."""


RETRIES = 3
BACKOFF_SEC = 2.0   # 2s, 4s, 8s


def _transient(e: Exception, action: str) -> bool:
    """Retry only read-only actions (timeout / network / 5xx).

    `add` is NEVER retried (v2.19, user decision): any error after the request may have left the
    panel holding the order, so the caller marks it ambiguous and a person checks the panel.
    """
    if action == "add":
        return False
    if isinstance(e, httpx.HTTPStatusError):
        code = e.response.status_code
        if code < 500:
            return False
        return action != "add" or code != 504
    if isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)):
        return True
    if isinstance(e, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)):
        return action != "add"
    return False


class PanelClient:
    def __init__(self, key: str, base_url: str, timeout: float = 30.0) -> None:
        if not key:
            raise ValueError("panel API key is empty")
        self._key = key
        self._base_url = base_url
        self._timeout = timeout

    async def _post(self, params: dict):
        if DRY_RUN and params.get("action") in ("add", "status", "cancel"):
            return _dry(params)
        data = {"key": self._key, **params}
        for attempt in range(RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    resp = await client.post(self._base_url, data=data)
                    resp.raise_for_status()
                    payload = resp.json()
                break
            except Exception as e:  # noqa: BLE001
                if attempt >= RETRIES or not _transient(e, params.get("action", "")):
                    raise
                await asyncio.sleep(BACKOFF_SEC * 2 ** attempt)
        if isinstance(payload, dict) and payload.get("error"):
            raise PanelError(str(payload["error"]))
        return payload

    async def services(self) -> list:
        return await self._post({"action": "services"})

    async def balance(self) -> dict:
        return await self._post({"action": "balance"})

    async def add(self, service: str, link: str, quantity: int, **extra) -> dict:
        params = {"action": "add", "service": service, "link": link, "quantity": quantity}
        params.update(extra)
        return await self._post(params)

    async def status(self, order_ids):
        """Single id -> {id: status}. Multiple -> {id: status}. Always a mapping."""
        if isinstance(order_ids, (list, tuple)):
            ids = [str(o) for o in order_ids]
            return await self._post({"action": "status", "orders": ",".join(ids)})
        raw = await self._post({"action": "status", "order": str(order_ids)})
        return {str(order_ids): raw}

    async def cancel(self, order_ids) -> dict[str, str | None]:
        """PerfectPanel v2 action=cancel&orders=<ids>. -> {order_id: None (accepted) | refusal reason}.
        Any error, unsupported action or unexpected shape counts as refused, with the panel's reason."""
        ids = [str(o) for o in order_ids]
        try:
            raw = await self._post({"action": "cancel", "orders": ",".join(ids)})
        except Exception as e:  # noqa: BLE001
            return {i: f"panel refused: {e!s}"[:160] for i in ids}
        out: dict[str, str | None] = {i: f"no answer for this order: {str(raw)[:100]}" for i in ids}
        for r in raw if isinstance(raw, list) else []:
            oid = str(r.get("order", "")) if isinstance(r, dict) else ""
            if oid not in out:
                continue
            c = r.get("cancel")
            if isinstance(c, dict):
                out[oid] = str(c.get("error") or c)[:160]
            elif str(c) in ("1", "True", "true"):
                out[oid] = None
            else:
                out[oid] = f"unexpected answer: {str(c)[:100]}"
        return out


# PANEL_DRY_RUN=1: orders are faked (no money spent); balance + services stay live.
# A fake order "completes" DRY_COMPLETE_SEC after it was placed.
DRY_RUN = os.getenv("PANEL_DRY_RUN") == "1"
DRY_COMPLETE_SEC = 180
_dry_orders: dict[str, tuple[float, int]] = {}


def _dry(params: dict):
    import time
    if params["action"] == "cancel":
        return [{"order": o, "cancel": 1} for o in str(params["orders"]).split(",")]
    if params["action"] == "add":
        oid = f"dry-{int(time.time() * 1000)}-{len(_dry_orders)}"
        _dry_orders[oid] = (time.time(), int(params.get("quantity", 0)))
        return {"order": oid}
    ids = params.get("orders") or params.get("order")
    out = {}
    for oid in str(ids).split(","):
        t0, qty = _dry_orders.get(oid, (time.time() - DRY_COMPLETE_SEC, 0))
        done = min(1.0, (time.time() - t0) / DRY_COMPLETE_SEC)
        out[oid] = {"status": "Completed" if done >= 1 else "In progress", "start_count": "0",
                    "remaining": str(int(qty * (1 - done))), "charge": "0", "currency": "USD"}
    return out if params.get("orders") else next(iter(out.values()))


_clients: dict[str, PanelClient] = {}


def client_for(provider: str) -> PanelClient:
    if provider not in _clients:
        cfg = PROVIDERS[provider]
        _clients[provider] = PanelClient(
            os.environ.get(cfg["key_env"], ""), base_url=cfg["base_url"]
        )
    return _clients[provider]


# --------------------------------------------------------------------------- #
# Cached catalog (10 min) + balance (60 s)
# --------------------------------------------------------------------------- #
SERVICES_TTL = 3600   # catalogs are big (JAP ~1.5MB, ~26s) and rarely change
BALANCE_TTL = 60
_services: dict[str, tuple[float, list]] = {}
_balance: dict[str, tuple[float, float]] = {}


# Outage breaker: a failed lookup marks the provider down for FAIL_TTL so callers
# fail fast instead of each paying the full retry/timeout cost again.
FAIL_TTL = 60
LOOKUP_TIMEOUT = 20.0
SERVICES_TIMEOUT = 90.0
_down: dict[str, tuple[float, str]] = {}


class PanelUnavailable(Exception):
    """Panel unreachable (network/timeout/5xx), or recently so (cached)."""


def mark_down(provider: str, err) -> None:
    _down[provider] = (time.time(), str(err)[:120])


def down(provider: str) -> str | None:
    hit = _down.get(provider)
    return hit[1] if hit and time.time() - hit[0] < FAIL_TTL else None


async def _lookup(provider: str, fn, timeout: float = LOOKUP_TIMEOUT):
    why = down(provider)
    if why:
        raise PanelUnavailable(f"panel unavailable (retry in <{FAIL_TTL}s): {why}")
    try:
        return await asyncio.wait_for(fn(), timeout)
    except PanelError:
        raise   # the panel answered; not an outage
    except Exception as e:  # noqa: BLE001
        mark_down(provider, f"{type(e).__name__}: {e}")
        raise PanelUnavailable(f"panel unavailable: {type(e).__name__}: {e}"[:200]) from e


async def services_cached(provider: str) -> list[dict]:
    hit = _services.get(provider)
    if hit and time.time() - hit[0] < SERVICES_TTL:
        return hit[1]
    try:
        raw = await _lookup(provider, client_for(provider).services, SERVICES_TIMEOUT)
    except PanelUnavailable:
        if hit:   # serve the last good catalog rather than fail
            return hit[1]
        raise
    rows = [s for s in (raw or []) if isinstance(s, dict) and s.get("service") is not None]
    _services[provider] = (time.time(), rows)
    return rows


def cached_rate(provider: str, service: str) -> float | None:
    """Rate from the already-loaded catalog only; never fetches."""
    hit = _services.get(provider)
    s = next((x for x in hit[1] if str(x["service"]) == str(service)), None) if hit else None
    try:
        return float(s["rate"]) if s and s.get("rate") is not None else None
    except (TypeError, ValueError):
        return None


async def service_info(provider: str, service: str) -> dict | None:
    sid = str(service)
    return next((s for s in await services_cached(provider) if str(s["service"]) == sid), None)


async def balance_cached(provider: str) -> float:
    hit = _balance.get(provider)
    if hit and time.time() - hit[0] < BALANCE_TTL:
        return hit[1]
    bal = float((await _lookup(provider, client_for(provider).balance)).get("balance", 0) or 0)
    _balance[provider] = (time.time(), bal)
    return bal


def balance_spend(provider: str, amount: float) -> None:
    """Deduct a just-placed order's estimate so the next check inside the TTL is honest."""
    hit = _balance.get(provider)
    if hit:
        _balance[provider] = (hit[0], hit[1] - amount)


def balance_peek(provider: str) -> float | None:
    hit = _balance.get(provider)
    return hit[1] if hit else None


async def prewarm_services() -> None:
    """Load every panel catalog in the background at startup so pickers open fast."""
    from config import PROVIDERS
    await asyncio.gather(*[services_cached(p) for p in PROVIDERS], return_exceptions=True)
