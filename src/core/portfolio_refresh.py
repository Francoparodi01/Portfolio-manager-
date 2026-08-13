"""Request/response channel for portfolio refreshes owned by the scheduler."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from src.core.redis_client import client as redis_client


PORTFOLIO_REFRESH_QUEUE_KEY = "cocos:portfolio:refresh:requests"
PORTFOLIO_REFRESH_RESPONSE_PREFIX = "cocos:portfolio:refresh:response"
PORTFOLIO_REFRESH_REQUEST_TTL_SECONDS = 300


@dataclass(frozen=True, slots=True)
class PortfolioRefreshRequest:
    request_id: str
    requester: str
    owner_chat_id: int | None
    include_fills: bool
    include_market: bool
    requested_at: float

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "PortfolioRefreshRequest":
        owner = payload.get("owner_chat_id")
        return cls(
            request_id=str(payload["request_id"]),
            requester=str(payload.get("requester") or "unknown"),
            owner_chat_id=int(owner) if owner is not None else None,
            include_fills=bool(payload.get("include_fills", True)),
            include_market=bool(payload.get("include_market", False)),
            requested_at=float(payload.get("requested_at") or time.time()),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "requester": self.requester,
            "owner_chat_id": self.owner_chat_id,
            "include_fills": self.include_fills,
            "include_market": self.include_market,
            "requested_at": self.requested_at,
        }


def portfolio_refresh_response_key(request_id: str) -> str:
    return f"{PORTFOLIO_REFRESH_RESPONSE_PREFIX}:{request_id}"


async def request_portfolio_refresh(
    *,
    requester: str,
    owner_chat_id: int | None = None,
    include_fills: bool = True,
    include_market: bool = False,
    timeout_seconds: float = 120.0,
) -> dict[str, Any]:
    request = PortfolioRefreshRequest(
        request_id=uuid4().hex,
        requester=requester,
        owner_chat_id=owner_chat_id,
        include_fills=include_fills,
        include_market=include_market,
        requested_at=time.time(),
    )
    response_key = portfolio_refresh_response_key(request.request_id)
    await redis_client.delete(response_key)
    await redis_client.rpush(
        PORTFOLIO_REFRESH_QUEUE_KEY,
        json.dumps(request.to_payload(), ensure_ascii=True),
    )
    deadline = time.monotonic() + max(1.0, timeout_seconds)
    while time.monotonic() < deadline:
        raw = await redis_client.get(response_key)
        if raw:
            await redis_client.delete(response_key)
            return json.loads(raw)
        await asyncio.sleep(0.25)
    return {
        "ok": False,
        "request_id": request.request_id,
        "error": "scheduler_refresh_timeout",
    }


async def pop_portfolio_refresh_request() -> PortfolioRefreshRequest | None:
    raw = await redis_client.lpop(PORTFOLIO_REFRESH_QUEUE_KEY)
    if not raw:
        return None
    try:
        return PortfolioRefreshRequest.from_payload(json.loads(raw))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


async def complete_portfolio_refresh_request(
    request: PortfolioRefreshRequest,
    result: dict[str, Any],
) -> None:
    payload = {"request_id": request.request_id, **result}
    await redis_client.set(
        portfolio_refresh_response_key(request.request_id),
        json.dumps(payload, ensure_ascii=True, default=str),
        ex=PORTFOLIO_REFRESH_REQUEST_TTL_SECONDS,
    )


__all__ = [
    "PORTFOLIO_REFRESH_QUEUE_KEY",
    "PortfolioRefreshRequest",
    "complete_portfolio_refresh_request",
    "pop_portfolio_refresh_request",
    "portfolio_refresh_response_key",
    "request_portfolio_refresh",
]
