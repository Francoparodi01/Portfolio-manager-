"""Redis-backed cache for the latest portfolio snapshot and live valuation."""
from __future__ import annotations

import json
import logging
from typing import Optional

from src.core.redis_client import client as redis_client

logger = logging.getLogger(__name__)

PORTFOLIO_SNAPSHOT_CACHE_KEY = "cocos:portfolio:snapshot"
PORTFOLIO_LIVE_CACHE_KEY = "cocos:portfolio:live"


def _portfolio_key(prefix: str, owner_chat_id: Optional[int] = None) -> str:
    if owner_chat_id is None:
        return prefix
    return f"{prefix}:{int(owner_chat_id)}"


async def _set_json(key: str, payload: dict, ttl_seconds: int) -> bool:
    try:
        await redis_client.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl_seconds)
        return True
    except Exception as exc:
        logger.debug("Redis portfolio cache set ignorado [%s]: %s", key, exc)
        return False


async def _get_json(key: str) -> Optional[dict]:
    try:
        raw = await redis_client.get(key)
        if not raw:
            return None
        return json.loads(raw)
    except Exception as exc:
        logger.debug("Redis portfolio cache get ignorado [%s]: %s", key, exc)
        return None


async def cache_portfolio_snapshot(
    snapshot: dict,
    ttl_seconds: int = 600,
    *,
    owner_chat_id: Optional[int] = None,
) -> bool:
    return await _set_json(
        _portfolio_key(PORTFOLIO_SNAPSHOT_CACHE_KEY, owner_chat_id),
        snapshot,
        ttl_seconds,
    )


async def get_cached_portfolio_snapshot(
    *,
    owner_chat_id: Optional[int] = None,
) -> Optional[dict]:
    return await _get_json(_portfolio_key(PORTFOLIO_SNAPSHOT_CACHE_KEY, owner_chat_id))


async def cache_live_portfolio(
    portfolio: dict,
    ttl_seconds: int = 600,
    *,
    owner_chat_id: Optional[int] = None,
) -> bool:
    return await _set_json(
        _portfolio_key(PORTFOLIO_LIVE_CACHE_KEY, owner_chat_id),
        portfolio,
        ttl_seconds,
    )


async def get_cached_live_portfolio(
    *,
    owner_chat_id: Optional[int] = None,
) -> Optional[dict]:
    return await _get_json(_portfolio_key(PORTFOLIO_LIVE_CACHE_KEY, owner_chat_id))
