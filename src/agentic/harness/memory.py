from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Literal

from src.core.redis_client import client as redis_client

MemoryKind = Literal["methodology", "configuration", "structural_preference"]


class MemoryStore:
    """Small durable memory namespace; never stores the raw conversation."""

    def __init__(self, owner_chat_id: int, *, ttl_seconds: int | None = None) -> None:
        self.owner_chat_id = int(owner_chat_id)
        self.ttl_seconds = ttl_seconds or int(os.getenv("QUANTIA_HARNESS_MEMORY_TTL_SECONDS", "2592000"))
        self.key = f"quantia:harness:memory:{self.owner_chat_id}"

    async def load(self) -> dict[str, Any]:
        try:
            raw = await redis_client.get(self.key)
            value = json.loads(raw) if raw else {}
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    async def remember(self, kind: MemoryKind, name: str, value: Any) -> None:
        if kind not in {"methodology", "configuration", "structural_preference"}:
            raise ValueError("unsupported memory kind")
        current = await self.load()
        bucket = current.setdefault(kind, {})
        bucket[str(name)[:100]] = {
            "value": value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        # Bound memory explicitly. It is not a transcript store.
        for category in list(current):
            if isinstance(current[category], dict) and len(current[category]) > 30:
                keys = list(current[category])[-30:]
                current[category] = {key: current[category][key] for key in keys}
        try:
            await redis_client.set(self.key, json.dumps(current, ensure_ascii=False), ex=self.ttl_seconds)
        except Exception:
            return
