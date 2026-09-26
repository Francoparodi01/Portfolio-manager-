from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from src.core.redis_client import client as redis_client
from src.agentic.harness.schemas import ConversationState

_FALLBACK: dict[int, ConversationState] = {}


class ConversationSessionStore:
    """Explicit conversation state, separate from evidence and long-term memory."""

    def __init__(self, owner_chat_id: int, *, ttl_seconds: int | None = None) -> None:
        self.owner_chat_id = int(owner_chat_id)
        self.ttl_seconds = ttl_seconds or int(os.getenv("QUANTIA_HARNESS_SESSION_TTL_SECONDS", "86400"))
        self.key = f"quantia:harness:conversation:{self.owner_chat_id}"

    async def load(self) -> ConversationState:
        try:
            raw = await redis_client.get(self.key)
            if raw:
                return ConversationState.model_validate_json(raw)
        except Exception:
            pass
        cached = _FALLBACK.get(self.owner_chat_id)
        if cached:
            return cached.model_copy(deep=True)
        return ConversationState(owner_chat_id=self.owner_chat_id)

    async def save(self, state: ConversationState) -> None:
        state.updated_at = datetime.now(timezone.utc)
        state.recent_user_messages = state.recent_user_messages[-6:]
        state.active_symbols = state.active_symbols[-8:]
        state.evidence_refs = state.evidence_refs[-20:]
        _FALLBACK[self.owner_chat_id] = state.model_copy(deep=True)
        try:
            await redis_client.set(self.key, state.model_dump_json(), ex=self.ttl_seconds)
        except Exception:
            return

    async def reset(self) -> ConversationState:
        state = ConversationState(owner_chat_id=self.owner_chat_id)
        _FALLBACK[self.owner_chat_id] = state
        try:
            await redis_client.delete(self.key)
        except Exception:
            pass
        return state
