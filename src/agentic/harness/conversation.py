from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from uuid import uuid4

from src.agentic.persistence import AgentRunStore
from src.core.redis_client import client as redis_client

from .contracts import ConversationState, TaskSpec


class ConversationStateStore:
    """Hot structured conversation state.

    Redis is intentionally a short-lived state cache, not a store of financial truth.
    If Redis is unavailable we rebuild a safe boundary from the audit store and never
    reuse assistant conclusions as evidence.
    """

    def __init__(self, *, run_store: AgentRunStore | None = None, ttl_seconds: int | None = None) -> None:
        self.run_store = run_store
        self.ttl_seconds = int(ttl_seconds or os.getenv("QUANTIA_CONVERSATION_TTL_SECONDS", "86400"))
        self.ttl_seconds = max(300, min(self.ttl_seconds, 7 * 86400))

    @staticmethod
    def _key(owner_chat_id: int) -> str:
        return f"quantia:harness:conversation:{int(owner_chat_id)}"

    async def load(self, owner_chat_id: int, *, force_new: bool = False) -> tuple[ConversationState, list[dict]]:
        if not owner_chat_id:
            raise ValueError("conversation requires explicit owner_chat_id")
        recent: list[dict] = []
        if self.run_store:
            try:
                await self.run_store.ensure_schema()
                recent = await self.run_store.recent_context(owner_chat_id, namespace="conversational-harness")
            except Exception:
                recent = []

        if not force_new:
            try:
                raw = await redis_client.get(self._key(owner_chat_id))
                if raw:
                    state = ConversationState.model_validate_json(raw)
                    return state, recent
            except Exception:
                pass

        conversation_id = (
            str(recent[-1].get("conversation_id"))
            if recent and recent[-1].get("conversation_id") and not force_new
            else str(uuid4())
        )
        return ConversationState(conversation_id=conversation_id, owner_chat_id=owner_chat_id), recent

    async def save(self, state: ConversationState) -> None:
        state.updated_at = datetime.now(timezone.utc)
        try:
            await redis_client.set(self._key(state.owner_chat_id), state.model_dump_json(), ex=self.ttl_seconds)
        except Exception:
            # Audit metadata remains the durable trace. Redis loss must degrade
            # conversational convenience, never financial correctness.
            return

    async def reset(self, owner_chat_id: int) -> ConversationState:
        state = ConversationState(conversation_id=str(uuid4()), owner_chat_id=owner_chat_id)
        try:
            await redis_client.delete(self._key(owner_chat_id))
        except Exception:
            pass
        await self.save(state)
        return state

    @staticmethod
    def apply_task(state: ConversationState, task: TaskSpec, *, run_id: str | None = None) -> ConversationState:
        symbols = task.entities or state.active_symbols
        state.active_symbols = list(dict.fromkeys(symbols))[:8]
        state.last_intent = task.intent
        if run_id:
            state.last_run_id = run_id
        if state.active_symbols:
            state.conversation_subject = " vs ".join(state.active_symbols[:2])
        elif task.intent != "general":
            state.conversation_subject = task.intent
        state.updated_at = datetime.now(timezone.utc)
        return state


def wants_new_conversation(message: str) -> bool:
    clean = " ".join(str(message or "").lower().split())
    return clean in {"nuevo chat", "nueva conversacion", "nueva conversación", "empezar de cero", "reiniciar conversacion", "reiniciar conversación"}
