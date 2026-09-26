from __future__ import annotations

from pathlib import Path
from typing import Awaitable, Callable

from src.agentic.harness.runtime import ConversationalHarness
from src.agentic.harness.task import TaskParser
from src.agentic.tools import verify_single_owner
from src.agentic.conversation.session import ConversationSessionStore

RefreshCallback = Callable[[], Awaitable[str]]


async def run_message(
    *,
    message: str,
    database_url: str,
    owner_chat_id: int,
    multiuser_enabled: bool,
    configured_owner_chat_id: int | None,
    repo_root: str | Path,
    refresh_callback: RefreshCallback | None = None,
):
    """Single natural-language gateway used by Telegram and future chat surfaces."""
    session = await ConversationSessionStore(owner_chat_id).load()
    task = TaskParser().parse(message, session)
    refresh_warning = ""
    if refresh_callback and any(
        name in task.required_evidence for name in ("portfolio", "decision", "technical", "risk")
    ):
        try:
            refresh_warning = str(await refresh_callback() or "").strip()
        except Exception as exc:
            refresh_warning = f"No pude refrescar la fuente operativa antes de responder ({type(exc).__name__})."

    legacy_single_owner = False
    if not multiuser_enabled and configured_owner_chat_id == int(owner_chat_id):
        try:
            legacy_single_owner = await verify_single_owner(database_url, int(owner_chat_id))
        except Exception:
            legacy_single_owner = False

    harness = ConversationalHarness(
        database_url=database_url,
        owner_chat_id=int(owner_chat_id),
        repo_root=str(repo_root),
        legacy_single_owner=legacy_single_owner,
    )
    result = await harness.run(message)
    if refresh_warning:
        result.verification.warnings.append("operational_refresh_warning")
        result.metadata["operational_refresh_warning"] = refresh_warning
    return result
