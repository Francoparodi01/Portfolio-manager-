from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

from src.agentic.conversation.session import ConversationSessionStore
from src.agentic.harness.runtime import ConversationalHarness
from src.agentic.harness.task import TaskParser
from src.agentic.read_only import connect_read_only
from src.agentic.tools import read_only_dsn, verify_single_owner

RefreshCallback = Callable[[], Awaitable[str]]


async def _persisted_snapshot_is_fresh(
    *,
    database_url: str,
    owner_chat_id: int,
    legacy_single_owner: bool,
) -> tuple[bool, float | None]:
    """Cheap freshness probe before asking Cocos for another live refresh.

    The scheduler already persists portfolio snapshots. Reusing a recent one avoids
    blocking a chat turn on the broker refresh channel while preserving an explicit
    source timestamp in the downstream evidence.
    """
    max_age_seconds = max(
        15,
        int(os.getenv("QUANTIA_CHAT_SNAPSHOT_FRESH_SECONDS", os.getenv("PORTFOLIO_CACHE_TTL_SECONDS", "600"))),
    )
    conn = None
    try:
        conn = await connect_read_only(read_only_dsn(database_url), command_timeout=10)
        scraped_at = await conn.fetchval(
            """
            SELECT MAX(scraped_at)
            FROM portfolio_snapshots
            WHERE owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL)
            """,
            int(owner_chat_id),
            bool(legacy_single_owner),
        )
        if scraped_at is None:
            return False, None
        if scraped_at.tzinfo is None:
            scraped_at = scraped_at.replace(tzinfo=timezone.utc)
        age_seconds = max(0.0, (datetime.now(timezone.utc) - scraped_at).total_seconds())
        return age_seconds <= max_age_seconds, age_seconds
    except Exception:
        return False, None
    finally:
        if conn is not None:
            await conn.close()


def _explicit_refresh_request(message: str) -> bool:
    text = " ".join(str(message or "").lower().split())
    return any(
        marker in text
        for marker in (
            "actualizá",
            "actualiza",
            "actualizar",
            "refrescá",
            "refresca",
            "refrescar",
            "ahora mismo",
            "en vivo",
            "live",
        )
    )


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
    started = time.monotonic()
    stage_ms: dict[str, int | float | bool | None] = {}

    stage_started = time.monotonic()
    session = await ConversationSessionStore(owner_chat_id).load()
    task = TaskParser().parse(message, session)
    stage_ms["session_and_parse"] = int((time.monotonic() - stage_started) * 1000)

    legacy_single_owner = False
    stage_started = time.monotonic()
    if not multiuser_enabled and configured_owner_chat_id == int(owner_chat_id):
        try:
            legacy_single_owner = await verify_single_owner(database_url, int(owner_chat_id))
        except Exception:
            legacy_single_owner = False
    stage_ms["owner_verification"] = int((time.monotonic() - stage_started) * 1000)

    refresh_warning = ""
    refresh_needed = any(
        name in task.required_evidence for name in ("portfolio", "decision", "technical", "risk")
    )
    if refresh_callback and refresh_needed:
        stage_started = time.monotonic()
        fresh = False
        age_seconds: float | None = None
        if not _explicit_refresh_request(message):
            fresh, age_seconds = await _persisted_snapshot_is_fresh(
                database_url=database_url,
                owner_chat_id=int(owner_chat_id),
                legacy_single_owner=legacy_single_owner,
            )
        stage_ms["snapshot_age_seconds"] = round(age_seconds, 1) if age_seconds is not None else None
        stage_ms["refresh_skipped_fresh_snapshot"] = fresh
        if not fresh:
            try:
                refresh_warning = str(await refresh_callback() or "").strip()
            except Exception as exc:
                refresh_warning = f"No pude refrescar la fuente operativa antes de responder ({type(exc).__name__})."
        stage_ms["refresh"] = int((time.monotonic() - stage_started) * 1000)

    harness = ConversationalHarness(
        database_url=database_url,
        owner_chat_id=int(owner_chat_id),
        repo_root=str(repo_root),
        legacy_single_owner=legacy_single_owner,
    )
    stage_started = time.monotonic()
    result = await harness.run(message)
    stage_ms["harness"] = int((time.monotonic() - stage_started) * 1000)
    stage_ms["gateway_total"] = int((time.monotonic() - started) * 1000)
    result.metadata["gateway_stage_ms"] = stage_ms
    if refresh_warning:
        result.verification.warnings.append("operational_refresh_warning")
        result.metadata["operational_refresh_warning"] = refresh_warning
    return result
