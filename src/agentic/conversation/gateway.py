from __future__ import annotations

import asyncio
import logging
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
logger = logging.getLogger(__name__)
_BACKGROUND_REFRESH_TASKS: dict[int, asyncio.Task[None]] = {}


async def _persisted_snapshot_status(
    *,
    database_url: str,
    owner_chat_id: int,
    legacy_single_owner: bool,
) -> tuple[bool, bool, float | None]:
    """Return (exists, fresh, age_seconds) for the latest persisted snapshot.

    This is intentionally a cheap read-only probe. A snapshot older than the
    conversational soft TTL can still be used as explicitly timestamped evidence
    while Cocos is revalidated in the background. Only a missing snapshot or an
    explicit user refresh request blocks the current turn on the broker channel.
    """
    max_age_seconds = max(
        15,
        int(os.getenv("QUANTIA_CHAT_SNAPSHOT_FRESH_SECONDS", "120")),
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
            return False, False, None
        if scraped_at.tzinfo is None:
            scraped_at = scraped_at.replace(tzinfo=timezone.utc)
        age_seconds = max(0.0, (datetime.now(timezone.utc) - scraped_at).total_seconds())
        return True, age_seconds <= max_age_seconds, age_seconds
    except Exception:
        # Probe failure must not be mistaken for proof that no snapshot exists.
        # Fall back to the safe blocking refresh path in this case.
        return False, False, None
    finally:
        if conn is not None:
            await conn.close()


async def _persisted_snapshot_is_fresh(
    *,
    database_url: str,
    owner_chat_id: int,
    legacy_single_owner: bool,
) -> tuple[bool, float | None]:
    """Backward-compatible freshness helper retained for callers/tests."""
    _exists, fresh, age_seconds = await _persisted_snapshot_status(
        database_url=database_url,
        owner_chat_id=owner_chat_id,
        legacy_single_owner=legacy_single_owner,
    )
    return fresh, age_seconds


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


def _schedule_background_refresh(owner_chat_id: int, refresh_callback: RefreshCallback) -> bool:
    """Schedule one broker refresh per owner without delaying the chat turn."""
    owner = int(owner_chat_id)
    current = _BACKGROUND_REFRESH_TASKS.get(owner)
    if current is not None and not current.done():
        logger.info("[CHAT][REFRESH] background already_running owner_chat_id=%s", owner)
        return False

    async def _runner() -> None:
        started = time.monotonic()
        try:
            warning = str(await refresh_callback() or "").strip()
            logger.info(
                "[CHAT][REFRESH] background_done owner_chat_id=%s elapsed_ms=%s warning=%s",
                owner,
                int((time.monotonic() - started) * 1000),
                bool(warning),
            )
            if warning:
                logger.warning(
                    "[CHAT][REFRESH] background_warning owner_chat_id=%s detail=%s",
                    owner,
                    warning.replace("\n", " ")[:300],
                )
        except Exception as exc:
            logger.warning(
                "[CHAT][REFRESH] background_failed owner_chat_id=%s elapsed_ms=%s error=%s detail=%s",
                owner,
                int((time.monotonic() - started) * 1000),
                type(exc).__name__,
                str(exc)[:300],
            )
        finally:
            task = asyncio.current_task()
            if _BACKGROUND_REFRESH_TASKS.get(owner) is task:
                _BACKGROUND_REFRESH_TASKS.pop(owner, None)

    task = asyncio.create_task(_runner(), name=f"quantia_chat_refresh_{owner}")
    _BACKGROUND_REFRESH_TASKS[owner] = task
    return True


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
    stage_ms: dict[str, int | float | bool | str | None] = {}

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
    stage_ms["refresh_mode"] = "not_needed"
    if refresh_callback and refresh_needed:
        stage_started = time.monotonic()
        explicit_refresh = _explicit_refresh_request(message)
        exists = False
        fresh = False
        age_seconds: float | None = None
        if not explicit_refresh:
            exists, fresh, age_seconds = await _persisted_snapshot_status(
                database_url=database_url,
                owner_chat_id=int(owner_chat_id),
                legacy_single_owner=legacy_single_owner,
            )

        stage_ms["snapshot_exists"] = exists
        stage_ms["snapshot_age_seconds"] = round(age_seconds, 1) if age_seconds is not None else None
        stage_ms["refresh_skipped_fresh_snapshot"] = fresh

        if explicit_refresh or not exists:
            # No persisted evidence exists (or the user explicitly requested live
            # refresh), so correctness takes priority over latency for this turn.
            stage_ms["refresh_mode"] = "blocking_explicit" if explicit_refresh else "blocking_missing_snapshot"
            try:
                refresh_warning = str(await refresh_callback() or "").strip()
            except Exception as exc:
                refresh_warning = f"No pude refrescar la fuente operativa antes de responder ({type(exc).__name__})."
        elif fresh:
            stage_ms["refresh_mode"] = "fresh_snapshot"
        else:
            # Stale-while-revalidate: preserve the timestamped persisted snapshot
            # for the current answer and refresh Cocos once in the background.
            scheduled = _schedule_background_refresh(int(owner_chat_id), refresh_callback)
            stage_ms["refresh_mode"] = "background_revalidate" if scheduled else "background_already_running"
            stage_ms["background_refresh_scheduled"] = scheduled

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
    logger.info("[CHAT][GATEWAY] intent=%s stage_ms=%s", task.intent, stage_ms)
    if refresh_warning:
        result.verification.warnings.append("operational_refresh_warning")
        result.metadata["operational_refresh_warning"] = refresh_warning
    return result
