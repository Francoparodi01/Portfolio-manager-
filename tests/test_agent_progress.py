import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.agentic.contracts import AgentDecision, ToolObservation, ToolSpec
from src.agentic.orchestrator import AgentOrchestrator
from src.agentic.progress import (
    AgentProgressEvent,
    AgentProgressState,
    JsonlProgressWriter,
    read_progress_events,
    render_progress,
    safe_tool_label,
)
from src.agentic.telegram import run_report
from src.agentic.tools import ToolRegistry


class FixtureModel:
    name = "fixture"

    def __init__(self, decisions):
        self._decisions = iter(decisions)

    async def decide(self, **kwargs):
        return next(self._decisions)


def _registry() -> ToolRegistry:
    registry = ToolRegistry()

    async def handler(arguments):
        return ToolObservation("get_portfolio_snapshot", arguments, True, "evidence")

    registry.register(
        ToolSpec(
            "get_portfolio_snapshot",
            "fixture",
            {"type": "object", "properties": {}, "additionalProperties": False},
        ),
        handler,
    )
    return registry


def test_safe_tool_labels_never_expose_arguments_or_reasoning():
    assert safe_tool_label("get_portfolio_snapshot") == "Consultando cartera"
    assert safe_tool_label("get_macro_context") == "Revisando contexto macro"
    assert safe_tool_label("compare_plan_vs_hold") == "Contrastando evidencia histórica"
    text = render_progress([
        AgentProgressEvent(AgentProgressState.RECEIVED),
        AgentProgressEvent(AgentProgressState.USING_TOOL, "get_portfolio_snapshot", 1),
    ])
    assert "Consultando cartera" in text
    assert "secret" not in text.lower()
    assert "arguments" not in text.lower()
    assert "rationale" not in text.lower()


def test_jsonl_progress_boundary_contains_only_sanitized_contract(tmp_path):
    path = tmp_path / "progress.jsonl"
    writer = JsonlProgressWriter(path)
    asyncio.run(writer(AgentProgressEvent(AgentProgressState.USING_TOOL, "analyze_ticker", 2)))
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert set(payload) == {"state", "tool_name", "step_no", "created_at"}
    assert payload["state"] == "USING_TOOL"
    assert payload["tool_name"] == "analyze_ticker"
    assert read_progress_events(path)[0].step_no == 2


def test_orchestrator_emits_real_lifecycle_without_changing_result():
    events = []

    async def progress(event):
        events.append(event)

    model = FixtureModel([
        AgentDecision("tool", "get_portfolio_snapshot", {}),
        AgentDecision("final", answer="Resumen: evidencia suficiente para responder con límites claros."),
    ])
    result = asyncio.run(
        AgentOrchestrator(
            model=model,
            registry=_registry(),
            require_audit=False,
            progress_callback=progress,
        ).run(goal="Revisá mi cartera")
    )
    states = [event.state for event in events]
    assert states == [
        AgentProgressState.PLANNING,
        AgentProgressState.USING_TOOL,
        AgentProgressState.ANALYZING,
        AgentProgressState.PLANNING,
        AgentProgressState.COMPOSING,
    ]
    assert result.status == "COMPLETE"
    assert result.answer.startswith("Resumen:")


def test_progress_sink_failure_never_breaks_agent():
    async def broken_progress(event):
        raise RuntimeError("telegram unavailable")

    model = FixtureModel([
        AgentDecision("tool", "get_portfolio_snapshot", {}),
        AgentDecision("final", answer="Resumen: respuesta basada en evidencia observada y con límites."),
    ])
    result = asyncio.run(
        AgentOrchestrator(
            model=model,
            registry=_registry(),
            require_audit=False,
            progress_callback=broken_progress,
        ).run(goal="Revisá mi cartera")
    )
    assert result.status == "COMPLETE"


def test_failed_progress_is_safe_and_does_not_render_internal_details():
    text = render_progress([
        AgentProgressEvent(AgentProgressState.RECEIVED),
        AgentProgressEvent(AgentProgressState.FAILED, "private_sql_explorer", 3),
    ])
    assert text == "🧠 Quantia\n\n⚠️ El análisis se detuvo de forma segura."
    assert "sql" not in text.lower()


def test_telegram_edits_one_progress_message_then_deletes_it(tmp_path):
    edits = []
    sent_progress = []
    sent_answers = []

    async def send_message(**kwargs):
        sent_progress.append(kwargs["text"])
        return SimpleNamespace(message_id=77)

    async def edit_message_text(**kwargs):
        edits.append((kwargs["message_id"], kwargs["text"]))

    delete_message = AsyncMock()
    send_document = AsyncMock()
    context = SimpleNamespace(
        bot=SimpleNamespace(
            send_message=send_message,
            edit_message_text=edit_message_text,
            delete_message=delete_message,
            send_document=send_document,
        )
    )

    async def send_text(_context, _chat_id, text, **kwargs):
        sent_answers.append(text)

    async def command(args, timeout):
        progress_path = Path(args[args.index("--progress-jsonl") + 1])
        artifact = Path(args[args.index("--output-json") + 1])
        writer = JsonlProgressWriter(progress_path)
        await writer(AgentProgressEvent(AgentProgressState.RECEIVED))
        await writer(AgentProgressEvent(AgentProgressState.PLANNING, step_no=1))
        await writer(AgentProgressEvent(AgentProgressState.USING_TOOL, "get_portfolio_snapshot", 1))
        await asyncio.sleep(0.65)
        await writer(AgentProgressEvent(AgentProgressState.ANALYZING, "get_portfolio_snapshot", 1))
        await writer(AgentProgressEvent(AgentProgressState.COMPOSING, step_no=2))
        await asyncio.sleep(0.65)
        await writer(AgentProgressEvent(AgentProgressState.COMPLETED))
        artifact.write_text(json.dumps({
            "status": "COMPLETE",
            "objective_status": "EXPLAINED",
            "answer": "Respuesta final",
            "audit_persisted": True,
            "run_id": "progress-test",
            "steps": [
                {"decision": {"kind": "tool"}},
                {"decision": {"kind": "final"}},
            ],
        }), encoding="utf-8")
        return 0, "", "", 1.3

    asyncio.run(run_report(context, 123, "Revisá mi cartera", run_command=command, send_text=send_text))

    assert len(sent_progress) == 1
    assert edits
    assert all(message_id == 77 for message_id, _ in edits)
    assert any("Consultando cartera" in text for _, text in edits)
    assert any("Preparando respuesta" in text for _, text in edits)
    delete_message.assert_awaited_once_with(chat_id=123, message_id=77)
    assert any("Respuesta final" in text for text in sent_answers)
    send_document.assert_awaited_once()


def test_telegram_cancellation_reaps_agent_task_and_cleans_active_chat():
    from src.agentic.telegram import _active_chats

    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def command(args, timeout):
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            cancelled.set()

    context = SimpleNamespace(
        bot=SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(message_id=88)),
            edit_message_text=AsyncMock(),
            delete_message=AsyncMock(),
            send_document=AsyncMock(),
        )
    )

    async def scenario():
        task = asyncio.create_task(
            run_report(context, 321, "Revisá mi cartera", run_command=command, send_text=AsyncMock())
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()

    asyncio.run(scenario())
    assert 321 not in _active_chats
