import asyncio
import hashlib
import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from src.agentic.contracts import AgentDecision, AgentModelError, ToolObservation, ToolSpec, ToolValidationError
from src.agentic.model import OllamaAgentModel
from src.agentic.orchestrator import AgentOrchestrator
from src.agentic import tools


class Model:
    name = "fixture"
    def __init__(self, decisions):
        self.decisions = iter(decisions)
        self.calls = 0
    async def decide(self, **kwargs):
        self.calls += 1
        return next(self.decisions)


def registry(handler=None):
    result = tools.ToolRegistry()
    result.register(ToolSpec("test", "fixture", {"type": "object", "properties": {
        "ticker": {"type": "string", "maxLength": 15},
        "limit": {"type": "integer", "minimum": 1, "maximum": 12, "default": 8}},
        "additionalProperties": False}), handler or AsyncMock(return_value=ToolObservation("test", {}, True, "data")))
    return result


@pytest.mark.parametrize("arguments", [{"limit": 1.5}, {"limit": True}, {"limit": "2"}, {"ticker": []}, {"ticker": "MU;rm"}, {"limit": 13}, {"arbitrary": "x"}])
def test_strict_schema(arguments):
    with pytest.raises(ToolValidationError):
        registry().validate("test", arguments)


def test_semantically_identical_calls_do_not_execute_twice():
    handler = AsyncMock(return_value=ToolObservation("test", {}, True, "evidence"))
    model = Model([AgentDecision("tool", "test", {"ticker": "mu"}),
                   AgentDecision("tool", "test", {"ticker": "MU", "limit": 8}),
                   AgentDecision("final", answer="done")])
    result = asyncio.run(AgentOrchestrator(model=model, registry=registry(handler), require_audit=False).run(goal="test"))
    assert handler.await_count == 1
    assert result.steps[1].observation.ok is False
    for step in result.steps[:2]:
        assert step.observation.content_sha256 == hashlib.sha256(step.observation.content.encode()).hexdigest()


def test_no_evidence_cannot_be_complete():
    result = asyncio.run(AgentOrchestrator(model=Model([AgentDecision("final", answer="invented")]),
                        registry=registry(), require_audit=False).run(goal="test"))
    assert result.status == "FAILED" and result.stop_reason == "model_error"
    assert "invented" not in result.answer


class Store:
    def __init__(self, failed_step=False):
        self.failed_step = failed_step
        self.finished = None
    async def ensure_schema(self): pass
    async def start_run(self, **kwargs): pass
    async def record_step(self, **kwargs):
        if self.failed_step: raise OSError("fixture audit outage")
    async def finish_run(self, **kwargs): self.finished = kwargs


@pytest.mark.parametrize("required", [True, False])
def test_incomplete_audit_is_never_reported_as_persisted(required):
    store = Store(failed_step=True)
    model = Model([AgentDecision("tool", "test"), AgentDecision("final", answer="done")])
    result = asyncio.run(AgentOrchestrator(model=model, registry=registry(), store=store, require_audit=required).run(goal="test"))
    assert result.audit_persisted is False
    assert result.status == ("FAILED" if required else "COMPLETE")
    assert store.finished is not None


def test_audit_setup_fails_before_model_or_tools():
    store = Store()
    store.ensure_schema = AsyncMock(side_effect=OSError("db unavailable"))
    model = Model([])
    with pytest.raises(OSError):
        asyncio.run(AgentOrchestrator(model=model, registry=registry(), store=store).run(goal="test"))
    assert model.calls == 0


def test_read_only_dsn_overrides_write_settings():
    params = parse_qs(urlsplit(tools.read_only_dsn("postgresql+asyncpg://user@host/db?sslmode=require&default_transaction_read_only=off")).query)
    assert params["default_transaction_read_only"] == ["on"]
    assert params["sslmode"] == ["require"]


def test_account_required_and_legacy_pipeline_denied_without_verification():
    with pytest.raises(ValueError, match="owner"):
        tools.ToolContext("postgresql://test")
    context = tools.ToolContext("postgresql://test", owner_chat_id=123)
    with pytest.raises(ToolValidationError, match="single-owner"):
        asyncio.run(tools.execute_tool(tools.build_default_registry(context), name="analyze_portfolio", arguments={}))


def test_performance_does_only_scoped_selects(monkeypatch):
    conn = SimpleNamespace(fetch=AsyncMock(return_value=[{"source": "execution_plan", "n_raw": 2}]), close=AsyncMock(),
                           execute=AsyncMock(), fetchval=AsyncMock(return_value="on"))
    connect = AsyncMock(return_value=conn)
    monkeypatch.setattr(tools.asyncpg, "connect", connect)
    context = tools.ToolContext("postgresql://user@host/db", owner_chat_id=123)
    result = asyncio.run(tools.execute_tool(tools.build_default_registry(context), name="get_performance", arguments={}))
    sql, owner, legacy = conn.fetch.call_args.args
    assert sql.lstrip().startswith("SELECT") and "owner_chat_id=$1" in sql
    assert owner == 123 and legacy is False
    assert "default_transaction_read_only=on" in connect.call_args.args[0]
    assert result.ok and "NOT_DEDUPLICATED" in result.content
    conn.close.assert_awaited_once()


def test_cancellation_kills_and_reaps_subprocess(monkeypatch):
    proc = SimpleNamespace(returncode=None, kill=lambda: setattr(proc, "returncode", -9))
    ready = None
    async def communicate():
        if proc.returncode is None:
            ready.set()
            await asyncio.sleep(60)
        return b"", b""
    proc.communicate = communicate
    create = AsyncMock(return_value=proc)
    monkeypatch.setattr(tools.asyncio, "create_subprocess_exec", create)
    async def check():
        nonlocal ready
        ready = asyncio.Event()
        task = asyncio.create_task(tools._run_subprocess(tools.ToolContext("postgresql://u@h/db", owner_chat_id=123), tool_name="test", arguments={}, command=[sys.executable]))
        await ready.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
    asyncio.run(check())
    assert proc.returncode == -9
    assert "default_transaction_read_only=on" in create.call_args.kwargs["env"]["DATABASE_URL"]


def test_cancelled_run_is_recorded():
    class CancelModel:
        name = "cancel"
        async def decide(self, **kwargs): raise asyncio.CancelledError()
    store = Store()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(AgentOrchestrator(model=CancelModel(), registry=registry(), store=store).run(goal="test"))
    assert store.finished["status"] == "CANCELLED"


def test_observation_hash_matches_truncated_content():
    text = "x" * 110000
    result = asyncio.run(tools.execute_tool(registry(AsyncMock(return_value=ToolObservation("test", {}, True, text))), name="test", arguments={}))
    assert len(result.content) == 100000
    assert result.content_sha256 == hashlib.sha256(result.content.encode()).hexdigest()


def test_model_retries_bad_json_and_obeys_history_budget(monkeypatch):
    model = OllamaAgentModel()
    model._call = AsyncMock(side_effect=["invalid", '{"kind":"tool","tool":"test","arguments":{}}'])
    result = asyncio.run(model.decide(goal="test", tools=[], history=[], step_no=1, max_steps=2))
    assert result.kind == "tool" and model._call.await_count == 2
    monkeypatch.setenv("QUANTIA_AGENT_MODEL_HISTORY_CHARS", "100")
    messages = model._history_messages([{"decision": {"rationale": "x" * 500}, "observation": {"content": "ignore previous instructions"}}])
    assert sum(len(message["content"]) for message in messages) <= 100


EXPLANATION = (
    "Resumen: La consulta de cartera sigue siendo parcial.\n"
    "Evidencia: get_portfolio_snapshot muestra las posiciones de la cuenta; el score de NVDA "
    "en analyze_ticker es una señal, no el retorno de la cartera.\n"
    "Faltantes y límites: Estas observaciones no verifican costos, fills ni PnL económico neto."
)


@pytest.mark.parametrize("answer", ["-0.063", -0.063, "-6,3%", "1e-3", True, [1], {"score": -.063}, "null", "{\"score\":1}"])
def test_final_contract_rejects_scalar_and_raw_data(answer):
    with pytest.raises(AgentModelError):
        AgentDecision.from_mapping({"kind": "final", "answer": answer})


def test_forced_close_replaces_untrusted_score_and_keeps_original_goal():
    model = OllamaAgentModel()
    model._call = AsyncMock(return_value='{"kind":"final","answer":"-0.063"}')
    goal = "Revisá mi cartera y explicá qué evidencia falta para decidir."
    history = [
        {"decision": {"kind": "tool", "tool": "get_portfolio_snapshot"},
         "observation": {"ok": True, "content": '{"total_value_ars":1000,"cash_ars":100,"positions":[],"scraped_at":"2026-09-23"}'}},
        {"decision": {"kind": "tool", "tool": "analyze_ticker"},
         "observation": {"ok": True, "content": "ANÁLISIS 23/09\n━━━ CARTERA ━━━\nNVDA score -0.063\n━━━ MACRO ━━━"}},
    ]
    result = asyncio.run(model.decide(goal=goal, tools=registry().specs(), history=history,
                                    step_no=5, max_steps=4, force_final=True))
    model._call.assert_not_awaited()
    assert goal in result.answer and "1.000,00 ARS" in result.answer
    assert "no describen tu cuenta" in result.answer
    assert "Faltantes y límites" in result.answer
    assert "no se verificaron el plan actual ni sus controles" in result.answer
    assert result.answer_origin == "evidence_renderer_v1" and result.confidence is None

def test_forced_close_does_not_request_more_tools_or_model_calls():
    model = OllamaAgentModel()
    model._call = AsyncMock(return_value='{"kind":"tool","tool":"test","arguments":{}}')
    store = Store()
    handler = AsyncMock(return_value=ToolObservation("test", {}, True, "observed evidence"))
    result = asyncio.run(AgentOrchestrator(model=model, registry=registry(handler), store=store,
                                          max_steps=1).run(goal="Revisá la cartera"))
    assert handler.await_count == 1 and model._call.await_count == 1
    assert result.status == store.finished["status"] == "LIMIT_REACHED"
    assert result.audit_persisted and "observed evidence" in result.answer
    assert store.finished["metadata_patch"]["answer_origin"] == "evidence_renderer_v1"

def test_no_evidence_cannot_produce_a_source_report():
    model = OllamaAgentModel()
    model._call = AsyncMock()
    with pytest.raises(AgentModelError, match="no successful"):
        asyncio.run(model.decide(goal="Revisá mi cartera", tools=[], history=[], step_no=5, max_steps=4, force_final=True))
    model._call.assert_not_awaited()

def test_replayed_four_consultations_then_synthesis_preserves_audit():
    model = OllamaAgentModel()
    model._call = AsyncMock(side_effect=[
        *(json.dumps({"kind": "tool", "tool": "test", "arguments": {"ticker": ticker}})
          for ticker in ["NVDA", "AMD", "GDX", "MU"]),
    ])
    store = Store()
    store.record_step = AsyncMock()
    result = asyncio.run(AgentOrchestrator(model=model, registry=registry(), store=store,
                                          max_steps=4).run(goal="Revisá mi cartera y la evidencia faltante"))
    assert result.status == "LIMIT_REACHED" and "Resumen:" in result.answer
    assert len(result.steps) == 5 and store.record_step.await_count == 5
    assert store.finished["final_answer"] == result.answer and result.audit_persisted
    assert result.to_dict()["answer_origin"] == "evidence_renderer_v1"
    assert model._call.await_count == 4
    assert model._call.call_args.args[0]["options"]["num_ctx"] == 16384

def test_runtime_rejects_scalar_even_if_model_adapter_skips_parsing():
    result = asyncio.run(AgentOrchestrator(model=Model([
        AgentDecision("tool", "test"), AgentDecision("final", answer="-0.063")]),
        registry=registry(), require_audit=False).run(goal="Revisá mi cartera"))
    assert result.status == "FAILED" and result.stop_reason == "model_error"
    assert "-0.063" not in result.answer


def test_history_hard_caps_old_large_environment_settings(monkeypatch):
    monkeypatch.setenv("QUANTIA_AGENT_MODEL_HISTORY_CHARS", "48000")
    monkeypatch.setenv("QUANTIA_AGENT_MODEL_OBSERVATION_CHARS", "9000")
    messages = OllamaAgentModel._history_messages([
        {"decision": {"kind": "tool"}, "observation": {"content": "x" * 20000}} for _ in range(4)])
    assert sum(len(m["content"]) for m in messages) <= 16000
    assert any("excerpt truncated" in m["content"] for m in messages)


def test_synthesis_keeps_every_source_and_discards_controller_rationales():
    from src.agentic.answer import evidence_decision
    history = [{"decision": {"tool": tool, "arguments": {}, "rationale": "LAST_TICKER_BIAS"},
                "observation": {"tool": tool, "ok": True, "content": tool + "x" * 20000}}
               for tool in ["get_portfolio_snapshot", "get_macro_context", "analyze_portfolio", "analyze_ticker"]]
    result = evidence_decision("Revisá mi cartera", history)
    assert all(tool in result.answer for tool in ["get_portfolio_snapshot", "get_macro_context", "analyze_portfolio", "analyze_ticker"])
    assert "LAST_TICKER_BIAS" not in result.answer
    assert "fuente completa en la traza" in result.answer
    assert len(result.answer) < 12000

def test_early_completion_also_synthesizes_all_evidence_without_more_tools():
    model = OllamaAgentModel()
    model._call = AsyncMock(return_value='{"kind":"final","answer":"-0.063","confidence":0.95}')
    result = asyncio.run(model.decide(goal="Revisá mi cartera", tools=registry().specs(),
        history=[{"decision": {"tool": "test"}, "observation": {"ok": True, "content": "evidence"}}],
        step_no=2, max_steps=4))
    assert "evidence" in result.answer and "-0.063" not in result.answer
    assert model._call.await_count == 1 and result.confidence is None


@pytest.mark.parametrize("arguments", [[], None, "bad"])
def test_decision_rejects_non_object_arguments(arguments):
    with pytest.raises(AgentModelError):
        AgentDecision.from_mapping({"kind": "tool", "tool": "test", "arguments": arguments})


def test_source_report_preserves_macro_values_and_exact_plan_guards():
    from src.agentic.answer import evidence_decision
    history = [
        {"observation": {"tool": "get_macro_context", "ok": True,
                         "content": json.dumps({"sp500": 100, "vix": 15, "reservas": None,
                                                "missing_indicators": ["reservas"], "fetched_at": "2026-09-23"})}},
        {"observation": {"tool": "analyze_portfolio", "ok": True,
                         "content": "ANÁLISIS 23/09\n━━━ PLAN AHORA ━━━\nSELL AMD score -0.253\n"
                                    "WATCH NVDA bloqueado score -0.063 BUY_SCORE_GUARD\n━━━ CARTERA ━━━\nNVDA"}},
    ]
    answer = evidence_decision("Revisá mi cartera", history).answer
    assert "SP500: 100,00" in answer and "VIX: 15,00" in answer
    assert "Indicadores no informados por esta consulta macro: reservas." in answer
    assert "WATCH NVDA bloqueado score -0.063 BUY_SCORE_GUARD" in answer
    assert "SELL NVDA" not in answer and "no son fills" in answer
    assert "no se obtuvo el análisis de cartera" not in answer


@pytest.mark.parametrize("snapshot", [{"scraped_at": None}, {"total_value_ars": None, "cash_ars": None}])
def test_source_report_does_not_invent_zero_for_missing_fields(snapshot):
    from src.agentic.answer import evidence_decision
    result = evidence_decision("Revisá mi cartera", [{"observation": {
        "tool": "get_portfolio_snapshot", "ok": True, "content": json.dumps(snapshot)}}])
    assert "Valor informado: N/D ARS; cash: N/D ARS; posiciones informadas: N/D" in result.answer


def test_source_report_lists_failures_without_turning_them_into_evidence():
    from src.agentic.answer import evidence_decision
    result = evidence_decision("Revisá mi cartera", [
        {"observation": {"tool": "observed", "ok": True, "content": "Dato observado"}},
        {"observation": {"tool": "failed", "ok": False, "content": "INVALID DATA", "error": "private trace detail"}},
    ])
    assert "Consultas con resultado: 1" in result.answer
    assert "failed: consulta sin resultado válido" in result.answer
    assert "INVALID DATA" not in result.answer and "private trace detail" not in result.answer


def test_cli_missing_owner_in_multiuser_fails_before_database(monkeypatch):
    from scripts import run_agent as cli
    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(multiuser_enabled=True, scraper=SimpleNamespace(telegram_chat_id="123")))
    with pytest.raises(ValueError, match="owner-chat-id"):
        asyncio.run(cli.async_main(SimpleNamespace(force=True, owner_chat_id=None)))


def test_connection_guard_checks_server_and_closes_on_failure(monkeypatch):
    from src.agentic import read_only
    conn = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value="off"), close=AsyncMock())
    monkeypatch.setattr(read_only.asyncpg, "connect", AsyncMock(return_value=conn))
    with pytest.raises(RuntimeError, match="read-only guard"):
        asyncio.run(read_only.connect_read_only("postgresql://test"))
    conn.close.assert_awaited_once()


def test_pool_guard_reapplies_after_pool_reset(monkeypatch):
    from src.agentic import read_only
    from unittest.mock import Mock
    original = Mock(return_value="pool")
    monkeypatch.setattr(read_only.asyncpg, "create_pool", original)
    assert read_only.guarded_pool("postgresql://test") == "pool"
    assert original.call_args.kwargs["init"] is read_only.protect_connection
    assert original.call_args.kwargs["setup"] is read_only.protect_connection


@pytest.mark.parametrize("argv", [["runner", "scripts/run_performance.py"],
                                  ["runner", "scripts/run_analysis.py", "--no-persist"]])
def test_child_runner_rejects_unregistered_scripts_or_missing_guards(monkeypatch, argv):
    from src.agentic import read_only_runner
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(ValueError): read_only_runner.main()


def test_controller_timeout_is_async_and_bounded():
    model = OllamaAgentModel(timeout_seconds=.01)
    async def slow(payload): await asyncio.sleep(60)
    model._call = slow
    with pytest.raises(AgentModelError):
        asyncio.run(model.decide(goal="test", tools=[], history=[], step_no=1, max_steps=2))


@pytest.mark.parametrize("failure", [None, "process", "send"])
def test_telegram_owner_full_artifact_and_cleanup(failure):
    from pathlib import Path
    from src.agentic.telegram import run_report, _active_chats
    paths = []
    async def command(args, timeout):
        assert args[args.index("--owner-chat-id") + 1] == "123"
        assert "--force" not in args and "--timeout-seconds" in args and timeout == 300
        path = Path(args[args.index("--output-json") + 1]); paths.append(path)
        if failure != "process":
            path.write_text(json.dumps({"status": "COMPLETE", "answer": "<untrusted>", "audit_persisted": True, "run_id": "test", "steps": []}))
        return (1 if failure == "process" else 0), "x"*30000, "PRIVATE_DETAIL", 1.0
    send = AsyncMock()
    context = SimpleNamespace(bot=SimpleNamespace(send_document=AsyncMock(side_effect=RuntimeError("send") if failure == "send" else None)))
    if failure == "send":
        with pytest.raises(RuntimeError): asyncio.run(run_report(context, 123, "test", run_command=command, send_text=send))
    else:
        asyncio.run(run_report(context, 123, "test", run_command=command, send_text=send))
    assert 123 not in _active_chats and not paths[0].parent.exists()
    assert "PRIVATE_DETAIL" not in str(send.call_args_list)
    if failure != "process": assert "&lt;untrusted&gt;" in str(send.call_args_list)


def test_telegram_menu_alias_and_authorization(monkeypatch):
    from scripts import telegram_bot as bot
    from pathlib import Path
    assert dict(bot.BOT_COMMAND_SPECS)["agente"]
    assert bot.CALLBACK_ALIASES["agent_prompt"] == "agent_prompt"
    source = Path(bot.__file__).read_text(encoding="utf-8")
    assert 'CommandHandler("agente",' in source and 'CommandHandler("agent",' in source
    assert 'callback_data="agent_prompt"' in source
    monkeypatch.setattr(bot, "ensure_allowed_chat", AsyncMock(return_value=False))
    send = AsyncMock(); monkeypatch.setattr(bot, "send_text", send)
    asyncio.run(bot.agent_handler(SimpleNamespace(message=True), SimpleNamespace(args=["test"])))
    send.assert_not_awaited()


def test_telegram_explains_limit_and_distinguishes_consultations_from_close():
    from pathlib import Path
    from src.agentic.telegram import run_report
    async def command(args, timeout):
        path = Path(args[args.index("--output-json") + 1])
        path.write_text(json.dumps({"status": "LIMIT_REACHED", "answer": EXPLANATION,
                                   "audit_persisted": True, "run_id": "regression",
                                   "steps": [{"decision": {"kind": "tool"}} for _ in range(4)]
                                            + [{"decision": {"kind": "final"}}]}), encoding="utf-8")
        return 0, "", "", 1.0
    send = AsyncMock()
    context = SimpleNamespace(bot=SimpleNamespace(send_document=AsyncMock()))
    asyncio.run(run_report(context, 123, "Revisá mi cartera", run_command=command, send_text=send))
    message = send.call_args.args[-1]
    assert "Consultas: 4 · Cierres: 1" in message
    assert "Límite de consultas alcanzado" in message
    assert "no valida la conclusión" in message
    assert "Faltantes y límites" in message


@pytest.mark.parametrize("goal", ["", "x"*4001])
def test_telegram_prompt_and_budget_validation(goal):
    from src.agentic.telegram import run_report
    command, send = AsyncMock(), AsyncMock()
    asyncio.run(run_report(SimpleNamespace(), 123, goal, run_command=command, send_text=send))
    command.assert_not_awaited(); send.assert_awaited_once()
