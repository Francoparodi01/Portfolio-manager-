import asyncio
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.agentic import tools
from src.agentic.contracts import ToolObservation, ToolSpec, ToolValidationError
from src.agentic.diagnostics import question_plan, diagnostic_decision
from src.agentic.model import OllamaAgentModel
from src.agentic.orchestrator import AgentOrchestrator
from src.agentic.analysis_export import decision_evidence
from src.analysis.macro import MacroSnapshot
from src.analysis.synthesis import SynthesisResult, LayerScore


def evidence():
    return {"schema_version": "agent-decision-evidence-v1", "evaluated_at": "2026-09-23T20:00:00Z",
            "snapshot_as_of": "2026-09-23T19:00:00Z", "cash_ars": 1234,
            "signals": [{"ticker": "AMD", "final_score": -.207, "layers": [
                {"name": "Técnico", "weighted": -.20, "reasons": ["MACD bajista"]}]}],
            "plan": {"buy_orders": [], "blocked_orders": [], "decisions": [
                {"ticker": "AMD", "current_weight": .12, "target_weight": .04,
                 "action": "SELL_PARTIAL", "reason_primary": "reducción al objetivo", "reason_secondary": None}]},
            "buy_policy": {"negative_block": -.01, "minimum": .08}}


def observation(name, data, ok=True):
    return {"decision": {"tool": name}, "observation": {"tool": name, "ok": ok, "content": json.dumps(data)}}


@pytest.mark.parametrize("goal,intent,required", [
    ("revisa mi cartera y explica que evidencia falta para decidir", "portfolio_review", ("get_portfolio_snapshot", "get_decision_evidence")),
    ("el riesgo país no tiene tan en cuenta los cedears por ser posiciones en wall street", "cedear_risk", ("get_macro_exposure",)),
    ("con el boom de la ia la recesión de los últimos 2 meses y ahora una nueva tendencia alcista. No es favorable la compra/hold?", "market_thesis", ("get_decision_evidence",)),
    ("porque no es recomendable comprar?", "explain_plan", ("get_decision_evidence",)),
])
def test_actual_user_questions_require_relevant_evidence(goal, intent, required):
    plan = question_plan(goal)
    assert (plan.intent, plan.required_tools) == (intent, required)


def test_followup_remembers_user_thesis_without_certifying_it():
    context = [{"run_id": "previous", "goal": "Hubo una recesión y ahora tendencia alcista"}]
    plan = question_plan("¿Y por qué?", context)
    assert plan.inherited_from == "previous" and plan.intent == "market_thesis"
    answer = diagnostic_decision("¿Y por qué?", [observation("get_decision_evidence", evidence())], plan, context)
    assert answer.objective_status == "PARTIAL"
    assert "no verificado" in answer.answer and "MACD bajista" in answer.answer
    assert "recesión económica no son equivalentes" in answer.answer
    assert "han caído" not in answer.answer
    assert question_plan("¿Y por qué?", []).intent == "general"


def test_complete_mechanics_does_not_claim_buy_or_hold_is_inferior():
    goal = "por qué no comprar AMD?"
    result = diagnostic_decision(goal, [observation("get_decision_evidence", evidence())], question_plan(goal))
    assert result.objective_status == "EXPLAINED"
    assert "12,0% → objetivo 4,0%" in result.answer
    assert "no es un retorno" in result.answer and "No prueba" in result.answer
    assert "-0.207" in result.answer and "MACD bajista" in result.answer


def test_required_sources_cannot_be_skipped_by_premature_llm_close():
    registry = tools.ToolRegistry()
    for name, payload in [("get_decision_evidence", evidence()), ("get_portfolio_snapshot", {"cash_ars": 1234})]:
        registry.register(ToolSpec(name, "test", {"type": "object", "properties": {}}),
                          AsyncMock(return_value=ToolObservation(name, {}, True, json.dumps(payload))))
    model = OllamaAgentModel()
    model._call = AsyncMock(return_value='{"kind":"final","answer":"comprar es malo"}')
    run = asyncio.run(AgentOrchestrator(model=model, registry=registry, require_audit=False,
                                      max_steps=4).run(goal="revisá mi cartera"))
    assert [s.observation.tool_name for s in run.steps if s.observation] == ["get_portfolio_snapshot", "get_decision_evidence"]
    model._call.assert_not_awaited()
    assert run.status == "COMPLETE" and run.to_dict()["objective_status"] == "PARTIAL"
    assert "comprar es malo" not in run.answer


def test_budget_exhaustion_does_not_hide_missing_required_plan():
    registry = tools.ToolRegistry()
    registry.register(ToolSpec("get_portfolio_snapshot", "test", {"type": "object", "properties": {}}),
                      AsyncMock(return_value=ToolObservation("get_portfolio_snapshot", {}, True, '{"cash_ars":1234}')))
    result = asyncio.run(AgentOrchestrator(model=OllamaAgentModel(), registry=registry, require_audit=False,
                                          max_steps=1).run(goal="revisá mi cartera"))
    assert result.status == "LIMIT_REACHED" and result.to_dict()["objective_status"] == "INSUFFICIENT"
    assert "No obtuve evidencia estructurada" in result.answer


@pytest.mark.parametrize("payload", [None, {"schema_version": "bad"}])
def test_failed_or_invalid_evidence_is_not_invented(payload):
    history = [observation("get_decision_evidence", payload, ok=bool(payload))]
    result = diagnostic_decision("por qué no comprar", history, question_plan("por qué no comprar"))
    assert result.objective_status == "INSUFFICIENT"
    assert "score -" not in result.answer


@pytest.mark.parametrize("country,ccl,expected", [(555,1604.1,"EXPLAINED"), (None,1604.1,"PARTIAL"), (555,None,"PARTIAL")])
def test_macro_cause_uses_actual_rule_and_preserves_missingness(monkeypatch, country, ccl, expected):
    monkeypatch.setattr(tools, "fetch_macro", lambda: MacroSnapshot(riesgo_pais=country, ccl=ccl))
    registry = tools.build_default_registry(tools.ToolContext("postgresql://fixture", owner_chat_id=123))
    obs = asyncio.run(tools.execute_tool(registry, name="get_macro_exposure", arguments={}))
    goal = "El riesgo país no tiene tan en cuenta los CEDEAR"
    answer = diagnostic_decision(goal, [observation(obs.tool_name, json.loads(obs.content))], question_plan(goal))
    assert answer.objective_status == expected
    if country is not None and ccl is not None:
        assert "el CCL sí" in answer.answer
    else:
        assert "el CCL sí" not in answer.answer
    assert "no demuestra ausencia de todos" in answer.answer
    assert "CDEEAR" not in answer.answer


def test_unknown_catalog_symbol_never_launches_analysis(monkeypatch):
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=False), close=AsyncMock())
    monkeypatch.setattr(tools, "connect_read_only", AsyncMock(return_value=conn))
    child = AsyncMock(); monkeypatch.setattr(tools, "_run_subprocess", child)
    registry = tools.build_default_registry(tools.ToolContext("postgresql://fixture", owner_chat_id=123, legacy_single_owner=True))
    with pytest.raises(ToolValidationError, match="catalog"):
        asyncio.run(tools.execute_tool(registry, name="analyze_ticker", arguments={"ticker": "CDEEAR"}))
    child.assert_not_awaited(); conn.close.assert_awaited_once()


@pytest.mark.parametrize("raw,ok", [(json.dumps(evidence()),True), ('{"partial":',False), ('x'*100001,False)], ids=["valid", "malformed", "oversized"])
def test_structured_subprocess_never_truncates_into_valid_evidence(monkeypatch, raw, ok):
    proc = SimpleNamespace(returncode=0, communicate=AsyncMock(return_value=(raw.encode(),b"")))
    monkeypatch.setattr(tools.asyncio, "create_subprocess_exec", AsyncMock(return_value=proc))
    result = asyncio.run(tools._run_subprocess(tools.ToolContext("postgresql://fixture", owner_chat_id=123, output_limit_chars=256),
                        tool_name="get_decision_evidence", arguments={}, command=["python"], structured=True))
    assert result.ok == ok
    if ok: assert json.loads(result.content) == evidence()
    else: assert result.error and not result.content


def test_export_uses_planner_constants_and_observed_layers_without_mutation():
    from src.analysis import execution_planner as planner
    result = SynthesisResult("AMD", "SELL", .5, -.207, .04,
                             layers=[LayerScore("Técnico", -.5, .3, -.15, ["MACD bajista"])])
    output = decision_evidence(results=[result], execution_plan=None, macro_snap=MacroSnapshot(),
                               portfolio_snapshot={"scraped_at":"2026-09-23", "_stale_reason":"stale"},
                               total_ars=1000, cash_ars=100, analysis_run_id="fixture")
    assert output["plan"] is None and output["snapshot_stale_reason"] == "stale"
    assert output["buy_policy"]["minimum"] == planner.SCORE_BUY_MIN
    assert output["signals"][0]["layers"][0]["weighted"] == -.15
    assert result.final_score == -.207
    json.dumps(output, allow_nan=False)


def test_context_read_is_owner_scoped_bounded_and_only_user_goals(monkeypatch):
    from src.agentic import persistence
    now = datetime(2026,9,23,tzinfo=timezone.utc)
    conn = SimpleNamespace(fetch=AsyncMock(return_value=[{"id":"prior", "goal":"thesis", "started_at":now, "conversation_id":"session"}]), close=AsyncMock())
    monkeypatch.setattr(persistence, "connect_read_only", AsyncMock(return_value=conn))
    context = asyncio.run(persistence.AgentRunStore("postgresql://fixture").recent_context(123, as_of=now))
    sql, owner, start, end, namespace = conn.fetch.call_args.args
    assert owner == 123 and start == now-timedelta(hours=24) and end == now
    assert namespace == "interactive" and sql.count("metadata->>'context_namespace'=$4") == 2
    assert sql.count("owner_chat_id=$1") == 2 and "LIMIT 3" in sql
    assert "(SELECT conversation_id FROM latest)" in sql and "final_answer" not in sql
    assert context[0]["goal"] == "thesis"
    conn.close.assert_awaited_once()
    with pytest.raises(ValueError): asyncio.run(persistence.AgentRunStore("x").recent_context(None))


@pytest.mark.parametrize("goal,new", [("nuevo por qué no comprar",True),("por qué no comprar",False)])
def test_telegram_context_reset_flags_and_useful_status(goal,new):
    from src.agentic.telegram import run_report
    async def command(args,timeout):
        assert ("--new-conversation" in args) == new
        assert ("--continue-conversation" in args) != new
        assert args[args.index("--goal")+1] == "por qué no comprar"
        Path(args[args.index("--output-json")+1]).write_text(json.dumps({"status":"COMPLETE", "objective_status":"PARTIAL", "answer":"Tesis no verificada", "steps":[]}),encoding="utf-8")
        return 0,"","",0
    send = AsyncMock()
    context = SimpleNamespace(bot=SimpleNamespace(send_document=AsyncMock()))
    asyncio.run(run_report(context,123,goal,run_command=command,send_text=send))
    assert "Resultado: Respuesta parcial" in send.call_args.args[-1]
