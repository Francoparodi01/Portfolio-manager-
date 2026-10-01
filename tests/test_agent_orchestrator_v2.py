from __future__ import annotations

import asyncio
import json

from src.agentic.contracts import AgentDecision, ToolObservation, ToolSpec
from src.agentic.evidence_gate import (
    canonical_sql_is_redundant,
    evaluate_evidence,
    normalize_portfolio_review,
    render_portfolio_review,
)
from src.agentic.grounded_composer import GroundedAgentComposer, VerifierVerdict
from src.agentic.harness.models import ModelRoles
from src.agentic.orchestrator import AgentOrchestrator
from src.agentic.tools import ToolRegistry


QUERY = (
    "Para la decisión más relevante de mi cartera actual, explicame la diferencia entre el peso actual, "
    "el target teórico del optimizer y el target ejecutable. Mostrame también SignalClass, PortfolioIntent, "
    "Risk, Regime y Trend, y decime si alguna posición quedó frozen/no evaluable y cómo afecta eso al presupuesto total."
)


class ControllerMustNotRun:
    name = "qwen3.5:9b"
    conversation_context = []

    def __init__(self):
        self.calls = 0

    async def decide(self, **_kwargs):
        self.calls += 1
        raise AssertionError("deterministic portfolio_review must not return to the controller")


class FakeComposer:
    synthesis_model = "fixture-synthesis"
    verifier_model = "fixture-verifier"

    def __init__(self):
        self.calls = []

    async def compose(self, *, goal, intent, history, gate):
        self.calls.append({"goal": goal, "intent": intent, "history": history, "gate": gate})
        answer = render_portfolio_review(gate)
        return (
            AgentDecision(
                kind="final",
                answer=answer,
                rationale="fixture grounded synthesis",
                answer_origin="grounded_synthesis_v2",
                objective_status="EXPLAINED",
            ),
            VerifierVerdict("APPROVE"),
        )


def run(coro):
    return asyncio.run(coro)


def _snapshot():
    tickers = ["GDX", "NVDA", "AMD", "MU", "IREN", "SNDK", "AAPL", "MSFT", "GOOGL", "TQQQ", "USESPECIE"]
    return {
        "scraped_at": "2026-10-01T03:00:00+00:00",
        "total_value_ars": 2_800_000.0,
        "cash_ars": 3_835.36,
        "positions": [
            {
                "ticker": ticker,
                "quantity": 1.0,
                "price": 100.0,
                "market_value_ars": 100_000.0,
                "weight": 0.05,
            }
            for ticker in tickers
        ],
    }


def _decision(ticker, *, current, theoretical, executable, action="HOLD"):
    return {
        "ticker": ticker,
        "action": action,
        "current_weight": current,
        "target_weight": theoretical,
        "theoretical_target_weight": theoretical,
        "executable_target_weight": executable,
        "signal_class": "POS_WEAK" if action == "WATCH" else "NEUTRAL",
        "portfolio_intent": "INCREASE" if theoretical > current else "REDUCE" if theoretical < current else "MAINTAIN",
        "reason_primary": "fixture",
        "reason_secondary": "fixture",
    }


def _signal(ticker, *, trend=0.31, regime="TRANSITIONAL", risk=0.22):
    return {
        "ticker": ticker,
        "decision": "HOLD",
        "final_score": 0.01,
        "technical_regime": regime,
        "trend_score": trend,
        "layers": [{"name": "risk", "raw_score": risk, "weighted": risk * 0.1}],
    }


def _decision_evidence(*, omit_trend=False):
    tickers = ["GDX", "NVDA", "AMD", "MU", "IREN", "SNDK", "AAPL", "MSFT", "GOOGL", "TQQQ"]
    decisions = [
        _decision("GDX", current=0.1256, theoretical=0.3495, executable=0.1256, action="WATCH"),
        _decision("NVDA", current=0.175, theoretical=0.020, executable=0.0219, action="SELL_PARTIAL"),
    ]
    decisions.extend(
        _decision(ticker, current=0.05, theoretical=0.05, executable=0.05)
        for ticker in tickers[2:]
    )
    signals = [_signal(ticker) for ticker in tickers]
    if omit_trend:
        for row in signals:
            row.pop("trend_score", None)
    return {
        "schema_version": "agent-decision-evidence-v1",
        "analysis_run_id": "fixture-run",
        "evaluated_at": "2026-10-01T03:00:05+00:00",
        "snapshot_as_of": "2026-10-01T03:00:00+00:00",
        "snapshot_stale_reason": None,
        "cash_ars": 3_835.36,
        "total_value_ars": 2_800_000.0,
        "signals": signals,
        "plan": {
            "decisions": decisions,
            "buy_orders": [],
            "sell_orders": [],
            "blocked_orders": [],
            "cash_before": 3_835.36,
            "cash_after": 3_835.36,
        },
        "scope": "CURRENT_PROPOSED_PLAN_NOT_EXECUTION_NOT_RETURN",
    }


def _history(*, omit_trend=False):
    return [
        {
            "decision": {"kind": "tool", "tool": "get_portfolio_snapshot", "arguments": {}},
            "observation": {
                "tool": "get_portfolio_snapshot",
                "ok": True,
                "content": json.dumps(_snapshot()),
            },
        },
        {
            "decision": {"kind": "tool", "tool": "get_decision_evidence", "arguments": {}},
            "observation": {
                "tool": "get_decision_evidence",
                "ok": True,
                "content": json.dumps(_decision_evidence(omit_trend=omit_trend)),
            },
        },
    ]


def _registry(call_log):
    registry = ToolRegistry()

    async def portfolio(arguments):
        call_log.append("get_portfolio_snapshot")
        return ToolObservation(
            tool_name="get_portfolio_snapshot",
            arguments=arguments,
            ok=True,
            content=json.dumps(_snapshot()),
        )

    async def evidence(arguments):
        call_log.append("get_decision_evidence")
        return ToolObservation(
            tool_name="get_decision_evidence",
            arguments=arguments,
            ok=True,
            content=json.dumps(_decision_evidence()),
        )

    async def forbidden_extra(arguments):
        call_log.append("query_quantia_sql")
        return ToolObservation(
            tool_name="query_quantia_sql",
            arguments=arguments,
            ok=True,
            content=json.dumps({"rows": []}),
        )

    async def old_evidence(arguments):
        call_log.append("get_persisted_decision_evidence")
        return ToolObservation(
            tool_name="get_persisted_decision_evidence",
            arguments=arguments,
            ok=True,
            content=json.dumps({"schema_version": "persisted-decision-evidence-v1"}),
        )

    empty_schema = {"type": "object", "properties": {}, "additionalProperties": False}
    registry.register(ToolSpec(name="get_portfolio_snapshot", description="fixture", input_schema=empty_schema), portfolio)
    registry.register(ToolSpec(name="get_decision_evidence", description="fixture", input_schema=empty_schema), evidence)
    registry.register(
        ToolSpec(
            name="query_quantia_sql",
            description="fixture",
            input_schema={
                "type": "object",
                "properties": {"sql": {"type": "string"}},
                "required": ["sql"],
                "additionalProperties": False,
            },
        ),
        forbidden_extra,
    )
    registry.register(ToolSpec(name="get_persisted_decision_evidence", description="fixture", input_schema=empty_schema), old_evidence)
    return registry


def test_portfolio_review_gate_normalizes_real_nested_fields():
    gate = evaluate_evidence("portfolio_review", QUERY, _history())
    assert gate.complete
    gdx = next(row for row in gate.normalized["decisions"] if row["ticker"] == "GDX")
    assert gdx["current_weight"] == 0.1256
    assert gdx["theoretical_target_weight"] == 0.3495
    assert gdx["executable_target_weight"] == 0.1256
    assert gdx["signal_class"] == "POS_WEAK"
    assert gdx["portfolio_intent"] == "INCREASE"
    assert gdx["risk"] == 0.22
    assert gdx["technical_regime"] == "TRANSITIONAL"
    assert gdx["trend_score"] == 0.31


def test_missing_required_trend_keeps_gate_unsatisfied():
    gate = evaluate_evidence("portfolio_review", QUERY, _history(omit_trend=True))
    assert not gate.complete
    assert "trend_score" in gate.missing_fields


def test_non_evaluable_position_is_residual_not_zero_target():
    normalized = normalize_portfolio_review(_history())
    assert normalized["non_evaluable_positions"] == ["USESPECIE"]
    assert not any(row["ticker"] == "USESPECIE" for row in normalized["decisions"])
    rendered = render_portfolio_review(evaluate_evidence("portfolio_review", QUERY, _history()))
    assert "USESPECIE" in rendered
    assert "No les asigno target 0" in rendered
    assert "posiciones optimizables + posiciones frozen/no evaluables + cash" in rendered


def test_watch_renderer_preserves_theoretical_and_executable_targets():
    gate = evaluate_evidence("portfolio_review", QUERY, _history())
    rendered = render_portfolio_review(gate)
    assert "GDX" in rendered
    assert "12,56%" in rendered
    assert "34,95%" in rendered
    assert rendered.count("12,56%") >= 2
    assert "WATCH no es una orden" in rendered


def test_integer_rounding_case_keeps_optimizer_separate_from_planner():
    gate = evaluate_evidence("portfolio_review", QUERY, _history())
    nvda = next(row for row in gate.normalized["decisions"] if row["ticker"] == "NVDA")
    assert nvda["current_weight"] == 0.175
    assert nvda["theoretical_target_weight"] == 0.020
    assert nvda["executable_target_weight"] == 0.0219
    assert nvda["theoretical_target_weight"] != nvda["executable_target_weight"]


def test_canonical_sql_is_blocked_after_decision_evidence():
    assert canonical_sql_is_redundant("portfolio_review", _history())


def test_early_completion_executes_only_two_canonical_tools():
    calls = []
    controller = ControllerMustNotRun()
    composer = FakeComposer()
    agent = AgentOrchestrator(
        model=controller,
        registry=_registry(calls),
        composer=composer,
        store=None,
        max_steps=8,
        require_audit=False,
    )
    result = run(agent.run(goal=QUERY))

    assert result.status == "COMPLETE"
    assert result.stop_reason == "evidence_complete"
    assert calls == ["get_portfolio_snapshot", "get_decision_evidence"]
    assert controller.calls == 0
    assert len([step for step in result.steps if step.decision.kind == "tool"]) == 2
    assert result.steps[-1].decision.kind == "final"
    assert result.steps[-1].decision.objective_status == "EXPLAINED"
    assert result.metadata["completion_gate"] == "SATISFIED"
    assert result.metadata["fallback_used"] is False
    assert result.metadata["progress_events"] == [
        "✓ Consulta interpretada",
        "✓ Cartera cargada",
        "✓ Evidencia de decisión obtenida",
        "✓ Evidencia suficiente",
        "⏳ Elaborando respuesta",
        "✓ Respuesta verificada",
    ]
    assert len(composer.calls) == 1


def test_budget_exhaustion_with_fresh_complete_evidence_does_not_use_old_persisted_data():
    calls = []
    controller = ControllerMustNotRun()
    agent = AgentOrchestrator(
        model=controller,
        registry=_registry(calls),
        composer=FakeComposer(),
        store=None,
        max_steps=2,
        require_audit=False,
    )
    result = run(agent.run(goal=QUERY))

    assert result.status == "COMPLETE"
    assert result.stop_reason != "max_steps"
    assert calls == ["get_portfolio_snapshot", "get_decision_evidence"]
    assert "get_persisted_decision_evidence" not in calls
    assert "query_quantia_sql" not in calls


def test_composer_verifier_rejects_watch_as_order_without_changing_decision():
    gate = evaluate_evidence("portfolio_review", QUERY, _history())
    composer = GroundedAgentComposer(synthesis_model="fixture", verifier_model="fixture")
    bad = (
        "GDX está WATCH. Hay que comprar GDX para llevarla a 34,95%; "
        "el target ejecutable es 34,95% y la operación ya fue ejecutada como fill."
    )
    issues = composer.deterministic_issues(answer=bad, gate=gate)
    assert any(item.startswith("non_order_action_presented_as_order:GDX:WATCH") for item in issues)
    assert "plan_presented_as_fill" in issues


def test_model_roles_are_environment_driven(monkeypatch):
    monkeypatch.setenv("QUANTIA_AGENT_MODEL", "controller-default")
    monkeypatch.setenv("QUANTIA_LLM_ROUTER", "router-9b")
    monkeypatch.setenv("QUANTIA_LLM_REASONING", "reasoning-27b")
    monkeypatch.setenv("QUANTIA_LLM_SYNTHESIS", "synthesis-27b")
    monkeypatch.setenv("QUANTIA_LLM_VERIFIER", "verifier-27b")
    roles = ModelRoles.from_env()
    assert roles.router == "router-9b"
    assert roles.reasoning == "reasoning-27b"
    assert roles.synthesis == "synthesis-27b"
    assert roles.verifier == "verifier-27b"
