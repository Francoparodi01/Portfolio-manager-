import asyncio
import json
from unittest.mock import AsyncMock

from src.agentic.contracts import ToolObservation, ToolSpec
from src.agentic.diagnostics import (
    question_plan,
    decision_lab_arguments,
    diagnostic_decision,
)
from src.agentic.model import OllamaAgentModel
from src.agentic.orchestrator import AgentOrchestrator
from src.agentic.tools import ToolRegistry, ToolContext, build_default_registry
from src.decision_lab.queries import TOOLS, explain_evidence, query_evidence
from .test_decision_lab_statistics import rows_fixture, experiment
from src.decision_lab.statistics import metrics


def test_lab_requires_tool_and_does_not_ask_llm_for_returns():
    registry = ToolRegistry()
    payload = {
        "schema_version": "decision-lab-agent-evidence-v1",
        "status": "INSUFFICIENT",
        "metrics": [],
    }
    registry.register(
        ToolSpec(
            "get_decision_value_added",
            "test",
            {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string"},
                    "horizon": {"type": "integer"},
                },
            },
        ),
        AsyncMock(
            return_value=ToolObservation(
                "get_decision_value_added", {}, True, json.dumps(payload)
            )
        ),
    )
    model = OllamaAgentModel()
    model._call = AsyncMock(return_value='{"kind":"final","answer":"DVA +99%"}')
    result = asyncio.run(
        AgentOrchestrator(
            model=model, registry=registry, require_audit=False, max_steps=3
        ).run(goal="DVA de MSFT 20D")
    )
    model._call.assert_not_awaited()
    assert "INSUFFICIENT" in result.answer and "99" not in result.answer
    assert result.steps[0].decision.arguments == {"ticker": "MSFT", "horizon": 20}


def test_mechanism_economic_separation_and_routing():
    plan = question_plan("por qué Quantia quiere reducir Microsoft?")
    assert plan.required_tools == ("get_decision_evidence", "get_decision_value_added")
    assert decision_lab_arguments("por qué reducir Microsoft?") == {"ticker": "MSFT"}
    result = diagnostic_decision("por qué reducir MSFT?", [], plan)
    assert (
        "Mecanismo actual" in result.answer and "Evidencia económica" in result.answer
    )


def test_ci_including_zero_and_no_causal_claim():
    rows = metrics(rows_fixture(), experiment())
    metric = next(
        r for r in rows if r["population"] == "PRIMARY" and r["segment"] == "ALL"
    )
    metric["ci"].update(lower=-0.01, upper=0.04)
    answer, status = explain_evidence(
        {
            "schema_version": "decision-lab-agent-evidence-v1",
            "metrics": [metric],
            "horizon": 5,
            "evaluated_as_of": "2026-09-24",
            "mode": "CURRENT_POLICY_ON_HISTORICAL_DATA",
            "replay_run_id": "id",
        }
    )
    assert "incluye cero" in answer and "no concluyente" in answer and "n=100" in answer
    assert status == "PARTIAL"


def test_six_tools_read_only_and_account_scope(monkeypatch):
    import src.decision_lab.queries as q
    from types import SimpleNamespace

    conn = SimpleNamespace(
        fetchval=AsyncMock(return_value=True),
        fetch=AsyncMock(return_value=[]),
        close=AsyncMock(),
    )
    monkeypatch.setattr(q, "connect_read_only", AsyncMock(return_value=conn))
    result = asyncio.run(query_evidence("dsn", 123, {"ticker": "MSFT"}))
    assert result["status"] == "INSUFFICIENT"
    assert (
        conn.fetch.call_args.args[1] == 123
        and "owner_chat_id=$1" in conn.fetch.call_args.args[0]
    )
    registry = build_default_registry(ToolContext("dsn", 123))
    assert all(registry.get(name).spec.read_only for name in TOOLS)
