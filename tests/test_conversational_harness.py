from __future__ import annotations

from pathlib import Path

import pytest

from src.agentic.contracts import ToolObservation
from src.agentic.harness.capabilities import ToolPolicy, assert_tool_allowed
from src.agentic.harness.context import build_context_pack, observation_to_evidence
from src.agentic.harness.contracts import (
    Capability,
    ConversationState,
    Evidence,
    EvidenceMode,
    EvidenceQuality,
    ExecutionBudget,
    RunState,
    TaskSpec,
)
from src.agentic.harness.conversation import wants_new_conversation
from src.agentic.harness.runtime import ConversationalHarness
from src.agentic.harness.tasking import parse_task
from src.agentic.harness.verifier import verify_response


ROOT = Path(__file__).resolve().parents[1]


def state(*, symbols=None, intent=None) -> ConversationState:
    return ConversationState(
        conversation_id="conv-1",
        owner_chat_id=123,
        active_symbols=list(symbols or []),
        last_intent=intent,
    )


def test_portfolio_message_routes_without_commands():
    task = parse_task("¿Cómo está mi cartera hoy?", state(), [])
    assert task.intent == "portfolio_review"
    assert task.required_tools == ["get_portfolio_snapshot", "get_decision_evidence"]


def test_performance_uses_analytics_v2_as_primary_evidence():
    task = parse_task("¿Cuánto ganó Quantia?", state(), [])
    assert task.intent == "performance"
    assert "get_analytics_v2" in task.required_tools
    assert "get_performance" not in task.required_tools


def test_follow_up_inherits_structured_subject():
    task = parse_task(
        "¿por qué?",
        state(symbols=["GDX"], intent="position_analysis"),
        [],
    )
    assert task.intent == "position_analysis"
    assert task.entities == ["GDX"]
    assert "get_decision_evidence" in task.required_tools


def test_comparison_inherits_previous_symbol_and_adds_new_one():
    task = parse_task(
        "comparalo con NVDA",
        state(symbols=["GDX"], intent="position_analysis"),
        [],
    )
    assert task.intent == "position_comparison"
    assert task.entities == ["NVDA", "GDX"]


def test_meta_policy_routes_to_shadow_tool():
    task = parse_task(
        "¿Por qué A y B permiten esta señal pero C no?",
        state(symbols=["MU"], intent="position_analysis"),
        [],
    )
    assert task.intent == "meta_policy"
    assert task.entities == ["MU"]
    assert "get_meta_policy_shadow" in task.required_tools


def test_historical_outcome_preserves_requested_horizon():
    task = parse_task("¿Qué pasó con las decisiones de hace 20 días?", state(), [])
    assert task.intent == "historical_outcomes"
    assert task.horizons == [20]
    assert task.required_tools == ["get_ledger_outcomes"]


def test_context_pack_never_exceeds_hard_budget():
    observation = ToolObservation(
        tool_name="get_portfolio_snapshot",
        arguments={},
        ok=True,
        content='{"source":"portfolio","positions":[' + '"X",' * 3000 + '"Y"]}',
    )
    evidence = observation_to_evidence(observation, max_chars=4000)
    task = TaskSpec(
        raw_message="cartera",
        intent="portfolio_review",
        required_tools=["get_portfolio_snapshot"],
    )
    budget = ExecutionBudget(max_context_chars=1200)
    pack = build_context_pack(
        state=state(),
        task=task,
        evidence=[evidence],
        recent_context=[],
        budget=budget,
    )
    assert pack.selected_chars <= 1200
    assert pack.pruned_items == 1


def test_evidence_mode_preserves_shadow_boundary():
    observation = ToolObservation(
        tool_name="get_meta_policy_shadow",
        arguments={},
        ok=True,
        content='{"source":"economic_meta_policy_v1","mode":"SHADOW","capital_effect":"NO"}',
    )
    evidence = observation_to_evidence(observation)
    assert evidence.mode is EvidenceMode.SHADOW


def test_verifier_accepts_deterministic_list_count():
    task = TaskSpec(
        raw_message="¿Cómo está mi cartera?",
        intent="portfolio_review",
        required_tools=["get_portfolio_snapshot"],
    )
    run = RunState(run_id="run-1", conversation_id="conv-1", task=task)
    run.evidence = [
        Evidence(
            source="portfolio",
            tool="get_portfolio_snapshot",
            data={"positions": [{"ticker": str(index)} for index in range(7)]},
            excerpt='{"positions":[]}',
        )
    ]
    verification = verify_response(answer="Tu cartera tiene 7 posiciones.", state=run)
    assert verification.passed is True


def test_verifier_blocks_ungrounded_number():
    task = TaskSpec(
        raw_message="resultado",
        intent="performance",
        required_tools=["get_analytics_v2"],
    )
    run = RunState(run_id="run-1", conversation_id="conv-1", task=task)
    run.evidence = [
        Evidence(
            source="analytics",
            tool="get_analytics_v2",
            data={"value": 0.01},
            excerpt='{"value":0.01}',
        )
    ]
    verification = verify_response(answer="Ganó 99,9%.", state=run)
    assert verification.passed is False
    assert verification.checks["numeric_grounding"] is False


def test_verifier_blocks_weak_research_promoted_to_edge():
    task = TaskSpec(
        raw_message="plan vs hold",
        intent="position_analysis",
        required_tools=["compare_plan_vs_hold"],
    )
    run = RunState(run_id="run-1", conversation_id="conv-1", task=task)
    run.evidence = [
        Evidence(
            source="decision_lab",
            tool="compare_plan_vs_hold",
            mode=EvidenceMode.RESEARCH,
            quality=EvidenceQuality.LOW,
            data={"quality": "LOW"},
            excerpt='{"quality":"LOW"}',
        )
    ]
    verification = verify_response(
        answer="La evidencia demuestra que PLAN supera a HOLD.",
        state=run,
    )
    assert verification.passed is False
    assert verification.checks["weak_research_not_promoted"] is False


def test_forbidden_capability_fails_closed():
    policies = {
        "execute_trade": ToolPolicy(Capability.FORBIDDEN, EvidenceMode.PRODUCTION),
    }
    with pytest.raises(PermissionError):
        assert_tool_allowed("execute_trade", policies)


def test_natural_reset_phrase_is_not_a_command():
    assert wants_new_conversation("empezar de cero") is True
    assert wants_new_conversation("¿Cómo viene todo?") is False


def test_planner_expands_ticker_comparison_without_unbounded_calls():
    harness = ConversationalHarness(
        database_url="postgresql://user:pass@localhost/db",
        configured_owner_chat_id="123",
    )
    task = parse_task("compará GDX con NVDA", state(), [])
    calls = harness._optional_calls(task)
    ticker_calls = [call for call in calls if call.name == "analyze_ticker"]
    assert len(ticker_calls) <= 4


def test_production_telegram_has_no_keyboard_navigation():
    source = (ROOT / "scripts" / "telegram_conversational.py").read_text(encoding="utf-8")
    assert "InlineKeyboard" not in source
    assert "ReplyKeyboard" not in source
    assert "set_my_commands([])" in source
    assert "BOT_COMMAND_SPECS" not in source


def test_compose_runs_conversational_gateway_by_default():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    multiuser = (ROOT / "docker-compose.multiuser.yml").read_text(encoding="utf-8")
    assert 'scripts/telegram_conversational.py' in compose
    assert 'scripts/telegram_conversational.py' in multiuser
