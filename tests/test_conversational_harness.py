from __future__ import annotations

from datetime import datetime, timezone

from src.agentic.harness.context import ContextSelector
from src.agentic.harness.permissions import Capability, PermissionPolicy
from src.agentic.harness.schemas import (
    ConversationState,
    EvidenceMode,
    EvidenceObject,
    EvidenceQuality,
    TaskSpec,
)
from src.agentic.harness.task import TaskParser
from src.agentic.harness.verifier import HarnessVerifier


def test_portfolio_language_does_not_become_fake_tickers():
    task = TaskParser().parse("¿Cómo está mi cartera hoy?")
    assert task.intent == "portfolio_review"
    assert task.entities == []


def test_position_analysis_extracts_lowercase_known_ticker():
    task = TaskParser().parse("¿qué hago con nvda?")
    assert task.intent == "position_analysis"
    assert task.entities == ["NVDA"]


def test_why_followup_inherits_active_symbol():
    state = ConversationState(
        owner_chat_id=123,
        active_symbols=["GDX"],
        last_intent="position_analysis",
        conversation_subject="GDX",
    )
    task = TaskParser().parse("¿por qué?", state)
    assert task.entities == ["GDX"]
    assert task.intent == "decision_explanation"
    assert task.inherited_subject == "GDX"


def test_comparison_followup_keeps_previous_subject_and_new_symbol():
    state = ConversationState(
        owner_chat_id=123,
        active_symbols=["GDX"],
        last_intent="position_analysis",
        conversation_subject="GDX",
    )
    task = TaskParser().parse("comparalo con NVDA", state)
    assert task.intent == "position_comparison"
    assert task.entities == ["GDX", "NVDA"]


def test_meta_policy_routes_to_shadow_evidence():
    task = TaskParser().parse("¿Por qué A y B permiten GDX pero C no?")
    assert task.intent == "meta_policy"
    assert task.entities == ["GDX"]


def test_context_selector_minimizes_performance_surface():
    task = TaskParser().parse("¿Cuánto ganó Quantia?")
    available = {
        "get_decision_ledger", "get_performance", "get_portfolio_snapshot",
        "scan_opportunities", "get_macro_context",
    }
    plan = ContextSelector().select(task, available)
    assert plan.required_tools == ["get_decision_ledger"]
    assert set(plan.allowed_tools) == {"get_decision_ledger", "get_performance"}
    assert ["get_decision_ledger", "get_performance"] in plan.parallel_groups


def test_permission_policy_never_allows_trade_execution():
    policy = PermissionPolicy()
    assert policy.capability_for("get_portfolio_snapshot") == Capability.READ
    assert policy.capability_for("analyze_ticker") == Capability.COMPUTE
    assert policy.capability_for("execute_order") == Capability.FORBIDDEN
    assert not policy.allow("place_order")


def _task() -> TaskSpec:
    return TaskSpec(raw_message="estado", intent="system_status")


def test_verifier_requires_successful_evidence():
    report = HarnessVerifier().verify(task=_task(), answer="Todo bien.", evidence=[])
    assert not report.passed
    assert "no_successful_evidence" in report.failures


def test_verifier_blocks_shadow_as_production():
    evidence = EvidenceObject(
        source="economic_meta_policy",
        tool_name="get_meta_policy",
        timestamp=datetime.now(timezone.utc),
        payload={"decision": "ALLOW_SHADOW", "capital_effect": "NO"},
        quality=EvidenceQuality.MEDIUM,
        mode=EvidenceMode.SHADOW,
        ok=True,
    )
    report = HarnessVerifier().verify(
        task=TaskSpec(raw_message="meta", intent="meta_policy"),
        answer="Esta política está vigente en producción.",
        evidence=[evidence],
        required_tools=["get_meta_policy"],
    )
    assert not report.passed
    assert "shadow_presented_as_production" in report.failures


def test_verifier_accepts_explicit_shadow_label():
    evidence = EvidenceObject(
        source="economic_meta_policy",
        tool_name="get_meta_policy",
        timestamp=datetime.now(timezone.utc),
        payload={"decision": "ALLOW_SHADOW", "capital_effect": "NO"},
        quality=EvidenceQuality.MEDIUM,
        mode=EvidenceMode.SHADOW,
        ok=True,
    )
    report = HarnessVerifier().verify(
        task=TaskSpec(raw_message="meta", intent="meta_policy"),
        answer="En SHADOW, la política permite la señal; no tiene efecto sobre capital.",
        evidence=[evidence],
        required_tools=["get_meta_policy"],
    )
    assert report.passed
