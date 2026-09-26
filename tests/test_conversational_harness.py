from __future__ import annotations

from datetime import datetime, timezone

from src.agentic.contracts import ToolObservation, ToolSpec
from src.agentic.harness.context import ContextSelector
from src.agentic.harness.permissions import Capability, PermissionPolicy
from src.agentic.harness.runtime import ConversationalHarness
from src.agentic.harness.schemas import (
    ConversationState,
    EvidenceMode,
    EvidenceObject,
    EvidenceQuality,
    TaskSpec,
)
from src.agentic.harness.task import TaskParser
from src.agentic.harness.verifier import HarnessVerifier
from src.agentic.tools import ToolRegistry


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


def test_why_followup_can_inherit_grounded_subject_from_portfolio_review():
    state = ConversationState(
        owner_chat_id=123,
        active_symbols=["NVDA"],
        last_intent="portfolio_review",
        conversation_subject="NVDA",
    )
    task = TaskParser().parse("¿por qué?", state)
    assert task.entities == ["NVDA"]
    assert task.intent == "decision_explanation"


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


async def _tool_stub(arguments):
    return ToolObservation(tool_name="stub", arguments=arguments, ok=True, content="{}")


def test_permission_policy_uses_registry_capability_and_fails_closed():
    registry = ToolRegistry()
    registry.register(
        ToolSpec("get_portfolio_snapshot", "fixture", {"type": "object", "properties": {}}, capability="READ"),
        _tool_stub,
    )
    registry.register(
        ToolSpec("harmless_writer", "fixture", {"type": "object", "properties": {}}, capability="WRITE"),
        _tool_stub,
    )
    policy = PermissionPolicy()
    assert policy.capability_for(registry, "get_portfolio_snapshot") == Capability.READ
    assert policy.capability_for(registry, "harmless_writer") == Capability.FORBIDDEN
    assert policy.capability_for(registry, "missing_tool") == Capability.FORBIDDEN
    assert not policy.allow(registry, "harmless_writer")


def test_permission_policy_keeps_name_block_as_secondary_defense():
    registry = ToolRegistry()
    registry.register(
        ToolSpec("execute_order", "fixture", {"type": "object", "properties": {}}, capability="READ"),
        _tool_stub,
    )
    assert PermissionPolicy().capability_for(registry, "execute_order") == Capability.FORBIDDEN


def test_answer_subject_is_kept_only_when_supported_by_evidence():
    harness = ConversationalHarness(
        database_url="postgresql://example.invalid/db",
        owner_chat_id=123,
        repo_root=".",
        legacy_single_owner=True,
        require_audit=False,
    )
    evidence = [
        EvidenceObject(
            source="portfolio",
            tool_name="get_portfolio_snapshot",
            timestamp=datetime.now(timezone.utc),
            payload={"positions": [{"ticker": "NVDA"}]},
            quality=EvidenceQuality.HIGH,
            mode=EvidenceMode.PRODUCTION,
            ok=True,
        )
    ]
    assert harness._supported_answer_symbols("Me preocupa más NVDA.", evidence, []) == ["NVDA"]
    assert harness._supported_answer_symbols("Me preocupa más GDX.", evidence, []) == []


def _task() -> TaskSpec:
    return TaskSpec(raw_message="estado", intent="system_status")


def test_verifier_requires_successful_evidence():
    report = HarnessVerifier().verify(task=_task(), answer="Todo bien.", evidence=[])
    assert not report.passed
    assert "no_successful_evidence" in report.failures


def test_verifier_rejects_failed_required_source():
    evidence = EvidenceObject(
        source="ledger",
        tool_name="get_decision_ledger",
        timestamp=datetime.now(timezone.utc),
        payload="database unavailable",
        quality=EvidenceQuality.LOW,
        mode=EvidenceMode.PRODUCTION,
        warnings=["tool_error:database unavailable"],
        ok=False,
    )
    report = HarnessVerifier().verify(
        task=TaskSpec(raw_message="¿Cuánto ganó Quantia?", intent="performance"),
        answer="No pude determinarlo.",
        evidence=[evidence],
        required_tools=["get_decision_ledger"],
    )
    assert not report.passed
    assert "no_successful_evidence" in report.failures
    assert any(item.startswith("missing_required_tools:") for item in report.failures)


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


def test_verifier_matches_spanish_and_json_decimal_separators():
    evidence = EvidenceObject(
        source="ledger",
        tool_name="get_decision_ledger",
        timestamp=datetime.now(timezone.utc),
        payload={"ev_net": 1.3, "sample": 42},
        quality=EvidenceQuality.HIGH,
        mode=EvidenceMode.PRODUCTION,
        ok=True,
    )
    report = HarnessVerifier().verify(
        task=TaskSpec(raw_message="resultado", intent="performance"),
        answer="El EV neto observado fue 1,3% sobre una muestra de 42 episodios.",
        evidence=[evidence],
        required_tools=["get_decision_ledger"],
    )
    assert report.numeric_consistency
