from __future__ import annotations

from src.agentic.harness.context import ContextSelector
from src.agentic.harness.schemas import (
    ContextPlan,
    EvidenceMode,
    EvidenceObject,
    EvidenceQuality,
    TaskSpec,
    VerificationReport,
)
from src.agentic.harness.skills import SkillRouter
from src.agentic.harness.synthesis import GroundedSynthesizer
from src.agentic.harness.task import TaskParser


def _router() -> SkillRouter:
    return SkillRouter(".")


def test_routes_domain_intents_to_quantia_skills():
    router = _router()
    assert router.select(TaskSpec(raw_message="x", intent="portfolio_review")).name == "portfolio-risk-review"
    assert router.select(TaskSpec(raw_message="x", intent="position_analysis")).name == "analyze-ticker"
    assert router.select(TaskSpec(raw_message="x", intent="decision_explanation")).name == "explain-investment-decision"
    assert router.select(TaskSpec(raw_message="x", intent="opportunities")).name == "find-opportunities"
    assert router.select(TaskSpec(raw_message="x", intent="decision_lab")).name == "compare-against-hold"


def test_skill_never_widens_tool_surface():
    router = _router()
    skill = router.select(TaskSpec(raw_message="x", intent="decision_consistency_audit"))
    plan = ContextPlan(
        allowed_tools=["get_decision_evidence", "execute_order"],
        required_tools=[],
    )
    constrained = router.constrain(plan, skill)
    assert constrained.allowed_tools == ["get_decision_evidence"]
    assert constrained.required_tools == ["get_decision_evidence"]


def test_consistency_question_routes_to_dedicated_intent_and_context():
    task = TaskParser().parse("¿Hay inconsistencia entre señal HOLD y planner BUY para GDX?")
    assert task.intent == "decision_consistency_audit"
    assert task.entities == ["GDX"]
    plan = ContextSelector().select(task, {"get_decision_evidence", "scan_opportunities"})
    assert plan.allowed_tools == ["get_decision_evidence"]
    assert plan.required_tools == ["get_decision_evidence"]


def test_consistency_skill_fails_closed_without_planner_decisions():
    router = _router()
    skill = router.select(TaskSpec(raw_message="x", intent="decision_consistency_audit"))
    evidence = [
        EvidenceObject(
            source="quantia_analysis",
            tool_name="get_decision_evidence",
            payload={
                "schema_version": "agent-decision-evidence-v1",
                "signals": [{"ticker": "GDX", "decision": "HOLD", "final_score": 0.08}],
                "plan": None,
            },
            quality=EvidenceQuality.MEDIUM,
            mode=EvidenceMode.OBSERVATION,
            ok=True,
        )
    ]
    report = router.apply_verification(
        skill,
        VerificationReport(passed=True, grounded=True),
        evidence,
        TaskSpec(raw_message="audit GDX", intent="decision_consistency_audit", entities=["GDX"]),
    )
    assert not report.passed
    assert "skill_missing_planner_decisions" in report.failures


def test_decision_compaction_preserves_signal_and_planner_authority():
    item = EvidenceObject(
        source="quantia_analysis",
        tool_name="get_decision_evidence",
        payload={
            "schema_version": "agent-decision-evidence-v1",
            "signals": [{
                "ticker": "GDX",
                "decision": "HOLD",
                "final_score": 0.0883,
                "technical_regime": "RANGE",
                "layers": [{"name": "macro", "weighted": -0.02, "reasons": ["negative"]}],
            }],
            "plan": {
                "gate": "PASS",
                "decisions": [{
                    "ticker": "GDX",
                    "action": "BUY",
                    "current_weight": 0.126,
                    "target_weight": 0.35,
                    "delta_weight": 0.224,
                    "reason_primary": "rebalance",
                }],
            },
            "buy_policy": {"minimum": 0.08},
        },
        quality=EvidenceQuality.MEDIUM,
        mode=EvidenceMode.OBSERVATION,
        ok=True,
    )
    compact = GroundedSynthesizer._compact_dict(item, item.payload)
    assert compact["signals"][0]["decision"] == "HOLD"
    assert compact["plan"]["decisions"][0]["action"] == "BUY"
    assert compact["plan"]["decisions"][0]["target_weight"] == 0.35
    assert compact["buy_policy"]["minimum"] == 0.08


def test_consistency_skill_requires_requested_ticker_on_both_sides():
    router = _router()
    task = TaskSpec(
        raw_message="auditá GDX",
        intent="decision_consistency_audit",
        entities=["GDX"],
    )
    skill = router.select(task)
    evidence = [
        EvidenceObject(
            source="quantia_analysis",
            tool_name="get_decision_evidence",
            payload={
                "schema_version": "agent-decision-evidence-v1",
                "signals": [{"ticker": "NVDA", "decision": "HOLD", "final_score": 0.02}],
                "plan": {
                    "decisions": [{
                        "ticker": "NVDA",
                        "action": "HOLD",
                        "current_weight": 0.15,
                        "target_weight": 0.15,
                        "delta_weight": 0.0,
                    }]
                },
            },
            quality=EvidenceQuality.MEDIUM,
            mode=EvidenceMode.OBSERVATION,
            ok=True,
        )
    ]
    report = router.apply_verification(
        skill,
        VerificationReport(passed=True, grounded=True),
        evidence,
        task,
    )
    assert not report.passed
    assert "skill_missing_signal_entities:GDX" in report.failures
    assert "skill_missing_planner_entities:GDX" in report.failures
