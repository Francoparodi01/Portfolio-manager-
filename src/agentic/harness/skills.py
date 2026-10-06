from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .schemas import ContextPlan, EvidenceObject, TaskSpec, VerificationReport


@dataclass(frozen=True)
class SkillDefinition:
    """Trusted, repository-owned workflow policy for one class of agent tasks."""

    name: str
    version: str
    path: str
    intents: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    required_tools: tuple[str, ...] = ()


_PORTFOLIO_RISK = SkillDefinition(
    name="portfolio-risk-review",
    version="v1",
    path="skills/quantia-agent/portfolio-risk-review/SKILL.md",
    intents=("portfolio_review",),
    allowed_tools=("get_portfolio_snapshot", "get_persisted_decision_evidence"),
    required_tools=("get_portfolio_snapshot", "get_persisted_decision_evidence"),
)
_ANALYZE_TICKER = SkillDefinition(
    name="analyze-ticker",
    version="v1",
    path="skills/quantia-agent/analyze-ticker/SKILL.md",
    intents=("position_analysis",),
    allowed_tools=(
        "get_portfolio_snapshot",
        "get_decision_evidence",
        "analyze_ticker",
        "get_macro_context",
        "get_decision_value_added",
    ),
    required_tools=("get_portfolio_snapshot", "get_decision_evidence"),
)
_EXPLAIN_DECISION = SkillDefinition(
    name="explain-investment-decision",
    version="v1",
    path="skills/quantia-agent/explain-investment-decision/SKILL.md",
    intents=("decision_explanation",),
    allowed_tools=(
        "get_portfolio_snapshot",
        "get_decision_evidence",
        "analyze_ticker",
        "get_decision_value_added",
    ),
    required_tools=("get_decision_evidence",),
)
_AUDIT_CONSISTENCY = SkillDefinition(
    name="audit-decision-consistency",
    version="v1",
    path="skills/quantia-agent/audit-decision-consistency/SKILL.md",
    intents=("decision_consistency_audit",),
    allowed_tools=("get_decision_evidence",),
    required_tools=("get_decision_evidence",),
)
_FIND_OPPORTUNITIES = SkillDefinition(
    name="find-opportunities",
    version="v1",
    path="skills/quantia-agent/find-opportunities/SKILL.md",
    intents=("opportunities",),
    allowed_tools=(
        "get_portfolio_snapshot",
        "get_decision_evidence",
        "scan_opportunities",
        "get_decision_value_added",
    ),
    required_tools=("get_portfolio_snapshot", "scan_opportunities"),
)
_COMPARE_HOLD = SkillDefinition(
    name="compare-against-hold",
    version="v1",
    path="skills/quantia-agent/compare-against-hold/SKILL.md",
    intents=("decision_lab",),
    allowed_tools=(
        "compare_plan_vs_hold",
        "get_decision_value_added",
        "get_decision_counterfactuals",
        "get_similar_historical_episodes",
        "get_replay_evidence_quality",
        "compare_strategy_versions",
    ),
)

_BY_INTENT = {
    skill.intents[0]: skill
    for skill in (
        _PORTFOLIO_RISK,
        _ANALYZE_TICKER,
        _EXPLAIN_DECISION,
        _AUDIT_CONSISTENCY,
        _FIND_OPPORTUNITIES,
        _COMPARE_HOLD,
    )
}


class SkillRouter:
    """Route tasks to trusted workflows without widening the capability surface."""

    def __init__(self, repo_root: str) -> None:
        self.repo_root = Path(repo_root or ".").resolve()
        try:
            configured = int(os.getenv("QUANTIA_AGENT_SKILL_CHARS", "7000"))
        except (TypeError, ValueError):
            configured = 7000
        self.max_instruction_chars = max(1000, min(12000, configured))

    def select(self, task: TaskSpec) -> SkillDefinition | None:
        return _BY_INTENT.get(task.intent)

    @staticmethod
    def constrain(plan: ContextPlan, skill: SkillDefinition | None) -> ContextPlan:
        """Skills may only reduce an already-authorized context plan."""
        if skill is None:
            return plan

        allowed_by_skill = set(skill.allowed_tools)
        allowed = [name for name in plan.allowed_tools if name in allowed_by_skill]
        required = [name for name in plan.required_tools if name in allowed]
        for name in skill.required_tools:
            if name in allowed and name not in required:
                required.append(name)
        parallel = [
            [name for name in group if name in allowed]
            for group in plan.parallel_groups
        ]
        parallel = [group for group in parallel if len(group) > 1]
        return plan.model_copy(update={
            "allowed_tools": allowed,
            "required_tools": required,
            "parallel_groups": parallel,
        })

    def load_instructions(self, skill: SkillDefinition) -> str:
        path = (self.repo_root / skill.path).resolve()
        if path != self.repo_root and self.repo_root not in path.parents:
            return ""
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            return ""
        return text[: self.max_instruction_chars]

    @staticmethod
    def metadata(skill: SkillDefinition | None) -> dict[str, str] | None:
        if skill is None:
            return None
        return {"name": skill.name, "version": skill.version, "path": skill.path}

    @staticmethod
    def _successful(evidence: Iterable[EvidenceObject]) -> set[str]:
        return {item.tool_name for item in evidence if item.ok}

    def verification_failures(
        self,
        skill: SkillDefinition | None,
        evidence: Iterable[EvidenceObject],
        task: TaskSpec | None = None,
    ) -> list[str]:
        if skill is None:
            return []

        items = list(evidence)
        observed = self._successful(items)
        failures = [
            f"skill_missing_required_tool:{name}"
            for name in skill.required_tools
            if name not in observed
        ]

        decision_item = next(
            (item for item in items if item.ok and item.tool_name == "get_decision_evidence"),
            None,
        )
        if skill.name in {"analyze-ticker", "explain-investment-decision", "audit-decision-consistency"}:
            payload = decision_item.payload if decision_item is not None else None
            signals = payload.get("signals") if isinstance(payload, dict) else None
            if not isinstance(signals, list) or not signals:
                failures.append("skill_missing_decision_signals")
            elif task is not None and task.entities:
                observed_symbols = {
                    str(row.get("ticker") or "").upper()
                    for row in signals
                    if isinstance(row, dict)
                }
                missing_symbols = [symbol for symbol in task.entities if symbol not in observed_symbols]
                if missing_symbols:
                    failures.append("skill_missing_signal_entities:" + ",".join(missing_symbols))

        if skill.name == "audit-decision-consistency":
            payload = decision_item.payload if decision_item is not None else None
            plan = payload.get("plan") if isinstance(payload, dict) else None
            decisions = plan.get("decisions") if isinstance(plan, dict) else None
            if not isinstance(decisions, list) or not decisions:
                failures.append("skill_missing_planner_decisions")
            elif task is not None and task.entities:
                planner_symbols = {
                    str(row.get("ticker") or "").upper()
                    for row in decisions
                    if isinstance(row, dict)
                }
                missing_symbols = [symbol for symbol in task.entities if symbol not in planner_symbols]
                if missing_symbols:
                    failures.append("skill_missing_planner_entities:" + ",".join(missing_symbols))

        return list(dict.fromkeys(failures))

    def apply_verification(
        self,
        skill: SkillDefinition | None,
        report: VerificationReport,
        evidence: Iterable[EvidenceObject],
        task: TaskSpec | None = None,
    ) -> VerificationReport:
        failures = self.verification_failures(skill, evidence, task)
        if not failures:
            return report
        return report.model_copy(update={
            "passed": False,
            "failures": list(dict.fromkeys([*report.failures, *failures])),
        })
