from __future__ import annotations

import os

from .schemas import ContextPlan, TaskSpec


_BASE_TOOLS = {
    "get_portfolio_snapshot",
    "get_persisted_decision_evidence",
    "get_decision_evidence",
    "analyze_portfolio",
    "analyze_ticker",
    "get_macro_context",
    "get_macro_exposure",
    "scan_opportunities",
    "get_performance",
    "get_decision_ledger",
    "get_net_decision_report",
    "get_analytics_v2",
    "get_viability_audit",
    "get_regression_audit",
    "get_calibration_audit",
    "get_system_status",
    "get_meta_policy",
    "compare_plan_vs_hold",
    "get_decision_value_added",
    "get_decision_counterfactuals",
    "get_similar_historical_episodes",
    "get_replay_evidence_quality",
    "compare_strategy_versions",
}

_INTENT_TOOLS = {
    # A broad status question should be DB-bound and fast: current persisted
    # holdings + the latest persisted formal decision run. Recomputing the full
    # analysis pipeline belongs to explicit analysis/revalidation requests.
    "portfolio_review": ["get_portfolio_snapshot", "get_persisted_decision_evidence"],
    "position_analysis": ["get_portfolio_snapshot", "get_decision_evidence", "analyze_ticker", "get_macro_context", "get_decision_value_added"],
    "decision_explanation": ["get_portfolio_snapshot", "get_decision_evidence", "analyze_ticker", "get_decision_value_added"],
    "position_comparison": ["get_portfolio_snapshot", "get_decision_evidence", "analyze_ticker", "get_decision_value_added", "scan_opportunities"],
    "opportunities": ["get_portfolio_snapshot", "get_decision_evidence", "scan_opportunities", "get_decision_value_added"],
    "performance": ["get_decision_ledger", "get_performance", "get_net_decision_report"],
    "net_performance": ["get_net_decision_report", "get_decision_ledger"],
    "analytics_v2": ["get_analytics_v2"],
    "viability": ["get_viability_audit"],
    "regression_audit": ["get_regression_audit"],
    "calibration_audit": ["get_calibration_audit"],
    "decision_history": ["get_decision_ledger", "get_performance", "get_decision_value_added"],
    "decision_lab": ["compare_plan_vs_hold", "get_decision_value_added", "get_decision_counterfactuals", "get_similar_historical_episodes", "get_replay_evidence_quality", "compare_strategy_versions"],
    "meta_policy": ["get_meta_policy", "get_decision_evidence", "get_decision_value_added"],
    "market_context": ["get_macro_context", "get_macro_exposure"],
    "system_status": ["get_system_status"],
}

_REQUIRED = {
    "portfolio_review": ["get_portfolio_snapshot", "get_persisted_decision_evidence"],
    "position_analysis": ["get_portfolio_snapshot", "get_decision_evidence"],
    "decision_explanation": ["get_decision_evidence"],
    "position_comparison": ["get_portfolio_snapshot", "get_decision_evidence"],
    "opportunities": ["get_portfolio_snapshot", "scan_opportunities"],
    "performance": ["get_decision_ledger"],
    "net_performance": ["get_net_decision_report"],
    "analytics_v2": ["get_analytics_v2"],
    "viability": ["get_viability_audit"],
    "regression_audit": ["get_regression_audit"],
    "calibration_audit": ["get_calibration_audit"],
    "decision_history": ["get_decision_ledger"],
    "meta_policy": ["get_meta_policy"],
    "market_context": ["get_macro_context"],
    "system_status": ["get_system_status"],
}

_PARALLEL = {
    "portfolio_review": [["get_portfolio_snapshot", "get_persisted_decision_evidence"]],
    "position_analysis": [["get_portfolio_snapshot", "get_decision_evidence", "analyze_ticker"]],
    "decision_explanation": [["get_decision_evidence", "analyze_ticker"]],
    "position_comparison": [["get_portfolio_snapshot", "get_decision_evidence"]],
    "opportunities": [["get_portfolio_snapshot", "scan_opportunities"]],
    "performance": [["get_decision_ledger", "get_performance", "get_net_decision_report"]],
    "net_performance": [["get_net_decision_report", "get_decision_ledger"]],
    "market_context": [["get_macro_context", "get_macro_exposure"]],
}


class ContextSelector:
    """Select the smallest useful tool/context surface for the current task."""

    def select(self, task: TaskSpec, available_tools: set[str]) -> ContextPlan:
        if task.intent == "general":
            allowed = sorted((_BASE_TOOLS & available_tools))
            required: list[str] = []
            parallel: list[list[str]] = []
        else:
            requested = _INTENT_TOOLS.get(task.intent, [])
            allowed = [name for name in requested if name in available_tools]
            required = [name for name in _REQUIRED.get(task.intent, []) if name in available_tools]
            parallel = [
                [name for name in group if name in allowed]
                for group in _PARALLEL.get(task.intent, [])
            ]
            parallel = [group for group in parallel if len(group) > 1]

        # Unknown requests are not blocked by rigid intents: expose the safe
        # registry and let the bounded planner choose.
        if not allowed:
            allowed = sorted(available_tools)

        complex_task = task.intent in {
            "position_analysis", "position_comparison", "opportunities", "decision_lab", "meta_policy"
        }
        return ContextPlan(
            allowed_tools=allowed,
            required_tools=required,
            parallel_groups=parallel,
            max_steps=self._env_int("QUANTIA_HARNESS_MAX_STEPS", 8 if complex_task else 5, 1, 20),
            max_tool_calls=self._env_int("QUANTIA_HARNESS_MAX_TOOL_CALLS", 10 if complex_task else 6, 1, 30),
            max_retries=self._env_int("QUANTIA_HARNESS_MAX_RETRIES", 1, 0, 3),
            max_seconds=self._env_int("QUANTIA_HARNESS_MAX_SECONDS", 300 if complex_task else 180, 1, 1800),
            max_context_chars=self._env_int("QUANTIA_HARNESS_CONTEXT_CHARS", 16000, 1000, 100000),
            max_observation_chars=self._env_int("QUANTIA_HARNESS_OBSERVATION_CHARS", 6000, 500, 100000),
        )

    @staticmethod
    def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
        try:
            return max(minimum, min(maximum, int(os.getenv(name, str(default)))))
        except (TypeError, ValueError):
            return default
