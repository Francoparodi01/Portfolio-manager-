from __future__ import annotations

import json
from pathlib import Path

from src.agentic.harness.context import ContextSelector
from src.agentic.harness.permissions import PermissionPolicy
from src.agentic.harness.schemas import ConversationState
from src.agentic.harness.task import TaskParser


ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / "evals" / "conversations" / "harness_cases.json"

AVAILABLE_TOOLS = {
    "get_portfolio_snapshot", "get_decision_evidence", "analyze_portfolio", "analyze_ticker",
    "get_macro_context", "get_macro_exposure", "scan_opportunities", "get_performance",
    "get_decision_ledger", "get_net_decision_report", "get_analytics_v2", "get_viability_audit",
    "get_system_status", "get_meta_policy", "compare_plan_vs_hold", "get_decision_value_added",
    "get_decision_counterfactuals", "get_similar_historical_episodes", "get_replay_evidence_quality",
    "compare_strategy_versions",
}


def test_conversation_eval_dataset_routes_expected_intents_and_safe_tools():
    dataset = json.loads(DATASET.read_text(encoding="utf-8"))
    assert dataset["schema_version"] == "quantia-conversation-evals-v1"
    parser = TaskParser()
    selector = ContextSelector()
    policy = PermissionPolicy()

    for case in dataset["cases"]:
        state = ConversationState(owner_chat_id=123)
        actual_intents = []
        for turn in case["turns"]:
            task = parser.parse(turn, state)
            actual_intents.append(task.intent)
            if task.entities:
                state.active_symbols = task.entities
                state.conversation_subject = " vs ".join(task.entities[:2])
            state.last_intent = task.intent
        assert actual_intents == case["expected_intents"], case["id"]

        final_task = parser.parse(case["turns"][-1], state)
        plan = selector.select(final_task, AVAILABLE_TOOLS)
        for tool in case.get("must_offer_tools", []):
            assert tool in plan.allowed_tools, (case["id"], tool)
        for tool in case.get("forbidden_tools", []):
            assert tool not in plan.allowed_tools
            assert not policy.allow(tool)
