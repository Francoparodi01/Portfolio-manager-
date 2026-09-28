from __future__ import annotations

from pathlib import Path

import pytest

from src.agentic.contracts import ToolSpec, ToolValidationError
from src.agentic.grounded_model import GroundedQuantiaAgentModel
from src.agentic.prompt_context import load_agent_prompt_context
from src.agentic.sql_explorer import _scoped_query, validate_exploratory_sql


ROOT = Path(__file__).resolve().parents[1]


def test_prompt_context_loads_contract_semantics_and_catalog():
    context = load_agent_prompt_context(ROOT)
    assert "Quantia Agent Operating Contract" in context.text
    assert "Quantia semantic layer" in context.text
    assert "Quantia agent data catalog" in context.text
    assert set(context.source_hashes) == {
        "AGENTS.md",
        "docs/agent/quantia-semantics.md",
        "docs/agent/quantia-data-model.md",
    }


def test_grounded_model_injects_dynamic_planning_context():
    model = GroundedQuantiaAgentModel(model="fixture", project_context="final_score is not PnL")
    tool = ToolSpec(
        name="query_quantia_sql",
        description="fixture",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
    )
    prompt = model._system_prompt([tool], 1, 4, False)
    assert "final_score is not PnL" in prompt
    assert "decide the evidence plan yourself" in prompt
    assert "query_quantia_sql" in prompt
    assert "Never request a write capability" in prompt


def test_sql_explorer_accepts_owner_scoped_aggregate_shape():
    query, relations = validate_exploratory_sql(
        "SELECT decision, COUNT(*) AS n, AVG(outcome_20d) AS ev FROM decision_log "
        "WHERE outcome_20d IS NOT NULL GROUP BY decision ORDER BY n DESC"
    )
    assert relations == ("decision_log",)
    wrapped = _scoped_query(query, relations, allow_legacy_null=False, max_rows=100)
    assert "public.decision_log WHERE owner_chat_id = $1" in wrapped
    assert "LIMIT 100" in wrapped


def test_sql_explorer_legacy_scope_is_explicit():
    query, relations = validate_exploratory_sql("SELECT ticker, final_score FROM decision_log LIMIT 5")
    wrapped = _scoped_query(query, relations, allow_legacy_null=True, max_rows=5)
    assert "owner_chat_id = $1 OR owner_chat_id IS NULL" in wrapped


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM decision_log",
        "SELECT * FROM public.decision_log",
        "SELECT * FROM users",
        "SELECT * FROM decision_log; SELECT * FROM broker_fills",
        "SELECT * FROM decision_log -- bypass",
        "SELECT pg_sleep(10) FROM decision_log",
        "SELECT * FROM decision_log UNION SELECT * FROM broker_fills",
        "SELECT * FROM decision_log, broker_fills",
        "SELECT * FROM (SELECT * FROM decision_log) x",
    ],
)
def test_sql_explorer_rejects_unsafe_or_ambiguous_sql(sql):
    with pytest.raises(ToolValidationError):
        validate_exploratory_sql(sql)


def test_sql_explorer_allows_explicit_join_between_scoped_relations():
    query, relations = validate_exploratory_sql(
        "SELECT d.ticker, COUNT(*) AS n FROM decision_log d "
        "JOIN broker_fills b ON b.ticker = d.ticker GROUP BY d.ticker"
    )
    assert query.startswith("SELECT")
    assert relations == ("decision_log", "broker_fills")
