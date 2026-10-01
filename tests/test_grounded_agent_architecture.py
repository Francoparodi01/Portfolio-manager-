from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from src.agentic.contracts import ToolSpec, ToolValidationError
from src.agentic.docs_retriever import search_project_docs, search_project_source
from src.agentic.grounded_model import GroundedQuantiaAgentModel
from src.agentic.prompt_context import load_agent_prompt_context, missing_prompt_context_files
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
    assert missing_prompt_context_files(context) == ()


def test_missing_prompt_context_is_detectable(tmp_path):
    (tmp_path / "AGENTS.md").write_text("fixture", encoding="utf-8")
    context = load_agent_prompt_context(tmp_path)
    assert missing_prompt_context_files(context) == (
        "docs/agent/quantia-semantics.md",
        "docs/agent/quantia-data-model.md",
    )


def test_docker_context_keeps_runtime_grounding_and_rag_docs():
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
    lines = [line.strip() for line in dockerignore.splitlines() if line.strip() and not line.startswith("#")]
    assert "docs" not in lines
    assert "*.md" not in lines
    assert "AGENTS.md" not in lines
    assert "docs/agent" not in lines

    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "test -f /app/AGENTS.md" in dockerfile
    assert "test -f /app/docs/agent/quantia-semantics.md" in dockerfile
    assert "test -f /app/docs/agent/quantia-data-model.md" in dockerfile


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
    assert "search_quantia_source" in prompt
    assert "Never request a write capability" in prompt


def test_dynamic_planner_can_choose_generic_sql_without_hardcoded_route(monkeypatch):
    model = GroundedQuantiaAgentModel(model="fixture", project_context="fixture")

    async def fake_call(_payload):
        return json.dumps({
            "kind": "tool",
            "tool": "query_quantia_sql",
            "arguments": {
                "sql": "SELECT decision, COUNT(*) AS n FROM decision_log GROUP BY decision",
                "purpose": "comparar decisiones",
                "max_rows": 50,
            },
            "rationale": "Necesito agrupar evidencia histórica.",
        })

    monkeypatch.setattr(model, "_call", fake_call)
    tool = ToolSpec(
        name="query_quantia_sql",
        description="fixture",
        input_schema={
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "purpose": {"type": "string"},
                "max_rows": {"type": "integer"},
            },
            "required": ["sql"],
            "additionalProperties": False,
        },
    )
    decision = asyncio.run(model.decide(
        goal="Buscá patrones de decisiones por tu cuenta.",
        tools=[tool],
        history=[],
        step_no=1,
        max_steps=4,
    ))
    assert decision.kind == "tool"
    assert decision.tool_name == "query_quantia_sql"


def test_grounded_llm_synthesis_keeps_runtime_provenance(monkeypatch):
    model = GroundedQuantiaAgentModel(model="fixture", project_context="fixture")

    async def fake_call(_payload):
        return json.dumps({
            "kind": "final",
            "answer": "En la muestra observada, SELL muestra un EV mayor que BUY; lo trataría como evidencia exploratoria, no como regla de producción.",
            "rationale": "La consulta ya devolvió la comparación necesaria.",
        })

    monkeypatch.setattr(model, "_call", fake_call)
    history = [{
        "decision": {"tool": "query_quantia_sql"},
        "observation": {
            "tool_name": "query_quantia_sql",
            "ok": True,
            "content": json.dumps({
                "schema_version": "quantia-sql-explorer-v1",
                "query_sha256": "a" * 64,
                "rows": [{"decision": "SELL", "ev": 0.03}],
            }),
        },
    }]
    decision = asyncio.run(model.decide(
        goal="Compará BUY y SELL.",
        tools=[],
        history=history,
        step_no=2,
        max_steps=4,
    ))
    assert decision.kind == "final"
    assert decision.answer_origin == "grounded_llm_v1"
    assert "Fuentes auditadas: query_quantia_sql" in decision.answer
    assert "aaaaaaaaaaaa" in decision.answer


def test_sql_explorer_accepts_owner_scoped_aggregate_shape():
    query, relations = validate_exploratory_sql(
        "SELECT decision, COUNT(*) AS n, AVG(outcome_20d) AS ev FROM decision_log "
        "WHERE outcome_20d IS NOT NULL GROUP BY decision ORDER BY n DESC"
    )
    assert relations == ("decision_log",)
    wrapped = _scoped_query(query, relations, allow_legacy_null=False, max_rows=100)
    assert "public.decision_log WHERE owner_chat_id = $1" in wrapped
    assert "LIMIT 100" in wrapped


def test_sql_explorer_accepts_a_single_trailing_semicolon():
    query, relations = validate_exploratory_sql("SELECT ticker FROM decision_log LIMIT 5;")
    assert query == "SELECT ticker FROM decision_log LIMIT 5"
    assert relations == ("decision_log",)


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


def test_docs_retriever_returns_grounded_snippet_and_hash():
    payload = search_project_docs(ROOT, "Decision Lab DVA HOLD", max_results=5)
    assert payload["schema_version"] == "quantia-doc-search-v1"
    assert payload["results"]
    assert all(item["path"].endswith(".md") for item in payload["results"])
    assert all(len(item["sha256"]) == 64 for item in payload["results"])


def test_source_retriever_reads_allowlisted_code_but_not_environment_files(tmp_path):
    source_dir = tmp_path / "scripts"
    source_dir.mkdir()
    (source_dir / "telegram_bot.py").write_text(
        "async def action_analysis_full():\n    --no-persist\n    run_intent = exploratory\n",
        encoding="utf-8",
    )
    (tmp_path / ".env").write_text("run_intent=secret\n", encoding="utf-8")

    payload = search_project_source(tmp_path, "action_analysis_full no-persist exploratory")

    assert payload["schema_version"] == "quantia-source-search-v1"
    assert payload["results"]
    assert payload["results"][0]["path"] == "scripts/telegram_bot.py"
    assert payload["results"][0]["start_line"] >= 1
    assert all(len(item["sha256"]) == 64 for item in payload["results"])
    assert all(".env" not in item["path"] for item in payload["results"])


def test_grounded_eval_corpus_has_safety_and_source_selection_coverage():
    cases = json.loads((ROOT / "evals/agent/grounded_queries_v1.json").read_text(encoding="utf-8"))
    assert len(cases) >= 10
    classes = {case["expected_source_class"] for case in cases}
    assert "exploratory_sql" in classes
    assert "canonical_decision_lab" in classes
    assert "refuse_write" in classes
    assert any("outcome_filled_at" in " ".join(case.get("must_not", [])) for case in cases)
