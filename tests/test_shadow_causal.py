from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest

from src.analysis.shadow_causal import (
    CausalAnalysisInput,
    analyze_with_ollama,
    build_causal_prompt,
    normalize_causal_response,
)
from src.analysis.shadow_causal_store import ShadowCausalAnalysisStore, ticker_aliases


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "shadow_causal_examples.json"


def _examples() -> list[CausalAnalysisInput]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return [CausalAnalysisInput.from_mapping(row) for row in payload["examples"]]


def _llm_payload(ticker: str) -> dict:
    return {
        "primary_driver": {
            "driver": f"Driver causal plausible para {ticker}",
            "nature": "MIXTO",
            "evidence": ["La evidencia provista combina un factor fundamental y uno transitorio."],
        },
        "durability": {
            "assessment": "El factor puede sostenerse, pero la proyeccion excede lo demostrado.",
            "horizon": "SEMANAS",
            "supporting_factors": ["Hay evidencia operativa o macro a favor."],
            "weakening_signals": ["Hay sensibilidad a valuacion, politica o reversión macro."],
        },
        "reversal_risks": [
            {
                "risk": "Normalizacion del factor transitorio",
                "trigger": "La variable macro que impulso el movimiento revierte",
                "severity": "ALTO",
            },
            {
                "risk": "La mejora operativa no alcanza la valuacion implicita",
                "trigger": "El siguiente reporte no confirma crecimiento suficiente",
                "severity": "MEDIO",
            }
        ],
        "conclusion": "MIXTO",
        "conclusion_reason": "Existe fundamento parcial, no confirmacion suficiente para todo el movimiento.",
        "evidence_gaps": ["No se aportaron resultados posteriores ni guidance cuantificado."],
    }


def test_three_manual_examples_validate_the_structured_contract():
    examples = _examples()
    assert [item.projection.ticker for item in examples] == ["CVX", "AMD", "YPF"]
    assert all(len(item.ticker_news) == 3 for item in examples)

    analyses = [
        normalize_causal_response(item, _llm_payload(item.projection.ticker), model="qwen-test")
        for item in examples
    ]
    assert all(item.conclusion == "MIXTO" for item in analyses)
    assert all(item.reversal_risks for item in analyses)
    assert len({item.input_fingerprint for item in analyses}) == 3


def test_prompt_is_evidence_bounded_and_non_executable():
    prompt = build_causal_prompt(_examples()[0])
    assert "no la recalcules" in prompt
    assert "no propongas compras, ventas, sizing ni targets" in prompt
    assert "Correlacion no prueba causalidad" in prompt
    assert "decision_log" not in prompt


def test_ollama_adapter_uses_json_and_normalizes_response():
    example = _examples()[1]
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content.decode("utf-8")))
        return httpx.Response(
            200,
            json={"message": {"content": json.dumps(_llm_payload("AMD"))}},
        )

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await analyze_with_ollama(
                example,
                model="qwen-test",
                ollama_url="http://ollama.test",
                client=client,
            )

    result = asyncio.run(run())
    assert captured["format"]["type"] == "object"
    risk_schema = captured["format"]["properties"]["reversal_risks"]
    assert risk_schema["minItems"] == 2
    assert "severity" in risk_schema["items"]["required"]
    assert captured["options"]["temperature"] == 0.0
    assert result.ticker == "AMD"
    assert result.model == "qwen-test"


def test_ollama_adapter_retries_a_semantically_invalid_risk():
    example = _examples()[2]
    invalid = _llm_payload("YPF")
    invalid["reversal_risks"][0] = {
        "risk": "Cambio favorable en precios",
        "trigger": "Las decisiones politicas benefician a YPF",
        "severity": "BAJO",
    }
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        calls.append(body)
        payload = invalid if len(calls) == 1 else _llm_payload("YPF")
        return httpx.Response(200, json={"message": {"content": json.dumps(payload)}})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await analyze_with_ollama(
                example,
                model="qwen-test",
                ollama_url="http://ollama.test",
                client=client,
            )

    result = asyncio.run(run())
    assert result.conclusion == "MIXTO"
    assert len(calls) == 2
    assert "invalid risk=" in calls[1]["messages"][-1]["content"]


def test_invalid_conclusion_is_rejected():
    payload = _llm_payload("CVX")
    payload["conclusion"] = "COMPRAR"
    with pytest.raises(ValueError, match="invalid conclusion"):
        normalize_causal_response(_examples()[0], payload, model="qwen-test")


def test_single_reversal_risk_is_rejected():
    payload = _llm_payload("CVX")
    payload["reversal_risks"] = payload["reversal_risks"][:1]
    with pytest.raises(ValueError, match="at least two"):
        normalize_causal_response(_examples()[0], payload, model="qwen-test")


def test_favorable_factor_cannot_be_persisted_as_a_reversal_risk():
    payload = _llm_payload("YPF")
    payload["reversal_risks"][0] = {
        "risk": "Cambio favorable en precios",
        "trigger": "Las decisiones politicas benefician a YPF",
        "severity": "BAJO",
    }
    with pytest.raises(ValueError, match="adverse mechanism"):
        normalize_causal_response(_examples()[2], payload, model="qwen-test")


def test_market_correction_and_valuation_contraction_are_adverse():
    payload = _llm_payload("AMD")
    payload["reversal_risks"][0] = {
        "risk": "Market correction leading to a valuation contraction",
        "trigger": "Semiconductor multiples contract after weak guidance",
        "severity": "ALTO",
    }
    result = normalize_causal_response(_examples()[1], payload, model="qwen-test")
    assert result.reversal_risks[0].severity == "ALTO"


def test_news_input_is_limited_to_three_ticker_items():
    raw = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["examples"][0]
    raw["ticker_news"].append(dict(raw["ticker_news"][0]))
    with pytest.raises(ValueError, match="at most 3"):
        CausalAnalysisInput.from_mapping(raw)


def test_ypf_alias_matches_the_canonical_market_symbol():
    assert ticker_aliases("YPF") == ("YPF", "YPFD")
    assert ticker_aliases("ypfd") == ("YPF", "YPFD")


def test_persistence_writes_only_the_causal_audit_table():
    example = _examples()[0]
    analysis = normalize_causal_response(example, _llm_payload("CVX"), model="qwen-test")

    class Connection:
        query = ""

        async def fetchval(self, query, *args):
            self.query = query
            assert len(args) == 23
            return 17

    class Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class Pool:
        def __init__(self):
            self.conn = Connection()

        def acquire(self):
            return Acquire(self.conn)

    pool = Pool()
    row_id = asyncio.run(
        ShadowCausalAnalysisStore(pool).save_analysis(
            owner_chat_id=0,
            input_data=example,
            analysis=analysis,
        )
    )
    assert row_id == 17
    assert "INSERT INTO shadow_thesis_causal_analysis" in pool.conn.query
    assert "decision_log" not in pool.conn.query
    assert "shadow_thesis_forecasts" not in pool.conn.query


def test_schema_keeps_the_causal_audit_outside_decision_log():
    schema = (Path(__file__).parents[1] / "init.sql").read_text(encoding="utf-8")
    block = schema.split("CREATE TABLE IF NOT EXISTS shadow_thesis_causal_analysis", 1)[1]
    block = block.split("-- Sentiment pipeline", 1)[0]
    assert "conclusion IN ('FUNDADO', 'ESPECULATIVO', 'MIXTO')" in block
    assert "decision_log" not in block
