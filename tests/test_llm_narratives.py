from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest

from src.analysis.llm_narratives import (
    build_decision_explanation_prompt,
    build_market_report_prompt,
    explain_decision_with_ollama,
    generate_market_narrative_with_ollama,
    known_fact_ids,
    normalize_decision_explanation,
    normalize_market_narrative,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _market_packet() -> dict:
    return json.loads((FIXTURE_DIR / "llm_market_report_packet.json").read_text(encoding="utf-8"))


def _decision_packet() -> dict:
    return json.loads((FIXTURE_DIR / "llm_decision_evidence_packet.json").read_text(encoding="utf-8"))


def _market_payload() -> dict:
    return {
        "headline": "Apertura positiva con cobertura completa",
        "executive_summary": "La cartera abre en terreno positivo y la lectura es publicable.",
        "sections": [
            {
                "title": "Cartera",
                "paragraph": "El valor total y la variacion diaria muestran una apertura positiva.",
                "supporting_fact_ids": [
                    "portfolio.total_value_ars",
                    "portfolio.day_change_pct",
                ],
            },
            {
                "title": "Cobertura",
                "paragraph": "La cobertura de precios es suficiente para publicar el informe.",
                "supporting_fact_ids": ["coverage.priced_weight_pct"],
            },
        ],
        "caveats": [],
        "insufficiency_flag": False,
    }


def _decision_payload() -> dict:
    return {
        "effective_action": "rebalance",
        "short_explanation": "La salida es un rebalanceo parcial por sobrepeso, no una tesis bearish.",
        "primary_reason_codes": ["overweight"],
        "supporting_fact_ids": [
            "weights.current.axp",
            "weights.target.axp",
            "planner.reason.overweight",
        ],
        "constraints_applied": ["planner.reason.overweight"],
        "rejected_interpretations": ["No implica venta por deterioro fundamental."],
        "insufficiency_flag": False,
    }


def test_market_prompt_is_bounded_to_qwen_and_fact_ids():
    prompt = build_market_report_prompt(_market_packet())
    assert "Qwen/Ollama" in prompt
    assert "No inventes numeros" in prompt
    assert "No recomiendes compras" in prompt
    assert "portfolio.day_change_pct" in prompt


def test_decision_prompt_separates_rebalance_from_bearish_thesis():
    prompt = build_decision_explanation_prompt(_decision_packet())
    assert "Si fue rebalanceo, no lo conviertas en tesis bearish" in prompt
    assert "No confundas ejecucion, rebalanceo, scoring" in prompt
    assert "planner.reason.overweight" in prompt


def test_known_fact_ids_are_extracted_from_packet():
    assert known_fact_ids(_decision_packet()) == {
        "weights.current.axp",
        "weights.target.axp",
        "planner.reason.overweight",
    }


def test_market_narrative_validates_supporting_fact_ids():
    result = normalize_market_narrative(_market_packet(), _market_payload(), model="qwen-test")
    assert result.headline == "Apertura positiva con cobertura completa"
    assert result.sections[0].supporting_fact_ids == (
        "portfolio.total_value_ars",
        "portfolio.day_change_pct",
    )
    assert result.model == "qwen-test"


def test_market_narrative_canonicalizes_underscore_fact_id_aliases():
    payload = _market_payload()
    payload["sections"][0]["supporting_fact_ids"] = [
        "portfolio_total_value_ars",
        "portfolio_day_change_pct",
    ]
    result = normalize_market_narrative(_market_packet(), payload, model="qwen-test")
    assert result.sections[0].supporting_fact_ids == (
        "portfolio.total_value_ars",
        "portfolio.day_change_pct",
    )


def test_market_narrative_rejects_unknown_fact_id():
    payload = _market_payload()
    payload["sections"][0]["supporting_fact_ids"] = ["not.in.packet"]
    with pytest.raises(ValueError, match="unknown fact_ids"):
        normalize_market_narrative(_market_packet(), payload, model="qwen-test")


def test_market_narrative_rejects_unsupported_ticker_expansion():
    packet = _market_packet()
    packet["evidence_items"].append(
        {
            "kind": "position",
            "fact_id": "position.axp.summary",
            "ticker": "AXP",
            "source": "portfolio",
        }
    )
    payload = _market_payload()
    payload["sections"][0]["paragraph"] = "AXP (American Express) lidera el movimiento."
    with pytest.raises(ValueError, match="unsupported ticker expansion"):
        normalize_market_narrative(packet, payload, model="qwen-test")


def test_low_coverage_requires_insufficiency_flag():
    packet = _market_packet()
    packet["coverage"]["priced_weight_pct"] = "0.55"
    payload = _market_payload()
    payload["insufficiency_flag"] = False
    with pytest.raises(ValueError, match="insufficiency_flag"):
        normalize_market_narrative(packet, payload, model="qwen-test")


def test_decision_explanation_validates_action_and_support():
    result = normalize_decision_explanation(_decision_packet(), _decision_payload(), model="qwen-test")
    assert result.effective_action == "rebalance"
    assert result.primary_reason_codes == ("overweight",)
    assert result.supporting_fact_ids[-1] == "planner.reason.overweight"


def test_decision_explanation_rejects_action_mismatch():
    payload = _decision_payload()
    payload["effective_action"] = "sell"
    with pytest.raises(ValueError, match="effective_action mismatch"):
        normalize_decision_explanation(_decision_packet(), payload, model="qwen-test")


def test_decision_explanation_rejects_rebalance_as_bearish_thesis():
    payload = _decision_payload()
    payload["primary_reason_codes"] = ["bearish_thesis"]
    with pytest.raises(ValueError, match="invalid semantic mix"):
        normalize_decision_explanation(_decision_packet(), payload, model="qwen-test")


def test_market_ollama_adapter_uses_json_schema_and_normalizes_response():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content.decode("utf-8")))
        return httpx.Response(200, json={"message": {"content": json.dumps(_market_payload())}})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await generate_market_narrative_with_ollama(
                _market_packet(),
                model="qwen-test",
                ollama_url="http://ollama.test",
                client=client,
            )

    result = asyncio.run(run())
    assert captured["model"] == "qwen-test"
    assert captured["format"]["required"] == [
        "headline",
        "executive_summary",
        "sections",
        "caveats",
        "insufficiency_flag",
    ]
    assert captured["options"]["temperature"] == 0.0
    assert result.headline.startswith("Apertura positiva")


def test_decision_ollama_adapter_retries_invalid_response():
    calls = []
    invalid = _decision_payload()
    invalid["supporting_fact_ids"] = ["not.in.packet"]

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content.decode("utf-8")))
        payload = invalid if len(calls) == 1 else _decision_payload()
        return httpx.Response(200, json={"message": {"content": json.dumps(payload)}})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await explain_decision_with_ollama(
                _decision_packet(),
                model="qwen-test",
                ollama_url="http://ollama.test",
                client=client,
            )

    result = asyncio.run(run())
    assert len(calls) == 2
    assert "incumple el contrato" in calls[1]["messages"][-1]["content"]
    assert result.effective_action == "rebalance"
