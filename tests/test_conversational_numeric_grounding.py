from __future__ import annotations

from datetime import datetime, timezone

from src.agentic.harness.schemas import EvidenceMode, EvidenceObject, EvidenceQuality, TaskSpec
from src.agentic.harness.verifier import HarnessVerifier


def _evidence(payload):
    return EvidenceObject(
        source="fixture",
        tool_name="fixture_tool",
        timestamp=datetime.now(timezone.utc),
        payload=payload,
        quality=EvidenceQuality.HIGH,
        mode=EvidenceMode.PRODUCTION,
        ok=True,
    )


def test_verifier_accepts_argentine_thousands_and_decimal_formatting():
    report = HarnessVerifier().verify(
        task=TaskSpec(intent="portfolio_review", raw_message="estado"),
        answer="La cartera vale 2.895.125,00 ARS y el cash es 3.842,34 ARS.",
        evidence=[_evidence({"total_value_ars": 2895125.0, "cash_ars": 3842.34})],
        required_tools=["fixture_tool"],
    )
    assert report.passed
    assert report.numeric_consistency


def test_verifier_accepts_ratio_rendered_as_percentage():
    report = HarnessVerifier().verify(
        task=TaskSpec(intent="portfolio_review", raw_message="estado"),
        answer="NVDA pesa 16,8% de la cartera.",
        evidence=[_evidence({"ticker": "NVDA", "weight": 0.168})],
        required_tools=["fixture_tool"],
    )
    assert report.passed
    assert report.numeric_consistency


def test_verifier_accepts_signed_money_with_spanish_thousands():
    report = HarnessVerifier().verify(
        task=TaskSpec(intent="bot_follow_pnl", raw_message="pnl"),
        answer="El resultado fue -$115.960 ARS sobre 35 planes.",
        evidence=[_evidence({"bot_pnl_5d_ars": -115960.0, "plans_closed_5d": 35})],
        required_tools=["fixture_tool"],
    )
    assert report.passed
    assert report.numeric_consistency


def test_verifier_rejects_multiple_invented_numeric_claims():
    report = HarnessVerifier().verify(
        task=TaskSpec(intent="bot_follow_pnl", raw_message="pnl"),
        answer="El bot ganó $999.999 ARS con 77 operaciones y 88% de acierto.",
        evidence=[_evidence({"bot_pnl_5d_ars": -115960.0, "plans_closed_5d": 35, "win_rate": 0.4})],
        required_tools=["fixture_tool"],
    )
    assert not report.passed
    assert not report.numeric_consistency
    assert "numeric_claims_not_grounded" in report.failures
