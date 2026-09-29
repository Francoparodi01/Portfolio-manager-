from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.agentic.harness.schemas import (
    ClaimStatus,
    EvidenceMode,
    EvidenceObject,
    EvidenceQuality,
    TaskSpec,
)
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


def _tool_evidence(
    tool_name: str,
    *,
    source: str = "fixture",
    ok: bool = True,
    timestamp: datetime | None = None,
    payload=None,
):
    return EvidenceObject(
        source=source,
        tool_name=tool_name,
        timestamp=timestamp or datetime.now(timezone.utc),
        payload={} if payload is None else payload,
        quality=EvidenceQuality.HIGH,
        mode=EvidenceMode.PRODUCTION,
        ok=ok,
        warnings=[] if ok else ["tool_error:fixture"],
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


def test_claim_verifier_marks_required_claims_supported_and_optional_gaps_visible():
    report = HarnessVerifier().verify(
        task=TaskSpec(intent="position_analysis", raw_message="Analizá GDX", entities=["GDX"]),
        answer="La evidencia requerida de cartera y decisión está disponible.",
        evidence=[
            _tool_evidence("get_portfolio_snapshot", source="portfolio"),
            _tool_evidence("get_decision_evidence", source="decision"),
        ],
        required_tools=["get_portfolio_snapshot", "get_decision_evidence"],
    )

    claims = {item.claim_id: item for item in report.claim_results}
    assert report.passed
    assert report.required_claim_coverage == 1.0
    assert claims["portfolio_state"].status == ClaimStatus.SUPPORTED
    assert claims["current_decision"].status == ClaimStatus.SUPPORTED
    assert claims["historical_edge"].status == ClaimStatus.MISSING
    assert claims["market_context"].status == ClaimStatus.MISSING
    assert "optional_claim_missing:historical_edge" in report.warnings


def test_claim_verifier_fails_closed_when_required_claim_is_missing():
    report = HarnessVerifier().verify(
        task=TaskSpec(intent="position_analysis", raw_message="Analizá GDX", entities=["GDX"]),
        answer="Sólo pude observar la cartera.",
        evidence=[_tool_evidence("get_portfolio_snapshot", source="portfolio")],
        required_tools=["get_portfolio_snapshot", "get_decision_evidence"],
    )

    claims = {item.claim_id: item for item in report.claim_results}
    assert not report.passed
    assert report.required_claim_coverage == 0.5
    assert claims["current_decision"].status == ClaimStatus.MISSING
    assert "required_claim_not_supported:current_decision:missing" in report.failures


def test_claim_verifier_treats_stale_required_portfolio_evidence_as_unsupported():
    report = HarnessVerifier().verify(
        task=TaskSpec(intent="position_analysis", raw_message="Analizá GDX", entities=["GDX"]),
        answer="La cartera observada es antigua; la decisión actual sí está disponible.",
        evidence=[
            _tool_evidence(
                "get_portfolio_snapshot",
                source="portfolio",
                timestamp=datetime.now(timezone.utc) - timedelta(days=2),
            ),
            _tool_evidence("get_decision_evidence", source="decision"),
        ],
        required_tools=["get_portfolio_snapshot", "get_decision_evidence"],
    )

    claims = {item.claim_id: item for item in report.claim_results}
    assert not report.passed
    assert claims["portfolio_state"].status == ClaimStatus.STALE
    assert report.required_claim_coverage == 0.5
    assert "get_portfolio_snapshot" in report.stale_or_missing_sources
    assert "required_claim_not_supported:portfolio_state:stale" in report.failures


def test_claim_verifier_distinguishes_failed_tool_from_missing_evidence():
    report = HarnessVerifier().verify(
        task=TaskSpec(intent="meta_policy", raw_message="Explicá Meta Policy de GDX", entities=["GDX"]),
        answer="La consulta de Meta Policy falló; no la doy por demostrada.",
        evidence=[
            _tool_evidence("get_meta_policy", source="meta_policy", ok=False),
            _tool_evidence("get_decision_evidence", source="decision"),
        ],
        required_tools=["get_meta_policy"],
    )

    claims = {item.claim_id: item for item in report.claim_results}
    assert not report.passed
    assert claims["meta_policy_state"].status == ClaimStatus.FAILED
    assert claims["current_decision"].status == ClaimStatus.SUPPORTED
    assert "required_claim_not_supported:meta_policy_state:failed" in report.failures
