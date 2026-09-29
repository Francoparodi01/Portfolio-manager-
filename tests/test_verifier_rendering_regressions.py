from __future__ import annotations

from datetime import datetime, timezone

from src.agentic.harness.schemas import EvidenceMode, EvidenceObject, EvidenceQuality, TaskSpec
from src.agentic.harness.verifier import HarnessVerifier


def _evidence(tool_name: str, payload: dict, source: str) -> EvidenceObject:
    return EvidenceObject(
        source=source,
        tool_name=tool_name,
        timestamp=datetime.now(timezone.utc),
        payload=payload,
        quality=EvidenceQuality.HIGH,
        mode=EvidenceMode.PRODUCTION,
        ok=True,
    )


def test_portfolio_renderer_rounding_and_localized_timestamp_are_grounded():
    task = TaskSpec(
        intent="portfolio_review",
        raw_message="¿Cuál es hoy la decisión más importante de mi cartera y por qué?",
    )
    snapshot = _evidence(
        "get_portfolio_snapshot",
        {
            "scraped_at": "2026-09-29T22:56:03.960000+00:00",
            "total_value_ars": 2817520.4519448215,
            "cash_ars": 3838.8375576665503,
            "positions": [
                {"ticker": "NVDA", "weight": 0.17462},
                {"ticker": "GDX", "weight": 0.12144},
            ],
        },
        "portfolio",
    )
    decisions = _evidence(
        "get_persisted_decision_evidence",
        {
            "evaluated_at": "2026-09-29T13:17:28.123456+00:00",
            "snapshot_as_of": "2026-09-29T13:15:00+00:00",
            "signals": [
                {"ticker": "IREN", "decision": "REDUCE", "final_score": -0.05841, "status": "APPROVED"},
                {"ticker": "MU", "decision": "REDUCE", "final_score": -0.08461, "status": "APPROVED"},
                {"ticker": "GDX", "decision": "BUY", "final_score": -0.02261, "status": "APPROVED"},
            ],
        },
        "decision",
    )
    answer = (
        "Tu cartera tiene 2.817.520,45 ARS y 3.838,84 ARS de cash. "
        "La mayor concentración está en NVDA 17,5% y GDX 12,1%. "
        "Las señales incluyen IREN REDUCE (score -0,058), MU REDUCE (score -0,085) "
        "y GDX BUY (score -0,023). Último análisis formal: 29/09 10:17 ART."
    )

    report = HarnessVerifier().verify(
        task=task,
        answer=answer,
        evidence=[snapshot, decisions],
        required_tools=["get_portfolio_snapshot", "get_persisted_decision_evidence"],
    )

    assert report.passed
    assert report.numeric_consistency
    assert report.required_claim_coverage == 1.0


def test_verifier_still_rejects_unrelated_invented_numbers_after_rounding_fix():
    task = TaskSpec(intent="portfolio_review", raw_message="estado cartera")
    evidence = [
        _evidence(
            "get_portfolio_snapshot",
            {"total_value_ars": 2817520.45, "cash_ars": 3838.84, "positions": []},
            "portfolio",
        ),
        _evidence(
            "get_persisted_decision_evidence",
            {"signals": []},
            "decision",
        ),
    ]
    answer = "La cartera vale 9.999.999 ARS, tiene 88 posiciones y ganó 77%."

    report = HarnessVerifier().verify(
        task=task,
        answer=answer,
        evidence=evidence,
        required_tools=["get_portfolio_snapshot", "get_persisted_decision_evidence"],
    )

    assert not report.passed
    assert not report.numeric_consistency
    assert "numeric_claims_not_grounded" in report.failures
