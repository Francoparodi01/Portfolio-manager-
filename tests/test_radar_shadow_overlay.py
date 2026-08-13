from __future__ import annotations

from datetime import date

from scripts.run_opportunity import _attach_latest_radar_outcomes, _radar_candidate_layers
from src.analysis.opportunity_screener import (
    CandidateStatus,
    OpportunityCandidate,
    OpportunityReport,
    TradeType,
    _shadow_alignment_from_context,
    render_opportunity_report,
)


def test_shadow_alignment_labels_weak_forecast_without_changing_status():
    label, expected_20, probability_20, action = _shadow_alignment_from_context(
        {
            "expected_return_20": 0.023,
            "probability_up_20": 0.53,
            "thesis_action": "ABSTAIN",
        }
    )

    assert label == "SHADOW DÉBIL"
    assert expected_20 == 0.023
    assert probability_20 == 0.53
    assert action == "ABSTAIN"


def test_radar_render_shows_shadow_note_but_keeps_candidate_category():
    candidate = OpportunityCandidate(
        ticker="UPST",
        status=CandidateStatus.COMPRABLE_AHORA,
        trade_type=TradeType.NEW_ENTRY,
        final_score=0.21,
        conviction=0.72,
        price_usd=10.06,
        action_concreta="Evaluar entrada solo con pullback",
        shadow_alignment="SHADOW DÉBIL",
        shadow_expected_return_20=0.023,
        shadow_probability_up_20=0.53,
        shadow_action="ABSTAIN",
    )
    report = OpportunityReport(
        universe_size=1,
        screened_count=1,
        ranked_count=1,
        displayed_count=1,
        candidates=[candidate],
        comprable_ahora=[candidate],
    )

    rendered = render_opportunity_report(report, market_session_open=True)

    assert candidate.status == CandidateStatus.COMPRABLE_AHORA
    assert "SHADOW DÉBIL" in rendered
    assert "no perseguir sin pullback/catalyst" in rendered


def test_radar_candidate_layers_include_shadow_context():
    candidate = OpportunityCandidate(
        ticker="AMD",
        status=CandidateStatus.VIGILANCIA_A,
        trade_type=TradeType.WATCHLIST,
        final_score=0.12,
        conviction=0.60,
        shadow_alignment="SHADOW CONFIRMA",
        shadow_expected_return_20=0.231,
        shadow_probability_up_20=0.79,
        shadow_action="WATCH_ENTRY",
        reversion_score=0.43,
        reversion_components={"rsi": 1.5, "bollinger": 2.0},
        technical_shadow_v2={
            "version": "technical-shadow-v2",
            "score": 0.31,
            "bias": "POSITIVE",
            "affects_analysis": False,
            "affects_execution": False,
        },
        technical_buy_shadow_v3={
            "version": "technical-buy-shadow-v3",
            "classification": "PRIMARY_BUY_CANDIDATE",
            "priority_tier": "A",
            "eligible_for_buy_research": True,
            "affects_radar_ranking": False,
            "affects_analysis": False,
            "affects_execution": False,
        },
    )

    layers = _radar_candidate_layers(candidate)

    assert layers["shadow_alignment"] == "SHADOW CONFIRMA"
    assert layers["shadow_expected_return_20"] == 0.231
    assert layers["shadow_probability_up_20"] == 0.79
    assert layers["shadow_action"] == "WATCH_ENTRY"
    assert layers["reversion_shadow"] == {
        "score": 0.43,
        "components": {"rsi": 1.5, "bollinger": 2.0},
        "informational_only": True,
    }
    assert layers["technical_shadow_v2"]["version"] == "technical-shadow-v2"
    assert layers["technical_shadow_v2"]["score"] == 0.31
    assert layers["technical_shadow_v2"]["affects_execution"] is False
    assert layers["technical_buy_shadow_v3"]["version"] == "technical-buy-shadow-v3"
    assert layers["technical_buy_shadow_v3"]["priority_tier"] == "A"
    assert layers["technical_buy_shadow_v3_protocol"] == {
        "cohort": "prospective_daily_radar",
        "target_horizon_days": 20,
        "benchmark": "same_date_eligible_universe_median",
        "promotion_eligible": False,
        "affects_radar_ranking": False,
        "affects_analysis": False,
        "affects_execution": False,
    }


def test_radar_render_shows_buy_v3_without_changing_candidate_status():
    candidate = OpportunityCandidate(
        ticker="AMD",
        status=CandidateStatus.VIGILANCIA_A,
        trade_type=TradeType.WATCHLIST,
        final_score=0.12,
        conviction=0.60,
        price_usd=81_825,
        action_concreta="Vigilar confirmación",
        technical_buy_shadow_v3={
            "version": "technical-buy-shadow-v3",
            "classification": "PRIMARY_BUY_CANDIDATE",
            "priority_tier": "A",
            "affects_radar_ranking": False,
        },
    )
    report = OpportunityReport(
        universe_size=1,
        screened_count=1,
        ranked_count=1,
        displayed_count=1,
        candidates=[candidate],
        en_vigilancia=[candidate],
    )

    rendered = render_opportunity_report(report)

    assert candidate.status == CandidateStatus.VIGILANCIA_A
    assert "Compra técnica V3" in rendered
    assert "compra primaria" in rendered


def test_radar_render_shows_reversion_and_prior_audited_returns():
    candidate = OpportunityCandidate(
        ticker="AMD",
        status=CandidateStatus.VIGILANCIA_A,
        trade_type=TradeType.WATCHLIST,
        final_score=0.12,
        conviction=0.60,
        price_usd=81_825,
        reversion_score=0.50,
        reversion_components={"rsi": 1.5, "bollinger": 2.0},
        action_concreta="Vigilar confirmación",
    )
    report = OpportunityReport(candidates=[candidate], en_vigilancia=[candidate])
    attached = _attach_latest_radar_outcomes(
        report,
        {
            "AMD": {
                "decided_at": date(2026, 7, 1),
                "outcomes": {5: 0.041, 10: 0.05, 20: None, 40: None},
            }
        },
    )

    rendered = render_opportunity_report(report)

    assert attached == 1
    assert "posible rebote alcista" in rendered
    assert "RSI, Bollinger" in rendered
    assert "Última idea radar 01/07/2026" in rendered
    assert "5D +4.1%" in rendered
    assert "20D pendiente" in rendered


def test_radar_render_shows_shadow_for_non_operable_candidates():
    candidate = OpportunityCandidate(
        ticker="AMD",
        status=CandidateStatus.NO_OPERABLE,
        trade_type=TradeType.WATCHLIST,
        final_score=0.11,
        conviction=0.64,
        price_usd=81_825,
        action_concreta="No operar",
        shadow_alignment="SHADOW CONFIRMA",
        shadow_expected_return_20=0.231,
        shadow_probability_up_20=0.79,
        shadow_action="WATCH_ENTRY",
    )
    report = OpportunityReport(
        universe_size=1,
        screened_count=1,
        ranked_count=1,
        displayed_count=1,
        candidates=[candidate],
        no_operables=[candidate],
    )

    rendered = render_opportunity_report(report)

    assert "SHADOW CONFIRMA" in rendered
    assert "Shadow confirma continuidad" in rendered
