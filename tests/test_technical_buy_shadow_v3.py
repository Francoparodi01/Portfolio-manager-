from src.analysis.technical_buy_shadow_v3 import build_technical_buy_shadow_v3


def _v2(score: float = 0.4, bias: str = "POSITIVE") -> dict:
    return {
        "version": "technical-shadow-v2",
        "score": score,
        "bias": bias,
        "structural_break_gate": False,
    }


def test_strong_uptrend_positive_v2_is_primary_buy_candidate():
    result = build_technical_buy_shadow_v3(
        technical_shadow_v2=_v2(),
        regime="STRONG_UPTREND",
        trend_score=0.8,
        reversion_score=-0.2,
        source_mode="official",
        volume_quality_20=1.0,
    )

    assert result.classification == "PRIMARY_BUY_CANDIDATE"
    assert result.priority_tier == "A"
    assert result.eligible_for_buy_research is True
    assert result.target_horizon_days == 20
    assert result.affects_radar_ranking is False
    assert result.affects_analysis is False
    assert result.affects_execution is False


def test_downtrend_is_rejected_even_when_v2_score_is_positive():
    result = build_technical_buy_shadow_v3(
        technical_shadow_v2=_v2(),
        regime="DOWNTREND",
        trend_score=-0.6,
        reversion_score=0.8,
    )

    assert result.classification == "REJECTED_FOR_BUY_RESEARCH"
    assert "DOWNTREND_NOT_ELIGIBLE" in result.gates


def test_range_reversion_is_secondary_and_data_issues_are_warnings_only():
    result = build_technical_buy_shadow_v3(
        technical_shadow_v2=_v2(0.3),
        regime="RANGE",
        trend_score=0.1,
        reversion_score=0.6,
        asset_type="CEDEAR",
        source_mode="mixed",
        volume_quality_20=0.2,
    )

    assert result.classification == "SECONDARY_BUY_CANDIDATE"
    assert result.priority_tier == "B"
    assert "VOLUME_COVERAGE_LOW" in result.warnings
    assert "CEDEAR_LOCAL_PRICE_INCLUDES_CCL" in result.warnings


def test_v3_does_not_reinterpret_a_non_buy_v2_observation():
    result = build_technical_buy_shadow_v3(
        technical_shadow_v2=_v2(-0.5, "NEGATIVE"),
        regime="STRONG_UPTREND",
        trend_score=0.8,
        reversion_score=-0.3,
    )

    assert result.eligible_for_buy_research is False
    assert "V2_BUY_NOT_ACTIVE" in result.gates
